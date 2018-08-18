/*
 * Copyright (c) 2018, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * preload_runner.cc a simple vpic io skeleton program for emulating
 * vpic workloads that use the file-per-particle io pattern.
 */

/*
 * To run this program, either compile and link this program with the
 * rest preload code, or compile and link it without the preload
 * code but use LD_PRELOAD at runtime to intercept io calls.
 */
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <mpi.h>

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
static char* argv0; /* argv[0], program name */
static int myrank = 0;

/* emulated particle name and data */
static char* pdata;
static char pname[256];
static size_t psz; /* total write size per particle */

/*
 * vcomplain/complain about something.  if ret is non-zero we exit(ret)
 * after complaining.  if r0only is set, we only print if myrank == 0.
 */
static void vcomplain(int ret, int r0only, const char* format, va_list ap) {
  if (!r0only || myrank == 0) {
    fprintf(stderr, "%s: ", argv0);
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
  }
  if (ret) {
    MPI_Finalize();
    exit(ret);
  }
}

static void complain(int ret, int r0only, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vcomplain(ret, r0only, format, ap);
  va_end(ap);
}

/*
 * default values
 */
#define DEF_STEPTIME 1.0    /* secs per vpic timestep */
#define DEF_NSTEPS 15       /* total steps */
#define DEF_NDUMPS 3        /* total epoch dumps */
#define DEF_PARTICLESIZE 40 /* bytes per particle */
#define DEF_NPARTICLES 16   /* total particles per rank */
#define DEF_TIMEOUT 120     /* alarm timeout */

/*
 * gs: shared global data (e.g. from the command line)
 */
static struct gs {
  char pdir[128]; /* particle dirname */
  /* note: MPI rank stored in global "myrank" */
  int size;           /* world size (from MPI) */
  double steptime;    /* computation time per vpic timestep (sec) */
  int p[3];           /* particles on x,y,z dimension */
  int t[3];           /* topology on x,y,z dimension  */
  const char* deckid; /* vpic deck id (vpic app name) */
  const char* deck;   /* vpic deck (run time) */
  int nsteps;         /* total vpic timesteps to execute */
  int ndumps;         /* total dumps to perform */
  int psize;          /* total state per vpic particle (bytes) */
  int nps;            /* number of particles per rank */
  int timeout;        /* alarm timeout */
} g;

/*
 * alarm signal handler
 */
static void sigalarm(int foo) {
  fprintf(stderr, "SIGALRM detected (%d)\n", myrank);
  fprintf(stderr, "Alarm clock\n");
  MPI_Finalize();
  exit(1);
}

/*
 * usage
 */
static void usage(const char* msg) {
  /* only have rank 0 print usage error message */
  if (myrank) goto skip_prints;

  if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
  fprintf(stderr,
          "usage: %s [options] [deck deck_id px py pz tx ty tz "
          "num_dumps num_steps]\n",
          argv0);
  fprintf(stderr, "\noptions:\n");
  fprintf(stderr, "\t-b bytes    bytes for each particle\n");
  fprintf(stderr, "\t-c count    number of particles to simulate per rank\n");
  fprintf(stderr, "\t-d dump     number of frame dumps\n");
  fprintf(stderr, "\t-o output   particle output dir (can be relative)\n");
  fprintf(stderr, "\t-s step     number of steps to perform\n");
  fprintf(stderr, "\t-T time     step time in seconds\n");
  fprintf(stderr, "\t-t sec      timeout (alarm), in seconds\n");

skip_prints:
  MPI_Finalize();
  exit(1);
}

/*
 * forward prototype decls.
 */
static void run_vpic_app();
static void do_dump();

/*
 * main program.
 */
int main(int argc, char* argv[]) {
  char* env;
  int ch;

  argv0 = argv[0];

  /* mpich says we should call this early as possible */
  if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
    complain(EXIT_FAILURE, 1, "%s: MPI_Init failed.  MPI is required.", argv0);
  }

  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  /* setup default to zero/null, except as noted below */
  memset(&g, 0, sizeof(g));
  if (MPI_Comm_rank(MPI_COMM_WORLD, &myrank) != MPI_SUCCESS)
    complain(EXIT_FAILURE, 0, "unable to get MPI rank");
  if (MPI_Comm_size(MPI_COMM_WORLD, &g.size) != MPI_SUCCESS)
    complain(EXIT_FAILURE, 0, "unable to get MPI size");

  strcpy(g.pdir, "particle");
  g.p[0] = g.p[1] = g.p[2] = g.t[0] = g.t[1] = g.t[2] = -1;
  g.deckid = g.deck = "unknown";

  g.steptime = DEF_STEPTIME;
  g.nsteps = DEF_NSTEPS;
  g.ndumps = DEF_NDUMPS;
  g.psize = DEF_PARTICLESIZE;
  g.nps = DEF_NPARTICLES;
  g.timeout = DEF_TIMEOUT;

  while ((ch = getopt(argc, argv, "b:c:d:o:s:T:t:")) != -1) {
    switch (ch) {
      case 'b':
        g.psize = atoi(optarg);
        if (g.psize < 0) usage("bad particle bytes");
        break;
      case 'c':
        g.nps = atoi(optarg);
        if (g.nps < 0) usage("bad num particles per rank");
        break;
      case 'd':
        g.ndumps = atoi(optarg);
        if (g.ndumps < 0) usage("bad num dumps");
        break;
      case 'o':
        strncpy(g.pdir, optarg, sizeof(g.pdir));
        break;
      case 's':
        g.nsteps = atoi(optarg);
        if (g.nsteps < 0) usage("bad num steps");
        break;
      case 'T':
        g.steptime = atof(optarg);
        if (g.steptime < 0) usage("bad steptime");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
        break;
      default:
        usage(NULL);
    }
  }
  argc -= optind;
  argv += optind;

  /* all other args are optional, but are honored
   * when they are set, and override previous settings */
  if (argc > 0) g.deck = argv[0];
  if (argc > 1) g.deckid = argv[1];
  if (argc > 2) g.p[0] = atoi(argv[2]);
  if (argc > 3) g.p[1] = atoi(argv[3]);
  if (argc > 4) g.p[2] = atoi(argv[4]);
  if (argc > 5) g.t[0] = atoi(argv[5]);
  if (argc > 6) g.t[1] = atoi(argv[6]);
  if (argc > 7) g.t[2] = atoi(argv[7]);
  if (argc > 8) g.ndumps = atoi(argv[8]);
  if (argc > 9) g.nsteps = atoi(argv[9]);

  if (g.p[0] != -1 && g.p[1] != -1 && g.p[2] != -1) {
    g.nps = 100LL * g.p[0] * g.p[1] * g.p[2] / g.size;
  }

  /* check env vars */
  env = getenv("PRELOAD_Particle_size");
  if (env && env[0]) {
    g.psize = atoi(env);
    if (g.psize < 0) {
      g.psize = 0;
    }
  }

  if (myrank == 0) {
    printf("== VPIC options:\n");
    printf(" > MPI_rank   = %d\n", myrank);
    printf(" > MPI_size   = %d\n", g.size);
    printf(" > deck       = %s\n", g.deck);
    printf(" > deckid     = %s\n", g.deckid);
    printf(" > @p         = [ %d x %d x %d ]\n", g.p[0], g.p[1], g.p[2]);
    printf(" > @t         = [ %d x %d x %d ]\n", g.t[0], g.t[1], g.t[2]);
    printf(" > output_dir = %s\n", g.pdir);
    printf(" > time_per_step       = %.3f secs\n", g.steptime);
    printf(" > bytes_per_particle  = %d bytes\n", g.psize);
    if (g.nps > 1000000)
      printf(" > num particles       = %d M per rank\n", g.nps / 1000000);
    else if (g.nps > 1000)
      printf(" > num particles       = %d K per rank\n", g.nps / 1000);
    else
      printf(" > num particles       = %d per rank\n", g.nps);
    printf(" > num_dumps  = %d\n", g.ndumps);
    printf(" > num_steps  = %d\n", g.nsteps);
    printf(" > timeout    = %d secs\n", g.timeout);
    printf("\n");
  }

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);
  if (myrank == 0) printf("== VPIC Starting ...\n");

  psz = 8 + static_cast<size_t>(g.psize);
  pdata = (char*)malloc(psz);
  if (!pdata) complain(EXIT_FAILURE, 0, "malloc pdata failed");
  memset(pdata, 'x', psz);
  run_vpic_app();
  MPI_Barrier(MPI_COMM_WORLD);
  if (myrank == 0) printf("== VPIC Exiting...\n");
  free(pdata);

  MPI_Finalize();
  return 0;
}

static void run_vpic_app() {
  int rv = 0;
  if (myrank == 0) {
    rv = mkdir(g.pdir, 0777);
  }
  if (rv != 0) {
    complain(EXIT_FAILURE, 0, "mkdir %s failed errno=%d", g.pdir, errno);
  }
  for (int epoch = 0; epoch < g.ndumps; epoch++) {
    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0) printf("== VPIC Epoch %d ...\n", epoch + 1);
    int steps = g.nsteps / g.ndumps; /* vpic timesteps per epoch */
    usleep(int(g.steptime * steps * 1000 * 1000));
    do_dump();
  }
}

static void do_dump() {
  FILE* file;
  DIR* dir;
  dir = opendir(g.pdir);
  if (!dir) {
    complain(EXIT_FAILURE, 0, "!opendir errno=%d", errno);
  }
  const int prefix = snprintf(pname, sizeof(pname), "%s/", g.pdir);
  memcpy(pdata, &myrank, 4);
  for (int p = 0; p < g.nps; p++) {
    snprintf(pname + prefix, sizeof(pname) - prefix, "%08X%08X", myrank, p);
    file = fopen(pname, "a");
    if (!file) {
      complain(EXIT_FAILURE, 0, "!fopen errno=%d", errno);
    }
    memcpy(pdata + 4, &p, 4);
    fwrite(pdata, 1, psz, file);
    fclose(file);
  }
  closedir(dir);
}
