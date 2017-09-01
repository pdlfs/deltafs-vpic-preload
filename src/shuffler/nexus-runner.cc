/*
 * Copyright (c) 2017, Carnegie Mellon University.
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
 * nexus-runner.cc  run deltafs-nexus and report results
 * 14-Jun-2017  chuck@ece.cmu.edu
 */

/*
 * this program tests/benchmarks the deltafs-nexus shuffle/routing
 * module.  we use MPI to managing the processes in the test.  the
 * test requires deltafs-nexus (which pulls in Mercury and MPI
 * itself).
 *
 * this is a peer-to-peer style application, so it contains both a
 * mercury RPC client and a mercury RPC server.  the client sends
 * "count" number of shuffler send requests via nexus to random ranks.
 * the application exits when all requested sends have completed
 * (finished processes will wait at a MPI barrier until all sending
 * and processing has completed).
 *
 * nexus-runner was initially based on the mercury-runner
 * test/benchmark program (thus the name), but it serves a different
 * function (e.g. nexus-runner doesn't do mercury bulk ops).
 *
 * to use this program you need to launch it as an MPI application.
 * the launch process will determine the number of nodes allocated and
 * the number of processes per node.  nexus uses MPI_Comm_split_type
 * to determine the node-level configuration.  thus nexus-runner
 * itself does not have any topology configuration command line flags,
 * it uses whatever it gets from the MPI launcher.
 *
 * the shuffler queue config controls how much buffering is used and
 * how many RPCs can be active at one time.
 *
 * usage: nexus-runner [options] mercury-protocol subnet
 *
 * options:
 *  -c count     number of shuffle send ops to perform
 *  -e           exclude sending to ourself (skip those sends)
 *  -l           loop through dsts rather than random sends
 *  -n minsndr   rank must be >= minsndr to send requests
 *  -o m         add 'm' msec output delay to delivery
 *  -p baseport  base port number
 *  -q           quiet mode - don't print during RPCs
 *  -r n         enable tag suffix with this run number
 *  -R n         only send to rank 'n'
 *  -s maxsndr   rank must be <= maxsndr to send requests
 *  -t secs      timeout (alarm)
 *
 * shuffler queue config:
 *  -B bytes     batch buffer target for network output queues
 *  -b bytes     batch buffer target for shared memory output queues
 *  -d count     delivery queue limit
 *  -M count     maxrpcs for network output queues
 *  -m count     maxrpcs for shared memory output queues
 *
 * size related options:
 * -i size     input req size (> 12 if specified)
 *
 * the input reqs contain:
 *
 *  <seq,src,dest><extra bytes...>
 *
 * (so 3*sizeof(int) == 12, assuming 32 bit ints).  the "-i" flag can
 * be used to add additional un-used data to the payload if desired.
 *
 * logging related options (rank <= max can have xtra logging, use -X):
 *  -C mask      mask cfg for non-extra rank procs
 *  -E mask      mask cfg for extra rank procs
 *  -D priority  default log priority
 *  -F logfile   logfile (rank # will be appended)
 *  -I n         message buffer size (0=disable)
 *  -L           enable logging
 *  -O options   options (a=alllogs, s=stderr, x=xtra stderr)
 *  -S priority  print to stderr priority
 *  -X n         max extra rank#
 *
 * priorities are: ERR, WARN, NOTE, INFO, DBG, DBG0, DBG1, DBG2, DBG3
 * facilities are: CLI (client), DLV (delivery), SHF (general shuffle)
 * masks can be spec'd like: CLI=ERR,DLV=INFO,SHF=WARN
 *
 * examples:
 *
 *   ./nexus-runner -c 50 -q cci+tcp 10.92
 *
 * XXXCDC: port# handling --- maybe just add rank to base
 * XXXCDC: handle caches?
 * XXXCDC: non-ip may not be possible with nexus
 * XXXCDC: when know to exit?   (flushing)
 */

#include <ctype.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>

#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>

#include <mercury.h>
#include <mercury_macros.h>

#include <mpi.h>   /* XXX: nexus requires this */

#include <deltafs-nexus/deltafs-nexus_api.h>

#include "shuffler.h"

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
char *argv0;                     /* argv[0], program name */
int myrank = 0;

/*
 * vcomplain/complain about something.  if ret is non-zero we exit(ret)
 * after complaining.  if r0only is set, we only print if myrank == 0.
 */
void vcomplain(int ret, int r0only, const char *format, va_list ap) {
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

void complain(int ret, int r0only, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    vcomplain(ret, r0only, format, ap);
    va_end(ap);
}

/*
 * start-end usage state
 */
struct useprobe {
    int who;                /* flag to getrusage */
    struct timeval t0, t1;
    struct rusage r0, r1;
};

/* load starting values into useprobe */
static void useprobe_start(struct useprobe *up, int who) {
    up->who = who;
    if (gettimeofday(&up->t0, NULL) < 0 || getrusage(up->who, &up->r0) < 0)
        complain(1, 0, "useprobe_start syscall failed?!");
}


/* load final values into useprobe */
static void useprobe_end(struct useprobe *up) {
    if (gettimeofday(&up->t1, NULL) < 0 || getrusage(up->who, &up->r1) < 0)
        complain(1, 0, "useprobe_end syscall failed?!");
}

/* print useprobe info */
void useprobe_print(FILE *out, struct useprobe *up, const char *tag, int n) {
    char nstr[32];
    double start, end;
    double ustart, uend, sstart, send;
    long nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw;

    if (n >= 0) {
        snprintf(nstr, sizeof(nstr), "%d: ", n);
    } else {
        nstr[0] = '\0';
    }

    start = up->t0.tv_sec + (up->t0.tv_usec / 1000000.0);
    end = up->t1.tv_sec + (up->t1.tv_usec / 1000000.0);

    ustart = up->r0.ru_utime.tv_sec + (up->r0.ru_utime.tv_usec / 1000000.0);
    uend = up->r1.ru_utime.tv_sec + (up->r1.ru_utime.tv_usec / 1000000.0);

    sstart = up->r0.ru_stime.tv_sec + (up->r0.ru_stime.tv_usec / 1000000.0);
    send = up->r1.ru_stime.tv_sec + (up->r1.ru_stime.tv_usec / 1000000.0);

    nminflt = up->r1.ru_minflt - up->r0.ru_minflt;
    nmajflt = up->r1.ru_majflt - up->r0.ru_majflt;
    ninblock = up->r1.ru_inblock - up->r0.ru_inblock;
    noublock = up->r1.ru_oublock - up->r0.ru_oublock;
    nnvcsw = up->r1.ru_nvcsw - up->r0.ru_nvcsw;
    nnivcsw = up->r1.ru_nivcsw - up->r0.ru_nivcsw;

    fprintf(out, "%s%s: times: wall=%f, usr=%f, sys=%f (secs)\n", nstr, tag,
        end - start, uend - ustart, send - sstart);
    fprintf(out,
      "%s%s: minflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld\n",
      nstr, tag, nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw);
}

/*
 * getsize: a souped up version of atoi() that handles suffixes like
 * 'k' (so getsize("1k") == 1024).
 */
int64_t getsize(char *from) {
    int len, end;
    int64_t rv;

    len = strlen(from);
    if (len == 0)
        return(0);
    rv = atoi(from);
    end = tolower(from[len-1]);
    switch (end) {    /* ordered to fallthrough */
        case 'g':
            rv = rv * 1024;
        case 'm':
            rv = rv * 1024;
        case 'k':
            rv = rv * 1024;
    }

    return(rv);
}

/*
 * end of helper/utility functions.
 */

/*
 * default values for port and count
 */
#define DEF_BASEPORT 19900 /* starting TCP port we listen on (instance 0) */
#define DEF_BUFTARGET 1    /* target #bytes for a batch */
#define DEF_COUNT 5        /* default # of msgs to send and recv in a run */
#define DEF_DELIVERQMAX 1  /* max# of reqs in deliverq before using waitq */
#define DEF_MAXRPCS 1      /* max# of outstanding RPCs */
#define DEF_TIMEOUT 120    /* alarm timeout */

/*
 * gs: shared global data (e.g. from the command line)
 */
struct gs {
    int ninst;               /* currently locked at 1 */
    /* note: MPI rank stored in global "myrank" */
    int size;                /* world size (from MPI) */
    char *hgproto;           /* hg protocol to use */
    char *hgsubnet;          /* subnet to use (XXX: assumes IP) */
    int baseport;            /* base port number */
    int buftarg_net;         /* batch target for network queues */
    int buftarg_shm;         /* batch target for shared memory queues */
    int count;               /* number of msgs to send/recv in a run */
    int excludeself;         /* exclude sending to self (skip those sends) */
    int deliverq_max;        /* max# reqs in deliverq before waitq */
    int loop;                /* loop through dsts rather than random sends */
    int minsndr;             /* rank must be >= minsndr to send requests */
    int odelay;              /* delay delivery output this many msec */
    struct timespec odspec;  /* odelay in a timespec for nanosleep(3) */
    int maxrpcs_net;         /* max # outstanding RPCs, network */
    int maxrpcs_shm;         /* max # outstanding RPCs, shared memory */
    int quiet;               /* don't print so much */
    int rflag;               /* -r tag suffix spec'd */
    int rflagval;            /* value for -r */
    int rcvr_only;           /* only send to this rank (if >0) */
    int maxsndr;             /* rank must be <= maxsndr to send requests */
    int timeout;             /* alarm timeout */

    char tagsuffix[64];      /* tag suffix: ninst-count-mode-limit-run# */

    /*
     * inreq size includes bytes used for seq,src,dest.
     * if is zero then we just have those three numbers.  otherwise
     * it must be > 12 to account for the header (we pad the rest).
     */
    int inreqsz;             /* input request size */

    /* logging */
    int lenable;             /* enable logging */
    char *cmask;             /* mask cfg for non-extra rank procs */
    char *emask;             /* mask cfg for extra ranks */
    const char *defpri;      /* default priority */
    char *logfile;           /* logfile */
    int msgbufsz;            /* msgbuffer size */
    int o_alllogs;           /* if logfile, create on non-extra ranks */
    int o_stderr;            /* always log to stderr (non extra ranks) */
    int o_xstderr;           /* always log to stderr (xtra ranks) */
    const char *serrpri;     /* stderr priority */
    int max_xtra;            /* max extra rank# */
} g;

/*
 * is: per-instance state structure.   currently we only allow
 * one instance per proc (but we keep this broken out in case
 * we want to change it...).
 */
struct is {
    int n;                   /* our instance number (0 .. n-1) */
    nexus_ctx_t nxp;         /* nexus context */
    char myfun[64];          /* my function name */
    shuffler_t shand;        /* shuffler handler */
    int nsends;              /* number of times we've called send */
    int ncallbacks;          /* #times our callback was called */
};
struct is *isa;    /* an array of state */

/*
 * alarm signal handler
 */
void sigalarm(int foo) {
    int lcv;
    fprintf(stderr, "SIGALRM detected (%d)\n", myrank);
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        fprintf(stderr, "%d: %d: @alarm: ", myrank, lcv);
        fprintf(stderr, "nsends=%d, ncallbacks=%d\n",
                isa[lcv].nsends, isa[lcv].ncallbacks);
    }
    fprintf(stderr, "Alarm clock\n");
    MPI_Finalize();
    exit(1);
}

/*
 * usage
 */
static void usage(const char *msg) {

    /* only have rank 0 print usage error message */
    if (myrank) goto skip_prints;

    if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
    fprintf(stderr, "usage: %s [options] mercury-protocol subnet\n", argv0);
    fprintf(stderr, "\noptions:\n");
    fprintf(stderr, "\t-c count    number of shuffle send ops to perform\n");
    fprintf(stderr, "\t-e          exclude sending to elf (skip sends)\n");
    fprintf(stderr, "\t-l          loop through dsts (no random sends)\n");
    fprintf(stderr, "\t-n minsndr  rank must be >= minsndr to send requests\n");
    fprintf(stderr, "\t-o m        add 'm' msec output delay to delivery\n");
    fprintf(stderr, "\t-p port     base port number\n");
    fprintf(stderr, "\t-q          quiet mode\n");
    fprintf(stderr, "\t-r n        enable tag suffix with this run number\n");
    fprintf(stderr, "\t-R rank     only do sends to this rank\n");
    fprintf(stderr, "\t-s maxsndr  rank must be <= maxsndr to send requests\n");
    fprintf(stderr, "\t-t sec      timeout (alarm), in seconds\n");

    fprintf(stderr, "shuffler queue config:\n");
    fprintf(stderr, "\t-B bytes    batch buf target for network\n");
    fprintf(stderr, "\t-b bytes    batch buf target for shm\n");
    fprintf(stderr, "\t-d count    delivery queue size limit\n");
    fprintf(stderr, "\t-M count    maxrpcs for network output queues\n");
    fprintf(stderr, "\t-m count    maxrpcs for shm output queues\n");
    fprintf(stderr, "\nsize related options:\n");
    fprintf(stderr, "\t-i size     input req size (> 12 if specified)\n");
    fprintf(stderr, "\ndefault payload size is 12.\n\n");
    fprintf(stderr,
     "logging related options (rank <= max can have xtra logging, use -X):\n");
    fprintf(stderr, "\t-C mask      mask cfg for non-extra rank procs\n");
    fprintf(stderr, "\t-E mask      mask cfg for extra rank procs\n");
    fprintf(stderr, "\t-D priority  default log priority\n");
    fprintf(stderr, "\t-F logfile   logfile (rank # will be appended)\n");
    fprintf(stderr, "\t-I n         message buffer size (0=disable)\n");
    fprintf(stderr, "\t-L           enable logging\n");
    fprintf(stderr, "\t-O options   opts (a=alllogs,s=stderr,x=xtra stderr)\n");
    fprintf(stderr, "\t-S priority  print to stderr priority\n");
    fprintf(stderr, "\t-X n         max extra rank#\n");

skip_prints:
    MPI_Finalize();
    exit(1);
}

/*
 * forward prototype decls.
 */
static void *run_instance(void *arg);   /* run one instance */
static void do_delivery(int src, int dst, int type, void *d, int datalen);

/*
 * main program.  usage:
 *
 * ./nexus-runner [options] mercury-protocol subnet
 */
int main(int argc, char **argv) {
    struct timeval tv;
    int ch, lcv, rv;
    pthread_t *tarr;
    struct useprobe mainuse;
    char mytag[128];

    argv0 = argv[0];

    /* mpich says we should call this early as possible */
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(stderr, "%s: MPI_Init failed.  MPI is required.\n", argv0);
        exit(1);
    }

    /* we want lines, even if we are writing to a pipe */
    setlinebuf(stdout);

    /* init random for random data */
    (void)gettimeofday(&tv, NULL);
    srandom(getpid() + tv.tv_sec);

    /* setup default to zero/null, except as noted below */
    memset(&g, 0, sizeof(g));
    if (MPI_Comm_rank(MPI_COMM_WORLD, &myrank) != MPI_SUCCESS)
        complain(1, 0, "unable to get MPI rank");
    if (MPI_Comm_size(MPI_COMM_WORLD, &g.size) != MPI_SUCCESS)
        complain(1, 0, "unable to get MPI size");
    g.baseport = DEF_BASEPORT;
    g.buftarg_net = DEF_BUFTARGET;
    g.buftarg_shm = DEF_BUFTARGET;
    g.count = DEF_COUNT;
    g.deliverq_max = DEF_DELIVERQMAX;
    g.maxrpcs_net = DEF_MAXRPCS;
    g.maxrpcs_shm = DEF_MAXRPCS;
    g.rcvr_only = -1;            /* disable by default */
    g.minsndr = 0;
    g.maxsndr = g.size - 1;      /* everyone sends by default */
    g.timeout = DEF_TIMEOUT;

    g.defpri = "WARN";
    g.serrpri = "CRIT";
    g.max_xtra = g.size;

    while ((ch = getopt(argc, argv,
            "B:b:C:c:D:d:E:eF:I:i:LlM:m:n:O:o:p:qR:r:S:s:t:X:")) != -1) {
        switch (ch) {
            case 'B':
                g.buftarg_net = atoi(optarg);
                if (g.buftarg_net < 1) usage("bad buftarget net");
                break;
            case 'b':
                g.buftarg_shm = atoi(optarg);
                if (g.buftarg_shm < 1) usage("bad buftarget shm");
                break;
            case 'C':
                g.cmask = optarg;
                break;
            case 'c':
                g.count = atoi(optarg);
                if (g.count < 1) usage("bad count");
                break;
            case 'D':
                g.defpri = optarg;
                break;
            case 'd':
                g.deliverq_max = atoi(optarg);
                if (g.deliverq_max < 1) usage("bad deliverq_max shm");
                break;
            case 'E':
                g.emask = optarg;
                break;
            case 'e':
                g.excludeself = 1;
                break;
            case 'F':
                g.logfile = optarg;
                break;
            case 'I':
                g.msgbufsz = getsize(optarg);
                if (g.msgbufsz < 0) usage("bad msgbuf size");
                break;
            case 'i':
                g.inreqsz = getsize(optarg);
                if (g.inreqsz <= 12) usage("bad inreqsz (must be > 12)");
                break;
            case 'L':
                g.lenable = 1;
                break;
            case 'l':
                g.loop = 1;
                break;
            case 'M':
                g.maxrpcs_net = atoi(optarg);
                if (g.maxrpcs_net < 1) usage("bad maxrpc net");
                break;
            case 'm':
                g.maxrpcs_shm = atoi(optarg);
                if (g.maxrpcs_shm < 1) usage("bad maxrpc shm");
                break;
            case 'n':
                g.minsndr = atoi(optarg);
                if (g.minsndr < 0 || g.minsndr >= g.size)
                    usage("bad min sender");
                break;
            case 'O':
                g.o_alllogs = (strchr(optarg, 'a') != NULL);
                g.o_stderr =  (strchr(optarg, 's') != NULL);
                g.o_xstderr = (strchr(optarg, 'x') != NULL);
                break;
            case 'o':
                g.odelay = atoi(optarg);
                if (g.odelay < 0) usage("bad output delay");
                g.odspec.tv_sec  = g.odelay / 1000;
                g.odspec.tv_nsec = (g.odelay % 1000) * 1000000;
                break;
            case 'p':
                g.baseport = atoi(optarg);
                if (g.baseport < 1) usage("bad port");
                break;
            case 'q':
                g.quiet = 1;
                break;
            case 'R':
                g.rcvr_only = atoi(optarg);
                if (g.rcvr_only < 0 || g.rcvr_only >= g.size)
                  usage("bad -R recv only rank");
                break;
            case 'r':
                g.rflag++;  /* will gen tag suffix after args parsed */
                g.rflagval = atoi(optarg);
                break;
            case 'S':
                g.serrpri = optarg;
                break;
            case 's':
                g.maxsndr = atoi(optarg);
                if (g.maxsndr < 0 || g.maxsndr >= g.size)
                    usage("bad max sender");
                break;
            case 't':
                g.timeout = atoi(optarg);
                if (g.timeout < 0) usage("bad timeout");
                break;
            case 'X':
                g.max_xtra = atoi(optarg);
                break;
            default:
                usage(NULL);
        }
    }
    argc -= optind;
    argv += optind;

    if (argc != 2)          /* hgproto and hgsubnet must be provided on cli */
      usage("bad args");
    g.ninst = 1;
    g.hgproto = argv[0];
    g.hgsubnet = argv[1];
    if (g.rflag) {
        snprintf(g.tagsuffix, sizeof(g.tagsuffix), "-%d-%d",
                 g.count, g.rflagval);
    }

    if (myrank == 0) {
        printf("\n%s options:\n", argv0);
        printf("\tMPI_rank   = %d\n", myrank);
        printf("\tMPI_size   = %d\n", g.size);
        printf("\thgproto    = %s\n", g.hgproto);
        printf("\thgsubnet   = %s\n", g.hgsubnet);
        printf("\tbaseport   = %d\n", g.baseport);
        printf("\tcount      = %d\n", g.count);
        printf("\texcludeself= %d\n", g.excludeself);
        printf("\tloop       = %d\n", g.loop);
        printf("\tquiet      = %d\n", g.quiet);
        if (g.rflag)
            printf("\tsuffix     = %s\n", g.tagsuffix);
        if (g.rcvr_only >= 0)
            printf("\trcvr_only  = %d\n", g.rcvr_only);
        printf("\tminsndr    = %d\n", g.minsndr);
        printf("\tmaxsndr    = %d\n", g.maxsndr);
        printf("\ttimeout    = %d\n", g.timeout);
        printf("sizes:\n");
        printf("\tbuftarget  = %d / %d (net/shm)\n", g.buftarg_net,
               g.buftarg_shm);
        printf("\tmaxrpcs    = %d / %d (net/shm)\n", g.maxrpcs_net,
               g.maxrpcs_shm);
        printf("\tdeliverqmx = %d\n", g.deliverq_max);
        if (g.odelay > 0)
            printf("\tout_delay  = %d msec\n", g.odelay);
        printf("\tinput      = %d\n", (g.inreqsz == 0) ? 12 : g.inreqsz);
        if (!g.lenable) {
            printf("\tlogging    = disabled\n");
        } else {
            printf("\tlogging    = enabled\n");
            printf("\tmax_xtra   = %d\n", g.max_xtra);
            printf("\tdefpri     = %s\n", g.defpri);
            printf("\tstderrpri  = %s\n", g.serrpri);
            printf("\tmsgbufsize = %d\n", g.msgbufsz);
            if (g.logfile)
                printf("\tlogfile    = %s\n", g.logfile);
            if (g.cmask)
                printf("\tcmask      = %s\n", g.cmask);
            if (g.emask)
                printf("\temask      = %s\n", g.emask);
            if (g.o_alllogs)
                printf("\talllogs    = on\n");
            if (g.o_stderr)
                printf("\tostderr    = on\n");
            if (g.o_xstderr)
                printf("\toxstderr   = on\n");
        }
        printf("\n");
    }

    /* plug in the log options */
    if (g.lenable) {
        rv = shuffler_cfglog(g.max_xtra, g.defpri, g.serrpri, g.cmask,
                             g.emask, g.logfile, g.o_alllogs, g.msgbufsz,
                             g.o_stderr, g.o_xstderr);
        if (rv < 0) {
            fprintf(stderr, "shuffler_cfglog failed!\n");
            exit(-1);
        }
    }

    signal(SIGALRM, sigalarm);
    alarm(g.timeout);
    if (myrank == 0) printf("main: starting ...\n");

    tarr = (pthread_t *)malloc(g.ninst * sizeof(pthread_t));
    if (!tarr) complain(1, 0, "malloc tarr thread array failed");
    isa = (struct is *)malloc(g.ninst *sizeof(*isa));    /* array */
    if (!isa) complain(1, 0, "malloc 'isa' instance state failed");
    memset(isa, 0, g.ninst * sizeof(*isa));

    /* fork off a thread for each instance */
    useprobe_start(&mainuse, RUSAGE_SELF);
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        isa[lcv].n = lcv;
        rv = pthread_create(&tarr[lcv], NULL, run_instance, (void*)&isa[lcv]);
        if (rv != 0)
            complain(1, 0, "pthread create failed %d", rv);
    }

    /* now wait for everything to finish */
    if (myrank == 0) printf("main: collecting\n");
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        pthread_join(tarr[lcv], NULL);
    }
    useprobe_end(&mainuse);

    if (myrank == 0) printf("main: collection done.\n");
    snprintf(mytag, sizeof(mytag), "ALL%s", g.tagsuffix);
    if (myrank == 0 || !g.quiet)
        useprobe_print(stdout, &mainuse, mytag, -1);

    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0) printf("main exiting...\n");

    MPI_Finalize();
    exit(0);
}

/*
 * run_instance: the main routine for running one instance of mercury.
 * we pass the instance state struct in as the arg...
 */
void *run_instance(void *arg) {
    struct is *isp = (struct is *)arg;
    int n = isp->n;               /* recover n from isp */
    nexus_ret_t nrv;
    int lcv, sendto, mylen;
    hg_return_t ret;
    uint32_t *msg, msg_store[3];

    printf("%d: instance running\n", myrank);
    isa[n].n = n;    /* make it easy to map 'is' structure back to n */

    /* setup send buffer based on requested size (-i) */
    if (g.inreqsz <= 12) {
        msg = msg_store;
        mylen = 12;
    } else {
        msg = (uint32_t *)calloc(1, g.inreqsz);
        if (msg == NULL)
            complain(1, 0, "malloc of inreq failed");
        mylen = g.inreqsz;
    }

    isa[n].nxp = nexus_bootstrap(g.hgsubnet, g.hgproto);
    if (!isa[n].nxp)
        complain(1, 0, "%d: nexus_bootstrap failed", myrank);
    printf("%d: nexus powered up!\n", myrank);

    /* make a funcion name and register it in both HGs */
    snprintf(isa[n].myfun, sizeof(isa[n].myfun), "f%d", n);

    isa[n].shand = shuffler_init(isa[n].nxp, isa[n].myfun, g.maxrpcs_shm,
                   g.buftarg_shm, g.maxrpcs_net, g.buftarg_net,
                   g.deliverq_max, do_delivery);

    if (myrank >= g.minsndr && myrank <= g.maxsndr) {
        for (lcv = 0 ; lcv < g.count ; lcv++) {
            if (g.loop) {
                sendto = (myrank + lcv) % g.size;
            } else {
                sendto = random() % g.size;
            }

            /* skip sendto if we've limited who we send to */
            if (g.rcvr_only >= 0 && sendto != g.rcvr_only)
                continue;
            if (g.excludeself && sendto == myrank)
                continue;

            msg[0] = htonl(lcv);
            msg[1] = htonl(myrank);
            msg[2] = htonl(sendto);
            if (!g.quiet)
                printf("%d: snd msg %d->%d, t=%d, lcv=%d, sz=%d\n",
                       myrank, myrank, sendto, lcv % 4, lcv, mylen);
            /* vary type value by mod'ing lcv by 4 */
            ret = shuffler_send(isa[n].shand, sendto, lcv % 4,
                                msg, mylen);
            if (ret != HG_SUCCESS)
                fprintf(stderr, "shuffler_send failed(%d)\n", ret);
        }
    }

    /* done sending */
    printf("%d: sends complete!\n", myrank);
    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0)
        printf("%d: crossed send barrier.\n", myrank);

    /* flush it now */
    ret = shuffler_flush_localqs(isa[n].shand);  /* clear out SRC->SRCREP */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush local failed(%d)\n", ret);
    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0)
        printf("%d: flushed local (hop1).\n", myrank);

    ret = shuffler_flush_remoteqs(isa[n].shand); /* clear SRCREP->DSTREP */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush remote failed(%d)\n", ret);
    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0)
        printf("%d: flushed remote (hop2).\n", myrank);

    ret = shuffler_flush_localqs(isa[n].shand);  /* clear DSTREP->DST */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush local2 failed(%d)\n", ret);
    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0)
        printf("%d: flushed local (hop3).\n", myrank);

    ret = shuffler_flush_delivery(isa[n].shand); /* clear deliverq */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush delivery failed(%d)\n", ret);
    if (myrank == 0)
        printf("%d: flushed delivery.\n", myrank);

    ret = shuffler_shutdown(isa[n].shand);
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush shutdown failed(%d)\n", ret);
    printf("%d: shuf shutdown.\n", myrank);

    nexus_destroy(isa[n].nxp);
    if (msg != msg_store) free(msg);

    return(NULL);
}

/*
 * do_delivery: callback from shuffler for doing a local delivery
 *
 * @param src src rank
 * @param dst dst rank (should be us!)
 * @param type request type (user defined)
 * @param d data buffer
 * @param datalen length of data buffer
 */
static void do_delivery(int src, int dst, int type, void *d, int datalen) {
    uint32_t msg[3];
    struct timespec rem;

    if (datalen == sizeof(msg))
        memcpy(msg, d, datalen);  /* just copy the data since it is small */
    else
        memset(msg, 0, sizeof(msg));

    if (!g.quiet)
        printf("%d: got msg %d->%d, t=%d, len=%d [%d %d %d]\n",
               myrank, src, dst, type, datalen,
               ntohl(msg[0]), ntohl(msg[1]), ntohl(msg[2]));

    if (g.odelay > 0)    /* add some fake processing delay if requested */
        nanosleep(&g.odspec, &rem);
}
