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
 * usage: nexus-runner [options] mercury-protocol [subnet]
 *
 * options:
 *  -c count     number of shuffle send ops to perform
 *  -e           exclude sending to ourself (skip those sends)
 *  -f rate      do a flush (collective) every 'rate' sends
 *  -l           loop through dsts rather than random sends
 *  -N filespec  do a nexus_dump() to filespec at startup
 *  -n minsndr   rank must be >= minsndr to send requests
 *  -o m         add 'm' msec output delay to delivery
 *  -p baseport  base port number
 *  -q           quiet mode - don't print during RPCs
 *  -R n         only send to rank 'n'
 *  -r n         enable tag suffix with this run number
 *  -s maxsndr   rank must be <= maxsndr to send requests
 *  -T           report extra time/usage stats info for instance thread
 *  -t secs      timeout (alarm)
 *  -x           use network progressor for local requests (single hg mode)
 *
 * shuffler queue config:
 *  -B bytes     batch buffer target for network output queues
 *  -a bytes     batch buffer target for origin/client local output queues
 *  -b bytes     batch buffer target for relayed local output queues (to dst)
 *  -d count     delivery queue limit
 *  -h count     delivery thread wakeup threshold
 *  -M count     maxrpcs for network output queues
 *  -m count     maxrpcs for origin/client local output queues
 *  -y count     maxrpcs for relayed local output queues (to dst)
 *  -Z count     remote RPC limit on shuffler_send
 *  -z count     local RPC limit on shuffler_send
 *
 * size related options:
 *  -i size      input req size (> 12 if specified)
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
#include <ifaddrs.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>

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

#ifdef RUSAGE_THREAD
#define USEPROBE_THREAD RUSAGE_THREAD   /* linux-specific? */
#else
#define USEPROBE_THREAD RUSAGE_SELF     /* fallback if THREAD not available */
#endif

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
    char nstr[32], msg[256];
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

    snprintf(msg, sizeof(msg), "%s%s: times: wall=%f, usr=%f, sys=%f (secs)\n"
        "%s%s: minflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld",
         nstr, tag, end - start, uend - ustart, send - sstart,
        nstr, tag, nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw);
    puts(msg);
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

/**
 * mpi_localcfg: get local-node mpi config (for working around bmi+tcp issues)
 *
 * assumes MPI has been init'd.  this is a collective MPI call.
 *
 * @param world our top-level comm
 * @param lrnk local rank will be placed here
 * @param lsz local size will be placed here
 * @return 0 or -1 on error
 */
int mpi_localcfg(MPI_Comm world, int *lrnk, int *lsz) {
    MPI_Comm local;
    int ok;

    /* split the world into local and remote */
    if (MPI_Comm_split_type(world, MPI_COMM_TYPE_SHARED, 0,
                            MPI_INFO_NULL, &local) != MPI_SUCCESS)
    return(-1);

    ok = MPI_Comm_rank(local, lrnk) == MPI_SUCCESS &&
         MPI_Comm_size(local, lsz) == MPI_SUCCESS;

    MPI_Comm_free(&local);    /* ignore errors */
    return(ok ? 0 : -1);
}

/*
 * mercury_gen_ipurl: generate a mercury IP url using subnet spec
 * to select the network to use.  if subnet is NULL or empty, we
 * use the first non-127.0.0.1 IP address.
 *
 * @param protocol mercury protocol (e.g. "bmi+tcp")
 * @param subnet IP subnet to use (e.g. "10.92")
 * @param port port number to use, zero means any
 * @param wa_base work-around base port (for bmi+tcp workaround)
 * @param wa_stride work-around port stride (for bmi+tcp workaround)
 * @return a malloc'd buffer with the new URL, or NULL on error
 */
char *mercury_gen_ipurl(char *protocol, const char *subnet, int port,
                        int wa_base, int wa_stride) {
    int snetlen, rlen, so, n, lcv;
    struct ifaddrs *ifaddr, *cur;
    char tmpip[16];   /* strlen("111.222.333.444") == 15 */
    char *ret;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    /* query socket layer to get our IP address list */
    if (getifaddrs(&ifaddr) == -1) {
        fprintf(stderr, "mercury_gen_ipurl: getifaddrs failed?\n");
        return(NULL);
    }

    snetlen = (subnet) ? strlen(subnet) : 0;

    /* walk list looking for match */
    for (cur = ifaddr ; cur != NULL ; cur = cur->ifa_next) {

        /* skip interfaces without an IP address */
        if (cur->ifa_addr == NULL || cur->ifa_addr->sa_family != AF_INET)
            continue;

        /* get full IP address */
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                        tmpip, sizeof(tmpip), NULL, 0, NI_NUMERICHOST) == -1)
            continue;

        if (snetlen == 0) {
          if (strcmp(tmpip, "127.0.0.1") == 0)
            continue; /* skip localhost */
          break;      /* take first non-localhost match */
        }
        if (strncmp(subnet, tmpip, snetlen) == 0)
          break;
    }

    /* dump the ifaddr list and return if there was no match */
    freeifaddrs(ifaddr);
    if (cur == NULL)
        return(NULL);

    rlen = strlen(protocol) + 32; /* +32 enough for ip, port, etc. */
    ret = (char *)malloc(rlen);
    if (ret == NULL)
      return(NULL);

    if (port != 0 || strcmp(protocol, "bmi+tcp") != 0) {
        /* set port 0, let OS fill it, collect later w/HG_Addr_to_string */
        snprintf(ret, rlen, "%s://%s:%d", protocol, tmpip, port);
        return(ret);
    }

    /*
     * XXX: bmi+tcp HG_Addr_to_string() is broken.  if we request
     * port 0 (to let the OS fill it in) and later use HG_Addr_to_string()
     * to request the actual port number allocated, it still returns
     * 0 as the port number...  here's an attempt to hack around this
     * problem.  we take wa_base and wa_stride as hints on how to pick
     * a port number so that it doesn't conflict with other local ports.
     * e.g. wa_base= X+my_local_rank, wa_stride=#local_ranks
     */
    if (wa_base < 1) wa_base = 10000;
    if (wa_stride < 1) wa_stride = 1;
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so < 0) {
        perror("socket");
        free(ret);
        return(NULL);
    }
    n = 1;
    setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
    for (lcv = 0 ; lcv < 1024 ; lcv++) {   /* try up to 1024 times */
        port = wa_base + (lcv * wa_stride);
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        n = bind(so, (struct sockaddr*)&addr, addr_len);
        if (n == 0) break;
    }
    close(so);

    if (n != 0) {
        perror("bind");
        free(ret);
        return(NULL);
    }

    snprintf(ret, rlen, "%s://%s:%d", protocol, tmpip, port);
    return(ret);
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
#define DEF_DELIVERQTHR 0  /* delivery thread wakeup threshold */
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
    const char *hgsubnet;    /* subnet to use (XXX: assumes IP) */
    int baseport;            /* base port number */
    int buftarg_net;         /* batch target for network queues */
    int buftarg_origin;      /* batch target for origin/client local shm q's */
    int buftarg_relay;       /* batch target for relayed local shm q's */
    int count;               /* number of msgs to send/recv in a run */
    int excludeself;         /* exclude sending to self (skip those sends) */
    int flushrate;           /* do extra flushes while sending */
    int deliverq_max;        /* max# reqs in deliverq before waitq */
    int deliverq_thold;      /* delivery thread wakeup threshold */
    int loop;                /* loop through dsts rather than random sends */
    char *nxdumpspec;        /* file spec to nexus dump to */
    int minsndr;             /* rank must be >= minsndr to send requests */
    int odelay;              /* delay delivery output this many msec */
    struct timespec odspec;  /* odelay in a timespec for nanosleep(3) */
    int maxrpcs_net;         /* max # outstanding RPCs, network */
    int maxrpcs_origin;      /* max # outstanding RPCs, origin/cli shm q's */
    int maxrpcs_relay;       /* max # outstanding RPCs, relayed shm q's */
    int remoterpclim;        /* remote RPC limit on shuffler_send */
    int localrpclim;         /* local RPC limit on shuffler_send */
    int quiet;               /* don't print so much */
    int rflag;               /* -r tag suffix spec'd */
    int rflagval;            /* value for -r */
    int rcvr_only;           /* only send to this rank (if >0) */
    int maxsndr;             /* rank must be <= maxsndr to send requests */
    int timestats;           /* report extra time/usage stats for instance */
    int timeout;             /* alarm timeout */
    int xsinglehg;           /* use single hg mode */

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
        /* only force to stderr if nprocs <= 4 */
        shuffler_statedump(isa[lcv].shand, (g.size <= 4) ? 1 : 0);
    }
    fprintf(stderr, "Alarm clock\n");
    MPI_Finalize();
    exit(1);
}

/*
 * sigusr1 signal handler
 */
void sigusr1(int foo) {
    int lcv;
    fprintf(stderr, "SIGUSR1 detected (%d)\n", myrank);
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        fprintf(stderr, "%d: %d: @usr1: ", myrank, lcv);
        fprintf(stderr, "nsends=%d, ncallbacks=%d\n",
                isa[lcv].nsends, isa[lcv].ncallbacks);
        /* only force to stderr if nprocs <= 4 */
        shuffler_statedump(isa[lcv].shand, (g.size <= 4) ? 1 : 0);
    }
}

/*
 * usage
 */
static void usage(const char *msg) {

    /* only have rank 0 print usage error message */
    if (myrank) goto skip_prints;

    if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
    fprintf(stderr, "usage: %s [options] mercury-protocol [subnet]\n", argv0);
    fprintf(stderr, "\noptions:\n");
    fprintf(stderr, "\t-c count    number of shuffle send ops to perform\n");
    fprintf(stderr, "\t-e          exclude sending to self (skip sends)\n");
    fprintf(stderr, "\t-f rate     do a flush every 'rate' sends\n");
    fprintf(stderr, "\t-l          loop through dsts (no random sends)\n");
    fprintf(stderr, "\t-N filespec nexus dump to this filespec\n");
    fprintf(stderr, "\t-n minsndr  rank must be >= minsndr to send requests\n");
    fprintf(stderr, "\t-o m        add 'm' msec output delay to delivery\n");
    fprintf(stderr, "\t-p port     base port number\n");
    fprintf(stderr, "\t-q          quiet mode\n");
    fprintf(stderr, "\t-R rank     only do sends to this rank\n");
    fprintf(stderr, "\t-r n        enable tag suffix with this run number\n");
    fprintf(stderr, "\t-s maxsndr  rank must be <= maxsndr to send requests\n");
    fprintf(stderr, "\t-T          extra time/usage stats for instance\n");
    fprintf(stderr, "\t-t sec      timeout (alarm), in seconds\n");
    fprintf(stderr, "\t-x          use network hg prog for local reqs\n");

    fprintf(stderr, "shuffler queue config:\n");
    fprintf(stderr, "\t-B bytes    batch buf target for network\n");
    fprintf(stderr, "\t-a bytes    batch buf target for client/origin shm\n");
    fprintf(stderr, "\t-b bytes    batch buf target for relayed shm\n");
    fprintf(stderr, "\t-d count    delivery queue size limit\n");
    fprintf(stderr, "\t-h count    delivery thread wakeup threshold\n");
    fprintf(stderr, "\t-M count    maxrpcs for network output queues\n");
    fprintf(stderr, "\t-m count    maxrpcs for shm client/origin queues\n");
    fprintf(stderr, "\t-y count    maxrpcs for shm relayed queues\n");
    fprintf(stderr, "\t-Z count    remote RPC limit on shuffler_send\n");
    fprintf(stderr, "\t-z count    local RPC limit on shuffler_send\n");
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
static void do_delivery(int src, int dst, uint32_t type,
    void *d, uint32_t datalen);
static void do_flush(shuffler_t sh, int verbo);

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
    g.buftarg_origin = DEF_BUFTARGET;
    g.buftarg_relay = DEF_BUFTARGET;
    g.count = DEF_COUNT;
    g.deliverq_max = DEF_DELIVERQMAX;
    g.deliverq_thold = DEF_DELIVERQTHR;
    g.maxrpcs_net = DEF_MAXRPCS;
    g.maxrpcs_origin = DEF_MAXRPCS;
    g.maxrpcs_relay = DEF_MAXRPCS;
    g.rcvr_only = -1;            /* disable by default */
    g.minsndr = 0;
    g.maxsndr = g.size - 1;      /* everyone sends by default */
    g.timeout = DEF_TIMEOUT;

    g.defpri = "WARN";
    g.serrpri = "CRIT";
    g.max_xtra = g.size;

    while ((ch = getopt(argc, argv,
  "a:B:b:C:c:D:d:E:eF:f:h:I:i:LlM:m:N:n:O:o:p:qR:r:S:s:Tt:X:xy:Z:z:")) != -1) {
        switch (ch) {
            case 'a':
                g.buftarg_origin = atoi(optarg);
                if (g.buftarg_origin < 1) usage("bad buftarget origin");
                break;
            case 'B':
                g.buftarg_net = atoi(optarg);
                if (g.buftarg_net < 1) usage("bad buftarget net");
                break;
            case 'b':
                g.buftarg_relay = atoi(optarg);
                if (g.buftarg_relay < 1) usage("bad buftarget relay");
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
                if (g.deliverq_max == 0) usage("bad deliverq_max shm");
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
            case 'f':
                g.flushrate = atoi(optarg);
                if (g.flushrate < 0) usage("bad flush rate");
                break;
            case 'h':
                g.deliverq_thold = atoi(optarg);
                if (g.deliverq_thold < 0) usage("bad deliver threshold");
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
                g.maxrpcs_origin = atoi(optarg);
                if (g.maxrpcs_origin < 1) usage("bad maxrpc origin");
                break;
            case 'N':
                g.nxdumpspec = optarg;
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
            case 'T':
                g.timestats = 1;
                break;
            case 't':
                g.timeout = atoi(optarg);
                if (g.timeout < 0) usage("bad timeout");
                break;
            case 'X':
                g.max_xtra = atoi(optarg);
                break;
            case 'x':
                g.xsinglehg = 1;
                break;
            case 'y':
                g.maxrpcs_relay = atoi(optarg);
                if (g.maxrpcs_relay < 1) usage("bad maxrpc relay");
                break;
            case 'Z':
                g.remoterpclim = atoi(optarg);
                if (g.remoterpclim < 0) usage("bad remote rpc limit");
                break;
            case 'z':
                g.localrpclim = atoi(optarg);
                if (g.localrpclim < 0) usage("bad local rpc limit");
                break;
            default:
                usage(NULL);
        }
    }
    argc -= optind;
    argv += optind;

    if (argc < 1 || argc > 2)
      usage("bad args");
    g.ninst = 1;
    g.hgproto = argv[0];
    g.hgsubnet = (argc == 1) ? "" : argv[1];
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
        if (g.flushrate)
            printf("\tflushrate  = %d\n", g.flushrate);
        printf("\tloop       = %d\n", g.loop);
        if (g.nxdumpspec)
            printf("\tnxdump     = %s\n", g.nxdumpspec);
        printf("\tquiet      = %d\n", g.quiet);
        if (g.rflag)
            printf("\tsuffix     = %s\n", g.tagsuffix);
        if (g.rcvr_only >= 0)
            printf("\trcvr_only  = %d\n", g.rcvr_only);
        printf("\tminsndr    = %d\n", g.minsndr);
        printf("\tmaxsndr    = %d\n", g.maxsndr);
        printf("\ttimestats  = %s\n", (g.timestats) ? "on" : "off");
        printf("\ttimeout    = %d\n", g.timeout);
        printf("\tsinglehg   = %d\n", g.xsinglehg);
        printf("sizes:\n");
        printf("\tbuftarget  = %d / %d / %d (net/origin/relay)\n",
               g.buftarg_net, g.buftarg_origin, g.buftarg_relay);
        printf("\tmaxrpcs    = %d / %d / %d (net/origin/relay)\n",
               g.maxrpcs_net, g.maxrpcs_origin, g.maxrpcs_relay);
        printf("\trpclimits  = %d / %d (local/remote)\n",
               g.localrpclim, g.remoterpclim);
        printf("\tdeliverqmx = %d\n", g.deliverq_max);
        printf("\tdeliverthd = %d\n", g.deliverq_thold);
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
    signal(SIGUSR1, sigusr1);
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
    struct useprobe instuse;
    int flcnt, lcv, sendto, mylen, nbuflen;
    hg_return_t ret;
    uint32_t *msg, msg_store[3];
    char *myurl, *nbuf;
    hg_class_t *cls;
    hg_context_t *ctx;
    progressor_handle_t *phand;

    useprobe_start(&instuse, USEPROBE_THREAD);
    if (!g.quiet)
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

    if (strcmp(g.hgproto, "bmi+tcp") == 0) {  /* bmi+tcp hack required? */
        int lr, ls;
        if (mpi_localcfg(MPI_COMM_WORLD, &lr, &ls) < 0) /* HACK! */
            complain(1, 0, "mpi_localcfg failed?");
        myurl = mercury_gen_ipurl(g.hgproto, g.hgsubnet, 0, 10000+lr, ls);
        if (!myurl)
            complain(1, 0, "mercury_gen_ipurl failed?");
    } else if (strlen(g.hgsubnet)) {
        myurl = mercury_gen_ipurl(g.hgproto, g.hgsubnet, 0, 0, 0);
    } else {
        myurl = g.hgproto;
    }
    cls = HG_Init(myurl, HG_TRUE);
    if (!cls)
        complain(1, 0, "HG_Init(%s, TRUE) failed", myurl);
    if (myurl != g.hgproto)
        free(myurl);
    myurl = NULL;
    ctx = HG_Context_create(cls);
    if (!ctx)
        complain(1, 0, "HG_Context_create failed!");
    phand = mercury_progressor_init(cls, ctx);
    if (!phand)
        complain(1, 0, "mercury_progressor_init failed!");

    if (g.xsinglehg)
        isa[n].nxp = nexus_bootstrap(phand, phand);  /* use phand for local */
    else
        isa[n].nxp = nexus_bootstrap(phand, NULL);
    if (!isa[n].nxp)
        complain(1, 0, "%d: nexus_bootstrap failed", myrank);
    if (!g.quiet)
        printf("%d: nexus powered up!\n", myrank);

    if (g.nxdumpspec) {
        nbuflen = strlen(g.nxdumpspec) + 32;  /* extra room for instance# */
        nbuf = (char *)malloc(nbuflen);
        if (!nbuf)
            complain(1, 0, "%d: nexus_dump buf alloc failed", myrank);
        snprintf(nbuf, nbuflen, "%s.%d", g.nxdumpspec, n);
        nexus_dump(isa[n].nxp, nbuf);
        free(nbuf);
    }

    /* make a funcion name and register it in both HGs */
    snprintf(isa[n].myfun, sizeof(isa[n].myfun), "f%d", n);

    isa[n].shand = shuffler_init(isa[n].nxp, isa[n].myfun, g.localrpclim,
                   g.remoterpclim, g.maxrpcs_origin,
                   g.buftarg_origin, g.maxrpcs_relay, g.buftarg_relay,
                   g.maxrpcs_net, g.buftarg_net, g.deliverq_max,
                   g.deliverq_thold, do_delivery);
    flcnt = 0;

    /* make sure that all ranks are ready to recv before we start sending */
    MPI_Barrier(MPI_COMM_WORLD);

    if (myrank >= g.minsndr && myrank <= g.maxsndr) {   /* are we a sender? */
        for (lcv = 0 ; lcv < g.count ; lcv++) {

            /* flush if requested */
            if (lcv && g.flushrate && (lcv % g.flushrate) == 0) {
                flcnt++;
                do_flush(isa[n].shand, 0);
            }

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
            isa[n].nsends++;
        }

    } else if (g.flushrate) {

        /* need to do collective flush even if we are not a sender */
        for (lcv = 0 ; lcv < g.count ; lcv++) {

            if (lcv && (lcv % g.flushrate) == 0) {
                flcnt++;
                do_flush(isa[n].shand, 0);
            }
        }

    }

    /* done sending */
    printf("%d: sends complete (nsends=%d,flcnt=%d)!\n", myrank,
           isa[n].nsends, flcnt);
    if (g.timestats) {
        useprobe_end(&instuse);
        useprobe_print(stdout, &instuse, "instance-prebar", myrank);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (g.timestats) {
        useprobe_end(&instuse);
        useprobe_print(stdout, &instuse, "instance-postbar", myrank);
    }
    if (myrank == 0)
        printf("%d: crossed send barrier.\n", myrank);

    /* flush it now */
    do_flush(isa[n].shand, 1);

    ret = shuffler_shutdown(isa[n].shand);
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush shutdown failed(%d)\n", ret);
    printf("%d: shuf shutdown.\n", myrank);

    nexus_destroy(isa[n].nxp);
    if (msg != msg_store) free(msg);

    mercury_progressor_freehandle(phand); /* ignore errors */

    useprobe_end(&instuse);
    if (g.quiet == 0 || g.size <= 4) {
        useprobe_print(stdout, &instuse, "instance", myrank);
    }

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
static void do_delivery(int src, int dst, uint32_t type,
    void *d, uint32_t datalen) {
    uint32_t msg[3];
    struct timespec rem;

    isa[0].ncallbacks++;          /* assume only 1 instance */
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

/*
 * do_flush: do a full shuffler flush (collective call)
 *
 * @param sh shuffler to flush
 * @param verbo have rank 0 print flush info
 */
static void do_flush(shuffler_t sh, int verbo) {
    hg_return_t ret;

    ret = shuffler_flush_originqs(sh);  /* clear out SRC->SRCREP */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush local failed(%d)\n", ret);
    MPI_Barrier(MPI_COMM_WORLD);
    if (verbo && myrank == 0)
        printf("%d: flushed local (hop1).\n", myrank);

    ret = shuffler_flush_remoteqs(sh); /* clear SRCREP->DSTREP */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush remote failed(%d)\n", ret);
    MPI_Barrier(MPI_COMM_WORLD);
    if (verbo && myrank == 0)
        printf("%d: flushed remote (hop2).\n", myrank);

    ret = shuffler_flush_relayqs(sh);  /* clear DSTREP->DST */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush local2 failed(%d)\n", ret);
    MPI_Barrier(MPI_COMM_WORLD);
    if (verbo && myrank == 0)
        printf("%d: flushed local (hop3).\n", myrank);

    ret = shuffler_flush_delivery(sh); /* clear deliverq */
    if (ret != HG_SUCCESS)
            fprintf(stderr, "shuffler_flush delivery failed(%d)\n", ret);
    if (verbo && myrank == 0)
        printf("%d: flushed delivery.\n", myrank);

}
