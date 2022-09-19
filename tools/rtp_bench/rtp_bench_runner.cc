#include <stdio.h>

#include "rtp_bench.h"

static char* argv0;
static pdlfs::carp::RTPBenchOpts opts;

/* Requires the following environment variables to be set:
 * SHUFFLE_Mercury_proto
 * SHUFFLE_Subnet
 * NEXUS_ALT_LOCAL
 * NEXUS_BYPASS_LOCAL
 */

void set_default_opts() {
  opts.nrounds = 1;
  opts.nwarmup = 0;
  opts.pvtcnt = 256;
}

void usage() {
  fprintf(stderr, "usage: %s [-w warmup_rounds] [-n num_rounds] [-p pvtcnt]\n",
          argv0);
  exit(1);
}

void parse_opts(int argc, char* argv[]) {
  int ch;

  while ((ch = getopt(argc, argv, "n:w:p:")) != -1) {
    switch (ch) {
      case 'n':
        opts.nrounds = atoi(optarg);
        break;
      case 'w':
        opts.nwarmup = atoi(optarg);
        break;
      case 'p':
        opts.pvtcnt = atoi(optarg);
        break;
      default:
        usage();
        break;
    }
  }
}

int main(int argc, char* argv[]) {
  argv0 = argv[0];

  set_default_opts();
  parse_opts(argc, argv);

  MPI_Init(&argc, &argv);

  pdlfs::carp::RTPBench rtp_bench(opts);
  rtp_bench.Run();

  MPI_Finalize();
  return 0;
}
