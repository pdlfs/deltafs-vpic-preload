#include <stdio.h>

#include "rtp_bench.h"

pdlfs::carp::RTPBenchOpts opts;

/* Requires the following environment variables to be set:
 * SHUFFLE_Mercury_proto
 * SHUFFLE_Subnet
 * NEXUS_ALT_LOCAL
 * NEXUS_BYPASS_LOCAL
 */

void ParseOpts(int argc, char* argv[]) {
  int ch;

  while ((ch = getopt(argc, argv, "n:")) != -1) {
    switch (ch) {
      case 'n':
        opts.nrounds = atoi(optarg);
        break;
      default:
        break;
    }
  }
}

int main(int argc, char* argv[]) {
  opts.nrounds = 1;
  ParseOpts(argc, argv);
  MPI_Init(&argc, &argv);
  pdlfs::carp::RTPBench rtp_bench(opts);
  rtp_bench.Run();
  MPI_Finalize();
  return 0;
}
