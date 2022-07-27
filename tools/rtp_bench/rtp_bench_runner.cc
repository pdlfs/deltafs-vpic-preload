#include <stdio.h>

#include "rtp_bench.h"

/* Requires the following environment variables to be set:
 * SHUFFLE_Mercury_proto
 * SHUFFLE_Subnet
 * NEXUS_ALT_LOCAL
 * NEXUS_BYPASS_LOCAL
 */

int main(int argc, char* argv[]) {
  printf("hello world");
  MPI_Init(&argc, &argv);
  pdlfs::carp::RTPBench rtp_bench;
  rtp_bench.Run();
  return 0;
}