//
// Created by Ankush J on 10/24/22.
//

#include <stdio.h>

#include "pivot_bench.h"

static char* argv0;
pdlfs::PivotBenchOpts opts;

void usage() {
  fprintf(stderr, "usage: %s [-w warmup_rounds] [-n num_rounds] [-p pvtcnt]\n",
          argv0);
  exit(1);
}

void set_default_opts() {
  opts.env = pdlfs::Env::Default();
  opts.nranks = 8;
  opts.pvtcnt = 16;
  opts.oobsz = 256;
  opts.nthreads = 32;
  opts.trace_root =
      "/Users/schwifty/Repos/workloads/data/particle.compressed.uniform.mini";
  opts.log_file = "tmp.txt";

//  opts.trace_root = "/mnt/lustre/carp-big-run/particle.compressed.uniform.mini";
}

void parse_opts(int argc, char* argv[]) {
  int ch;

  while ((ch = getopt(argc, argv, "l:n:p:o:t:")) != -1) {
    switch (ch) {
      case 'l':
        opts.log_file = optarg;
        break;
      case 'n':
        opts.nranks = atoi(optarg);
        break;
      case 'p':
        opts.pvtcnt = atoi(optarg);
        break;
      case 'o':
        opts.oobsz = atoi(optarg);
      case 't':
        opts.trace_root = optarg;
        break;
      default:
        usage();
        break;
    }
  }
}

void print_opts() {
  flog(LOG_INFO, "[NumRanks] %d", opts.nranks);
  flog(LOG_INFO, "[PivotCount] %d", opts.pvtcnt);
  flog(LOG_INFO, "[OOBSize] %d", opts.oobsz);
  flog(LOG_INFO, "[TraceRoot] %s", opts.trace_root);
  flog(LOG_INFO, "[LogFile] %s", opts.log_file);
}

int main(int argc, char* argv[]) {
  setlinebuf(stdout);
  argv0 = argv[0];

  set_default_opts();
  parse_opts(argc, argv);
  print_opts();

  pdlfs::PivotBench pvt_bench(opts);
  pvt_bench.Run();

  return 0;
}
