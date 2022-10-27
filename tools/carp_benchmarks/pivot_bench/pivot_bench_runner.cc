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
  opts.nranks = 512;
  opts.pvtcnt = 512;
  opts.trace_root =
      "/Users/schwifty/Repos/workloads/data/particle.compressed.uniform.mini";
}

void parse_opts(int argc, char* argv[]) {
  int ch;

  while ((ch = getopt(argc, argv, "n:p:t:")) != -1) {
    switch (ch) {
      case 'n':
        opts.nranks = atoi(optarg);
        break;
      case 'p':
        opts.pvtcnt = atoi(optarg);
        break;
      case 't':
        opts.trace_root = optarg;
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
//  parse_opts(argc, argv);

  pdlfs::PivotBench pvt_bench(opts);
  pvt_bench.Run();

  return 0;
}
