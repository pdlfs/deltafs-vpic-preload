//
// Created by Ankush J on 10/24/22.
//

#pragma once

namespace pdlfs {
struct PivotBenchOpts {
  Env* env;
  unsigned int nranks;
  int pvtcnt;
  int oobsz;
  int nthreads;
  const char* trace_root;
  const char* log_file;
};
}  // namespace pdlfs
