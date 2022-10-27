//
// Created by Ankush J on 10/24/22.
//

#pragma once

namespace pdlfs {
struct PivotBenchOpts {
  Env* env;
  int nranks;
  const char* trace_root;
  int pvtcnt;
  int oobsz;
};
}  // namespace pdlfs