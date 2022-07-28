//
// Created by Ankush J on 7/27/22.
//

#pragma once

#include <pdlfs-common/status.h>
#include <stdlib.h>

#include "preload_internal.h"

namespace pdlfs {
namespace carp {
class RTPBench {
 public:
  RTPBench() {}
  Status Run() {
    InitParams();
    InitCarp();
//    RunCarp();
    DestroyCarp();
    return Status::OK();
  }

 private:
  void InitParams();

  void InitCarp() {
    shuffle_init(&pctx.sctx);
    pctx.carp = new pdlfs::carp::Carp(*pctx.opts);
  }

  void RunCarp() {
    pctx.carp->ForceRenegotiation();
    sleep(20);
  }

  void DestroyCarp() {
    delete pctx.carp;
    pctx.carp = nullptr;
    delete pctx.opts;
    pctx.opts = nullptr;
    shuffle_finalize(&pctx.sctx);

    if (pctx.recv_comm != MPI_COMM_NULL && pctx.recv_comm != MPI_COMM_WORLD) {
      MPI_Comm_free(&pctx.recv_comm);
    }
  }
};
}  // namespace carp
}  // namespace pdlfs