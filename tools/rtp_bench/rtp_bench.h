//
// Created by Ankush J on 7/27/22.
//

#pragma once

#include <pdlfs-common/status.h>
#include <stdlib.h>

#include "preload_internal.h"

namespace pdlfs {
namespace carp {

uint64_t sumsq(const uint64_t& total, const uint64_t& v);

struct RTPBenchOpts {
  int nrounds;
};

class RTPBench {
 public:
  RTPBench(RTPBenchOpts& opts) : my_rank_(-1), nrounds_(opts.nrounds) {}
  Status Run() {
    InitParams();
    MPI_Barrier(MPI_COMM_WORLD);
    InitCarp();
    MPI_Barrier(MPI_COMM_WORLD);
    RunCarp();
    MPI_Barrier(MPI_COMM_WORLD);
    DestroyCarp();
    MPI_Barrier(MPI_COMM_WORLD);

    if (my_rank_ == 0) {
      PrintStats();
    }

    return Status::OK();
  }

 private:
  void InitParams();

  void InitCarp() {
    int rv = perfstats_init(&(pctx.perf_ctx), pctx.my_rank, pctx.log_home,
                            pctx.local_root);
    if (rv) {
      ABORT("perfstats_init");
    }
    shuffle_init(&pctx.sctx);
    pctx.carp = new pdlfs::carp::Carp(*pctx.opts);
  }

  void RunCarp() {
    if (my_rank_ == 0) {
      for (size_t n = 0; n < nrounds_; n++) {
        TriggerRound();
      }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    /* give all ranks the chance to obtain messages */
    sleep(1);
  }

  void TriggerRound() {
    uint64_t rbeg_us = Now();
    pctx.carp->ForceRenegotiation();
    uint64_t rend_us = Now();
    LogRound(rbeg_us, rend_us);
  }

  static uint64_t Now() {
    struct timespec tv;
    clock_gettime(CLOCK_MONOTONIC, &tv);

    uint64_t t;
    t = static_cast<uint64_t>(tv.tv_sec) * 1000000;
    t += tv.tv_nsec / 1000;
    return t;
  }

  void LogRound(uint64_t rbeg_us, uint64_t rend_us) {
    all_rounds_.push_back(rend_us - rbeg_us);
  }

  void PrintStats() {
    size_t rlatcnt = all_rounds_.size();

    uint64_t const& (*min)(uint64_t const&, uint64_t const&) =
        std::min<uint64_t>;
    uint64_t const& (*max)(uint64_t const&, uint64_t const&) =
        std::max<uint64_t>;

    uint64_t rlatsum =
        std::accumulate(all_rounds_.begin(), all_rounds_.end(), 0ull);
    uint64_t rlatsqsum =
        std::accumulate(all_rounds_.begin(), all_rounds_.end(), 0ull, sumsq);

    double rlatmean = rlatsum / rlatcnt;
    double rlatmeansq = rlatmean * rlatmean;
    double rlatsqmean = rlatsqsum / rlatcnt;
    double rlatvar = rlatsqmean - rlatmeansq;
    double rlatstd = pow(rlatvar, 0.5);

    uint64_t rlatmin =
        std::accumulate(all_rounds_.begin(), all_rounds_.end(), UINT_MAX, min);
    uint64_t rlatmax =
        std::accumulate(all_rounds_.begin(), all_rounds_.end(), 0ull, max);

    logf(LOG_INFO,
         "Rounds: %zu, Latency: %.1lfus+-%.1lfus (Range: %" PRIu64 "us-%" PRIu64
         "us)\n",
         rlatcnt, rlatmean, rlatstd, rlatmin, rlatmax);
  }

  void DestroyCarp() {
    delete pctx.carp;
    pctx.carp = nullptr;
    delete pctx.opts;
    pctx.opts = nullptr;
    shuffle_finalize(&pctx.sctx);

    int rv = perfstats_destroy(&(pctx.perf_ctx));
    if (rv) {
      ABORT("perfstats_destroy");
    }

    if (pctx.recv_comm != MPI_COMM_NULL && pctx.recv_comm != MPI_COMM_WORLD) {
      MPI_Comm_free(&pctx.recv_comm);
    }
  }

  int my_rank_;
  int nrounds_;
  std::vector<uint64_t> all_rounds_;
};
}  // namespace carp
}  // namespace pdlfs
