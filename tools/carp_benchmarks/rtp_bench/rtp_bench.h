//
// Created by Ankush J on 7/27/22.
//

#pragma once

#include <pdlfs-common/status.h>
#include <stdlib.h>

#include "carp/carp_preload.h"
#include "preload_internal.h"

namespace pdlfs {
namespace carp {

uint64_t sumsq(const uint64_t& total, const uint64_t& v);

struct RTPBenchOpts {
  int nrounds;  // number of RTP rounds to run
  int nwarmup;  // number of untimed rounds to warm up with
  int pvtcnt;   // CARP pvt count
};

class RTPBench {
 public:
  RTPBench(RTPBenchOpts& opts) : opts_(opts), my_rank_(-1) {}
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
    const char* stripped = pctx.plfsdir;

    shuffle_init(&pctx.sctx);
    preload_mpiinit_carpopts(&pctx, pctx.opts, stripped);
    pctx.carp = new pdlfs::carp::Carp(*pctx.opts);
  }

  void RunCarp() {
    if (my_rank_ == 0) {
      for (size_t n = 0; n < opts_.nwarmup; n++) {
        TriggerRound(/* untimed */ true);
      }

      for (size_t n = 0; n < opts_.nrounds; n++) {
        TriggerRound();
      }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    /* give all ranks the chance to obtain messages */
    sleep(1);
  }

  void TriggerRound(bool untimed = false) {
    uint64_t rbeg_us = Now();
    pctx.carp->ForceRenegotiation();
    uint64_t rend_us = Now();

    if (!untimed) {
      LogRound(rbeg_us, rend_us);
    }
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
    all_rtimes_.push_back(rend_us - rbeg_us);
  }

  void PrintStats() {
    size_t rlatcnt = all_rtimes_.size();

    uint64_t const& (*min)(uint64_t const&, uint64_t const&) =
        std::min<uint64_t>;
    uint64_t const& (*max)(uint64_t const&, uint64_t const&) =
        std::max<uint64_t>;

    uint64_t rlatsum =
        std::accumulate(all_rtimes_.begin(), all_rtimes_.end(), 0ull);
    uint64_t rlatsqsum =
        std::accumulate(all_rtimes_.begin(), all_rtimes_.end(), 0ull, sumsq);

    double rlatmean = rlatsum / rlatcnt;
    double rlatmeansq = rlatmean * rlatmean;
    double rlatsqmean = rlatsqsum / rlatcnt;
    double rlatvar = rlatsqmean - rlatmeansq;
    double rlatstd = pow(rlatvar, 0.5);

    uint64_t rlatmin =
        std::accumulate(all_rtimes_.begin(), all_rtimes_.end(), UINT_MAX, min);
    uint64_t rlatmax =
        std::accumulate(all_rtimes_.begin(), all_rtimes_.end(), 0ull, max);

    logf(LOG_INFO,
         "Rounds: %zu, Latency: %.1lfus+-%.1lfus (Range: %" PRIu64 "us-%" PRIu64
         "us)\n",
         rlatcnt, rlatmean, rlatstd, rlatmin, rlatmax);
  }

  void DestroyCarp() {
    preload_finalize_carp(&pctx);
    shuffle_finalize(&pctx.sctx);

    if (pctx.recv_comm != MPI_COMM_NULL && pctx.recv_comm != MPI_COMM_WORLD) {
      MPI_Comm_free(&pctx.recv_comm);
    }
  }

  const RTPBenchOpts& opts_;
  int my_rank_;
  std::vector<uint64_t> all_rtimes_;
};
}  // namespace carp
}  // namespace pdlfs
