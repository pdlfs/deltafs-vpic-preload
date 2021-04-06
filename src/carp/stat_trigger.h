//
// Created by Ankush J on 4/2/21.
//

#pragma once

#include <common.h>
#include <inttypes.h>
#include <pdlfs-common/env.h>

#include <numeric>
#include <string>
#include <vector>

namespace pdlfs {
namespace carp {
class CarpOptions;
class StatTriggerUtils {
 public:
  static std::string RankToFname(int rank) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "RDB-%08x.tbl", rank);
    return tmp;
  }

  static int FnameToRank(const std::string& fname) {
    int rank;
    sscanf(fname.c_str(), "RDB-%08x.tbl", &rank);
    return rank;
  }

  static std::vector<std::string> RankPaths(const std::string& base_dir,
                                            int nranks) {
    std::vector<std::string> fpaths;
    for (int rank = 0; rank < nranks; rank++) {
      fpaths.push_back(base_dir + "/" + RankToFname(rank));
    }

    return fpaths;
  }
};
class StatTrigger {
 public:
  StatTrigger(const CarpOptions& options);

  bool Invoke() {
    bool ret = false;
    if (my_rank_ != 0) return ret;

    invoke_counter_++;
    if (invoke_counter_ == invoke_intvl_) {
      ret = InvokeInternal();
      invoke_counter_ = 0;
    }

    return ret;
  }

 private:
  bool InvokeInternal() {
    Status s = Status::OK();
    s = StatFiles();
    if (!s.ok()) return false;
    return EvalTrigger();
  }

  Status StatFiles() {
    Status s = Status::OK();

    if (rank_paths_.empty()) {
      rank_paths_ = StatTriggerUtils::RankPaths(base_dir_, nranks_);
    }

    assert(rank_paths_.size() == (size_t)nranks_);

    for (int rank = 0; rank < rank_paths_.size(); rank++) {
      const std::string& rpath = rank_paths_[rank];
      uint64_t rsize;
      s = env_->GetFileSize(rpath.c_str(), &rsize);
      if (!s.ok()) return s;

      fprintf(stderr, "%s: %" PRIu64 "\n", rpath.c_str(), rsize);

      rsize_diff_[rank] = rsize - prev_rsize_[rank];
      prev_rsize_[rank] = rsize;
    }

    return s;
  }

  bool EvalTrigger() {
    uint64_t load_sum =
        std::accumulate(rsize_diff_.begin(), rsize_diff_.end(), 0);
    uint64_t load_avg = load_sum / nranks_;
    uint64_t load_max =
        *std::max_element(rsize_diff_.begin(), rsize_diff_.end());
    float load_skew = load_max * 1.0f / load_avg;

    if (load_skew > thresh_) {
      logf(LOG_DBUG,
           "[StatTrigger] TRIGGER! Max: %" PRIu64 ", Avg: %" PRIu64
           ", Skew: %.3f\n",
           load_max, load_avg, load_skew);
    } else {
      logf(LOG_DBUG,
           "[StatTrigger] Max: %" PRIu64 ", Avg: %" PRIu64
           ", Skew: %.3f\n",
           load_max, load_avg, load_skew);
    }

    return load_skew > thresh_;
  }

  Env* env_;
  const int my_rank_;
  const int nranks_;
  const std::string base_dir_;
  const uint32_t invoke_intvl_;
  uint32_t invoke_counter_;

  const float thresh_;

  std::vector<std::string> rank_paths_;
  std::vector<uint64_t> prev_rsize_;
  std::vector<uint64_t> rsize_diff_;
};
}  // namespace carp
}  // namespace pdlfs
