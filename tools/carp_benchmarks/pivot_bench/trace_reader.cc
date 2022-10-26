//
// Created by Ankush J on 10/28/22.
//

#include "trace_reader.h"

namespace pdlfs {
TraceReader::TraceReader(const PivotBenchOpts& opts)
    : env_(opts.env), trace_root_(opts.trace_root), nranks_(opts.nranks) {}

Status TraceReader::DiscoverEpochs(size_t& num_ep) {
  Status s = Status::OK();

  std::vector<std::string> trace_subdirs;
  s = env_->GetChildren(trace_root_.c_str(), &trace_subdirs);
  if (!s.ok()) {
    flog(LOG_ERRO, "[TraceReader] DiscoverEpochs failed: %s",
         s.ToString().c_str());
    return s;
  }

  for (size_t dir_idx = 0; dir_idx < trace_subdirs.size(); dir_idx++) {
    std::string cur_dir = trace_subdirs[dir_idx];
    if (cur_dir.substr(0, 2) != "T.") continue;
    int ts = std::stoi(cur_dir.substr(2, std::string::npos));
    trace_epochs_.push_back(ts);
  }

  std::sort(trace_epochs_.begin(), trace_epochs_.end());

  flog(LOG_INFO, "[TraceReader] %zu epochs discovered.", trace_epochs_.size());

  num_ep = trace_epochs_.size();
  return Status::OK();
}

Status TraceReader::ReadRankIntoPivotCtx(size_t ep_idx, int rank,
                                         carp::PivotCalcCtx* pvt_ctx,
                                         int nitems) {
  Status s = Status::OK();
  std::string data;
  ReadEpoch(ep_idx, rank, data);

  const float* vals = reinterpret_cast<const float*>(data.c_str());
  int val_sz = data.size() / sizeof(float);
  if (nitems > 0) {
    val_sz = std::min(val_sz, nitems);
  }

  pvt_ctx->AddData(vals, val_sz);

  return s;
}

Status TraceReader::ReadRankIntoBins(size_t ep_idx, int rank,
                                     carp::OrderedBins& bins) {
  Status s = Status::OK();
  std::string data;
  ReadEpoch(ep_idx, rank, data);

  size_t valsz = data.size() / sizeof(float);
  const float* vals = reinterpret_cast<const float*>(data.c_str());

  for (size_t vi = 0; vi < valsz; vi++) {
    bins.AddVal(vals[vi], /* force */ true);
  }

  return s;
}

Status TraceReader::ReadAllRanksIntoBins(size_t ep_idx,
                                         carp::OrderedBins& bins) {
  Status s = Status::OK();
  for (int rank = 0; rank < nranks_; rank++) {
    s = ReadRankIntoBins(ep_idx, rank, bins);
    if (!s.ok()) return s;
  }

  return s;
}

Status TraceReader::ReadEpoch(size_t ep_idx, int rank, std::string& data) {
  Status s = Status::OK();

  if (ep_idx >= trace_epochs_.size()) {
    flog(LOG_ERRO, "Epoch does not exist");
    s = Status::InvalidArgument("Epoch dies not exist");
    return s;
  }

  int ts = trace_epochs_[ep_idx];
  char fpath[4096];
  snprintf(fpath, 4096, "%s/T.%d/eparticle.%d.%d", trace_root_.c_str(), ts, ts,
           rank);

  ReadFileToString(env_, fpath, &data);

  return s;
}
}  // namespace pdlfs
