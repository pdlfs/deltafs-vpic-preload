//
// Created by Ankush J on 10/28/22.
//

#include <common.h>
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
