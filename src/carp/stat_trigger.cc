//
// Created by Ankush J on 4/2/21.
//

#include "stat_trigger.h"

#include "carp.h"

namespace pdlfs {
namespace carp {
StatTrigger::StatTrigger(const CarpOptions& options)
    : env_(options.env),
      my_rank_(options.my_rank),
      nranks_(options.num_ranks),
      base_dir_(options.mount_path),
      invoke_intvl_(options.dynamic_intvl),
      invoke_counter_(0),
      thresh_(options.dynamic_thresh),
      prev_rsize_(nranks_, 0),
      rsize_diff_(nranks_, 0) {
}
}  // namespace carp
}  // namespace pdlfs
