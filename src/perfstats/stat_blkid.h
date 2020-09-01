#pragma once

#include <stdio.h>
#include <vector>
#include "perfstats/stat.h"

namespace pdlfs {
typedef struct bd_stats {
  long unsigned rd_ios;
  long unsigned rd_merges;
  long unsigned rd_secs;
  long unsigned rd_ticks;

  long unsigned wr_ios;
  long unsigned wr_merges;
  long unsigned wr_secs;
  long unsigned wr_ticks;

  unsigned in_flight;
  unsigned io_ticks;
  unsigned time_queue;
} bd_stats_t;

class StatBlkid : public StatLogger {
 private:
  const char *kSecsWrittenLabel = "SYSFS_SECTORS_WRITTEN";
  const char *kStatPath;
  char sysfs_path_[STAT_BUF_MAX];
  FILE *output_file_;
  bool enabled_ = true;

 public:
  StatBlkid(const char *stat_path);
  int LogOnce(uint64_t timestamp, Stat &s) override;
};
}  // namespace pdlfs
