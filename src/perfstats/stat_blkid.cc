#include <blkid/blkid.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include "perfstats/perfstats.h"
#include "perfstats/stat_blkid.h"

namespace {
int get_stats(const char *sysfs_path, pdlfs::bd_stats_t &bds) {
  if (sysfs_path == NULL) {
    return -1;
  }

  FILE *sys_fp;
  if ((sys_fp = fopen(sysfs_path, "r")) == NULL) {
    return -1;
  }

  int num_scanned =
      fscanf(sys_fp, "%lu %lu %lu %lu %lu %lu %lu %lu %u %u %u", &bds.rd_ios,
             &bds.rd_merges, &bds.rd_secs, &bds.rd_ticks, &bds.wr_ios,
             &bds.wr_merges, &bds.wr_secs, &bds.wr_ticks, &bds.in_flight,
             &bds.io_ticks, &bds.time_queue);

  fclose(sys_fp);

  if (num_scanned != 11) {
    /* Extended statistics not supported */
    return -1;
  }

  return 0;
}

int get_sysfs_path_for_bd(const char *dev_name, char *sys_path_buf,
                          int sys_path_buf_len) {
  if (dev_name == NULL) {
    return -1;
  }

  int devname_len = strlen(dev_name);
  const int devname_len_max = 64;

  if (devname_len >= devname_len_max) {
    return -1;
  }

  char part_name[devname_len_max];
  sscanf(dev_name, "/dev/%s", part_name);

  snprintf(sys_path_buf, sys_path_buf_len, "/sys/class/block/%s/stat",
           part_name);

  return 0;
}
}  // namespace

namespace pdlfs {
StatBlkid::StatBlkid(const char *stat_path)
    : kStatPath(stat_path) {

  struct stat s;

  if (lstat(kStatPath, &s)) {
    printf("Unable to stat\n");
    enabled_ = false;
    return;
  }

  dev_t dev_id = s.st_dev;

  char *dev_name = blkid_devno_to_devname(dev_id);
  if (dev_name == NULL) {
    enabled_ = false;
    return;
  }

  int rv = get_sysfs_path_for_bd(dev_name, this->sysfs_path_, STAT_BUF_MAX);
  if (rv) enabled_ = false;

  return;
}

int StatBlkid::LogOnce(uint64_t timestamp, Stat &s) {
  int rv = 0;

  bd_stats_t bd_stats;
  rv = get_stats(this->sysfs_path_, bd_stats);

  if (!rv) {
    s.SetType(StatType::V_INT, kSecsWrittenLabel);
    s.SetValue(timestamp, (int)bd_stats.wr_secs);
  }

  return rv;
}
}  // namespace pdlfs
