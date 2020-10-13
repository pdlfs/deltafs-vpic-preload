//
// Created by Ankush J on 10/9/20.
//

#include "manifest_analytics.h"

#include <sys/stat.h>

#include "common.h"

namespace pdlfs {
ManifestAnalytics::ManifestAnalytics(const char* manifest_path)
    : manifest_path_(manifest_path) {}

void ManifestAnalytics::PrintStats() {
  if (!stats_computed_) ComputeStats();

  logf(LOG_INFO,
       "[perfstats-analytics] %s SSTs (%s items, OOB: %.2f%%). "
       "Max overlap: %s SSTs (%s items, %.4f%%) \n",
       pretty_num(count_total_).c_str(), pretty_num(mass_total_).c_str(),
       oob_total_ * 100.0 / mass_total_, pretty_num(count_max_).c_str(),
       pretty_num(mass_max_).c_str(), mass_max_ * 100.0 / mass_total_);
}

int ManifestAnalytics::Read() {
  int rv = 0;

  stats_computed_ = false;

  int rank = 0;
  struct stat stat_buf;

  while (true) {
    char rank_path[2048];
    snprintf(rank_path, 2048, "%s/vpic-manifest.%d", manifest_path_, rank++);

    if (stat(rank_path, &stat_buf)) break;

    ReadManifestFile(rank_path);
  }

  return rv;
}

void ManifestAnalytics::ReadManifestFile(const char* mpath) {
  FILE* f = fopen(mpath, "r");

  float range_start;
  float range_end;
  uint32_t size;
  uint32_t size_oob;

  while (fscanf(f, "%f %f - %d %d\n", &range_start, &range_end, &size,
                &size_oob) != EOF) {
    global_manifest_.push_back({range_start, range_end, size, size_oob});

    range_min_ = std::min(range_min_, range_start);
    range_max_ = std::max(range_max_, range_end);

    count_total_++;
    mass_total_ += size;
    oob_total_ += size_oob;
  }

  fclose(f);
}

void ManifestAnalytics::GenerateQueryPoints() {
  query_points_.clear();

  int32_t num_points = 100;
  float intvl_sz = (range_max_ - range_min_) / num_points;

  float intvl_cur = range_min_;

  do {
    query_points_.push_back(intvl_cur);
    intvl_cur += intvl_sz;
  } while (intvl_cur <= range_max_);
}

int ManifestAnalytics::ComputeStats() {
  if (global_manifest_.empty()) Read();
  assert(!global_manifest_.empty());
  if (query_points_.empty()) GenerateQueryPoints();
  assert(!query_points_.empty());

  for (size_t i = 0; i < query_points_.size(); i++) {
    ComputeStats(query_points_[i]);
  }

  stats_computed_ = true;

  return 0;
}

int ManifestAnalytics::ComputeStats(float point) {
  int32_t count_cur = 0;
  int32_t mass_cur = 0;

  for (size_t idx = 0; idx < global_manifest_.size(); idx++) {
    PartitionManifestItem& item = global_manifest_[idx];
    bool overlap =
        item.part_range_begin <= point and item.part_range_end >= point;

    if (overlap) {
      count_cur++;
      mass_cur += item.part_item_count;
    }
  }

  count_max_ = std::max(count_max_, count_cur);
  mass_max_ = std::max(mass_max_, mass_cur);

  return 0;
}
}  // namespace pdlfs
