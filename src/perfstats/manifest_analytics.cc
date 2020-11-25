//
// Created by Ankush J on 10/9/20.
//

#include "manifest_analytics.h"

#include <sys/stat.h>

#include "common.h"
#include "perfstats.h"

namespace pdlfs {
ManifestAnalytics::ManifestAnalytics(const char* manifest_path)
    : manifest_path_(manifest_path) {}

ManifestAnalytics::ManifestAnalytics(RangeBackend* backend)
    : manifest_path_(backend->GetManifestDir()) {}

void ManifestAnalytics::PrintStats(perfstats_ctx_t* perf_ctx) {
  int epoch = 0;

  perfstats_log_carp(perf_ctx);

  while (PrintStats(perf_ctx, epoch) == 0) epoch++;

  logf(LOG_INFO,
       "[perfstats-analytics] %d epochs discovered. Analysis complete. \n",
       epoch);
}

int ManifestAnalytics::Read(int epoch) {
  int rv = 0;

  int rank = 0;
  struct stat stat_buf;

  while (true) {
    char rank_path[2048];
    snprintf(rank_path, 2048, "%s/vpic-manifest.%d.%d", manifest_path_.c_str(),
             epoch, rank++);

    if (stat(rank_path, &stat_buf)) break;

    manifest_.PopulateFromDisk(std::string(rank_path), rank - 1);
  }

  manifest_.GetRange(range_min_, range_max_);
  manifest_.GetMass(mass_total_, mass_oob_);
  count_total_ = manifest_.Size();

  return rv;
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

int ManifestAnalytics::ComputeStats(int epoch) {
  manifest_.Reset();
  Read(epoch);

  if (manifest_.Size() == 0u) return -1;

  GenerateQueryPoints();
  assert(!query_points_.empty());

  for (size_t i = 0; i < query_points_.size(); i++) {
    PartitionManifestMatch match;
    manifest_.GetOverLappingEntries(query_points_[i], match);
    count_max_ = std::max(count_max_, (uint64_t)match.items.size());
    mass_max_ = std::max(mass_max_, match.mass_total);
  }

  return 0;
}

int ManifestAnalytics::PrintStats(perfstats_ctx_t* perf_ctx, int epoch) {
  int rv = ComputeStats(epoch);
  if (rv != 0) return rv;

  float oob_frac = mass_oob_ * 100.0f / mass_total_;
  float overlap_frac = mass_max_ * 100.0f / mass_total_;

  logf(LOG_INFO,
       "[perfstats-analytics] [epoch %d] %s SSTs (%s items, OOB: %.2f%%). "
       "Max overlap: %s SSTs (%s items, %.4f%%) \n",
       epoch, pretty_num(count_total_).c_str(), pretty_num(mass_total_).c_str(),
       oob_frac, pretty_num(count_max_).c_str(), pretty_num(mass_max_).c_str(),
       overlap_frac);

  std::string event_label =
      "MANIFEST_ANALYTICS_E" + std::to_string(epoch) + "_";

#define PERFLOG(a, b)                                         \
  perfstats_log_eventstr(perf_ctx, (event_label + (a)).c_str(), \
                         std::to_string(b).c_str())

  PERFLOG("SST_COUNT", count_total_);
  PERFLOG("SST_MASS", mass_total_);
  PERFLOG("OOB_FRACPCT", oob_frac);
  PERFLOG("OLAP_MAXCNT", count_max_);
  PERFLOG("OLAP_MAXMASS", mass_max_);
  PERFLOG("OLAP_FRACPCT", overlap_frac);

#undef PERFLOG

  return rv;
}
}  // namespace pdlfs
