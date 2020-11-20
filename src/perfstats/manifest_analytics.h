//
// Created by Ankush J on 10/9/20.
//

#include <cfloat>
#include <string>

#include "range_backend/range_backend.h"

#pragma once

namespace pdlfs {
class ManifestAnalytics {
 public:
  explicit ManifestAnalytics(const char* query_path);
  explicit ManifestAnalytics(RangeBackend* backend);
  void PrintStats();

 private:
  std::string manifest_path_;
  PartitionManifest manifest_;
  std::vector<float> query_points_;

  float range_min_ = FLT_MAX;
  float range_max_ = FLT_MIN;

  uint32_t count_total_ = 0;
  uint64_t mass_total_ = 0;
  uint64_t mass_oob_ = 0;

  uint64_t count_max_ = 0;
  uint64_t mass_max_ = 0;

  int Read(int epoch);
  void GenerateQueryPoints();
  int ComputeStats(int epoch);
  int PrintStats(int epoch);
};
}  // namespace pdlfs
