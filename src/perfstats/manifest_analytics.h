//
// Created by Ankush J on 10/9/20.
//

#include <cfloat>

#include "mock_backend.h"

#pragma once

namespace pdlfs {
class ManifestAnalytics {
 public:
  explicit ManifestAnalytics(const char* query_path);
  explicit ManifestAnalytics(MockBackend* backend);
  void PrintStats();

 private:
  std::string manifest_path_;
  std::vector<PartitionManifestItem> global_manifest_;
  std::vector<float> query_points_;
  float range_min_ = FLT_MAX;
  float range_max_ = FLT_MIN;

  int32_t count_total_ = 0;
  int32_t mass_total_ = 0;
  int32_t oob_total_ = 0;

  int32_t count_max_ = 0;
  int32_t mass_max_ = 0;

  bool stats_computed_ = false;

  int Read();
  void ReadManifestFile(const char* mpath);
  void GenerateQueryPoints();
  int ComputeStats();
  int ComputeStats(float point);
};
}  // namespace pdlfs