//
// Created by Ankush J on 9/2/20.
//

#pragma once

#include <stdio.h>
#include <stdint.h>

#include <vector>

namespace pdlfs {
class PartitionManifest;

struct Range {
  float range_min = 0;
  float range_max = 0;

  Range() {}

  Range(const Range& r) : range_min(r.range_min), range_max(r.range_max) {}

  Range& operator=(Range& r) {
    range_min = r.range_min;
    range_max = r.range_max;
  }

  void Reset() {
    range_min = 0;
    range_max = 0;
  }

  bool Inside(float f) { return (f >= range_min && f <= range_max); }

  bool Extend(float f) {
    if (range_min == 0 and range_max == 0) {
      range_min = f;
      range_max = f;
    } else {
      range_min = std::min(range_min, f);
      range_max = std::max(range_min, f);
    }
  }
};

class Bucket {
 private:
  Range expected_;
  Range observed_;
  const uint32_t max_size_;
  uint32_t num_items_ = 0;
  uint32_t num_items_oob_ = 0;

 public:
  Bucket(const uint32_t max_size);

  bool Inside(float prop);

  int Insert(float prop);

  Range GetExpectedRange();

  void UpdateExpectedRange(Range expected);

  void UpdateExpectedRange(float bmin, float bmax);

  void Reset();

  int FlushAndReset(PartitionManifest& manifest);
};
}

namespace pdlfs {
typedef struct {
  float part_range_begin;
  float part_range_end;
  uint32_t part_item_count;
  uint32_t part_item_oob;
} PartitionManifestItem;

class PartitionManifest {
 private:
  std::vector<PartitionManifestItem> items_;

 public:
  PartitionManifest();
  int AddItem(float range_begin, float range_end, uint32_t part_count, 
      uint32_t part_oob);
  int Dump(FILE *out_file);
};

class MockBackend {
 private:
  uint32_t memtable_size_;
  uint32_t key_size_;
  uint32_t items_per_flush_;
  // float range_min_ = 0, range_max_ = 0;
  // uint32_t items_buffered_ = 0;

  // float bound_start_ = 0, bound_end_ = 0;
  // float bound_start_prev_ = 0, bound_end_prev_ = 0;
  // uint32_t items_oob_ = 0;

  Bucket current_;
  Bucket prev_;

  PartitionManifest manifest_;
  char dump_path_[255];
  bool dump_path_set_ = false;

  int Dump(const char *path);
  int FlushAndReset(Bucket& bucket);

 public:
  MockBackend(uint32_t memtable_size, uint32_t key_size);
  int UpdateBounds(const float bound_start, const float bound_end);
  int SetDumpPath(const char *path);
  int Write(const char *data);
  int Finish();
};
}  // namespace pdlfs
