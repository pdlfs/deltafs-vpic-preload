//
// Created by Ankush J on 9/2/20.
//

#pragma once

#include <float.h>
#include <stdint.h>
#include <stdio.h>

#include <string>
#include <vector>

namespace pdlfs {
class PartitionManifest;

struct Range {
  float range_min = 0;
  float range_max = 0;

  Range() = default;

  Range(const Range& r) = default;

  void operator=(Range& r) {
    range_min = r.range_min;
    range_max = r.range_max;
  }

  void Reset() {
    range_min = 0;
    range_max = 0;
  }

  bool Inside(float f) const { return (f >= range_min && f <= range_max); }

  void Extend(float f) {
    if (range_min == 0 and range_max == 0) {
      range_min = f;
      range_max = f;
    } else {
      range_min = std::min(range_min, f);
      range_max = std::max(range_min, f);
    }
  }
};

template <size_t BYTES>
struct BucketItem {
  char bytes[BYTES];
};

class Bucket {
 private:
  Range expected_;
  Range observed_;
  const uint32_t max_size_;
  const uint32_t size_per_item_;
  const uint32_t max_items_;
  uint32_t num_items_ = 0;
  uint32_t num_items_oob_ = 0;

  char* data_buffer_;
  size_t data_buffer_idx_ = 0;

  std::string bucket_dir_;
  const int rank_;

 public:
  Bucket(int rank, const char* bucket_dir, const uint32_t max_size,
         const uint32_t size_per_item);

  bool Inside(float prop);

  int Insert(float prop, const char* fname, int fname_len, const char* data,
             int data_len);

  Range GetExpectedRange();

  void UpdateExpectedRange(Range expected);

  void UpdateExpectedRange(float bmin, float bmax);

  void Reset();

  int FlushAndReset(PartitionManifest& manifest);

  ~Bucket();
};

typedef struct PartitionManifestItem {
  float part_range_begin;
  float part_range_end;
  uint32_t part_item_count;
  uint32_t part_item_oob;
  int bucket_idx;
  int rank;

  bool Overlaps(float point) const;
  bool Overlaps(float range_begin, float range_end) const;
} PartitionManifestItem;

typedef struct PartitionManifestMatch {
  std::vector<PartitionManifestItem> items;
  uint64_t mass_total = 0;
  uint64_t mass_oob = 0;
} PartitionManifestMatch;

class PartitionManifest {
 private:
  std::vector<PartitionManifestItem> items_;
  float range_min_ = FLT_MAX;
  float range_max_ = FLT_MIN;

  uint64_t mass_total_ = 0;
  uint64_t mass_oob_ = 0;

 public:
  PartitionManifest();
  size_t AddItem(float range_begin, float range_end, uint32_t part_count,
                 uint32_t part_oob, int rank = -1);
  int WriteToDisk(FILE* out_file);
  int GetRange(float& range_min, float& range_max) const;
  int GetMass(uint64_t& mass_total, uint64_t mass_oob) const;
  size_t Size() const;
  int PopulateFromDisk(const std::string& disk_path, int rank);
  int GetOverLappingEntries(float point, PartitionManifestMatch& match);
  int GetOverLappingEntries(float range_begin, float range_end,
                            PartitionManifestMatch& match);
};

class RangeBackend {
 private:
  const int rank_;
  std::string dirpath_;
  uint32_t memtable_size_;
  uint32_t key_size_;
  uint32_t items_per_flush_;

  Bucket current_;
  Bucket prev_;

  std::string manifest_path_;
  std::string manifest_bin_path_;
  PartitionManifest manifest_;

  /* XXX: needs to be atomic if writing is multithreaded */
  uint32_t bucket_idx_ = 0;

  int WriteManifestToDisk(const char* path);
  int FlushAndReset(Bucket& bucket);

 public:
  RangeBackend(int rank, const char* dirpath, uint32_t memtable_size,
               uint32_t key_size);
  int UpdateBounds(float bound_start, float bound_end);
  std::string GetManifestDir();
  int Write(const char* fname, int fname_len, const char* data, int data_len);
  int Finish();
};
}  // namespace pdlfs
