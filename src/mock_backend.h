//
// Created by Ankush J on 9/2/20.
//

#pragma once

#include <stdio.h>
#include <stdint.h>

#include <vector>

namespace pdlfs {
typedef struct {
  float part_range_begin;
  float part_range_end;
  uint32_t part_item_count;
} PartitionManifestItem;

class PartitionManifest {
 private:
  std::vector<PartitionManifestItem> items_;

 public:
  PartitionManifest();
  int AddItem(float range_begin, float range_end, uint32_t part_count);
  int Dump(FILE *out_file);
};

class MockBackend {
 private:
  uint32_t memtable_size_;
  uint32_t key_size_;
  uint32_t items_per_flush_;
  float range_min_ = 0, range_max_ = 0;
  uint32_t items_buffered_ = 0;
  PartitionManifest manifest_;
  char dump_path_[255];
  bool dump_path_set_ = false;

  int Dump(const char *path);
  int FlushAndReset();

 public:
  MockBackend(uint32_t memtable_size, uint32_t key_size);
  int SetDumpPath(const char *path);
  int Write(const char *data);
  int Finish();
};
}  // namespace pdlfs