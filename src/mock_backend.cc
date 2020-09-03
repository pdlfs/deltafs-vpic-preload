//
// Created by Ankush J on 9/2/20.
//

#include "mock_backend.h"

#include <stdio.h>
#include <string.h>

namespace {
float get_indexable_property(const char *data_buf) {
  const float *prop = reinterpret_cast<const float *>(data_buf);
  return prop[0];
}
}  // namespace

namespace pdlfs {
PartitionManifest::PartitionManifest() {}

int PartitionManifest::AddItem(float range_begin, float range_end,
                               uint32_t part_count) {
  int rv = 0;
  items_.push_back({range_begin, range_end, part_count});
  return rv;
}

int PartitionManifest::Dump(FILE *out_file) {
  int rv = 0;

  for (size_t idx = 0; idx < items_.size(); idx++) {
    PartitionManifestItem &item = items_[idx];
    fprintf(out_file, "%.3f %.3f - %u\n", item.part_range_begin,
            item.part_range_end, item.part_item_count);
  }
  return rv;
}

MockBackend::MockBackend(uint32_t memtable_size_bytes, uint32_t key_size_bytes)
    : memtable_size_(memtable_size_bytes), key_size_(key_size_bytes) {
  items_per_flush_ = memtable_size_ / key_size_;
}

int MockBackend::Write(const char *data) {
  int rv = 0;

  float indexed_prop = ::get_indexable_property(data);

  if (items_buffered_ == 0) {
    range_min_ = indexed_prop;
    range_max_ = indexed_prop;
  } else if (range_min_ > indexed_prop) {
    range_min_ = indexed_prop;
  } else if (range_max_ < indexed_prop) {
    range_max_ = indexed_prop;
  }

  items_buffered_++;

  if (items_buffered_ >= items_per_flush_) rv = FlushAndReset();
  return rv;
}

int MockBackend::FlushAndReset() {
  manifest_.AddItem(range_min_, range_max_, items_buffered_);

  items_buffered_ = 0;
  range_min_ = 0;
  range_max_ = 0;

  return 0;
}

int MockBackend::SetDumpPath(const char *path) {
  strncpy(dump_path_, path, 255);
  dump_path_set_ = true;
}

int MockBackend::Dump(const char *path) {
  int rv = 0;
  FILE *out_file = fopen(path, "w+");
  manifest_.Dump(out_file);
  fclose(out_file);
  return rv;
}
int MockBackend::Finish() {
  FlushAndReset();
  if (dump_path_set_) Dump(dump_path_);
  return 0;
}
}  // namespace pdlfs
