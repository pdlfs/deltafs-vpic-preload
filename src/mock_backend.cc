//
// Created by Ankush J on 9/2/20.
//

#include "mock_backend.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include <string>

namespace {
float get_indexable_property(const char* data_buf) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
}

}  // namespace

namespace pdlfs {

Bucket::Bucket(const uint32_t max_size) : max_size_(max_size){};

bool Bucket::Inside(float prop) { return expected_.Inside(prop); }

int Bucket::Insert(float prop) {
  int rv = 0;

  if (num_items_ >= max_size_) return -1;

  observed_.Extend(prop);
  num_items_++;

  if (not expected_.Inside(prop)) {
    num_items_oob_++;
  }

  return 0;
}

Range Bucket::GetExpectedRange() { return expected_; }

void Bucket::UpdateExpectedRange(Range expected) { expected_ = expected; }

void Bucket::UpdateExpectedRange(float bmin, float bmax) {
  assert(bmin <= bmax);

  expected_.range_min = bmin;
  expected_.range_max = bmax;
}

void Bucket::Reset() {
  num_items_ = 0;
  num_items_oob_ = 0;
  observed_.Reset();
}

int Bucket::FlushAndReset(pdlfs::PartitionManifest& manifest) {
  int rv = 0;

  rv = manifest.AddItem(observed_.range_min, observed_.range_max, num_items_,
                        num_items_oob_);
  Reset();

  return rv;
}
PartitionManifest::PartitionManifest() {}

int PartitionManifest::AddItem(float range_begin, float range_end,
                               uint32_t part_count, uint32_t part_oob) {
  int rv = 0;
  if (part_count == 0) return rv;
  items_.push_back({range_begin, range_end, part_count, part_oob});
  return rv;
}

int PartitionManifest::Dump(FILE* out_file) {
  int rv = 0;

  for (size_t idx = 0; idx < items_.size(); idx++) {
    PartitionManifestItem& item = items_[idx];
    fprintf(out_file, "%.4f %.4f - %u %u\n", item.part_range_begin,
            item.part_range_end, item.part_item_count, item.part_item_oob);
  }
  return rv;
}

MockBackend::MockBackend(uint32_t memtable_size_bytes, uint32_t key_size_bytes)
    : memtable_size_(memtable_size_bytes),
      key_size_(key_size_bytes),
      items_per_flush_(memtable_size_ / key_size_),
      current_(items_per_flush_),
      prev_(items_per_flush_) {}

int MockBackend::Write(const char* data) {
  int rv = 0;

  float indexed_prop = ::get_indexable_property(data);
  if (current_.Inside(indexed_prop)) {
    rv = current_.Insert(indexed_prop);

    if (rv) rv = FlushAndReset(current_);
  } else {
    rv = prev_.Insert(indexed_prop);

    if (rv) rv = FlushAndReset(prev_);
  }

  return rv;
}

int MockBackend::FlushAndReset(Bucket& bucket) {
  int rv = 0;

  rv = bucket.FlushAndReset(manifest_);

  return rv;
}

int MockBackend::UpdateBounds(const float bound_start, const float bound_end) {
  /* Strictly, should lock before updating, but this is only for measuring
   * "pollution" - who cares if it's a couple of counters off */
  prev_.UpdateExpectedRange(current_.GetExpectedRange());
  current_.UpdateExpectedRange(bound_start, bound_end);

  FlushAndReset(prev_);
  FlushAndReset(current_);

  return 0;
}

int MockBackend::SetDumpPath(const char* path) {
  strncpy(dump_path_, path, 255);
  dump_path_set_ = true;

  return 0;
}

int MockBackend::Dump(const char* path) {
  int rv = 0;
  FILE* out_file = fopen(path, "w+");
  manifest_.Dump(out_file);
  fclose(out_file);
  return rv;
}
int MockBackend::Finish() {
  FlushAndReset(prev_);
  FlushAndReset(current_);

  if (dump_path_set_) Dump(dump_path_);
  return 0;
}

std::string MockBackend::GetDumpDir() {
  assert(dump_path_set_);
  std::string dump_path(dump_path_);
  size_t par_idx = dump_path.find_last_of('/');
  return dump_path.substr(0, par_idx);
}

}  // namespace pdlfs
