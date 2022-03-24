//
// Created by Ankush J on 9/2/20.
//

#include "mock_backend.h"

#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <sstream>
#include <string>

namespace {
float get_indexable_property(const char* data_buf) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
}

}  // namespace

namespace pdlfs {
Bucket::Bucket(int rank, const char* bucket_dir, const uint32_t max_size,
               const uint32_t size_per_item)
    : rank_(rank),
      max_size_(max_size),
      size_per_item_(size_per_item),
      max_items_(max_size_ / size_per_item_) {
  bucket_dir_ = bucket_dir;
  bucket_dir_ += "/buckets";
  data_buffer_ = new char[max_size_];
};

bool Bucket::Inside(float prop) { return expected_.Inside(prop); }

int Bucket::Insert(float prop, const char* fname, int fname_len,
                   const char* data, int data_len) {
  int rv = 0;

  // assert(fname_len + data_len == size_per_item_);
  if (num_items_ >= max_items_) return -1;

  observed_.Extend(prop);
  // memcpy(&data_buffer_[data_buffer_idx_], fname, fname_len);
  // memcpy(&data_buffer_[data_buffer_idx_ + fname_len], data, data_len);
  data_buffer_idx_ += fname_len + data_len;
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

void Bucket::Reset(bool epoch_flush) {
  num_items_ = 0;
  num_items_oob_ = 0;
  data_buffer_idx_ = 0;
  observed_.Reset();
  if (epoch_flush) expected_.Reset();
}

int Bucket::FlushAndReset(pdlfs::PartitionManifest& manifest,
                          bool epoch_flush) {
  int rv = 0;

  size_t bidx = manifest.AddItem(observed_.range_min, observed_.range_max,
                                 num_items_, num_items_oob_);

  // if (bidx < SIZE_MAX) {
  // std::stringstream bucket_path;
  // bucket_path << bucket_dir_ << "/bucket." << rank_ << '.' << bidx;
  // FILE* bfile = fopen(bucket_path.str().c_str(), "wb+");
  // fwrite(data_buffer_, size_per_item_, num_items_, bfile);
  // fclose(bfile);
  // }

  Reset(epoch_flush);

  return rv;
}

Bucket::~Bucket() { delete[] data_buffer_; }

bool PartitionManifestItem::Overlaps(float point) const {
  return point >= part_range_begin and point <= part_range_end;
}

bool PartitionManifestItem::Overlaps(float range_begin, float range_end) const {
  return Overlaps(range_begin) or Overlaps(range_end) or
         range_begin < part_range_begin and range_end > part_range_end;
}

PartitionManifest::PartitionManifest() = default;

size_t PartitionManifest::AddItem(float range_begin, float range_end,
                                  uint32_t part_count, uint32_t part_oob,
                                  int rank) {
  if (part_count == 0) return SIZE_MAX;

  size_t item_idx = items_.size();

  items_.push_back(
      {range_begin, range_end, part_count, part_oob, (int)item_idx, rank});

  range_min_ = std::min(range_min_, range_begin);
  range_max_ = std::max(range_max_, range_end);

  mass_total_ += part_count;
  mass_oob_ += part_oob;

  return item_idx;
}

int PartitionManifest::WriteToDisk(FILE* out_file) {
  int rv = 0;

  for (size_t idx = 0; idx < items_.size(); idx++) {
    PartitionManifestItem& item = items_[idx];
    fprintf(out_file, "%.4f %.4f - %u %u\n", item.part_range_begin,
            item.part_range_end, item.part_item_count, item.part_item_oob);
  }
  return rv;
}

int PartitionManifest::PopulateFromDisk(const std::string& disk_path,
                                        int rank) {
  FILE* f = fopen(disk_path.c_str(), "r");
  if (!f) return -1;

  float range_begin;
  float range_end;
  uint32_t size;
  uint32_t size_oob;

  size_t count_total = 0;
  size_t count_returned = 0;

  while (fscanf(f, "%f %f - %d %d\n", &range_begin, &range_end, &size,
                &size_oob) != EOF) {
    if (size) {
      count_returned = AddItem(range_begin, range_end, size, size_oob, rank);
      count_total++;
    }
  }

  fclose(f);

  return (int)count_total;
}

int PartitionManifest::GetOverLappingEntries(float point,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(point)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  return 0;
}

int PartitionManifest::GetOverLappingEntries(float range_begin, float range_end,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(range_begin, range_end)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  return 0;
}
int PartitionManifest::GetRange(float& range_min, float& range_max) const {
  range_min = range_min_;
  range_max = range_max_;
  return 0;
}

int PartitionManifest::GetMass(uint64_t& mass_total, uint64_t mass_oob) const {
  mass_total = mass_total_;
  mass_oob = mass_oob_;
  return 0;
}

size_t PartitionManifest::Size() const { return items_.size(); }

void PartitionManifest::Reset() {
  items_.clear();
  range_min_ = FLT_MAX;
  range_max_ = FLT_MIN;
  mass_total_ = 0;
  mass_oob_ = 0;
}

MockBackend::MockBackend(int rank, const char* dirpath,
                           uint32_t memtable_size_bytes,
                           uint32_t key_size_bytes)
    : rank_(rank),
      dirpath_(dirpath),
      memtable_size_(memtable_size_bytes),
      key_size_(key_size_bytes),
      items_per_flush_(memtable_size_ / key_size_),
      current_(rank, dirpath, memtable_size_, key_size_),
      prev_(rank, dirpath, memtable_size_, key_size_),
      prev2_(rank, dirpath, memtable_size_, key_size_),
      epoch_(0) {
  std::string man_dirpath = dirpath_ + "/manifests";
  mkdir(man_dirpath.c_str(), S_IRWXU);

  std::string bucket_dirpath = dirpath_ + "/buckets";
  mkdir(bucket_dirpath.c_str(), S_IRWXU);

  ComputePaths();
}

int MockBackend::Write(const char* fname, int fname_len, const char* data,
                        int data_len) {
  int rv = 0;

  // float indexed_prop = ::get_indexable_property(data);
  float indexed_prop = *reinterpret_cast<const float*>(fname);
  if (current_.Inside(indexed_prop)) {
    rv = current_.Insert(indexed_prop, fname, fname_len, data, data_len);

    if (rv) rv = FlushAndReset(current_);
  } else if (prev_.Inside(indexed_prop)){
    rv = prev_.Insert(indexed_prop, fname, fname_len, data, data_len);

    if (rv) rv = FlushAndReset(prev_);
  } else {
    rv = prev2_.Insert(indexed_prop, fname, fname_len, data, data_len);

    if (rv) rv = FlushAndReset(prev2_);
  }

  return rv;
}

int MockBackend::FlushAndReset(Bucket& bucket, bool epoch_flush) {
  int rv = 0;

  rv = bucket.FlushAndReset(manifest_, epoch_flush);

  return rv;
}

int MockBackend::UpdateBounds(const float bound_start, const float bound_end) {
  /* Strictly, should lock before updating, but this is only for measuring
   * "pollution" - who cares if it's a couple of counters off */
  prev2_.UpdateExpectedRange(prev_.GetExpectedRange());
  prev_.UpdateExpectedRange(current_.GetExpectedRange());
  current_.UpdateExpectedRange(bound_start, bound_end);

  /* XXX: disabled flushing prev_, assuming that it will reduce bucket count
   * without significantly affecting overlaps */
  // FlushAndReset(prev2_);
  FlushAndReset(prev_);
  FlushAndReset(current_);

  return 0;
}

void MockBackend::ComputePaths() {
  std::string man_dirpath = dirpath_ + "/manifests";

  std::stringstream man_path;
  man_path << man_dirpath << '/' << "vpic-manifest." << epoch_ << "." << rank_;
  manifest_path_ = man_path.str();

  std::stringstream man_bin_path;
  man_bin_path << man_dirpath << '/' << "vpic-manifest.bin." << epoch_ << "."
               << rank_;
  manifest_bin_path_ = man_bin_path.str();
}

int MockBackend::WriteManifestToDisk(const char* path) {
  int rv = 0;
  FILE* out_file = fopen(path, "w+");
  manifest_.WriteToDisk(out_file);
  manifest_.Reset();
  fclose(out_file);
  return rv;
}

int MockBackend::EpochFinish() {
  int rv = 0;

  FlushAndReset(prev2_, /* epoch_end */ true);
  FlushAndReset(prev_, /* epoch_end */ true);
  FlushAndReset(current_, /* epoch_end */ true);

  WriteManifestToDisk(manifest_path_.c_str());

  epoch_++;
  ComputePaths();
  return rv;
}

int MockBackend::Finish() {
  FlushAndReset(prev2_);
  FlushAndReset(prev_);
  FlushAndReset(current_);

  WriteManifestToDisk(manifest_path_.c_str());
  return 0;
}

std::string MockBackend::GetManifestDir() {
  return manifest_path_.substr(0, manifest_path_.find_last_of('/'));
}
}  // namespace pdlfs
