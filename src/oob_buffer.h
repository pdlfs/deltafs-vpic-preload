//
// Created by Ankush J on 9/11/20.
//

#pragma once

#include <vector>

#include "range_constants.h"

namespace pdlfs {
typedef struct particle_mem {
  float indexed_prop;             // property for range query
  char buf[pdlfs::kMaxPartSize];  // other data
  int buf_sz;
} particle_mem_t;

class OobBuffer {
 private:
  float range_min_;
  float range_max_;
  bool range_set_ = false;

  std::vector<particle_mem_t> buf_;

  friend class OobFlushIterator;

 public:
  OobBuffer();

  bool OutOfBounds(float prop);

  int Insert(particle_mem_t& item);

  size_t Size() const;

  int SetRange(float range_min, float range_max);

  int GetPartitionedProps(std::vector<float>& left, std::vector<float>& right);
};

class OobFlushIterator {
 private:
  OobBuffer& buf_;
  size_t preserve_idx_ = 0;
  size_t flush_idx_ = 0;
  size_t buf_len_;

 public:
  explicit OobFlushIterator(OobBuffer& buf);
  int PreserveCurrent();
  particle_mem_t& operator*();
  particle_mem_t& operator++();
  bool operator==(size_t& other);
  bool operator!=(size_t& other);
  ~OobFlushIterator();
};

}  // namespace pdlfs