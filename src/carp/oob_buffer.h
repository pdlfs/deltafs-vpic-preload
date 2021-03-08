//
// Created by Ankush J on 9/11/20.
//

#pragma once

#include <vector>

#include "range_constants.h"

namespace pdlfs {
namespace carp {
typedef struct particle_mem {
  float indexed_prop;             // property for range query
  char buf[pdlfs::kMaxPartSize];  // other data
  int buf_sz;
  char* data_ptr;
  int data_sz;
  int shuffle_dest;
} particle_mem_t;

class OobBuffer {
 private:
  const size_t oob_max_sz_;
  float range_min_;
  float range_max_;
  bool range_set_ = false;

  std::vector<particle_mem_t> buf_;

  friend class OobFlushIterator;
  friend class Carp;

 public:
  OobBuffer(const size_t oob_max_sz);

  bool OutOfBounds(float prop) const;

  int Insert(particle_mem_t& item);

  size_t Size() const;

  bool IsFull() const;

  int SetRange(float range_min, float range_max);

  int GetPartitionedProps(std::vector<float>& left, std::vector<float>& right);

  int Reset();
};

class OobFlushIterator {
 private:
  OobBuffer& buf_;
  size_t preserve_idx_ = 0;
  size_t flush_idx_ = 0;
  size_t buf_len_;

 public:
  explicit OobFlushIterator(OobBuffer& buf);
  OobFlushIterator(const OobFlushIterator& other)
      : buf_(other.buf_),
        preserve_idx_(other.preserve_idx_),
        flush_idx_(other.flush_idx_),
        buf_len_(other.buf_len_) {}
  int PreserveCurrent();
  particle_mem_t& operator*();
  void operator++(int);
  bool operator==(size_t other) const;
  bool operator!=(size_t other) const;
  ~OobFlushIterator();
};
}  // namespace carp
}  // namespace pdlfs