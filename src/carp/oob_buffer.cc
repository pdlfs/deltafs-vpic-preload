//
// Created by Ankush J on 9/11/20.
//

#include "oob_buffer.h"

#include <assert.h>

#include <algorithm>

#include "common.h"

namespace pdlfs {
namespace carp {
OobBuffer::OobBuffer(const size_t oob_max_sz) : oob_max_sz_(oob_max_sz) {
  buf_.reserve(oob_max_sz_);
}

bool OobBuffer::OutOfBounds(float prop) const {
  if (not range_set_) return true;

  return (prop < range_min_ or prop > range_max_);
}

int OobBuffer::Insert(particle_mem_t& item) {
  int rv = 0;

  float prop = item.indexed_prop;

  if (range_set_ and prop > range_min_ and prop < range_max_) {
    rv = -1;
    logf(LOG_WARN, "[OOB] Buffering of in-bounds item attempted");
    return rv;
  }

  if (buf_.size() >= oob_max_sz_) {
    logf(LOG_WARN, "[OOB] Overflow alert");
    return -1;
  }

  buf_.push_back(item);

  return rv;
}

size_t OobBuffer::Size() const { return buf_.size(); }

bool OobBuffer::IsFull() const {
  size_t buf_sz = Size();
  assert(buf_sz <= oob_max_sz_);
  return buf_.size() == oob_max_sz_;
}

int OobBuffer::SetRange(float range_min, float range_max) {
  range_min_ = range_min;
  range_max_ = range_max;
  range_set_ = true;

  return 0;
}

int OobBuffer::GetPartitionedProps(std::vector<float>& left,
                                   std::vector<float>& right) {
  for (auto it = buf_.cbegin(); it != buf_.cend(); it++) {
    float prop = it->indexed_prop;
    if ((not range_set_) or (range_set_ and prop < range_min_)) {
      left.push_back(prop);
    } else if (prop > range_max_) {
      right.push_back(prop);
    }
  }

  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());

  return 0;
}
int OobBuffer::Reset() {
  range_set_ = false;
  range_min_ = 0;
  range_max_ = 0;

  buf_.clear();

  return 0;
}

OobFlushIterator::OobFlushIterator(OobBuffer& buf) : buf_(buf) {
  buf_len_ = buf_.buf_.size();
}

int OobFlushIterator::PreserveCurrent() {
  int rv = 0;

  if (preserve_idx_ > flush_idx_) {
    /* can't preserve ahead of flush */
    return -1;
  }

  buf_.buf_[preserve_idx_++] = buf_.buf_[flush_idx_];
  return rv;
}

particle_mem_t& OobFlushIterator::operator*() {
  if (flush_idx_ < buf_len_) {
    return buf_.buf_[flush_idx_];
  } else {
    /* iterator is out of bounds, do the safest possible thing */
    return buf_.buf_[0];
  }
}

void OobFlushIterator::operator++(int) {
  if (flush_idx_ < buf_len_) {
    flush_idx_++;
  }
}

bool OobFlushIterator::operator==(const size_t other) const {
  return (flush_idx_ == other);
}

bool OobFlushIterator::operator!=(const size_t other) const {
  return (flush_idx_ != other);
}

OobFlushIterator::~OobFlushIterator() { buf_.buf_.resize(preserve_idx_); }
}  // namespace carp
}  // namespace pdlfs
