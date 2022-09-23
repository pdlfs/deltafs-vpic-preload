//
// Created by Ankush J on 9/11/20.
//

/*
 * defines particle_mem struct and out of bounds OobBuffer class
 */

#pragma once
#include <assert.h>
#include <vector>
#include "range_constants.h"

namespace pdlfs {
namespace carp {

/*
 * particle_mem_t: struct used to store particle data in CARP.
 * populated in shuffle_write_range() using Carp::Serialize().
 * the format of the encoded particle in buf[] is:
 *
 *    <inkey> + <invalue> + <optional zero-pad extra bytes>
 *
 * where inkey is a 4 byte float and invalue is the filename
 * and filedata written by the application.
 *
 * note: the optional zero-padding is normally off (we used it
 *       when researching KNL performance)
 */
typedef struct particle_mem {
  float indexed_prop;            // float key for range q (cfg via CarpOptions)
  char buf[pdlfs::kMaxPartSize]; // buf w/encoded particle (key+filename+data)
  int buf_sz;                    // total size of encoded data in buf[]
  int shuffle_dest;              // rank# or -1 (unk), via AssignShuffleTarget
} particle_mem_t;

/*
 * OobBuffer: object used to store carp particle data (particle_mem_t)
 * that is outside of the key range that the local proc handles.  the
 * out of bounds particles can either be before the minimum value of
 * our range (i.e. to the "left" of our range on a floating point
 * number line) or past the maximum value of out range (to the right).
 */
class OobBuffer {
 public:
  //
  // ctor.   allocates an OobBuffer with the max number of oob particles
  // it can store (in buf_) set to oob_max_sz and the range unset.
  //
  explicit OobBuffer(const size_t oob_max_sz)
      : oob_max_sz_(oob_max_sz), range_set_(false) {
          buf_.reserve(oob_max_sz_);
        }

  //
  // check if the key ("prop") lies outside the key range managed by
  // the local carp proc.  if so, we return true.
  //
  bool OutOfBounds(float prop) const {
    if (!range_set_) return true;      // no in bounds range set?
    return(prop < range_min_ || prop > range_max_);
  }

  //
  // copy/append the particle "item" to the oob buffer.  the particle
  // should be OutOfBounds().  return 0 on success, -1 if OobBuffer full
  // or the particle is !OutOfBounds().
  //
  int Insert(particle_mem_t& item);

  //
  // return current number of particles OobBuffer is holding.
  //
  size_t Size() const { return buf_.size(); }

  //
  // return true if the OobBuffer has reached its size limit
  //
  bool IsFull() const {
     assert(buf_.size() <= oob_max_sz_);
     return buf_.size() == oob_max_sz_;
  }

  //
  // set the in-bounds range of the OobBuffer.  in-bounds ranges are
  // inclusive.  we only want to store particles outside of this range.
  //
  void SetRange(float range_min, float range_max) {
    range_min_ = range_min;
    range_max_ = range_max;
    range_set_ = true;
  }

  //
  // walk the set of out of bounds partciles we are currently storing.
  // for each particle we determine if its key is to the left or right
  // of the in-bounds range and append the key to the appropriate vector.
  // we then sort the left and right vectors before returning.
  //
  int GetPartitionedProps(std::vector<float>& left, std::vector<float>& right);

  //
  // reset OobBuffer to initial state (clear all stored particles and
  // unset the in-bounds range).
  //
  void Reset() {
    range_set_ = false;
    range_min_ = range_max_ = 0;     // just in case
    buf_.clear();
  }

 private:
  const size_t oob_max_sz_;          // max# oob particles we hold (set by ctor)
  float range_min_;                  // local in-bounds range start (inclusive)
  float range_max_;                  // local in-bounds range end (inclusive)
  bool range_set_;                   // has SetRange() set the range yet?
  std::vector<particle_mem_t> buf_;  // OOB buf particle array (prealloc'd)

  friend class OobFlushIterator;
  friend class Carp;      // XXX: for carp.h MarkFlushableBufferedItems()
};

/*
 * OobFlushIterator: iterator used when flushing buffered oob particles.
 * we can "preserve" the current oob particle so that it remains in the
 * OobBuffer after the flush (we will copy it earlier in buf_ to prevent
 * empty holes in the buf_ vector/array).  we typically preserve oob
 * particles when we do not yet know where to shuffle them to.
 */
class OobFlushIterator {
 public:
  //
  // ctor.   create a flush iterator for the given OobBuffer
  //
  explicit OobFlushIterator(OobBuffer& buf)
      : buf_(buf), flush_idx_(0), preserve_idx_(0) {
          buf_len_ = buf_.buf_.size();
  }

  //
  // copy ctor.  allows iterator to be copied to another iterator
  // XXX: just for OobIterator() ?
  //
  OobFlushIterator(const OobFlushIterator& other)
      : buf_(other.buf_),
        preserve_idx_(other.preserve_idx_),
        flush_idx_(other.flush_idx_),
        buf_len_(other.buf_len_) {}

  //
  // dtor.  empty out the buf_, except for any preserved entries.
  //
  ~OobFlushIterator() {
    buf_.buf_.resize(preserve_idx_);
  }

  //
  // return current oob particle
  //
  particle_mem_t& operator*() {
    if (flush_idx_ < buf_len_) return buf_.buf_[flush_idx_];
    return buf_.buf_[0];  // XXX: iterator out of bounds, try something safe
  }

  //
  // advance to next oob particle
  //
  void operator++(int) {
    if (flush_idx_ < buf_len_) flush_idx_++;
  }

  //
  // is our iterator index# equal to a given value (XXX: NOTUSED)
  //
  bool operator==(size_t other) const {
    return flush_idx_ == other;
  }

  //
  // is our iterator index# !equal to a given value
  // (used in shuffle_flush_oob() )
  //
  bool operator!=(size_t other) const {
    return flush_idx_ != other;
  }

  //
  // preserve current oob particle in the OobBuffer rather than
  // flush it out (e.g. because it doesn't yet have a shuffle dest
  // that shuffle_flush_oob() can send it to).  ret 0 if ok, -1 on error.
  //
  int PreserveCurrent() {
    if (preserve_idx_ > flush_idx_)
      return -1;    // shouldn't normally happen
    if (preserve_idx_ != flush_idx_)    // do we need to compact buf_[] ?
      buf_.buf_[preserve_idx_] = buf_.buf_[flush_idx_];
    preserve_idx_++;
    return 0;
  }

 private:
  OobBuffer& buf_;                   // OobBuffer we are flushing
  size_t flush_idx_;                 // current index we are flushing
  size_t preserve_idx_;              // # oob particles we are preserving
  size_t buf_len_;                   // # oob particles in buf_
};

}  // namespace carp
}  // namespace pdlfs
