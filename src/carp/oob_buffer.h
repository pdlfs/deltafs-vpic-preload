//
// Created by Ankush J on 9/11/20.
//

/*
 * defines particle_mem struct and out of bounds OobBuffer class
 */

#pragma once
#include <assert.h>

#include <vector>

#include "range.h"
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
  float indexed_prop;             // float key for range q (cfg via CarpOptions)
  char buf[CARP_MAXPARTSZ];       // buf w/encoded particle (key+filename+data)
  size_t buf_sz;                  // total size of encoded data in buf[]
  int shuffle_dest;               // rank# or -1 (unk), via AssignShuffleTarget
} particle_mem_t;

/*
 * OobBuffer: object used to store carp particle data (particle_mem_t)
 * that is outside of the key range that the local proc handles.  the
 * out of bounds particles can either be before the minimum value of
 * our range (i.e. to the "left" of our range on a floating point
 * number line) or past the maximum value of our range (to the right).
 *
 * Semantics: OobBuffer is initialized with a maximum size, but will
 * accept inserts beyond its configured size.  It will report
 * IsFull=true if in overflowed state.  It is up to the user of this
 * class to ensure that the OOB Buffer does not go into the overflow
 * state if a constant memory footprint is desired.
 *
 * we expect higher-level code to provide any needed locking for OobBuffer.
 */
class OobBuffer {
 public:
  //
  // ctor.   allocates an OobBuffer with the max number of oob particles
  // we want to store (in buf_) set to oob_max_sz and the range unset.
  // oob_max_sz is advisory (it just controls if IsFull() is true or not).
  //
  explicit OobBuffer(const size_t oob_max_sz)
      : oob_max_sz_(oob_max_sz) {
    buf_.reserve(oob_max_sz_);
  }

  //
  // copy/append the particle "item" to the oob buffer.
  // caller should ensure "item" is out of bounds before inserting.
  //
  void Insert(particle_mem_t& item);

  //
  // return current number of particles OobBuffer is holding.
  //
  size_t Size() const { return buf_.size(); }

  //
  // return true if the OobBuffer has reached/exceeded its size limit
  //
  bool IsFull() const {
    return buf_.size() >= oob_max_sz_;
  }

  //
  // walk the set of out of bounds partciles we are currently storing.
  // for each particle we determine if its key is to the left or right
  // of the in-bounds range and append the key to the appropriate vector.
  // we then sort the left and right vectors before returning.   if no
  // range is set, then we return all keys in left.
  //
  int GetPartitionedProps(Range ibrange,
                          std::vector<float>& left, std::vector<float>& right);

  //
  // swap OOB list into the given swpbuf for the caller to process.
  // init the new OOB list an empty state.
  //
  void SwapList(std::vector<particle_mem_t>& swpbuf) {
    buf_.swap(swpbuf);
    buf_.clear();
    buf_.reserve(oob_max_sz_);
  }

  //
  // reset OobBuffer to initial state (clear all stored particles).
  //
  void Reset() {
    buf_.clear();
  }

 private:
  const size_t oob_max_sz_;         // max# oob particles we hold (set by ctor)
  std::vector<particle_mem_t> buf_; // OOB buf particle array (prealloc'd)
};

}  // namespace carp
}  // namespace pdlfs
