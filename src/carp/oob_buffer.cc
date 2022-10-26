//
// Created by Ankush J on 9/11/20.
//

#include "oob_buffer.h"

#include <algorithm>

#include "common.h"

namespace pdlfs {
namespace carp {

/*
 * copy/append item to our out of bounds buffer
 * inserts will be accepted beyond configured size, but IsFull
 * will return true in the "overflow state"
 */
int OobBuffer::Insert(particle_mem_t& item) {
  int rv = 0;

  float prop = item.indexed_prop;

  if (range_.Inside(prop)) {
    rv = -1;
    flog(LOG_WARN, "[OOB] Buffering of in-bounds item attempted");
    return rv;
  }

  if (buf_.size() >= oob_max_sz_) {
    flog(LOG_DBUG, "[OOB] Overflow alert");
  }

  buf_.push_back(item);

  return rv;
}

/*
 * return keys of our out of bounds buffers in two sorted vectors.
 * left is filled with keys prior to inbounds area.
 */
int OobBuffer::GetPartitionedProps(std::vector<float>& left,
                                   std::vector<float>& right) {
  for (auto it = buf_.cbegin(); it != buf_.cend(); it++) {
    float prop = it->indexed_prop;
    if ((not range_.IsSet()) or (range_.IsSet() and prop < range_.rmin())) {
      left.push_back(prop);
    } else if (prop > range_.rmax()) {  /* XXXCDC: > vs >=, inclusive */
      right.push_back(prop);
    }
  }

  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());

  size_t oldlsz = left.size(), oldrsz = right.size();

  deduplicate_sorted_vector(left);
  deduplicate_sorted_vector(right);

  size_t newlsz = left.size(), newrsz = right.size();

  if (newlsz != oldlsz) {
    flog(LOG_INFO, "[OOBBuffer, Left] Duplicates removed (%zu to %zu)", oldlsz,
         newlsz);
  }

  if (newrsz != oldrsz) {
    flog(LOG_INFO, "[OOBBuffer, Right] Duplicates removed (%zu to %zu)", oldrsz,
         newrsz);
  }

  return 0;
}
}  // namespace carp
}  // namespace pdlfs
