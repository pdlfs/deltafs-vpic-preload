//
// Created by Ankush J on 9/11/20.
//

#include "oob_buffer.h"
#include "range_common.h" /* for deduplicate_sorted_vector */

#include <algorithm>

#include "common.h"

namespace pdlfs {
namespace carp {

/*
 * copy/append item to our out of bounds buffer.  inserts beyond
 * oob_max_sz_ are allowed (puts us in IsFull() overflow state).
 */
void OobBuffer::Insert(particle_mem_t& item) {
  if (buf_.size() >= oob_max_sz_) {
    flog(LOG_DBUG, "[OOB] Overflow alert");
  }

  buf_.push_back(item);
}

/*
 * return keys of our out of bounds buffers in two sorted vectors.
 * left is filled with keys prior to inbounds area.
 */
int OobBuffer::GetPartitionedProps(Range ibrange,
                                   std::vector<float>& left,
                                   std::vector<float>& right) {
  for (auto it = buf_.cbegin(); it != buf_.cend(); it++) {
    float prop = it->indexed_prop;
    if ((not ibrange.IsSet()) or
        (ibrange.IsSet() and prop < ibrange.rmin())) {
      left.push_back(prop);
    } else if (prop >= ibrange.rmax()) {
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
