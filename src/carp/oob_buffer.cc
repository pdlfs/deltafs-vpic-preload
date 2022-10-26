//
// Created by Ankush J on 9/11/20.
//

#include "oob_buffer.h"

#include <algorithm>
#include "common.h"

namespace pdlfs {
namespace carp {

/*
 * copy/append item to our out of bounds buffer (if there is room)
 */
int OobBuffer::Insert(particle_mem_t& item) {
  int rv = 0;

  float prop = item.indexed_prop;

  if (range_set_ and prop >= range_min_ and prop < range_max_) {
    rv = -1;
    flog(LOG_WARN, "[OOB] Buffering of in-bounds item attempted");
    return rv;
  }

  if (buf_.size() >= oob_max_sz_) {
    flog(LOG_WARN, "[OOB] Overflow alert");
    return -1;
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
    if ((not range_set_) or (range_set_ and prop < range_min_)) {
      left.push_back(prop);
    } else if (prop >= range_max_) {
      right.push_back(prop);
    }
  }

  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());

  return 0;
}

}  // namespace carp
}  // namespace pdlfs
