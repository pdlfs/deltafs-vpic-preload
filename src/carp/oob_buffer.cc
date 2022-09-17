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

/*
 * return keys of our out of bounds buffers in two sorted vectors.
 * left is filled with keys prior to inbounds area.
 */
int OobBuffer::GetPartitionedProps(std::vector<float>& left,
                                   std::vector<float>& right) {
  std::vector<float> temp_left;
  std::vector<float> temp_right;

  for (auto it = buf_.cbegin(); it != buf_.cend(); it++) {
    float prop = it->indexed_prop;
    if ((not range_set_) or (range_set_ and prop < range_min_)) {
      temp_left.push_back(prop);
    } else if (prop > range_max_) {
      temp_right.push_back(prop);
    }
  }

  std::sort(temp_left.begin(), temp_left.end());
  std::sort(temp_right.begin(), temp_right.end());

  CopyWithoutDuplicates(temp_left, left);
  CopyWithoutDuplicates(temp_right, right);

  return 0;
}

void OobBuffer::CopyWithoutDuplicates(std::vector<float>& in,
                                      std::vector<float>& out) {
  if (in.size() == 0) return;

  out.push_back(in[0]);
  float last_copied = in[0];

  for (size_t idx = 1; idx < in.size(); idx++) {
    float cur = in[idx];
    float prev = in[idx - 1];

    assert(cur >= prev);
    assert(cur >= last_copied);

    if (cur - last_copied > 1e-7) {
      // arbitrary comparison threshold
      out.push_back(cur);
      last_copied = cur;
    }
  }

  size_t in_sz = in.size(), out_sz = out.size();
  if (in_sz != out_sz) {
    logf(LOG_WARN,
         "[OOB::RemoveDuplicates] Some elements dropped (orig: %zu, "
         "dupl: %zu)",
         in_sz, out_sz);
  }
}
}  // namespace carp
}  // namespace pdlfs
