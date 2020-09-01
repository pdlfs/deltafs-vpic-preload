#include <algorithm>
#include "rtp.h"

namespace pdlfs {
DataBuffer::DataBuffer() {
  memset(data_len, 0, sizeof(data_len));
  // XXX: revisit
  this->num_pivots[1] = RANGE_RTP_PVTCNT1;
  this->num_pivots[2] = RANGE_RTP_PVTCNT2;
  this->num_pivots[3] = RANGE_RTP_PVTCNT3;

  this->cur_store_idx = 0;
}

int DataBuffer::store_data(int stage, float *pivot_data, int dlen,
                           float pivot_width, bool isnext) {
  int sidx = this->cur_store_idx;
  if (isnext) sidx = !sidx;

  if (stage < 1 || stage > 3) {
    return -1;
  }

  if (data_len[sidx][stage] >= FANOUT_MAX) {
    return -2;
  }

  if (dlen != num_pivots[stage]) {
    logf(LOG_ERRO, "[DataBuffer] Expected %d, got %d\n", num_pivots[stage],
         dlen);
    return -3;
  }

  int idx = data_len[sidx][stage];

  memcpy(data_store[sidx][stage][idx], pivot_data, dlen * sizeof(float));
  data_widths[sidx][stage][idx] = pivot_width;
  int new_size = ++data_len[sidx][stage];
  assert(new_size > 0);

  logf(LOG_INFO, "Rank %d: new store size %d\n", -1, new_size);

  return new_size;
}

int DataBuffer::get_num_items(int stage, bool isnext) {
  if (stage < 1 || stage > STAGES_MAX) {
    return -1;
  }

  int sidx = this->cur_store_idx;
  if (isnext) sidx = !sidx;

  return data_len[sidx][stage];
}

int DataBuffer::advance_round() {
  int old_sidx = this->cur_store_idx;
  memset(data_len[old_sidx], 0, sizeof(data_len[old_sidx]));

  this->cur_store_idx = !old_sidx;
  return 0;
}

int DataBuffer::clear_all_data() {
  memset(data_len, 0, sizeof(data_len));
  return 0;
}

int DataBuffer::get_pivot_widths(int stage, std::vector<float> &widths) {
  int sidx = this->cur_store_idx;
  int item_count = data_len[sidx][stage];
  widths.resize(item_count);
  std::copy(data_widths[sidx][stage], data_widths[sidx][stage] + item_count,
            widths.begin());
  return 0;
}

int DataBuffer::load_into_rbvec(int stage, std::vector<rb_item_t> &rbvec) {
  int sidx = this->cur_store_idx;

  int num_ranks = data_len[sidx][stage];
  int bins_per_rank = num_pivots[stage];

  for (int rank = 0; rank < num_ranks; rank++) {
    for (int bidx = 0; bidx < bins_per_rank - 1; bidx++) {
      float bin_start = data_store[sidx][stage][rank][bidx];
      float bin_end = data_store[sidx][stage][rank][bidx + 1];

      if (bin_start == bin_end) continue;

      rbvec.push_back({rank, bin_start, bin_end, true});
      rbvec.push_back({rank, bin_end, bin_start, false});
    }
  }

  std::sort(rbvec.begin(), rbvec.end(), rb_item_lt);

  return 0;
}
} // namespace pdlfs
