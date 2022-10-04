#include <algorithm>

#include "common.h"
#include "rtp.h"

namespace pdlfs {
DataBuffer::DataBuffer(const int num_pivots[STAGES_MAX + 1]) {
  memset(data_len_, 0, sizeof(data_len_));

  for (int sidx = 1; sidx <= STAGES_MAX; sidx++ ){
    this->num_pivots_[sidx] = num_pivots[sidx];
  }

  this->cur_store_idx_ = 0;

  return;
}

int DataBuffer::StoreData(int stage, const double* pivot_data, int dlen,
                           double pivot_width, bool isnext) {
  int sidx = this->cur_store_idx_;
  if (isnext) sidx = !sidx;

  if (stage < 1 || stage > 3) {
    return -1;
  }

  if (data_len_[sidx][stage] >= FANOUT_MAX) {
    return -2;
  }

  if (dlen != num_pivots_[stage]) {
    logf(LOG_ERRO, "[DataBuffer] Expected %d, got %d", num_pivots_[stage],
         dlen);
    return -3;
  }

  int idx = data_len_[sidx][stage];

  memcpy(data_store_[sidx][stage][idx], pivot_data, dlen * sizeof(double));
  data_widths_[sidx][stage][idx] = pivot_width;
  int new_size = ++data_len_[sidx][stage];
  assert(new_size > 0);

  logf(LOG_INFO, "[DataBuffer] Stage %d, New Store Size: %d", stage, new_size);

  return new_size;
}

int DataBuffer::GetNumItems(int stage, bool isnext) {
  if (stage < 1 || stage > STAGES_MAX) {
    return -1;
  }

  int sidx = this->cur_store_idx_;
  if (isnext) sidx = !sidx;

  return data_len_[sidx][stage];
}

int DataBuffer::AdvanceRound() {
  int old_sidx = this->cur_store_idx_;
  memset(data_len_[old_sidx], 0, sizeof(data_len_[old_sidx]));

  this->cur_store_idx_ = !old_sidx;
  return 0;
}

int DataBuffer::ClearAllData() {
  memset(data_len_, 0, sizeof(data_len_));
  return 0;
}

int DataBuffer::GetPivotWidths(int stage, std::vector<double>& widths) {
  int sidx = this->cur_store_idx_;
  int item_count = data_len_[sidx][stage];
  widths.resize(item_count);
  std::copy(data_widths_[sidx][stage], data_widths_[sidx][stage] + item_count,
            widths.begin());
  return 0;
}

int DataBuffer::LoadIntoRbvec(int stage, std::vector<rb_item_t>& rbvec) {
  int sidx = this->cur_store_idx_;

  int num_ranks = data_len_[sidx][stage];
  int bins_per_rank = num_pivots_[stage];

  for (int rank = 0; rank < num_ranks; rank++) {
    for (int bidx = 0; bidx < bins_per_rank - 1; bidx++) {
      double bin_start = data_store_[sidx][stage][rank][bidx];
      double bin_end = data_store_[sidx][stage][rank][bidx + 1];

      if (bin_start == bin_end) continue;

      rbvec.push_back({rank, bin_start, bin_end, true});
      rbvec.push_back({rank, bin_end, bin_start, false});
    }
  }

  std::sort(rbvec.begin(), rbvec.end(), rb_item_lt);

  return 0;
}
}  // namespace pdlfs
