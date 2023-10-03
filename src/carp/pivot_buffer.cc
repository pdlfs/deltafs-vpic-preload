#include <algorithm>

#include "common.h"
#include "rtp.h"

namespace pdlfs {
namespace carp {

/*
 * return true if a is smaller - we prioritize smaller b_value
 * and for same b_value, we prioritize ending items (is_start == false)
 * first.
 */
static bool bounds_lt(const bounds_t& a, const bounds_t& b) {
  return (a.b_value < b.b_value) ||
         ((a.b_value == b.b_value) && (!a.is_start && b.is_start));
}

PivotBuffer::PivotBuffer(const int pivot_counts[STAGES_MAX + 1]) {
  memset(pbuf_count_, 0, sizeof(pbuf_count_));

  for (int stage = 1; stage <= STAGES_MAX; stage++) {
    this->pivot_counts_[stage] = pivot_counts[stage];
  }

  this->cur_round_ = 0;

  return;
}

int PivotBuffer::StoreData(int stage, const double* pivot_data, int dlen,
                           double pivot_weight, bool isnext) {
  int round = this->cur_round_;
  if (isnext) round = !round;   /* round is either 0 or 1 */

  if (stage < 1 || stage > 3) {
    return -1;
  }

  if (pbuf_count_[round][stage] >= FANOUT_MAX) {
    return -2;
  }

  if (dlen != pivot_counts_[stage]) {
    flog(LOG_ERRO, "[PivotBuffer] Expected %d, got %d", pivot_counts_[stage],
         dlen);
    return -3;
  }

  int pbidx = pbuf_count_[round][stage];  /* allocate a new pbidx */

  memcpy(pivot_store_[round][stage][pbidx], pivot_data, dlen * sizeof(double));
  pivot_weights_[round][stage][pbidx] = pivot_weight;
  int new_size = ++pbuf_count_[round][stage];
  assert(new_size > 0);

  flog(LOG_INFO, "[PivotBuffer] Stage %d, New Size: %d", stage, new_size);

  return new_size;
}

int PivotBuffer::AdvanceRound() {
  int old_round = this->cur_round_;   /* discard old current */
  memset(pbuf_count_[old_round], 0, sizeof(pbuf_count_[old_round]));

  this->cur_round_ = !old_round;      /* next becomes current */
  return 0;
}

int PivotBuffer::ClearAllData() {
  memset(pbuf_count_, 0, sizeof(pbuf_count_));
  return 0;
}

int PivotBuffer::GetPivotWeights(int stage, std::vector<double>& weights) {
  int round = this->cur_round_;
  size_t item_count = pbuf_count_[round][stage];
  weights.resize(item_count);

  for (size_t pbidx = 0; pbidx < item_count; pbidx++) {
    weights[pbidx] = pivot_weights_[round][stage][pbidx];
  }

  return 0;
}

int PivotBuffer::LoadBounds(int stage, std::vector<bounds_t>& boundsv) {
  int round = this->cur_round_;

  int num_pbufs = pbuf_count_[round][stage];    /* #pivot bufs received */
  int pivot_count = pivot_counts_[stage];       /* length of bins array */

  for (int pbidx = 0; pbidx < num_pbufs; pbidx++) {
    double bin_weight = pivot_weights_[round][stage][pbidx];
    if (bin_weight == CARP_BAD_PIVOTS) continue;

    for (int p = 0; p < pivot_count - 1; p++) { /* decode bins to bounds */
      double bin_start = pivot_store_[round][stage][pbidx][p];
      double bin_end = pivot_store_[round][stage][pbidx][p + 1];

      if (bin_start == bin_end) continue;  /* zero width => a noop */

      boundsv.push_back({pbidx, bin_start, bin_end, true});
      boundsv.push_back({pbidx, bin_end, bin_start, false});
    }
  }

  std::sort(boundsv.begin(), boundsv.end(), bounds_lt);

  return 0;
}
}  // namespace carp
}  // namespace pdlfs
