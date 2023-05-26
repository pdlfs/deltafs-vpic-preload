#include "range_utils.h"

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <list>

#include "preload_internal.h"

#define RANGE_DEBUG_T 1

extern preload_ctx_t pctx;

void pivot_union(std::vector<rb_item_t> rb_items,
                 std::vector<double>& unified_bins,
                 std::vector<float>& unified_bin_counts,
                 std::vector<double>& rank_bin_weights, int num_ranks) {
  assert(rb_items.size() >= 2u);

  float rank_bin_start[num_ranks];
  float rank_bin_end[num_ranks];

  const float BIN_EMPTY = rb_items[0].bin_val - 10;

  std::fill(rank_bin_start, rank_bin_start + num_ranks, BIN_EMPTY);

  float prev_bin_val = rb_items[0].bin_val;
  float prev_bp_bin_val = rb_items[0].bin_val;

  /* Ranks that currently have an active bin */
  std::list<int> active_ranks;

  for (size_t i = 0; i < rb_items.size(); i++) {
    int bp_rank = rb_items[i].rank;
    float bp_bin_val = rb_items[i].bin_val;
    float bp_bin_other = rb_items[i].bin_other;
    bool bp_is_start = rb_items[i].is_start;

#ifdef RANGE_DEBUG
    fprintf(stderr, "BP_ERR Rank %d/%d - bin_prop: %d %.1f %.1f %s\n",
            pctx.my_rank, i, bp_rank, bp_bin_val, bp_bin_other,
            bp_is_start ? "true" : "false");
#endif

    std::list<int>::iterator remove_item;
    /* Can't set iterators to null apparently false means iter is NULL */
    bool remove_item_flag = false;

    if (bp_bin_val != prev_bin_val) {
      /* Breaking point - we break all active intervals
       * at this point and put them into a new bin */
      float cur_bin = bp_bin_val;
      float cur_bin_count = 0;

      auto lit = active_ranks.begin();
      auto lend = active_ranks.end();

      while (lit != lend) {
        int rank = (*lit);
        assert(rank_bin_start[rank] != BIN_EMPTY);

        float rank_total_range = rank_bin_end[rank] - rank_bin_start[rank];
        float rank_left_range = cur_bin - prev_bp_bin_val;

        assert(rank_left_range >= 0);

        float rank_contrib =
            rank_bin_weights[rank] * rank_left_range / rank_total_range;

        assert(rank_contrib >= 0);

        cur_bin_count += rank_contrib;

        if (rank == bp_rank) {
          /* If you come across the same interval in active_ranks,
           * you must be the closing end of the rank */
          assert(!bp_is_start);
          remove_item = lit;
          remove_item_flag = true;
        }

        lit++;
      }

      unified_bin_counts.push_back(cur_bin_count);
      unified_bins.push_back(cur_bin);

      if (remove_item_flag) {
        active_ranks.erase(remove_item);
      }

      prev_bp_bin_val = bp_bin_val;
    }

    if (bp_is_start) {
      assert(rank_bin_start[bp_rank] == BIN_EMPTY);
      rank_bin_start[bp_rank] = bp_bin_val;
      rank_bin_end[bp_rank] = bp_bin_other;
      active_ranks.push_back(bp_rank);
      if (i == 0) unified_bins.push_back(bp_bin_val);
    } else {
      assert(rank_bin_start[bp_rank] != BIN_EMPTY);
      rank_bin_start[bp_rank] = BIN_EMPTY;
      rank_bin_end[bp_rank] = BIN_EMPTY;
      // remove corresponding start from active_ranks
      if (remove_item_flag == false) {
        int old_len = active_ranks.size();
        /* The following command must remove exactly one element */
        active_ranks.remove(bp_rank);

        int new_len = active_ranks.size();

        if (old_len != new_len + 1) {
          flog(LOG_ERRO,
               "[Rank %d] [pivot_calc] ASSERT FAIL old_len (%d) != new_len "
               "(%d) + 1",
               pctx.my_rank, old_len, new_len + 1);
          ABORT("Assert failed!");
        }

        assert(old_len == new_len + 1);
      }
    }
    prev_bin_val = bp_bin_val;
  }
}

int get_particle_count(int total_ranks, int total_bins, int par_per_bin) {
  /* There is one extra bin entry for every rank. If a rank has two
   * bins, its bin range contains 3 entries 1, 2, 3 for 1 - 2 and 2- 3
   * Hence we subtract total_bins from total_ranks to adjust for that
   */
  return (total_bins - total_ranks) * par_per_bin;
}

int resample_bins_irregular(const std::vector<double>& bins,
                            const std::vector<float>& bin_counts,
                            std::vector<double>& samples, double& sample_weight,
                            int nsamples) {
  const int bins_size = bins.size();
  const int bin_counts_size = bin_counts.size();

  samples.resize(nsamples);

  assert(bins_size == bin_counts_size + 1);
  assert(nsamples >= 2);
  assert(bins_size >= 2);

  float nparticles =
      std::accumulate(bin_counts.begin(), bin_counts.end(), 0.0f);

  assert(nparticles >= 0);

  const double part_per_rbin = nparticles * 1.0 / (nsamples - 1);
  sample_weight = part_per_rbin;

  int sidx = 1;

  double accumulated = 0;
  for (int rbidx = 0; rbidx < bins_size - 1; rbidx++) {
    float rcount_total = bin_counts[rbidx];

    double rbin_start = bins[rbidx];
    double rbin_end = bins[rbidx + 1];
    while (accumulated + rcount_total > part_per_rbin - RANGE_EPSILON) {
      double rcount_cur = part_per_rbin - accumulated;
      accumulated = 0;

      double rbin_cur =
          (rcount_cur / rcount_total) * (rbin_end - rbin_start) + rbin_start;

      assert(sidx < nsamples);
      samples[sidx] = rbin_cur;
      sidx++;

      rcount_total -= rcount_cur;
      rbin_start = rbin_cur;
    }

    accumulated += rcount_total;
  }

  /* extend bounds marginally to handle edge cases */
  samples[0] = bins[0] - 1e-6;
  samples[nsamples - 1] = bins[bins_size - 1] + 1e-6;

  /* if we were unable to fill in the last sample_idx
   * we fill it manually above  (since accumulated can have some leftover
   * from the last for loop)
   */
  if (sidx == nsamples - 1) sidx++;

  if (sidx != nsamples) {
    flog(LOG_ERRO,
         "rank %d, sidx expected to be equal to nsamples, %d-%d, accumulated: "
         "%.1f",
         pctx.my_rank, sidx, nsamples, accumulated);
    ABORT("sidx != nsamples");
  }

  assert(sidx == nsamples);

  return 0;
}
