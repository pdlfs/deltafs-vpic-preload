#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <list>

#include "preload_internal.h"
#include "range_utils.h"

/* return true if a is smaller - we prioritize smaller bin_val
 * and for same bin_val, we prioritize ending items (is_start == false)
 * first */
bool rb_item_lt(const rb_item_t& a, const rb_item_t& b) {
  return (a.bin_val < b.bin_val) ||
         ((a.bin_val == b.bin_val) && (!a.is_start && b.is_start));
}

extern preload_ctx_t pctx;

void load_bins_into_rbvec(std::vector<float>& bins,
                          std::vector<rb_item_t>& rbvec, int num_bins,
                          int num_ranks, int bins_per_rank) {
  assert(num_bins == num_ranks * bins_per_rank);

  for (int rank = 0; rank < num_ranks; rank++) {
    for (int bidx = 0; bidx < bins_per_rank - 1; bidx++) {
      float bin_start = bins[rank * bins_per_rank + bidx];
      float bin_end = bins[rank * bins_per_rank + bidx + 1];

      if (bin_start == bin_end) continue;

#ifdef RANGE_DEBUG
      fprintf(stderr, "Rank %d, bin rank %d, Range: %.1f %.1f (%lu-%d)\n",
              pctx.my_rank, rank, bin_start, bin_end, bins.size(),
              rank * bins_per_rank + bidx + 1);
#endif

      rbvec.push_back({rank, bin_start, bin_end, true});
      rbvec.push_back({rank, bin_end, bin_start, false});
    }
  }
  std::sort(rbvec.begin(), rbvec.end(), rb_item_lt);
}

void pivot_union(std::vector<rb_item_t> rb_items,
                 std::vector<float>& unified_bins,
                 std::vector<float>& unified_bin_counts,
                 std::vector<float>& rank_bin_widths, int num_ranks) {
  assert(rb_items.size() >= 2u);

  float rank_bin_start[num_ranks];
  float rank_bin_end[num_ranks];

  const float BIN_EMPTY = rb_items[0].bin_val - 10;

  std::fill(rank_bin_start, rank_bin_start + num_ranks, BIN_EMPTY);

  int prev_bin_val = rb_items[0].bin_val;
  int prev_bp_bin_val = rb_items[0].bin_val;

  /* Ranks that currently have an active bin */
  std::list<int> active_ranks;

  int cur_bin_count = 0;
  int rb_item_sz = rb_items.size();

  for (int i = 0; i < rb_items.size(); i++) {
    int bp_rank = rb_items[i].rank;
    float bp_bin_val = rb_items[i].bin_val;
    float bp_bin_other = rb_items[i].bin_other;
    bool bp_is_start = rb_items[i].is_start;

#ifdef RANGE_DEBUG
    fprintf(stderr, "Rank %d/%d - bin %.1f\n", -1, i, bp_bin_val);
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
        float rank_contrib =
            rank_bin_widths[rank] * rank_left_range / rank_total_range;

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
          fprintf(stderr,
                  "PIVOT_CALC ASSERT_FAIL"
                  "Old Len: %d, New Len: %d\n",
                  old_len, new_len + 1);
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

int resample_bins_irregular(const std::vector<float>& bins,
                            const std::vector<float>& bin_counts,
                            std::vector<float>& samples, int nsamples) {
  const int bins_size = bins.size();
  const int bin_counts_size = bin_counts.size();

  samples.resize(nsamples);

  assert(bins_size == bin_counts_size + 1);
  assert(nsamples >= 2);
  assert(bins_size >= 2);

  float nparticles =
      std::accumulate(bin_counts.begin(), bin_counts.end(), 0.0f);

  const float part_per_rbin = nparticles * 1.0 / (nsamples - 1);

  int sidx = 1;

  float accumulated = 0;
  for (int rbidx = 0; rbidx < bins_size - 1; rbidx++) {
    float rcount_total = bin_counts[rbidx];

    float rbin_start = bins[rbidx];
    float rbin_end = bins[rbidx + 1];
    while (accumulated + rcount_total > part_per_rbin - RANGE_EPSILON) {
      float rcount_cur = part_per_rbin - accumulated;
      accumulated = 0;

      float rbin_cur =
          (rcount_cur / rcount_total) * (rbin_end - rbin_start) + rbin_start;

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
    logf(LOG_ERRO,
         "rank %d,sidx expected to be equal to nsamples, %d-%d, accumulated: "
         "%.1f\n",
         pctx.my_rank, sidx, nsamples, accumulated);
  }

  assert(sidx == nsamples);

  return 0;
}

void repartition_bin_counts(std::vector<float>& old_bins,
                            std::vector<float>& old_bin_counts,
                            std::vector<float>& new_bins,
                            std::vector<float>& new_bin_counts) {
  int ob_sz = old_bins.size() - 1;
  int nb_sz = new_bins.size() - 1;
  new_bin_counts.resize(new_bins.size() - 1);
  std::fill(new_bin_counts.begin(), new_bin_counts.end(), 0);

  int oidx = 0;
  int nidx = 0;

  float nbin_sum = 0;
  float obin_left = old_bin_counts[0];

  while (1) {
    if (oidx == ob_sz) {
      break;
    }

    if (nidx == nb_sz) {
      break;
    }

    float obs = old_bins[oidx];      // old bin start
    float obe = old_bins[oidx + 1];  // old bin end
    float obw = obe - obs;           // old bin width

    float obc = old_bin_counts[oidx];  // old bin count

    float nbs = new_bins[nidx];      // new bin start
    float nbe = new_bins[nidx + 1];  // new bin end
    float nbw = nbe - nbs;           // new bin width

    if (obe <= nbs) {
      /* no overlap between ob and nb */
      oidx++;
      continue;
    }

    if (nbe <= obs) {
      /* zero count for current nbin */
      new_bin_counts[nidx] = nbin_sum;
      nbin_sum = 0;
      nidx++;
      continue;
    }

    // types of overlap
    if (obs <= nbs && obe <= nbe) {
      /* left overlap */
      nbin_sum += (obe - nbs) * obc / obw;
      oidx++;
    } else if (obs <= nbs && obe > nbe) {
      /* ob engulfs nb */
      nbin_sum += (nbw * obc / obw);
      new_bin_counts[nidx] = nbin_sum;
      nbin_sum = 0;
      nidx++;
    } else if (obs > nbs && obe <= nbe) {
      /* nb engulfs ob */
      nbin_sum += obc;
      oidx++;
    } else {
      /* right overlap */
      nbin_sum += (nbe - obs) * obc / obw;
      new_bin_counts[nidx] = nbin_sum;
      nbin_sum = 0;
      nidx++;
    }
  }

  if (nidx < nb_sz) new_bin_counts[nidx] = nbin_sum;

#ifdef RANGE_PARANOID_CHECKS
  float osum_temp =
      std::accumulate(old_bin_counts.begin(), old_bin_counts.end(), 0.f);
  float nsum_temp =
      std::accumulate(new_bin_counts.begin(), new_bin_counts.end(), 0.f);

  assert(fabs(osum_temp - nsum_temp) < 1e-3);
#endif

  return;
}
