#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <list>

#include "preload_internal.h"
#include "range_utils.h"
#define RANGE_MAIN_DEBUG

/* return true if a is smaller - we prioritize smaller bin_val
 * and for same bin_val, we prioritize ending items (is_start == false)
 * first */
bool rb_item_lt(const rb_item_t& a, const rb_item_t& b) {
  return (a.bin_val < b.bin_val) ||
         ((a.bin_val == b.bin_val) && (!a.is_start && b.is_start));
}

#ifdef RANGE_MAIN_DEBUG2
void pbs() {
  for (int i = 0; i < bs.size(); i++) {
    fprintf(stderr, "(%f, %d, %c) ", bs[i].bin_val, bs[i].rank,
            bs[i].is_start ? 's' : 'e');
  }
  fprintf(stderr, "\n");
}

void pub() {
  assert(ubins.size() == ubin_count.size() + 1u);
  fprintf(stderr, "%0.1f", ubins[0]);

  for (auto i = 0; i < ubin_count.size(); i++) {
    fprintf(stderr, " - %0.1f - ", ubin_count[i]);
    fprintf(stderr, "%0.1f,  %0.1f", ubins[i + 1], ubins[i + 1]);
  }
  fprintf(stderr, "\n");
}
#endif

extern preload_ctx_t pctx;

void load_bins_into_rbvec(std::vector<float>& bins,
                          std::vector<rb_item_t>& rbvec, int num_bins,
                          int num_ranks, int bins_per_rank) {
  assert(num_bins == num_ranks * bins_per_rank);

  for (int rank = 0; rank < num_ranks; rank++) {
    for (int bidx = 0; bidx < bins_per_rank - 1; bidx++) {
      float bin_start = bins[rank * bins_per_rank + bidx];
      float bin_end = bins[rank * bins_per_rank + bidx + 1];

      fprintf(stderr, "Rank %d, Range: %.1f %.1f (%lu-%d)\n", rank, bin_start,
              bin_end, bins.size(), rank * bins_per_rank + bidx + 1);

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

    // fprintf(stderr, "Rank %d/%d - bin %.1f\n", -1, i, bp_bin_val);

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
        // float rank_contrib = part_per_rank * rank_left_range /
        // rank_total_range;
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

#ifdef RANGE_PARANOID_CHECKS
  assert(nsamples > 2);
  assert(bins_size > 2);
  // int bin_sum = std::accumulate(bin_counts.begin(), bin_counts.end(), 0);
  // assert(bin_sum == nparticles);
#endif
  // for (int i = 0; i < bin_counts.size(); i++) {
  // fprintf(stderr, "UnitedBinsOfNegotiation: %.1f - %.1f\t%.1f\n", bins[i],
  // bins[i + 1], bin_counts[i]);
  // }
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

    int obc = old_bin_counts[oidx];  // old bin count

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
  // fprintf(stderr, "Case 4: nbinprev: %.1f\n", nbin_sum);
  // fprintf(stdout, "(%.1f %.1f) (%.1f %.1f)\n", obs, obe, nbs, nbe);

  if (nidx < nb_sz - 1) new_bin_counts[nidx] = nbin_sum;
}

#ifdef RANGE_MAIN_DEBUG
// float bins[] = {1, 3, 5, 2, 4, 6};
// std::vector<rb_item_t> bs;

// std::vector<float> ubins;
// std::vector<float> ubin_count;
// std::vector<float> samples;

// int main2() {
// load_bins_into_rbvec(bins, bs, 6, 2, 3);
// pbs();
// pivot_union(bs, ubins, ubin_count, 2, 20);
// fprintf(stderr, "Ubin: %zu %zu\n", ubins.size(), ubin_count.size());
// pub();
// resample_bins_irregular(ubins, ubin_count, samples, 10, 80);

// fprintf(stdout, "------------------\n");
// for (int i = 0; i < samples.size(); i++) {
// printf("Sample - %f\n", samples[i]);
// }
// return 0;
// }

int main() {
  std::vector<float> v1 = {1, 9.5, 16.1};
  std::vector<float> v1c = {4, 4};
  std::vector<float> v2 = {1, 16.9, 32.5};
  std::vector<float> v2c;
  repartition_bin_counts(v1, v1c, v2, v2c);

  printf("V2count: ");
  for (int i = 0; i < v2c.size(); i++) {
    printf("%.1f ", v2c[i]);
  }
  printf("\n");
  return 0;
}
#endif
