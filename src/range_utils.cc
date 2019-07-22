#include <assert.h>
#include <string.h>
#include <limits.h>
#include <stdio.h>
#include <algorithm>
#include <list>

float bins[] = {1, 3, 5, 2, 4, 6};
std::vector<rb_item_t> bs;

std::vector<float> ubins;
std::vector<float> ubin_count;
std::vector<float> samples;

bool ri_lt(const rb_item_t& a, const rb_item_t& b) {
  return (a.bin_val < b.bin_val) || (a.bin_val == b.bin_val && !a.is_start);
}

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

void fun() {
  for (int rank = 0; rank < 2; rank++) {
    for (int bidx = 0; bidx < 2; bidx++) {
      fprintf(stdout, "Rank %d, Start: %f\n", rank, bins[rank * 3 + bidx]);
      fprintf(stdout, "Rank %d, End: %f\n", rank, bins[rank * 3 + bidx + 1]);
      float bin_start = bins[rank * 3 + bidx];
      float bin_end = bins[rank * 3 + bidx + 1];
      bs.push_back({rank, bin_start, bin_end, true});
      bs.push_back({rank, bin_end, bin_start, false});
    }
  }
  std::sort(bs.begin(), bs.end(), ri_lt);
}

void pivot_union(std::vector<rb_item_t> rb_items,
                 std::vector<float> unified_bins,
                 std::vector<float> unified_bin_counts, int num_ranks,
                 int part_per_rank) {
  float rank_bin_start[num_ranks];
  float rank_bin_end[num_ranks];

  const float BIN_EMPTY = rb_items[0].bin_val - 10;

  std::fill(rank_bin_start, rank_bin_start + num_ranks, BIN_EMPTY);

  int prev_bin_val = bs[0].bin_val;
  int prev_bp_bin_val = bs[0].bin_val;

  /* Ranks that currently have an active bin */
  std::list<int> active_ranks;

  int cur_bin_count = 0;
  int rb_item_sz = rb_items.size();

  for (int i = 0; i < rb_items.size(); i++) {
    int bp_rank = rb_items[i].rank;
    float bp_bin_val = rb_items[i].bin_val;
    float bp_bin_other = rb_items[i].bin_other;
    bool bp_is_start = rb_items[i].is_start;

    fprintf(stderr, "Currently processing: Rank %d, Bin %f, %s\n", bp_rank,
            bp_bin_val, bp_is_start ? "START" : "END");

    std::list<int>::iterator remove_item;
    /* Can't set iterators to null apparently */
    bool remove_item_flag = false;

    if (bp_bin_val != prev_bin_val) {
      /* Breaking point - we break all active intervals
       * at this point and put them into a new bin */
      float cur_bin = bp_bin_val; float cur_bin_count = 0;

      auto lit = active_ranks.begin();
      auto lend = active_ranks.end();

      while (lit != lend) {
        int rank = (*lit);
        fprintf(stderr, "%d\n", rank);
        assert(rank_bin_start[rank] != BIN_EMPTY);

        float rank_total_range = rank_bin_end[rank] - rank_bin_start[rank];
        float rank_left_range = cur_bin - prev_bp_bin_val;
        float rank_contrib = part_per_rank * rank_left_range / rank_total_range;

        cur_bin_count += rank_contrib;

        fprintf(stdout, "Cur Bin: %0.1f, Rank: %0.1f %0.1f\n", cur_bin,
                rank_bin_start[rank], rank_bin_end[rank]);
        fprintf(stderr, "Rank: %d, contrib: %.1f\n", rank, rank_contrib);

        if (rank == bp_rank) {
          /* If you come across the same interval in active_ranks,
           * you must be the closing end of the rank */
          assert(!bp_is_start);
          remove_item = lit;
          remove_item_flag = true;
        }

        lit++;
      }

      ubin_count.push_back(cur_bin_count);
      ubins.push_back(cur_bin);

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
      if (i == 0) ubins.push_back(bp_bin_val);
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

        fprintf(stdout, "Old len: %d, new len: %d\n", old_len, new_len);
        assert(old_len == new_len + 1);
      }
    }

    fprintf(stdout, "Active rank size ITE: %zu\n", active_ranks.size());
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
                            std::vector<float>& samples, int nsamples,
                            int nparticles) {
  const int bins_size = bins.size();
  const int bin_counts_size = bin_counts.size();

  samples.resize(nsamples);

  assert(bins_size == bin_counts_size + 1);

#ifdef RANGE_PARANOID_CHECKS
  assert(nsamples > 2);
  assert(bins_size > 2);
  int bin_sum = std::accumulate(bin_counts.begin(), bin_counts.end(), 0);
  assert(bin_sum == nparticles);
#endif

  const float part_per_rbin = nparticles * 1.0 / (nsamples - 1);

  samples[0] = bins[0];
  samples[nsamples - 1] = bins[bins_size - 1];

  fprintf(stderr, "PPB: %f\n", part_per_rbin);
  int sidx = 1;

  float accumulated = 0;
  for (int rbidx = 0; rbidx < bins_size - 1; rbidx++) {
    float rcount_total = bin_counts[rbidx];
    fprintf(stderr, "Accumulated: %f\n", accumulated);

    float rbin_start = bins[rbidx];
    float rbin_end = bins[rbidx + 1];

    while (accumulated + rcount_total > part_per_rbin - RANGE_EPSILON) {
      fprintf(stdout, "Accumulated Prev: %f, Cur left: %f\n", accumulated,
              rcount_total);
      float rcount_cur = part_per_rbin - accumulated;
      accumulated = 0;

      fprintf(stdout, "Rcount cur: %f, tot: %f\n", rcount_cur, rcount_total);
      fprintf(stdout, "Rbin st: %f, end: %f\n", rbin_start, rbin_end);

      float rbin_cur =
          (rcount_cur / rcount_total) * (rbin_end - rbin_start) + rbin_start;

      samples[sidx] = rbin_cur;
      fprintf(stderr, "Sample: %f\n", rbin_cur);
      sidx++;

      rcount_total -= rcount_cur;
      rbin_start = rbin_cur;
    }

    accumulated += rcount_total;
  }

  fprintf(stdout, "Final accumulated ---> %f\n", accumulated);

#ifdef PARANOID_CHECKS
  assert(sidx == nsamples - 1);
#endif

  return 0;
}

int main() {
  fun();
  pbs();
  pivot_union(bs, ubins, ubin_count, 2, 20);
  pub();
  resample_bins_irregular(ubins, ubin_count, samples, 10, 80);

  fprintf(stdout, "------------------\n");
  for (int i = 0; i < samples.size(); i++) {
    printf("Sample - %f\n", samples[i]);
  }
  return 0;
}
