#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <list>

#include "preload_internal.h"

#include "pivot_buffer.h"
#include "range_utils.h"

#define RANGE_DEBUG_T 1

extern preload_ctx_t pctx;

/*
 * union bounds and weights into a binhistogram
 */
void pivot_union(std::vector<bounds_t> bounds,
                 std::vector<double>& weights,
                 size_t ninputs,
                 pdlfs::carp::BinHistogram<double,float>& mergedhist) {
  assert(weights.size() && ninputs == weights.size() && bounds.size() >= 2);
  assert(bounds[0].is_start);
  std::list<int> active;
  double active_size[ninputs];     /* valid only if in active */
  double starting_point = bounds[0].b_value;

  /* track prev value and prev value that was different than current val */
  double prev_val = starting_point;
  double prev_different_val = starting_point;
  mergedhist.InitForExtend(starting_point);    /* bins start at smallest val */

  for (size_t i = 0 ; i < bounds.size() ; i++) {
    assert(bounds[i].b_value >= prev_val);

    int current_uidx = bounds[i].pbidx;
    double current_val = bounds[i].b_value;
    bool deactivated = false;   /* set if we deactivated bounds[i] */

    if (current_val != prev_val) {  /* advance?  finish bin in union */
      float bin_weight = 0.0;

      /* each active item contributes to the new bin_weight */
      std::list<int>::iterator lit = active.begin();
      while (lit != active.end()) {
        int luidx = *lit;
        double frac = (current_val - prev_different_val) / active_size[luidx];
        float contrib = weights[luidx] * frac;
        assert(contrib >= 0.0);
        bin_weight += contrib;
        if (bounds[i].pbidx == luidx) {
          assert(bounds[i].is_start == false);    /* since already active */
          lit = active.erase(lit);                /* advance and remove now */
          deactivated = true;
        } else {
          lit++;
        }
      }
      mergedhist.Extend(current_val, bin_weight);
      prev_different_val = current_val;        /* start of next bin in union */
    }

    if (bounds[i].is_start) {      /* start?  activate current_uidx */
      active_size[current_uidx] = bounds[i].b_far_end - bounds[i].b_value;
      active.push_back(current_uidx);
    } else {                       /* end?  deactivate current_uidx */
      active_size[current_uidx] = 0;
      if (!deactivated) {          /* only if we haven't already done it */
        size_t old_size = active.size();
        active.remove(current_uidx);
        if (active.size() + 1 != old_size) {
          assert(0);
        }
      }
    }

    prev_val = current_val;
  }
}
