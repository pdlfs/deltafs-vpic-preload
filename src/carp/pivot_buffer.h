/*
 * pivot_buffer.h  buffer received RTP pivots here until we can union them
 */

#pragma once

#include "carp/rtp_internal.h"
#include "range_common.h"

namespace pdlfs {
namespace carp {

/*
 * in RTP, parents in the reduction tree receive pivots from their
 * children.  parents must buffer received pivots until they have
 * received them from all of their children.  then they can retreive
 * the pivot data and perform a pivot union operation on them (to
 * either pass up the result to the next level or, if we are the root,
 * broadcast to all nodes).
 *
 * we store pivots by stage/level and round in the reduction tree.  each
 * pivot received for a given stage/level and round from a process is
 * assigned a "pivot buffer index" or "pbidx" value (0 to FANOUT_MAX)
 * in the order it is received in its round.  the pbidx is returned when
 * we extract the merged set of pivot boundaries and weights for a given
 * stage from the pivot buffer (e.g. with GetPivotWeights() and
 * LoadBounds()).
 *
 * rounds: it is possible for the next reneg operation to start
 * before the current one finishes, so we keep two sets of pivot
 * buffers -- one for the current round and one for the next round.
 * note that the reneg cannot progress further than the next round
 * as the current proc must complete its part in RTP before further
 * progress is possible.
 */

/*
 * when we use LoadBounds to extract the merged set of pivots
 * boundaries for a given stage/level in the current round, we
 * return them in a vector of bounds_t structures.  each buffered
 * pivot for a stage/level and round has its own unique pivot
 * buffer index value (pbidx).  we don't care which MPI rank a
 * pbidx maps to as long as each MPI rank has its own unique pbidx
 * value for the given stage/level of a round.
 *
 * note that bounds are either a starting value (inclusive) or an
 * ending value (exclusive) -- use "is_start" to determine which
 * type it is.   A range will produce 2 bounds_t structs: one
 * for each end.  a bin array of size "x" represents "x-1" ranges
 * and thus will produce "2*(x-1)" bounds.  Note that LoadBounds
 * can discard zero-width ranges (so those bounds won't be returned).
 */
typedef struct bounds {
  int pbidx;         /* pivot buffer index associated with this bound */
  double b_value;    /* bounds value */
  double b_far_end;  /* the value of the other end of the bounds */
  bool is_start;     /* true if b_value is a 'start' value, o.w. end */
} bounds_t;

class PivotBuffer {
 private:
  PivotBuffer() {};   /* disallow ctor w/o num_pivots[] arg */
  /* This simple storage format has 2*512KB of theoretical
   * footprint. (2* 4 * 128 * 256 * 4B). But no overhead will
   * be incurred for ranks that aren't actually using those
   * stages. (Virtual Memory ftw)
   */
  double pivot_store_[2][STAGES_MAX + 1][FANOUT_MAX][CARP_MAXPIVOTS];
  double pivot_weights_[2][STAGES_MAX + 1][FANOUT_MAX];
  int pbuf_count_[2][STAGES_MAX + 1];  /* how many currently buffered */

  int num_pivots_[STAGES_MAX + 1];     /* expected pivot count for a stage */
  int cur_round_;                      /* ptr to current round (vs next) */

 public:
  //
  // PivotBuffer constructor
  // num_pivots contains the expected pivot count for each stage
  // (configured at init time).
  //
  PivotBuffer(const int num_pivots[STAGES_MAX + 1]);

  /* accessor functions */
  int PivotCount(int stage) { return num_pivots_[stage]; }

  //
  // StoreData: Store pivots for the given round and stage.
  // isnext is true if data is for the next round, false o/w
  // returns resulting number of pivots stored for the stage and round.
  //
  int StoreData(int stage, const double* pivot_data, int dlen,
                double pivot_weight, bool isnext);

  //
  // Clear all data for current round, set next round data as cur. Returns err/0
  //
  int AdvanceRound();

  //
  // Get pivot weight for given stage in the current round.
  // weights is indexed by the pbidx values from LoadBounds().
  //
  int GetPivotWeights(int stage, std::vector<double>& weights);

  //
  // load a merged set of boundaries of all the pivots we've buffered
  // for the current round at the given stage.
  //
  int LoadBounds(int stage, std::vector<bounds_t>& boundsv);

  //
  // Clear ALL data (both current round and next). Use with caution.
  //
  int ClearAllData();
};
}  // namespace carp
}  // namespace pdlfs
