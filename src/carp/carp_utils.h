#pragma once

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#include <vector>

#include "carp/oob_buffer.h"
#include "range_constants.h"

namespace pdlfs {
namespace carp {

/* forward declaration */
class Carp;

class PivotUtils {
 public:
  template <typename T>
  static std::string SerializeVector(std::vector<T>& v);

  template <typename T>
  static std::string SerializeVector(T* v, size_t vsz);
  /**
   * @brief Calculate pivots from the current pivot_ctx state.
   * This also modifies OOB buffers (sorts them), but their order shouldn't
   * be relied upon anyway.
   *
   * SAFE version computes "token pivots" in case no mass is there to
   * actually compute pivots. This ensures that merging calculations
   * do not fail.
   *
   * XXX: a more semantically appropriate fix would be to define addition
   * and resampling for zero-pivots
   *
   * @param carp pivot context
   *
   * @return
   */
  static int CalculatePivots(Carp* carp, size_t num_pivots);

  /**
   * @brief Update pivots after renegotiation. This *does not* manipulate the
   * state manager. State manager needs to be directly controlled by the
   * renegotiation provider because of synchronization implications
   *
   * @param carp
   * @param pivots
   * @param num_pivots
   * @return
   */
  static int UpdatePivots(Carp* carp, double* pivots, int num_pivots);

 private:
  static int CalculatePivotsFromOob(Carp* carp, int num_pivots);

  static int CalculatePivotsFromAll(Carp* carp, int num_pivots);

  static int GetRangeBounds(Carp* carp, std::vector<float>& oobl,
                            std::vector<float>& oobr, float& range_start,
                            float& range_end);
};
}  // namespace carp
}  // namespace pdlfs
