//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <assert.h>
#include <float.h>

namespace pdlfs {
namespace carp {

/*
 * range of floating point numbers (doubles).  the range is defined
 * by a min and max value (inclusive and exclusive, respectively).
 * the default/reset range is an undefined one (where we set rmin=DBL_MAX
 * and rmax=DBL_MAX).  note that a range with rmin==mrax is a zero-width
 * range (nothing can be inside of it).   ranges are safe to copy.
 * owner must provide locking.
 */
class Range {
 public:
  Range() : rmin_(DBL_MAX), rmax_(DBL_MIN) {}    /* unset */

  Range(double rmin, double rmax) : rmin_(rmin), rmax_(rmax) {}

  Range(const Range& other) = default;

  /* accessor functions */
  double rmin() const { return rmin_; }
  double rmax() const { return rmax_; }

  void Reset() { /* reset everything to a clean unset state */
    rmin_ = DBL_MAX;
    rmax_ = DBL_MIN;
  }

  bool IsSet() const { /* is range set? */
    return (rmin_ != DBL_MAX) and (rmax_ != DBL_MIN);
  }

  void Set(double qr_min, double qr_max) { /* set a new range */
    assert(qr_min <= qr_max);
    rmin_ = qr_min;
    rmax_ = qr_max;
  }

  /* is "f" inside our range? returns false for unset ranges */
  virtual bool Inside(double f) const { return (f >= rmin_ && f < rmax_); }

  /* Return the relative position of a point relative to the range */
  /* -1: to left. 0: inside. +1: to right. */
  int GetRelPos(double f) {
    if (!IsSet() || f < rmin_)
      return -1;
    if (f >= rmax_)
      return 1;
    return 0;
  }

 protected:
  double rmin_;        /* start of range (inclusive) */
  double rmax_;        /* end of range (exclusive) */
};

}  // namespace carp
}  // namespace pdlfs
