#include <assert.h>
#include <math.h>

#include <vector>

#include "range.h"

namespace pdlfs {
namespace carp {

/*
 * FloatVecConsumer: we are given a vector of floats of size "w" ...
 * w is the total weight of the vector (each entry has weight of 1.0).
 * in CARP, the vector is a sorted list of float keys from the oob buffer.
 * we start at the first entry in the vector and as we consume weight we
 * move towards the end of the vector (which we reach when we've consumed
 * "w" units of weight).  so our value ranges from the first entry of the
 * vector (with no weight consumed) to the nextafterf() of the last entry
 * (with all weight consumed).  this should match the behavior of the old
 * CalculatePivotsFromOob().  if we are between two entries in the array
 * we interpolate to compute our value.
 *
 * The vector should be locked/protected during our lifetime.
 */

class FloatVecConsumer {
 public:
  FloatVecConsumer(std::vector<float>* vec) {
    this->SetVec(vec);
  }

  void SetVec(std::vector<float>* vec) {
    vec_ = vec;
    size_ = (vec) ? vec->size() : 0;
    if (size_) {
      float rend = nextafterf(vec->back(), HUGE_VALF);
      assert(rend != HUGE_VALF);  // overflow?
      range_.Set(vec->front(), rend);
      scale_ = (size_ - 1.0) / (double)(size_);
      curval_ = vec->front();
    } else {
      range_.Reset();
      scale_ = 0;
      curval_ = 0;
    }
    curloc_ = 0;
  }

  /* some helpful accessor functions */
  size_t TotalWeight() { return size_; }               /* total weight, >= 0 */
  Range GetRange() { return range_; }                  /* our range */
  double RemainingWeight() { return size_ - curloc_; } /* remaining weight */
  double CurrentValue() { return curval_; }            /* our current value */

  /* consume to desired value (no-op if already past desired value) */
  double ConsumeWeightTo(double desired_remaining) {
    double remaining = this->RemainingWeight();
    if (remaining <= desired_remaining) /* already at or past desired value? */
      return remaining;
    return this->ConsumeWeight(remaining - desired_remaining);
  }

  double ConsumeWeight(double consume) {
    curloc_ += consume;         /* consume it now! */
    if (curloc_ >= size_) {     /* special case: use range max at the end */
      curval_ = (size_) ? range_.rmax() : 0;
      curloc_ = size_;
      return(0.0);
    }

    /*
     * update curval_ by taking the amount of weight we've now consumed
     * (curloc_) and scaling it an index into the vector to get the new
     * value (interpolating if we are between values in the vector).
     * [ curloc_ runs from 0 to size_ while the vector index runs from
     *   0 to size_-1, so we scale by (size_-1)/size_. ]
     */
    double idx = curloc_ * scale_;   /* convert to index */
    size_t trunc_idx = trunc(idx);
    double frac = idx - trunc_idx;   /* non-zero means we interpolate */

    if (frac == 0.0) {
      if (trunc_idx >= size_)
        curval_ = range_.rmax();     /* unlikely, be safe */
      else
        curval_ = (*vec_)[trunc_idx];
    } else {
      /* in between vector values, interpolate result */
      float a = (*vec_)[trunc_idx];
      float b = (*vec_)[trunc_idx + 1];
      curval_ = a + ( (b - a) * frac );
    }

    return(size_ - curloc_);      /* return residual weight */
  }

 private:
  FloatVecConsumer() {};          /* private to disallow */
  std::vector<float>* vec_;       /* pointer to vector we consume */
  size_t size_;                   /* size of the vector */
  Range range_;                   /* range of vector (assumes sorted) */
  double scale_;                  /* scale factor */
  /* the remaining field change as we consume entries */
  double curloc_;                 /* current location in vector */
  double curval_;                 /* value at current location */
};
}  // namespace carp
}  // namespace pdlfs
