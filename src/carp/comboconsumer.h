#include <vector>

#include "binhistogram.h"
#include "oob_buffer.h"
#include "range.h"

#include "binhistconsumer.h"
#include "floatvecconsumer.h"

namespace pdlfs {
namespace carp {

/*
 * ComboConsumer: treat oob buffer and binhistogram as a combined
 * sorted and weighted set of index values.  at construction time
 * we point to the smallest value in the combined index value range.
 * we advance through the index by consuming weight in order (left,
 * middle, right).  when all the weight is consumed we point to the
 * end of the indexed value range.   this allows CARP to create a
 * set of ~equal weight pivots from the oob buffer and histogram.
 * we use a FloatVecConsumer for the oob parts (left, right) and
 * a BinHistConsumer for the binhistogram in the middle (those
 * objects will handle interpolation related issues for us).
 * when an oob buffer is provided (it can be NULL), the weight of a
 * particle is always 1.  generally, weight values must be >= 0 and
 * zero-width bins cannot contain particles.   the bins and oob buffer
 * used should be externally locked during our lifespan (we keep an
 * internal copy of the pointer to the bins for access).
 */
template <typename BT, typename WT> class ComboConsumer {
 public:
  ComboConsumer(BinHistogram<BT,WT>* bins, OobBuffer* oob) : bincons_(bins),
                leftcons_(NULL), rightcons_(NULL), cursrc_(NONE) {
    if (oob) {
      oob->GetPartitionedProps(oob_left_, oob_right_);
      leftcons_.SetVec(&oob_left_);
      rightcons_.SetVec(&oob_right_);
      leftresid_ = leftcons_.RemainingWeight();
      rightresid_ = rightcons_.RemainingWeight();
    } else {
      leftresid_ = rightresid_ = 0.0;
    }
    middleresid_ = bincons_.RemainingWeight();
    wtotal_ = leftresid_ + middleresid_ + rightresid_;

    if (wtotal_) {   /* compute our range if we have something */
      double rstart, rend;

      if (oob_left_.size())            /* get rstart */
        rstart = leftcons_.GetRange().rmin();
      else if (bins && bins->Size())
        rstart = bincons_.GetRange().rmin();
      else
        rstart = rightcons_.GetRange().rmin();

      if (oob_right_.size())           /* get rend */
        rend = rightcons_.GetRange().rmax();
      else if (bins && bins->Size())
        rend = bincons_.GetRange().rmax();
      else
        rend = leftcons_.GetRange().rmax();

      range_.Set(rstart, rend);
    }

    advance_to_next_source();          /* from NONE */
  }

  /*
   * some helpful accessor functions
   */
  WT TotalWeight() { return wtotal_; }           /* total weight, >= 0 */
  Range GetRange() { return range_; }            /* range of combined data */
  double RemainingWeight() {                     /* remaining weight */
    return leftresid_ + middleresid_ + rightresid_;
  }

  /*
   * return current indexed value.  varies over our range_ based on
   * how much weight we've consumed.
   */
  double CurrentValue() {
    if (cursrc_ == LEFT)
      return leftcons_.CurrentValue();
    if (cursrc_ == MIDDLE)
      return bincons_.CurrentValue();
    if (cursrc_ == RIGHT)
      return rightcons_.CurrentValue();
    return range_.rmax();   /* at end (or no data) */
  }

  /*
   * consume to desired value (no-op if already past desired value)
   */
  double ConsumeWeightTo(double desired_remaining) {
    double remaining = this->RemainingWeight();
    if (remaining <= desired_remaining) /* already at or past desired value? */
      return remaining;
    return this->ConsumeWeight(remaining - desired_remaining);
  }

  /*
   * consume weight (advancing our location).  returns remaining weight.
   */
  double ConsumeWeight(double consume) {
    double need = consume;
    if (need && cursrc_ == LEFT) {
      double use = (need > leftresid_) ? leftresid_ : need;
      leftresid_ = leftcons_.ConsumeWeight(use);
      need -= use;
      if (leftresid_ <= 0.0)
        advance_to_next_source();
    }
    if (need && cursrc_ == MIDDLE) {
      double use = (need > middleresid_) ? middleresid_ : need;
      middleresid_ = bincons_.ConsumeWeight(use);
      need -= use;
      if (middleresid_ <= 0.0)
        advance_to_next_source();
    }
    if (need && cursrc_ == RIGHT) {
      double use = (need > rightresid_) ? rightresid_ : need;
      rightresid_ = rightcons_.ConsumeWeight(use);
      need -= use;
      if (rightresid_ <= 0.0)
        advance_to_next_source();
    }
    return this->RemainingWeight();
  }

 private:
  enum srcenum { NONE=0, LEFT=1, MIDDLE=2, RIGHT=3, DONE=4 };

  void advance_to_next_source() {
    if (cursrc_ <= LEFT && leftresid_ > 0) {
      cursrc_ = LEFT;
    } else if (cursrc_ <= MIDDLE && middleresid_ > 0) {
      leftresid_ = 0;                                  /* to make sure */
      cursrc_ = MIDDLE;
    } else if (cursrc_ <= RIGHT && rightresid_ > 0) {
      leftresid_ = middleresid_ = 0;                   /* to make sure */
      cursrc_ = RIGHT;
    } else {
      leftresid_ = middleresid_ = rightresid_ = 0;     /* to make sure */
      cursrc_ = DONE;
    }
  }

  /* data structures */
  BinHistConsumer<BT,WT> bincons_;  /* bins consumer */
  std::vector<float> oob_left_;     /* sorted oob particle keys; left side */
  std::vector<float> oob_right_;    /* sorted oob particle keys; right side */
  FloatVecConsumer leftcons_;       /* oob_left_ consumer */
  FloatVecConsumer rightcons_;      /* oob_right_ consumer */
  WT wtotal_;                       /* total weight */
  Range range_;                     /* combined range of oob+bins */
  /* the remaining fields change as we consume weight */
  double leftresid_;                /* remaining weight on left side */
  double rightresid_;               /* remaining weight on right side */
  double middleresid_;              /* remaining weight in bins */
  srcenum cursrc_;                  /* current source */
};
}  // namespace carp
}  // namespace pdlfs
