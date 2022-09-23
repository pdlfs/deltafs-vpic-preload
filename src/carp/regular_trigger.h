//
// Created by Ankush J on 1/19/21.
//

#pragma once

class RegularTrigger {
 public:
  RegularTrigger(int trigger_intvl, int my_rank)
      : trigger_intvl_(trigger_intvl), my_rank_(my_rank), cur_count_(0) {}

  void Invoke() {
    cur_count_++;

    if (cur_count_ == trigger_intvl_) {
      cur_count_ = 0;
    }
  }

  void Reset() { cur_count_ = 0; }

 private:
  const int trigger_intvl_;
  int my_rank_;
  int cur_count_;
};
