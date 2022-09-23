//
// Created by Ankush J on 3/5/21.
//

#pragma once

#include <stdint.h>
#include <stdio.h>

#include "oob_buffer.h"
#include "stat_trigger.h"

namespace pdlfs {
namespace carp {
class Carp;
class CarpOptions;
class InvocationPolicy {
 public:
  InvocationPolicy(Carp& carp, const CarpOptions& options);
  virtual bool BufferInOob(particle_mem_t& p);
  virtual bool TriggerReneg() = 0;
  virtual void AdvanceEpoch() = 0;
  virtual int ComputeShuffleTarget(particle_mem_t& p) = 0;

 protected:
  bool FirstRenegCompleted();

  void Reset();

  int ComputeShuffleTarget(particle_mem_t& p, int& rank);

  bool IsOobFull();

  uint32_t epoch_;
  uint64_t num_writes_;
  Carp& carp_;
  const CarpOptions& options_;
};

class InvocationPeriodic : public InvocationPolicy {
 public:
  InvocationPeriodic(Carp& carp, const CarpOptions& options);

  bool TriggerReneg() override;

  void AdvanceEpoch() override {
    Reset();
    epoch_++;
    num_writes_ = 0;
  }

  int ComputeShuffleTarget(particle_mem_t& p) override {
    int rank;
    InvocationPolicy::ComputeShuffleTarget(p, rank);
    return rank;
  }

 private:
  const uint64_t invoke_intvl_;
};

class InvocationDynamic : public InvocationPeriodic {
 public:
  InvocationDynamic(Carp& carp, const CarpOptions& options);

  bool TriggerReneg() override;

  void AdvanceEpoch() override;

 private:
  StatTrigger stat_trigger_;
};

class InvocationPerEpoch : public InvocationPolicy {
 public:
  InvocationPerEpoch(Carp& carp, const CarpOptions& options)
      : InvocationPolicy(carp, options), reneg_triggered_(false) {}

  bool BufferInOob(particle_mem_t& p) override { return !reneg_triggered_; }

  bool TriggerReneg() override;

  void AdvanceEpoch() override {
    Reset();
    epoch_++;
    reneg_triggered_ = false;
  }

  int ComputeShuffleTarget(particle_mem_t& p) override;

 private:
  bool reneg_triggered_;
};

class InvocationOnce : public InvocationPerEpoch {
 public:
  InvocationOnce(Carp& carp, const CarpOptions& options)
      : InvocationPerEpoch(carp, options) {}
  // Don't Reset
  void AdvanceEpoch() override { epoch_++; }
};
}  // namespace carp
}  // namespace pdlfs
