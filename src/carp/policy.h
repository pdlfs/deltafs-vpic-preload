//
// Created by Ankush J on 3/5/21.
//

#pragma once

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

#include "common.h"
#include "oob_buffer.h"

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
  virtual int ComputeShuffleTarget(particle_mem_t& p, int& rank) = 0;

 protected:
  bool FirstRenegCompleted();

  void Reset();

  bool IsOobFull();

  const CarpOptions& options_;
  Carp& carp_;

  const uint64_t invoke_intvl_;
  uint32_t epoch_;
  uint64_t num_writes_;
};

/* InvocationIntraEpoch: triggers every reneg_intvl writes
 * OR if OOB buffer is full
 */
class InvocationIntraEpoch : public InvocationPolicy {
 public:
  InvocationIntraEpoch(Carp& carp, const CarpOptions& options);

  bool TriggerReneg() override;

  void AdvanceEpoch() override {
    Reset();
    epoch_++;
    num_writes_ = 0;
  }

  int ComputeShuffleTarget(particle_mem_t& p, int& rank) override;
};

/* InvocationInterEpoch: triggers ONCE every reneg_intvl epochs
 * buffer in OOB until first reneg of the epoch
 *
 * This policy only triggers on Rank 0. If a reneg is to be
 * triggered in the current epoch, writes for that epoch will be
 * buffered in OOB buffers on all ranks until the OOB on Rank 0
 * fills up and triggers a reneg. OOBs on other ranks may be in
 * overflow state by then.
 *
 * Once a reneg has been triggered, a shuffle target will always be
 * returned, and no rank will be asked to buffer any particle in an
 * OobBuffer
 */
class InvocationInterEpoch : public InvocationPolicy {
 public:
  InvocationInterEpoch(Carp& carp, const CarpOptions& options)
      : InvocationPolicy(carp, options), reneg_triggered_(false) {}

  bool BufferInOob(particle_mem_t& p) override {
    // reneg_triggered_ reflects this rank's policy decision
    // FirstRenegCompleted() checks whether any rank triggered a reneg
    return !FirstRenegCompleted();
  }

  bool TriggerReneg() override;

  void AdvanceEpoch() override {
    epoch_++;

    // epochs are 1-indexed, comparison is 0-indexed
    if ((epoch_ - 1) % invoke_intvl_ == 0) {
      Reset();
      reneg_triggered_ = false;
    }
  }

  int ComputeShuffleTarget(particle_mem_t& p, int& rank) override;

 private:
  bool reneg_triggered_;
};
}  // namespace carp
}  // namespace pdlfs
