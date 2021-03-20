//
// Created by Ankush J on 3/5/21.
//

#include "carp.h"

#include "rtp.h"

namespace pdlfs {
namespace carp {
Status Carp::Serialize(const char* fname, unsigned char fname_len, char* data,
                       unsigned char data_len, unsigned char extra_data_len,
                       particle_mem_t& p) {
  Status s = Status::OK();

  float indexed_prop = GetIndexedAttr(data, data_len);

  char data_reorg[255];
  memcpy(data_reorg, fname, fname_len);
  memcpy(data_reorg + fname_len, data, data_len);

  p.buf_sz = msgfmt_write_data(
      p.buf, 255, reinterpret_cast<char*>(&indexed_prop), sizeof(float),
      data_reorg, fname_len + data_len, extra_data_len);

  p.indexed_prop = indexed_prop;
  /* XXX: breaking msgfmt abstraction; fix */
  p.data_ptr = p.buf + fname_len + 2;
  p.data_sz = fname_len + data_len;

  logf(LOG_DBG2, "shuffle_write, bufsz: %d\n", p.buf_sz);
  return s;
}

Status Carp::AttemptBuffer(particle_mem_t& p, bool& shuffle, bool& flush) {
  MutexLock ml(&mutex_);
  Status s = Status::OK();

  bool can_buf = policy_->BufferInOob(p);
  /* shuffle = true if we can't buffer */
  shuffle = !can_buf;

  if (can_buf) {
    /* XXX: check RV */
    int rv = oob_buffer_.Insert(p);
    if (rv < 0) {
      logf(LOG_INFO, "OOB Insert failed: %d\n", options_.my_rank);
    }
  }

  /* reneg = true iff
   * 1. Trigger returns true
   * 2. OOB is full
   * 3. Reneg is ongoing
   */

  bool reneg_ongoing = (mts_mgr_.GetState() != MainThreadState::MT_READY);
  bool reneg = policy_->TriggerReneg() || reneg_ongoing;

  /* Attempt a flush if reneg happened */
  flush = reneg;

  if (reneg) {
    rtp_.InitRound();
    MarkFlushableBufferedItems();
  }

  if (shuffle) {
    AssignShuffleTarget(p);
    if (p.shuffle_dest == -1) {
      ABORT("Invalid shuffle target");
    }
  }

  return s;
}
}  // namespace carp
}  // namespace pdlfs
