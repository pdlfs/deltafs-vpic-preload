//
// Created by Ankush J on 3/5/21.
//

#include "carp.h"

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

Status Carp::AttemptBuffer(particle_mem_t& p, bool& shuffle) {
  Status s = Status::OK();
  mutex_.Lock();

  bool can_buf = oob_buffer_.OutOfBounds(p.indexed_prop);
  /* shuffle = true if we can't buffer */
  shuffle = !can_buf;

  if (can_buf) {
    /* XXX: check RV */
    oob_buffer_.Insert(p);
  }

  /* reneg = true iff
   * 1. Trigger returns true
   * 2. OOB is full
   * 3. Reneg is ongoing
   */

  bool reneg_ongoing = (mts_mgr_.get_state() != MainThreadState::MT_READY);
  bool reneg =
      policy_->TriggerReneg() || oob_buffer_.IsFull() || reneg_ongoing;

  if (reneg) {
    rtp_init_round(NULL);
    MarkFlushableBufferedItems();
  }

  if (shuffle) {
    AssignShuffleTarget(p);
    if (p.shuffle_dest == -1) {
      ABORT("Invalid shuffle target");
    }
  }

  mutex_.Unlock();
  return s;
}
}  // namespace carp
}  // namespace pdlfs