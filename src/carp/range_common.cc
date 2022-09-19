#include "range_common.h"

#include <math.h>

#include <algorithm>
#include <numeric>

#include "common.h"
#include "preload_internal.h"

/**** FOR DEBUGGING ****/
#define PRINTBUF_LEN 16384

static char rs_pb_buf[16384];
static char rs_pb_buf2[16384];
static char rs_pbin_buf[16384];

static char* print_vec(char* buf, int buf_len, float* v, int vlen) {
  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.1f ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

static char* print_vec(char* buf, int buf_len, std::vector<float>& v,
                       int vlen) {
  assert(v.size() >= vlen);

  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.2f, ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

/* local functions */

/* return true if a is smaller - we prioritize smaller bin_val
 * and for same bin_val, we prioritize ending items (is_start == false)
 * first */
bool rb_item_lt(const rb_item_t& a, const rb_item_t& b);

namespace {
bool pmt_comp(const pdlfs::carp::particle_mem_t& a, const pdlfs::carp::particle_mem_t& b) {
  return a.indexed_prop < b.indexed_prop;
}
}  // namespace

MainThreadStateMgr::MainThreadStateMgr()
    : current_state_{MT_INIT}, prev_state_{MT_INIT}, first_block_(true) {};

MainThreadState MainThreadStateMgr::GetState() { return this->current_state_; }

MainThreadState MainThreadStateMgr::GetPrevState() {
  return this->prev_state_;
}

MainThreadState MainThreadStateMgr::UpdateState(MainThreadState new_state) {
  MainThreadState cur_state = this->current_state_;

#define IS_TRANS(a, b) \
  (cur_state == (MainThreadState::a) && new_state == (MainThreadState::b))
  if (IS_TRANS(MT_INIT, MT_READY)) {
    // accept
  } else if (IS_TRANS(MT_READY, MT_READYBLOCK)) {
    // accept
  } else if (IS_TRANS(MT_READYBLOCK, MT_BLOCK)) {
    // accept
  } else if (IS_TRANS(MT_READY, MT_BLOCK)) {
    // accept
  } else if (IS_TRANS(MT_BLOCK, MT_READY)) {
    first_block_ = false;
    // accept
  } else if (IS_TRANS(MT_BLOCK, MT_REMAIN_BLOCKED)) {
    first_block_ = false;
    // indicates that next round has already started
    // same as allowing a transition from MT_BLOCK to MT_BLOCK
    // but making it explicit helps us catch more errors
    // accept
  } else if (IS_TRANS(MT_REMAIN_BLOCKED, MT_BLOCK)) {
    // accept
  } else {
    logf(LOG_ERRO, "UpdateState @ R%d: %d to %d", pctx.my_rank, cur_state,
         new_state);
    ABORT("MainThreadStateMgr::UpdateState: unexpected transition");
  }
#undef IS_TRANS

  this->prev_state_ = this->current_state_;
  this->current_state_ = new_state;

  return this->prev_state_;
}

void MainThreadStateMgr::Reset() {
  this->prev_state_ = this->current_state_;
  this->current_state_ = MainThreadState::MT_READY;
  first_block_ = true;
}

bool MainThreadStateMgr::FirstBlock() const {
  return first_block_;
}

