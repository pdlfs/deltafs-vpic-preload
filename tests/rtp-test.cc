//
// Created by Ankush J on 3/15/21.
//

#include "carp/rtp.h"

#include "carp/carp.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "perfstats/manifest_analytics.h"

namespace pdlfs {
namespace carp {
class RTPTest {
 public:
  void PrepareOpts() {
    opts_.my_rank = 1;
    opts_.num_ranks = 512;
    opts_.sctx = new shuffle_ctx_t;
    opts_.sctx->type = SHUFFLE_XN;

    opts_.rtp_pvtcnt[1] = 128;
    opts_.rtp_pvtcnt[2] = 128;
    opts_.rtp_pvtcnt[3] = 128;
  }

  void ClearOpts() {
    delete opts_.sctx;
  }

  void AssertReady(RTP* rtp) {
    ASSERT_EQ(rtp->state_.GetState(), RenegState::READY);
  }

  uint64_t ValidateTopology(RTP* rtp) {
#define FANOUT(x) rtp->fanout_[(x)]
    ASSERT_EQ(FANOUT(1)*FANOUT(2)*FANOUT(3), opts_.num_ranks);
    ASSERT_EQ(rtp->root_[3], 0);

    /* proxy for hash of peer set */
    uint64_t peer_sum = 0;

    for (int stage = 1; stage <= 3; stage++) {
      int prev = -1;
      for (int pidx = 0; pidx < rtp->fanout_[stage]; pidx++) {
        int peer = rtp->peers_[stage][pidx];
        ASSERT_GT(peer, prev);
        prev = peer;
        peer_sum += peer;

        ASSERT_GT(peer, -1);
        ASSERT_LT(peer, opts_.num_ranks);
      }
    }

    return peer_sum;
  }

  CarpOptions opts_;

  ~RTPTest() {
   ClearOpts();
  }
};

TEST(RTPTest, InitRTP) {
  PrepareOpts();
  RTP* rtp = new RTP(nullptr, opts_);
  AssertReady(rtp);
  delete rtp;
}

TEST(RTPTest, ComputeTopology) {
  PrepareOpts();
  int my_rank_bak = opts_.my_rank;

  opts_.my_rank = 1;
  RTP* rtp_1 = new RTP(nullptr, opts_);
  uint64_t rtp_hash_1 = ValidateTopology(rtp_1);

  opts_.my_rank = 2;
  RTP* rtp_2 = new RTP(nullptr, opts_);
  uint64_t rtp_hash_2 = ValidateTopology(rtp_2);

  opts_.my_rank = 8;
  RTP* rtp_3 = new RTP(nullptr, opts_);
  uint64_t rtp_hash_3 = ValidateTopology(rtp_3);

  /* peer arrays are self-inclusive, so should be the same
   * for rtp1 and rtp2: assuming they're in the same leaf
   * which is reasonable for a fanout of 512
   */
  ASSERT_EQ(rtp_hash_1, rtp_hash_2);
  ASSERT_NE(rtp_hash_1, rtp_hash_3);
  opts_.my_rank = my_rank_bak;
  delete rtp_1;
  delete rtp_2;
  delete rtp_3;
}
}  // namespace carp
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
