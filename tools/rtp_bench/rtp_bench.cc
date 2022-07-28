#include "rtp_bench.h"

namespace pdlfs {
namespace carp {
uint64_t sumsq(const uint64_t& total, const uint64_t& v) {
  return total + v*v;
}

void RTPBench::InitParams() {
  int overwrite = 0;
#define SETENV(k, v) setenv(k, v, overwrite);

  SETENV("SHUFFLE_Mercury_proto", "na+sm");
  SETENV("SHUFFLE_Mercury_progress_timeout", "100");
  SETENV("SHUFFLE_Mercury_progress_warn_interval", "1000");
  SETENV("SHUFFLE_Mercury_cache_handles", "0");
  SETENV("SHUFFLE_Mercury_rusage", "0");
  SETENV("SHUFFLE_Mercury_nice", "0");
  SETENV("SHUFFLE_Mercury_max_errors", "1");
  SETENV("SHUFFLE_Buffer_per_queue", "32768");
  SETENV("SHUFFLE_Num_outstanding_rpc", "16");
  SETENV("SHUFFLE_Use_worker_thread", "1");
  SETENV("SHUFFLE_Force_sync_rpc", "0");
  SETENV("SHUFFLE_Placement_protocol", "ring");
  SETENV("SHUFFLE_Virtual_factor", "1024");
  SETENV("SHUFFLE_Subnet", "172.26");
  SETENV("SHUFFLE_Finalize_pause", "0");
  SETENV("SHUFFLE_Force_global_barrier", "0");
  SETENV("SHUFFLE_Local_senderlimit", "16");
  SETENV("SHUFFLE_Remote_senderlimit", "16");
  SETENV("SHUFFLE_Local_maxrpc", "16");
  SETENV("SHUFFLE_Relay_maxrpc", "16");
  SETENV("SHUFFLE_Remote_maxrpc", "16");
  SETENV("SHUFFLE_Local_buftarget", "32768");
  SETENV("SHUFFLE_Relay_buftarget", "32768");
  SETENV("SHUFFLE_Remote_buftarget", "32768");
  SETENV("SHUFFLE_Dq_min", "1024");
  SETENV("SHUFFLE_Dq_max", "4096");
  SETENV("SHUFFLE_Log_file", "/");
  SETENV("SHUFFLE_Force_rpc", "0");
  SETENV("SHUFFLE_Hash_sig", "0");
  SETENV("SHUFFLE_Paranoid_checks", "0");
  SETENV("SHUFFLE_Random_flush", "0");
  SETENV("SHUFFLE_Recv_radix", "0");
  SETENV("SHUFFLE_Use_multihop", "1");
  SETENV("NEXUS_ALT_LOCAL", "na+sm");
  SETENV("NEXUS_BYPASS_LOCAL", "0");

  pctx.carp_on = true;
  pctx.particle_indexed_attr_size = 4;
  pctx.particle_id_size = 8;
  pctx.particle_size = 40;
  pctx.particle_extra_size = 8;
  pctx.my_cpus = my_cpu_cores();

  pctx.log_home = "logs";
  pctx.local_root = "data";

  MPI_Comm_rank(MPI_COMM_WORLD, &pctx.my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &pctx.comm_sz);

  /* Everyone is a shuffle sender + receiver for the benchmark */
  pctx.recv_comm = MPI_COMM_WORLD;
  MPI_Comm_rank(pctx.recv_comm, &pctx.recv_rank);
  MPI_Comm_size(pctx.recv_comm, &pctx.recv_sz);

  my_rank_ = pctx.my_rank;

  pctx.opts = new pdlfs::carp::CarpOptions();
  pctx.opts->rtp_pvtcnt[1] = 256;
  pctx.opts->rtp_pvtcnt[2] = 256;
  pctx.opts->rtp_pvtcnt[3] = 256;
  /* doesn't matter, not used */
  pctx.opts->oob_sz = 256;
  pctx.carp_dynamic_reneg = 0;
  pctx.opts->reneg_policy = pdlfs::kDefaultRenegPolicy;
  pctx.opts->reneg_intvl = pdlfs::kRenegInterval;
  pctx.opts->dynamic_thresh = pdlfs::kDynamicThreshold;
  pctx.opts->num_ranks = pctx.comm_sz;
  pctx.opts->my_rank = pctx.my_rank;
  pctx.opts->sctx = &(pctx.sctx);
  pctx.opts->mock_io_enabled = false;
  pctx.opts->io_enabled = false;

//  pctx.opts->mount_path = pctx.local_root; // XXX: local_root not set
//  pctx.opts->mount_path += "/";
//  pctx.opts->mount_path += "stripped"; // XXX: what is this?
//  pctx.carp = new pdlfs::carp::Carp(*pctx.opts);
//

#undef SETENV
}
}  // namespace carp
}  // namespace pdlfs
