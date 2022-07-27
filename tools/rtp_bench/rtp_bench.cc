#include "rtp_bench.h"

namespace pdlfs {
namespace carp {
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
  SETENV("NEXUS_ALT_LOCAL", "na+sm");
  SETENV("NEXUS_BYPASS_LOCAL", "0");

  pctx.carp_on = true;
  pctx.particle_indexed_attr_size = 4;
  pctx.particle_id_size = 8;
  pctx.particle_size = 40;
  pctx.particle_extra_size = 8;
  pctx.my_cpus = my_cpu_cores();

  MPI_Comm_rank(MPI_COMM_WORLD, &pctx.my_rank);

  /* Everyone is a shuffle sender + receiver for the benchmark */
  pctx.recv_comm = MPI_COMM_WORLD;
  MPI_Comm_rank(pctx.recv_comm, &pctx.recv_rank);
  MPI_Comm_size(pctx.recv_comm, &pctx.recv_sz);

#undef SETENV
}
}  // namespace carp
}  // namespace pdlfs