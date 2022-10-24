#include "rtp_bench.h"

#include <pdlfs-common/env.h>
#include <pdlfs-common/env_files.h>

namespace pdlfs {
namespace carp {
uint64_t sumsq(const uint64_t& total, const uint64_t& v) {
  return total + v * v;
}

void EnsureDir(const char* dpath) {
  if (dpath == nullptr) {
    logf(LOG_ERRO, "[EnsureDir] No directory path specificied.");
    exit(-1);
  }

  logf(LOG_INFO, "[EnsureDir] Checking directory: %s\n", dpath);

  Env* env = pdlfs::Env::Default();
  Status s = env->CreateDir(dpath);

  if (s.ok() or s.IsAlreadyExists()) {
    // success;
    return;
  } else {
    logf(LOG_ERRO, "[EnsureDir] Error creating directory %s (%s)", dpath,
         s.ToString().c_str());
    exit(-1);
  }
}

void EnsureAllDirs() {
  if (pctx.my_rank != 0) {
    return;
  }

  EnsureDir(pctx.log_home);
  EnsureDir(pctx.local_root);
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
  SETENV("PRELOAD_Log_home", "logs");
  SETENV("PRELOAD_Local_root", "data");

  /* CARP params */
  SETENV("PRELOAD_Enable_CARP", "1");
  SETENV("PRELOAD_Filename_size", "8")
  SETENV("PRELOAD_Filedata_size", "48")
  SETENV("PRELOAD_Particle_indexed_attr_size", "4");
  SETENV("PRELOAD_Particle_indexed_attr_offset", "0");
  SETENV("RANGE_Oob_size", "512");
  SETENV("RANGE_Reneg_policy", "InvocationPeriodic");
  SETENV("RANGE_Reneg_interval", "500000");
  SETENV("RANGE_Pvtcnt_s1", "2048");
  SETENV("RANGE_Pvtcnt_s2", "2048");
  SETENV("RANGE_Pvtcnt_s3", "2048");

  pctx.carp_on = 1;
  pctx.opts = pdlfs::carp::preload_init_carpopts(&pctx.sctx);

  pctx.filename_size = 8;
  pctx.filedata_size = 48;

  pctx.preload_invalue_size = pctx.preload_outvalue_size = pctx.filedata_size;

  pctx.preload_inkey_size = pctx.opts->index_attr_size;
  pctx.preload_outkey_size = pctx.preload_inkey_size;
  pctx.preload_invalue_size += pctx.filename_size;
  pctx.preload_outvalue_size += pctx.filename_size;
  pctx.key_size = pctx.preload_outkey_size;
  pctx.value_size = pctx.preload_outvalue_size;
  pctx.serialized_size = pctx.preload_inkey_size + pctx.preload_invalue_size;


  pctx.deltafs_mntp = "particle";
  pctx.len_deltafs_mntp = strlen(pctx.deltafs_mntp);
  pctx.plfsdir = pctx.deltafs_mntp;
  pctx.len_plfsdir = pctx.len_deltafs_mntp;

  pctx.log_home = "logs";
  pctx.len_log_home = strlen(pctx.log_home);
  pctx.local_root = "data";
  pctx.len_local_root = strlen(pctx.local_root);

  pctx.my_cpus = my_cpu_cores();

  MPI_Comm_rank(MPI_COMM_WORLD, &pctx.my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &pctx.comm_sz);

  /* only executes at Rank 0 */
  EnsureAllDirs();

  /* Everyone is a shuffle sender + receiver for the benchmark */
  pctx.recv_comm = MPI_COMM_WORLD;
  MPI_Comm_rank(pctx.recv_comm, &pctx.recv_rank);
  MPI_Comm_size(pctx.recv_comm, &pctx.recv_sz);

  my_rank_ = pctx.my_rank;

  pctx.opts->num_ranks = pctx.comm_sz;
  pctx.opts->my_rank = pctx.my_rank;

#undef SETENV
}
}  // namespace carp
}  // namespace pdlfs
