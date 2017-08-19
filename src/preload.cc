/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <mpi.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>

#include <map>
#include <string>
#include <vector>

#include <pdlfs-common/xxhash.h>
#include "preload_internal.h"

/* particle bytes */
#define PRELOAD_PARTICLE_SIZE 40

/* mon output */
static int mon_dump_bin = 0;
static int mon_dump_txt = 1;

/* mutex to protect preload state */
static pthread_mutex_t preload_mtx = PTHREAD_MUTEX_INITIALIZER;

/* mutex to synchronize writes */
static pthread_mutex_t write_mtx = PTHREAD_MUTEX_INITIALIZER;

/* number of MPI barriers invoked by app */
static int num_barriers = 0;

/* number of epoches generated */
static int num_epochs = 0;

/*
 * we use the address of fake_dirptr as a fake DIR* with opendir/closedir
 */
static int fake_dirptr = 0;

/*
 * next_functions: libc replacement functions we are providing to the preloader.
 */
struct next_functions {
  /* functions we need */
  int (*MPI_Init)(int* argc, char*** argv);
  int (*MPI_Finalize)(void);
  int (*MPI_Barrier)(MPI_Comm comm);
  int (*chdir)(const char* path);
  int (*mkdir)(const char* path, mode_t mode);
  DIR* (*opendir)(const char* filename);
  int (*closedir)(DIR* dirp);
  FILE* (*fopen)(const char* filename, const char* mode);
  size_t (*fwrite)(const void* ptr, size_t size, size_t nitems, FILE* stream);
  int (*fclose)(FILE* stream);

  /* for error catching we do these */
  int (*feof)(FILE* stream);
  int (*ferror)(FILE* stream);
  void (*clearerr)(FILE* stream);
  size_t (*fread)(void* ptr, size_t size, size_t nitems, FILE* stream);
  int (*fseek)(FILE* stream, long offset, int whence);
  long (*ftell)(FILE* stream);
};

static struct next_functions nxt = {0};

/*
 * this once is used to trigger the init of the preload library...
 */
static pthread_once_t init_once = PTHREAD_ONCE_INIT;

/* helper: must_getnextdlsym: get next symbol or fail */
static void must_getnextdlsym(void** result, const char* symbol) {
  *result = dlsym(RTLD_NEXT, symbol);
  if (*result == NULL) msg_abort(symbol);
}

/*
 * preload_init: called via init_once.   if this fails we are sunk, so
 * we'll abort the process....
 */
static void preload_init() {
  std::vector<std::pair<const char*, size_t> > paths;
  const char* tmp;

  must_getnextdlsym(reinterpret_cast<void**>(&nxt.MPI_Init), "MPI_Init");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.MPI_Finalize),
                    "MPI_Finalize");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.MPI_Barrier), "MPI_Barrier");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.chdir), "chdir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.mkdir), "mkdir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.opendir), "opendir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.closedir), "closedir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fopen), "fopen");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fwrite), "fwrite");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fclose), "fclose");

  must_getnextdlsym(reinterpret_cast<void**>(&nxt.feof), "feof");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.ferror), "ferror");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.clearerr), "clearerr");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fread), "fread");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fseek), "fseek");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.ftell), "ftell");

  pctx.logfd = -1;
  pctx.monfd = -1;

  pctx.isdeltafs = new std::set<FILE*>;
  pctx.fnames = new std::set<std::string>;
  pctx.smap = new std::map<std::string, int>;

  pctx.sthres = 100; /* 100 samples per 1 million input */

  pctx.sampling = 1;
  pctx.paranoid_checks = 1;
  pctx.paranoid_barrier = 1;
  pctx.paranoid_post_barrier = 1;
  pctx.paranoid_pre_barrier = 1;
  pctx.pre_flushing = 1;
  pctx.my_rank = 0;
  pctx.comm_sz = 1;

  pctx.deltafs_root = maybe_getenv("PRELOAD_Deltafs_root");
  if (!pctx.deltafs_root) pctx.deltafs_root = DEFAULT_DELTAFS_ROOT;
  pctx.len_deltafs_root = strlen(pctx.deltafs_root);

  /* deltafs root:
   *   - any non-null path,
   *   - not "/", and
   *   - not ending in "/"
   */
  if (pctx.len_deltafs_root == 0 ||
      (pctx.len_deltafs_root == 1 && pctx.deltafs_root[0] == '/') ||
      pctx.deltafs_root[pctx.len_deltafs_root - 1] == '/')
    msg_abort("bad deltafs_root");

  tmp = maybe_getenv("PRELOAD_Ignore_dirs");
  if (tmp != NULL && tmp[0] != 0) {
    for (const char* ch = strchr(tmp, ':'); ch != NULL;) {
      paths.push_back(std::make_pair(tmp, ch - tmp));
      tmp = ch + 1;
      ch = strchr(tmp, ':');
    }
    if (tmp[0] != 0) {
      paths.push_back(std::make_pair(tmp, strlen(tmp)));
    }
  }
  /* for each ignore dir:
   * - may be NULL or empty, otherwise,
   * - not "/", and
   * - not ending in "/"
   */
  if (paths.size() != 0) {
    pctx.num_ignore_dirs = paths.size();
    pctx.ignore_dirs =
        static_cast<const char**>(malloc(pctx.num_ignore_dirs * sizeof(void*)));
    pctx.len_ignore_dirs =
        static_cast<size_t*>(malloc(pctx.num_ignore_dirs * sizeof(size_t)));

    for (size_t i = 0; i < pctx.num_ignore_dirs; i++) {
      pctx.len_ignore_dirs[i] = paths[i].second;
      pctx.ignore_dirs[i] = paths[i].first;
      if (pctx.len_ignore_dirs[i] != 0) {
        if (pctx.len_ignore_dirs[i] == 1 && pctx.ignore_dirs[i][0] == '/')
          msg_abort("bad ignore_dir");
        if (pctx.ignore_dirs[i][pctx.len_ignore_dirs[i] - 1] == '/')
          msg_abort("bad ignore_dir");
      }
    }
  }

  /* obtain the path to plfsdir */
  pctx.plfsdir = maybe_getenv("PRELOAD_Plfsdir");

  /* plfsdir:
   *   - if null, no plfsdir will ever be created
   *   - otherwise, it will be created and opened at MPI_Init
   */
  if (pctx.plfsdir == NULL) {
    if (pctx.deltafs_root[0] != '/') {
      /* default to deltafs_root if deltafs_root is relative */
      pctx.plfsdir = pctx.deltafs_root;
    }
  }
  if (pctx.plfsdir != NULL) {
    pctx.len_plfsdir = strlen(pctx.plfsdir);
  }

  pctx.plfsfd = -1;

  pctx.log_home = maybe_getenv("PRELOAD_Log_home");
  if (!pctx.log_home) pctx.log_home = DEFAULT_LOG_HOME;
  pctx.len_log_home = strlen(pctx.log_home);

  /* log home:
   *   - any non-null path,
   *   - not "/",
   *   - starting with "/", and
   *   - not ending in "/"
   */
  if (pctx.len_log_home == 0 || pctx.len_log_home == 1 ||
      pctx.log_home[0] != '/' || pctx.log_home[pctx.len_log_home - 1] == '/')
    msg_abort("bad log_root");

  pctx.local_root = maybe_getenv("PRELOAD_Local_root");
  if (!pctx.local_root) pctx.local_root = DEFAULT_LOCAL_ROOT;
  pctx.len_local_root = strlen(pctx.local_root);

  /* local root:
   *   - any non-null path,
   *   - not "/",
   *   - starting with "/", and
   *   - not ending in "/"
   */
  if (pctx.len_local_root == 0 || pctx.len_local_root == 1 ||
      pctx.local_root[0] != '/' ||
      pctx.local_root[pctx.len_local_root - 1] == '/')
    msg_abort("bad local_root");

  if (is_envset("PRELOAD_Skip_sampling")) pctx.sampling = 0;

  tmp = maybe_getenv("PRELOAD_Sample_threshold");
  if (tmp != NULL) {
    pctx.sthres = atoi(tmp);
    if (pctx.sthres < 1) {
      pctx.sthres = 1;
    }
  }

  if (is_envset("PRELOAD_Bypass_shuffle")) pctx.mode |= BYPASS_SHUFFLE;
  if (is_envset("PRELOAD_Bypass_placement")) pctx.mode |= BYPASS_PLACEMENT;

  if (is_envset("PRELOAD_Bypass_deltafs_plfsdir"))
    pctx.mode |= BYPASS_DELTAFS_PLFSDIR;
  if (is_envset("PRELOAD_Bypass_deltafs_namespace"))
    pctx.mode |= BYPASS_DELTAFS_NAMESPACE;
  if (is_envset("PRELOAD_Bypass_deltafs")) pctx.mode |= BYPASS_DELTAFS;
  if (is_envset("PRELOAD_Bypass_write")) pctx.mode |= BYPASS_WRITE;

  if (is_envset("PRELOAD_Skip_mon")) pctx.nomon = 1;
  if (is_envset("PRELOAD_Skip_mon_dist")) pctx.nodist = 1;
  if (is_envset("PRELOAD_Enable_verbose_mon")) pctx.vmon = 1;
  if (is_envset("PRELOAD_Enable_verbose_error")) pctx.verr = 1;

  if (is_envset("PRELOAD_No_paranoid_checks")) pctx.paranoid_checks = 0;
  if (is_envset("PRELOAD_No_paranoid_pre_barrier"))
    pctx.paranoid_pre_barrier = 0;
  if (is_envset("PRELOAD_No_epoch_pre_flushing")) pctx.pre_flushing = 0;
  if (is_envset("PRELOAD_No_paranoid_barrier")) pctx.paranoid_barrier = 0;
  if (is_envset("PRELOAD_No_paranoid_post_barrier"))
    pctx.paranoid_post_barrier = 0;
  if (is_envset("PRELOAD_No_sys_probing")) pctx.noscan = 1;
  if (is_envset("PRELOAD_Inject_fake_data")) pctx.fake_data = 1;
  if (is_envset("PRELOAD_Testing")) pctx.testin = 1;

  /* additional init can go here or MPI_Init() */
}

/*
 * should_ignore: inspect the path to see if we should just ignore it
 */
static int should_ignore(const char* path) {
  for (size_t i = 0; i < pctx.num_ignore_dirs; i++) {
    if (pctx.len_ignore_dirs[i] != 0) {
      if (strncmp(pctx.ignore_dirs[i], path, pctx.len_ignore_dirs[i]) == 0) {
        if (path[pctx.len_ignore_dirs[i]] == '/') {
          return 1;
        }
      }
    }
  }
  return 0;
}

/*
 * claim_path: look at path to see if we can claim it
 */
static int claim_path(const char* path, int* exact) {
  if (strncmp(pctx.deltafs_root, path, pctx.len_deltafs_root) != 0) return 0;
  if (path[pctx.len_deltafs_root] != '/' &&
      path[pctx.len_deltafs_root] != '\0') {
    return 0;
  }
  /* if we've just got pctx.root, caller may convert it to a "/" */
  *exact = int(path[pctx.len_deltafs_root] == '\0');
  return 1;
}

/*
 * under_plfsdir: if a given path is a plfsdir or plfsdir files
 */
static int under_plfsdir(const char* path) {
  if (pctx.plfsdir == NULL) return 0;
  if (strncmp(pctx.plfsdir, path, pctx.len_plfsdir) != 0) return 0;
  if (path[pctx.len_plfsdir] == 0 || path[pctx.len_plfsdir] == '/') return 1;
  return 0;
}

/*
 * claim_FILE: look at FILE* and see if we claim it
 */
static int claim_FILE(FILE* stream) {
  std::set<FILE*>::iterator it;
  int rv;

  pthread_mtx_lock(&preload_mtx);
  assert(pctx.isdeltafs != NULL);
  it = pctx.isdeltafs->find(stream);
  rv = int(it != pctx.isdeltafs->end());
  pthread_mtx_unlock(&preload_mtx);

  return rv;
}

/*
 * dump in-memory mon stats to files.
 */
static void dump_mon(mon_ctx_t* mon, dir_stat_t* tmp_stat,
                     const dir_stat_t* prev_stat) {
  uint64_t ts;
  uint64_t diff;
  char buf[MON_BUF_SIZE];
  char msg[100];
  int n;

  if (!pctx.nomon) {
    /* collect stats from deltafs */
    if (pctx.plfsh != NULL) {
      mon_fetch_plfsdir_stat(pctx.plfsh, tmp_stat);
      mon->dir_stat.num_keys = tmp_stat->num_keys - prev_stat->num_keys;
      mon->dir_stat.max_num_keys =
          tmp_stat->max_num_keys - prev_stat->max_num_keys;
      mon->dir_stat.min_num_keys =
          tmp_stat->min_num_keys - prev_stat->min_num_keys;
      mon->dir_stat.num_dropped_keys =
          tmp_stat->num_dropped_keys - prev_stat->num_dropped_keys;
      mon->dir_stat.total_fblksz =
          tmp_stat->total_fblksz - prev_stat->total_fblksz;
      mon->dir_stat.total_iblksz =
          tmp_stat->total_iblksz - prev_stat->total_iblksz;
      mon->dir_stat.total_dblksz =
          tmp_stat->total_dblksz - prev_stat->total_dblksz;
      mon->dir_stat.total_datasz =
          tmp_stat->total_datasz - prev_stat->total_datasz;
      mon->dir_stat.num_sstables =
          tmp_stat->num_sstables - prev_stat->num_sstables;
    } else if (pctx.plfsfd != -1) {
      // XXX: TODO
    }

    /* dump txt mon stats to log file if in testing mode */
    if (pctx.testin) {
      if (pctx.logfd != -1) {
        mon_dumpstate(pctx.logfd, mon);
      }
    }

    /* dump txt mon stats to stderr if in verbose mode */
    if (pctx.vmon) {
      if (pctx.my_rank == 0) {
        mon_dumpstate(fileno(stderr), mon);
      }
    }

    if (pctx.monfd != -1) {
      if (pctx.my_rank == 0) {
        info("saving epoch statistics ... (rank 0)");
        ts = now_micros();
      }
      memset(buf, 0, sizeof(buf));
      assert(sizeof(mon_ctx_t) < sizeof(buf));
      memcpy(buf, mon, sizeof(mon_ctx_t));
      n = write(pctx.monfd, buf, sizeof(buf));
      if (pctx.my_rank == 0) {
        diff = now_micros() - ts;
        snprintf(msg, sizeof(msg), "saving ok %s (rank 0)",
                 pretty_dura(diff).c_str());
        info(msg);
      }

      errno = 0;
    }
  }
}

/*
 * plfsdir_conf: plfsdir configurations.
 */
struct plfsdir_conf {
  const char* key_size;
  const char* bits_per_key;
  const char* comp_buf;
  const char* min_index_write_size;
  const char* index_buf;
  const char* min_data_write_size;
  const char* data_buf;
  const char* memtable_size;
  const char* lg_parts;
  int skip_checksums;
};

static struct plfsdir_conf dirc = {0};

/*
 * gen_plfsdir_conf: initialize plfsdir conf and obtain it's string literal.
 */
static std::string gen_plfsdir_conf(int rank) {
  char tmp[500];
  int n;

  n = snprintf(tmp, sizeof(tmp), "rank=%d", rank);

  dirc.key_size = maybe_getenv("PLFSDIR_Key_size");
  if (dirc.key_size == NULL) {
    dirc.key_size = DEFAULT_KEY_SIZE;
  }

  dirc.bits_per_key = maybe_getenv("PLFSDIR_Filter_bits_per_key");
  if (dirc.bits_per_key == NULL) {
    dirc.bits_per_key = DEFAULT_BITS_PER_KEY;
  }

  dirc.memtable_size = maybe_getenv("PLFSDIR_Memtable_size");
  if (dirc.memtable_size == NULL) {
    dirc.memtable_size = DEFAULT_MEMTABLE_SIZE;
  }

  dirc.comp_buf = maybe_getenv("PLFSDIR_Compaction_buf_size");
  if (dirc.comp_buf == NULL) {
    dirc.comp_buf = DEFAULT_COMPACTION_BUF;
  }

  dirc.min_index_write_size = maybe_getenv("PLFSDIR_Index_min_write_size");
  if (dirc.min_index_write_size == NULL) {
    dirc.min_index_write_size = DEFAULT_INDEX_MIN_WRITE_SIZE;
  }

  dirc.index_buf = maybe_getenv("PLFSDIR_Index_buf_size");
  if (dirc.index_buf == NULL) {
    dirc.index_buf = DEFAULT_INDEX_BUF;
  }

  dirc.min_data_write_size = maybe_getenv("PLFSDIR_Data_min_write_size");
  if (dirc.min_data_write_size == NULL) {
    dirc.min_data_write_size = DEFAULT_DATA_MIN_WRITE_SIZE;
  }

  dirc.data_buf = maybe_getenv("PLFSDIR_Data_buf_size");
  if (dirc.data_buf == NULL) {
    dirc.data_buf = DEFAULT_DATA_BUF;
  }

  dirc.lg_parts = maybe_getenv("PLFSDIR_Lg_parts");
  if (dirc.lg_parts == NULL) {
    dirc.lg_parts = DEFAULT_LG_PARTS;
  }

  if (is_envset("PLFSDIR_Skip_checksums")) {
    dirc.skip_checksums = 1;
  } else {
    dirc.skip_checksums = 0;
  }

  n += snprintf(tmp + n, sizeof(tmp) - n, "&lg_parts=%s", dirc.lg_parts);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&memtable_size=%s",
                dirc.memtable_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&compaction_buffer=%s",
                dirc.comp_buf);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&index_buffer=%s", dirc.index_buf);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&min_index_buffer=%s",
                dirc.min_index_write_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&data_buffer=%s", dirc.data_buf);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&min_data_buffer=%s",
                dirc.min_data_write_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&skip_checksums=%d",
                dirc.skip_checksums);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&filter_bits_per_key=%s",
                dirc.bits_per_key);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&value_size=%d",
                PRELOAD_PARTICLE_SIZE);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&key_size=%s", dirc.key_size);

  return tmp;
}

static std::string& pretty_plfsdir_conf(std::string& conf) {
  std::string::size_type pos;
  pos = conf.find('=', 0);
  for (; pos != std::string::npos; pos = conf.find('=', 0))
    conf.replace(pos, 1, " -> ");
  pos = conf.find('&', 0);
  for (; pos != std::string::npos; pos = conf.find('&', 0))
    conf.replace(pos, 1, "\n // ");
  conf = std::string("plfsdir_conf = (\n // ") + conf;
  conf += "\n)";
  return conf;
}

namespace {
/*
 * fake_file is a replacement for FILE* that we use to accumulate all the
 * VPIC particle data before sending it to the shuffle layer (on fclose).
 *
 * we assume only one thread is writing to the file at a time, so we
 * do not put a mutex on it.
 *
 * we ignore out of memory errors.
 */
class fake_file {
 private:
  std::string path_; /* path of particle file (malloc'd c++) */
  char data_[64];    /* enough for one VPIC particle */
  char* dptr_;       /* ptr to next free space in data_ */
  size_t resid_;     /* residual */

 public:
  fake_file() : dptr_(data_), resid_(sizeof(data_)) { path_.reserve(256); }

  void reset(const char* path) {
    path_.assign(path);
    resid_ = sizeof(data_);
    dptr_ = data_;
  }

  explicit fake_file(const char* path)
      : path_(path), dptr_(data_), resid_(sizeof(data_)){};

  /* returns the actual number of bytes added. */
  size_t add_data(const void* toadd, size_t len) {
    int n = (len > resid_) ? resid_ : len;
    if (n) {
      memcpy(dptr_, toadd, n);
      dptr_ += n;
      resid_ -= n;
    }
    return n;
  }

  /* get data length */
  size_t size() { return sizeof(data_) - resid_; }

  /* recover filename. */
  const char* file_name() { return path_.c_str(); }

  /* get data */
  char* data() { return data_; }
};

/* avoids repeated malloc if vpic only opens one file a time */
static fake_file vpic_file_buffer;
static fake_file* vpic_file = &vpic_file_buffer;

}  // namespace

/*
 * here are the actual override functions from libc...
 */
extern "C" {

/*
 * MPI_Init
 */
int MPI_Init(int* argc, char*** argv) {
  int exact;
  const char* stripped;
  const char* cwd;
  time_t now;
  char buf[50];   // ctime_r
  char msg[200];  // snprintf
  char dirpath[PATH_MAX];
  char path[PATH_MAX];
  std::string conf;
#if MPI_VERSION >= 3
  size_t l;
  char mpi_info[MPI_MAX_LIBRARY_VERSION_STRING];
  char* c;
#endif
  int deltafs_major;
  int deltafs_minor;
  int deltafs_patch;
  intptr_t mpi_wtime_is_global;
  uid_t uid;
  int flag;
  int size;
  int rank;
  int rv;
  int n;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  rv = nxt.MPI_Init(argc, argv);
  if (rv == MPI_SUCCESS) {
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    uid = getuid();
    pctx.my_rank = rank;
    pctx.comm_sz = size;
    if (rank == 0) {
      deltafs_major = deltafs_version_major();
      deltafs_minor = deltafs_version_minor();
      deltafs_patch = deltafs_version_patch();
      snprintf(msg, sizeof(msg), "deltafs %d.%d.%d", deltafs_major,
               deltafs_minor, deltafs_patch);
      info(msg);
      snprintf(msg, sizeof(msg),
               "deltafs-vpic lib initializing "
               "... %s MPI ranks",
               pretty_num(size).c_str());
      info(msg);
    }
  } else {
    return rv;
  }

  if (rank == 0) {
#if MPI_VERSION < 3
    warn(
        "using non-recent MPI release: some features disabled\n>>> "
        "MPI ver 3 is suggested in production mode");
#else
    MPI_Get_library_version(mpi_info, &n);
    c = strchr(mpi_info, '\n');
    if (c != NULL) {
      *c = 0;
    }
    c = strchr(mpi_info, '\r');
    if (c != NULL) {
      *c = 0;
    }
    l = strlen(mpi_info);
    if (l > 125) {
      mpi_info[120] = 0;
      strcat(mpi_info, " ...");
    }
    info(mpi_info);
#endif
  }

  if (rank == 0) {
#if defined(MPI_WTIME_IS_GLOBAL)
    MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_WTIME_IS_GLOBAL, &mpi_wtime_is_global,
                      &flag);
    if (flag != 0) {
      if (mpi_wtime_is_global == 0) {
        warn(
            "MPI_Wtime() is NOT globally synchronized\n>>> "
            "MPI_WTIME_IS_GLOBAL is 0");
      } else {
        info(
            "MPI_Wtime() is globally synchronized\n>>> "
            "MPI_WTIME_IS_GLOBAL is 1");
      }
    } else {
      warn(
          "cannot determine if MPI_Wtime() is global\n>>> "
          "MPI_WTIME_IS_GLOBAL not set");
    }
#else
    warn(
        "cannot determine if MPI_Wtime() is global\n>>> "
        "MPI_WTIME_IS_GLOBAL undefined");
#endif
  }

  if (rank == 0) {
#if defined(__INTEL_COMPILER)
    n = snprintf(msg, sizeof(msg),
                 "[cc] compiled by Intel (icc/icpc) %d.%d.%d %d on %s %s "
                 "(__cplusplus: %ld)",
                 __INTEL_COMPILER / 100, __INTEL_COMPILER % 100,
                 __INTEL_COMPILER_UPDATE, __INTEL_COMPILER_BUILD_DATE, __DATE__,
                 __TIME__, __cplusplus);
#if defined(__GNUC__)
    snprintf(msg + n, sizeof(msg) - n, "\n>>> with gcc %d.%d.%d compatibility",
             __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#endif
    info(msg);
#elif defined(_CRAYC)
    n = snprintf(msg, sizeof(msg),
                 "[cc] compiled by Cray (crayc/crayc++) %d.%d on %s %s "
                 "(__cplusplus: %ld)",
                 _RELEASE, _RELEASE_MINOR, __DATE__, __TIME__, __cplusplus);
#if defined(__GNUC__)
    snprintf(msg + n, sizeof(msg) - n, "\n>>> with gcc %d.%d.%d compatibility",
             __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#endif
    info(msg);
#elif defined(__clang__)
    n = snprintf(
        msg, sizeof(msg),
        "[cc] compiled by LLVM/Clang (clang/clang++) %d.%d.%d on %s %s "
        "(__cplusplus: %ld)",
        __clang_major__, __clang_minor__, __clang_patchlevel__, __DATE__,
        __TIME__, __cplusplus);
#if defined(__GNUC__)
    snprintf(msg + n, sizeof(msg) - n, "\n>>> with gcc %d.%d.%d compatibility",
             __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#endif
    info(msg);
#elif defined(__GNUC__)
    snprintf(msg, sizeof(msg),
             "[cc] compiled by GNU (gcc/g++) %d.%d.%d on %s %s "
             "(__cplusplus: %ld)",
             __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__, __DATE__, __TIME__,
             __cplusplus);
    info(msg);
#endif
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    warn(
        "c/c++ OPTIMIZATION disabled: benchmarks unnecessarily slow\n>>> "
        "recompile with -O1, -O2, or -O3 to enable optimization");
#endif
#ifndef NDEBUG
    warn(
        "c/c++ ASSERTIONS enabled: benchmarks unnecessarily slow\n>>> "
        "recompile with \"-DNDEBUG\" to disable assertions");
#endif
  }

  /* print current directory */
  if (rank == 0) {
    n = snprintf(dirpath, sizeof(dirpath), "[cwd] ");
    cwd = getcwd(dirpath + n, sizeof(dirpath) - n);
    if (cwd == NULL) {
      msg_abort("getcwd");
    } else {
      info(dirpath);
    }
  }

  if (pctx.testin) {
    if (rank == 0) {
      warn(
          "testing mode: benchmarks unnecessarily slow\n>>> rerun with "
          "\"export PRELOAD_Testing=0\" to "
          "disable testing");
    }

    snprintf(dirpath, sizeof(dirpath), "/tmp/vpic-deltafs-run-%u",
             static_cast<unsigned>(uid));
    snprintf(path, sizeof(path), "%s/vpic-deltafs-trace.log.%d", dirpath, rank);

    n = nxt.mkdir(dirpath, 0777);
    errno = 0;
    pctx.logfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (pctx.logfd == -1) {
      msg_abort("cannot create log");
    } else {
      now = time(NULL);
      n = snprintf(msg, sizeof(msg), "%s\n--- trace ---\n", ctime_r(&now, buf));
      n = write(pctx.logfd, msg, n);

      errno = 0;
    }
  }

  /* obtain number of logic cpu cores */
  pctx.my_cpus = my_cpu_cores();

  /* probe system info, will skip if we have no access */
  if (rank == 0) {
    check_sse42();
    maybe_warn_cpuaffinity();
    maybe_warn_rlimit(pctx.my_rank, pctx.comm_sz);
    /* cpu info and os version */
    if (!pctx.noscan) try_scan_procfs();
    /* numa topo and nic */
    if (!pctx.noscan) try_scan_sysfs();
    check_clockres();
  }

  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    if (rank == 0) {
      info("shuffle starting ...");
    }
    shuffle_init(&pctx.sctx);
    /* ensures all peers have the shuffle ready */
    preload_barrier(MPI_COMM_WORLD);
    if (rank == 0) {
      info("shuffle started");
    }
  } else {
    if (rank == 0) {
      warn("shuffle bypassed");
    }
  }

  /* pre-create plfsdirs if there is any */
  if (pctx.plfsdir != NULL && !IS_BYPASS_WRITE(pctx.mode)) {
    if (!claim_path(pctx.plfsdir, &exact)) {
      msg_abort("plfsdir out of deltafs"); /* Oops!! */
    }

    /* relative paths we pass through; absolute we strip off prefix */

    if (pctx.plfsdir[0] != '/') {
      stripped = pctx.plfsdir;
    } else if (!exact) {
      stripped = pctx.plfsdir + pctx.len_deltafs_root;
    } else {
      msg_abort("bad plfsdir");
    }

    if (rank == 0) {
      if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode) ||
          IS_BYPASS_DELTAFS(pctx.mode)) {
        snprintf(path, sizeof(path), "%s/%s", pctx.local_root, stripped);
        n = nxt.mkdir(path, 0777);
        errno = 0;
        rv = 0;
      } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode)) {
        rv = deltafs_mkdir(stripped, 0777 | DELTAFS_DIR_PLFS_STYLE);
      } else {
        rv = deltafs_mkdir(stripped, 0777);
      }

      if (rv != 0) {
        msg_abort("cannot make plfsdir");
      } else {
        info("plfsdir created (rank 0)");
      }
    }

    /* so everyone sees the dir created */
    preload_barrier(MPI_COMM_WORLD);

    /* everyone opens it */
    if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      snprintf(path, sizeof(path), "%s/%s", pctx.local_root, stripped);
      conf = gen_plfsdir_conf(rank);

      pctx.plfsh = deltafs_plfsdir_create_handle(conf.c_str(), O_WRONLY);
      deltafs_plfsdir_enable_io_measurement(pctx.plfsh, 0);
      pctx.plfsparts = deltafs_plfsdir_get_memparts(pctx.plfsh);
      pctx.plfstp = deltafs_tp_init(pctx.plfsparts);
      deltafs_plfsdir_set_thread_pool(pctx.plfsh, pctx.plfstp);

      rv = deltafs_plfsdir_open(pctx.plfsh, path);
      if (rv != 0) {
        msg_abort("cannot open plfsdir");
      } else if (rank == 0) {
        info("plfsdir (via deltafs-LT) opened (rank 0)");
        if (pctx.verr) {
          pretty_plfsdir_conf(conf);
          info(conf.c_str());
        }
      }
    } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode) &&
               !IS_BYPASS_DELTAFS(pctx.mode)) {
      pctx.plfsfd = deltafs_open(stripped, O_WRONLY | O_DIRECTORY, 0);
      if (pctx.plfsfd == -1) {
        msg_abort("cannot open plfsdir");
      } else if (rank == 0) {
        info("plfsdir opened (rank 0)");
      }
    }
  }

  if (!pctx.nomon) {
    snprintf(dirpath, sizeof(dirpath), "/tmp/vpic-deltafs-run-%u",
             static_cast<unsigned>(uid));
    snprintf(path, sizeof(path), "%s/vpic-deltafs-mon.bin.%d", dirpath, rank);

    n = nxt.mkdir(dirpath, 0777);
    errno = 0;
    pctx.monfd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);

    if (pctx.monfd == -1) {
      msg_abort("cannot create tmp stats file");
    } else if (rank == 0) {
      snprintf(msg, sizeof(msg),
               "in-mem epoch mon stats %d bytes\n>>> MON_BUF_SIZE is %d",
               int(sizeof(mon_ctx_t)), MON_BUF_SIZE);
      info(msg);
    }
  }

  if (rank == 0) {
    if (pctx.sampling) {
      snprintf(msg, sizeof(msg), "########## | >>> particle sampling: %s in %s",
               pretty_num(pctx.sthres).c_str(), pretty_num(1000000).c_str());
      info(msg);
    } else {
      info("particle sampling skipped");
    }

    if (pctx.fake_data) warn("vpic output replaced with fake data");
    if (pctx.paranoid_checks)
      warn(
          "paranoid checks enabled: benchmarks unnecessarily slow "
          "and memory usage unnecessarily high\n>>> "
          "rerun with \"export PRELOAD_No_paranoid_checks=1\" to disable");
    if (pctx.noscan) warn("auto os & hardware detection disabled");
    if (pctx.nomon)
      warn("self-mon disabled: detailed stats reporting not available");

    n = snprintf(msg, sizeof(msg), "deltafs root is \"%s\"", pctx.deltafs_root);
    if (pctx.num_ignore_dirs != 0) {
      n += snprintf(msg + n, sizeof(msg) - n, "\n>>> ignore dirs: ");
      for (size_t i = 0; i < pctx.num_ignore_dirs; i++)
        n += snprintf(
            msg + n, sizeof(msg) - n, "\"%s\",",
            std::string(pctx.ignore_dirs[i], pctx.len_ignore_dirs[i]).c_str());
      if (n - 1 < sizeof(msg)) {
        msg[n - 1] = 0;
      }
    }
    info(msg);

    if (IS_BYPASS_WRITE(pctx.mode)) {
      warn("particle writes bypassed");
    } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      warn("deltafs metadata bypassed");
    } else if (IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode)) {
      warn("deltafs plfsdir bypassed");
    } else if (IS_BYPASS_DELTAFS(pctx.mode)) {
      warn("deltafs bypassed");
    }
  }

  srand(rank);

  return rv;
}

/*
 * MPI_Barrier
 */
int MPI_Barrier(MPI_Comm comm) {
  int rv;

  rv = nxt.MPI_Barrier(comm);
  num_barriers++;

  return rv;
}

/*
 * MPI_Finalize
 */
int MPI_Finalize(void) {
  int fd0;
  int fd1;
  int fd2;
  mon_ctx_t local;
  mon_ctx_t glob;
  dir_stat_t tmp_stat;
  char buf[MON_BUF_SIZE];
  char path[PATH_MAX];
  char suffix[100];
  char msg[200];
  unsigned long long num_writes;
  unsigned long long min_writes;
  unsigned long long max_writes;
  uint64_t finish_start;
  uint64_t finish_end;
  std::string tmp;
  unsigned long long num_samples[2];
  unsigned long long sum_samples[2];
  size_t num_names;
  double ucpu;
  double scpu;
  time_t now;
  struct tm timeinfo;
  uint64_t ts;
  uint64_t diff;
  int ok;
  int go;
  int epoch;
  int rv;
  int n;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (pctx.my_rank == 0) {
    info("lib finalizing ... ");
    snprintf(msg, sizeof(msg), "%d epochs generated in total", num_epochs);
    info(msg);
    if (!pctx.nodist) {
      nxt.mkdir(pctx.log_home, 0755);
      snprintf(path, sizeof(path), "%s/exp-info", pctx.log_home);
      nxt.mkdir(path, 0755);
      now = time(NULL);
      localtime_r(&now, &timeinfo);
      snprintf(suffix, sizeof(suffix), "%04d%02d%02d-%02d:%02d:%02d",
               timeinfo.tm_year + 1900,  // YYYY
               timeinfo.tm_mon + 1,      // MM
               timeinfo.tm_mday,         // DD
               timeinfo.tm_hour,         // hh
               timeinfo.tm_min,          // mm
               timeinfo.tm_sec           // ss
               );
      snprintf(path, sizeof(path), "%s/exp-info/TIMESTAMP-%s", pctx.log_home,
               suffix);
      mknod(path, 0644, S_IFREG);
      if (pctx.plfsdir != NULL) {
        snprintf(path, sizeof(path), "%s/exp-info/MANIFEST", pctx.log_home);
        fd0 = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd0 != -1) {
          n = snprintf(msg, sizeof(msg), "key_size=%s\n", dirc.key_size);
          n = write(fd0, msg, n);
          n = snprintf(msg, sizeof(msg), "filter_bits_per_key=%s\n",
                       dirc.bits_per_key);
          n = write(fd0, msg, n);
          n = snprintf(msg, sizeof(msg), "lg_parts=%s\n", dirc.lg_parts);
          n = write(fd0, msg, n);
          n = snprintf(msg, sizeof(msg), "skip_checksums=%d\n",
                       dirc.skip_checksums);
          n = write(fd0, msg, n);
          n = snprintf(msg, sizeof(msg), "comm_sz=%d", pctx.comm_sz);
          n = write(fd0, msg, n);
          close(fd0);
          errno = 0;
        } else {
          error("open");
        }
      }
    }
  }

  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    if (pctx.my_rank == 0) {
      info("shuffle shutting down ...");
    }
    /* ensures all peer messages are received */
    preload_barrier(MPI_COMM_WORLD);
    /* shuffle flush */
    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      shuffle_epoch_start(&pctx.sctx);
    }
    shuffle_finalize(&pctx.sctx);

    if (pctx.my_rank == 0) {
      info("shuffle closed");
    }
  }

  /* all writes are concluded, time to close all plfsdirs */
  if (pctx.plfsh != NULL) {
    if (pctx.my_rank == 0) {
      finish_start = now_micros();
      info("finalizing plfsdir ... (rank 0)");
    }
    deltafs_plfsdir_finish(pctx.plfsh);
    if (pctx.my_rank == 0) {
      finish_end = now_micros();
      snprintf(msg, sizeof(msg), "finalizing done %s",
               pretty_dura(finish_end - finish_start).c_str());
      info(msg);
    }
    if (num_epochs != 0) {
      dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
    }

    deltafs_plfsdir_free_handle(pctx.plfsh);
    if (pctx.plfstp != NULL) {
      deltafs_tp_close(pctx.plfstp);
      pctx.plfstp = NULL;
    }
    pctx.plfsh = NULL;

    if (pctx.my_rank == 0) {
      info("plfsdir (via deltafs-LT) closed (rank 0)");
    }
  } else if (pctx.plfsfd != -1) {
    if (num_epochs != 0) {
      dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
    }
    deltafs_close(pctx.plfsfd);
    pctx.plfsfd = -1;

    if (pctx.my_rank == 0) {
      info("plfsdir closed (rank 0)");
    }
  } else {
    if (num_epochs != 0) {
      dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
    }
  }

  /* conclude sampling */
  if (pctx.sampling) {
    num_samples[0] = num_samples[1] = 0;
    for (std::map<std::string, int>::const_iterator it = pctx.smap->begin();
         it != pctx.smap->end(); ++it) {
      if (it->second == num_epochs) {
        num_samples[1]++;
      }
      num_samples[0]++;
    }
    MPI_Reduce(num_samples, sum_samples, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    if (pctx.my_rank == 0) {
      snprintf(msg, sizeof(msg),
               "########## | >>> total particles sampled: %s (%s valid)",
               pretty_num(sum_samples[0]).c_str(),
               pretty_num(sum_samples[1]).c_str());
      info(msg);
    }
    if (!pctx.nodist) {
      num_names = 0;
      snprintf(path, sizeof(path), "%s/exp-info/NAMES-%07d.txt", pctx.log_home,
               pctx.my_rank);
      if (pctx.my_rank == 0) {
        info("dumping valid particle names to ...");
        info(path);
      }
      fd0 = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
      if (fd0 != -1) {
        tmp = "dumped names = (\n    ...\n";
        for (std::map<std::string, int>::const_iterator it = pctx.smap->begin();
             it != pctx.smap->end(); ++it) {
          if (it->second == num_epochs) {
            n = snprintf(msg, sizeof(msg), "%s\n", it->first.c_str());
            n = write(fd0, msg, n);
            if (n == -1) {
              break;
            }
            num_names++;
            if (num_names <= 7) {
              tmp += " !! ";
              tmp += it->first;
              tmp += "\n";
            }
          }
        }
        tmp += "    ...\n";
        tmp += ")";
        if (num_names != 0 && pctx.my_rank == 0) {
          info(tmp.c_str());
        }
        close(fd0);
        errno = 0;
      } else {
        error("open");
      }
      num_samples[0] = num_names;
      MPI_Reduce(num_samples, sum_samples, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
                 0, MPI_COMM_WORLD);
      if (pctx.my_rank == 0) {
        snprintf(msg, sizeof(msg), "dumping ok (%s names)",
                 pretty_num(sum_samples[0]).c_str());
        info(msg);
      }
    }
  }

  /* close, merge, and dist mon files */
  if (pctx.monfd != -1) {
    if (!pctx.nodist) {
      ok = 1; /* ready to go */

      if (pctx.my_rank == 0) {
        fd1 = fd2 = -1;
        info("merging and saving epoch mon stats to ...");
        ts = now_micros();
        if (mon_dump_bin) {
          snprintf(path, sizeof(path), "%s/exp-info/DUMP-mon.bin",
                   pctx.log_home);
          info(path);
          fd1 = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
          if (fd1 == -1) {
            error("open");
            ok = 0;
          }
        }
        if (mon_dump_txt) {
          snprintf(path, sizeof(path), "%s/exp-info/DUMP-mon.txt",
                   pctx.log_home);
          info(path);
          fd2 = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
          if (fd2 == -1) {
            error("open");
            ok = 0;
          }
        }
      }

      if (ok) {
        n = lseek(pctx.monfd, 0, SEEK_SET);
        if (n != 0) {
          ok = 0;
        }
      }

      epoch = 0;

      while (epoch != num_epochs) {
        if (ok) {
          n = read(pctx.monfd, buf, sizeof(buf));
          if (n == sizeof(buf)) {
            assert(sizeof(mon_ctx_t) < sizeof(buf));
            memcpy(&local, buf, sizeof(mon_ctx_t));
          } else {
            warn("cannot read stats");
            ok = 0;
          }
        }

        MPI_Allreduce(&ok, &go, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

        if (go) {
          /* per-rank total writes = local writes + foreign writes */
          num_writes = local.nlw + local.nfw;
          MPI_Reduce(&num_writes, &min_writes, 1, MPI_UNSIGNED_LONG_LONG,
                     MPI_MIN, 0, MPI_COMM_WORLD);
          MPI_Reduce(&num_writes, &max_writes, 1, MPI_UNSIGNED_LONG_LONG,
                     MPI_MAX, 0, MPI_COMM_WORLD);
          mon_reinit(&glob);
          mon_reduce(&local, &glob);
          glob.epoch_seq = epoch + 1;
          glob.global = 1;
        } else if (pctx.my_rank == 0) {
          snprintf(msg, sizeof(msg),
                   "error merging mon stats %d; "
                   "ABORT action!",
                   epoch + 1);
          warn(msg);
        }

        if (go) {
          if (pctx.my_rank == 0) {
            if (mon_dump_txt) mon_dumpstate(fd2, &glob);
            if (mon_dump_bin) {
              memset(buf, 0, sizeof(buf));
              assert(sizeof(buf) > sizeof(mon_ctx_t));
              memcpy(buf, &glob, sizeof(mon_ctx_t));
              n = write(fd1, buf, sizeof(buf));
              errno = 0;
            }
            if (sizeof(buf) != 0) {
              ucpu =
                  100 * double(glob.cpu_stat.usr_micros) / glob.cpu_stat.micros;
              scpu =
                  100 * double(glob.cpu_stat.sys_micros) / glob.cpu_stat.micros;
              snprintf(msg, sizeof(msg),
                       " @ epoch #%-3d %s - %s  (%d%% - %d%% cpu usage)",
                       epoch + 1, pretty_dura(glob.min_dura).c_str(),
                       pretty_dura(glob.max_dura).c_str(),
                       glob.cpu_stat.min_cpu, glob.cpu_stat.max_cpu);
              info(msg);
              snprintf(msg, sizeof(msg),
                       "       > avg cpu: %.2f%% user + %.2f%% system ="
                       " %.2f%% total",
                       ucpu, scpu, ucpu + scpu);
              info(msg);
              snprintf(msg, sizeof(msg),
                       "   > %s particle writes (%s collisions), %s per rank "
                       "(min: %s, max: %s)",
                       pretty_num(glob.nw).c_str(),
                       pretty_num(glob.ncw).c_str(),
                       pretty_num(double(glob.nw) / pctx.comm_sz).c_str(),
                       pretty_num(glob.min_nw).c_str(),
                       pretty_num(glob.max_nw).c_str());
              info(msg);
              snprintf(msg, sizeof(msg),
                       "         > %s foreign + %s local = %s total writes",
                       pretty_num(glob.nfw).c_str(),
                       pretty_num(glob.nlw).c_str(),
                       pretty_num(glob.nfw + glob.nlw).c_str());
              info(msg);
              snprintf(msg, sizeof(msg),
                       "               > %s per rank (min: %s, max: %s)",
                       pretty_num(double(glob.nfw + glob.nlw) / pctx.comm_sz)
                           .c_str(),
                       pretty_num(min_writes).c_str(),
                       pretty_num(max_writes).c_str());
              info(msg);
              snprintf(msg, sizeof(msg),
                       "     > %s sst data (+%.3f%%), %s sst indexes (+%.3f%%),"
                       " %s bloom filter (+%.3f%%)",
                       pretty_size(glob.dir_stat.total_dblksz).c_str(),
                       glob.dir_stat.total_datasz
                           ? (1.0 * glob.dir_stat.total_dblksz /
                                  glob.dir_stat.total_datasz -
                              1.0) *
                                 100.0
                           : 0,
                       pretty_size(glob.dir_stat.total_iblksz).c_str(),
                       glob.dir_stat.total_datasz
                           ? (1.0 * glob.dir_stat.total_iblksz /
                              glob.dir_stat.total_datasz) *
                                 100.0
                           : 0,
                       pretty_size(glob.dir_stat.total_fblksz).c_str(),
                       glob.dir_stat.total_datasz
                           ? (1.0 * glob.dir_stat.total_fblksz /
                              glob.dir_stat.total_datasz) *
                                 100.0
                           : 0);
              info(msg);
              snprintf(
                  msg, sizeof(msg),
                  "           > %s sst, %s per rank, %.1f per mem partition",
                  pretty_num(glob.dir_stat.num_sstables).c_str(),
                  pretty_num(double(glob.dir_stat.num_sstables) / pctx.comm_sz)
                      .c_str(),
                  pctx.plfsparts
                      ? double(glob.dir_stat.num_sstables) / pctx.comm_sz /
                            pctx.plfsparts
                      : 0);
              info(msg);
              snprintf(msg, sizeof(msg),
                       "     > %s keys (%s dropped),"
                       " %s per rank (min: %s, max %s)",
                       pretty_num(glob.dir_stat.num_keys).c_str(),
                       pretty_num(glob.dir_stat.num_dropped_keys).c_str(),
                       pretty_num(double(glob.dir_stat.num_keys) / pctx.comm_sz)
                           .c_str(),
                       pretty_num(glob.dir_stat.min_num_keys).c_str(),
                       pretty_num(glob.dir_stat.max_num_keys).c_str());
              info(msg);
              snprintf(
                  msg, sizeof(msg), "         > %s table data, %s, %s per rank",
                  pretty_size(glob.dir_stat.total_datasz).c_str(),
                  pretty_bw(glob.dir_stat.total_datasz, glob.max_dura).c_str(),
                  pretty_bw(double(glob.dir_stat.total_datasz) / pctx.comm_sz,
                            glob.max_dura)
                      .c_str());
              info(msg);
              snprintf(
                  msg, sizeof(msg), "             > %s per op",
                  pretty_dura(double(glob.max_dura) / glob.nw * pctx.comm_sz)
                      .c_str());
              info(msg);
              snprintf(msg, sizeof(msg),
                       "   > %s rpc sent (%s replied), %s per rank "
                       "(min: %s, max: %s)",
                       pretty_num(glob.nms).c_str(),
                       pretty_num(glob.nmd).c_str(),
                       pretty_num(double(glob.nms) / pctx.comm_sz).c_str(),
                       pretty_num(glob.min_nms).c_str(),
                       pretty_num(glob.max_nms).c_str());
              info(msg);
              snprintf(
                  msg, sizeof(msg), "       > %s, %s per rank",
                  pretty_tput(glob.nms, glob.max_dura).c_str(),
                  pretty_tput(double(glob.nms) / pctx.comm_sz, glob.max_dura)
                      .c_str());
              info(msg);
              snprintf(msg, sizeof(msg),
                       "   > %s rpc recv, %s per rank (min: %s, max: %s)",
                       pretty_num(glob.nmr).c_str(),
                       pretty_num(double(glob.nmr) / pctx.comm_sz).c_str(),
                       pretty_num(glob.min_nmr).c_str(),
                       pretty_num(glob.max_nmr).c_str());
              info(msg);
              snprintf(
                  msg, sizeof(msg), "       > %s, %s per rank",
                  pretty_tput(glob.nmr, glob.max_dura).c_str(),
                  pretty_tput(double(glob.nmr) / pctx.comm_sz, glob.max_dura)
                      .c_str());
              info(msg);
              snprintf(
                  msg, sizeof(msg), "           > %s per rpc",
                  pretty_dura(double(glob.max_dura) / glob.nmr * pctx.comm_sz)
                      .c_str());
              info(msg);
            }

            errno = 0;
          }
        } else {
          break;
        }

        epoch++;
      }

      if (pctx.my_rank == 0) {
        if (fd1 != -1) {
          close(fd1);
        }
        if (fd2 != -1) {
          close(fd2);
        }
        diff = now_micros() - ts;
        snprintf(msg, sizeof(msg), "merging ok (%d epochs) %s", epoch,
                 pretty_dura(diff).c_str());
        info(msg);
      }
    }

    close(pctx.monfd);
    pctx.monfd = -1;
  }

  /* close testing log file */
  if (pctx.logfd != -1) {
    close(pctx.logfd);
    pctx.logfd = -1;
  }

  /* !!! OK !!! */
  rv = nxt.MPI_Finalize();
  if (pctx.my_rank == 0) info("all done");
  if (pctx.my_rank == 0) info("bye");
  return rv;
}

/*
 * chdir
 */
int chdir(const char* dir) {
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  rv = nxt.chdir(dir);
  if (rv) msg_abort("chdir");

  return rv;
}

/*
 * mkdir
 */
int mkdir(const char* dir, mode_t mode) {
  int exact;
  const char* stripped;
  char path[PATH_MAX];
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_path(dir, &exact)) {
    return nxt.mkdir(dir, mode);
  } else if (under_plfsdir(dir)) {
    return 0; /* plfsdirs are pre-created at MPI_Init */
  }

  /* relative paths we pass through; absolute we strip off prefix */

  if (*dir != '/') {
    stripped = dir;
  } else {
    stripped = (exact) ? "/" : (dir + pctx.len_deltafs_root);
  }

  if (IS_BYPASS_WRITE(pctx.mode)) {
    rv = 0; /* noop */

  } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode) ||
             IS_BYPASS_DELTAFS(pctx.mode)) {
    snprintf(path, sizeof(path), "%s/%s", pctx.local_root, stripped);
    rv = nxt.mkdir(path, mode);
  } else {
    rv = deltafs_mkdir(stripped, mode);
  }

  if (rv) msg_abort("xxmkdir");

  return rv;
}

/*
 * opendir
 */
DIR* opendir(const char* dir) {
  int ignored_exact;
  char msg[100];
  dir_stat_t tmp_stat;
  uint64_t epoch_start;
  uint64_t flush_start;
  uint64_t flush_end;
  DIR* rv;

  int ret = pthread_once(&init_once, preload_init);
  if (ret) msg_abort("pthread_once");

  if (!claim_path(dir, &ignored_exact)) {
    return nxt.opendir(dir);
  } else if (!under_plfsdir(dir)) {
    return NULL; /* not supported */
  }

  /* return a fake DIR* since we don't actually open */
  rv = reinterpret_cast<DIR*>(&fake_dirptr);

  /* initialize tmp mon stats */
  if (!pctx.nomon) {
    memset(&tmp_stat, 0, sizeof(dir_stat_t));
    epoch_start = now_micros();
  }

  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "epoch %d begins (rank 0)", num_epochs + 1);
    info(msg);
  }

  if (num_epochs != 0 && pctx.paranoid_barrier) {
    /*
     * this ensures we have received all peer writes and no more
     * writes will happen for the previous epoch.
     */
    preload_barrier(MPI_COMM_WORLD);
  }

  /* shuffle flush */
  if (num_epochs != 0 && !IS_BYPASS_SHUFFLE(pctx.mode)) {
    shuffle_epoch_start(&pctx.sctx);
  }

  /* epoch flush */
  if (num_epochs != 0) {
    /*
     * XXX: explicit epoch flush.
     *
     * unable to perform this at closedir() time because we are
     * likely to progress faster than some peers, causing
     * an epoch to be flushed prematurely and confusing
     * deltafs.
     *
     * could be removed when deltafs supports auto epoch flush though.
     *
     * epoch flush may also be triggered by an unexpected
     * write from a remote peer.
     *
     * XXX: need to send the epoch num to deltafs.
     *
     */
    if (IS_BYPASS_WRITE(pctx.mode)) {
      /* noop */

    } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      if (pctx.plfsh != NULL) {
        if (pctx.my_rank == 0) {
          flush_start = now_micros();
          info("flushing plfsdir ... (rank 0)");
        }
        deltafs_plfsdir_epoch_flush(pctx.plfsh, num_epochs - 1);
        if (pctx.my_rank == 0) {
          flush_end = now_micros();
          snprintf(msg, sizeof(msg), "flushing done %s",
                   pretty_dura(flush_end - flush_start).c_str());
          info(msg);
        }
      } else {
        msg_abort("plfsdir not opened");
      }

    } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode) &&
               !IS_BYPASS_DELTAFS(pctx.mode)) {
      if (pctx.plfsfd != -1) {
        deltafs_epoch_flush(pctx.plfsfd, NULL); /* XXX */
        if (pctx.my_rank == 0) {
          info("plfsdir flushed (rank 0)");
        }
      } else {
        msg_abort("plfsdir not opened");
      }

    } else {
      /* noop */
    }
  }

  if (num_epochs != 0) {
    /*
     * delay dumping mon stats collected from the previous epoch
     * until the beginning of the next epoch, which allows us
     * to get mostly up-to-date stats on the background
     * compaction work.
     */
    dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
  }

  if (num_epochs != 0 && pctx.paranoid_post_barrier) {
    /*
     * this ensures all writes made for the next epoch
     * will go to a new write buffer.
     */
    preload_barrier(MPI_COMM_WORLD);
  }

  /* increase epoch seq */
  num_epochs++;

  if (!pctx.nomon) {
    mon_reinit(&pctx.mctx); /* clear mon stats */
    /* reset epoch id */
    pctx.mctx.epoch_seq = num_epochs;

    pctx.epoch_start = epoch_start; /* record epoch start */

    /* take a snapshot of dir stats */
    pctx.last_dir_stat = tmp_stat;
    /* take a snapshot of sys usage */
    pctx.last_sys_usage_snaptime = now_micros();
    ret = getrusage(RUSAGE_SELF, &pctx.last_sys_usage);
    if (ret) msg_abort("getrusage");
  }

  if (pctx.my_rank == 0) {
    info("dumping particles ... (rank 0)");
  }

  pctx.fnames->clear();

  return rv;
}

/*
 * closedir
 */
int closedir(DIR* dirp) {
  uint64_t tmp_usage_snaptime;
  struct rusage tmp_usage;
  double cpu;
  uint64_t flush_start;
  uint64_t flush_end;
  char msg[100];
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (dirp != reinterpret_cast<DIR*>(&fake_dirptr)) {
    return nxt.closedir(dirp);
  }

  if (pctx.my_rank == 0) {
    info("dumping done (rank 0)");
  }

  if (pctx.paranoid_checks) {
    if (!pctx.isdeltafs->empty()) {
      msg_abort("some plfsdir files still open!");
    }
    pctx.fnames->clear();
  }

  if (!pctx.nomon) {
    tmp_usage_snaptime = now_micros();
    rv = getrusage(RUSAGE_SELF, &tmp_usage);
    if (rv) msg_abort("getrusage");
    pctx.mctx.cpu_stat.micros =
        pctx.my_cpus * (tmp_usage_snaptime - pctx.last_sys_usage_snaptime);
    pctx.mctx.cpu_stat.sys_micros =
        timeval_to_micros(&tmp_usage.ru_stime) -
        timeval_to_micros(&pctx.last_sys_usage.ru_stime);
    pctx.mctx.cpu_stat.usr_micros =
        timeval_to_micros(&tmp_usage.ru_utime) -
        timeval_to_micros(&pctx.last_sys_usage.ru_utime);

    cpu = 100 * double(pctx.mctx.cpu_stat.sys_micros +
                       pctx.mctx.cpu_stat.usr_micros) /
          pctx.mctx.cpu_stat.micros;

    pctx.mctx.cpu_stat.min_cpu = int(floor(cpu));
    pctx.mctx.cpu_stat.max_cpu = int(ceil(cpu));
  }

  /* drain on-going rpc */
  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    shuffle_epoch_end(&pctx.sctx);
  }

  /* this ensures we have received all peer messages */
  if (pctx.paranoid_pre_barrier) {
    preload_barrier(MPI_COMM_WORLD);
  }

  /* epoch pre-flush */
  if (pctx.pre_flushing) {
    if (IS_BYPASS_WRITE(pctx.mode)) {
      /* noop */

    } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      if (pctx.plfsh != NULL) {
        if (pctx.my_rank == 0) {
          flush_start = now_micros();
          info("pre-flushing plfsdir ... (rank 0)");
        }
        deltafs_plfsdir_flush(pctx.plfsh, num_epochs - 1);
        if (pctx.my_rank == 0) {
          flush_end = now_micros();
          snprintf(msg, sizeof(msg), "pre-flushing done %s",
                   pretty_dura(flush_end - flush_start).c_str());
          info(msg);
        }
      } else {
        msg_abort("plfsdir not opened");
      }

    } else {
      /* XXX */
    }
  }

  /* record epoch duration */
  if (!pctx.nomon) {
    pctx.mctx.max_dura = now_micros() - pctx.epoch_start;
    pctx.mctx.min_dura = pctx.mctx.max_dura;
    if (pctx.my_rank == 0) {
      snprintf(msg, sizeof(msg), "epoch %s (rank 0)",
               pretty_dura(pctx.mctx.max_dura).c_str());
      info(msg);
    }
  }

  if (pctx.my_rank == 0) {
    info("epoch ends (rank 0)");
  }

  return 0;
}

/*
 * fopen
 */
FILE* fopen(const char* fpath, const char* mode) {
  int exact;
  const char* stripped;
  const char* fname;
  FILE* rv;

  int ret = pthread_once(&init_once, preload_init);
  if (ret) msg_abort("pthread_once");

  if (should_ignore(fpath)) {
    return nxt.fopen("/dev/null", mode);
  } else if (!claim_path(fpath, &exact)) {
    return nxt.fopen(fpath, mode);
  } else if (!under_plfsdir(fpath)) {
    return NULL; /* XXX: support this */
  }

  /* relative paths we pass through; absolute we strip off prefix */

  if (*fpath != '/') {
    stripped = fpath;
  } else {
    stripped = (exact) ? "/" : (fpath + pctx.len_deltafs_root);
  }

  pthread_mtx_lock(&preload_mtx);
  if (pctx.paranoid_checks) {
    fname = stripped + pctx.len_plfsdir + 1;
    if (pctx.fnames->count(std::string(fname)) == 0) {
      pctx.fnames->insert(std::string(fname));
    } else {
      pctx.mctx.ncw++;
    }
  }
  pctx.mctx.min_nw++;
  pctx.mctx.max_nw++;
  pctx.mctx.nw++;
  /* allocate a fake FILE* and put it in the set */
  fake_file* ff = NULL;
  if (vpic_file != &vpic_file_buffer) {
    ff = new fake_file(stripped);
    warn("vpic is opening multiple particle files simultaneously");
  } else {
    ff = vpic_file;
    ff->reset(stripped);
    vpic_file = NULL;
  }
  rv = reinterpret_cast<FILE*>(ff);
  assert(pctx.isdeltafs != NULL);
  pctx.isdeltafs->insert(rv);
  pthread_mtx_unlock(&preload_mtx);

  return rv;
}

/*
 * fwrite
 */
size_t fwrite(const void* ptr, size_t size, size_t nitems, FILE* stream) {
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fwrite(ptr, size, nitems, stream);
  }

  fake_file* ff = reinterpret_cast<fake_file*>(stream);
  size_t cnt = ff->add_data(ptr, size * nitems);

  /*
   * fwrite returns number of items written.  it can return a short
   * object count on error.
   */

  return (cnt / size); /* truncates on error */
}

/*
 * fclose.   returns EOF on error.
 */
int fclose(FILE* stream) {
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fclose(stream);
  }

  fake_file* ff = reinterpret_cast<fake_file*>(stream);

  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    rv = shuffle_write(&pctx.sctx, ff->file_name(), ff->data(), ff->size(),
                       num_epochs - 1);
    if (rv) {
      msg_abort("xxshuffle");
    }
  } else {
    rv = preload_local_write(ff->file_name(), ff->data(), ff->size(),
                             num_epochs - 1);
    if (rv) {
      msg_abort("xxwrite");
    }
  }

  pthread_mtx_lock(&preload_mtx);
  assert(pctx.isdeltafs != NULL);
  pctx.isdeltafs->erase(stream);
  if (ff == &vpic_file_buffer) {
    vpic_file = &vpic_file_buffer;
  } else {
    delete ff;
  }
  pthread_mtx_unlock(&preload_mtx);

  return rv;
}

/*
 * the rest of these we do not override for deltafs.   if we get a
 * deltafs FILE*, we've got a serious problem and we abort...
 */

/*
 * feof
 */
int feof(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.feof(stream);
  }

  errno = ENOTSUP;
  msg_abort("feof!");
  return 0;
}

/*
 * ferror
 */
int ferror(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.ferror(stream);
  }

  errno = ENOTSUP;
  msg_abort("ferror!");
  return 0;
}

/*
 * clearerr
 */
void clearerr(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    nxt.clearerr(stream);
    return;
  }

  errno = ENOTSUP;
  msg_abort("clearerr!");
}

/*
 * fread
 */
size_t fread(void* ptr, size_t size, size_t nitems, FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fread(ptr, size, nitems, stream);
  }

  errno = ENOTSUP;
  msg_abort("fread!");
  return 0;
}

/*
 * fseek
 */
int fseek(FILE* stream, long offset, int whence) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fseek(stream, offset, whence);
  }

  errno = ENOTSUP;
  msg_abort("fseek!");
  return 0;
}

/*
 * ftell
 */
long ftell(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) msg_abort("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.ftell(stream);
  }

  errno = ENOTSUP;
  msg_abort("ftell!");
  return 0;
}

} /* extern "C" */

/*
 * preload_write
 */
int preload_write(const char* fn, char* data, size_t len, int epoch) {
  int rv;
  char buf[PRELOAD_PARTICLE_SIZE];
  char path[PATH_MAX];
  const char* fname;
  ssize_t n;
  int fd;
  int k;

  assert(pctx.plfsdir != NULL);
  /* remove parent directory path */
  fname = fn + pctx.len_plfsdir + 1;
  errno = 0;

  pthread_mtx_lock(&write_mtx);

  if (pctx.fake_data) {
    memset(buf, 0, sizeof(buf));
    k = pdlfs::xxhash32(fname, strlen(fname), 0);
    snprintf(buf, sizeof(buf), "key=%08x, epoch=%d\n", k, epoch);
    len = sizeof(buf);
    data = buf;
  }

  if (pctx.paranoid_checks) {
    if (len != PRELOAD_PARTICLE_SIZE) {
      msg_abort("bad write size!");
    }
    if (epoch != num_epochs - 1) {
      msg_abort("bad epoch!");
    }
  }

  if (pctx.sampling) {
    if (epoch == 0) {
      /* during the initial epoch, we accept as many names as possible */
      if (getr(0, 1000000 - 1) < pctx.sthres) {
        pctx.smap->insert(std::make_pair(std::string(fname), 1));
      }
    } else {
      if (pctx.smap->count(std::string(fname)) != 0) {
        pctx.smap->at(std::string(fname))++;
      }
    }
  }

  rv = EOF; /* Return 0 on success, or EOF on errors */

  if (IS_BYPASS_WRITE(pctx.mode)) {
    rv = 0; /* noop */

  } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
    if (pctx.plfsh == NULL) {
      msg_abort("plfsdir not opened");
    }

    rv = deltafs_plfsdir_append(pctx.plfsh, fname, epoch, data, len);

    if (rv != 0) {
      rv = EOF;
    }

  } else if (IS_BYPASS_DELTAFS(pctx.mode)) {
    snprintf(path, sizeof(path), "%s/%s", pctx.local_root, fn);
    fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);

    if (fd != -1) {
      n = write(fd, data, len);
      if (n == len) {
        rv = 0;
      }
      close(fd);
    }
  } else {
    if (pctx.plfsfd == -1) {
      msg_abort("plfsdir not opened");
    }

    fd =
        deltafs_openat(pctx.plfsfd, fname, O_WRONLY | O_CREAT | O_APPEND, 0644);

    if (fd != -1) {
      n = deltafs_write(fd, data, len);
      if (n == len) {
        rv = 0;
      }
      deltafs_close(fd);
    }
  }

  pthread_mtx_unlock(&write_mtx);

  return rv;
}
