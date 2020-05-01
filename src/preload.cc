/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <mpi.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>

#include <map>
#include <string>
#include <vector>

#include "preload_internal.h"
#include "pthreadtap.h"

#ifdef PRELOAD_HAS_PAPI
#include <papi.h>
#endif

/* default particle format */
#define DEFAULT_PARTICLE_ID_BYTES 8 /* particle filename length */
#define DEFAULT_PARTICLE_EXTRA_BYTES 0
#define DEFAULT_PARTICLE_BYTES 40 /* particle payload */
#define DEFAULT_PARTICLE_BUFSIZE (2 << 20)

/*
 * default number of million seconds for asynchronous MPI barrier
 * (MPI_Iallreduce in fact) completion probing (MPI_Test).
 */
#define DEFAULT_MPI_WAIT 50

/* mon output */
static int mon_dump_bin = 0;
static int mon_dump_txt = 0;

/* mutex to protect preload state */
static pthread_mutex_t preload_mtx = PTHREAD_MUTEX_INITIALIZER;

/* mutex to synchronize writes */
static pthread_mutex_t write_mtx = PTHREAD_MUTEX_INITIALIZER;

/* number of pthread created */
static int num_pthreads = 0;

/* number of MPI barriers invoked by app */
static int num_barriers = 0;

/* number of epochs generated */
// XXX: removed static since we're stealing it read-only in preload_range
int num_eps = 0;

/*
 * we use the address of fake_dirptr as a fake DIR* with opendir/closedir
 */
static int fake_dirptr = 0;

/*
 * next_functions: libc replacement functions we are providing to the preloader.
 */
static struct next_functions {
  /* functions we need */
  int (*MPI_Init)(int* argc, char*** argv);
  int (*MPI_Finalize)(void);
  int (*MPI_Barrier)(MPI_Comm comm);
  int (*pthread_create)(pthread_t* thread, const pthread_attr_t* attr,
                        void* (*)(void*), void* arg);
  int (*chdir)(const char* path);
  int (*mkdir)(const char* path, mode_t mode);
  DIR* (*opendir)(const char* filename);
  int (*closedir)(DIR* dirp);
  FILE* (*fopen)(const char* filename, const char* mode);
  size_t (*fwrite)(const void* ptr, size_t size, size_t nitems, FILE* stream);
  int (*fputc)(int character, FILE* stream);
  int (*fclose)(FILE* stream);

  /* for error catching we do these */
  int (*feof)(FILE* stream);
  int (*ferror)(FILE* stream);
  void (*clearerr)(FILE* stream);
  size_t (*fread)(void* ptr, size_t size, size_t nitems, FILE* stream);
  int (*fseek)(FILE* stream, long offset, int whence);
  long (*ftell)(FILE* stream);
} nxt = {0};

/*
 * this once is used to trigger the init of the preload library...
 */
static pthread_once_t init_once = PTHREAD_ONCE_INIT;

/* helper: must_getnextdlsym: get next symbol or fail */
static void must_getnextdlsym(void** result, const char* symbol) {
  *result = dlsym(RTLD_NEXT, symbol);
  if (*result == NULL) ABORT(symbol);
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
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.pthread_create),
                    "pthread_create");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.chdir), "chdir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.mkdir), "mkdir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.opendir), "opendir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.closedir), "closedir");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fopen), "fopen");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fwrite), "fwrite");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fputc), "fputc");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fclose), "fclose");

  must_getnextdlsym(reinterpret_cast<void**>(&nxt.feof), "feof");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.ferror), "ferror");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.clearerr), "clearerr");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fread), "fread");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.fseek), "fseek");
  must_getnextdlsym(reinterpret_cast<void**>(&nxt.ftell), "ftell");

  pctx.plfsfd = -1;
  pctx.monfd = -1;

  pctx.trace = NULL;

#ifdef PRELOAD_HAS_PAPI
  pctx.papi_events = new std::vector<const char*>;
  pctx.papi_set = PAPI_NULL;
#endif

  pctx.isdeltafs = new std::set<FILE*>;
  pctx.fnames = new std::set<std::string>;
  pctx.smap = new std::map<std::string, int>;

  pctx.mpi_wait = DEFAULT_MPI_WAIT;
  pctx.particle_id_size = DEFAULT_PARTICLE_ID_BYTES;
  pctx.particle_extra_size = DEFAULT_PARTICLE_EXTRA_BYTES;
  pctx.particle_size = DEFAULT_PARTICLE_BYTES;
  pctx.particle_buf_size = DEFAULT_PARTICLE_BUFSIZE;
  pctx.particle_count = 0;
  pctx.sthres = 100; /* 100 samples per 1 million input */

  pctx.sampling = 1;
  pctx.paranoid_checks = 1;
  pctx.paranoid_barrier = 1;
  pctx.paranoid_post_barrier = 1;
  pctx.paranoid_pre_barrier = 1;
  pctx.pre_flushing = 1;
  pctx.pre_flushing_sync = 1;
  pctx.pre_flushing_wait = 1;
  pctx.my_rank = 0;
  pctx.comm_sz = 1;
  pctx.recv_comm = MPI_COMM_NULL;
  pctx.recv_rank = -1;
  pctx.recv_sz = -1;

  /* obtain deltafs mount point */
  pctx.deltafs_mntp = maybe_getenv("PRELOAD_Deltafs_mntp");
  if (pctx.deltafs_mntp != NULL) {
    pctx.len_deltafs_mntp = strlen(pctx.deltafs_mntp);
  }
  /* deltafs mount point:
   *   - may be NULL or empty, otherwise,
   *   - not "/", and
   *   - not ending in "/"
   */
  if (pctx.len_deltafs_mntp != 0) {
    if ((pctx.len_deltafs_mntp == 1 && pctx.deltafs_mntp[0] == '/') ||
        pctx.deltafs_mntp[pctx.len_deltafs_mntp - 1] == '/') {
      ABORT("bad deltafs_mntp");
    }
    /*
     * if deltafs is not mounted, skip plfsdir
     * otherwise, we obtain path to it
     */
    pctx.plfsdir = maybe_getenv("PRELOAD_Plfsdir");

    /* plfsdir:
     *   - if null, no plfsdir will ever be created
     *   - otherwise, it will be created and opened at MPI_Init
     */
    if (pctx.plfsdir == NULL) {
      if (pctx.deltafs_mntp[0] != '/') {
        /* default to deltafs_root if deltafs_root is relative */
        pctx.plfsdir = pctx.deltafs_mntp;
      }
    }
    if (pctx.plfsdir != NULL) {
      pctx.len_plfsdir = strlen(pctx.plfsdir);
    }
  }

  tmp = maybe_getenv("PRELOAD_Ignore_dirs");
  if (tmp != NULL && tmp[0] != 0) {
    for (const char* ch = strchr(tmp, ':'); ch != NULL;) {
      if (ch != tmp) {
        paths.push_back(std::make_pair(tmp, static_cast<size_t>(ch - tmp)));
      }
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
          ABORT("bad ignore_dir");
        if (pctx.ignore_dirs[i][pctx.len_ignore_dirs[i] - 1] == '/')
          ABORT("bad ignore_dir");
      }
    }
  }

  /* obtain path to log home */
  pctx.log_home = maybe_getenv("PRELOAD_Log_home");
  if (pctx.log_home == NULL) pctx.log_home = DEFAULT_TMP_DIR;
  pctx.len_log_home = strlen(pctx.log_home);

  /* log home:
   *   - any non-null path,
   *   - not "/",
   *   - starting with "/", and
   *   - not ending in "/"
   */
  if (pctx.len_log_home == 0 || pctx.len_log_home == 1 ||
      pctx.log_home[0] != '/' || pctx.log_home[pctx.len_log_home - 1] == '/')
    ABORT("bad log_root");

  /* obtain path to local file system root */
  pctx.local_root = maybe_getenv("PRELOAD_Local_root");
  if (pctx.local_root == NULL) pctx.local_root = DEFAULT_TMP_DIR;
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
    ABORT("bad local_root");

  tmp = maybe_getenv("PRELOAD_Number_particles_per_rank");
  if (tmp != NULL) {
    pctx.particle_count = atoi(tmp);
    if (pctx.particle_count < 0) {
      ABORT("bad particle count");
    }
  }

  tmp = maybe_getenv("PRELOAD_Particle_buf_size");
  if (tmp != NULL) {
    pctx.particle_buf_size = atoi(tmp);
    if (pctx.particle_buf_size <= 0) {
      ABORT("bad particle buf size");
    }
  }

  tmp = maybe_getenv("PRELOAD_Particle_id_size");
  if (tmp != NULL) {
    pctx.particle_id_size = atoi(tmp);
    if (pctx.particle_id_size <= 0) {
      ABORT("bad particle id size");
    }
  }

  tmp = maybe_getenv("PRELOAD_Particle_extra_size");
  if (tmp != NULL) {
    pctx.particle_extra_size = atoi(tmp);
    if (pctx.particle_extra_size < 0) {
      pctx.particle_extra_size = 0;
    }
  }

  tmp = maybe_getenv("PRELOAD_Particle_size");
  if (tmp != NULL) {
    pctx.particle_size = atoi(tmp);
    if (pctx.particle_size < 0) {
      pctx.particle_size = 0;
    }
  }

  tmp = maybe_getenv("PRELOAD_Bg_threads");
  if (tmp != NULL) {
    pctx.bgdepth = atoi(tmp);
    if (pctx.bgdepth < 1) {
      pctx.bgdepth = 1;
    }
  }

  tmp = maybe_getenv("PRELOAD_Mpi_wait");
  if (tmp != NULL) {
    pctx.mpi_wait = atoi(tmp);
    if (pctx.mpi_wait < 0) {
      pctx.mpi_wait = -1;
    }
  }

  if (is_envset("PRELOAD_Skip_sampling")) pctx.sampling = 0;

  tmp = maybe_getenv("PRELOAD_Sample_threshold");
  if (tmp != NULL) {
    pctx.sthres = atoi(tmp);
    if (pctx.sthres < 1) {
      pctx.sthres = 1;
    }
  }

#ifdef PRELOAD_HAS_PAPI
  tmp = maybe_getenv("PRELOAD_Papi_events");
  if (tmp == NULL || tmp[0] == 0) {
    pctx.papi_events->push_back("PAPI_L2_TCM");  // L2 total cache misses
    pctx.papi_events->push_back("PAPI_L2_TCA");  // ... and accesses
  } else {
    char* str = strdup(tmp);
    pctx.papi_events->push_back(str);
    for (char* c = strchr(str, ';'); c != NULL; c = strchr(c + 1, ';')) {
      c[0] = 0;
      if (pctx.papi_events->back()[0] == 0) {
        pctx.papi_events->pop_back();
      }
      if (c[1] != 0) {
        pctx.papi_events->push_back(c + 1);
      }
    }
  }
#endif

  tmp = maybe_getenv("PRELOAD_Pthread_tap");
  if (tmp != NULL) {
    pctx.pthread_tap = atoi(tmp);
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
  if (is_envset("PRELOAD_Skip_papi")) pctx.nopapi = 1;
  if (is_envset("PRELOAD_Skip_mon_dist")) pctx.nodist = 1;
  if (is_envset("PRELOAD_Enable_verbose_mode")) pctx.verbose = 1;
  if (is_envset("PRELOAD_Print_meminfo")) pctx.print_meminfo = 1;
  if (is_envset("PRELOAD_Enable_bg_pause")) pctx.bgpause = 1;
  if (is_envset("PRELOAD_Enable_bloomy")) pctx.sideft = 1;
  if (is_envset("PRELOAD_Enable_wisc")) pctx.sideio = 1;

  if (is_envset("PRELOAD_No_paranoid_checks")) pctx.paranoid_checks = 0;
  if (is_envset("PRELOAD_No_paranoid_pre_barrier"))
    pctx.paranoid_pre_barrier = 0;
  if (is_envset("PRELOAD_No_epoch_pre_flushing")) pctx.pre_flushing = 0;
  if (is_envset("PRELOAD_No_epoch_pre_flushing_wait"))
    pctx.pre_flushing_wait = 0;
  if (is_envset("PRELOAD_No_epoch_pre_flushing_sync"))
    pctx.pre_flushing_sync = 0;
  if (is_envset("PRELOAD_No_paranoid_barrier")) pctx.paranoid_barrier = 0;
  if (is_envset("PRELOAD_No_paranoid_post_barrier"))
    pctx.paranoid_post_barrier = 0;
  if (is_envset("PRELOAD_No_sys_probing")) pctx.noscan = 1;
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
  if (pctx.len_deltafs_mntp == 0) return 0;
  if (strncmp(pctx.deltafs_mntp, path, pctx.len_deltafs_mntp) != 0) return 0;
  if (path[pctx.len_deltafs_mntp] != '/' &&
      path[pctx.len_deltafs_mntp] != '\0') {
    return 0;
  }
  /* if we've just got pctx.root, caller may convert it to a "/" */
  *exact = int(path[pctx.len_deltafs_mntp] == '\0');
  return 1;
}

/*
 * under_plfsdir: if a given path is a plfsdir or plfsdir files
 */
static int under_plfsdir(const char* path) {
  if (pctx.len_plfsdir == 0) return 0;
  if (strncmp(pctx.plfsdir, path, pctx.len_plfsdir) != 0) return 0;
  if (path[pctx.len_plfsdir] == 0 || path[pctx.len_plfsdir] == '/') return 1;
  return 0;
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
  char data_[255];   /* enough for one VPIC particle */
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
fake_file the_stock_file;
fake_file* stock_file = &the_stock_file;

}  // namespace

/*
 * claim_FILE: look at FILE* and see if we claim it
 */
static int claim_FILE(FILE* stream) {
  std::set<FILE*>::iterator it;
  int rv;

  pthread_mtx_lock(&preload_mtx);

  if (stream != reinterpret_cast<FILE*>(&the_stock_file)) {
    assert(pctx.isdeltafs != NULL);
    it = pctx.isdeltafs->find(stream);
    rv = (it != pctx.isdeltafs->end());

  } else {
    rv = 1;
  }

  pthread_mtx_unlock(&preload_mtx);

  return rv;
}

/*
 * dump in-memory mon stats to files.
 */
static void dump_mon(mon_ctx_t* mon, dir_stat_t* tmp_stat,
                     const dir_stat_t* prev_stat) {
  char buf[sizeof(mon_ctx_t)];
  uint64_t ts;

  if (!pctx.nomon) {
    /* collect stats from deltafs */
    if (pctx.plfshdl != NULL) {
      mon_fetch_plfsdir_stat(pctx.plfshdl, tmp_stat);
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
    if (pctx.testin && pctx.trace != NULL)
      mon_dumpstate(fileno(pctx.trace), mon);

    if (pctx.monfd != -1) {
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "saving epoch statistics ... (rank 0)");
        ts = now_micros();
      }
      memcpy(buf, mon, sizeof(mon_ctx_t));
      if (write(pctx.monfd, buf, sizeof(mon_ctx_t)) != sizeof(mon_ctx_t))
        ABORT("!write");
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "saving ok %s (rank 0)",
             pretty_dura(now_micros() - ts).c_str());
      }
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
  int force_leveldb_format;
  int unordered_storage;
  int skip_checksums;
  int io_engine;
};

static struct plfsdir_conf dirc = {0};

static void plfsdir_error_printer(const char* msg, void*) {
  logf(LOG_ERRO, msg);
}

/*
 * gen_plfsdir_conf: initialize plfsdir conf and obtain it's string literal.
 */
static std::string gen_plfsdir_conf(int rank, int* io_engine, int* unordered,
                                    int* force_leveldb_fmt) {
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

  n += snprintf(tmp + n, sizeof(tmp) - n, "&key_size=%s", dirc.key_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&value_size=%d",
                pctx.sideio ? 12 : pctx.particle_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&memtable_size=%s",
                dirc.memtable_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&bf_bits_per_key=%s",
                dirc.bits_per_key);

  if (is_envset("PLFSDIR_Use_plaindb")) {
    *io_engine = DELTAFS_PLFSDIR_PLAINDB;
    dirc.io_engine = *io_engine;
    return tmp;
  } else if (is_envset("PLFSDIR_Use_leveldb")) {
    *io_engine = DELTAFS_PLFSDIR_LEVELDB;
    if (is_envset("PLFSDIR_Ldb_force_l0"))
      *io_engine = DELTAFS_PLFSDIR_LEVELDB_L0ONLY;
    if (is_envset("PLFSDIR_Ldb_use_bf"))
      *io_engine = DELTAFS_PLFSDIR_LEVELDB_L0ONLY_BF;
    dirc.io_engine = *io_engine;
    return tmp;
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

  if (is_envset("PLFSDIR_Force_leveldb_format")) {
    dirc.force_leveldb_format = 1;
  }

  if (is_envset("PLFSDIR_Unordered_storage")) {
    dirc.unordered_storage = 1;
  }

  if (is_envset("PLFSDIR_Skip_checksums")) {
    dirc.skip_checksums = 1;
  }

  n += snprintf(tmp + n, sizeof(tmp) - n, "&lg_parts=%s", dirc.lg_parts);
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
  n += snprintf(tmp + n, sizeof(tmp) - n, "&block_padding=%s&tail_padding=%s",
                "false", "false");
  n += snprintf(tmp + n, sizeof(tmp) - n, "&filter=%s-filter", "bloom");
  n += snprintf(tmp + n, sizeof(tmp) - n, "&filter_bits_per_key=%s",
                dirc.bits_per_key);

  *force_leveldb_fmt = dirc.force_leveldb_format;
  *unordered = dirc.unordered_storage;

  *io_engine = DELTAFS_PLFSDIR_DEFAULT;
  dirc.io_engine = *io_engine;

  return tmp;
}

static std::string& pretty_plfsdir_conf(std::string& conf) {
  std::string::size_type pos;
  pos = conf.find('=', 0);
  for (; pos != std::string::npos; pos = conf.find('=', 0))
    conf.replace(pos, 1, "\t->\t");
  pos = conf.find('&', 0);
  for (; pos != std::string::npos; pos = conf.find('&', 0))
    conf.replace(pos, 1, "\n // ");
  conf = std::string("plfsdir_conf = (\n // ") + conf;
  conf += "\n)";
  return conf;
}

/*
 * here are the actual override functions from libc...
 */
extern "C" {

/*
 * MPI_Init
 */
int MPI_Init(int* argc, char*** argv) {
  int exact;
  const char* env;
  const char* stripped;
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
  int unordered;
  int force_leveldb_fmt;
  int io_engine;
  int rv;
  int n;

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  rv = nxt.MPI_Init(argc, argv);
  if (rv == MPI_SUCCESS) {
    MPI_Comm_size(MPI_COMM_WORLD, &pctx.comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &pctx.my_rank);
    uid = getuid();
    if (pctx.my_rank == 0) {
      if (pctx.len_deltafs_mntp != 0) {
        deltafs_major = deltafs_version_major();
        deltafs_minor = deltafs_version_minor();
        deltafs_patch = deltafs_version_patch();
        logf(LOG_INFO, "deltafs %d.%d.%d", deltafs_major, deltafs_minor,
             deltafs_patch);
      }
      logf(LOG_INFO, "LIB initializing ... %s MPI ranks (MPI wait=%d ms)",
           pretty_num(pctx.comm_sz).c_str(), pctx.mpi_wait);
      if (pctx.print_meminfo) {
        print_meminfo();
      }
    }
  } else {
    return rv;
  }

  range_ctx_t* rctx = &pctx.rctx;

  /* init range structures */
  // XXX: revisit this if considering 3-hop etc
  rctx->rank_bins.resize(pctx.comm_sz + 1);
  rctx->rank_bin_count.resize(pctx.comm_sz);
  rctx->rank_bins_ss.resize(pctx.comm_sz + 1);
  rctx->rank_bin_count_ss.resize(pctx.comm_sz);
  rctx->all_pivots.resize(pctx.comm_sz * RANGE_NUM_PIVOTS);
  rctx->all_pivot_widths.resize(pctx.comm_sz);

  rctx->ranks_acked.resize(pctx.comm_sz);
  rctx->ranks_acked_next.resize(pctx.comm_sz);

  rctx->oob_buffer_left.resize(RANGE_MAX_OOB_THRESHOLD);
  rctx->oob_buffer_right.resize(RANGE_MAX_OOB_THRESHOLD);

  std::fill(rctx->ranks_acked.begin(), rctx->ranks_acked.end(), false);
  std::fill(rctx->ranks_acked_next.begin(), rctx->ranks_acked_next.end(),
            false);

  /* Round number is never reset, it keeps monotonically increasing
   * even through all the epochs */
  rctx->nneg_round_num = 0;

  rctx->pvt_round_num = 0;
  rctx->ack_round_num = 0;

  /* Ranks_responded is reset after the end of the previous round
   * because when the next round starts is ambiguous and either
   * a RENEG ACK or a RENEG PIVOT can initiate the next round
   */
  rctx->ranks_responded = 0;

  rctx->ranks_acked_count = 0;
  rctx->ranks_acked_count_next = 0;

  rctx->range_state = range_state_t::RS_INIT;

  if (pctx.my_rank == 0) {
#if MPI_VERSION < 3
    logf(LOG_WARN,
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
    logf(LOG_INFO, mpi_info);
#endif
  }

  if (pctx.my_rank == 0) {
#if defined(MPI_WTIME_IS_GLOBAL)
    MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_WTIME_IS_GLOBAL, &mpi_wtime_is_global,
                      &flag);
    if (flag != 0) {
      if (mpi_wtime_is_global == 0) {
        logf(LOG_WARN,
             "MPI_Wtime() is NOT globally synchronized\n>>> "
             "MPI_WTIME_IS_GLOBAL is 0");
      } else {
        logf(LOG_INFO,
             "MPI_Wtime() is globally synchronized\n>>> "
             "MPI_WTIME_IS_GLOBAL is 1");
      }
    } else {
      logf(LOG_WARN,
           "cannot determine if MPI_Wtime() is global\n>>> "
           "MPI_WTIME_IS_GLOBAL not set");
    }
#else
    logf(LOG_WARN,
         "cannot determine if MPI_Wtime() is global\n>>> "
         "MPI_WTIME_IS_GLOBAL undefined");
#endif
  }

  if (pctx.my_rank == 0) {
#if defined(__INTEL_COMPILER)
    logf(LOG_INFO,
         "[cc] compiled by Intel (icc/icpc) %d.%d.%d %d on %s %s "
         "(__cplusplus: %ld)",
         __INTEL_COMPILER / 100, __INTEL_COMPILER % 100,
         __INTEL_COMPILER_UPDATE, __INTEL_COMPILER_BUILD_DATE, __DATE__,
         __TIME__, __cplusplus);

#elif defined(_CRAYC)
    logf(LOG_INFO,
         "[cc] compiled by Cray (crayc/crayc++) %d.%d on %s %s "
         "(__cplusplus: %ld)",
         _RELEASE, _RELEASE_MINOR, __DATE__, __TIME__, __cplusplus);

#elif defined(__clang__)
    logf(LOG_INFO,
         "[cc] compiled by LLVM/Clang (clang/clang++) %d.%d.%d on %s %s "
         "(__cplusplus: %ld)",
         __clang_major__, __clang_minor__, __clang_patchlevel__, __DATE__,
         __TIME__, __cplusplus);

#elif defined(__GNUC__)
    logf(LOG_INFO,
         "[cc] compiled by GNU (gcc/g++) %d.%d.%d on %s %s "
         "(__cplusplus: %ld)",
         __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__, __DATE__, __TIME__,
         __cplusplus);

#endif
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    logf(LOG_WARN,
         "c/c++ OPTIMIZATION disabled: benchmarks unnecessarily slow\n>>> "
         "recompile with -O1, -O2, or -O3 to enable optimization");
#endif
#ifndef NDEBUG
    logf(LOG_WARN,
         "c/c++ ASSERTIONS enabled: benchmarks unnecessarily slow\n>>> "
         "recompile with \"-DNDEBUG\" to disable assertions");
#endif
  }

  if (pctx.testin) {
    if (pctx.my_rank == 0) {
      logf(LOG_WARN,
           "testing mode: benchmarks unnecessarily slow\n>>> rerun with "
           "\"export PRELOAD_Testing=0\" to "
           "disable testing");
    }

    snprintf(dirpath, sizeof(dirpath), "/tmp/vpic-deltafs-run-%u",
             static_cast<unsigned>(uid));
    snprintf(path, sizeof(path), "%s/vpic-deltafs-trace.log.%d", dirpath,
             pctx.my_rank);

    /* ignore error since directory may exist */
    nxt.mkdir(dirpath, 0777);

    pctx.trace = fopen(path, "w");
    if (pctx.trace != NULL) {
      setvbuf(pctx.trace, NULL, _IOLBF, 0);
    } else {
      ABORT("!fopen");
    }
  }

  /* obtain number of logic cpu cores */
  pctx.my_cpus = my_cpu_cores();

  /* probe system info */
  if (pctx.my_rank == 0) {
#ifndef NDEBUG
    check_clockres();
#endif
    maybe_warn_rlimit(pctx.my_rank, pctx.comm_sz);
    if (!pctx.noscan) try_scan_procfs();
    if (!pctx.noscan) try_scan_sysfs();
    maybe_warn_numa();
#ifndef NDEBUG
    check_sse42();
#endif
  }

  if (pctx.my_rank == 0) {
    if (pctx.len_deltafs_mntp != 0) {
      logf(LOG_INFO, "deltafs is mounted at \"%s\"", pctx.deltafs_mntp);
    } else {
      logf(LOG_INFO, "deltafs is not mounted");
    }
    if (pctx.num_ignore_dirs != 0) {
      fputs(">>> ignore dirs: ", stderr);
      for (size_t i = 0; i < pctx.num_ignore_dirs; i++) {
        fwrite(pctx.ignore_dirs[i], 1, pctx.len_ignore_dirs[i], stderr);
        if (i != pctx.num_ignore_dirs - 1) {
          fputc(',', stderr);
        }
      }
      fputc('\n', stderr);
    }
  }

  if (pctx.len_deltafs_mntp != 0 && pctx.len_plfsdir != 0) {
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "particle id: %d bytes, data: %d (+ %d) bytes",
           pctx.particle_id_size, pctx.particle_size, pctx.particle_extra_size);
    }

    /* everyone is a receiver by default. when shuffle is enabled, some ranks
     * may become sender-only */
    pctx.recv_comm = MPI_COMM_WORLD;

    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "shuffle starting ... (rank 0)");
        if (pctx.print_meminfo) {
          print_meminfo();
        }
      }
      shuffle_init(&pctx.sctx);
      /* ensures all peers have the shuffle ready */
      PRELOAD_Barrier(MPI_COMM_WORLD);
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "shuffle started (rank 0)");
        if (pctx.print_meminfo) {
          print_meminfo();
        }
      }
      if (!shuffle_is_everyone_receiver(&pctx.sctx)) {
        /* rank 0 must be a receiver */
        if (pctx.my_rank == 0)
          assert(shuffle_is_rank_receiver(&pctx.sctx, pctx.my_rank) != 0);
        rv = MPI_Comm_split(
            MPI_COMM_WORLD,
            shuffle_is_rank_receiver(&pctx.sctx, pctx.my_rank) != 0
                ? 1
                : MPI_UNDEFINED,
            pctx.my_rank, &pctx.recv_comm);
        if (rv != MPI_SUCCESS) {
          ABORT("MPI_Comm_split");
        }
      }
    } else {
      if (pctx.my_rank == 0) {
        logf(LOG_WARN, "shuffle bypassed");
      }
    }

    if (pctx.recv_comm != MPI_COMM_NULL) {
      MPI_Comm_rank(pctx.recv_comm, &pctx.recv_rank);
      MPI_Comm_size(pctx.recv_comm, &pctx.recv_sz);
    }
    if (pctx.my_rank == 0) {
      assert(pctx.recv_comm != MPI_COMM_NULL);
      /* the 0th rank must also be the 0th rank in the
       * receiver group */
      assert(pctx.recv_rank == 0);
      assert(pctx.recv_sz != -1);
      logf(LOG_INFO, "recv MPI_Comm formed ---> sz=%d (world_sz=%d)",
           pctx.recv_sz, pctx.comm_sz);
    }

    /* pre-create plfsdirs if there is any */
    if (!IS_BYPASS_WRITE(pctx.mode)) {
      assert(claim_path(pctx.plfsdir, &exact));
      /* relative paths we pass through; absolute we strip off prefix */
      if (pctx.plfsdir[0] != '/') {
        stripped = pctx.plfsdir;
      } else if (!exact) {
        stripped = pctx.plfsdir + pctx.len_deltafs_mntp;
      } else {
        ABORT("bad plfsdir");
      }

      /* rank 0 creates the dir.
       * note that rank 0 is always a receiver. */
      if (pctx.my_rank == 0) {
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
          ABORT("cannot make plfsdir");
        } else {
          logf(LOG_INFO, "plfsdir created (rank 0)");
        }
      }

      /* so everyone sees the dir created */
      PRELOAD_Barrier(MPI_COMM_WORLD);

      /* every receiver opens it */
      if (pctx.recv_comm != MPI_COMM_NULL) {
        if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
          snprintf(path, sizeof(path), "%s/%s", pctx.local_root, stripped);
          assert(pctx.recv_rank != -1);
          conf = gen_plfsdir_conf(pctx.recv_rank, &io_engine, &unordered,
                                  &force_leveldb_fmt);
          env = maybe_getenv("PLFSDIR_Env_name");
          if (env == NULL) {
            env = "posix.unbufferedio";
          }
          pctx.plfshdl =
              deltafs_plfsdir_create_handle(conf.c_str(), O_WRONLY, io_engine);
          deltafs_plfsdir_set_fixed_kv(pctx.plfshdl, 1);
          deltafs_plfsdir_force_leveldb_fmt(pctx.plfshdl, force_leveldb_fmt);
          deltafs_plfsdir_set_unordered(pctx.plfshdl, unordered);
          deltafs_plfsdir_set_side_io_buf_size(pctx.plfshdl,
                                               pctx.particle_buf_size);
          deltafs_plfsdir_set_side_filter_size(pctx.plfshdl,
                                               pctx.particle_count);
          pctx.plfsparts = deltafs_plfsdir_get_memparts(pctx.plfshdl);
          pctx.plfstp = deltafs_tp_init(pctx.bgdepth);
          deltafs_plfsdir_set_thread_pool(pctx.plfshdl, pctx.plfstp);
          pctx.plfsenv = deltafs_env_init(
              1, reinterpret_cast<void**>(const_cast<char**>(&env)));
          deltafs_plfsdir_set_env(pctx.plfshdl, pctx.plfsenv);
          deltafs_plfsdir_set_err_printer(pctx.plfshdl, &plfsdir_error_printer,
                                          NULL);
          rv = deltafs_plfsdir_open(pctx.plfshdl, path);
          if (rv != 0) {
            ABORT("cannot open plfsdir");
          } else {
            if (pctx.my_rank == 0) {
              logf(LOG_INFO,
                   "plfsdir (via deltafs-LT, env=%s, io_engine=%d, "
                   "unordered=%d, leveldb_fmt=%d) opened (rank 0)\n>>> bg "
                   "thread pool size: %d",
                   env, io_engine, unordered, force_leveldb_fmt, pctx.bgdepth);
            }
          }

          if (pctx.sideft) {
            rv = deltafs_plfsdir_filter_open(pctx.plfshdl, path);
            if (rv != 0) {
              ABORT("cannot open plfsdir filter");
            } else {
              if (pctx.my_rank == 0) {
                logf(LOG_INFO, "plfsdir side filter opened\n>>> num keys: %s",
                     pretty_num(pctx.particle_count).c_str());
              }
            }
          }

          if (pctx.sideio) {
            rv = deltafs_plfsdir_io_open(pctx.plfshdl, path);
            if (rv != 0) {
              ABORT("cannot open plfsdir io");
            } else {
              if (pctx.my_rank == 0) {
                logf(LOG_INFO, "plfsdir side io opened\n>>> io buf size: %s",
                     pretty_size(pctx.particle_buf_size).c_str());
              }
            }
          }

          if (pctx.my_rank == 0) {
            if (pctx.verbose) {
              pretty_plfsdir_conf(conf);
              logf(LOG_INFO, conf.c_str());
            }
          }
        } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode) &&
                   !IS_BYPASS_DELTAFS(pctx.mode)) {
          pctx.plfsfd = deltafs_open(stripped, O_WRONLY | O_DIRECTORY, 0);
          if (pctx.plfsfd == -1) {
            ABORT("cannot open plfsdir");
          } else if (pctx.my_rank == 0) {
            logf(LOG_INFO, "plfsdir opened (rank 0)");
          }
        }
      }
    }

    if (!pctx.nomon) {
      snprintf(dirpath, sizeof(dirpath), "/tmp/vpic-deltafs-run-%u",
               static_cast<unsigned>(uid));
      snprintf(path, sizeof(path), "%s/vpic-deltafs-mon.bin.%d", dirpath,
               pctx.my_rank);

      /* ignore error since directory may exist */
      nxt.mkdir(dirpath, 0777);

      pctx.monfd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
      if (pctx.monfd == -1) {
        ABORT("cannot create tmp stats file");
      }

      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "in-mem epoch mon stats %d bytes",
             int(sizeof(mon_ctx_t)));
      }
    }

#ifdef PRELOAD_HAS_PAPI
    if (!pctx.nomon && !pctx.nopapi) {
      assert(pctx.papi_events != NULL);
      if (pctx.papi_events->size() > MAX_PAPI_EVENTS) {
        if (pctx.my_rank == 0)
          logf(LOG_WARN, "too many papi events so some are ignored");
        pctx.papi_events->resize(MAX_PAPI_EVENTS);
      }

      if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
        ABORT("cannot init PAPI");
      }

      rv = PAPI_thread_init(pthread_self);
      if (rv != PAPI_OK) ABORT("cannot init PAPI thread");
      rv = PAPI_create_eventset(&pctx.papi_set);
      if (rv != PAPI_OK) ABORT("cannot init PAPI event set");

      for (std::vector<const char*>::const_iterator it =
               pctx.papi_events->begin();
           it != pctx.papi_events->end(); ++it) {
        if (pctx.my_rank == 0) {
          logf(LOG_INFO, "add papi event: %s", *it);
        }
        rv = PAPI_event_name_to_code(const_cast<char*>(*it), &n);
        if (rv == PAPI_OK) rv = PAPI_add_event(pctx.papi_set, n);
        if (rv != PAPI_OK) {
          ABORT(PAPI_strerror(rv));
        }
      }
    }
#endif

    if (pctx.my_rank == 0) {
      if (pctx.sampling) {
        logf(LOG_INFO, "particle sampling enabled: %s in %s",
             pretty_num(pctx.sthres).c_str(), pretty_num(1000000).c_str());
      } else {
        logf(LOG_WARN, "particle sampling skipped");
      }
      if (pctx.paranoid_checks)
        logf(LOG_WARN,
             "paranoid checks enabled: benchmarks unnecessarily slow "
             "and memory usage unnecessarily high\n>>> "
             "rerun with \"export PRELOAD_No_paranoid_checks=1\" to disable");
      if (pctx.nomon) logf(LOG_WARN, "mon off: some stats not available");

      if (IS_BYPASS_WRITE(pctx.mode)) {
        logf(LOG_WARN, "particle writes bypassed");
      } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
        logf(LOG_WARN, "deltafs metadata bypassed");
      } else if (IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode)) {
        logf(LOG_WARN, "deltafs plfsdir bypassed");
      } else if (IS_BYPASS_DELTAFS(pctx.mode)) {
        logf(LOG_WARN, "deltafs bypassed");
      }
    }

    /* force background activities to stop */
    if (pctx.bgpause) {
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "pausing background activities ... (rank 0)");
      }
      if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
        shuffle_pause(&pctx.sctx);
      }
      if (pctx.plfstp != NULL) {
        deltafs_tp_pause(pctx.plfstp);
      }
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "pausing done (rank 0)");
      }
    }
  }

  struct reneg_opts ro;
  ro.fanout_s1 = 4;
  ro.fanout_s2 = 4;
  ro.fanout_s3 = 4;

  pthread_mutex_init(&(pctx.data_mutex), NULL);
  reneg_init(&(pctx.rtp_ctx), &(pctx.sctx), pctx.data, &(pctx.data_len), 20,
             &(pctx.data_mutex), ro);

  srand(pctx.my_rank);

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
  FILE* f0;
  int fd1;
  int fd2;
  mon_ctx_t local;
  mon_ctx_t glob;
  dir_stat_t tmp_stat;
  char buf[sizeof(mon_ctx_t)];
  char path[PATH_MAX];
  char suffix[100];
  /* total num of pthreads */
  int sum_pthreads;
  /* total num of barriers */
  int sum_barriers;
  unsigned long long num_writes_min; /* per rank */
  unsigned long long num_writes_max; /* per rank */
  unsigned long long min_writes;
  unsigned long long max_writes;
  uint64_t flush_start;
  uint64_t flush_end;
  uint64_t finish_start;
  uint64_t finish_end;
  double finish_dura; /* per-rank, seconds */
  double max_finish_dura;
  double io_time;
  /* num io performed */
  unsigned long long num_files_writ; /* per rank */
  unsigned long long sum_files_writ;
  unsigned long long num_bytes_writ; /* per rank */
  unsigned long long sum_bytes_writ;
  unsigned long long num_files_read; /* per rank */
  unsigned long long sum_files_read;
  unsigned long long num_bytes_read; /* per rank */
  unsigned long long sum_bytes_read;
  /* num names sampled: 0 -> total, 1 -> total valid */
  unsigned long long num_samples[2]; /* per rank */
  unsigned long long sum_samples[2];
  /* num names dumped */
  unsigned long long num_names; /* per rank */
  unsigned long long sum_names;
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

  finish_dura = 0;
  num_files_writ = num_bytes_writ = 0;
  num_files_read = num_bytes_read = 0;
  io_time = 0;

  pthread_mutex_destroy(&(pctx.data_mutex));
  reneg_destroy(&(pctx.rtp_ctx));

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "LIB finalizing ... (%d epochs)", num_eps);
    if (pctx.print_meminfo) {
      print_meminfo();
    }
  }

  /* resuming background activities */
  if (pctx.bgpause) {
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "resuming background activities ... (rank 0)");
    }
    if (pctx.plfstp != NULL) {
      deltafs_tp_rerun(pctx.plfstp);
    }
    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      shuffle_resume(&pctx.sctx);
    }
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "resuming done (rank 0)");
    }
  }

  if (pctx.my_rank == 0) {
    if (!pctx.nodist) {
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
      snprintf(path, sizeof(path), "%s/TIMESTAMP-%s", pctx.log_home, suffix);
      if (mknod(path, 0644, S_IFREG) == -1) {
        loge("mknod", path);
      }

      if (pctx.len_plfsdir != 0) {
        snprintf(path, sizeof(path), "%s/MANIFEST", pctx.log_home);
        f0 = fopen(path, "w");
        if (f0 != NULL) {
          fprintf(f0, "num_epochs=%d\n", num_eps);
          fprintf(f0, "key_size=%s\n", dirc.key_size);
          fprintf(f0, "value_size=%d\n", pctx.sideio ? 12 : pctx.particle_size);
          fprintf(f0, "filter_bits_per_key=%s\n", dirc.bits_per_key);
          fprintf(f0, "memtable_size=%s\n", dirc.memtable_size);
          fprintf(f0, "lg_parts=%s\n", dirc.lg_parts);
          fprintf(f0, "skip_checksums=%d\n", dirc.skip_checksums);
          fprintf(f0, "bypass_shuffle=%d\n", IS_BYPASS_SHUFFLE(pctx.mode));
          fprintf(f0, "force_leveldb_format=%d\n", dirc.force_leveldb_format);
          fprintf(f0, "unordered_storage=%d\n", dirc.unordered_storage);
          fprintf(f0, "particle_id_size=%d\n", pctx.particle_id_size);
          fprintf(f0, "particle_size=%d\n", pctx.particle_size);
          fprintf(f0, "io_engine=%d\n", dirc.io_engine);
          fprintf(f0, "comm_sz=%d\n", pctx.recv_sz);
          if (pctx.sideft)
            fprintf(f0, "fmt=bloomy\n");
          else if (pctx.sideio)
            fprintf(f0, "fmt=wisc\n");

          fflush(f0);
          fclose(f0);
        } else {
          loge("fopen", path);
        }
      }
    }
  }

  if (pctx.len_deltafs_mntp != 0 && pctx.len_plfsdir != 0) {
    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "shuffle shutting down ...");
      }
      /* ensures all peer messages are received */
      PRELOAD_Barrier(MPI_COMM_WORLD);
      /* shuffle flush */
      if (pctx.my_rank == 0) {
        flush_start = now_micros();
        logf(LOG_INFO, "flushing shuffle ... (rank 0)");
      }
      shuffle_epoch_start(&pctx.sctx);
      if (pctx.my_rank == 0) {
        flush_end = now_micros();
        logf(LOG_INFO, "flushing done %s",
             pretty_dura(flush_end - flush_start).c_str());
      }
      /*
       * ensures everyone has the flushing done before finalizing so we can get
       * up-to-date and consistent shuffle stats
       */
      PRELOAD_Barrier(MPI_COMM_WORLD);
      shuffle_finalize(&pctx.sctx);
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "shuffle off");
      }
    }

    /* all writes are concluded, do the last flush, finish the directory,
     * retrieve final mon stats, and free the directory. note that the mon stats
     * must be retrieved before the directory is destroyed. */
    if (pctx.plfshdl != NULL) {
      finish_start = now_micros();
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "finalizing plfsdir ... (rank 0)");
      }
      if (pctx.sideft) deltafs_plfsdir_filter_finish(pctx.plfshdl);
      if (pctx.sideio) deltafs_plfsdir_io_finish(pctx.plfshdl);
      deltafs_plfsdir_finish(pctx.plfshdl);
      finish_end = now_micros();
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "finalizing done %s",
             pretty_dura(finish_end - finish_start).c_str());
      }
      finish_dura = double(finish_end - finish_start) / 1000.0 / 1000.0;
      if (num_eps != 0) {
        dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
      }

      num_files_writ = deltafs_plfsdir_get_integer_property(
          pctx.plfshdl, "io.total_write_open");
      num_bytes_writ = deltafs_plfsdir_get_integer_property(
          pctx.plfshdl, "io.total_bytes_written");
      num_files_read = deltafs_plfsdir_get_integer_property(
          pctx.plfshdl, "io.total_read_open");
      num_bytes_read = deltafs_plfsdir_get_integer_property(
          pctx.plfshdl, "io.total_bytes_read");

      deltafs_plfsdir_free_handle(pctx.plfshdl);
      if (pctx.plfsenv != NULL) {
        deltafs_env_close(pctx.plfsenv);
        pctx.plfsenv = NULL;
      }
      if (pctx.plfstp != NULL) {
        deltafs_tp_close(pctx.plfstp);
        pctx.plfstp = NULL;
      }

      pctx.plfshdl = NULL;

      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "plfsdir (via deltafs-LT) closed (rank 0)");
      }
    } else if (pctx.plfsfd != -1) {
      if (num_eps != 0) {
        dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
      }
      deltafs_close(pctx.plfsfd);
      pctx.plfsfd = -1;

      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "plfsdir closed (rank 0)");
      }
    } else {
      if (num_eps != 0) {
        dump_mon(&pctx.mctx, &tmp_stat, &pctx.last_dir_stat);
      }
    }

#ifdef PRELOAD_HAS_PAPI
    /* close papi */
    if (pctx.papi_set != PAPI_NULL) {
      PAPI_destroy_eventset(&pctx.papi_set);
      PAPI_shutdown();
    }
#endif

    /* conclude sampling */
    if (pctx.sampling && pctx.recv_comm != MPI_COMM_NULL) {
      num_samples[0] = num_samples[1] = 0;
      for (std::map<std::string, int>::const_iterator it = pctx.smap->begin();
           it != pctx.smap->end(); ++it) {
        num_samples[0]++; /* number samples */
        if (it->second == num_eps) {
          num_samples[1]++; /* number valid samples */
        }
      }
      MPI_Reduce(num_samples, sum_samples, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
                 0, pctx.recv_comm);
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "total particles sampled: %s (%s valid)",
             pretty_num(sum_samples[0]).c_str(),
             pretty_num(sum_samples[1]).c_str());
      }
      if (!pctx.nodist) {
        num_names = 0;
        snprintf(path, sizeof(path), "%s/NAMES-%07d.txt", pctx.log_home,
                 pctx.recv_rank);
        if (pctx.my_rank == 0) {
          logf(LOG_INFO, "dumping valid particle names to ...");
          fputs(path, stderr);
          fputc('\n', stderr);
        }
        f0 = fopen(path, "w");
        if (f0 != NULL) {
          if (pctx.my_rank == 0 && pctx.verbose)
            fputs("dumped names = (\n    ...\n", stderr);
          for (std::map<std::string, int>::const_iterator it =
                   pctx.smap->begin();
               it != pctx.smap->end(); ++it) {
            if (it->second == num_eps) {
              fprintf(f0, "%s\n", it->first.c_str());

              num_names++;
              if (pctx.my_rank == 0 && pctx.verbose) {
                if (num_names <= 7) {
                  fputs(" !! ", stderr);
                  fputs(it->first.c_str(), stderr);
                  fputc('\n', stderr);
                }
              }
            }
          }
          if (pctx.my_rank == 0 && pctx.verbose) {
            fputs("    ...\n", stderr);
            fputs(")\n", stderr);
          }

          fflush(f0);
          fclose(f0);
        } else {
          loge("fopen", path);
        }
        MPI_Reduce(&num_names, &sum_names, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
                   0, pctx.recv_comm);
        if (pctx.my_rank == 0) {
          logf(LOG_INFO, "dumping ok (%s names)",
               pretty_num(sum_names).c_str());
        }
      }
    }

    /* close, merge, and dist mon files */
    if (pctx.monfd != -1) {
      if (!pctx.nodist) {
        ok = 1; /* ready to go */

        if (pctx.my_rank == 0) {
          fd1 = fd2 = -1;
          logf(LOG_INFO, "merging and saving epoch mon stats to ...");
          ts = now_micros();
          if (mon_dump_bin) {
            snprintf(path, sizeof(path), "%s/DUMP-mon.bin", pctx.log_home);
            fputs(path, stderr);
            fputc('\n', stderr);
            fd1 = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (fd1 == -1) {
              loge("open", path);
              ok = 0;
            }
          }
          if (mon_dump_txt) {
            snprintf(path, sizeof(path), "%s/DUMP-mon.txt", pctx.log_home);
            fputs(path, stderr);
            fputc('\n', stderr);
            fd2 = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (fd2 == -1) {
              loge("open", path);
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

        while (epoch != num_eps) {
          if (ok) {
            n = read(pctx.monfd, buf, sizeof(buf));
            if (n == sizeof(buf)) {
              memcpy(&local, buf, sizeof(mon_ctx_t));
            } else {
              loge("read", "pctx.monfd");
              ok = 0;
            }
          }

          MPI_Allreduce(&ok, &go, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

          if (!go) {
            if (pctx.my_rank == 0) {
              logf(LOG_WARN, "unable to merge epoch stats");
            }
          } else {
            /* per-rank total writes = local writes + foreign writes */
            num_writes_min = num_writes_max = local.nlw + local.nfw;
            MPI_Reduce(&num_writes_min, &min_writes, 1, MPI_UNSIGNED_LONG_LONG,
                       MPI_MIN, 0, MPI_COMM_WORLD);
            MPI_Reduce(&num_writes_max, &max_writes, 1, MPI_UNSIGNED_LONG_LONG,
                       MPI_MAX, 0, MPI_COMM_WORLD);
            mon_reinit(&glob);
            mon_reduce(&local, &glob);
            glob.epoch_seq = epoch + 1;
            glob.global = 1;
          }

          if (go) {
            if (pctx.my_rank == 0) {
              if (glob.nlw + glob.nfw != glob.nw)
                logf(LOG_WARN,
                     "total local and remote writes != total num particles !?");
              if (glob.nms != glob.nmd)
                logf(LOG_WARN, "num msg sent != num msg delivered !?");
              if (glob.nms != glob.nmr)
                logf(LOG_WARN, "num msg sent != num msg recv'ed !?");
              io_time += glob.max_dura / 1000.0 / 1000.0;
              if (mon_dump_txt) mon_dumpstate(fd2, &glob);
              if (mon_dump_bin) {
                memcpy(buf, &glob, sizeof(mon_ctx_t));
                n = write(fd1, buf, sizeof(buf));
                if (n != sizeof(buf)) {
                  /* ignore */
                }
              }

              if (sizeof(buf) != 0) {
                ucpu = 100 * double(glob.cpu_stat.usr_micros) /
                       glob.cpu_stat.micros;
                scpu = 100 * double(glob.cpu_stat.sys_micros) /
                       glob.cpu_stat.micros;
                logf(LOG_INFO,
                     " @ epoch #%-3d  %s - %s  (%d%% - %d%% cpu usage)",
                     epoch + 1, pretty_dura(glob.min_dura).c_str(),
                     pretty_dura(glob.max_dura).c_str(), glob.cpu_stat.min_cpu,
                     glob.cpu_stat.max_cpu);
                logf(LOG_INFO,
                     "       > avg cpu: %.2f%% user + %.2f%% system ="
                     " %.2f%% total",
                     ucpu, scpu, ucpu + scpu);
                logf(LOG_INFO, "           > %s v/cs, %s i/cs",
                     pretty_num(glob.cpu_stat.vcs).c_str(),
                     pretty_num(glob.cpu_stat.ics).c_str());
#ifdef PRELOAD_HAS_PAPI
                for (size_t ix = 0; ix < pctx.papi_events->size(); ix++) {
                  if (glob.mem_stat.num[ix] != 0) {
                    logf(LOG_INFO, "         > %s: %s (min: %s, max: %s)",
                         pctx.papi_events->at(ix),
                         pretty_num(glob.mem_stat.num[ix]).c_str(),
                         pretty_num(glob.mem_stat.min[ix]).c_str(),
                         pretty_num(glob.mem_stat.max[ix]).c_str());
                  }
                }
#endif
                logf(LOG_INFO,
                     "   > %s particle writes (%s collisions), %s per rank "
                     "(min: %s, max: %s)",
                     pretty_num(glob.nw).c_str(), pretty_num(glob.ncw).c_str(),
                     pretty_num(double(glob.nw) / pctx.comm_sz).c_str(),
                     pretty_num(glob.min_nw).c_str(),
                     pretty_num(glob.max_nw).c_str());
                logf(LOG_INFO,
                     "         > %s foreign + %s local = %s total writes",
                     pretty_num(glob.nfw).c_str(), pretty_num(glob.nlw).c_str(),
                     pretty_num(glob.nfw + glob.nlw).c_str());
                logf(LOG_INFO,
                     "               > %s per rank (min: %s, max: %s)",
                     pretty_num(double(glob.nfw + glob.nlw) / pctx.comm_sz)
                         .c_str(),
                     pretty_num(min_writes).c_str(),
                     pretty_num(max_writes).c_str());
                if (glob.dir_stat.num_sstables != 0) {
                  logf(LOG_INFO,
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
                  logf(LOG_INFO,
                       "           > %s sst, %s per rank, %.1f per mem "
                       "partition",
                       pretty_num(glob.dir_stat.num_sstables).c_str(),
                       pretty_num(double(glob.dir_stat.num_sstables) /
                                  pctx.comm_sz)
                           .c_str(),
                       pctx.plfsparts ? double(glob.dir_stat.num_sstables) /
                                            pctx.comm_sz / pctx.plfsparts
                                      : 0);
                }
                if (glob.dir_stat.num_keys != 0) {
                  logf(LOG_INFO,
                       "     > %s keys (%s dropped),"
                       " %s per rank (min: %s, max %s)",
                       pretty_num(glob.dir_stat.num_keys).c_str(),
                       pretty_num(glob.dir_stat.num_dropped_keys).c_str(),
                       pretty_num(double(glob.dir_stat.num_keys) / pctx.comm_sz)
                           .c_str(),
                       pretty_num(glob.dir_stat.min_num_keys).c_str(),
                       pretty_num(glob.dir_stat.max_num_keys).c_str());
                  logf(LOG_INFO, "         > %s table data, %s, %s per rank",
                       pretty_size(glob.dir_stat.total_datasz).c_str(),
                       pretty_bw(glob.dir_stat.total_datasz, glob.max_dura)
                           .c_str(),
                       pretty_bw(
                           double(glob.dir_stat.total_datasz) / pctx.comm_sz,
                           glob.max_dura)
                           .c_str());
                  logf(LOG_INFO, "             > %s per op",
                       pretty_dura(double(glob.max_dura) /
                                   glob.dir_stat.num_keys * pctx.comm_sz)
                           .c_str());
                }
                if (glob.nlms + glob.nms != 0) {
                  logf(LOG_INFO,
                       "   > %s + %s rpc sent (%s + %s replied), %s + %s "
                       "per rank (min: %s | %s, max: %s | %s)",
                       pretty_num(glob.nlms).c_str(),
                       pretty_num(glob.nms).c_str(),
                       pretty_num(glob.nlmd).c_str(),
                       pretty_num(glob.nmd).c_str(),
                       pretty_num(double(glob.nlms) / pctx.comm_sz).c_str(),
                       pretty_num(double(glob.nms) / pctx.comm_sz).c_str(),
                       pretty_num(glob.min_nlms).c_str(),
                       pretty_num(glob.min_nms).c_str(),
                       pretty_num(glob.max_nlms).c_str(),
                       pretty_num(glob.max_nms).c_str());
                  logf(LOG_INFO, "       > %s + %s, %s + %s per rank",
                       pretty_tput(glob.nlms, glob.max_dura).c_str(),
                       pretty_tput(glob.nms, glob.max_dura).c_str(),
                       pretty_tput(double(glob.nlms) / pctx.comm_sz,
                                   glob.max_dura)
                           .c_str(),
                       pretty_tput(double(glob.nms) / pctx.comm_sz,
                                   glob.max_dura)
                           .c_str());
                }
                if (glob.nlmr + glob.nmr != 0) {
                  logf(LOG_INFO,
                       "   > %s + %s rpc recv, %s + %s per rank (min: %s | %s, "
                       "max: %s | %s)",
                       pretty_num(glob.nlmr).c_str(),
                       pretty_num(glob.nmr).c_str(),
                       pretty_num(double(glob.nlmr) / pctx.comm_sz).c_str(),
                       pretty_num(double(glob.nmr) / pctx.comm_sz).c_str(),
                       pretty_num(glob.min_nlmr).c_str(),
                       pretty_num(glob.min_nmr).c_str(),
                       pretty_num(glob.max_nlmr).c_str(),
                       pretty_num(glob.max_nmr).c_str());
                  logf(LOG_INFO, "       > %s + %s, %s + %s per rank",
                       pretty_tput(glob.nlmr, glob.max_dura).c_str(),
                       pretty_tput(glob.nmr, glob.max_dura).c_str(),
                       pretty_tput(double(glob.nlmr) / pctx.comm_sz,
                                   glob.max_dura)
                           .c_str(),
                       pretty_tput(double(glob.nmr) / pctx.comm_sz,
                                   glob.max_dura)
                           .c_str());
                  logf(LOG_INFO, "           > %s | %s per rpc",
                       pretty_dura(double(glob.max_dura) / glob.nlmr *
                                   pctx.comm_sz)
                           .c_str(),
                       pretty_dura(double(glob.max_dura) / glob.nmr *
                                   pctx.comm_sz)
                           .c_str());
                }
              }
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
          logf(LOG_INFO, "merging ok (%d epochs) %s", epoch,
               pretty_dura(diff).c_str());
        }
      }

      close(pctx.monfd);
      pctx.monfd = -1;
    }
  }

  /* extra stats */
  MPI_Reduce(&num_bytes_writ, &sum_bytes_writ, 1, MPI_UNSIGNED_LONG_LONG,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&num_files_writ, &sum_files_writ, 1, MPI_UNSIGNED_LONG_LONG,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&num_bytes_read, &sum_bytes_read, 1, MPI_UNSIGNED_LONG_LONG,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&num_files_read, &sum_files_read, 1, MPI_UNSIGNED_LONG_LONG,
             MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&finish_dura, &max_finish_dura, 1, MPI_DOUBLE, MPI_MAX, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&num_pthreads, &sum_pthreads, 1, MPI_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "final stats...");
    logf(LOG_INFO, "num renegotiations: %d\n", pctx.rctx.pvt_round_num.load());
    logf(LOG_INFO, "== dir data compaction");
    logf(LOG_INFO,
         "   > %llu bytes written (%llu files), %llu bytes read (%llu files)",
         sum_bytes_writ, sum_files_writ, sum_bytes_read, sum_files_read);
    logf(LOG_INFO, "       > final compaction draining: %.6f secs",
         max_finish_dura);
    io_time += max_finish_dura;
    logf(LOG_INFO, "           > total io time: %.6f secs", io_time);
    logf(LOG_INFO, "== ALL epochs");
    logf(LOG_INFO, "       > %.1f per rank",
         double(sum_pthreads) / pctx.comm_sz);
  }

  /* close testing log file */
  if (pctx.trace != NULL) {
    fflush(pctx.trace);
    fclose(pctx.trace);
  }

  /* release the receiver communicator */
  if (pctx.recv_comm != MPI_COMM_NULL && pctx.recv_comm != MPI_COMM_WORLD) {
    MPI_Comm_free(&pctx.recv_comm);
  }

  /* !!! OK !!! */
  rv = nxt.MPI_Finalize();
  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "all done!");
    if (pctx.print_meminfo) {
      print_meminfo();
    }

    logf(LOG_INFO, "BYE");
  }

  return rv;
}

/*
 * chdir
 */
int chdir(const char* dir) {
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  rv = nxt.chdir(dir);
  if (rv) ABORT("chdir");

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
  if (rv) ABORT("pthread_once");

  if (!claim_path(dir, &exact)) {
    return nxt.mkdir(dir, mode);
  } else if (under_plfsdir(dir)) {
    return 0; /* plfsdirs are pre-created at MPI_Init */
  }

  /* relative paths we pass through; absolute we strip off prefix */

  if (*dir != '/') {
    stripped = dir;
  } else {
    stripped = (exact) ? "/" : (dir + pctx.len_deltafs_mntp);
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

  if (rv) ABORT("xxmkdir");

  return rv;
}

/*
 * begin an epoch.
 */
int opendir_impl(const char* dir) {
  dir_stat_t tmp_dir_stat;
  uint64_t flush_start;
  uint64_t flush_end;
  uint64_t start;
  int rv;

  /* initialize tmp mon stats */
  if (!pctx.nomon) {
    memset(&tmp_dir_stat, 0, sizeof(dir_stat_t));
    start = now_micros();
  }

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "epoch %d begins (rank 0)", num_eps + 1);
    if (pctx.print_meminfo) {
      print_meminfo();
    }
  }

  /* resuming background activities */
  if (pctx.bgpause) {
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "resuming background activities ... (rank 0)");
    }
    if (pctx.plfstp != NULL) {
      deltafs_tp_rerun(pctx.plfstp);
    }
    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      shuffle_resume(&pctx.sctx);
    }
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "resuming done (rank 0)");
    }
  }

  if (pctx.paranoid_barrier) {
    if (num_eps != 0) {
      /*
       * this ensures we have received all peer writes and no more
       * writes will happen for the previous epoch.
       */
      PRELOAD_Barrier(MPI_COMM_WORLD);
    }
  }

  /*
   * if shuffle ON, thanks to the barrier above i must have received all data
   * belong to me from my peers. the next step is to flush shuffle and wait
   * until it finishes delivering all data it receives. if shuffle has been
   * pre-flushed previously, this is likely a no-op.
   */
  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    if (num_eps != 0) {
      if (pctx.my_rank == 0) {
        flush_start = now_micros();
        logf(LOG_INFO, "flushing shuffle receivers ... (rank 0)");
      }
      shuffle_epoch_start(&pctx.sctx); /* shuffle receiver flush */
      if (pctx.my_rank == 0) {
        flush_end = now_micros();
        logf(LOG_INFO, "receiver flushing done %s",
             pretty_dura(flush_end - flush_start).c_str());
      }
    }
  }

  /*
   * i'm a receiver. i have received all data belonging to me and i have waited
   * until shuffle finishes processing all data coming to it. now it's time to
   * flush writes.
   */
  if (num_eps != 0 && pctx.recv_comm != MPI_COMM_NULL) {
    /*
     * the reason write flush is deferred from as early as closedir()
     * time until now is that i am likely to progress faster than
     * other peers at closedir() time and may not have
     * received all data belonging to me.
     */
    if (IS_BYPASS_WRITE(pctx.mode)) {
      /* noop */

    } else if (IS_BYPASS_DELTAFS_NAMESPACE(
                   pctx.mode)) { /* direct plfsdir mode */
      if (pctx.plfshdl != NULL) {
        if (pctx.my_rank == 0) {
          flush_start = now_micros();
          logf(LOG_INFO, "flushing plfsdir ... (rank 0)");
        }

        if (pctx.sideft && deltafs_plfsdir_filter_flush(pctx.plfshdl) != 0)
          ABORT("fail to flush plfsdir side filter");
        if (pctx.sideio && deltafs_plfsdir_io_flush(pctx.plfshdl) != 0)
          ABORT("fail to flush plfsdir side io");
        if (deltafs_plfsdir_epoch_flush(pctx.plfshdl, num_eps - 1) != 0)
          ABORT("fail to flush plfsdir");
        if (pctx.my_rank == 0) {
          flush_end = now_micros();
          logf(LOG_INFO, "flushing done %s",
               pretty_dura(flush_end - flush_start).c_str());
        }
      } else {
        ABORT("plfsdir not opened");
      }

    } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode) &&
               !IS_BYPASS_DELTAFS(pctx.mode)) {
      if (pctx.plfsfd != -1) {
        deltafs_epoch_flush(pctx.plfsfd, NULL); /* XXX */
        if (pctx.my_rank == 0) {
          logf(LOG_INFO, "plfsdir flushed (rank 0)");
        }
      } else {
        ABORT("plfsdir not opened");
      }

    } else {
      /* noop */
    }
  }

  if (num_eps != 0) {
    /*
     * we delay dumping mon stats collected from the previous epoch
     * until the beginning of the next epoch. this allows us
     * to get mostly up-to-date stats on the background
     * compaction work.
     */
    dump_mon(&pctx.mctx, &tmp_dir_stat, &pctx.last_dir_stat);
  }

  /* epoch count is increased before the beginning of each epoch */
  num_eps++; /* must go before the barrier below */

  {
    /* reset range stats */
    std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

    assert(range_state_t::RS_RENEGO != pctx.rctx.range_state);

    pctx.rctx.range_state = range_state_t::RS_INIT;
    std::fill(pctx.rctx.rank_bins.begin(), pctx.rctx.rank_bins.end(), 0);
    std::fill(pctx.rctx.rank_bin_count.begin(), pctx.rctx.rank_bin_count.end(),
              0);

    // pctx.rctx.neg_round_num = 0;
    pctx.rctx.range_min = 0;
    pctx.rctx.range_max = 0;
    pctx.rctx.ts_writes_received = 0;
    pctx.rctx.ts_writes_shuffled = 0;
    pctx.rctx.oob_count_left = 0;
    pctx.rctx.oob_count_right = 0;

    /* XXX: we don't have an explicit flush mechanism before
     * so this might fail but currently we block fwrites during negotiation
     * so it should not fail, but we might lose OOB buffered particles until
     * we implement OOB flushing at the end of an epoch
     */
    assert(pctx.rctx.ranks_acked_count == 0);
  }

  if (pctx.paranoid_post_barrier) {
    /*
     * this ensures all writes made for the next epoch
     * will go to a new write buffer.
     */
    PRELOAD_Barrier(MPI_COMM_WORLD);
  }

  if (!pctx.nomon) {
    mon_reinit(&pctx.mctx); /* clear mon stats */
    /* reset epoch id */
    pctx.mctx.epoch_seq = num_eps;

    pctx.epoch_start = start; /* record epoch start */

    /* take a snapshot of dir stats */
    pctx.last_dir_stat = tmp_dir_stat;
    /* take a snapshot of sys usage */
    pctx.last_sys_usage_snaptime = now_micros();
    rv = getrusage(RUSAGE_SELF, &pctx.last_sys_usage);
    if (rv) ABORT("getrusage");
  }

#ifdef PRELOAD_HAS_PAPI
  if (pctx.papi_set != PAPI_NULL) {
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "starting papi ... (rank 0)");
    }
    if ((rv = PAPI_reset(pctx.papi_set)) != PAPI_OK) {
      ABORT(PAPI_strerror(rv));
    }
    if ((rv = PAPI_start(pctx.papi_set)) != PAPI_OK) {
      ABORT(PAPI_strerror(rv));
    }
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "papi on");
    }
  }
#endif

  /* mon status resets and epoch increments must go before the barrier */
  if (pctx.paranoid_post_barrier) {
    /*
     * this ensures that all data and mon status updates made
     * for the next epoch will go to that epoch.
     */
    PRELOAD_Barrier(MPI_COMM_WORLD);
  }

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "dumping particles ... (rank 0)");
  }

  /*
   * restart paranoid checking status. this is sender-local status
   * so no need for any barrier synchronization.
   */
  pctx.fnames->clear();

  return 0;
}

/*
 * opendir
 */
DIR* opendir(const char* dir) {
  int ignored_exact;
  DIR* rv;

  int ret = pthread_once(&init_once, preload_init);
  if (ret) ABORT("pthread_once");

  if (!claim_path(dir, &ignored_exact)) {
    return nxt.opendir(dir);
  } else if (!under_plfsdir(dir)) {
    return NULL; /* not supported */
  }

  /* return a fake DIR* since we don't actually open */
  rv = reinterpret_cast<DIR*>(&fake_dirptr);

  opendir_impl(dir);

  return rv;
}

/*
 * close an epoch.
 */
int closedir_impl(DIR* dirp) {
  uint64_t tmp_usage_snaptime;
  struct rusage tmp_usage;
  uint64_t flush_start;
  uint64_t flush_end;
  double cpu;
  int rv;

  if (pctx.my_rank == 0) logf(LOG_INFO, "dumping done!!!");

  if (pctx.paranoid_checks) {
    assert(pctx.isdeltafs != NULL);
    if (!pctx.isdeltafs->empty()) {
      ABORT("some plfsdir files still open!");
    }
    pctx.fnames->clear();
  }

  /*
   * if shuffle is ON, flush all sender buffers and wait until all data
   * reaches its destinations (or its destination representatives).
   */
  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    if (pctx.my_rank == 0) {
      flush_start = now_micros();
      logf(LOG_INFO, "flushing shuffle senders ... (rank 0)");
    }
    shuffle_epoch_end(&pctx.sctx); /* shuffle sender flush */
    if (pctx.my_rank == 0) {
      flush_end = now_micros();
      logf(LOG_INFO, "sender flushing done %s",
           pretty_dura(flush_end - flush_start).c_str());
    }
  }

  /*
   * i'm a receiver (or a representative of a set of receivers). the barrier
   * below ensures that i have received all data belong to me (and the peers i
   * represent). the barrier below is required if bg pause is ON. the barrier
   * below is rather useless if shuffle is OFF.
   */
  if (pctx.paranoid_pre_barrier ||
      (!IS_BYPASS_SHUFFLE(pctx.mode) && pctx.bgpause)) {
    PRELOAD_Barrier(MPI_COMM_WORLD);
    /*
     * i'm a receiver (or a representative of a set of receivers). thanks to the
     * barrier above i have received all data belong to me (and the peers i
     * represent). the next step is to forward data to its real destinations,
     * and to wait until shuffle finishes delivering all data it receivers.
     */
    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      if (pctx.my_rank == 0) {
        flush_start = now_micros();
        logf(LOG_INFO, "pre-flushing shuffle receivers ... (rank 0)");
      }
      shuffle_epoch_pre_start(&pctx.sctx); /* shuffle receiver pre-flush */
      if (pctx.my_rank == 0) {
        flush_end = now_micros();
        logf(LOG_INFO, "receiver pre-flushing done %s",
             pretty_dura(flush_end - flush_start).c_str());
      }
    }
  }

  /*
   * pre-flush writes so less work is needed at the next opendir(). if the
   * barrier above is ON, i must have received all data belong to me and have
   * waited until shuffle finishes processing all data coming to it. in such
   * cases, all data will be flushed for background compaction at this step.
   */
  if (pctx.pre_flushing && pctx.recv_comm != MPI_COMM_NULL) {
    if (IS_BYPASS_WRITE(pctx.mode)) {
      /* noop */

    } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      if (pctx.plfshdl != NULL) {
        if (pctx.my_rank == 0) {
          flush_start = now_micros();
          logf(LOG_INFO, "pre-flushing plfsdir ... (rank 0)");
        }

        if (pctx.sideio && deltafs_plfsdir_io_flush(pctx.plfshdl) != 0)
          ABORT("fail to flush plfsdir side io");
        if (deltafs_plfsdir_flush(pctx.plfshdl, num_eps - 1) != 0)
          ABORT("fail to flush plfsdir");

        if (pctx.pre_flushing_wait) {
          if (pctx.my_rank == 0 && pctx.verbose)
            fputs("   WaitForCompaction\n", stderr);
          if (pctx.sideio && deltafs_plfsdir_io_wait(pctx.plfshdl) != 0)
            ABORT("fail to wait for plfsdir side io");
          if (deltafs_plfsdir_wait(pctx.plfshdl) != 0)
            ABORT("fail to wait for plfsdir");
        }

        if (pctx.pre_flushing_sync) {
          if (pctx.my_rank == 0 && pctx.verbose) fputs("   FsyncIo\n", stderr);
          if (pctx.sideio && deltafs_plfsdir_io_sync(pctx.plfshdl) != 0)
            ABORT("fail to sync plfsdir side io");
          if (deltafs_plfsdir_sync(pctx.plfshdl) != 0)
            ABORT("fail to sync plfsdir");
        }

        if (pctx.my_rank == 0) {
          flush_end = now_micros();
          logf(LOG_INFO, "pre-flushing done %s",
               pretty_dura(flush_end - flush_start).c_str());
        }
      } else {
        ABORT("plfsdir not opened");
      }

    } else {
      /* XXX */
    }
  }

#ifdef PRELOAD_HAS_PAPI
  if (pctx.papi_set != PAPI_NULL) {
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "stopping papi (rank 0)");
    }
    if ((rv = PAPI_stop(pctx.papi_set, pctx.mctx.mem_stat.num)) != PAPI_OK) {
      ABORT(PAPI_strerror(rv));
    }
    memcpy(pctx.mctx.mem_stat.min, pctx.mctx.mem_stat.num,
           sizeof(pctx.mctx.mem_stat.num));
    memcpy(pctx.mctx.mem_stat.max, pctx.mctx.mem_stat.num,
           sizeof(pctx.mctx.mem_stat.num));
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "papi off");
    }
  }
#endif

  if (!pctx.nomon) {
    tmp_usage_snaptime = now_micros();
    rv = getrusage(RUSAGE_SELF, &tmp_usage);
    if (rv) ABORT("getrusage");
    pctx.mctx.cpu_stat.micros =
        pctx.my_cpus * (tmp_usage_snaptime - pctx.last_sys_usage_snaptime);
    pctx.mctx.cpu_stat.sys_micros =
        timeval_to_micros(&tmp_usage.ru_stime) -
        timeval_to_micros(&pctx.last_sys_usage.ru_stime);
    pctx.mctx.cpu_stat.usr_micros =
        timeval_to_micros(&tmp_usage.ru_utime) -
        timeval_to_micros(&pctx.last_sys_usage.ru_utime);

    pctx.mctx.cpu_stat.vcs = static_cast<unsigned long long>(
        tmp_usage.ru_nvcsw - pctx.last_sys_usage.ru_nvcsw);
    pctx.mctx.cpu_stat.ics = static_cast<unsigned long long>(
        tmp_usage.ru_nivcsw - pctx.last_sys_usage.ru_nivcsw);

    cpu =
        100 *
        double(pctx.mctx.cpu_stat.sys_micros + pctx.mctx.cpu_stat.usr_micros) /
        pctx.mctx.cpu_stat.micros;

    pctx.mctx.cpu_stat.min_cpu = int(floor(cpu));
    pctx.mctx.cpu_stat.max_cpu = int(ceil(cpu));
  }

  /* force background activities to stop */
  if (pctx.bgpause) {
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "pausing background activities ... (rank 0)");
    }
    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
      shuffle_pause(&pctx.sctx);
    }
    if (pctx.plfstp != NULL) {
      deltafs_tp_pause(pctx.plfstp);
    }
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "pausing done (rank 0)");
    }
  }

  /* record epoch duration */
  if (!pctx.nomon) {
    pctx.mctx.max_dura = now_micros() - pctx.epoch_start;
    pctx.mctx.min_dura = pctx.mctx.max_dura;
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "epoch %s (rank 0)",
           pretty_dura(pctx.mctx.max_dura).c_str());
    }
  }

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "epoch ends (rank 0)");
    if (pctx.print_meminfo) {
      print_meminfo();
    }
  }

  return 0;
}

/*
 * closedir
 */
int closedir(DIR* dirp) {
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (dirp != reinterpret_cast<DIR*>(&fake_dirptr)) {
    return nxt.closedir(dirp);
  }

  closedir_impl(dirp);

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
  if (ret) ABORT("pthread_once");

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
    stripped = (exact) ? "/" : (fpath + pctx.len_deltafs_mntp);
  }

  pthread_mtx_lock(&preload_mtx);
  if (pctx.paranoid_checks) {
    fname = stripped + pctx.len_plfsdir + 1;
    if (pctx.fnames->count(fname) == 0) {
      pctx.fnames->insert(fname);
    } else {
      pctx.mctx.ncw++;
    }
  }
  pctx.mctx.min_nw++;
  pctx.mctx.max_nw++;
  pctx.mctx.nw++;
  if (stock_file == NULL) {
    rv = reinterpret_cast<FILE*>(new fake_file(stripped));
    logf(LOG_WARN, "VPIS IS OPENING MULTIPLE PLFSDIR FILES SIMULTANEOUSLY!");
    assert(pctx.isdeltafs != NULL);
    pctx.isdeltafs->insert(rv);
  } else {
    assert(stock_file == &the_stock_file);
    stock_file->reset(stripped);
    rv = reinterpret_cast<FILE*>(stock_file);
    stock_file = NULL;
  }

  pthread_mtx_unlock(&preload_mtx);

  return rv;
}

/*
 * fwrite
 */
size_t fwrite(const void* ptr, size_t size, size_t nitems, FILE* stream) {
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

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
 * fputc
 */
int fputc(int character, FILE* stream) {
  int rv;
  char a[1];

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fputc(character, stream);
  }

  a[0] = static_cast<char>(character);
  fake_file* ff = reinterpret_cast<fake_file*>(stream);
  ff->add_data(a, 1);

  return a[0];
}

/*
 * fclose.   returns EOF on error.
 */
int fclose(FILE* stream) {
  static uint64_t off = 0;
  ssize_t n;
  const char* fname;
  size_t fname_len;
  char* data;
  size_t data_len;
  int rv;

  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fclose(stream);
  }

  fake_file* const ff = reinterpret_cast<fake_file*>(stream);
  fname = ff->file_name();
  assert(fname != NULL);
  n = 0;

  /* check file path and remove parent directories */
  assert(pctx.len_plfsdir != 0 && pctx.plfsdir != NULL);
  assert(strncmp(fname, pctx.plfsdir, pctx.len_plfsdir) == 0);
  assert(fname[pctx.len_plfsdir] == '/');
  fname += pctx.len_plfsdir + 1;

  /* obtain filename length */
  fname_len = strlen(fname);

  if (pctx.paranoid_checks) {
    if (pctx.particle_id_size != fname_len) {
      ABORT("bad particle id size");
    }
    if (pctx.particle_size != ff->size()) {
      ABORT("bad particle size");
    }
  }

  if (pctx.sideft) { /* switch to the bloomy fmt */
    if (IS_BYPASS_WRITE(pctx.mode)) {
      /* noop */

    } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      assert(pctx.plfshdl != NULL);
      n = deltafs_plfsdir_append(pctx.plfshdl, fname, num_eps - 1, ff->data(),
                                 ff->size());
      if (n != ff->size()) {
        ABORT("plfsdir write failed");
      }
    } else {
      ABORT("not implemented");
    }

    data_len = 0;
    data = NULL;

  } else if (pctx.sideio) { /* switch to the wisc-key fmt */
    if (IS_BYPASS_WRITE(pctx.mode)) {
      /* noop */

    } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
      assert(pctx.plfshdl != NULL);
      n = deltafs_plfsdir_io_append(pctx.plfshdl, ff->data(), ff->size());
      if (n != ff->size()) {
        ABORT("plfsdir sideio write failed");
      }

    } else {
      ABORT("not implemented");
    }

    data_len = sizeof(off);
    data = reinterpret_cast<char*>(&off);
    off += n;

  } else { /* use the default fmt */
    data_len = ff->size();
    data = ff->data();
  }

  if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
    rv = shuffle_write_mux(&pctx.sctx, fname, fname_len, data, data_len,
                           num_eps - 1);
    if (rv) {
      ABORT("plfsdir shuffler write failed");
    }
  } else {
    rv = native_write(fname, fname_len, data, data_len, num_eps - 1);
    if (rv) {
      ABORT("plfsdir write failed");
    }
  }

  pthread_mtx_lock(&preload_mtx);

  if (ff == &the_stock_file) {
    stock_file = &the_stock_file; /* to be reused*/
  } else {
    assert(pctx.isdeltafs != NULL);
    pctx.isdeltafs->erase(stream);
    delete ff;
  }

  pthread_mtx_unlock(&preload_mtx);

  return rv;
}

/*
 * pthread_create: here we do the thread counting.
 */
int pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                   void* (*start_routine)(void*), void* arg) {
  int rv;
  char* start;
  char tagbuf[20];
  std::string tagstr;
  const char* tag;
  void* bt[16];
  char** syms;

  if (pctx.my_rank >= pctx.pthread_tap) {
    rv = nxt.pthread_create(thread, attr, start_routine, arg);
  } else {
    snprintf(tagbuf, sizeof(tagbuf), "rank %d, bg %d, ", pctx.my_rank,
             num_pthreads);
    tagstr = tagbuf;
    /* obtain the caller stack */
    int nptr = backtrace(bt, 16);
    syms = backtrace_symbols(bt, nptr);
    if (syms && 1 < nptr) {
      start = strrchr(syms[1], '/');
      tagstr += start ? start + 1 : syms[1];
    } else {
      tagstr += "???";
    }
    if (syms) {
      free(syms);
    }
    tag = strdup(tagstr.c_str());
    rv = pthread_create_tap(thread, attr, start_routine, arg, tag, NULL, NULL,
                            nxt.pthread_create);
  }

  num_pthreads++;
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
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.feof(stream);
  }

  errno = ENOTSUP;
  ABORT("feof!");
  return 0;
}

/*
 * ferror
 */
int ferror(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.ferror(stream);
  }

  errno = ENOTSUP;
  ABORT("ferror!");
  return 0;
}

/*
 * clearerr
 */
void clearerr(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    nxt.clearerr(stream);
    return;
  }

  errno = ENOTSUP;
  ABORT("clearerr!");
}

/*
 * fread
 */
size_t fread(void* ptr, size_t size, size_t nitems, FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fread(ptr, size, nitems, stream);
  }

  errno = ENOTSUP;
  ABORT("fread!");
  return 0;
}

/*
 * fseek
 */
int fseek(FILE* stream, long offset, int whence) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.fseek(stream, offset, whence);
  }

  errno = ENOTSUP;
  ABORT("fseek!");
  return 0;
}

/*
 * ftell
 */
long ftell(FILE* stream) {
  int rv;
  rv = pthread_once(&init_once, preload_init);
  if (rv) ABORT("pthread_once");

  if (!claim_FILE(stream)) {
    return nxt.ftell(stream);
  }

  errno = ENOTSUP;
  ABORT("ftell!");
  return 0;
}

} /* extern "C" */

/*
 * preload_write
 */
int preload_write(const char* fname, unsigned char fname_len, char* data,
                  unsigned char data_len, int epoch, int src) {
  ssize_t n;
  char buf[12];
  int rv;

  if (epoch == -1) {
    epoch = num_eps - 1;
  }

  pthread_mtx_lock(&write_mtx);

  if (pctx.paranoid_checks) {
    if (fname_len != strlen(fname)) {
      ABORT("bad particle filename length");
    }
    if (fname_len != pctx.particle_id_size || data_len != pctx.particle_size) {
      ABORT("bad particle format");
    }
    if (epoch != num_eps - 1) {
      ABORT("bad epoch num");
    }
  }

  if (pctx.sampling) {
    assert(pctx.smap != NULL);
    if (num_eps == 1) {
      /* during the initial epoch, we accept as many names as possible */
      if (getr(0, 1000000 - 1) < pctx.sthres) {
        pctx.smap->insert(std::make_pair(fname, 1));
      }
    } else {
      if (pctx.smap->count(fname) != 0) {
        pctx.smap->at(fname)++;
      }
    }
  }

  rv = 0;

  if (IS_BYPASS_WRITE(pctx.mode)) {
    /* noop */

  } else if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
    if (pctx.sideft) { /* use the bloomy fmt */
      rv = deltafs_plfsdir_filter_put(pctx.plfshdl, fname, fname_len, src);
    } else {
      if (pctx.sideio) { /* use the wisc-key fmt */
        memcpy(buf, &src, 4);
        assert(data_len == 8);
        memcpy(buf + 4, data, 8);
        data_len = 12;
        data = buf;
      }

      n = deltafs_plfsdir_append(pctx.plfshdl, fname, epoch, data, data_len);
      if (n != data_len) {
        rv = EOF;
      }
    }

  } else {
    ABORT("not implemented");
  }

  pthread_mtx_unlock(&write_mtx);

  return rv;
}
