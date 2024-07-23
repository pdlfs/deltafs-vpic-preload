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

#include "common.h"

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <stdarg.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>

#include <string>
#include <vector>

#ifdef PRELOAD_HAS_NUMA
#include <numa.h>
#endif

uint64_t timeval_to_micros(const struct timeval* tv) {
  uint64_t t;
  t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
  t += tv->tv_usec;
  return t;
}

#define PRELOAD_USE_CLOCK_GETTIME

uint64_t now_micros() {
  uint64_t t;

#if defined(__linux) && defined(PRELOAD_USE_CLOCK_GETTIME)
  struct timespec tp;

  clock_gettime(CLOCK_MONOTONIC, &tp);
  t = static_cast<uint64_t>(tp.tv_sec) * 1000000;
  t += tp.tv_nsec / 1000;
#else
  struct timeval tv;

  gettimeofday(&tv, NULL);
  t = timeval_to_micros(&tv);
#endif

  return t;
}

uint64_t now_micros_coarse() {
  uint64_t t;

#if defined(__linux) && defined(PRELOAD_USE_CLOCK_GETTIME)
  struct timespec tp;

  clock_gettime(CLOCK_MONOTONIC_COARSE, &tp);
  t = static_cast<uint64_t>(tp.tv_sec) * 1000000;
  t += tp.tv_nsec / 1000;
#else
  struct timeval tv;

  gettimeofday(&tv, NULL);
  t = timeval_to_micros(&tv);
#endif

  return t;
}

void check_clockres() {
  int n;
#if defined(__linux) && defined(PRELOAD_USE_CLOCK_GETTIME)
  struct timespec res;
  n = clock_getres(CLOCK_MONOTONIC_COARSE, &res);
  if (n == 0) {
    flog(LOG_INFO, "[clock] CLOCK_MONOTONIC_COARSE: %d us",
         int(res.tv_sec * 1000 * 1000 + res.tv_nsec / 1000));
  }
  n = clock_getres(CLOCK_MONOTONIC, &res);
  if (n == 0) {
    flog(LOG_INFO, "[clock] CLOCK_MONOTONIC: %d ns",
         int(res.tv_sec * 1000 * 1000 * 1000 + res.tv_nsec));
  }
#endif
}

#undef PRELOAD_USE_CLOCK_GETTIME

/* Check for SSE 4.2.  SSE 4.2 was first supported in Nehalem processors
   introduced in November, 2008.  This does not check for the existence of the
   cpuid instruction itself, which was introduced on the 486SL in 1992, so this
   will fail on earlier x86 processors.  cpuid works on all Pentium and later
   processors. */
#define CHECK_SSE42(have)                                     \
  do {                                                        \
    uint32_t eax, ecx;                                        \
    eax = 1;                                                  \
    __asm__("cpuid" : "=c"(ecx) : "a"(eax) : "%ebx", "%edx"); \
    (have) = (ecx >> 20) & 1;                                 \
  } while (0)

void check_sse42() {
  int sse42;
  CHECK_SSE42(sse42);
  if (sse42) {
    flog(LOG_INFO, "[sse] SSE4_2 extension is available");
#if defined(__GUNC__) && defined(__SSE4_2__)
    fputs(">>> __SSE4_2__ is defined\n", stderr);
#else
    fputs(">>> __SSE4_2__ is not defined\n", stderr);
#endif
  } else {
    flog(LOG_INFO, "[sse] SSE4_2 is not available");
  }
}

/* read a line from file */
static std::string readline(const char* fname) {
  char tmp[256];
  ssize_t l;
  ssize_t n;
  int fd;

  memset(tmp, 0, sizeof(tmp));
  fd = open(fname, O_RDONLY);
  if (fd != -1) {
    n = read(fd, tmp, sizeof(tmp));
    if (n > 0) {
      tmp[n - 1] = 0; /* remove end-of-line */
    }

    close(fd);
  }

  l = strlen(tmp);
  if (l > 120) {
    tmp[120] = 0;
    strcat(tmp, " ...");
  } else if (l == 0) {
    strcat(tmp, "?");
  }

  errno = 0;

  return tmp;
}

/* remove leading and tailing space */
static std::string trim(const char* str, size_t limit) {
  char tmp[256];
  size_t start;
  size_t off;
  size_t sz;

  start = 0;
  while (start < limit && isspace(str[start])) start++;
  off = limit;
  while (off > start && isspace(str[off - 1])) off--;
  sz = off - start;
  if (sz >= sizeof(tmp)) sz = sizeof(tmp) - 1;
  if (sz != 0) memcpy(tmp, str + start, sz);
  tmp[sz] = 0;

  return tmp;
}

/*
 * try_scan_sysfs(): scan sysfs for important system information.
 */
void try_scan_sysfs() {
#if defined(__linux)
  DIR* d;
  DIR* dd;
  struct dirent* dent;
  struct dirent* ddent;
  const char* dirname;
  char path[PATH_MAX];
  std::string jobcpuset;
  std::string jobmemset;
  std::string idx[4];
  std::string mtu;
  std::string txqlen;
  std::string speed;
  std::string nic;
  int tx;
  int rx;
  int nnics;
  int nnodes;
  int ncpus;
  int n;

  if (access("/sys", R_OK) != 0) /* give up */
    return;

  ncpus = 0;
  dirname = "/sys/devices/system/cpu";
  d = opendir(dirname);
  if (d != NULL) {
    dent = readdir(d);
    for (; dent != NULL; dent = readdir(d)) {
      if (dent->d_type == DT_DIR || dent->d_type == DT_UNKNOWN) {
        if (sscanf(dent->d_name, "cpu%d", &n) == 1) {
          ncpus++;
        }
      }
    }
    closedir(d);
  }

  if (ncpus != 0) {
    for (int i = 0; i < 4; i++) {
      snprintf(path, sizeof(path),
               "/sys/devices/system/cpu/cpu0/cache/index%d/size", i);
      idx[i] = readline(path);
    }
  }

  nnodes = 0;
  dirname = "/sys/devices/system/node";
  d = opendir(dirname);
  if (d != NULL) {
    dent = readdir(d);
    for (; dent != NULL; dent = readdir(d)) {
      if (dent->d_type == DT_DIR || dent->d_type == DT_UNKNOWN) {
        if (sscanf(dent->d_name, "node%d", &n) == 1) {
          nnodes++;
        }
      }
    }
    closedir(d);
  }

  flog(LOG_INFO,
       "[sys] %d NUMA nodes / %d CPU cores (L1: %s + %s, L2: %s, L3: %s)",
       nnodes, ncpus, idx[0].c_str(), idx[1].c_str(), idx[2].c_str(),
       idx[3].c_str());

  nnics = 0;
  dirname = "/sys/class/net";
  d = opendir(dirname);
  if (d != NULL) {
    dent = readdir(d);
    for (; dent != NULL; dent = readdir(d)) {
      if (strcmp(dent->d_name, "lo") != 0 && strcmp(dent->d_name, ".") != 0 &&
          strcmp(dent->d_name, "..") != 0) {
        nic = dent->d_name;
        snprintf(path, sizeof(path), "%s/%s/tx_queue_len", dirname,
                 dent->d_name);
        txqlen = readline(path);
        snprintf(path, sizeof(path), "%s/%s/speed", dirname, dent->d_name);
        speed = readline(path);
        snprintf(path, sizeof(path), "%s/%s/mtu", dirname, dent->d_name);
        mtu = readline(path);
        tx = 0;
        rx = 0;
        snprintf(path, sizeof(path), "%s/%s/queues", dirname, dent->d_name);
        dd = opendir(path);
        if (dd != NULL) {
          ddent = readdir(dd);
          for (; ddent != NULL; ddent = readdir(dd)) {
            if (sscanf(ddent->d_name, "tx-%d", &n) == 1) {
              tx++;
            } else if (sscanf(ddent->d_name, "rx-%d", &n) == 1) {
              rx++;
            }
          }
          closedir(dd);
        }
        nnics++;
        flog(LOG_INFO,
             "[if] speed %5s Mbps, tx_queue_len "
             "%5s, mtu %5s, rx-irq: %3d, tx-irq: %3d (%s)",
             speed.c_str(), txqlen.c_str(), mtu.c_str(), rx, tx, nic.c_str());
      }
    }
    closedir(d);
  }

  dirname = "/sys/fs/cgroup/cpuset/slurm";
  d = opendir(dirname);
  if (d != NULL) {
    dent = readdir(d);
    for (; dent != NULL; dent = readdir(d)) {
      if (strncmp(dent->d_name, "uid_", strlen("uid_")) == 0) {
        snprintf(path, sizeof(path), "%s/%s", dirname, dent->d_name);
        dd = opendir(path);
        if (dd != NULL) {
          ddent = readdir(dd);
          for (; ddent != NULL; ddent = readdir(dd)) {
            if (strncmp(ddent->d_name, "job_", strlen("job_")) == 0) {
              snprintf(path, sizeof(path), "%s/%s/%s/cpus", dirname,
                       dent->d_name, ddent->d_name);
              jobcpuset = readline(path);
              snprintf(path, sizeof(path), "%s/%s/%s/mems", dirname,
                       dent->d_name, ddent->d_name);
              jobmemset = readline(path);
              flog(LOG_INFO,
                   "[slurm] job cgroup cpuset: %s, memset: %s\n>>> %s/%s",
                   jobcpuset.c_str(), jobmemset.c_str(), dent->d_name,
                   ddent->d_name);
              break;
            }
          }
          closedir(dd);
        }
        break;
      }
    }
    closedir(d);
  }
#endif
}

/*
 * try_scan_procfs(): scan procfs for important device information.
 */
void try_scan_procfs() {
#if defined(__linux)
  char line[256];
  int num_cpus;
  std::string cpu_type;
  std::string L1_cache_size;
  const char* sep;
  std::string overcommit_memory;
  std::string overcommit_ratio;
  std::string value;
  std::string key;
  std::string os;
  FILE* cpuinfo;

  if (access("/proc", R_OK) != 0) /* give up */
    return;

  cpuinfo = fopen("/proc/cpuinfo", "r");
  if (cpuinfo != NULL) {
    num_cpus = 0;
    cpu_type = "?";
    while (fgets(line, sizeof(line), cpuinfo) != NULL) {
      sep = strchr(line, ':');
      if (sep == NULL) {
        continue;
      }
      key = trim(line, sep - 1 - line);
      value = trim(sep + 1, strlen(sep + 1));
      if (key == "model name") {
        cpu_type = value;
      } else if (key == "cache size") {
        L1_cache_size = value;
      } else if (key == "processor") {
        num_cpus++;
      }
    }
    fclose(cpuinfo);
    if (num_cpus != 0) {
      flog(LOG_INFO, "[cpu] %d x %s (L2/L3 cache: %s)", num_cpus,
           cpu_type.c_str(), L1_cache_size.c_str());
    }
  }

  overcommit_memory = readline("/proc/sys/vm/overcommit_memory");
  overcommit_ratio = readline("/proc/sys/vm/overcommit_ratio");

  flog(LOG_INFO, "[vm] page size: %d bytes (overcommit memory: %s, ratio: %s)",
       getpagesize(), overcommit_memory.c_str(), overcommit_ratio.c_str());

  os = readline("/proc/version_signature");
  if (strcmp(os.c_str(), "?") == 0) {
    os = readline("/proc/version");
  }

  flog(LOG_INFO, "[os] %s", os.c_str());
#endif
}

void maybe_warn_rlimit(int myrank, int worldsz) {
  struct rlimit rl;
  long long softnofile;
  long long hardnofile;
  long long oknofile;
  long long softmemlock;
  long long hardmemlock;
  int n;

  n = getrlimit(RLIMIT_NOFILE, &rl);
  if (n == 0) {
    oknofile = 2 * static_cast<long long>(worldsz) + 128;
    if (rl.rlim_cur != RLIM_INFINITY)
      softnofile = rl.rlim_cur;
    else
      softnofile = -1;
    if (rl.rlim_max != RLIM_INFINITY)
      hardnofile = rl.rlim_max;
    else
      hardnofile = -1;
    flog(LOG_INFO,
         "[ulimit] max open files per process: %lld soft, %lld hard, %lld "
         "suggested",
         softnofile, hardnofile, oknofile);
  }

  n = getrlimit(RLIMIT_MEMLOCK, &rl);
  if (n == 0) {
    if (rl.rlim_cur != RLIM_INFINITY)
      softmemlock = rl.rlim_cur;
    else
      softmemlock = -1;
    if (rl.rlim_max != RLIM_INFINITY)
      hardmemlock = rl.rlim_max;
    else
      hardmemlock = -1;
    flog(LOG_INFO, "[ulimit] max memlock size: %lld soft, %lld hard",
         softmemlock, hardmemlock);
  }
}

void maybe_warn_numa() {
#if defined(__linux) && PRELOAD_HAS_NUMA
  int os;
  int my;
  int r;

  if (numa_available() == -1) return;
  os = numa_num_configured_cpus();
  my = numa_num_task_cpus();
  std::string cpu(os, 'o');
  {
    struct bitmask* bits = numa_allocate_cpumask();
    r = numa_sched_getaffinity(getpid(), bits);
    if (r != -1) {
      for (int i = 0; i < os; i++) {
        if (numa_bitmask_isbitset(bits, i)) {
          cpu[i] = 'x';
        }
      }
    }
    numa_free_cpumask(bits);
  }
  flog(LOG_INFO, "[numa] cpu: %d/%d cores:", my, os);
  fputs(cpu.c_str(), stderr);
  fputc('\n', stderr);

  os = numa_num_configured_nodes();
  my = numa_num_task_nodes();
  std::string mem(os, 'o');
  struct bitmask* const bits = numa_get_mems_allowed();
  struct bitmask* const mybits = numa_get_membind();
  for (int i = 0; i < os; i++) {
    if (numa_bitmask_isbitset(bits, i) && numa_bitmask_isbitset(mybits, i)) {
      mem[i] = 'x';
    }
  }
  flog(LOG_INFO, "[numa] mem: %d/%d nodes:", my, os);
  fputs(mem.c_str(), stderr);
  fputc('\n', stderr);

#endif
}

void print_meminfo() {
#if defined(__linux)
  char fp[100], line[256];
  std::string info;
  FILE* file;

  if (access("/proc", R_OK) != 0) /* give up */
    return;

  snprintf(fp, sizeof(fp), "/proc/%d/statm", getpid());
  info = readline(fp);
  fprintf(stderr, "[MEMINFO] %s Pages (Per-Process)\n", info.c_str());
  fputs("   Fmt: VM_size rss sha txt lib dat dt\n", stderr);

  snprintf(fp, sizeof(fp), "/proc/%d/maps", getpid());
  file = fopen(fp, "r");
  if (file != NULL) {
    while (fgets(line, sizeof(line), file) != NULL) {
      fputs(" -> ", stderr);
      fputs(line, stderr);
    }
    fclose(file);
  }

  file = fopen("/proc/meminfo", "r");
  if (file != NULL) {
    while (fgets(line, sizeof(line), file) != NULL) {
      if (strncmp(line, "Mem", 3) == 0 || strncmp(line, "Commit", 6) == 0) {
        fputs("      > ", stderr);
        fputs(line, stderr);
      }
    }
    fclose(file);
  }

  fputs("   RUSAGE[maxrss]=", stderr);
  fprintf(stderr, "%ld KiB", my_maxrss());
  fputc('\n', stderr);
#endif
}

long my_maxrss() {
#if defined(__linux)
  struct rusage ru;
  int r;

  r = getrusage(RUSAGE_SELF, &ru);
  if (r == 0) return ru.ru_maxrss;
#endif
  return 0;
}

int my_cpu_cores() {
  int ncpus = 0;
#if defined(__linux)
  cpu_set_t cpuset;
  int n;

  CPU_ZERO(&cpuset);
  n = sched_getaffinity(0, sizeof(cpuset), &cpuset);
  if (n == 0) {
    ncpus = CPU_COUNT(&cpuset);
  }
#endif
  return ncpus;
}

int flog_io(int lvl, const char* fmt, ...) {
  /* flog() macro should have already filtered on LOG_LEVEL */

  const char* prefix;
  va_list ap;
  switch (lvl) {
    case LOG_ERRO:
      prefix = "!!! ERROR !!! ";
      break;
    case LOG_WARN:
      prefix = "-WARNING- ";
      break;
    case LOG_INFO:
      prefix = "-INFO- ";
      break;
    case LOG_DBUG:
      prefix = "-DEBUG- ";
      break;
    default:
      prefix = "";
      break;
  }
  fprintf(stderr, "%s", prefix);

  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);

  fprintf(stderr, "\n");
  return 0;
}

int loge(const char* op, const char* path) {
  flog(LOG_ERRO, "!%s(%s): %s", strerror(errno), op, path);
  return 0;
}

/*
 * msg_abort is called from preload_init(), so it should not use
 * any functions that we preload (e.g. fwrite()).  note that gcc
 * will convert simple printf/fputs into fwrite calls, so we avoid
 * that by using one big fprintf() before calling abort().
 */
void msg_abort(int err, const char* msg, const char* func, const char* file,
               int line) {
  char ebuf[64];
  if (err == 0) {
    ebuf[0] = '\0';
  } else {
    snprintf(ebuf, sizeof(ebuf), ": %s (errno=%d)", strerror(err), err);
  }
  fprintf(stderr, "*** ABORT *** @@ %s:%d @@ %s] %s%s\n", file, line, func,
          msg, ebuf);
  abort();
}
