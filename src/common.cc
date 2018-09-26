/*
 * Copyright (c) 2017-2018, Carnegie Mellon University.
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

#include "common.h"

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <sched.h>
#include <stdarg.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>

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
  char msg[100];
  int n;
#if defined(__linux) && defined(PRELOAD_USE_CLOCK_GETTIME)
  struct timespec res;
  n = clock_getres(CLOCK_MONOTONIC_COARSE, &res);
  if (n == 0) {
    snprintf(msg, sizeof(msg), "[clock] CLOCK_MONOTONIC_COARSE: %d us",
             int(res.tv_sec * 1000 * 1000 + res.tv_nsec / 1000));
    INFO(msg);
  }
  n = clock_getres(CLOCK_MONOTONIC, &res);
  if (n == 0) {
    snprintf(msg, sizeof(msg), "[clock] CLOCK_MONOTONIC: %d ns",
             int(res.tv_sec * 1000 * 1000 * 1000 + res.tv_nsec));
    INFO(msg);
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
  char msg[100];
  int sse42;
  int n;
  CHECK_SSE42(sse42);
  if (sse42) {
    n = snprintf(msg, sizeof(msg), "[sse] SSE V4.2 extension is available");
#if defined(__GUNC__) && defined(__SSE4_2__)
    snprintf(msg + n, sizeof(msg) - n, "\n>>> __SSE4_2__ is %d", __SSE4_2__);
#else
    snprintf(msg + n, sizeof(msg) - n, "\n>>> yet __SSE4_2__ is not defined");
#endif
  } else {
    snprintf(msg, sizeof(msg), "[sse] SSE V4.2 is not available");
  }

  INFO(msg);
}

/* read a line from file */
static std::string readline(const char* fname) {
  char tmp[1000];
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
  char tmp[1000];
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
  DIR* d;
  DIR* dd;
  struct dirent* dent;
  struct dirent* ddent;
  const char* dirname;
  char msg[600];
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

  if (access("/sys", R_OK) != 0) {
    /* give up */
    errno = 0;
    return;
  }

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

  snprintf(msg, sizeof(msg),
           "[sys] %d NUMA nodes / %d CPU cores (L1: %s + %s, L2: %s, L3: %s)",
           nnodes, ncpus, idx[0].c_str(), idx[1].c_str(), idx[2].c_str(),
           idx[3].c_str());
  INFO(msg);

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
        snprintf(msg, sizeof(msg),
                 "[if] speed %5s Mbps, tx_queue_len "
                 "%5s, mtu %5s, rx-irq: %3d, tx-irq: %3d (%s)",
                 speed.c_str(), txqlen.c_str(), mtu.c_str(), rx, tx,
                 nic.c_str());
        INFO(msg);
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
              snprintf(msg, sizeof(msg),
                       "[slurm] job cgroup cpuset: %s, memset: %s\n>>> %s/%s",
                       jobcpuset.c_str(), jobmemset.c_str(), dent->d_name,
                       ddent->d_name);
              INFO(msg);
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

  errno = 0;
}

/*
 * try_scan_procfs(): scan procfs for important device information.
 */
void try_scan_procfs() {
  int num_cpus;
  std::string cpu_type;
  std::string L1_cache_size;
  char msg[200];
  char line[1000];
  const char* sep;
  std::string value;
  std::string key;
  std::string os;
  FILE* cpuinfo;

  if (access("/proc", R_OK) != 0) {
    /* give up */
    errno = 0;
    return;
  }

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
      snprintf(msg, sizeof(msg), "[cpu] %d x %s (L2/L3 cache: %s)", num_cpus,
               cpu_type.c_str(), L1_cache_size.c_str());
      INFO(msg);
    }
  }

  os = readline("/proc/version_signature");
  if (strcmp(os.c_str(), "?") == 0) {
    os = readline("/proc/version");
  }
  snprintf(msg, sizeof(msg), "[os] %s (VM page: %d bytes)", os.c_str(),
           getpagesize());
  INFO(msg);

  errno = 0;
}

void maybe_warn_rlimit(int myrank, int worldsz) {
  struct rlimit rl;
  long long softnofile;
  long long hardnofile;
  long long oknofile;
  long long softmemlock;
  long long hardmemlock;
  char msg[200];
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
    snprintf(msg, sizeof(msg),
             "[ulimit] max open files per process: "
             "%lld soft, %lld hard, %lld suggested",
             softnofile, hardnofile, oknofile);
    if (softnofile < oknofile) {
      WARN(msg);
    } else {
      INFO(msg);
    }
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
    snprintf(msg, sizeof(msg),
             "[ulimit] max memlock size: "
             "%lld soft, %lld hard",
             softmemlock, hardmemlock);
    INFO(msg);
  }

  errno = 0;
}

void maybe_warn_numa() {
#ifdef PRELOAD_HAS_NUMA
  char msg[500];
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
  snprintf(msg, sizeof(msg), "[numa] cpu: %d/%d cores\n>>> %s", my, os,
           cpu.c_str());
  INFO(msg);

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
  snprintf(msg, sizeof(msg), "[numa] mem: %d/%d nodes\n>>> %s", my, os,
           mem.c_str());
  INFO(msg);

#endif
  errno = 0;
}

std::string get_meminfo() {
  char fp[100];

  snprintf(fp, sizeof(fp), "/proc/%d/statm", getpid());
  std::string info = readline(fp);

  return info;
}

long my_maxrss() {
  struct rusage ru;
  int r;

  r = getrusage(RUSAGE_SELF, &ru);
  if (r == 0) return ru.ru_maxrss;

  return 0;
}

int my_cpu_cores() {
  cpu_set_t cpuset;
  int ncpus;
  int n;

  CPU_ZERO(&cpuset);
  ncpus = 0;
  n = sched_getaffinity(0, sizeof(cpuset), &cpuset);
  if (n == 0) {
    ncpus = CPU_COUNT(&cpuset);
  }

  return ncpus;
}

void SAY(int err, const char* prefix, const char* msg) {
  fprintf(stderr, "%s %s", prefix, msg);
  if (err != 0) fprintf(stderr, ": %s (errno=%d)", strerror(err), err);
  fprintf(stderr, "\n");
}

void LOG(int fd, int e, const char* fmt, ...) {
  char tmp[500];
  va_list va;
  int n;
  va_start(va, fmt);
  n = vsnprintf(tmp, sizeof(tmp), fmt, va);
  if (e != 0) {
    n += snprintf(tmp + n, sizeof(tmp) - n, ": %s(err=%d)", strerror(e), e);
  }
  n += snprintf(tmp + n, sizeof(tmp) - n, "\n");
  n = write(fd, tmp, n);
  va_end(va);
  errno = 0;
}
