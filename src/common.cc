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
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <unistd.h>

#include "common.h"

#include <string>

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
    errno = 0;
  }
  l = strlen(tmp);
  if (l > 120) {
    tmp[120] = 0;
    strcat(tmp, " ...");
  } else if (l == 0) {
    strcat(tmp, "?");
  }

  return (tmp);
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

  return (tmp);
}

/*
 * try_scan_sysfs(): scan sysfs for important information ^_%
 */
void try_scan_sysfs() {
  DIR* d;
  DIR* dd;
  struct dirent* dent;
  struct dirent* ddent;
  const char* dirname;
  char msg[200];
  char path[PATH_MAX];
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
    snprintf(msg, sizeof(msg), "[sys] %d CPU_cores", ncpus);
    info(msg);
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
    snprintf(msg, sizeof(msg), "[sys] %d NUMA_nodes", nnodes);
    info(msg);
  }

  nnics = 0;
  dirname = "/sys/class/net";
  d = opendir(dirname);
  if (d != NULL) {
    dent = readdir(d);
    for (; dent != NULL; dent = readdir(d)) {
      if (strcmp(dent->d_name, "lo") != 0 && strcmp(dent->d_name, ".") != 0 &&
          strcmp(dent->d_name, "..") != 0) {
        nic = dent->d_name;
        snprintf(path, sizeof(path), "/sys/class/net/%s/tx_queue_len",
                 dent->d_name);
        txqlen = readline(path);
        snprintf(path, sizeof(path), "/sys/class/net/%s/speed", dent->d_name);
        speed = readline(path);
        snprintf(path, sizeof(path), "/sys/class/net/%s/mtu", dent->d_name);
        mtu = readline(path);
        tx = 0;
        rx = 0;
        snprintf(path, sizeof(path), "/sys/class/net/%s/queues", dent->d_name);
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
                 "%5s, mtu %4s, rx-irq: %3d, tx-irq: %3d (%s)",
                 speed.c_str(), txqlen.c_str(), mtu.c_str(), rx, tx,
                 nic.c_str());
        info(msg);
      }
    }
    closedir(d);
  }

  errno = 0;
}

/*
 * try_scan_procfs(): scan procfs for important information ^_%
 */
void try_scan_procfs() {
  int num_cpus;
  std::string cpu_type;
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
      } else if (key == "processor") {
        num_cpus++;
      }
    }
    fclose(cpuinfo);
    if (num_cpus != 0) {
      snprintf(msg, sizeof(msg), "[cpu] %d x %s", num_cpus, cpu_type.c_str());
      info(msg);
    }
  }

  os = readline("/proc/version_signature");
  if (strcmp(os.c_str(), "?") == 0) {
    os = readline("/proc/version");
  }
  snprintf(msg, sizeof(msg), "[os] %s", os.c_str());
  info(msg);

  errno = 0;
}

/*
 * misc_checks(): check cpu affinity and rlimits.
 */
void misc_checks(int myrank, int worldsz) {
  struct rlimit rl;
  long long softnofile;
  long long hardnofile;
  long long oknofile;
  long long softmemlock;
  long long hardmemlock;
  cpu_set_t cpuset;
  int ncputset;
  int cpus;
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
      warn(msg);
    } else {
      info(msg);
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
    info(msg);
  }

#if defined(_SC_NPROCESSORS_CONF)
  cpus = sysconf(_SC_NPROCESSORS_CONF);
  if (cpus != -1) {
    n = sched_getaffinity(0, sizeof(cpuset), &cpuset);
    if (n == 0) {
      ncputset = CPU_COUNT(&cpuset);
      snprintf(msg, sizeof(msg), "[numa] cpu affinity: %d/%d cores", ncputset,
               cpus);
      if (ncputset == cpus) {
        warn(msg);
      } else {
        info(msg);
      }
    }
  }
#endif

  errno = 0;
}
