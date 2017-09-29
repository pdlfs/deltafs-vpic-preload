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

#pragma once

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <string>
/* a set of utilities for probing important system configurations. */
void check_clockres();
void check_sse42();
void maybe_warn_rlimit(int myrank, int worldsz);
void maybe_warn_cpuaffinity();
void try_scan_procfs();
void try_scan_sysfs();

/* get the number of cpu cores that we may use */
int my_cpu_cores();

/* get the current time in us. */
uint64_t now_micros();

/* get the current time in us with fast but coarse-grained timestamps. */
uint64_t now_micros_coarse();

/* convert posix timeval to micros */
uint64_t timeval_to_micros(const struct timeval* tv);

/* log message into a given file using unbuffered io. */
inline void LOG(int fd, int e, const char* fmt, ...) {
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

/*
 * logging facilities and helpers
 */
#define ABORT_FILENAME \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define ABORT(msg) msg_abort(msg, __func__, ABORT_FILENAME, __LINE__)
#define LOG_SINK fileno(stderr)

inline void INFO(const char* msg) { LOG(LOG_SINK, 0, "-INFO- %s", msg); }
inline void WARN(const char* msg) { LOG(LOG_SINK, 0, "-- WARNING -- %s", msg); }
inline void ERROR(const char* msg) {
  LOG(LOG_SINK, errno, "!!! ERROR !!! %s", msg);
}

inline void msg_abort(const char* msg, const char* func, const char* file,
                      int line) {
  LOG(LOG_SINK, errno, "*** ABORT *** (%s:%d) %s()] %s", file, line, func, msg);
  abort();
}

inline const char* maybe_getenv(const char* key) {
  const char* env;
  env = getenv(key);
  errno = 0;
  return env;
}

inline int is_envset(const char* key) {
  std::string str;
  const char* env;
  env = maybe_getenv(key);
  if (env == NULL || env[0] == 0) return false;
  str = env;

  return int(str != "0");
}

inline int getr(int min, int max) {
  return static_cast<int>(
      (static_cast<double>(rand()) / (static_cast<double>(RAND_MAX) + 1)) *
          (max - min + 1) +
      min);
}

inline int pthread_cv_notifyall(pthread_cond_t* cv) {
  errno = 0;
  int r = pthread_cond_broadcast(cv);
  if (r != 0) {
    errno = r;
    ABORT("cv_sigall");
  }

  return 0;
}

inline int pthread_cv_timedwait(pthread_cond_t* cv, pthread_mutex_t* mtx,
                                const timespec* due) {
  errno = 0;
  int r = pthread_cond_timedwait(cv, mtx, due);
  if (r == ETIMEDOUT) {
    return r;
  }
  if (r != 0) {
    errno = r;
    ABORT("cv_timedwait");
  }

  return 0;
}

inline int pthread_cv_wait(pthread_cond_t* cv, pthread_mutex_t* mtx) {
  errno = 0;
  int r = pthread_cond_wait(cv, mtx);
  if (r != 0) {
    errno = r;
    ABORT("cv_wait");
  }

  return 0;
}

inline int pthread_mtx_lock(pthread_mutex_t* mtx) {
  errno = 0;
  int r = pthread_mutex_lock(mtx);
  if (r != 0) {
    errno = r;
    ABORT("mtx_lock");
  }

  return 0;
}

inline int pthread_mtx_unlock(pthread_mutex_t* mtx) {
  errno = 0;
  int r = pthread_mutex_unlock(mtx);
  if (r != 0) {
    errno = r;
    ABORT("mtx_unlock");
  }

  return 0;
}

inline void softlink(const char* src, const char* dst) {
  int n;
  n = unlink(dst);
  n = symlink(src, dst);
  errno = 0;
}

#define PRELOAD_PRETTY_SIZE_USE_BINARY

/* print a human-readable time duration. */
inline std::string pretty_dura(double us) {
  char tmp[100];
#ifndef NDEBUG
  snprintf(tmp, sizeof(tmp), "%.0f us", us);
#else
  if (us >= 1000000) {
    snprintf(tmp, sizeof(tmp), "%.3f s", us / 1000000.0);
  } else {
    snprintf(tmp, sizeof(tmp), "%.3f ms", us / 1000.0);
  }
#endif
  return tmp;
}

/* print a human-readable integer number. */
inline std::string pretty_num(double num) {
  char tmp[100];
#ifndef NDEBUG
  snprintf(tmp, sizeof(tmp), "%.0f", num);
#else
#if defined(PRELOAD_PRETTY_USE_BINARY)
  if (num >= 1099511627776.0) {
    num /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.1f Ti", num);
  } else if (num >= 1073741824.0) {
    num /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.1f Gi", num);
  } else if (num >= 1048576.0) {
    num /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.1f Mi", num);
  } else if (num >= 1024.0) {
    num /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.1f Ki", num);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f", num);
  }
#else
  if (num >= 1000000000000.0) {
    num /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f T", num);
  } else if (num >= 1000000000.0) {
    num /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f G", num);
  } else if (num >= 1000000.0) {
    num /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f M", num);
  } else if (num >= 1000.0) {
    num /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.1f K", num);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f", num);
  }
#endif
#endif
  return tmp;
}

/* print a human-readable I/O throughput number. */
inline std::string pretty_tput(double ops, double us) {
  char tmp[100];
  double ops_per_s = ops / us * 1000000;
#ifndef NDEBUG
  snprintf(tmp, sizeof(tmp), "%.0f", ops_per_s);
#else
#if defined(PRELOAD_PRETTY_USE_BINARY)
  if (ops_per_s >= 1099511627776.0) {
    ops_per_s /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.3f Tiop/s", ops_per_s);
  } else if (ops_per_s >= 1073741824.0) {
    ops_per_s /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.3f Giop/s", ops_per_s);
  } else if (ops_per_s >= 1048576.0) {
    ops_per_s /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.3f Miop/s", ops_per_s);
  } else if (ops_per_s >= 1024.0) {
    ops_per_s /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.3f Kiop/s", ops_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f op/s", ops_per_s);
  }
#else
  if (ops_per_s >= 1000000000000.0) {
    ops_per_s /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Top/s", ops_per_s);
  } else if (ops_per_s >= 1000000000.0) {
    ops_per_s /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Gop/s", ops_per_s);
  } else if (ops_per_s >= 1000000.0) {
    ops_per_s /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Mop/s", ops_per_s);
  } else if (ops_per_s >= 1000.0) {
    ops_per_s /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Kop/s", ops_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f op/s", ops_per_s);
  }
#endif
#endif
  return tmp;
}

/* print a human-readable I/O size. */
inline std::string pretty_size(double size) {
  char tmp[100];
#ifndef NDEBUG
  snprintf(tmp, sizeof(tmp), "%.0f bytes", size);
#else
#if defined(PRELOAD_PRETTY_SIZE_USE_BINARY)
  if (size >= 1099511627776.0) {
    size /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.1f TiB", size);
  } else if (size >= 1073741824.0) {
    size /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.1f GiB", size);
  } else if (size >= 1048576.0) {
    size /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.1f MiB", size);
  } else if (size >= 1024.0) {
    size /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.1f KiB", size);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f bytes", size);
  }
#else
  if (size >= 1000000000000.0) {
    size /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f TB", size);
  } else if (size >= 1000000000.0) {
    size /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f GB", size);
  } else if (size >= 1000000.0) {
    size /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f MB", size);
  } else if (size >= 1000.0) {
    size /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.1f KB", size);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f bytes", size);
  }
#endif
#endif
  return tmp;
}

/* print a human-readable data bandwidth number. */
inline std::string pretty_bw(double bytes, double us) {
  char tmp[100];
  double bytes_per_s = bytes / us * 1000000;
#ifndef NDEBUG
  snprintf(tmp, sizeof(tmp), "%.0f bytes/s", bytes_per_s);
#else
#if defined(PRELOAD_PRETTY_SIZE_USE_BINARY)
  if (bytes_per_s >= 1099511627776.0) {
    bytes_per_s /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.3f TiB/s", bytes_per_s);
  } else if (bytes_per_s >= 1073741824.0) {
    bytes_per_s /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.3f GiB/s", bytes_per_s);
  } else if (bytes_per_s >= 1048576.0) {
    bytes_per_s /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.3f MiB/s", bytes_per_s);
  } else if (bytes_per_s >= 1024.0) {
    bytes_per_s /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.3f KiB/s", bytes_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.3f bytes/s", bytes_per_s);
  }
#else
  if (bytes_per_s >= 1000000000000.0) {
    bytes_per_s /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f TB/s", bytes_per_s);
  } else if (bytes_per_s >= 1000000000.0) {
    bytes_per_s /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f GB/s", bytes_per_s);
  } else if (bytes_per_s >= 1000000.0) {
    bytes_per_s /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f MB/s", bytes_per_s);
  } else if (bytes_per_s >= 1000.0) {
    bytes_per_s /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.3f KB/s", bytes_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.3f bytes/s", bytes_per_s);
  }
#endif
#endif
  return tmp;
}

#undef PRELOAD_PRETTY_SIZE_USE_BINARY
