#pragma once

#include <linux/limits.h>

#include <algorithm>

#include "tools_common.h"

class TraceReader {
 public:
  TraceReader(int rank, const char* dir_path, int cur_ts)
      : rank_(rank), dir_path_(dir_path), cur_ts_(cur_ts) {}

  virtual bool ReadNext(float* f) = 0;

 protected:
  virtual void BeginTimestep(int ts) = 0;

  static void OpenFile(FILE** f, std::string& dpath, int rank, int timestep) {
    char fpath[PATH_MAX];
    snprintf(fpath, PATH_MAX, "%s/T.%d/eparticle.%d.%d", dpath.c_str(),
             timestep, timestep, rank);

    if (rank == 0) {
      fprintf(stdout, "[range-runner] trace: %s\n", fpath);
    }

    FILE* tmp = fopen(fpath, "r");
    if (tmp == nullptr) {
      complain(EXIT_FAILURE, 0, "TraceOpen failed: %s\n", fpath);
    }

    *f = tmp;
  }

  static void CloseFile(FILE** f) {
    if (*f == nullptr) return;
    fclose(*f);
    *f = nullptr;
  }

  static int GetTraceRanks(const char* trace_path, int ts) {
    char ts_path[PATH_MAX];
    snprintf(ts_path, PATH_MAX, "%s/T.%d", trace_path, ts);

    struct dirent* entry;
    DIR* dir = opendir(ts_path);

    if (dir == NULL) {
      fprintf(stderr, "[range-runner] Failed to open tracedir: %s\n",
              trace_path);
      exit(1);
    }

    int nranks = 0;

    char fprefix[255];
    snprintf(fprefix, 255, "eparticle.%d.", ts);
    size_t fprefixlen = strlen(fprefix);

    while ((entry = readdir(dir))) {
      const char* dname = entry->d_name;
      size_t dsz = strlen(dname);
      if (dsz < fprefixlen) continue;

      if ((strncmp(dname, fprefix, fprefixlen))) continue;

      long int rank = strtol(&(dname[fprefixlen]), NULL, 10);
      if (rank == LONG_MAX) continue;

      if (rank > INT_MAX) continue;
      nranks = std::max(nranks, (int)(rank + 1));
    }

    closedir(dir);

    return nranks;
  }

  int rank_;
  std::string dir_path_;
  int cur_ts_;
};

class SimpleTraceReader : public TraceReader {
 public:
  SimpleTraceReader(int rank, const char* dir_path, int cur_ts)
      : trace_file_(nullptr), TraceReader(rank, dir_path, cur_ts) {
    BeginTimestep(cur_ts);
  }

  bool ReadNext(float* key) override {
    size_t bytes_read =
        fread(static_cast<void*>(key), 1, sizeof(float), trace_file_);

    if (bytes_read != sizeof(float)) {
      if (feof(trace_file_)) {
        return false;
      } else {
        complain(EXIT_FAILURE, 0, "trace fread error");
        /* should die anyway */
        return false;
      }
    }

    return true;
  }

  ~SimpleTraceReader() {
    if (trace_file_) {
      CloseFile(&trace_file_);
    }
  }

 protected:
  void BeginTimestep(int ts) override {
    if (trace_file_) {
      CloseFile(&trace_file_);
    }

    OpenFile(&trace_file_, dir_path_, rank_, cur_ts_);
  }

  FILE* trace_file_;
};

class WeakScalingTraceReader : public SimpleTraceReader {
 public:
  WeakScalingTraceReader(int rank, const char* dir_path, int cur_ts)
      : trace_nranks_(GetTraceRanks(dir_path, cur_ts)),
        rank_to_mirror_(rank % trace_nranks_),
        SimpleTraceReader(rank_to_mirror_, dir_path, cur_ts) {
    fprintf(stdout, "Rank %d, mirroring rank %d\n", rank, rank_to_mirror_);
  }

 protected:
  int trace_nranks_;
  int rank_to_mirror_;
};
