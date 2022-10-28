#pragma once

#include <unistd.h>

namespace pdlfs {
class PivotLogger {
 public:
  PivotLogger(std::string log_file) : log_file_(log_file) {}

  void LogHeader() {
    if (access(log_file_.c_str(), F_OK) == 0) return;

    const char* header = "runtype,nranks,pvtcnt,epidx,load_std\n";
    FILE* f = fopen(log_file_.c_str(), "a+");
    fwrite(header, strlen(header), 1, f);
    fclose(f);
  }

  void LogData(std::string& runtype,  int nranks, int pvtcnt, int ep_idx, double load_std) {
    LogHeader();
    logf(LOG_INFO, "Logging run to: %s\n", log_file_.c_str());
    FILE* f = fopen(log_file_.c_str(), "a+");
    fprintf(f, "%s,%d,%d,%d,%f\n", runtype.c_str(), nranks, pvtcnt, ep_idx, load_std);
    fclose(f);
  }

 private:
  const std::string log_file_;
};
}  // namespace pdlfs
