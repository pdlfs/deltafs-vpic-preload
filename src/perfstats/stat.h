#pragma once

#include <stdio.h>
#include <stdint.h>

#define STAT_BUF_MAX 1024

namespace pdlfs {
enum class StatType { V_FLOAT, V_INT, V_UINT64, V_STR };

class Stat {
 private:
  uint64_t timestamp_;
  StatType stat_type_;
  const char *stat_label_;
  union {
    float v_float_;
    int v_int_;
    uint64_t v_uint64_;
    char v_str_[STAT_BUF_MAX];
  };
 public:
  Stat(const StatType stat_type, const char *stat_label)
      : stat_type_(stat_type), stat_label_(stat_label) {}

  int SetType(const StatType stat_type, const char* label);

  int SetValue(uint64_t timestamp, float value);

  int SetValue(uint64_t timestamp, int value);

  int SetValue(uint64_t timestamp, uint64_t value);

  int SetValue(uint64_t timestamp, const char* value);

  int Serialize(FILE *output_file);
};

class StatLogger {
 public:
  virtual int LogOnce(uint64_t timestamp, Stat &s) = 0;
  virtual ~StatLogger() {}
};
}