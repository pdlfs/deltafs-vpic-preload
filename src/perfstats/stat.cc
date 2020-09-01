#include "stat.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

namespace pdlfs {
int Stat::SetType(const StatType stat_type, const char *label) {
  int rv = 0;
  stat_type_ = stat_type;
  stat_label_ = label;
  return rv;
}

int Stat::SetValue(uint64_t timestamp, float value) {
  int rv = 0;
  if (stat_type_ != StatType::V_FLOAT) {
    rv = -1;
    return rv;
  }

  timestamp_ = timestamp;
  v_float_ = value;
  return rv;
}

int Stat::SetValue(uint64_t timestamp, int value) {
  int rv = 0;
  if (stat_type_ != StatType::V_INT) {
    rv = -1;
    return rv;
  }

  timestamp_ = timestamp;
  v_int_ = value;
  return rv;
}

int Stat::SetValue(uint64_t timestamp, uint64_t value) {
  int rv = 0;
  if (stat_type_ != StatType::V_UINT64) {
    rv = -1;
    return rv;
  }

  timestamp_ = timestamp;
  v_uint64_ = value;
  return rv;
}

int Stat::SetValue(uint64_t timestamp, const char *value) {
  int rv = 0;
  if (stat_type_ != StatType::V_STR) {
    rv = -1;
    return rv;
  }

  timestamp_ = timestamp;
  strncpy(v_str_, value, STAT_BUF_MAX);
  return rv;
}

int Stat::Serialize(FILE *output_file) {
  int rv = 0;

  switch (stat_type_) {
    case StatType::V_FLOAT:
      fprintf(output_file, "%lu,%s,%.2f\n", timestamp_, stat_label_, v_float_);
      break;
    case StatType::V_INT:
      fprintf(output_file, "%lu,%s,%d\n", timestamp_, stat_label_, v_int_);
      break;
    case StatType::V_UINT64:
      fprintf(output_file, "%lu,%s,%lu\n", timestamp_, stat_label_, v_uint64_);
      break;
    case StatType::V_STR:
      fprintf(output_file, "%lu,%s,%s\n", timestamp_, stat_label_, v_str_);
      break;
    default:
      rv = -1;
      break;
  }

  return rv;
}
}  // namespace pdlfs