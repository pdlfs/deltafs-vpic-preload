#pragma once

#include <vector>

// TODO: make this configurable
#define RANGE_BUFSZ 1000
// TODO: Can shorten this by using indirect ptr?
#define RANGE_MAX_PSZ 255
#define RANGE_MAX_OOB_THRESHOLD 1000

typedef struct particle_mem {
  float indexed_prop;       // property for range query
  char ptr[RANGE_MAX_PSZ];  // other data
} particle_mem_t;

enum class range_state_t { RS_INIT, RS_READY };

typedef struct range_ctx {
  /* range data structures */
  float negotiated_range_start;
  float negotiated_range_end;

  int ts_writes_received; 
  int ts_writes_shuffled;

  range_state_t range_state;
  std::vector<float> rank_bins;

  std::vector<particle_mem_t> oob_buffer;
  // oob_count = size of last filed oob_buffer idx
  int oob_count;
} range_ctx_t;

