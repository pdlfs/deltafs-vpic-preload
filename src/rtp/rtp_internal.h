#pragma once

#include <stdint.h>

#define FANOUT_MAX 128

/* This is not configurable. RTP is designed for 3 stages
 * Stage 1 - leaf stage (all shared memory, ideally)
 * Stage 3 - to final root
 * */
#define STAGES_MAX 3

/* Hash function, from
 * https://stackoverflow.com/questions/8317508/hash-function-for-a-string
 */
#define A 54059 /* a prime */
#define B 76963 /* another prime */
#define C 86969 /* yet another prime */
#define FIRSTH 37 /* also prime */
uint32_t hash_str(const char* data, int slen)
{
  uint32_t h = FIRSTH;
  for (int sidx = 0; sidx < slen; sidx++) {
    char s = data[sidx];
    h = (h * A) ^ (s * B);
  }
  return h % C;
}