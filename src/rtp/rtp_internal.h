#define FANOUT_MAX 128

/* This is not configurable. RTP is designed for 3 stages
 * Stage 1 - leaf stage (all shared memory, ideally)
 * Stage 3 - to final root
 * */
#define STAGES_MAX 3
