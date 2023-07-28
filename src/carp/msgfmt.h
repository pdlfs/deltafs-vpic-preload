#pragma once

/*
 * msgfmt.h: utility functions to wrap messages in buffers
 *
 * MsgFmt does not own any buffers or do any memory management.
 * It merely provides utilities to package various message types
 * to a user-specified buffer and vice versa.
 *
 * TODO: implement better error handling than msg_abort
 */

#include <unistd.h>

#define MSGFMT_RTP_MAGIC 0x57

/* NOTE: PIVOT and PVT_BCAST use the same underlying msg format */
#define MSGFMT_RTP_BEGIN     (0x0100|MSGFMT_RTP_MAGIC)
#define MSGFMT_RTP_PIVOT     (0x0200|MSGFMT_RTP_MAGIC)
#define MSGFMT_RTP_PVT_BCAST (0x0300|MSGFMT_RTP_MAGIC)

namespace pdlfs {
namespace carp {

int msgfmt_encode_rtp_begin(void* buf, size_t buf_sz, int rank, int round_num);

void msgfmt_decode_rtp_begin(void* buf, size_t buf_sz, int* rank,
                             int* round_num);

/**
 * @brief buffer space needed for num_pivots_
 *
 * @param num_pivots
 *
 * @return
 */
size_t msgfmt_bufsize_rtp_pivots(int num_pivots);

/**
 * @brief
 *
 * @param buf
 * @param buf_sz
 * @param round_num
 * @param stage_num 1-indexed, RTP stage [1|2|3]
 * @param sender_id
 * @param pivots
 * @param pivot_weight
 * @param num_pivots
 *
 * @return
 */
int msgfmt_encode_rtp_pivots(void* buf, size_t buf_sz, int round_num,
                             int stage_num, int sender_id, double* pivots,
                             double pivot_weight, int num_pivots);

void msgfmt_decode_rtp_pivots(void* buf, size_t buf_sz, int* round_num,
                              int* stage_num, int* sender_id, double** pivots,
                              double* pivot_weight, int* num_pivots);
}  // namespace carp
}  // namespace pdlfs
