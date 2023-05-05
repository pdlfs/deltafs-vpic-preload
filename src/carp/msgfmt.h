#pragma once

/* msgfmt.h: utility functions to wrap messages in buffers
 *
 * MsgFmt does not own any buffers or does any memory management
 * It merely provides utilities to package various message types to a
 * user-specified buffer and vice versa.
 *
 * MsgFmt does not provide any way to calculate the buffer size needed to handle
 * specific messages/message types. We assume that a XXX: 255-byte buffer is
 * sufficient for all message types, and all control message structs are
 * designed to adhere to this constraint.
 *
 * TODO: implement better error handling than msg_abort
 */

#include <vector>

#include "common.h"

/* XXX: we're not strictly following this limit
 * as the size of the pivot_msg can be anything
 * revaluate and remove it? */
#define MSGFMT_MAX_BUFSIZE 255

#define ABORT_FILENAME \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define ABORT(msg) msg_abort(errno, msg, __func__, ABORT_FILENAME, __LINE__)

/* abort with an error message: forward decl */
void msg_abort(int err, const char* msg, const char* func, const char* file,
               int line);

#define MSGFMT_RTP_MAGIC 0x57

#define MSGFMT_RTP_BEGIN     (0x0100|MSGFMT_RTP_MAGIC)
#define MSGFMT_RTP_PIVOT     (0x0200|MSGFMT_RTP_MAGIC)
#define MSGFMT_RTP_PVT_BCAST (0x0300|MSGFMT_RTP_MAGIC)

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
 * @param pivot_width
 * @param num_pivots
 *
 * @return
 */
int msgfmt_encode_rtp_pivots(void* buf, size_t buf_sz, int round_num,
                             int stage_num, int sender_id, double* pivots,
                             double pivot_width, int num_pivots,
                             bool bcast);

void msgfmt_decode_rtp_pivots(void* buf, size_t buf_sz, int* round_num,
                              int* stage_num, int* sender_id, double** pivots,
                              double* pivot_width, int* num_pivots,
                              bool bcast);
