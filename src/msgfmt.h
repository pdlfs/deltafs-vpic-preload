#pragma once

/* msgfmt.h: utility functions to wrap messages in buffers
 *
 * MsgFmt does not own any buffers or does any memory management
 * It merely provides utilities to package various message types to a
 * user-specified buffer and vice versa.
 *
 * For now, we assume two message types. Arbitrary control message types with
 * their own formats, and a generic data message type, which consists of a
 * filename and data
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

#define MSGFMT_DATA (unsigned char)0x01
#define MSGFMT_RENEG_BEGIN (unsigned char)0x02
#define MSGFMT_RENEG_PIVOTS (unsigned char)0x03
#define MSGFMT_RENEG_ACK (unsigned char)0x04

#define MSGFMT_TYPE_SIZE 1u

#define MSGFMT_RTP_MAGIC 0x57
#define MSGFMT_RTP_BEGIN 0x01
#define MSGFMT_RTP_PIVOT 0x02
#define MSGFMT_RTP_PVT_BCAST 0x03

uint32_t msgfmt_get_data_size(int fname_sz, int data_sz, int extra_data_sz);

uint32_t msgfmt_write_data(char* buf, int buf_sz, const char* fname,
                           int fname_sz, const char* fdata, int data_sz,
                           int extra_data_sz);

void msgfmt_parse_data(char* buf, int buf_sz, char** fname, int fname_sz,
                       char** fdata, int data_sz);

uint32_t msgfmt_encode_reneg_begin(char* buf, int buf_sz, int round_no,
                                   int my_rank);

void msgfmt_parse_reneg_begin(char* buf, int buf_sz, int* round_no,
                              int* my_rank);

unsigned char msgfmt_get_msgtype(char* buf);

uint32_t msgfmt_nbytes_reneg_pivots(int num_pivots);

void msgfmt_encode_reneg_pivots(char* buf, int buf_sz, int round_no,
                                double* pivots, double pivot_width,
                                int num_pivots);

void msgfmt_parse_reneg_pivots(char* buf, int buf_sz, int* round_no,
                               double** pivots, double* pivot_width,
                               int* num_pivots);

void msgfmt_encode_ack(char* buf, int buf_sz, int rank, int round_no);

void msgfmt_parse_ack(char* buf, int buf_sz, int* rank, int* round_no);

unsigned char msgfmt_get_rtp_msgtype(char* buf);

int msgfmt_encode_rtp_begin(char* buf, int buf_sz, int rank, int round_num);

void msgfmt_decode_rtp_begin(char* buf, int buf_sz, int* rank, int* round_num);

/**
 * @brief buffer space needed for num_pivots
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
int msgfmt_encode_rtp_pivots(char* buf, int buf_sz, int round_num,
                             int stage_num, int sender_id, double* pivots,
                             double pivot_width, int num_pivots,
                             bool bcast = false);

void msgfmt_decode_rtp_pivots(char* buf, int buf_sz, int* round_num,
                              int* stage_num, int* sender_id, double** pivots,
                              double* pivot_width, int* num_pivots,
                              bool bcast = false);
