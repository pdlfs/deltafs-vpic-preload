#include "msgfmt.h"

#include "common.h"

/* begin: fixed size => rank, round_num */
int msgfmt_encode_rtp_begin(void* buf, size_t buf_sz, int rank, int round_num) {
  memcpy(buf, &rank, sizeof(int));  /* XXXCDC: int not fixed sized */
  memcpy((char*)buf+sizeof(int), &round_num, sizeof(int));
  return(2 * sizeof(int));
}

void msgfmt_decode_rtp_begin(void* buf, size_t buf_sz, int* rank,
                             int* round_num) {
  memcpy(rank, buf, sizeof(*rank));  /* XXXCDC: int not fixed size */
  memcpy(round_num, (char*)buf+sizeof(*rank), sizeof(*round_num));
}

/*
 * pivots: round_num, stage_num, sender_id, num_pivots,
 *             pivot_weight, pivots[num_piv]        (last 2 are double)
 */
size_t msgfmt_bufsize_rtp_pivots(int num_pivots) {
  size_t rv;
  rv = 4*sizeof(int) + sizeof(double) + (num_pivots*sizeof(double));
  rv += 2;   /* XXX for good measure */
  return(rv);
}

int msgfmt_encode_rtp_pivots(void* buf, size_t buf_sz, int round_num,
                             int stage_num, int sender_id, double* pivots,
                             double pivot_weight, int num_pivots, bool bcast) {
  char *bp = (char *)buf;
  size_t rv = msgfmt_bufsize_rtp_pivots(num_pivots);
  assert(buf_sz >= rv);
  memcpy(bp, &round_num, sizeof(round_num));       bp += sizeof(round_num);
  memcpy(bp, &stage_num, sizeof(stage_num));       bp += sizeof(stage_num);
  memcpy(bp, &sender_id, sizeof(sender_id));       bp += sizeof(sender_id);
  memcpy(bp, &num_pivots, sizeof(num_pivots));     bp += sizeof(num_pivots);
  memcpy(bp, &pivot_weight, sizeof(pivot_weight)); bp += sizeof(pivot_weight);
  memcpy(bp, pivots, sizeof(pivots[0]) * num_pivots);

  return(rv);
}

void msgfmt_decode_rtp_pivots(void* buf, size_t buf_sz, int* round_num,
                              int* stage_num, int* sender_id, double** pivots,
                              double* pivot_weight, int* num_pivots,
                              bool bcast) {
  char *bp = (char *)buf;
  assert(buf_sz >= msgfmt_bufsize_rtp_pivots(0));
  memcpy(round_num, bp, sizeof(*round_num));       bp += sizeof(*round_num);
  memcpy(stage_num, bp, sizeof(*stage_num));       bp += sizeof(*stage_num);
  memcpy(sender_id, bp, sizeof(*sender_id));       bp += sizeof(*sender_id);
  memcpy(num_pivots, bp, sizeof(*num_pivots));     bp += sizeof(*num_pivots);
  memcpy(pivot_weight, bp, sizeof(*pivot_weight)); bp += sizeof(*pivot_weight);

  /* XXXCDC: assumes alignment of bp is ok for doubles */
  (*pivots) = reinterpret_cast<double*>(bp);

  return;
}
