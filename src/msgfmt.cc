#include "msgfmt.h"

uint32_t msgfmt_get_data_size(int fname_sz, int data_sz, int extra_data_sz) {
  return MSGFMT_TYPE_SIZE + fname_sz + 1 + data_sz + extra_data_sz;
}

uint32_t msgfmt_write_data(char *buf, int buf_sz, const char *fname,
                           int fname_sz, const char *fdata, int data_sz,
                           int extra_data_sz) {
  uint32_t base_sz = MSGFMT_TYPE_SIZE + fname_sz + 1 + data_sz;

  if (base_sz > buf_sz) {
    fprintf(stderr, "Base size: %u buf size: %u\n", base_sz, buf_sz);
    ABORT("Buffer overflow");
  }

  buf[0] = MSGFMT_DATA;

  memcpy(buf + MSGFMT_TYPE_SIZE, fname, fname_sz);
  buf[MSGFMT_TYPE_SIZE + fname_sz] = 0;
  memcpy(buf + MSGFMT_TYPE_SIZE + fname_sz + 1, fdata, data_sz);

  memset(buf + base_sz, 0, buf_sz - base_sz);

  return base_sz + extra_data_sz;
}

void msgfmt_parse_data(char *buf, int buf_sz, char **fname, int fname_sz,
                       char **fdata, int data_sz) {
  uint32_t base_sz = MSGFMT_TYPE_SIZE + fname_sz + 1 + data_sz;
  if (base_sz > buf_sz) {
    ABORT("Buffer overflow");
  }

  if (buf[0] != MSGFMT_DATA) {
    ABORT("Invalid data fmt");
  }

  (*fname) = &buf[MSGFMT_TYPE_SIZE];
  (*fdata) = &buf[MSGFMT_TYPE_SIZE + fname_sz + 1];

  return;
}

uint32_t msgfmt_begin_reneg(char *buf, int buf_sz, int my_rank) {
  assert((uint32_t)buf_sz >= MSGFMT_TYPE_SIZE + sizeof(int));

  buf[0] = MSGFMT_RENEG_BEGIN;
  memcpy(&buf[1], static_cast<void *>(&my_rank), sizeof(int));

  return MSGFMT_TYPE_SIZE + sizeof(int);
}

unsigned char msgfmt_get_msgtype(char *buf) { return buf[0]; }

uint32_t msgfmt_pivot_bytes(int num_pivots) {
  uint32_t data_bytes = num_pivots * sizeof(float);
  uint32_t header = MSGFMT_TYPE_SIZE + sizeof(int);

  fprintf(stderr, "MSGFMT hdr: %u, data: %u\n", header, data_bytes);

  return header + data_bytes;
}

void msgfmt_encode_pivots(char *buf, int buf_sz, std::vector<float> &pivots) {
  int num_pivots = pivots.size();
  int bytes_reqd = msgfmt_pivot_bytes(num_pivots);
  assert(buf_sz >= bytes_reqd);

  fprintf(stderr, "Bytes reqd: %d, bufsz: %d\n", bytes_reqd, buf_sz);

  buf[0] = MSGFMT_RENEG_PIVOTS;
  std::copy(reinterpret_cast<char *>(&num_pivots),
            reinterpret_cast<char *>(&num_pivots + sizeof(int)),
            &buf[1]);
  std::copy(pivots.begin(), pivots.end(), &buf[1 + sizeof(int)]);

  memset(&buf[bytes_reqd], 0, buf_sz - bytes_reqd);
  return;
}
