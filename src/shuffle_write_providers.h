#pragma once

#include "preload_internal.h"
#include "preload_mon.h"
#include "preload_range.h"
#include "preload_shuffle.h"

#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffle.h"

#include "common.h"
#include "msgfmt.h"

#include "rtp/rtp.h"

int shuffle_write_mock(shuffle_ctx_t* ctx, const char* fname,
                  unsigned char fname_len, char* data, unsigned char data_len,
                  int epoch);

int shuffle_write_nohash(shuffle_ctx_t* ctx, const char* fname,
                  unsigned char fname_len, char* data, unsigned char data_len,
                  int epoch);

int shuffle_write_treeneg(shuffle_ctx_t* ctx, const char* fname,
                       unsigned char fname_len, char* data,
                       unsigned char data_len, int epoch);

int shuffle_write_range(shuffle_ctx_t* ctx, const char* fname,
                       unsigned char fname_len, char* data,
                       unsigned char data_len, int epoch);
