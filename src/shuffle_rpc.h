/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <mercury.h>
#include <mercury_macros.h>
#include <ssg.h>

typedef struct shuffle_rpc_context {
    ssg_t s;
    int shutdown_flag;  /* Used for testing */
    int lookup_flag;    /* Used in dblgrp test */
} shuffle_rpc_ctx_t;

MERCURY_GEN_PROC(ping_t, ((int32_t)(rank)))

hg_return_t ping_rpc_handler(hg_handle_t h);
hg_return_t shutdown_rpc_handler(hg_handle_t h);
