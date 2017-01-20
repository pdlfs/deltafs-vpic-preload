/*
 * Copyright (c) 2017 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <stdio.h>

#include "shuffle.h"

#ifdef DELTAFS_SHUFFLE_DEBUG
hg_return_t ping_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    ping_t in, out;
    struct hg_info *info;
    shuffle_ctx_t *ctx;

    hret = HG_Get_input(h, &in);
    assert(hret == HG_SUCCESS);

    info = HG_Get_info(h);
    assert(info != NULL);

    /* Get ssg data */
    ctx = (shuffle_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    assert(ctx != NULL && ctx->s != SSG_NULL);
    out.rank = ssg_get_rank(ctx->s);
    assert(out.rank != SSG_RANK_UNKNOWN && out.rank != SSG_EXTERNAL_RANK);

    fprintf(stderr, "%d: got ping from rank %d\n", out.rank, in.rank);

    HG_Respond(h, NULL, NULL, &out);

    hret = HG_Free_input(h, &in);
    assert(hret == HG_SUCCESS);
    hret = HG_Destroy(h);
    assert(hret == HG_SUCCESS);
    return HG_SUCCESS;
}

/* For testing: ping RPCs go around all ranks in a ring to test Mercury/SSG */
void ping_test(int rank)
{
    hg_id_t ping_id;
    hg_handle_t ping_handle = HG_HANDLE_NULL;
    ping_t ping_in;
    hg_addr_t peer_addr;
    int peer_rank, ret;
    hg_return_t hret;
    hg_request_t *hgreq;
    unsigned int req_complete_flag = 0;

    /* Register ping RPC */
    ping_id = MERCURY_REGISTER(sctx.hgcl, "ping",
                               ping_t, ping_t,
                               &ping_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, ping_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (ping)");

    /* Sanity check count; if we're on our own, don't bother */
    if (ssg_get_count(sctx.s) == 1)
        return;

    peer_rank = (rank+1) % ssg_get_count(sctx.s);
    peer_addr = ssg_get_addr(sctx.s, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("ssg_get_addr");

    fprintf(stderr, "%d: pinging %d\n", rank, peer_rank);
    hret = HG_Create(sctx.hgctx, peer_addr, ping_id, &ping_handle);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Create");

    hgreq = hg_request_create(sctx.hgreqcl);
    if (hgreq == NULL)
        msg_abort("hg_request_create");

    ping_in.rank = rank;
    hret = HG_Forward(ping_handle, &hg_request_complete_cb, hgreq, &ping_in);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Forward");

    ret = hg_request_wait(hgreq, HG_MAX_IDLE_TIME, &req_complete_flag);
    if (ret == HG_UTIL_FAIL)
        msg_abort("ping failed");
    if (req_complete_flag == 0)
        msg_abort("ping timed out");

    HG_Destroy(ping_handle);
    hg_request_destroy(hgreq);
}
#endif /* DELTAFS_SHUFFLE_DEBUG */
