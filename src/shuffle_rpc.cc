/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <stdio.h>

#include "shuffle_rpc.h"

hg_return_t ping_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    ping_t in, out;
    struct hg_info *info;
    shuffle_rpc_ctx_t *ctx;

    hret = HG_Get_input(h, &in);
    assert(hret == HG_SUCCESS);

    info = HG_Get_info(h);
    assert(info != NULL);

    /* Get ssg data */
    ctx = (shuffle_rpc_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    assert(ctx != NULL && ctx->s != SSG_NULL);
    out.rank = ssg_get_rank(ctx->s);
    assert(out.rank != SSG_RANK_UNKNOWN && out.rank != SSG_EXTERNAL_RANK);

    fprintf(stdout, "%d: got ping from rank %d\n", out.rank, in.rank);

    HG_Respond(h, NULL, NULL, &out);

    hret = HG_Free_input(h, &in);
    assert(hret == HG_SUCCESS);
    hret = HG_Destroy(h);
    assert(hret == HG_SUCCESS);
    return HG_SUCCESS;
}

static hg_return_t shutdown_post_respond(const struct hg_cb_info *cb_info)
{
    hg_handle_t h;
    struct hg_info *info;
    shuffle_rpc_ctx_t *ctx;

    h = cb_info->info.respond.handle;
    info = HG_Get_info(h);
    assert(info != NULL);

    ctx = (shuffle_rpc_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    fprintf(stdout, "%d: post-respond, setting shutdown flag\n",
            ssg_get_rank(ctx->s));

    ctx->shutdown_flag = 1;
    HG_Destroy(h);
    return HG_SUCCESS;
}

static hg_return_t shutdown_post_forward(const struct hg_cb_info *cb_info)
{
    hg_handle_t fwd_handle, resp_handle;
    shuffle_rpc_ctx_t *ctx;
    int rank;
    hg_return_t hret;
    struct hg_info *info;

    /* RPC has completed, respond to previous rank */
    fwd_handle = cb_info->info.forward.handle;
    resp_handle = (hg_handle_t) cb_info->arg;
    info = HG_Get_info(fwd_handle);
    ctx = (shuffle_rpc_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    assert(ctx != NULL && ctx->s != SSG_NULL);

    rank = ssg_get_rank(ctx->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);
    if (rank > 0) {
        fprintf(stdout, "%d: sending shutdown response\n", rank);
        hret = HG_Respond(resp_handle, &shutdown_post_respond, NULL, NULL);
        assert(hret == HG_SUCCESS);
        //return HG_SUCCESS;
    } else {
        ctx->shutdown_flag = 1;
        fprintf(stdout, "%d: no recipient, setting shutdown flag\n", rank);
    }

    HG_Destroy(fwd_handle);
    return HG_SUCCESS;
}

/*
 * Shutdown RPC: does a ring communication for simplicity
 * TODO: should be doing multicast instead
 */
hg_return_t shutdown_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    struct hg_info *info;
    int rank;
    shuffle_rpc_ctx_t *ctx;

    info = HG_Get_info(h);
    assert(info != NULL);

    /* Get ssg data */
    ctx = (shuffle_rpc_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    assert(ctx != NULL && ctx->s != SSG_NULL);
    rank = ssg_get_rank(ctx->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);

    fprintf(stdout, "%d: received shutdown reqyest\n", rank);

    rank++;
    if (rank == ssg_get_count(ctx->s)) {
        /* End of the line, respond and shut down */
        fprintf(stdout, "%d: responding and setting shutdown flag\n", rank-1);
        hret = HG_Respond(h, &shutdown_post_respond, NULL, NULL);
        assert(hret == HG_SUCCESS);
        ctx->shutdown_flag = 1;
    } else {
        /* Forward shutdown to neighbor */
        hg_handle_t next_handle;
        na_addr_t next_addr;

        next_addr = ssg_get_addr(ctx->s, rank);
        assert(next_addr != NULL);

        /* XXX: Not so sure about this na_addr_t ~> hg_addr_t conversion here */
        hret = HG_Create(info->context, (hg_addr_t) next_addr, info->id,
                         &next_handle);
        assert(hret == HG_SUCCESS);

        fprintf(stdout, "%d: forwarding shutdown to neighbor\n", rank-1);
        hret = HG_Forward(next_handle, &shutdown_post_forward, h, NULL);
        assert(hret == HG_SUCCESS);

        hret = HG_Destroy(next_handle);
        assert(hret == HG_SUCCESS);
    }

    return HG_SUCCESS;
}
