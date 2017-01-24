/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <stdio.h>

#include "shuffle.h"

static hg_return_t shutdown_post_respond(const struct hg_cb_info *cb_info)
{
    hg_handle_t h;
    struct hg_info *info;
    shuffle_ctx_t *ctx;

    h = cb_info->info.respond.handle;
    info = HG_Get_info(h);
    assert(info != NULL);

    ctx = (shuffle_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    SHUFFLE_DEBUG("%d: post-respond, setting shutdown flag\n",
                  ssg_get_rank(ctx->s));

    ctx->shutdown_flag = 1;
    HG_Destroy(h);
    return HG_SUCCESS;
}

static hg_return_t shutdown_post_forward(const struct hg_cb_info *cb_info)
{
    hg_handle_t fwd_handle, resp_handle;
    shuffle_ctx_t *ctx;
    int rank;
    hg_return_t hret;
    struct hg_info *info;

    /* RPC has completed, respond to previous rank */
    fwd_handle = cb_info->info.forward.handle;
    resp_handle = (hg_handle_t) cb_info->arg;
    info = HG_Get_info(fwd_handle);
    ctx = (shuffle_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    assert(ctx != NULL && ctx->s != SSG_NULL);

    rank = ssg_get_rank(ctx->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);
    if (rank > 0) {
        SHUFFLE_DEBUG("%d: sending shutdown response\n", rank);
        hret = HG_Respond(resp_handle, &shutdown_post_respond, NULL, NULL);
        assert(hret == HG_SUCCESS);
    } else {
        ctx->shutdown_flag = 1;
        SHUFFLE_DEBUG("%d: no recipient, setting shutdown flag\n", rank);
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
    shuffle_ctx_t *ctx;

    info = HG_Get_info(h);
    assert(info != NULL);

    /* Get ssg data */
    ctx = (shuffle_ctx_t *) HG_Registered_data(info->hg_class, info->id);
    assert(ctx != NULL && ctx->s != SSG_NULL);
    rank = ssg_get_rank(ctx->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);

    SHUFFLE_DEBUG("%d: received shutdown request\n", rank);

    rank++;
    if (rank == ssg_get_count(ctx->s)) {
        /* End of the line, respond and shut down */
        SHUFFLE_DEBUG("%d: responding and setting shutdown flag\n", rank-1);
        hret = HG_Respond(h, &shutdown_post_respond, NULL, NULL);
        assert(hret == HG_SUCCESS);
        ctx->shutdown_flag = 1;
    } else {
        /* Forward shutdown to neighbor */
        hg_handle_t next_handle;
        hg_addr_t next_addr;

        next_addr = ssg_get_addr(ctx->s, rank);
        assert(next_addr != NULL);

        hret = HG_Create(info->context, next_addr, info->id, &next_handle);
        assert(hret == HG_SUCCESS);

        SHUFFLE_DEBUG("%d: forwarding shutdown to neighbor\n", rank-1);
        hret = HG_Forward(next_handle, &shutdown_post_forward, h, NULL);
        assert(hret == HG_SUCCESS);

        hret = HG_Destroy(next_handle);
        assert(hret == HG_SUCCESS);
    }

    SHUFFLE_DEBUG("%d: forwarded shutdown request\n", rank-1);

    return HG_SUCCESS;
}

/* Shutdown process used at MPI_Finalize() time to tear down shuffle layer */
void shuffle_shutdown(int rank)
{
    hg_return_t hret;

    SHUFFLE_DEBUG("%d: entering shutdown process\n", rank);

    /*
     * Rank 0: initialize the shutdown process
     * Others: enter progress
     */
    if (rank != 0) {
        unsigned int num_trigger;

        SHUFFLE_DEBUG("%d: waiting for shutdown message\n", rank);

        do {
            SHUFFLE_DEBUG("%d: entering HG_Trigger loop\n", rank);
            do {
                num_trigger = 0;
                hret = HG_Trigger(sctx.hgctx, 0, 1, &num_trigger);
            } while (hret == HG_SUCCESS && num_trigger == 1);

            SHUFFLE_DEBUG("%d: calling HG_Progress\n", rank);
            hret = HG_Progress(sctx.hgctx,
                               sctx.shutdown_flag ? 100 : HG_MAX_IDLE_TIME);
        } while ((hret == HG_SUCCESS || hret == HG_TIMEOUT) &&
                 !sctx.shutdown_flag);

        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            msg_abort("HG_Progress");

        SHUFFLE_DEBUG("%d: shutting down\n", rank);

        /* Trigger/progress remaining */
        do {
            hret = HG_Progress(sctx.hgctx, 0);
        } while (hret == HG_SUCCESS);

        do {
            num_trigger = 0;
            hret = HG_Trigger(sctx.hgctx, 0, 1, &num_trigger);
        } while (hret == HG_SUCCESS && num_trigger == 1);
    } else {
        hg_request_t *hgreq;
        unsigned int req_complete_flag = 0;
        hg_addr_t peer_addr;
        int peer_rank, ret;

        SHUFFLE_DEBUG("%d: initiating shutdown\n", rank);
        hg_handle_t shutdown_handle = HG_HANDLE_NULL;

        peer_rank = (rank+1) % ssg_get_count(sctx.s);
        peer_addr = ssg_get_addr(sctx.s, peer_rank);
        if (peer_addr == HG_ADDR_NULL)
            msg_abort("ssg_get_addr");

        hret = HG_Create(sctx.hgctx, peer_addr, sctx.shutdown_id, &shutdown_handle);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Create");

        hgreq = hg_request_create(sctx.hgreqcl);
        if (hgreq == NULL)
            msg_abort("hg_request_create");

        hret = HG_Forward(shutdown_handle, &hg_request_complete_cb, hgreq, NULL);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Forward");

        req_complete_flag = 0;
        SHUFFLE_DEBUG("%d: Waiting for hg_request_wait\n", rank);
        ret = hg_request_wait(hgreq, HG_MAX_IDLE_TIME, &req_complete_flag);
        if (ret != HG_UTIL_SUCCESS)
            msg_abort("hg_request_wait");
        if (req_complete_flag == 0)
            msg_abort("hg_request_wait timed out");

        HG_Destroy(shutdown_handle);
        SHUFFLE_DEBUG("%d: destroying request\n", rank);
        hg_request_destroy(hgreq);
    }

    SHUFFLE_DEBUG("%d: shutdown protocol completed\n", rank);
}
