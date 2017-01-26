/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <pdlfs-common/xxhash.h>

#include "shuffle.h"

struct write_bulk_args {
    hg_handle_t handle;
    size_t len;
    ssize_t ret;
    hg_const_string_t fname;
    int rank_in;
};

/* Mercury callback for bulk transfer requests */
static hg_return_t write_bulk_transfer_cb(const struct hg_cb_info *info)
{
    struct write_bulk_args *bulk_args = (struct write_bulk_args *)info->arg;
    hg_bulk_t data_handle = info->info.bulk.local_handle;
    hg_return_t hret = HG_SUCCESS;
    char *data;
    int rank;
    write_out_t out;
    write_in_t in;

    /* Grab bulk data */
    hret = HG_Bulk_access(data_handle, 0, bulk_args->len, HG_BULK_READWRITE, 1,
                          (void **) &data, NULL, NULL);
    assert(hret == HG_SUCCESS);

    /* Get my rank */
    rank = ssg_get_rank(sctx.ssg);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);

    SHUFFLE_LOG("%d: Writing %d bytes to %s (shuffle: %d -> %d)\n", rank,
            (int) bulk_args->len, bulk_args->fname, bulk_args->rank_in, rank);

    /* Perform the write and fill output structure */
    out.ret = preload_write(bulk_args->fname, data, (int) bulk_args->len);

    /* Write out to the log if we are running a test */
    if (pctx.testin) {
        char buf[1024] = { 0 };
        snprintf(buf, sizeof(buf), "source %5d target %5d size %d\n",
                 bulk_args->rank_in, rank, (int) bulk_args->len);
        int fd = open(pctx.log, O_WRONLY | O_APPEND);
        if (fd <= 0)
            msg_abort("log open failed");
        assert(write(fd, buf, strlen(buf)) == strlen(buf));
        close(fd);
    }

    /* Free block handle */
    hret = HG_Bulk_free(data_handle);
    assert(hret == HG_SUCCESS);

    /* Send response back */
    hret = HG_Respond(bulk_args->handle, NULL, NULL, &out);
    assert(hret == HG_SUCCESS);

    /* Get input struct just to free it */
    hret = HG_Get_input(bulk_args->handle, &in);
    assert(hret == HG_SUCCESS);
    HG_Free_input(bulk_args->handle, &in);

    /* Clean up */
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return hret;
}

/* Mercury RPC callback for redirected writes */
hg_return_t write_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    write_in_t in;
    struct hg_info *info = NULL;
    hg_bulk_t in_handle = HG_BULK_NULL;
    hg_bulk_t data_handle = HG_BULK_NULL;
    int rank_in;
    struct write_bulk_args *bulk_args = NULL;

    /* Get input struct */
    hret = HG_Get_input(h, &in);
    assert(hret == HG_SUCCESS);

    if (in.isbulk) {
        bulk_args = (struct write_bulk_args *) malloc(
                        sizeof(struct write_bulk_args));
        assert(bulk_args);

        /* Keep handle to pass to callback */
        bulk_args->handle = h;

        /* Get info from handle */
        info = HG_Get_info(h);
        assert(info != NULL);

        in_handle = in.data_handle;

        /* Create a new block handle to read the data */
        bulk_args->len = (size_t) ((unsigned)HG_Bulk_get_size(in_handle));
        bulk_args->fname = in.fname;
        bulk_args->rank_in = in.rank_in;

        SHUFFLE_LOG("Creating new bulk handle to read data (%s, len %zu)\n",
                bulk_args->fname, bulk_args->len);
        /* Create a new bulk handle to read the data */
        hret = HG_Bulk_create(info->hg_class, 1, NULL,
                              (hg_size_t *) &bulk_args->len,
                              HG_BULK_READWRITE, &data_handle);
        assert(hret == HG_SUCCESS);

        /* Pull bulk data */
        hret = HG_Bulk_transfer(info->context, write_bulk_transfer_cb,
                                bulk_args, HG_BULK_PULL, info->addr, in_handle,
                                0, data_handle, 0, bulk_args->len,
                                HG_OP_ID_IGNORE);
        assert(hret == HG_SUCCESS);

        /* Can't free input here because of filename. Do it in callback. */
    } else {
        int rank;
        write_out_t out;

        /* Get my rank */
        rank = ssg_get_rank(sctx.ssg);
        assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);

        SHUFFLE_LOG("Writing %d bytes to %s (shuffle: %d -> %d)\n",
                (int) in.data_len, in.fname, in.rank_in, rank);

        /* Perform the write and fill output structure */
        out.ret = preload_write(in.fname, in.data, in.data_len);

        /* Write out to the log if we are running a test */
        if (pctx.testin) {
            char buf[1024] = { 0 };
            snprintf(buf, sizeof(buf), "source %5d target %5d size %lu\n",
                     (int) in.rank_in, rank, in.data_len);
            int fd = open(pctx.log, O_WRONLY | O_APPEND);
            if (fd <= 0)
                msg_abort("log open failed");
            assert(write(fd, buf, strlen(buf)) == strlen(buf));
            close(fd);
        }

        /* Send response back */
        hret = HG_Respond(h, NULL, NULL, &out);
        assert(hret == HG_SUCCESS);

        /* Free input struct */
        HG_Free_input(h, &in);
    }

    return hret;
}

/* Redirects write to the right node through Mercury */
int shuffle_write(const char *fn, char *data, int len)
{
    write_in_t write_in;
    write_out_t write_out;
    hg_return_t hret;
    int rank, peer_rank;
    hg_addr_t peer_addr;
    hg_handle_t write_handle = HG_HANDLE_NULL;
    hg_bulk_t data_handle;
    int write_ret;

    /* Decide RPC receiver. If we're alone we execute it locally. */
    if (ssg_get_count(sctx.ssg) == 1)
        return preload_write(fn, data, len);

    rank = ssg_get_rank(sctx.ssg);
    if (rank == SSG_RANK_UNKNOWN || rank == SSG_EXTERNAL_RANK)
        msg_abort("ssg_get_rank: bad rank");

    if (IS_BYPASS_PLACEMENT(pctx.mode)) {
        /* Send to next-door neighbor instead of using ch-placement */
        peer_rank = (rank + 1) % ssg_get_count(sctx.ssg);
    } else {
        uint64_t oid = pdlfs::xxhash64(fn, strlen(fn), 0);
        unsigned long server_idx;

        /* Use ch-placement to decide receiver */
        ch_placement_find_closest(sctx.chp, oid, 1, &server_idx);
        SHUFFLE_LOG("File %s -> xxhash %16lx -> Server %lu\n",
                fn, oid, server_idx);
        peer_rank = (int) server_idx;
    }

    /* Are we trying to message ourselves? Write locally */
    if (peer_rank == rank) {
        /* Write out to the log if we are running a test */
        if (pctx.testin) {
            char buf[1024] = { 0 };
            snprintf(buf, sizeof(buf), "source %5d target %5d size %d\n",
                     rank, rank, len);
            int fd = open(pctx.log, O_WRONLY | O_APPEND);
            if (fd <= 0)
                msg_abort("log open failed");
            assert(write(fd, buf, strlen(buf)) == strlen(buf));
            close(fd);
        }

        return preload_write(fn, data, len);
    }

    peer_addr = ssg_get_addr(sctx.ssg, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("ssg_get_addr");

    /* Put together write RPC */
    SHUFFLE_LOG("Redirecting write of %s: %d -> %d\n", fn, rank, peer_rank);
    hret = HG_Create(sctx.hg_ctx, peer_addr, sctx.hg_id, &write_handle);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Create");

    /*
     * Depending on the amount of data sent, we may use bulk transfer, or
     * point-to-point messaging.
     */
    if (len > SHUFFLE_SMALL_WRITE) {
        /* Use bulk transfer */
        hret = HG_Bulk_create(sctx.hg_clz, 1,
                              (void **) &data, (hg_size_t *) &len,
                              HG_BULK_READ_ONLY, &data_handle);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Bulk_create");

        write_in.fname = fn;
        write_in.data_handle = data_handle;
        write_in.data = NULL;
        write_in.data_len = 0;
        write_in.rank_in = rank;
        write_in.isbulk = 1;
    } else {
        write_in.fname = fn;
        write_in.data_handle = HG_BULK_NULL;

        /* Regular point-to-point communication */
        write_in.data = (hg_string_t) malloc(len + 1);
        memcpy(write_in.data, data, len);

        /* Mercury requires string to be null terminated */
        write_in.data[len] = '\0';

        write_in.data_len = len;

        write_in.rank_in = rank;
        write_in.isbulk = 0;
    }

    SHUFFLE_LOG("%d: Forwarding write RPC: %d -> %d\n", rank, rank, peer_rank);
    /* Send off write RPC */
    hret = HG_Forward(write_handle, NULL, NULL, &write_in);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Forward");

    SHUFFLE_LOG("%d: Response received (%d -> %d)\n", rank, rank, peer_rank);

    hret = HG_Get_output(write_handle, &write_out);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Get_output");

    write_ret = (int) write_out.ret;

    hret = HG_Free_output(write_handle, &write_out);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Free_output");

    hret = HG_Destroy(write_handle);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Destroy");

    SHUFFLE_LOG("%d: Resources destroyed (%d -> %d)\n", rank, rank, peer_rank);

    /* Free bulk resources if used */
    if (len > SHUFFLE_SMALL_WRITE) {
        hret = HG_Bulk_free(data_handle);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Bulk_free");
    }

    return write_ret;
}
