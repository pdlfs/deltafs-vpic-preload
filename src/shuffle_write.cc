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

#include "shuffle_internal.h"
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

