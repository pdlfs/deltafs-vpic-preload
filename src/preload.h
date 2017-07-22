/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * preload.h  redirect related vpic i/o calls to deltafs plfsdir.
 *
 * A list of all environmental variables used by us:
 *
 *  PRELOAD_Deltafs_root
 *    Deltafs root (details below)
 *  PRELOAD_Plfsdir
 *    Path to the plfsdir (XXX: allows multi)
 *  PRELOAD_Bypass_shuffle
 *    Do not shuffle writes at all
 *  PRELOAD_Bypass_placement
 *    Shuffle without ch-placement
 *  PRELOAD_Bypass_deltafs_plfsdir
 *    Call deltafs without the plfsdir feature
 *  PRELOAD_Bypass_deltafs_namespace
 *    Use deltafs light-wright plfsdir api
 *  PRELOAD_Bypass_deltafs
 *    Write to local file system
 *  PRELOAD_Bypass_write
 *    Make every write and mkdir an noop
 *  PRELOAD_Skip_mon
 *    Skip perf monitoring
 *  PRELOAD_Skip_mon_dist
 *    Skip copying mon files out
 *  PRELOAD_Enable_verbose_mon
 *    Print mon info at the end of each epoch
 *  PRELOAD_Enable_verbose_error
 *    Print error info when write op fails
 *  PRELOAD_No_sys_probing
 *    Do not scan operating system or device settings
 *  PRELOAD_No_paranoid_checks
 *    Disable misc checks on vpic writes
 *  PRELOAD_No_paranoid_barrier
 *    Disable MPI barriers at the beginning of an epoch
 *      and right before an epoch flush
 *  PRELOAD_No_paranoid_post_barrier
 *    Disable MPI barriers at the beginning of an epoch
 *      and right after an epoch flush
 *  PRELOAD_No_paranoid_pre_barrier
 *    Disable MPI barriers at the end of an epoch
 *      and right before a soft epoch flush
 *  PRELOAD_No_epoch_pre_flushing
 *    No soft epoch flush at the end of an epoch
 *  PRELOAD_Local_root
 *    Local file system root
 *  PRELOAD_Testing
 *    Used by developers to debug code
 *  PRELOAD_Inject_fake_data
 *    Replace particle data with artificial data
 *  PLFSDIR_Key_size
 *    Hash key size for encoding file names
 *  PLFSDIR_Filter_bits_per_key
 *    Number of filter bits allocated for each key
 *  PLFSDIR_Compaction_buf_size
 *    Size of the compaction buffer per memtable partition
 *  PLFSDIR_Memtable_size
 *    Total size of the memtable
 *  PLFSDIR_Data_buf_size
 *    Max write buf size for the shared data log
 *  PLFSDIR_Data_min_write_size
 *    Min write size for the shared data log
 *  PLFSDIR_Index_buf_size
 *    Max write buf size for each index log
 *  PLFSDIR_Index_min_write_size
 *    Min write size for each index log
 *  PLFSDIR_Lg_parts
 *    Logarithmic number of memtable partitions
 *  PLFSDIR_Skip_checksums
 *    Skip generating checksums
 */

#pragma once

/*
 * If "deltafs_root" is not specified, we set it to the following path.
 *
 * App writes with a path that starts with "deltafs_root" will
 * be redirected to deltafs.
 *
 * "deltafs_root" maybe a relative path.
 */
#define DEFAULT_DELTAFS_ROOT "/deltafs"

/*
 * If "local_root" is not specified, we set it to the following path.
 *
 * When "bypass_deltafs" or "bypass_deltafs_namespace" is set, we
 * will redirect deltafs writes to the local file system.
 *
 * In such cases, all local file system writes will
 * be rooted under the following path.
 */
#define DEFAULT_LOCAL_ROOT "/tmp/vpic-deltafs-test"

/*
 * Preload mode bits
 */
#define BYPASS_SHUFFLE (1 << 0)
#define BYPASS_PLACEMENT (1 << 1)
#define BYPASS_DELTAFS_PLFSDIR (1 << 2)
#define BYPASS_DELTAFS_NAMESPACE (1 << 3)
#define BYPASS_DELTAFS (1 << 4)
#define BYPASS_WRITE (1 << 5)

/*
 * Preload mode query interface
 */
#define IS_BYPASS_SHUFFLE(m) (((m)&BYPASS_SHUFFLE) == BYPASS_SHUFFLE)
#define IS_BYPASS_PLACEMENT(m) (((m)&BYPASS_PLACEMENT) == BYPASS_PLACEMENT)
#define IS_BYPASS_DELTAFS_PLFSDIR(m) \
  (((m)&BYPASS_DELTAFS_PLFSDIR) == BYPASS_DELTAFS_PLFSDIR)
#define IS_BYPASS_DELTAFS_NAMESPACE(m) \
  (((m)&BYPASS_DELTAFS_NAMESPACE) == BYPASS_DELTAFS_NAMESPACE)
#define IS_BYPASS_DELTAFS(m) (((m)&BYPASS_DELTAFS) == BYPASS_DELTAFS)
#define IS_BYPASS_WRITE(m) (((m)&BYPASS_WRITE) == BYPASS_WRITE)

/*
 * preload_write(fn, data, n):
 *   - ship data to deltafs
 */
extern int preload_write(const char* fn, char* data, size_t n, int epoch);

/*
 * Default hash key size for encoding file names.
 * Specified as a string.
 */
#define DEFAULT_KEY_SIZE "8"

/*
 * Default bloom filter bits per key.
 * Specified as a string.
 */
#define DEFAULT_BITS_PER_KEY "14"

/*
 * Default size of the in-memory write buffer.
 * Specified as a string.
 */
#define DEFAULT_MEMTABLE_SIZE "48MiB"

/*
 * Default size of the compaction buffer for each memtable partition.
 * Specified as a string.
 */
#define DEFAULT_COMPACTION_BUF "4MiB"

/*
 * Default size of the index write buffer.
 * Specified as a string.
 */
#define DEFAULT_INDEX_BUF "2MiB"

/*
 * Default min write size for the index log.
 * Specified as a string.
 */
#define DEFAULT_INDEX_MIN_WRITE_SIZE "2MiB"

/*
 * Default size of the data write buffer.
 * Specified as a string.
 */
#define DEFAULT_DATA_BUF "8MiB"

/*
 * Default min write size for the data log.
 * Specified as a string.
 */
#define DEFAULT_DATA_MIN_WRITE_SIZE "6MiB"

/*
 * Default logarithmic number of partitions.
 * Specified as a string.
 */
#define DEFAULT_LG_PARTS "2"
