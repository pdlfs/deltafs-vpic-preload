/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

/* !!! A list of all environmental variables used by us !!! */

//
//  Env                                Description
// ----------------------------------|----------------------------------
//  PLFSDIR_Compaction_buf_size        Size of the compaction buffer per memtable partition
//  PLFSDIR_Memtable_size              Total size of the memtable
//  PLFSDIR_Data_buf_size              Write buf size for the shared data log
//  PLFSDIR_Data_min_write_size        Min write size for the shared data log
//  PLFSDIR_Index_buf_size             Write buf size for each index log
//  PLFSDIR_Index_min_write_size       Min write size for each index log
//  PLFSDIR_Lg_parts                   Logarithmic number of memtable partitions
// ----------------------------------|----------------------------------
//

/*
 * Default size of the in-memory write buffer.
 *
 * Specified as a string.
 */
#define DEFAULT_MEMTABLE_SIZE "48m"

/*
 * Default size of the compaction buffer for each memtable partition.
 *
 * Specified as a string.
 */
#define DEFAULT_COMPACTION_BUF "4m"

/*
 * Default size of the index write buffer.
 *
 * Specified as a string.
 */
#define DEFAULT_INDEX_BUF "2m"

/*
 * Default min write size for the index log.
 *
 * Specified as a string.
 */
#define DEFAULT_INDEX_MIN_WRITE_SIZE "2m"

/*
 * Default size of the data write buffer.
 *
 * Specified as a string.
 */
#define DEFAULT_DATA_BUF "8m"

/*
 * Default min write size for the data log.
 *
 * Specified as a string.
 */
#define DEFAULT_DATA_MIN_WRITE_SIZE "6m"

/*
 * Default logarithmic number of partitions.
 *
 * Specified as a string.
 */
#define DEFAULT_LG_PARTS "2"

// END
