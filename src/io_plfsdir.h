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
//  PLFSDIR_Memtable_size              Size of in-memory write buf
//  PLFSDIR_Index_buf_size             Write buf size for the physical index log
//  PLFSDIR_Data_buf_size              Write buf size for the physical data log
//  PLFSDIR_Lg_parts                   Logarithmic number of partitions
// ----------------------------------|----------------------------------
//

/*
 * Default size of the in-memory write buffer.
 *
 * Specified as a string.
 */
#define DEFAULT_MEMTABLE_SIZE "32m"

/*
 * Default size of the index write buffer.
 *
 * Specified as a string.
 */
#define DEFAULT_INDEX_BUF "2m"

/*
 * Default size of the data write buffer.
 *
 * Specified as a string.
 */
#define DEFAULT_DATA_BUF "2m"

/*
 * Default logarithmic number of partitions.
 *
 * Specified as a string.
 */
#define DEFAULT_LG_PARTS "0"

// END
