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
//  PRELOAD_Deltafs_root               Deltafs root
//  PRELOAD_Plfsdir                    Path to the plfsdir (XXX: allows multi)
//  PRELOAD_Bypass_shuffle             Do not shuffle writes at all
//  PRELOAD_Bypass_placement           Shuffle without ch-placement
//  PRELOAD_Bypass_deltafs_plfsdir     Call deltafs without the plfsdir feature
//  PRELOAD_Bypass_deltafs             Write to local file system
//  PRELOAD_Skip_monitoring            Skip perf monitoring
//  PRELOAD_Local_root                 Local file system root
//  PRELOAD_Testing                    Used by developers to debug code
// ----------------------------------|----------------------------------
//

/*
 * Preload mode bits
 */
#define BYPASS_SHUFFLE (1 << 0)
#define BYPASS_PLACEMENT (1 << 1)
#define BYPASS_DELTAFS_PLFSDIR (1 << 2)
#define BYPASS_DELTAFS (1 << 3)
#define BYPASS_MASK 0xFF

/*
 * Preload mode query interface
 */
#define IS_BYPASS_SHUFFLE(m) (((m) & BYPASS_SHUFFLE) == BYPASS_SHUFFLE)
#define IS_BYPASS_PLACEMENT(m) (((m) & BYPASS_PLACEMENT) == BYPASS_PLACEMENT)
#define IS_BYPASS_DELTAFS_PLFSDIR(m) (((m) & BYPASS_DELTAFS_PLFSDIR) == BYPASS_DELTAFS_PLFSDIR)
#define IS_BYPASS_DELTAFS(m) (((m) & BYPASS_DELTAFS) == BYPASS_DELTAFS)

/*
 * If "deltafs_root" is not specified, we set it to the following path.
 *
 * App writes with a path that starts with "deltafs_root" will
 * be redirected to deltafs.
 *
 * "deltafs_root" maybe a relative path.
 */
#define DEFAULT_DELTAFS_ROOT "/tmp/pdlfs"

/*
 * If "local_root" is not specified, we set it to the following path.
 *
 * When "bypass_deltafs" is set, we will have to redirect
 * deltafs writes to the local file system.
 *
 * In such cases, all local file system writes will
 * be rooted under the following path.
 */
#define DEFAULT_LOCAL_ROOT "/tmp/pdlfs-test"

// END
