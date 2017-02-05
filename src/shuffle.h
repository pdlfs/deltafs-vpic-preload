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
//  SHUFFLE_Mercury_proto              Mercury rpc proto
//  SHUFFLE_Subnet                     IP prefix of the subnet we prefer to use
//  SHUFFLE_Min_port                   The min port number we can use
//  SHUFFLE_Max_port                   The max port number we can use
//  SHUFFLE_Timeout                    RPC timeout
// ----------------------------------|----------------------------------
//

/*
 * The default min.
 */
#define DEFAULT_MIN_PORT 50000

/*
 * The default max.
 */
#define DEFAULT_MAX_PORT 60000

/*
 * Default rpc timeout (in secs).
 */
#define DEFAULT_TIMEOUT 30

/*
 * The default subnet.
 *
 * Guaranteed to be wrong in production.
 */
#define DEFAULT_SUBNET "127.0.0.1"

/*
 * If "mercury_proto" is not specified, we set it to the follows.
 *
 * This assumes the mercury linked by us has been
 * built with this specific transport.
 */
#define DEFAULT_PROTO "bmi+tcp"

// END
