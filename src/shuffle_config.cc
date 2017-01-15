/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <unistd.h>

#include <arpa/inet.h>
#include <ifaddrs.h>

#include "shuffle.h"
#include "shuffle_rpc.h"

/*
 * msg_abort: abort with a message
 */
void msg_abort(const char *msg)
{
    int err = errno;

    fprintf(stderr, "ABORT: %s", msg);
    if (errno)
        fprintf(stderr, " (%s)\n", strerror(errno));
    else
        fprintf(stderr, "\n");

    abort();
}

/*
 * Generate a mercury address
 *
 * We have to assign a Mercury address to ourselves.
 * Get the first available IP (any interface that's not localhost)
 * and use the process ID to construct the port (but limit to [5000,60000])
 */
void genHgAddr(void)
{
    int family, found = 0, port;
    struct ifaddrs *ifaddr, *cur;
    char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs");

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr == NULL)
            continue;

        family = cur->ifa_addr->sa_family;

        /* For an AF_INET interface address, display the address */
        if (family == AF_INET) {
            if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST))
                msg_abort("getnameinfo");

            if (strcmp("127.0.0.1", host)) {
                found = 1;
                break;
            }
        }
    }

    if (!found)
        msg_abort("No valid IP found");

    port = ((long) getpid() % 55000) + 5000;

    sprintf(sctx.hgaddr, "%s://%s:%d", HG_PROTO, host, port);
    fprintf(stderr, "Address: %s\n", sctx.hgaddr);

    freeifaddrs(ifaddr);
}

//}   /* extern "C" */
