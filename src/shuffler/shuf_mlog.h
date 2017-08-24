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
 * this puts mlog into a "shuf" C++ namespace
 */

#define MLOG_NSPACE shuf

#include "mlog.h"

/* facilities */
#define SHUF_MLOG 0
#define UTIL_MLOG 1
#define CLNT_MLOG 2
#define DLIV_MLOG 3
#define SHUF_MAXFAC 4

/* shortcuts */
#define SHUF_CRIT (SHUF_MLOG|MLOG_CRIT)
#define SHUF_ERR  (SHUF_MLOG|MLOG_ERR)
#define SHUF_WARN (SHUF_MLOG|MLOG_WARN)
#define SHUF_NOTE (SHUF_MLOG|MLOG_NOTE)
#define SHUF_INFO (SHUF_MLOG|MLOG_INFO)
#define SHUF_CALL (SHUF_MLOG|MLOG_DBG0)
#define SHUF_D1   (SHUF_MLOG|MLOG_DBG1)

#define UTIL_CRIT (UTIL_MLOG|MLOG_CRIT)
#define UTIL_ERR  (UTIL_MLOG|MLOG_ERR)
#define UTIL_WARN (UTIL_MLOG|MLOG_WARN)
#define UTIL_NOTE (UTIL_MLOG|MLOG_NOTE)
#define UTIL_INFO (UTIL_MLOG|MLOG_INFO)
#define UTIL_CALL (UTIL_MLOG|MLOG_DBG0)
#define UTIL_D1   (UTIL_MLOG|MLOG_DBG1)

#define CLNT_CRIT (CLNT_MLOG|MLOG_CRIT)
#define CLNT_ERR  (CLNT_MLOG|MLOG_ERR)
#define CLNT_WARN (CLNT_MLOG|MLOG_WARN)
#define CLNT_NOTE (CLNT_MLOG|MLOG_NOTE)
#define CLNT_INFO (CLNT_MLOG|MLOG_INFO)
#define CLNT_CALL (CLNT_MLOG|MLOG_DBG0)
#define CLNT_D1   (CLNT_MLOG|MLOG_DBG1)

#define DLIV_CRIT (DLIV_MLOG|MLOG_CRIT)
#define DLIV_ERR  (DLIV_MLOG|MLOG_ERR)
#define DLIV_WARN (DLIV_MLOG|MLOG_WARN)
#define DLIV_NOTE (DLIV_MLOG|MLOG_NOTE)
#define DLIV_INFO (DLIV_MLOG|MLOG_INFO)
#define DLIV_CALL (DLIV_MLOG|MLOG_DBG0)
#define DLIV_D1   (DLIV_MLOG|MLOG_DBG1)
