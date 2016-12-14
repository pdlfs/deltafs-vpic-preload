/*
 * Copyright (c) 2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <dirent.h>
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <set>

#include <deltafs/deltafs_api.h>
#include <pdlfs-common/port.h>

#include "fake-file.h"

/*
 * we use the address of fake_dirptr as a fake DIR* with opendir/closedir
 */
static int fake_dirptr = 0;

/*
 * user specifies a prefix that we use to redirect to deltafs via
 * the PDLFS_Root env varible.   If not provided, we default to /tmp/pdlfs.
 */
#define DEFAULT_ROOT "/tmp/pdlfs"

/*
 * next_functions: libc replacement functions we are providing to the preloader.
 */
struct next_functions {
    /* functions we need */
    int (*mkdir)(const char *path, mode_t mode);
    DIR *(*opendir)(const char *filename);
    int (*closedir)(DIR *dirp);
    FILE *(*fopen)(const char *filename, const char *mode);
    size_t (*fwrite)(const void *ptr, size_t size, size_t nitems, FILE *stream);
    int (*fclose)(FILE *stream);

    /* for error catching we do these */
    int (*feof)(FILE *stream);
    int (*ferror)(FILE *stream);
    void (*clearerr)(FILE *stream);
    size_t (*fread)(void *ptr, size_t size, size_t nitems, FILE *stream);
    int (*fseek)(FILE *stream, long offset, int whence);
    long (*ftell)(FILE *stream);
    int (*fflush)(FILE *stream);
};
static struct next_functions nxt = { 0 };

/*
 * preload context: this is where we store the run-time state of the preload lib
 */
struct preload_context {
    const char *root;
    int len_root;                       /* strlen root */

    pdlfs::port::Mutex setlock;
    std::set<FILE *> isdeltafs;
};
static preload_context ctx = { 0 };

/*
 * this once is used to trigger the init of the preload library...
 */

static pthread_once_t init_once = PTHREAD_ONCE_INIT;

/* helper: must_getnextdlsym: get next symbol or assert */
static void must_getnextdlsym(void **result, const char *symbol) {
    *result = dlsym(RTLD_NEXT, symbol);
    assert(*result);
}

/*
 * preload_init: called via init_once.   if this fails we are sunk, so
 * we'll abort the process....
 */
static void preload_init() {
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.mkdir), "mkdir");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.opendir), "opendir");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.closedir), "closedir");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.fopen), "fopen");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.fwrite), "fwrite");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.fclose), "fclose");

    must_getnextdlsym(reinterpret_cast<void **>(&nxt.feof), "feof");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.ferror), "ferror");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.clearerr), "clearerr");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.fread), "fread");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.fseek), "fseek");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.ftell), "ftell");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.fflush), "fflush");

    ctx.root = getenv("PDLFS_Root");
    if (!ctx.root) ctx.root = DEFAULT_ROOT;
    ctx.len_root = strlen(ctx.root);
    /* ctx.setlock and ctx.isdeltafs init'd by ctor */

    /* root: absolute path, not "/" and not ending in "/" */
    assert(ctx.root[0] == '/' && ctx.len_root > 1 && 
           ctx.root[ctx.len_root-1] != '/');

    /* 
     * XXXCDC: what else do we need to init?   shuffle?
     * XXXCDC: what about MPI?   rank to IP mappings?
     */
}

/*
 * claim_path: look at path to see if we can claim it
 */
static bool claim_path(const char *path, bool *need_slash) {

    if (strncmp(ctx.root, path, ctx.len_root) != 0 ||
         (path[ctx.len_root] != '/' && path[ctx.len_root] != '\0') ) {
        return(false);
    }

    /* if we've just got ctx.root, we need convert it to a "/" */
    *need_slash = (path[ctx.len_root] == '\0');
    return(true);
}

/*
 * claim_FILE: look at FILE* and see if we claim it
 */
static bool claim_FILE(FILE *stream) {
    std::set<FILE *>::iterator it;
    bool rv;

    ctx.setlock.Lock();
    it = ctx.isdeltafs.find(stream);
    rv = (it != ctx.isdeltafs.end());
    ctx.setlock.Unlock();

    return(rv);
}

/*
 * here are the actual override functions from libc...
 */
extern "C" {

/*
 * mkdir
 */
int mkdir(const char *path, mode_t mode) {
    bool need_slash;
    const char *newpath;
    int rv;

    assert(pthread_once(&init_once, preload_init));

    if (!claim_path(path, &need_slash)) {
        return(nxt.mkdir(path, mode));
    }

    newpath = (need_slash) ? "/" : (path + ctx.len_root);

    rv = deltafs_mkdir(path, mode | DELTAFS_DIR_PLFS_STYLE);

    return(rv);
}

/*
 * opendir
 */
DIR *opendir(const char *filename) {
    bool need_slash;
    const char *newpath;

    assert(pthread_once(&init_once, preload_init));

    if (!claim_path(filename, &need_slash)) {
        return(nxt.opendir(filename));
    }

    /* XXXCDC: CALL EPOCH */

    /* we return a fake DIR* pointer for deltafs, since we don't actually open */
    return(reinterpret_cast<DIR *>(&fake_dirptr));
}

/*
 * closedir
 */
int closedir(DIR *dirp) {
    assert(pthread_once(&init_once, preload_init));

    if (dirp == reinterpret_cast<DIR *>(&fake_dirptr))
        return(0);   /* deltafs - it is a noop */

    return(nxt.closedir(dirp));
}

/*
 * fopen
 */
FILE *fopen(const char *filename, const char *mode) {
    bool need_slash;
    const char *newpath;

    assert(pthread_once(&init_once, preload_init));

    if (!claim_path(filename, &need_slash)) {
        return(nxt.fopen(filename, mode));
    }

    newpath = (need_slash) ? "/" : (filename + ctx.len_root);

    /* allocate our fake FILE* and put it in the set */
    deltafspreload::FakeFile *ff = new deltafspreload::FakeFile(newpath);
    FILE *rv = reinterpret_cast<FILE *>(ff);

    ctx.setlock.Lock();
    ctx.isdeltafs.insert(rv);
    ctx.setlock.Unlock();

    return(rv);
}

/*
 * fwrite
 */
size_t fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.fwrite(ptr, size, nitems, stream));
    }

    deltafspreload::FakeFile *ff = 
        reinterpret_cast<deltafspreload::FakeFile *>(stream);

    int cnt = ff->AddData(ptr, size);

    /* 
     * fwrite returns number of items written.  it can return a short
     * object count on error.
     */

    return(cnt / size);    /* truncates on error */
}

/*
 * fclose
 */
int fclose(FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.fclose(stream));
    }

    deltafspreload::FakeFile *ff = 
        reinterpret_cast<deltafspreload::FakeFile *>(stream);

    // XXXCDC: shuffle_write(ff->FileName(), ff->Data(), ff->DataLen());

    ctx.setlock.Lock();
    ctx.isdeltafs.erase(stream);
    ctx.setlock.Unlock();

    delete ff;
    return(0);
}

/*
 * the rest of these we do not override for deltafs.   if we get a 
 * deltafs FILE*, we've got a serious problem and we assert/abort...
 */

/*
 * feof
 */
int feof(FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.feof(stream));
    }

    assert(0);
    return(0);
}

/*
 * ferror
 */
int ferror(FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.ferror(stream));
    }

    assert(0);
    return(0);
}

/*
 * clearerr
 */
void clearerr(FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        nxt.clearerr(stream);
        return;
    }

    assert(0);
}

/*
 * fread
 */
size_t fread(void *ptr, size_t size, size_t nitems, FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.fread(ptr, size, nitems, stream));
    }

    assert(0);
    return(0);
}

/*
 * fseek
 */
int fseek(FILE *stream, long offset, int whence) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.fseek(stream, offset, whence));
    }

    assert(0);
    return(0);
}

/*
 * ftell
 */
long ftell(FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.ftell(stream));
    }

    assert(0);
    return(0);
}

/*
 * fflush
 */
int fflush(FILE *stream) {
    assert(pthread_once(&init_once, preload_init));

    if (!claim_FILE(stream)) {
        return(nxt.fflush(stream));
    }

    assert(0);
    return(0);
}

}   /* extern "C" */
