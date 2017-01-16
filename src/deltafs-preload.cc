/*
 * Copyright (c) 2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>

#include <deltafs/deltafs_api.h>

#include "fake-file.h"
#include "shuffle.h"

shuffle_ctx_t sctx = { 0 };

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
    int (*MPI_Init)(int *argc, char ***argv);
    int (*MPI_Finalize)(void);
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
};
static struct next_functions nxt = { 0 };

/*
 * this once is used to trigger the init of the preload library...
 */

static pthread_once_t init_once = PTHREAD_ONCE_INIT;

/* helper: must_getnextdlsym: get next symbol or fail */
static void must_getnextdlsym(void **result, const char *symbol)
{
    *result = dlsym(RTLD_NEXT, symbol);
    if (*result == NULL) msg_abort(symbol);
}

/*
 * preload_init: called via init_once.   if this fails we are sunk, so
 * we'll abort the process....
 */
static void preload_init()
{
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.MPI_Init), "MPI_Init");
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.MPI_Finalize), "MPI_Finalize");
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

    sctx.root = getenv("PDLFS_Root");
    if (!sctx.root) sctx.root = DEFAULT_ROOT;
    sctx.len_root = strlen(sctx.root);
    /* sctx.setlock and sctx.isdeltafs init'd by ctor */
    if (getenv("PDLFS_Testin"))
        sctx.testin = 1;

    /* root: any non-null path, not "/" and not ending in "/" */
    if ( sctx.len_root == 0 ||
        (sctx.len_root == 1 && sctx.root[0] == '/') ||
        sctx.root[sctx.len_root-1] == '/' )
        msg_abort("bad PDLFS_root");

    /* XXXCDC: additional init can go here or MPI_Init() */
}

/*
 * claim_path: look at path to see if we can claim it
 */
static bool claim_path(const char *path, bool *exact)
{
    if (strncmp(sctx.root, path, sctx.len_root) != 0 ||
         (path[sctx.len_root] != '/' && path[sctx.len_root] != '\0') ) {
        return(false);
    }

    /* if we've just got sctx.root, caller may convert it to a "/" */
    *exact = (path[sctx.len_root] == '\0');
    return(true);
}

/*
 * claim_FILE: look at FILE* and see if we claim it
 */
static bool claim_FILE(FILE *stream)
{
    std::set<FILE *>::iterator it;
    bool rv;

    sctx.setlock.Lock();
    it = sctx.isdeltafs.find(stream);
    rv = (it != sctx.isdeltafs.end());
    sctx.setlock.Unlock();

    return(rv);
}

/*
 * here are the actual override functions from libc...
 */
extern "C" {

/*
 * MPI_Init
 */
int MPI_Init(int *argc, char ***argv)
{
    int rv;

    rv = nxt.MPI_Init(argc, argv);

    genHgAddr();
    shuffle_init();

    /* XXXCDC: additional init can go here or preload_inipreload_init() */

    return(rv);
}

/*
 * MPI_Finalize
 */
int MPI_Finalize(void)
{
    int rv;

    rv = nxt.MPI_Finalize();

    shuffle_destroy();

    /* XXXCDC: additional teardown can go here */

    return(rv);
}

/*
 * mkdir
 */
int mkdir(const char *path, mode_t mode)
{
    bool exact;
    const char *newpath;
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("mkdir:pthread_once");

    if (!claim_path(path, &exact)) {
        return(nxt.mkdir(path, mode));
    }

    /* relatives paths we pass through, absolute we strip off prefix */
    if (*path != '/') {
        newpath = path;
    } else {
        newpath = (exact) ? "/" : (path + sctx.len_root);
    }

    if (sctx.testin) {
        printf("MKDIR %s %s\n", newpath, path);
        return(0);
    }

#ifdef PLFS_DIRMODE_BYPASS   /* XXX for initial testing... */
    rv = deltafs_mkdir(path, mode);
#else
    rv = deltafs_mkdir(path, mode | DELTAFS_DIR_PLFS_STYLE);
#endif

    return(rv);
}

/*
 * opendir
 */
DIR *opendir(const char *filename)
{
    int rv;
    bool exact;
    const char *newpath;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("opendir:pthread_once");

    if (!claim_path(filename, &exact)) {
        return(nxt.opendir(filename));
    }

    /* XXXCDC: CALL EPOCH HERE */
    //int deltafs_epoch_flush(int __fd, void* __arg);

    /* we return a fake DIR* pointer for deltafs, since we don't actually open */
    return(reinterpret_cast<DIR *>(&fake_dirptr));
}

/*
 * closedir
 */
int closedir(DIR *dirp)
{
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("closedir:pthread_once");

    if (dirp == reinterpret_cast<DIR *>(&fake_dirptr))
        return(0);   /* deltafs - it is a noop */

    return(nxt.closedir(dirp));
}

/*
 * fopen
 */
FILE *fopen(const char *filename, const char *mode)
{
    int rv;
    bool exact;
    const char *newpath;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("fopen:pthread_once");

    if (!claim_path(filename, &exact)) {
        return(nxt.fopen(filename, mode));
    }

    /* relatives paths we pass through, absolute we strip off prefix */
    if (*filename != '/') {
        newpath = filename;
    } else {
        newpath = (exact) ? "/" : (filename + sctx.len_root);
    }

    /* allocate our fake FILE* and put it in the set */
    deltafspreload::FakeFile *ff = new deltafspreload::FakeFile(newpath);
    FILE *fp = reinterpret_cast<FILE *>(ff);

    sctx.setlock.Lock();
    sctx.isdeltafs.insert(fp);
    sctx.setlock.Unlock();

    return(fp);
}

/*
 * fwrite
 */
size_t fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream)
{
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("fwrite:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.fwrite(ptr, size, nitems, stream));
    }

    deltafspreload::FakeFile *ff = 
        reinterpret_cast<deltafspreload::FakeFile *>(stream);

    int cnt = ff->AddData(ptr, size*nitems);

    /* 
     * fwrite returns number of items written.  it can return a short
     * object count on error.
     */

    return(cnt / size);    /* truncates on error */
}

#ifdef SHUFFLE_BYPASS   /* XXX for debug */
/*
 * shuffle_bypass_write(): write directly to deltafs without shuffle.
 * for debugging so print msg on any err.   returns 0 or EOF on error.
 */
static int shuffle_bypass_write(const char *fn, char *data, int len)
{
    int fd, rv;
    ssize_t wrote;

    fd = deltafs_open(fn, O_WRONLY|O_CREAT|O_APPEND, 0666);
    if (fd < 0) {
        fprintf(stderr, "shuffle_bypass: %s: open failed (%s)\n", fn,
                strerror(errno));
        return(EOF);
    }

    wrote = deltafs_write(fd, data, len);
    if (wrote != len)
        fprintf(stderr, "shuffle_bypass: %s: write failed: %d (want %d)\n",
                fn, (int)wrote, (int)len);

    rv = deltafs_close(fd);
    if (rv < 0)
        fprintf(stderr, "shuffle_bypass: %s: close failed (%s)\n", fn,
                strerror(errno));

    return((wrote != len || rv < 0) ? EOF : 0);
}
#endif

/*
 * fclose.   returns EOF on error.
 */
int fclose(FILE *stream)
{
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("fclose:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.fclose(stream));
    }

    rv = 0;
    deltafspreload::FakeFile *ff = 
        reinterpret_cast<deltafspreload::FakeFile *>(stream);

    if (sctx.testin) {
        printf("FCLOSE: %s %.*s %d\n", ff->FileName(), ff->DataLen(),
               ff->Data(), ff->DataLen());
    } else {
#ifdef SHUFFLE_BYPASS
        rv = shuffle_bypass_write(ff->FileName(), ff->Data(), ff->DataLen());
#else
        // XXXCDC: shuffle_write(ff->FileName(), ff->Data(), ff->DataLen());
#endif
    }

    sctx.setlock.Lock();
    sctx.isdeltafs.erase(stream);
    sctx.setlock.Unlock();

    delete ff;
    return(rv);
}

/*
 * the rest of these we do not override for deltafs.   if we get a 
 * deltafs FILE*, we've got a serious problem and we abort...
 */

/*
 * feof
 */
int feof(FILE *stream)
{
    int rv;
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("feof:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.feof(stream));
    }

    msg_abort("feof!");
    return(0);
}

/*
 * ferror
 */
int ferror(FILE *stream)
{
    int rv;
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("ferror:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.ferror(stream));
    }

    msg_abort("ferror!");
    return(0);
}

/*
 * clearerr
 */
void clearerr(FILE *stream)
{
    int rv;
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("clearerr:pthread_once");

    if (!claim_FILE(stream)) {
        nxt.clearerr(stream);
        return;
    }

    msg_abort("clearerr!");
}

/*
 * fread
 */
size_t fread(void *ptr, size_t size, size_t nitems, FILE *stream)
{
    int rv;
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("fread:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.fread(ptr, size, nitems, stream));
    }

    msg_abort("fread!");
    return(0);
}

/*
 * fseek
 */
int fseek(FILE *stream, long offset, int whence)
{
    int rv;
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("fseek:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.fseek(stream, offset, whence));
    }

    msg_abort("fseek!");
    return(0);
}

/*
 * ftell
 */
long ftell(FILE *stream)
{
    int rv;
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("ftell:pthread_once");

    if (!claim_FILE(stream)) {
        return(nxt.ftell(stream));
    }

    msg_abort("ftell!");
    return(0);
}

}   /* extern "C" */
