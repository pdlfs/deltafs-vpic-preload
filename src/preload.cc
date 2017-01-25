/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <errno.h>
#include <assert.h>
#include <dirent.h>
#include <dlfcn.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>

#include <string>

#include "preload_internal.h"

#include "preload.h"
#include "shuffle.h"

/* XXX: VPIC is usually a single-threaded process but mutex may be
 * needed if VPIC is running with openmp.
 */
static maybe_mutex_t mtx = MAYBE_MUTEX_INITIALIZER;

preload_ctx_t pctx = { 0 };

/*
 * we use the address of fake_dirptr as a fake DIR* with opendir/closedir
 */
static int fake_dirptr = 0;

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

    pctx.isdeltafs = new std::set<FILE*>;

    pctx.root = getenv("PDLFS_Root");
    if (!pctx.root) pctx.root = DEFAULT_ROOT;
    pctx.len_root = strlen(pctx.root);

    /* root: any non-null path, not "/" and not ending in "/" */
    if ( pctx.len_root == 0 ||
        (pctx.len_root == 1 && pctx.root[0] == '/') ||
        pctx.root[pctx.len_root-1] == '/' )
        msg_abort("bad PDLFS_root");

    /* pctx.testmode is set to NO_TEST by default */
    if (getenv("PDLFS_Preload_test"))
        pctx.testmode = PRELOAD_TEST;
    else if (getenv("PDLFS_Shuffle_test"))
        pctx.testmode = SHUFFLE_TEST;
    else if (getenv("PDLFS_Placement_test"))
        pctx.testmode = PLACEMENT_TEST;
    else if (getenv("PDLFS_Deltafs_NoPLFS_test"))
        pctx.testmode = DELTAFS_NOPLFS_TEST;

    if (getenv("PDLFS_Shuffle_bypass"))
        pctx.testbypass = 1;

    /* XXXCDC: additional init can go here or MPI_Init() */
}

/*
 * claim_path: look at path to see if we can claim it
 */
static bool claim_path(const char *path, bool *exact)
{
    /*
     * XXX - George: Chuck, this would not catch the case where cwd is pctx.root
     *               and the path is relative.
     *               In general, how do we handle relative paths?
     */

    if (strncmp(pctx.root, path, pctx.len_root) != 0 ||
         (path[pctx.len_root] != '/' && path[pctx.len_root] != '\0') ) {
        return(false);
    }

    /* if we've just got pctx.root, caller may convert it to a "/" */
    *exact = (path[pctx.len_root] == '\0');
    return(true);
}

/*
 * claim_FILE: look at FILE* and see if we claim it
 */
static bool claim_FILE(FILE *stream)
{
    std::set<FILE *>::iterator it;
    bool rv;

    must_lockmutex(&mtx);
    assert(pctx.isdeltafs != NULL);
    it = pctx.isdeltafs->find(stream);
    rv = (it != pctx.isdeltafs->end());
    must_unlock(&mtx);

    return(rv);
}

namespace {
/*
 * fake_file is a replacement for FILE* that we use to accumulate all the
 * VPIC particle data before sending it to the shuffle layer (on fclose).
 *
 * we assume only one thread is writing to the file at a time, so we
 * do not put a mutex on it.
 *
 * we ignore out of memory errors.
 */
class fake_file {
  private:
    std::string path_;       /* path of particle file (malloc'd c++) */
    char data_[64];          /* enough for one VPIC particle */
    char *dptr_;             /* ptr to next free space in data_ */
    size_t resid_;           /* residual */

  public:
    explicit fake_file(const char *path) :
        path_(path), dptr_(data_), resid_(sizeof(data_)) {};

    /* returns the actual number of bytes added. */
    size_t add_data(const void *toadd, size_t len) {
        int n = (len > resid_) ? resid_ : len;
        if (n) {
            memcpy(dptr_, toadd, n);
            dptr_ += n;
            resid_ -= n;
        }
        return(n);
    }

    /* get data length */
    size_t size() {
        return sizeof(data_) - resid_;
    }

    /* recover filename. */
    const char *file_name()  {
        return path_.c_str();
    }

    /* get data */
    char *data() {
        return data_;
    }
};
} // namespace

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
    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("MPI_Init:pthread_once");

    int rank;
    rv = nxt.MPI_Init(argc, argv);
    if (rv == MPI_SUCCESS) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    } else {
        return(rv);
    }

    genHgAddr();
    shuffle_init();

    if (rank == 0 && pctx.testmode) {
        nxt.mkdir(REDIRECT_TEST_ROOT, 0777);

        pctx.log = DEBUG_LOG;

        FILE* f = nxt.fopen(pctx.log, "w+");
        if (!f) {
            msg_abort("MPI_Init:fopen");
        } else {
            delete f;
        }
    }

    return(rv);
}

/*
 * MPI_Finalize
 */
int MPI_Finalize(void)
{
    int rv;

    shuffle_destroy();

    rv = nxt.MPI_Finalize();
    return(rv);
}

/*
 * mkdir
 */
int mkdir(const char *path, mode_t mode)
{
    bool exact;
    const char *stripped;
    char testpath[PATH_MAX];
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("mkdir:pthread_once");

    if (!claim_path(path, &exact)) {
        return(nxt.mkdir(path, mode));
    }

    /* relative paths we pass through, absolute we strip off prefix */

    /*
     * XXX - George: But if we passed claim_path above, we know that we
     *               can remove the prefix. Why do we need to keep it for
     *               relative paths then?
     */

    if (*path != '/') {
        stripped = path;
    } else {
        stripped = (exact) ? "/" : (path + pctx.len_root);
    }

    if (pctx.testmode &&
        snprintf(testpath, PATH_MAX, REDIRECT_TEST_ROOT "%s", stripped))
        msg_abort("mkdir:snprintf");

    switch (pctx.testmode) {
        case NO_TEST:
            rv = deltafs_mkdir(stripped, mode | DELTAFS_DIR_PLFS_STYLE);
            break;
        case PRELOAD_TEST:
        case SHUFFLE_TEST:
        case PLACEMENT_TEST:
            rv = mkdir(testpath, mode);
            break;
        case DELTAFS_NOPLFS_TEST:
            rv = deltafs_mkdir(stripped, mode);
            break;
    }

    return(rv);
}

/*
 * opendir
 */
DIR *opendir(const char *path)
{
    int rv;
    bool exact;
    const char *stripped;
    char testpath[PATH_MAX];

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("opendir:pthread_once");

    if (!claim_path(path, &exact)) {
        return(nxt.opendir(path));
    }

    if (*path != '/') {
        stripped = path;
    } else {
        stripped = (exact) ? "/" : (path + pctx.len_root);
    }

    if (pctx.testmode &&
        snprintf(testpath, PATH_MAX, REDIRECT_TEST_ROOT "%s", stripped))
        msg_abort("opendir:snprintf");

    switch (pctx.testmode) {
        case NO_TEST:
        case DELTAFS_NOPLFS_TEST:
            /* XXX: Call epoch ending function here */
            //int deltafs_epoch_flush(int __fd, void* __arg);

            /* we return a fake DIR* pointer for deltafs, since we don't actually open */
            return(reinterpret_cast<DIR *>(&fake_dirptr));
        case PRELOAD_TEST:
        case SHUFFLE_TEST:
        case PLACEMENT_TEST:
            /* We don't redirect opendir for our tests */
            return(nxt.opendir(path));
    }
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

    /* relative paths we pass through, absolute we strip off prefix */
    if (*filename != '/') {
        newpath = filename;
    } else {
        newpath = (exact) ? "/" : (filename + pctx.len_root);
    }

    /* allocate our fake FILE* and put it in the set */
    fake_file *ff = new fake_file(newpath);
    FILE *fp = reinterpret_cast<FILE *>(ff);

    must_lockmutex(&mtx);
    assert(pctx.isdeltafs != NULL);
    pctx.isdeltafs->insert(fp);
    must_unlock(&mtx);

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

    fake_file *ff = reinterpret_cast<fake_file *>(stream);
    int cnt = ff->add_data(ptr, size*nitems);

    /*
     * fwrite returns number of items written.  it can return a short
     * object count on error.
     */

    return(cnt / size);    /* truncates on error */
}

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
    fake_file *ff = reinterpret_cast<fake_file *>(stream);

    if (pctx.testbypass)
        rv = shuffle_write_local(ff->file_name(), ff->data(), ff->size());
    else
        rv = shuffle_write(ff->file_name(), ff->data(), ff->size());

    must_lockmutex(&mtx);
    assert(pctx.isdeltafs != NULL);
    pctx.isdeltafs->erase(stream);
    must_unlock(&mtx);

    delete ff;

    if (pctx.testmode)
        return 0;
    else
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

    errno = ENOTSUP;
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

    errno = ENOTSUP;
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

    errno = ENOTSUP;
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

    errno = ENOTSUP;
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

    errno = ENOTSUP;
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

    errno = ENOTSUP;
    msg_abort("ftell!");
    return(0);
}

}   /* extern "C" */
