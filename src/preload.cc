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

#include "io_plfsdir.h"

#include "shuffle_internal.h"
#include "preload_internal.h"

#include "preload.h"

/* XXX: VPIC is usually a single-threaded process but mutex may be
 * needed if VPIC is running with openmp.
 */
static maybe_mutex_t maybe_mtx = MAYBE_MUTEX_INITIALIZER;

/* global context */
preload_ctx_t pctx = { 0 };


/* number of MPI barriers invoked by app */
static int num_barriers = 0;

/* number of epoches generated */
static int num_epochs = 0;


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
    int (*MPI_Barrier)(MPI_Comm comm);
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
    must_getnextdlsym(reinterpret_cast<void **>(&nxt.MPI_Barrier), "MPI_Barrier");
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

    pctx.logfd = -1;
    pctx.monfd = -1;

    pctx.isdeltafs = new std::set<FILE*>;

    pctx.paranoid_barrier = 1;

    pctx.deltafs_root = getenv("PRELOAD_Deltafs_root");
    if (!pctx.deltafs_root) pctx.deltafs_root = DEFAULT_DELTAFS_ROOT;
    pctx.len_deltafs_root = strlen(pctx.deltafs_root);

    /* deltafs root:
     *   - any non-null path,
     *   - not "/", and
     *   - not ending in "/"
     */
    if ( pctx.len_deltafs_root == 0
            || (pctx.len_deltafs_root == 1 && pctx.deltafs_root[0] == '/')
            || pctx.deltafs_root[pctx.len_deltafs_root - 1] == '/' )
        msg_abort("bad deltafs_root");

    /* obtain the path to plfsdir */
    pctx.plfsdir = getenv("PRELOAD_Plfsdir");

    /* plfsdir:
     *   - if null, no plfsdir will ever be created
     *   - otherwise, it will be created and opened at MPI_Init
     */
    if (pctx.plfsdir == NULL) {
        if (pctx.deltafs_root[0] != '/') {
            /* default to deltafs_root if deltafs_root is relative */
            pctx.plfsdir = pctx.deltafs_root;
        }
    }
    if (pctx.plfsdir != NULL) {
        pctx.len_plfsdir = strlen(pctx.plfsdir);
    }

    pctx.plfsfd = -1;

    pctx.local_root = getenv("PRELOAD_Local_root");
    if (!pctx.local_root) pctx.local_root = DEFAULT_LOCAL_ROOT;
    pctx.len_local_root = strlen(pctx.local_root);

    /* local root:
     *   - any non-null path,
     *   - not "/",
     *   - starting with "/", and
     *   - not ending in "/"
     */
    if ( pctx.len_local_root == 0 || pctx.len_local_root == 1
            || pctx.local_root[0] != '/'
            || pctx.local_root[pctx.len_local_root - 1] == '/' )
        msg_abort("bad local_root");

    if (is_envset("PRELOAD_Bypass_shuffle"))
        pctx.mode |= BYPASS_SHUFFLE;
    if (is_envset("PRELOAD_Bypass_placement"))
        pctx.mode |= BYPASS_PLACEMENT;

    if (is_envset("PRELOAD_Bypass_deltafs_plfsdir"))
        pctx.mode |= BYPASS_DELTAFS_PLFSDIR;
    if (is_envset("PRELOAD_Bypass_deltafs_namespace"))
        pctx.mode |= BYPASS_DELTAFS_NAMESPACE;
    if (is_envset("PRELOAD_Bypass_deltafs"))
        pctx.mode |= BYPASS_DELTAFS;

    if (is_envset("PRELOAD_Skip_mon"))
        pctx.nomon = 1;
    if (is_envset("PRELOAD_Skip_mon_dist"))
        pctx.nomondist = 1;
    if (is_envset("PRELOAD_Enable_verbose_mon"))
        pctx.vmon = 1;
    if (is_envset("PRELOAD_Enable_verbose_error"))
        pctx.verr = 1;
    if (is_envset("PRELOAD_Testing"))
        pctx.testin = 1;

    /* XXXCDC: additional init can go here or MPI_Init() */
}

/*
 * claim_path: look at path to see if we can claim it
 */
static bool claim_path(const char *path, bool *exact)
{

    if (strncmp(pctx.deltafs_root, path, pctx.len_deltafs_root) != 0) {
        return(false);
    } else if (path[pctx.len_deltafs_root] != '/' &&
            path[pctx.len_deltafs_root] != '\0') {
        return(false);
    }

    /* if we've just got pctx.root, caller may convert it to a "/" */
    *exact = (path[pctx.len_deltafs_root] == '\0');
    return(true);
}

/*
 * under_plfsdir: if a given path is a plfsdir or plfsdir files
 */
static bool under_plfsdir(const char* path)
{
    if (pctx.plfsdir == NULL) {
        return(false);
    } else if (strncmp(pctx.plfsdir, path, pctx.len_plfsdir) != 0) {
        return(false);
    } else if (path[pctx.len_plfsdir] != '/' &&
            path[pctx.len_plfsdir] != '\0') {
        return(false);
    } else {
        return(true);
    }
}

/*
 * claim_FILE: look at FILE* and see if we claim it
 */
static bool claim_FILE(FILE *stream)
{
    std::set<FILE *>::iterator it;
    bool rv;

    must_maybelockmutex(&maybe_mtx);
    assert(pctx.isdeltafs != NULL);
    it = pctx.isdeltafs->find(stream);
    rv = (it != pctx.isdeltafs->end());
    must_maybeunlock(&maybe_mtx);

    return(rv);
}

/*
 * print a human-readable time duration
 */
static std::string pretty_dura(double us)
{
    char tmp[100];
    if (us >= 1000000) {
        snprintf(tmp, sizeof(tmp), "%.3f s", us / 1000000.0);
    } else {
        snprintf(tmp, sizeof(tmp), "%.3f ms", us / 1000.0);
    }

    return tmp;
}

/*
 * print a human-readable size.
 */
static std::string pretty_size(double size)
{
    char tmp[100];
    if (size >= 1000000000000.0) {
        size /= 1000000000000.0;
        snprintf(tmp, sizeof(tmp), "%.1f TB", size);
    } else if (size >= 1000000000.0) {
        size /= 1000000000.0;
        snprintf(tmp, sizeof(tmp), "%.1f GB", size);
    } else if (size >= 1000000.0) {
        size /= 1000000.0;
        snprintf(tmp, sizeof(tmp), "%.1f MB", size);
    } else if (size >= 1000.0) {
        size /= 1000.0;
        snprintf(tmp, sizeof(tmp), "%.1f K", size);
    } else {
        snprintf(tmp, sizeof(tmp), "%.0f bytes",
                size);
    }

    return tmp;
}

/*
 * print a human-readable tput.
 */
static std::string pretty_tput(double bytes, double us)
{
    char tmp[100];
    double bytes_per_s = bytes / us * 1000000;
    if (bytes_per_s >= 1000000000000.0) {
        bytes_per_s /= 1000000000000.0;
        snprintf(tmp, sizeof(tmp), "%.3f TB/s", bytes_per_s);
    } else if (bytes_per_s >= 1000000000.0) {
        bytes_per_s /= 1000000000.0;
        snprintf(tmp, sizeof(tmp), "%.3f GB/s", bytes_per_s);
    } else if (bytes_per_s >= 1000000.0) {
        bytes_per_s /= 1000000.0;
        snprintf(tmp, sizeof(tmp), "%.3f MB/s", bytes_per_s);
    } else if (bytes_per_s >= 1000.0) {
        bytes_per_s /= 1000.0;
        snprintf(tmp, sizeof(tmp), "%.3f K/s", bytes_per_s);
    } else {
        snprintf(tmp, sizeof(tmp), "%.3f bytes/s",
                bytes_per_s);
    }

    return tmp;
}

/*
 * dump in-memory mon stats to files.
 */
static void dump_mon(const mon_ctx_t* mon)
{
    uint64_t ts;
    uint64_t diff;
    char buf[4096];
    char msg[100];
    int n;

    if (!pctx.nomon) {
        /* dump txt mon stats to log file if in testing mode */
        if (pctx.testin) {
            if (pctx.logfd != -1) {
                mon_dumpstate(pctx.logfd, mon);
            }
        }

        /* dump txt mon stats to stderr if in verbose mode */
        if (pctx.vmon) {
            if (pctx.rank == 0) {
                mon_dumpstate(fileno(stderr), mon);
            }
        }

        if (pctx.monfd != -1) {
            if (pctx.rank == 0) {
                info("dumping epoch mon stats ... (rank 0)");
                ts = now_micros();
            }
            memset(buf, 0, sizeof(buf));
            assert(sizeof(mon_ctx_t) < sizeof(buf));
            memcpy(buf, mon, sizeof(mon_ctx_t));
            n = write(pctx.monfd, buf, sizeof(buf));
            if (pctx.rank == 0) {
                diff = now_micros() - ts;
                snprintf(msg, sizeof(msg), "dumping ok %s (rank 0)",
                        pretty_dura(diff).c_str());
                info(msg);
            }

            errno = 0;
        }
    }
}

static std::string plfsdir_conf() {
    char tmp[500];
    const char* lg_parts;
    const char* index_buf;
    const char* data_buf;
    const char* memtable_size;

    memtable_size = getenv("PLFSDIR_Memtable_size");
    if (memtable_size == NULL) {
        memtable_size = DEFAULT_MEMTABLE_SIZE;
    }

    index_buf = getenv("PLFSDIR_Index_buf_size");
    if (index_buf == NULL) {
        index_buf = DEFAULT_INDEX_BUF;
    }

    data_buf = getenv("PLFSDIR_Data_buf_size");
    if (data_buf == NULL) {
        data_buf = DEFAULT_DATA_BUF;
    }

    lg_parts = getenv("PLFSDIR_Lg_parts");
    if (lg_parts == NULL) {
        lg_parts = DEFAULT_LG_PARTS;
    }

    snprintf(tmp, sizeof(tmp), "memtable_size=%s&index_buffer=%s&"
            "data_buffer=%s&lg_parts=%s", memtable_size,
            index_buf, data_buf, lg_parts);

    return tmp;
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
    bool exact;
    const char* stripped;
    time_t now;
    char buf[50];   // ctime_r
    char msg[100];  // snprintf
    char path[PATH_MAX];
    char conf[500];
    int mpi_wtime_is_global;
    int flag;
    int size;
    int rank;
    int rv;
    int n;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("MPI_Init:pthread_once");

    rv = nxt.MPI_Init(argc, argv);
    if (rv == MPI_SUCCESS) {
        MPI_Comm_size(MPI_COMM_WORLD, &size);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        pctx.rank = rank;
        if (rank == 0) {
            info("lib initializing ...");
            snprintf(msg, sizeof(msg), "%d cores", size);
            info(msg);
        }
    } else {
        return(rv);
    }

#if MPI_VERSION < 3
    if (rank == 0) {
        warn("using non-recent MPI release: some features disabled\n>>> "
                "MPI ver 3 is suggested in production mode");
    }
#endif

    if (rank == 0) {
#if defined(MPI_WTIME_IS_GLOBAL)
        MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_WTIME_IS_GLOBAL,
                &mpi_wtime_is_global, &flag);
        if (flag != 0) {
            if (mpi_wtime_is_global == 0) {
                warn("wtime is not global");
            }
        } else {
            warn("cannot determine if wtime is global\n>>> "
                    "attr MPI_WTIME_IS_GLOBAL not set");
        }
#else
        warn("cannot determine if wtime is global\n>>> "
                "MPI_WTIME_IS_GLOBAL undefined");
#endif
    }

    if (rank == 0) {
#ifndef NDEBUG
        warn("assertions enabled: code unnecessarily slow\n>>> recompile with "
                "\"-DNDEBUG\" to disable assertions");
#endif
    }

    if (pctx.testin) {
        if (rank == 0) {
            warn("testing mode: code unnecessarily slow\n>>> rerun with "
                    "\"export PRELOAD_Testing=0\" to "
                    "disable testing");
        }

        snprintf(path, sizeof(path), "/tmp/vpic-deltafs-trace.log.%d", rank);

        pctx.logfd = open(path, O_WRONLY | O_CREAT | O_TRUNC,
                0666);

        if (pctx.logfd == -1) {
            msg_abort("cannot create log");
        } else {
            now = time(NULL);
            n = snprintf(msg, sizeof(msg), "%s\n--- trace ---\n",
                    ctime_r(&now, buf));
            n = write(pctx.logfd, msg, n);

            errno = 0;
        }
    }

    if (!pctx.nomon) {

        snprintf(path, sizeof(path), "/tmp/vpic-deltafs-mon.bin.%d", rank);

        pctx.monfd = open(path, O_RDWR | O_CREAT | O_TRUNC,
                0666);

        if (pctx.monfd == -1) {
            msg_abort("cannot create mon file");
        } else if (rank ==0) {
            snprintf(msg, sizeof(msg), "in-mem mon stats %d bytes",
                    int(sizeof(mctx)));
            info(msg);
        }
    }

    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
        shuffle_init();

        nxt.MPI_Barrier(MPI_COMM_WORLD);

        if (rank == 0) {
            info("shuffle layer ready");
        }
    }

    /* pre-create plfsdirs if there is any */
    if (pctx.plfsdir != NULL) {
        if (!claim_path(pctx.plfsdir, &exact)) {
            msg_abort("plfsdir out of deltafs");  /* Oops!! */
        }

        /* relative paths we pass through; absolute we strip off prefix */

        if (pctx.plfsdir[0] != '/') {
            stripped = pctx.plfsdir;
        } else if (!exact) {
            stripped = pctx.plfsdir + pctx.len_deltafs_root;
        } else {
            msg_abort("bad plfsdir");
        }

        if (rank == 0) {
            if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode) ||
                    IS_BYPASS_DELTAFS(pctx.mode)) {
                snprintf(path, sizeof(path), "%s/%s", pctx.local_root,
                        stripped);
                rv = nxt.mkdir(path, 0777);
            } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode)) {
                rv = deltafs_mkdir(stripped, 0777 | DELTAFS_DIR_PLFS_STYLE);
            } else {
                rv = deltafs_mkdir(stripped, 0777);
            }

            if (rv != 0) {
                msg_abort("cannot make plfsdir");
            } else {
                info("plfs dir created (rank 0)");
            }
        }

        /* so everyone sees the dir created */
        nxt.MPI_Barrier(MPI_COMM_WORLD);

        /* everyone opens it */
        if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
            snprintf(path, sizeof(path), "%s/%s", pctx.local_root, stripped);
            snprintf(conf, sizeof(conf), "rank=%d&%s", rank,
                    plfsdir_conf().c_str());

            trace(conf);

            pctx.plfsh = deltafs_plfsdir_create(path, conf);
            if (pctx.plfsh == NULL) {
                msg_abort("cannot open plfsdir");
            } else if (rank == 0) {
                info("LW plfs dir opened (rank 0)");
            }
        } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode) &&
                !IS_BYPASS_DELTAFS(pctx.mode)) {
            pctx.plfsfd = deltafs_open(stripped, O_WRONLY | O_DIRECTORY, 0);
            if (pctx.plfsfd == -1) {
                msg_abort("cannot open plfsdir");
            } else if (rank == 0) {
                info("plfs dir opened (rank 0)");
            }
        }
    }

    trace(__func__);
    return(rv);
}

/*
 * MPI_Barrier
 */
int MPI_Barrier(MPI_Comm comm)
{
    int rv = nxt.MPI_Barrier(comm);

    num_barriers++;

    return(rv);
}

/*
 * MPI_Finalize
 */
int MPI_Finalize(void)
{
    int fd1;
    int fd2;
    mon_ctx_t local;
    mon_ctx_t glob;
    char buf[4096];
    char path1[4096];
    char path2[4096];
    char suffix[100];
    char msg[200];
    time_t now;
    struct tm timeinfo;
    uint64_t ts;
    uint64_t diff;
    int ok;
    int go;
    int epoch;
    int rv;
    int n;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("MPI_Finalize:pthread_once");

    if (pctx.rank == 0) {
        info("lib finalizing ... ");
        snprintf(msg, sizeof(msg), "%d epochs generated in total", num_epochs);
        info(msg);
    }

    if (!IS_BYPASS_SHUFFLE(pctx.mode)) {
        /* ensures all peer messages are handled */
        nxt.MPI_Barrier(MPI_COMM_WORLD);
        shuffle_destroy();

        if (pctx.rank == 0) {
            info("shuffle layer closed");
        }
    }

    /* all writes done, time to close all plfsdirs */
    if (pctx.plfsh != NULL) {
        deltafs_plfsdir_close(pctx.plfsh);
        pctx.plfsh = NULL;

        if (pctx.rank == 0) {
            info("LW plfs dir closed (rank 0)");
        }
    }

    if (pctx.plfsfd != -1) {
        deltafs_close(pctx.plfsfd);
        pctx.plfsfd = -1;

        if (pctx.rank == 0) {
            info("plfs dir closed (rank 0)");
        }
    }

    /* close, merge, and dist mon files */
    if (pctx.monfd != -1) {
        if (!pctx.nomon && num_epochs != 0) {
            dump_mon(&mctx);
        }

        if (!pctx.nomondist) {
            ok = 1;  /* ready to go */

            if (pctx.rank == 0) {
                info("merging and copying mon stats to ...");
                ts = now_micros();
                now = time(NULL);
                localtime_r(&now, &timeinfo);
                snprintf(suffix, sizeof(suffix), "%04d%02d%02d-%02d:%02d:%02d",
                        timeinfo.tm_year + 1900, timeinfo.tm_mon + 1,
                        timeinfo.tm_mday, timeinfo.tm_hour, timeinfo.tm_min,
                        timeinfo.tm_sec);
                snprintf(path1, sizeof(path1), "%s/%s-%s.bin", pctx.local_root,
                        "vpic-deltafs-mon-reduced", suffix);
                info(path1);
                fd1 = open(path1, O_WRONLY | O_CREAT | O_EXCL, 0666);
                if (fd1 != -1) {
                    snprintf(path2, sizeof(path2), "%s/"
                            "vpic-deltafs-mon-reduced.bin",
                            pctx.local_root);
                    n = unlink(path2);
                    n = symlink(path1, path2);
                }
                snprintf(path1, sizeof(path1), "%s/%s-%s.txt", pctx.local_root,
                        "vpic-deltafs-mon-reduced", suffix);
                info(path1);
                fd2 = open(path1, O_WRONLY | O_CREAT | O_EXCL, 0666);
                if (fd2 != -1) {
                    snprintf(path2, sizeof(path2), "%s/"
                            "vpic-deltafs-mon-reduced.txt",
                            pctx.local_root);
                    n = unlink(path2);
                    n = symlink(path1, path2);
                }
                if (fd1 == -1 || fd2 == -1) {
                    warn("cannot open mon files");
                    ok = 0;
                }
            }

            if (ok) {
                n = lseek(pctx.monfd, 0, SEEK_SET);
                if (n != 0) {
                    ok = 0;
                }
            }

            epoch = 0;

            while (epoch != num_epochs) {
                if (ok) {
                    n = read(pctx.monfd, buf, sizeof(buf));
                    if (n == sizeof(buf)) {
                        memcpy(&local, buf, sizeof(mon_ctx_t));
                    } else {
                        ok = 0;
                    }
                }

                MPI_Allreduce(&ok, &go, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

                if (go) {
                    mon_reinit(&glob);
                    mon_reduce(&local, &glob);
                    glob.epoch_seq = epoch + 1;
                    glob.global = 1;
                } else if (pctx.rank == 0) {
                    snprintf(msg, sizeof(msg), "error merging epoch %d; "
                            "ABORT action!", epoch + 1);
                    warn(msg);
                }

                if (go) {
                    if (pctx.rank == 0) {
                        mon_dumpstate(fd2, &glob);
                        memset(buf, 0, sizeof(buf));
                        assert(sizeof(buf) > sizeof(mon_ctx_t));
                        memcpy(buf, &glob, sizeof(mon_ctx_t));
                        n = write(fd1, buf, sizeof(buf));

                        if (n == sizeof(buf)) {
                            snprintf(msg, sizeof(msg), " > epoch #%-3d "
                                    "%llu files, %s, %s ok", epoch + 1, glob.nw,
                                    pretty_size(glob.sum_wsz).c_str(),
                                    pretty_tput(glob.sum_wsz,
                                    glob.dura).c_str());
                            info(msg);
                        }

                        errno = 0;
                    }
                } else {
                    break;
                }

                epoch++;
            }

            if (pctx.rank == 0) {
                if (fd1 != -1) {
                    close(fd1);
                }
                if (fd2 != -1) {
                    close(fd2);
                }
                diff = now_micros() - ts;

                snprintf(msg, sizeof(msg),
                        "processed %d epoch stats %s", epoch,
                        pretty_dura(diff).c_str());
                info(msg);
            }
        }

        close(pctx.monfd);
        pctx.monfd = -1;
    }

    /* close testing log file */
    if (pctx.logfd != -1) {
        close(pctx.logfd);
        pctx.logfd = -1;
    }

    /* !!! OK !!! */
    rv = nxt.MPI_Finalize();
    if (pctx.rank == 0) info("all done");
    if (pctx.rank == 0) info("bye");
    trace(__func__);
    return(rv);
}

/*
 * mkdir
 */
int mkdir(const char *dir, mode_t mode)
{
    bool exact;
    const char *stripped;
    char path[PATH_MAX];
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("mkdir:pthread_once");

    if (!claim_path(dir, &exact)) {
        return(nxt.mkdir(dir, mode));
    } else if (under_plfsdir(dir)) {
        return(0);  /* plfsdirs are pre-created at MPI_Init */
    }

    /* relative paths we pass through; absolute we strip off prefix */

    if (*dir != '/') {
        stripped = dir;
    } else {
        stripped = (exact) ? "/" : (dir + pctx.len_deltafs_root);
    }

    if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode) ||
            IS_BYPASS_DELTAFS(pctx.mode)) {
        snprintf(path, sizeof(path), "%s/%s", pctx.local_root, stripped);
        rv = nxt.mkdir(path, mode);
    } else {
        rv = deltafs_mkdir(stripped, mode);
    }

    if (rv != 0) {
        if (pctx.verr) {
            error("mkdir:deltafs_mkdir");
        }
    }

    return(rv);
}

/*
 * opendir
 */
DIR *opendir(const char *dir)
{
    bool ignored_exact;
    char msg[100];
    uint64_t epoch_start;
    double start;
    double min;
    double dura;
    DIR* rv;

    int ret = pthread_once(&init_once, preload_init);
    if (ret) msg_abort("opendir:pthread_once");

    if (!claim_path(dir, &ignored_exact)) {
        return(nxt.opendir(dir));
    } else if (!under_plfsdir(dir)) {
        return(NULL);  /* not supported */
    }

    /* return a fake DIR* since we don't actually open */
    rv = reinterpret_cast<DIR*>(&fake_dirptr);
    epoch_start = now_micros();

    if (pctx.rank == 0) {
        snprintf(msg, sizeof(msg), "epoch %d bootstrapping ... (rank 0)",
                num_epochs + 1);
        info(msg);
    }

    if (num_epochs != 0) {
        /*
         * XXX: explicit epoch flush
         *
         * could be removed if deltafs supports auto epoch flush
         *
         */
        if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
            if (pctx.plfsh != NULL) {
                deltafs_plfsdir_epoch_flush(pctx.plfsh, NULL);
                if (pctx.rank == 0) {
                    info("LW plfs dir flushed (rank 0)");
                }
            } else {
                msg_abort("plfs not opened");
            }

        } else if (!IS_BYPASS_DELTAFS_PLFSDIR(pctx.mode) &&
                !IS_BYPASS_DELTAFS(pctx.mode)) {
            if (pctx.plfsfd != -1 ) {
                deltafs_epoch_flush(pctx.plfsfd, NULL);
                if (pctx.rank == 0) {
                    info("plfs dir flushed (rank 0)");
                }
            } else {
                msg_abort("plfs not opened");
            }

        } else {
            /* no op */
        }
    }

    if (num_epochs != 0) {
        /*
         * delay dumping mon stats collected from the previous
         * epoch until the beginning of the next epoch
         */
        dump_mon(&mctx);
    }

    /* increase epoch seq */
    num_epochs++;

    if (pctx.rank == 0) {
        snprintf(msg, sizeof(msg), "epoch %d begins (rank 0)", num_epochs);
        info(msg);
    }

    mon_reinit(&mctx);   /* reset mon stats */

    mctx.epoch_start = epoch_start;
    mctx.epoch_seq = num_epochs;

    if (!pctx.paranoid_barrier) {
        if (pctx.rank == 0) {
            info("dumping particles ... (rank 0)");
        }
    } else {
        /*
         * this ensures writes belong to the new epoch will go into a
         * new write buffer
         */
        if (pctx.rank == 0) {
            info("barrier ...");
        }
        start = MPI_Wtime();
        MPI_Reduce(&start, &min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
        if (pctx.rank == 0) {
            dura = MPI_Wtime() - min;
            snprintf(msg, sizeof(msg), "barrier %s", pretty_dura(
                    dura * 1000000).c_str());
            info(msg);

            info("dumping particles ...");
        }
    }

    trace(__func__);
    return(rv);
}

/*
 * closedir
 */
int closedir(DIR *dirp)
{
    double start;
    double min;
    double dura;
    char msg[100];
    int rv;

    rv = pthread_once(&init_once, preload_init);
    if (rv) msg_abort("closedir:pthread_once");

    if (dirp != reinterpret_cast<DIR*>(&fake_dirptr)) {
        return(nxt.closedir(dirp));

    } else {  /* deltafs */

        if (!pctx.paranoid_barrier) {
            if (pctx.rank == 0) {
                info("dumping done (rank 0)");
            }
        } else {
            /* this ensures we have received all incoming writes */
            if (pctx.rank == 0) {
                info("barrier ...");
            }
            start = MPI_Wtime();
            MPI_Reduce(&start, &min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
            if (pctx.rank == 0) {
                dura = MPI_Wtime() - min;
                snprintf(msg, sizeof(msg), "barrier %s", pretty_dura(
                        dura * 1000000).c_str());
                info(msg);

                info("dumping done");
            }
        }

        /* record epoch duration */
        if (!pctx.nomon) {
            mctx.dura = now_micros() - mctx.epoch_start;
        }

        if (pctx.rank == 0) {
            if (!pctx.paranoid_barrier)
                info("epoch ends (rank 0)");
            else
                info("epoch ends");
        }

        trace(__func__);
        return(0);
    }
}

/*
 * fopen
 */
FILE *fopen(const char *fname, const char *mode)
{
    bool exact;
    const char *stripped;
    FILE* rv;

    int ret = pthread_once(&init_once, preload_init);
    if (ret) msg_abort("fopen:pthread_once");

    if (!claim_path(fname, &exact)) {
        return(nxt.fopen(fname, mode));
    } else if (!under_plfsdir(fname)) {
        return(NULL);  /* XXX: support this */
    }

    /* relative paths we pass through; absolute we strip off prefix */

    if (*fname != '/') {
        stripped = fname;
    } else {
        stripped = (exact) ? "/" : (fname + pctx.len_deltafs_root);
    }

    /* allocate a fake FILE* and put it in the set */
    fake_file *ff = new fake_file(stripped);
    rv = reinterpret_cast<FILE*>(ff);

    must_maybelockmutex(&maybe_mtx);
    assert(pctx.isdeltafs != NULL);
    pctx.isdeltafs->insert(rv);
    must_maybeunlock(&maybe_mtx);

    return(rv);
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

    fake_file *ff = reinterpret_cast<fake_file*>(stream);
    size_t cnt = ff->add_data(ptr, size * nitems);

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

    fake_file *ff = reinterpret_cast<fake_file*>(stream);

    if (IS_BYPASS_SHUFFLE(pctx.mode)) {
        /*
         * preload_write() will check if
         *   - BYPASS_DELTAFS_PLFSDIR
         *   - BYPASS_DELTAFS
         */
        rv = mon_preload_write(ff->file_name(), ff->data(), ff->size(),
                num_epochs, &mctx);
        if (rv != 0) {
            if (pctx.verr) {
                error("fclose:preload_write");
            }
        }
    } else {
        /*
         * shuffle_write() will check if
         *   - BYPASS_PLACEMENT
         */
        rv = mon_shuffle_write(ff->file_name(), ff->data(), ff->size(),
                num_epochs, &mctx);
        if (rv != 0) {
            if (pctx.verr) {
                error("fclose:shuffle_write");
            }
        }
    }

    must_maybelockmutex(&maybe_mtx);
    assert(pctx.isdeltafs != NULL);
    pctx.isdeltafs->erase(stream);
    must_maybeunlock(&maybe_mtx);

    delete ff;

    return(rv);
}

/*
 * preload_write
 */
int preload_write(const char *fn, char *data, size_t len)
{
    int rv;
    char path[PATH_MAX];
    ssize_t n;
    int fd;

    /* Return 0 on success, or EOF on errors */

    rv = EOF;

    if (IS_BYPASS_DELTAFS_NAMESPACE(pctx.mode)) {
        if (pctx.plfsh == NULL) {
            msg_abort("plfsdir not opened");
        }

        rv = deltafs_plfsdir_append(pctx.plfsh, fn + pctx.len_plfsdir + 1,
                data, len);

        if (rv != 0) {
            rv = EOF;
        }

    } else if (IS_BYPASS_DELTAFS(pctx.mode)) {
        snprintf(path, sizeof(path), "%s/%s", pctx.local_root, fn);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0666);

        if (fd != -1) {
            n = write(fd, data, len);
            if (n == len) {
                rv = 0;
            }
            close(fd);
        }
    } else {
        if (pctx.plfsfd == -1) {
            msg_abort("plfsdir not opened");
        }

        fd = deltafs_openat(pctx.plfsfd, fn + pctx.len_plfsdir + 1,
                O_WRONLY | O_CREAT | O_APPEND, 0666);

        if (fd != -1) {
            n = deltafs_write(fd, data, len);
            if (n == len) {
                rv = 0;
            }
            deltafs_close(fd);
        }
    }

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
