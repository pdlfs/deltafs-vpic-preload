/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <fcntl.h>

#include <deltafs/deltafs_api.h>
#include <pdlfs-common/xxhash.h>
#include <ch-placement.h>
#include <map>

static const char* prefixes[] = { "electron_tracer", "ion_tracer", NULL };
typedef std::map<int,FILE*> FileMap;

static const char* argv0 = NULL;
static struct ch_placement_instance* ch_inst = NULL;
static const char* ch_type = "ring";
static int ch_vf = 1024;
static int ch_size = 1;
static int ch_seed = 0;

static void usage(int ret)
{
    assert(argv0 != NULL);
    printf("\nusage: %s [options] -i input_dir -o output_dir\n\n"
           "  options:\n"
           "    -s size   Consistent-hash ring size\n"
           "    -v factor Consistent-hash virtual factor\n"
           "    -n num    Number of particles to read (from 1 to num)\n"
           "    -h        Print this usage info\n"
           "\n",
           argv0);

    exit(ret);
}

void close_files(FileMap *out)
{
    FileMap::iterator it;

    for (it = out->begin(); it != out->end(); ++it) {
        if (it->second)
            fclose(it->second);
    }
}

int generate_files(char *outdir, long long int num, FileMap *out)
{
    char fpath[PATH_MAX];
    FileMap::iterator it;

    for (long long int i = 1; i <= num; i++) {
        if (!snprintf(fpath, PATH_MAX, "%s/particle%lld.out", outdir, i)) {
            perror("Error: snprintf failed");
            usage(1);
        }

        if (!((*out)[i] = fopen(fpath, "w"))) {
            perror("Error: fopen failed");
            close_files(out);
            return 1;
        }
    }

    return 0;
}

int deltafs_read_particles(long long int num, char *indir, char *outdir)
{
    int ret;
    unsigned long rank;
    deltafs_plfsdir_t *dir;
    char *file_data;
    char conf[100], msg[100], fname[PATH_MAX];
    size_t file_len;
    FileMap out;

    if (generate_files(outdir, num, &out)) {
        fprintf(stderr, "cannot create particle trajectory files\n");
        return 1;
    }

    assert(ch_inst != NULL);

    ret = 0;

    for (int i = 1; i <= num; i++) {
        for (int j = 0; prefixes[j] != NULL; j++) {
            /* determine file name for particle */
            snprintf(fname, sizeof(fname), "%s.%016lx", prefixes[j], long(i));
            fprintf(stderr, "%s ...\n", fname);
            ch_placement_find_closest(ch_inst,
                    pdlfs::xxhash64(fname, strlen(fname), 0), 1, &rank);
            dir = deltafs_plfsdir_create_handle(O_RDONLY);
            snprintf(conf, sizeof(conf),
                    "rank=%lu&verify_checksums=true", rank);
            ret = deltafs_plfsdir_open(dir, indir, conf);
            if (ret == 0) {
                file_data = (char*) deltafs_plfsdir_readall(dir, fname,
                        &file_len);
                if (file_data != NULL) {
                    /* dump particle trajectory data */
                    if (fwrite(file_data, 1, file_len, out[i]) != file_len) {
                        snprintf(msg, sizeof(msg), "cannot write into output "
                                "particle file #%d", i);
                        perror(msg);
                    } else {
                        fprintf(stderr, "%s %llu bytes ok\n", fname,
                                (unsigned long long) file_len);
                    }
                    free(file_data);
                } else {
                    snprintf(msg, sizeof(msg), "cannot fetch particle #%d", i);
                    perror(msg);
                }
            } else {
                perror("cannot open input directory");
            }

            deltafs_plfsdir_free_handle(dir);
            if (ret != 0) {
                break;
            }
        }
    }

    close_files(&out);
    return 0;
}

int main(int argc, char **argv)
{
    int ret, c;
    long long int num = 1;
    char indir[PATH_MAX], outdir[PATH_MAX];

    argv0 = argv[0];
    indir[0] = outdir[0] = '\0';

    while ((c = getopt(argc, argv, "hi:s:v:n:o:p:")) != -1) {
        switch(c) {
        case 'h': /* print help */
            usage(0);
        case 'i': /* input directory (vpic output) */
            strncpy(indir, optarg, PATH_MAX);
            break;
        case 's': { /* ring size */
            char* end;
            ch_size = strtol(optarg, &end, 10);
            if (end[0] != 0) {
                fprintf(stderr, "%s: invalid ring size -- '%s'\n",
                        argv0, optarg);
                usage(1);
            }
            break;
        }
        case 'v': { /* ring virtual factor */
            char* end;
            ch_vf = strtol(optarg, &end, 10);
            if (end[0] != 0) {
                fprintf(stderr, "%s: invalid virtual factor -- '%s'\n",
                        argv0, optarg);
                usage(1);
            }
            break;
        }
        case 'n': { /* number of particles to fetch */
            char* end;
            num = strtoll(optarg, &end, 10);
            if (end[0] != 0) {
                fprintf(stderr, "%s: invalid particle num -- '%s'\n",
                        argv0, optarg);
                usage(1);
            }
            break;
        }
        case 'o': /* output directory (trajectory files) */
            strncpy(outdir, optarg, PATH_MAX);
            break;
        default:
            usage(1);
        }
    }

    if (!indir[0] || !outdir[0]) {
        fprintf(stderr, "%s: input and output directories are mandatory\n",
                argv0);
        usage(1);
    }

    if (!(ch_inst = ch_placement_initialize(ch_type, ch_size, ch_vf, ch_seed))) {
        fprintf(stderr, "cannot init ch-placement");
        exit(1);
    }

    return deltafs_read_particles(num, indir, outdir);
}
