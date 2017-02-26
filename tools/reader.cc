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
#include <dirent.h>
#include <fcntl.h>

#include <deltafs/deltafs_api.h>
#include <pdlfs-common/xxhash.h>
#include <ch-placement.h>
#include <map>
#include <list>

typedef std::map<int,FILE*> FileMap;
typedef std::list<int> EpochList;

static const char* argv0 = NULL;
static struct ch_placement_instance* ch_inst = NULL;
static const char* ch_type = "ring";
static int ch_size = 4;
static int ch_vf = 1024;
static int ch_seed = 0;

void usage(int ret)
{
    printf("\n"
           "usage: %s [options] -i input_dir -o output_dir\n"
           "  options:\n"
           "    -d        Run in DeltaFS mode\n"
           "    -n num    Number of particles to read (reading ID 1 to num)\n"
           "    -h        This usage info\n"
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
        if (!snprintf(fpath, PATH_MAX, "%s/particle%lld.txt", outdir, i)) {
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

    for (int i = 1; ret == 0 && i <= num; i++) {
        /* determine file name for particle */
        snprintf(fname, sizeof(fname), "eparticle.%016lx", long(i));
        fprintf(stderr, "%s ...\n", fname);
        ch_placement_find_closest(ch_inst,
                pdlfs::xxhash64(fname, strlen(fname), 0), 1, &rank);
        dir = deltafs_plfsdir_create_handle(O_RDONLY);
        snprintf(conf, sizeof(conf), "rank=%lu&verify_checksums=true", rank);
        ret = deltafs_plfsdir_open(dir, indir, conf);
        if (ret == 0) {
            file_data = (char*) deltafs_plfsdir_readall(dir, fname, &file_len);
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
    }

    close_files(&out);
    return 0;
}

int process_epoch(char *ppath, int it, FileMap out)
{
    /* Open each per-process file and process it */

    /* Verify headers */

    /* Check whether particle data is found, stop if so (not for debugging) */

    /* TODO */
    return 0;
}

int read_particles(long long int num, char *indir, char *outdir)
{
    DIR *in;
    struct dirent *dp;
    char ppath[PATH_MAX];
    EpochList epochs;
    EpochList::iterator it;
    FileMap out;

    printf("Reading particles from %s.\n", indir);
    printf("Storing trajectories in %s.\n", outdir);

    /* Open particle directory and sort epoch directories */
    if (!snprintf(ppath, PATH_MAX, "%s/particle", indir)) {
        perror("Error: snprintf failed");
        usage(1);
    }

    if ((in = opendir(ppath)) == NULL) {
        perror("Error: cannot open input directory");
        usage(1);
    }

    while (dp = readdir(in)) {
        int epoch;
        char *end;

        if (dp->d_type != DT_DIR)
            continue;

        if (!strcmp(dp->d_name, ".") ||
            !strcmp(dp->d_name, "..") ||
            dp->d_name[0] != 'T')
            continue;

        epoch = strtoll(dp->d_name+2, &end, 10);
        if (*end) {
            perror("Error: strtoll failed");
            closedir(in);
            usage(1);
        }

        //printf("Found subdir %s, epoch %d.\n", dp->d_name, epoch);

        epochs.push_back(epoch);
    }

    closedir(in);
    epochs.sort();

    /* Iterate through epoch frames */
    if (generate_files(outdir, num, &out)) {
        fprintf(stderr, "Error: particle trajectory file creation failed\n");
        return 1;
    }

    for (it = epochs.begin(); it != epochs.end(); ++it) {
        printf("Processing epoch %d.\n", *it);
        if (process_epoch(ppath, *it, out)) {
            fprintf(stderr, "Error: epoch data processing failed\n");
            close_files(&out);
            return 1;
        }
    }

    close_files(&out);
    return 0;
}

int main(int argc, char **argv)
{
    int ret, c, d = 0;
    long long int num = 1;
    char indir[PATH_MAX], outdir[PATH_MAX];

    argv0 = argv[0];
    indir[0] = outdir[0] = '\0';

    while ((c = getopt(argc, argv, "dhi:n:o:p:")) != -1) {
        switch(c) {
        case 'd': /* run in DeltaFS mode */
            d = 1;
            break;
        case 'h': /* print help */
            usage(0);
        case 'i': /* input directory (VPIC output) */
            if (!strncpy(indir, optarg, PATH_MAX)) {
                perror("Error: invalid input dir");
                usage(1);
            }
            break;
        case 'n': /* number of particles to fetch */
            char *end;
            num = strtoll(optarg, &end, 10);
            if (*end) {
                perror("Error: invalid num argument");
                usage(1);
            }
            break;
        case 'o': /* output directory (trajectory files) */
            if (!strncpy(outdir, optarg, PATH_MAX)) {
                perror("Error: invalid output dir");
                usage(1);
            }
            break;
        default:
            usage(1);
        }
    }

    if (!indir[0] || !outdir[0]) {
        fprintf(stderr, "Error: input and output directories are mandatory\n");
        usage(1);
    }

    /* retrieve particles */

    if (d) {
        ch_inst = ch_placement_initialize(ch_type, ch_size, ch_vf, ch_seed);
        if (!ch_inst) {
            fprintf(stderr, "cannot init ch-placement");
            exit(1);
        }

        ret = deltafs_read_particles(num, indir, outdir);
    } else {
        ret = read_particles(num, indir, outdir);
    }

    return ret;
}
