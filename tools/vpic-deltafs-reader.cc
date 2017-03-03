/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <fcntl.h>
#include <sys/time.h>

#include <map>

#include <deltafs/deltafs_api.h>
#include <pdlfs-common/xxhash.h>
#include <ch-placement.h>

struct ch_placement_instance *ch_inst;
const char *prefixes[] = { "electron_tracer", "ion_tracer", NULL };
char *me;

static void usage(int ret)
{
    printf("\nusage: %s [options] -i input_dir -o output_dir\n\n"
           "  options:\n"
           "    -s size   Consistent-hash ring size\n"
           "    -v factor Consistent-hash virtual factor\n"
           "    -n num    Number of particles to read (from 1 to num)\n"
           "    -h        Print this usage info\n"
           "\n",
           me);

    exit(ret);
}

int deltafs_read_particles(int64_t num, char *indir, char *outdir)
{
    int ret = 0;
    unsigned long rank;
    deltafs_plfsdir_t *dir;
    char *file_data;
    char conf[100];
    char fname[PATH_MAX], wpath[PATH_MAX], rpath[PATH_MAX];
    size_t file_len;
    FILE *fp;

    printf("Reading particles from %s.\n", indir);
    printf("Storing trajectories in %s.\n", outdir);

    if (!ch_inst) {
        fprintf(stderr, "Error: ch-placement instance not initialized\n");
        return 1;
    }

    if (snprintf(rpath, sizeof(rpath), "%s/plfs/particle", indir) <= 0) {
        fprintf(stderr, "Error: snprintf for rpath failed\n");
        return 1;
    }

    for (int64_t i = 1; i <= num; i++) {
        for (int j = 0; prefixes[j] != NULL; j++) {
            /* Determine file name for particle */
            if (snprintf(fname, sizeof(fname), "%s.%016lx", prefixes[j], i) <= 0) {
                fprintf(stderr, "Error: snprintf for fname failed\n");
                return 1;
            }

            ch_placement_find_closest(ch_inst,
                                      pdlfs::xxhash64(fname, strlen(fname), 0),
                                      1, &rank);

            dir = deltafs_plfsdir_create_handle(O_RDONLY);

            if (snprintf(conf, sizeof(conf), "rank=%lu&verify_checksums=true",
                         rank) <= 0) {
                fprintf(stderr, "Error: snprintf for conf failed\n");
                return 1;
            }

            if (deltafs_plfsdir_open(dir, rpath, conf)) {
                perror("Error: cannot open DeltaFS input directory");
                return 1;
            }

            file_data = (char*) deltafs_plfsdir_readall(dir, fname, &file_len);

            if (!file_data) {
                fprintf(stderr, "Error: file_data is NULL\n");
                deltafs_plfsdir_free_handle(dir);
                return 1;
            }

            /* Dump particle trajectory data */
            if (snprintf(wpath, PATH_MAX, "%s/particle%ld.out", outdir, i) <= 0) {
                perror("Error: snprintf for wpath failed");
                goto err;
            }

            if (!(fp = fopen(wpath, "w"))) {
                perror("Error: fopen failed");
                goto err;
            }

            if (fwrite(file_data, 1, file_len, fp) != file_len) {
                perror("Error: fwrite failed");
                fclose(fp);
                goto err;
            }

            //fprintf(stderr, "%s %lu bytes ok\n", fname, (int64_t) file_len);

            free(file_data);
            fclose(fp);
            deltafs_plfsdir_free_handle(dir);
        }
    }

    return 0;

err:
    free(file_data);
    deltafs_plfsdir_free_handle(dir);
    return 1;
}

int main(int argc, char **argv)
{
    int ret, c;
    int64_t num = 1;
    char indir[PATH_MAX], outdir[PATH_MAX];
    struct timeval ts, te;
    int ch_vf = 1024;
    int ch_size = 1;
    char* end;

    me = argv[0];
    indir[0] = outdir[0] = '\0';

    while ((c = getopt(argc, argv, "hi:s:v:n:o:p:")) != -1) {
        switch(c) {
        case 'h': /* print help */
            usage(0);
        case 'i': /* input directory (vpic output) */
            if (!strncpy(indir, optarg, PATH_MAX)) {
                perror("Error: invalid input dir");
                usage(1);
            }
            break;
        case 's': /* ring size */
            ch_size = strtol(optarg, &end, 10);
            if (end[0] != 0) {
                fprintf(stderr, "%s: invalid ring size -- '%s'\n",
                        me, optarg);
                usage(1);
            }
            break;
        case 'v': /* ring virtual factor */
            ch_vf = strtol(optarg, &end, 10);
            if (end[0] != 0) {
                fprintf(stderr, "%s: invalid virtual factor -- '%s'\n",
                        me, optarg);
                usage(1);
            }
            break;
        case 'n': /* number of particles to fetch */
            num = strtoll(optarg, &end, 10);
            if (end[0] != 0) {
                fprintf(stderr, "%s: invalid particle num -- '%s'\n",
                        me, optarg);
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

    if (!(ch_inst = ch_placement_initialize("ring", ch_size, ch_vf, 0))) {
        fprintf(stderr, "Error: cannot init ch-placement");
        exit(1);
    }

    /* Perform the particle queries */
    gettimeofday(&ts, 0);
    ret = deltafs_read_particles(num, indir, outdir);
    gettimeofday(&te, 0);

    printf("Elapsed querying time: %ldms\n", (te.tv_sec-ts.tv_sec)*1000 +
                                             (te.tv_usec-ts.tv_usec)/1000);
    printf("Number of particles queries: %ld\n", num);

    return ret;
}
