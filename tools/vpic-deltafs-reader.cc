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
    printf("\nusage: %s [options] -i input_dir\n\n"
           "  options:\n"
           "    -o dir    Output directory, /dev/null if unspecified\n"
           "    -n num    Number of particles to read (reading ID 1 to num)\n"
           "              If unspecified we explore 10**i range, i in {0, k}\n"
           "              with 10**k approaching total num of particles\n"
           "    -r num    Number of query retries (before averaging, def. 3)\n"
           "    -s size   Consistent-hash ring size\n"
           "    -v factor Consistent-hash virtual factor\n"
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

    //printf("Reading particles from %s.\n", indir);
    //printf("Storing trajectories in %s.\n", outdir);

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

            /* Skip output if outdir is undefined */
            if (!outdir[0]) {
                free(file_data);
                deltafs_plfsdir_free_handle(dir);
                continue;
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

int64_t get_total_particles(char *indir)
{
    int64_t total = 0;
    char infop[PATH_MAX], buf[256];
    FILE *fd;

    if (snprintf(infop, PATH_MAX, "%s/info", indir) <= 0) {
        fprintf(stderr, "Error: snprintf for infop failed\n");
        return 1;
    }

    if (!(fd = fopen(infop, "r"))) {
        perror("Error: fopen failed for info");
        return 1;
    }

    while (fgets(buf, sizeof(buf), fd) != NULL) {
        char *str;

        if ((str = strstr(buf, "total # of particles = ")) != NULL) {
            total = (int64_t) strtof(str + 23, NULL);
            break;
        }
    }

    fclose(fd);
    return total;
}

int query_particles(int64_t retries, int64_t num, char *indir, char *outdir)
{
    int ret = 0;
    struct timeval ts, te;
    int64_t elapsed_sum = 0;

    printf("Querying %ld particles (%ld retries)\n", num, retries);

    for (int64_t i = 1; i <= retries; i++) {
        int64_t elapsed;

        gettimeofday(&ts, 0);
        ret = deltafs_read_particles(num, indir, outdir);
        gettimeofday(&te, 0);

        elapsed = (te.tv_sec-ts.tv_sec)*1000 + (te.tv_usec-ts.tv_usec)/1000;

        printf("(%ld) %ldms / query, %ld ms / particle\n", i, elapsed, elapsed / num);

        elapsed_sum += elapsed;
    }

    printf("Querying results: %ld ms / query, %ld ms / particle\n\n",
           elapsed_sum / retries, elapsed_sum / num / retries);

    return ret;
}

int main(int argc, char **argv)
{
    int ret, c;
    int64_t num = 0, retries = 3, total = 0;
    char indir[PATH_MAX], outdir[PATH_MAX];
    int ch_vf = 1024;
    int ch_size = 1;
    char* end;

    me = argv[0];
    indir[0] = outdir[0] = '\0';

    while ((c = getopt(argc, argv, "hi:n:r:o:s:v:p:")) != -1) {
        switch(c) {
        case 'h': /* print help */
            usage(0);
        case 'i': /* input directory (vpic output) */
            if (!strncpy(indir, optarg, PATH_MAX)) {
                perror("Error: invalid input dir");
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
        case 'r': /* number of query retries */
            retries = strtoll(optarg, &end, 10);
            if (*end) {
                perror("Error: invalid retry argument");
                usage(1);
            }
        case 'o': /* output directory (trajectory files) */
            if (!strncpy(outdir, optarg, PATH_MAX)) {
                perror("Error: invalid output dir");
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
        default:
            usage(1);
        }
    }

    if (!indir[0]) {
        fprintf(stderr, "Error: input directory unspecified\n");
        usage(1);
    }

    if (!(ch_inst = ch_placement_initialize("ring", ch_size, ch_vf, 0))) {
        fprintf(stderr, "Error: cannot init ch-placement");
        exit(1);
    }

    /* Get total number of particles */
    total = get_total_particles(indir);
    if (!total) {
        fprintf(stderr, "Error: failed to read the total number of particles\n");
        return 1;
    }

    printf("\nNumber of particles: %ld\n", total);
    /* XXX: The following is only until we figure out caching */
    if (total > 10e5) {
        total = 10e5;
        printf("Warning: will stop querying at 10K particles\n");
    }
    printf("\n");

    /*
     * Go through the query dance: increment num from 1 to total particles
     * multiplying by 10 each time, unless num is specified.
     */
    if (num) {
        ret = query_particles(retries, num, indir, outdir);
    } else {
        num = 1;

        while (num <= total) {
            ret = query_particles(retries, num, indir, outdir);
            if (ret)
                return ret;

            num *= 10;
        }
    }

    return ret;
}
