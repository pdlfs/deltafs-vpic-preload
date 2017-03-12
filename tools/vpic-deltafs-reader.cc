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
#include <mpi.h>
#include <sys/stat.h>

#include <map>

#include <deltafs/deltafs_api.h>
#include <pdlfs-common/xxhash.h>
#include <ch-placement.h>

struct ch_placement_instance *ch_inst;
char *me;
int rank, worldsz;

/* Name file state */
FILE *nf = NULL;
int core = 0;
char pname[20];
int64_t rank_num = 0;
int64_t num = 0;

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

int get_particle_name(char *indir)
{
    char nfpath[PATH_MAX];

try_again:
    /* Get next 19B particle name */
    if (fread(pname, sizeof(char), 19, nf) != 19) {
        if (feof(nf)) {
            fclose(nf);
            core++;
            goto next_open;
        }

        perror("Error: name file fread failed");
        goto err;
    }

    return 0;

next_open:
    if (snprintf(nfpath, PATH_MAX, "%s/names/names.%d", indir, core) <= 0) {
        fprintf(stderr, "Error: snprintf for nfpath failed\n");
        return 1;
    }

    if (!(nf = fopen(nfpath, "rb"))) {
        perror("Error: cannot open name file");
        return 1;
    }

    goto try_again;

err:
    fclose(nf);
    return 1;
}

int deltafs_read_particles(char *indir, char *outdir)
{
    FILE *fp;
    unsigned long chrank;
    deltafs_plfsdir_t *dir;
    char *file_data;
    char rpath[PATH_MAX], wpath[PATH_MAX];
    char conf[100];
    size_t file_len;

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

    for (int64_t i = 1; i <= rank_num; i++) {
        /* Get a particle name to search for */
        if (get_particle_name(indir)) {
            fprintf(stderr, "Error: get_particle_name failed");
            return 1;
        }

        ch_placement_find_closest(ch_inst, pdlfs::xxhash64(pname, 19, 0),
                                  1, &chrank);

        dir = deltafs_plfsdir_create_handle(O_RDONLY);

        if (snprintf(conf, sizeof(conf), "rank=%lu&verify_checksums=true",
                     chrank) <= 0) {
            fprintf(stderr, "Error: snprintf for conf failed\n");
            goto err_dir;
        }

        if (deltafs_plfsdir_open(dir, rpath, conf)) {
            perror("Error: cannot open DeltaFS input directory");
            goto err_dir;
        }

        file_data = (char*) deltafs_plfsdir_readall(dir, pname, &file_len);
        if (!file_data) {
            fprintf(stderr, "Error: file_data is NULL\n");
            goto err_dir;
        }

        printf("(%d) Found %s\n", rank, pname);

        /* Skip output if outdir is undefined */
        if (!outdir[0])
            goto done;

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

        fclose(fp);
done:
        free(file_data);
        deltafs_plfsdir_free_handle(dir);
    }

    return 0;

err:
    free(file_data);
err_dir:
    deltafs_plfsdir_free_handle(dir);
    return 1;
}

/*
 * Picks the name file we should start from and sets the
 * nf, core, and rank_num variables.
 */
int init_nf_data(char *indir)
{
    char nfpath[PATH_MAX];
    int64_t filesz = 0;
    struct stat s;
    int64_t rank_offt = 0;

    /*
     * Calculate our share of the particles,
     * and how many we need to skip until the first.
     * We assume total is divisible by worldsz.
     */
    rank_num = num / worldsz;
    if (rank < num % worldsz)
        rank_num++;
    rank_offt = rank * rank_num * 19;

    printf("Rank %d: Querying %ld particles\n", rank, rank_num);

    /* Go over name files until we find the one we should start with */
    while (rank_offt >= filesz) {
        rank_offt -= filesz;

        if (snprintf(nfpath, PATH_MAX, "%s/names/names.%d", indir, core) <= 0) {
            fprintf(stderr, "Error: snprintf for nfpath failed\n");
            return 1;
        }

        if (stat(nfpath, &s)) {
            fprintf(stderr, "Error: stat failed");
            return 1;
        }

        filesz = (int64_t) s.st_size;
        core++;
    }

    /* Open the file we picked, move it to the right offset */
    if (!(nf = fopen(nfpath, "rb"))) {
        perror("Error: fopen nfpath failed");
        return 1;
    }

    if (fseek(nf, rank_offt, SEEK_CUR)) {
        perror("Error: fseek on nf failed");
        fclose(nf);
        return 1;
    }

    return 0;
}

int query_particles(int64_t retries, char *indir, char *outdir)
{
    int ret = 0;
    struct timeval ts, te;
    int64_t elapsed_sum = 0, max_elapsed_avg = 0;

    if (init_nf_data(indir))
        return 1;

    if (rank == 0)
        printf("Querying %ld particles (%ld retries)\n", num, retries);

    for (int64_t i = 1; i <= retries; i++) {
        int64_t elapsed, max_elapsed;
        int64_t *elapsed_all;

        if (rank == 0 &&
            !(elapsed_all = (int64_t *)malloc(sizeof(int64_t) * worldsz))) {
            perror("Error: malloc failed");
            exit(1);
        }

        gettimeofday(&ts, 0);
        ret = deltafs_read_particles(indir, outdir);
        gettimeofday(&te, 0);

        elapsed = (te.tv_sec-ts.tv_sec)*1000 + (te.tv_usec-ts.tv_usec)/1000;

        MPI_Gather(&elapsed, 1, MPI_LONG_LONG_INT, elapsed_all, 1,
                   MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD);

        //printf("(Rank %d, %ld) %ldms / query, %ld ms / particle\n",
        //       rank, i, elapsed, elapsed / rank_num);
        if (rank == 0) {
            elapsed = max_elapsed = 0;
            for (int j = 0; j < worldsz; j++) {
                elapsed += elapsed_all[j];
                if (max_elapsed < elapsed_all[j])
                    max_elapsed = elapsed_all[j];
            }
            elapsed /= worldsz;
            printf("Overall: %ldms / query, %ld ms / particle\n",
                   max_elapsed, elapsed / num);
            free(elapsed_all);
        }

        elapsed_sum += elapsed;
        max_elapsed_avg += max_elapsed;
    }

    if (rank == 0)
        printf("Querying results: %ld ms / query, %ld ms / particle\n\n",
               elapsed_sum / retries, elapsed_sum / num / retries);

    return ret;
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

int main(int argc, char **argv)
{
    int ret, c;
    int64_t retries = 3, total = 0;
    char indir[PATH_MAX], outdir[PATH_MAX];
    int ch_vf = 1024;
    int ch_size = 1;
    char* end;

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(stderr, "Error: MPI_Init failed\n");
        return 1;
    }

    MPI_Comm_size(MPI_COMM_WORLD, &worldsz);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    me = argv[0];
    indir[0] = outdir[0] = pname[19] = '\0';

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

    if (rank == 0)
        printf("\nNumber of particles: %ld\n", total);
    /* XXX: The following is only until we figure out caching */
    if (total > 1e5) {
        total = 1e5;
        if (rank == 0)
            printf("Warning: will stop querying at 10K particles\n");
    }
    if (rank == 0)
        printf("\n");

    /*
     * Go through the query dance: increment num from 1 to total particles
     * multiplying by 10 each time, unless num is specified.
     */
    if (num) {
        ret = query_particles(retries, indir, outdir);
    } else {
        num = 1;

        while (num <= total) {
            ret = query_particles(retries, indir, outdir);
            if (ret)
                return ret;

            num *= 10;
        }
    }

    MPI_Finalize();

    return ret;
}
