
#ifdef NDEBUG
#undef NDEBUG    /* we always want to assert */
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mpi.h>

int main(int argc, char **argv) {
    FILE *fp;

    if (argc != 2) {
        fprintf(stderr, "usage: %s test-path\n", *argv);
        exit(1);
    }

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(stderr, "%s: mpi-init failed\n", *argv);
        exit(1);
    }

    unlink(argv[1]);    /* ignore errors here */

    fp = fopen(argv[1], "a");
    assert(fp);

    assert(fwrite("1234", 4, 1, fp) == 1);
    assert(fwrite("5678", 1, 4, fp) == 4);
    assert(fwrite("9", 1, 1, fp) == 1);
    assert(fwrite("0", 1, 1, fp) == 1);
    assert(fwrite("abcdefghijklmnopqrstuv", 1, 22, fp) == 22);

    assert(fclose(fp) == 0);
}
