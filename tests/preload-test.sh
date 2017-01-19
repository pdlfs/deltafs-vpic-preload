#!/bin/sh
#
# Basic test for preload library.
# Arguments are optional. If not provided, we run in cwd with 1 MPI process.
#
# Argument 1: directory containing library and test executable
# Argument 2: number of MPI processes to spawn

BUILD_PREFIX="."
if [ ! -z "$1" ]; then
    BUILD_PREFIX="$1"
fi

MPI_PROCS=1
if [ ! -z "$2" ]; then
    MPI_PROCS=$2
fi

mpirun -np $MPI_PROCS -mca btl ^openib $BUILD_PREFIX/tests/preload-test

if [ $? != 0 ]; then
    echo "Baseline test failed ($?)"
    exit 1
fi

#
# XXX: this assumes a SunOS/linux-style ld.so (won't work on macosx)
#
env LD_PRELOAD=$BUILD_PREFIX/src/libdeltafs-preload.so PDLFS_Testin=1 \
    mpirun -np $MPI_PROCS -mca btl ^openib $BUILD_PREFIX/tests/preload-test

if [ $? != 0 ]; then
    echo "Preload test failed ($?)"
    exit 1
fi

echo "Preload test successful"
exit 0
