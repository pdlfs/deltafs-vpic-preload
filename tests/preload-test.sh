#!/bin/sh
#
# Basic function preloading test for shuffle library.
# Arguments are optional. If not provided, we run in cwd with 1 MPI process.
#
# Argument 1: directory containing library and test executable
# Argument 2: number of MPI processes to spawn

DELTAFS_MNTP="particle"

LOCAL_ROOT="/tmp/vpic-deltafs-test"

BUILD_PREFIX="."
if [ ! -z "$1" ]; then
    BUILD_PREFIX="$1"
fi

MPI_PROCS=1
if [ ! -z "$2" ]; then
    MPI_PROCS="$2"
fi

rm -rf /tmp/vpic-deltafs-*

mkdir $LOCAL_ROOT || exit 1

for mpi in openmpi mpich
do
    which mpirun.${mpi}
    if [ $? -eq 0 ]; then
        MPI=$mpi
        break
    fi
done

set -x

if [ x"$MPI" = xmpich ]; then
    mpirun.mpich -np $MPI_PROCS -prepend-rank -bind-to hwthread \
        -env LD_PRELOAD "$BUILD_PREFIX/src/libdeltafs-preload.so" \
        -env PRELOAD_Bypass_deltafs "1" \
        -env PRELOAD_Bypass_shuffle "0" \
        -env PRELOAD_Deltafs_mntp "$DELTAFS_MNTP" \
        -env PRELOAD_Local_root "$LOCAL_ROOT" \
        -env PRELOAD_Testing "1" \
        -env SHUFFLE_Paranoid_checks "1" \
        -env SHUFFLE_Hash_sig "1" \
        $BUILD_PREFIX/tests/preload-test

    RC=$?

elif [ x"$MPI" = xopenmpi ]; then
    mpirun.openmpi -np $MPI_PROCS -tag-output -bind-to-core \
        -x "LD_PRELOAD=$BUILD_PREFIX/src/libdeltafs-preload.so" \
        -x "PRELOAD_Bypass_deltafs=1" \
        -x "PRELOAD_Bypass_shuffle=0" \
        -x "PRELOAD_Deltafs_mntp=$DELTAFS_MNTP" \
        -x "PRELOAD_Local_root=$LOCAL_ROOT" \
        -x "PRELOAD_Testing=1" \
        -x "SHUFFLE_Paranoid_checks=1" \
        -x "SHUFFLE_Hash_sig=1" \
        $BUILD_PREFIX/tests/preload-test

    RC=$?

else

    exit 1
fi

set +x

if [ $RC -ne 0 ]; then
    echo "Preload test failed ($RC)"
    exit 1
else
    echo "Preload test OK"
    head -n 1000 /tmp/vpic-deltafs-run-`id -u`/vpic-deltafs-trace*
    exit 0
fi
