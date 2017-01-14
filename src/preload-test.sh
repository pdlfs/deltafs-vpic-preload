#!/bin/sh
#
# Basic test for preload library.
# Arguments are optional. If not provided, we run in cwd with 1 MPI process.
#
# Argument 1: directory containing library and test executable
# Argument 2: number of MPI processes to spawn

PRELOAD_PREFIX="."
if [ ! -z "$1" ]; then
    PRELOAD_PREFIX="$1/"
fi

MPI_PROCS=1
if [ ! -z "$2" ]; then
    MPI_PROCS=$2
fi

msg="1234567890abcdefghijklmnopqrstuv"

mpirun -np $MPI_PROCS -mca btl ^openib $1/deltafs-preload-test /tmp/pt.$$
got=`cat /tmp/pt.$$`
rm -f /tmp/pt.$$

if [ "$got" != "$msg" ]; then
    echo "Baseline test failed (output: $got)"
    exit 1
fi

#
# XXX: this assumes a SunOS/linux-style ld.so (won't work on macosx)
#
got=`env LD_PRELOAD=$1/libdeltafs-preload.so PDLFS_Testin=1 \
    mpirun -np $MPI_PROCS -mca btl ^openib \
    $1/deltafs-preload-test /tmp/pdlfs/pt.$$`
got=`echo ${got} | sed -e 's/.*FCLOSE/FCLOSE/'`   # dump openmpi msg
want="FCLOSE: /pt.$$ $msg 32"

if [ "$got" != "$want" ]; then
    echo "Preload test failed (output: $got)"
    exit 1
fi

echo "Preload test successful"
exit 0
