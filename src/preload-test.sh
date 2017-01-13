#!/bin/sh

PRELOAD_PREFIX="."
if [ ! -z "$1" ]; then
    PRELOAD_PREFIX="$1/"
fi

msg="1234567890abcdefghijklmnopqrstuv"

$1/deltafs-preload-test /tmp/pt.$$
got=`cat /tmp/pt.$$`
rm -f /tmp/pt.$$

if [ "$got" != "$msg" ]; then
    echo MISMATCH...
    exit 1
fi

#
# XXX: this assumes a SunOS/linux-style ld.so (won't work on macosx)
#
got=`env LD_PRELOAD=$1/libdeltafs-preload.so PDLFS_Testin=1 \
	$1/deltafs-preload-test /tmp/pdlfs/pt.$$`
got=`echo ${got} | sed -e 's/.*FCLOSE/FCLOSE/'`   # dump openmpi msg
want="FCLOSE: /pt.$$ $msg 32"

if [ "$got" != "$want" ]; then
    echo "Preload test failed (error $got)"
    exit 1
fi

echo "Preload test successful"
exit 0
