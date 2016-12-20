#!/bin/sh -x

msg="1234567890abcdefghijklmnopqrstuv"

./deltafs-preload-test /tmp/pt.$$
got=`cat /tmp/pt.$$`
rm -f /tmp/pt.$$

if [ "$got" != "$msg" ]; then
    echo MISMATCH...
    exit 1
fi

#
# XXX: this assumes a SunOS/linux-style ld.so (won't work on macosx)
#
got=`env LD_PRELOAD=./libdeltafs-preload.so PDLFS_Testin=1 \
	./deltafs-preload-test /tmp/pdlfs/pt.$$`
got=`echo ${got} | sed -e 's/.*FCLOSE/FCLOSE/'`   # dump openmpi msg
want="FCLOSE: /pt.$$ $msg 32"

if [ "$got" != "$want" ]; then
    echo PDLFS MISMATCH... - $got
    exit 1
fi

exit 0
