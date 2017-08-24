# nexus-runner

The nexus-runner program is an MPI-based multi-instance 3-hop
shuffler test program.   In a shuffle, we distribute send data
across a set of processes using a destination rank number.

## 3 hop shuffle overview

The shuffler uses mercury RPCs to send a message from a SRC process
to a DST process.  Our goal is to reduce per-process memory usage by
reducing the number of output queues and connections to manage on
each node (we are assuming an environment where each node has
multiple CPUs and there is an app with processes running on each
CPU).

As an example, consider Trinity Haswell nodes: there are 10,000 nodes
with 32 cores on each node.   An application running on these nodes
would have 320,000 processes (10,000 nodes * 32 cores/node).  If
all processes communicate with each other, each process will need
memory for ~320,000 output queues!

To reduce the memory usage, we add layers of indirection to the system
(in the form of additional mercury RPC hops).   The result is a 3
hop shuffle:

```
 SRC  ---na+sm--->   SRCREP ---network--->   DSTREP   ---na+sm--->  DST
           1                      2                        3

note: "na+sm" is mercury's shared memory transport, "REP" == representative
```

We divide the job for sending to each remote node among the local
cores.  Furthermore, we only send to one process on each remote node.
We expect the remote receiving process to forward our message to
the final destination (if it isn't the final destination).

Thus, on Trinity, each SRC process has 31 na+sm output queues to
talk to other local processes, and each SRC process has 10,000/32
(~313) network output queues to talk to the remote nodes it is
responsible for.   This is much less than the ~320,000 output queues
needed in the all-to-all case.

A msg from a SRC to a remote DST on node N flows like this:
1. SRC find the local proc responsible for talking to N.  this is
   the SRCREP.   it forward the msg to the SRCREP over na+sm.
2. the SRCREP forwards all messages for node N to one process on
   node N over the network.   this is the DSTREP.
3. the DSTREP receives the message and looks for its na+sm connection
   to the DST (which is also on node N) and sends the msg to DST.
at that point the DST will deliver the msg.   Note that it is
possible to skip hops (e.g. if SRC==SRCREP, the first hop can be
skipped).

The shuffler library manages this three hop communication.  It
has support for batching multiple messages into a larger batch
message (to reduce the overhead of small writes), and it also
supports write-behind buffering (so that the application can
queue data in the buffer and continue, rather than waiting for
the RPC).  If/when the buffers fill, the library provides flow
control to stop the application from overfilling the buffers.

For output queues we provide:
* maxrpc:    the max number of outstanding RPCs we allow at one time.
             additional requests are queued on a wait list until
             something completes.
* buftarget: controls batching of requests... we batch multiple
             requests into a single RPC until we have at least
             "buftarget" bytes in the batch.  setting this value
             small effectively disables batching.

Also for delivery, we have a "deliverq_max" which is the max
number of delivery requests we will buffer before we start
putting additional requests on the waitq (waitq requests are
not ack'd until space is available... this triggers flow control).

Note that we identify endpoints by a global rank number (the
rank number is assigned by MPI... MPI is also used to determine
the topology -- i.e. which ranks are on the local node.  See
deltafs-nexus for details...).

## nexus-runner program

The nexus-runner program should be launched across a set of
one or more nodes using mpirun.  Each process will init the
nexus routing library and the shuffler code.  Processes will
then send the requested number of messages through the 3 hop
shuffler, flush the system, and then exit.

The topology of the run is specified using MPI (i.e. mpirun).
The two variables are the number of nodes to use, and the number
of processes to run on each node.  The configurarion of the
3 hop shuffler is specified using nexus-runner command line
flags.

## command line usage

```
usage: nexus-runner [options] mercury-protocol subnet

options:
        -c count    number of shuffle send ops to perform
        -l          loop through dsts (no random sends)
        -p port     base port number
        -q          quiet mode
        -r n        enable tag suffix with this run number
        -s maxsndr  only ranks <= maxsndr send requests
        -t sec      timeout (alarm), in seconds

shuffler queue config:
        -B bytes    batch buf target for network
        -b bytes    batch buf target for shm
        -d count    delivery queue size limit
        -M count    maxrpcs for network output queues
        -m count    maxrpcs for shm output queues

size related options:
        -i size     input req size (>= 24 if specified)

default payload size is 24.

logging related options (rank <= max can have xtra logging, use -X):
        -C mask      mask cfg for non-extra rank procs
        -E mask      mask cfg for extra rank procs
        -D priority  default log priority
        -F logfile   logfile (rank # will be appended)
        -I n         message buffer size (0=disable)
        -L           enable logging
        -O options   opts (a=alllogs,s=stderr,x=xtra stderr)
        -S priority  print to stderr priority
        -X n         max extra rank#

```

The program prints the current set of options at startup time.
The default count is 5 send ops, and the default timeout is 120
seconds.  The timeout is to prevent the program from hanging forever if
there is a problem with the transport.   Quiet mode can be used
to prevent the program from printing during the RPCs (so that printing
does not impact performance).

By default, destinations are chosen randomly, but if "-l" is used
the program will instead loop through the list of nodes.  "-s" can
be used to limit the number of processes sending requests.  For example,
"-s 0" means that only rank 0 sends requests (the rest just receive
them).

The shuffler queue configuration flags are used to configure the 3
hop shuffler.  There are limits for local shared memory and network
communications.  The batch buf target is the number of output bytes the
shuffler caches before sending out a batch of requests in an RPC.
If the target is 1, then batching is disabled.  If the target is
1MB, then the system will send an RPC once it has received a total
of 1MB of shuffler send requests.  The maxrpcs limits the number of
outstanding mercury RPC operations allowed for a destination.  If
this is 1, then that disables having multiple RPCs pending for a
given output host.  The delivery queue size limit sets the limit
for the number of pending request delivery ops the shuffler will
queue in memory before applying flow control to the delivery process.

The shuffler has extensive logging that can be enabled by setting
the "-L" flag.  The default logging priorities are set by the "-D"
and "-S" flags.  Priority levels are based on syslog(3) and include:
EMERG, ALERT, CRIT, ERR, WARN, NOTE, INFO, and DEBUG.  Debug
messages are divided into 4 streams at the same priority level
(D0, D1, D2, and D3).  Each stream can be selected as desired (or
all debug msgs can be selected with "DEBUG").  Messages are logged
if they at or above the current priority.  Logged messages are also
printed to stderr if they are at or above the stderr priority or
one of the stderr "-O" options is specified.   Log messages can
be saved in memory in a circular message buffer (specify the size
using "-I"), and/or saved in a log file (use "-F" to specify the
filename -- note that the rank of the process will be appended to
the filename).  Note that log data in the memory buffer will get
saved to a core file in the event of a crash (in case you want
to examine the log at the time of the crash).

For the purpose of logging, the processes are divided into two
groups using the "-X rank" flag.  Processes whose rank is less
than or equal to the "-X" value are targeted for extra logging.
Log files ("-F") are only created for the extra logging processes
unless the "-O a" option is specified.  The logging masks for
the processes can be tuned with "-C" and "-E" ... these flags
allow a log mask to be specified by facility using a format like:

SHUF=INFO,UTIL=ERR,CLNT=DEBUG

The current list of facilities available is: SHUF (general messages),
UTIL (utility functions), CLNT (client APIs functions), and DLVR
(delivery thread).

## examples

Run 4 processes on one node.  Only proc 0 sends messages, and it
sends one message to each destination:

```
mpirun -n 4 nexus-runner -c 4 -l -s 0 bmi+tcp 10

```

Above, with full logging (in "/tmp/log.[0-3]"):
```
mpirun -n 4 build/nexus-runner -c 4 -l -s 0 \
	-L -C DEBUG -E DEBUG -O a -F /tmp/log bmi+tcp 10

```

## to compile

First, you need to know where deltafs-nexus and mercury are installed.
You also need cmake.  To compile with a build subdirectory, starting from
the top-level source dir:

```
  mkdir build
  cd build
  cmake -DCMAKE_PREFIX_PATH=/path/to/mercury-install ..
  make
```

That will produce binaries in the current directory.  "make install"
will install the binaries in CMAKE_INSTALL_PREFIX/bin (defaults to
/usr/local/bin) ... add a -DCMAKE_INSTALL_PREFIX=dir to change the
install prefix from /usr/local to something else.
