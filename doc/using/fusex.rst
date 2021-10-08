.. highlight:: rst

eosxd
=====

eosxd log file
--------------

eosxd writes a log file into the fusex log direcotory ```/var/log/eos/fusex/fuse.<instancename>-<mountdir>.log```. The default verbosity is **warning** level.

eosxd statistics file
----------------------

eosxd writes out a statistics file with an update rate of 1Hz into the fusex log directory ```/var/log/eos/fusex/fuse.<instancename>-<mountdir>.stats```.


Here is an example: 

.. code-block:: bash


   ALL     Execution Time                   5.06 +- 16.69 = 5.01s (1270 ops)
   # -----------------------------------------------------------------------------------------------------------------------
   who     command                          sum             5s     1min     5min       1h exec(ms) +- sigma(ms)  = cumul(s)  
   # -----------------------------------------------------------------------------------------------------------------------
   ALL     :sum                                     1271     0.00     0.05     0.01     0.00     -NA- +- -NA-       = 0.00      
   ALL     access                                      4     0.00     0.00     0.00     0.00  1.82825 +- 1.64279    = 0.01      
   ALL     create                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     flush                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     forget                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     fsync                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     getattr                                    17     0.00     0.02     0.00     0.00  1.91859 +- 6.93590    = 0.03      
   ALL     getxattr                                   58     0.00     0.03     0.01     0.00  2.42547 +- 18.15372   = 0.14      
   ALL     link                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     listxattr                                   0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     lookup                                    342     0.00     0.00     0.00     0.00  0.78381 +- 3.70048    = 0.27      
   ALL     mkdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     mknod                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     open                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     opendir                                   215     0.00     0.00     0.00     0.00 20.56853 +- 26.64452   = 4.42      
   ALL     read                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     readdir                                   416     0.00     0.00     0.00     0.00  0.05781 +- 0.07550    = 0.02      
   ALL     readlink                                    1     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     release                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     releasedir                                215     0.00     0.00     0.00     0.00  0.00896 +- 0.00425    = 0.00      
   ALL     removexattr                                 0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     rename                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     rm                                          0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     rmdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     setattr                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     setattr:chmod                               0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     setattr:chown                               0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     setattr:truncate                            0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     setattr:utimes                              0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     setxattr                                    1     0.00     0.00     0.00     0.00  0.08500 +- -NA-       = 0.00      
   ALL     statfs                                      2     0.00     0.00     0.00     0.00 57.74450 +- 48.80550   = 0.12      
   ALL     symlink                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     unlink                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   ALL     write                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00      
   # -----------------------------------------------------------------------------------------------------------
   ALL        inodes              := 375
   ALL        inodes stack        := 0
   ALL        inodes-todelete     := 0
   ALL        inodes-backlog      := 0
   ALL        inodes-ever         := 3051
   ALL        inodes-ever-deleted := 0
   ALL        inodes-open         := 0
   ALL        inodes-vmap         := 3051
   ALL        inodes-caps         := 1
   # -----------------------------------------------------------------------------------------------------------
   ALL        threads             := 32
   ALL        visze               := 517.10 Mb
   ALL        rss                 := 35.63 Mb
   ALL        pid                 := 1689
   ALL        log-size            := 409384
   ALL        wr-buf-inflight     := 0 b
   ALL        wr-buf-queued       := 0 b
   ALL        wr-nobuff           := 0
   ALL        ra-buf-inflight     := 0 b
   ALL        ra-buf-queued       := 0 b
   ALL        ra-xoff             := 0
   ALL        ra-nobuff           := 0
   ALL        rd-buf-inflight     := 0 b
   ALL        rd-buf-queued       := 0 b
   ALL        version             := 4.4.17
   ALL        fuseversion         := 28
   ALL        starttime           := 1549548272
   ALL        uptime              := 66989
   ALL        total-mem           := 8201658368
   ALL        free-mem            := 149671936
   ALL        load                := 1313970496
   ALL        total-rbytes        := 0
   ALL        total-wbytes        := 0
   ALL        total-io-ops        := 1270
   ALL        read--mb/s          := 0.00
   ALL        write-mb/s          := 0.00
   ALL        iops                := 0
   ALL        xoffs               := 0
   ALL        instance-url        := myhost.cern.ch:1094
   ALL        client-uuid         := 4af8154c-2ae1-11e9-8e32-02163e009ce2
   ALL        server-version      := 4.4.17
   ALL        automounted         := 0
   ALL        max-inode-lock-ms   := 0.00
   # -----------------------------------------------------------------------------------------------------------


The first block contains global averages/sums for total IO time and IO operations:

.. epigraph::

   ======= ================================ =============   ================ ===========
   tag     description                      avg/dev in ms   cumulative time  sum IOPS
   ======= ================================ =============   ================ ===========
   ALL     Execution Time                   4.80 +- 15.56   4.87s            (1267 ops)
   ======= ================================ =============   ================ ===========

The second block contains counts for each filesystem operation the average rates in a 5s,1min,5min and 1h window, the average execution time and standard deviation for a given filesystem operation and cumulative seconds spent in each operation.


.. epigraph::

   ======= ================================ =============== ====== ======= =========== ====== ======= ============ =============
   who     filesystem counter name          sum of ops      5s avg 1m avg   5m avg     1h avg avg(ms) siggma(ms)   cumulative(s)
   ======= ================================ =============== ====== ======= =========== ====== ======= ============ =============
   ALL     :sum                             1268            0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     access                           4               0.00   0.00    0.00        0.00   1.82825 +- 1.64279   0.01      
   ALL     create                           0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     flush                            0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     forget                           0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     fsync                            0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     getattr                          16              0.00   0.00    0.00        0.00   2.01987 +- 7.13716   0.03      
   ALL     getxattr                         56              0.00   0.00    0.00        0.00   0.02023 +- 0.00463   0.00      
   ALL     link                             0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     listxattr                        0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     lookup                           342             0.00   0.00    0.00        0.00   0.78381 +- 3.70048   0.27      
   ALL     mkdir                            0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     mknod                            0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     open                             0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     opendir                          215             0.00   0.00    0.00        0.00   20.5685 +- 26.64452  4.42      
   ALL     read                             0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     readdir                          416             0.00   0.00    0.00        0.00   0.05781 +- 0.07550   0.02      
   ALL     readlink                         1               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     release                          0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     releasedir                       215             0.00   0.00    0.00        0.00   0.00896 +- 0.00425   0.00      
   ALL     removexattr                      0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     rename                           0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     rm                               0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     rmdir                            0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     setattr                          0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     setattr:chmod                    0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     setattr:chown                    0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     setattr:truncate                 0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     setattr:utimes                   0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     setxattr                         1               0.00   0.00    0.00        0.00   0.08500 +- -NA-      0.00      
   ALL     statfs                           2               0.00   0.00    0.00        0.00   57.7450 +- 48.80550  0.12      
   ALL     symlink                          0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     unlink                           0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ALL     write                            0               0.00   0.00    0.00        0.00   -NA-    +- -NA-      0.00      
   ======= ================================ =============== ====== ======= =========== ====== ======= ============ =============

The third block displays inode related counts, which are explained inline.


.. epigraph::

   ========== ====================== =============== ===========================================================================
   who        counter name           value           description
   ========== ====================== =============== ===========================================================================
   ALL        inodes                 375             currently in-memory known-inodes
   ALL        inodes stack           0               inodes which could be forgotten, but needed to be kept on the stack
   ALL        inodes-todelete        0               inodes which still have to be deleted upstream
   ALL        inodes-backlog         0               inodes which still have to be updated upstream
   ALL        inodes-ever            3051            inodes ever seen by this mount
   ALL        inodes-ever-deleted    0               inodes ever deleted by this mount
   ALL        inodes-open            0               inodes associated with an open file descriptor
   ALL        inodes-vmap            3051            size of logical inode translation map
   ALL        inodes-caps            0               inodes with a cache-callback subscription
   ALL        threads                32              currently running threads 
   ALL        visze                  517.10 Mb       virtual memory used by the running daemon
   ALL        rss                    35.13 Mb        resident memory used by the runnig daemon
   ALL        pid                    1689            process id of the running daemon
   ALL        log-size               367632          size of the logfile of the running daemon
   ALL        wr-buf-inflight        0 b             write buffer allocated with data in-flight in writing
   ALL        wr-buf-queued          0 b             write buffer allocated and kept on the queue for future reuse in writing
   ALL        wr-nobuff              0               counter how often a 'no available buffer' condition was hit in writing
   ALL        ra-buf-inflight        0 b             read-ahead buffer allocated with data in-flight in read-ahead
   ALL        ra-buf-queued          0 b             read-ahead buffer allocated and kept on the queue for future reuse in ra
   ALL        ra-xoff                0               counter how often we needed to wait for an available read-ahead buffer
   ALL        ra-nobuff              0               counter how often a 'no available buffer' condition was hit in read-ahead
   ALL        rd-buf-inflight        0 b             read buffer allocated with data in-flight for reading
   ALL        rd-buf-queued          0 b             read buffer allocated and kept on the queue for future reuse in reading
   ALL        version                4.4.17          current version of the daemon
   ALl        fuseversion            28              current version of the FUSE protocol
   ALL        starttime              1549548272      starttime as unixtimestamp
   ALL        uptime                 64772           run time of the daemon in seconds
   ALL        total-mem              8201658368      total memory of the hosting machine
   ALL        free-mem               153280512       free memory of the hosting machine
   ALL        load                   1313946976      1 minute load avg as returned by sysinfo
   ALL        total-rbytes           0               total number of bytes read on this mount
   ALL        total-wbytes           0               total number of bytes written on this mount
   ALL        total-io-ops           1267            total number of io operations done on this mount
   ALL        read--mb/s             0.00            1 minute average read rate in MB/s
   ALL        write-mb/s             0.00            1 minute average write rate in MB/s
   ALL        iops                   0               1 minute average io ops rate
   ALL        xoffs                  0               counter how often we needed to wait for an available write buffer
   ALL        instance-url           myhost:1094     hostname and port of the upstream EOS instance
   ALL        client-uuid            4af8154c.....   unique identifier of this client (UUID)
   ALL        server-version         4.4.17          server version where this client is connected
   ALL        automounted            0               indicates if the mount is done via autofs
   ALL        max-inode-lock-ms      0.00            maximum time any thread in the thread pool is stuck in ms
   ========== ====================== =============== ===========================================================================

The statistics file can be printed by any user on request by running:

.. code-block:: bash

   eosxd get eos.stats <mount-point>
  
The statistics file counter can be reset by running as root:

.. code-block:: bash

   eosxd set system.eos.resetstat - /eos/

Server Side Configuration
-------------------------

The **eosxd** network provides four configuration parameters, which can be shown or modified using **eos fusex conf**

.. code-block:: bash

   [root@eos ]# eos fusex conf
   info: configured FUSEX broadcast max. client audience 256 listeners
   info: configured FUESX broadcast audience to suppress match is '@b[67]'
   info: configured FUSEX heartbeat interval is 10 seconds
   info: configured FUSEX quota check interval is 10 seconds

The default heartbeat interval is 10 seconds. It is the interval each **eosxd** process sends a heartbeat message to the MGM server. The quota check interval is the interval after which the MGM FuseServer checks again if a **eosxd** client went out of quota or back to quota. The default is also 10 seconds. 

When working with thousands of clients within a single directory the amount of messages in the FuseServer broadcast network can overwhelm the MGM messaging capacity. To reduce the amount of messages sent around while files are open and written, a threshold can be defined after which a certain audience of clients will not receive anymore meta-data update or forced refresh messages. If 1000 clients write 1000 files within a single directory the message rate is 100kHz for file-size updates while the clients are writing. In the example above if a message hits more than 256 listeners and the client names start with b6 or b6 messages will be suppressed. Messages emitted when files are created or commmitted are not suppressed!


Namespace Configuration
-----------------------

By default each client sends his desired leastime for directory subscriptions (300s default at time of writing). For certain directories in the hierarchy which are essentially read-only it improves the overall performance to define a longer leasetime. In a home directory hierarchy like **/eos/user/f/foo** the first three directory level could have a longer lease time defined.

.. code-block:: bash

   [root@eos ]# eos attr set sys.forced.leasetime=86400 /eos/
   [root@eos ]# eos attr set sys.forced.leasetime=86400 /eos/user/
   [root@eos ]# eos attr set sys.forced.leasetime=86400 /eos/user/f
   
