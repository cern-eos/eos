.. index::
   single: Interfaces

.. highlight:: rst

.. _interfaces:

Interfaces
===========


.. index::
   pair: Interfaces; Namespace
   pair: CLI; eos ns

Namespace Interface
---------------------

The namespace interface `eos ns` has the main purpose to provide status and performance information about the namespace.

It provides optional sub-commands `[stat|mutex|compact|master|cache]` :

* `eos ns stat` - adds namespace rate, counter and execution time measurements for individual functions executed in the namespace
* `eos ns mutex` - allows to display or modify mutex contention measurements, deadlock and order violation detection
* `eos ns compact` - this command is deprecated since EOS5 and has no functionality anymore
* `eos ns master` - allows to inspect the logging of an active/passive MGM and state transitions
* `eos ns cache` - allows to change the in-memory cache settings for files and directories

.. index::
   pair: Namespace; Display

4.1.1 Display
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here you see a typical output of an `eos ns` command:

 .. code-block:: bash

    # ------------------------------------------------------------------------------------
    # Namespace Statistics
    # ------------------------------------------------------------------------------------
    # Namespace Statistics
    # ------------------------------------------------------------------------------------
    ALL      Files                            100000 [booted] (0s)
    ALL      Directories                      66637
    ALL      Total boot time                  0 s
    ALL      Contention                       write: 0.00 % read:0.00 %
    # ------------------------------------------------------------------------------------
    ALL      Replication                      is_master=true master_id=mgm.cern.ch:1094
    # ------------------------------------------------------------------------------------
    ALL      files created since boot         6
    ALL      container created since boot     0
    # ------------------------------------------------------------------------------------
    ALL      current file id                  22
    ALL      current container id             322347
    # ------------------------------------------------------------------------------------
    ALL      eosxd caps                       0 c: 0 cc: 0 cic: 0 ic: 0
    ALL      eosxd clients                    1
    ALL      eosxd active clients             0
    ALL      eosxd locked clients             0
    # ------------------------------------------------------------------------------------
    ALL      File cache max num               40000000
    ALL      File cache occupancy             13
    ALL      In-flight FileMD                 0
    ALL      Container cache max num          5000000
    ALL      Container cache occupancy        16
    ALL      In-flight ContainerMD            0
    # ------------------------------------------------------------------------------------
    ALL      eosViewRWMutex peak-latency      0ms (last) 0ms (1 min) 0ms (2 min) 0ms (5 min)
    # ------------------------------------------------------------------------------------
    ALL      QClient overall RTT              0ms (min)  0ms (avg)  18ms (max)
    ALL      QClient recent peak RTT          1ms (1 min) 1ms (2 min) 1ms (5 min)
    # ------------------------------------------------------------------------------------
    ALL      memory virtual                   3.75 GB
    ALL      memory resident                  195.15 MB
    ALL      memory share                     48.03 MB
    ALL      memory growths                   1.48 GB
    ALL      threads                          379
    ALL      fds                              428
    ALL      uptime                           59205
    # ------------------------------------------------------------------------------------
    ALL      drain info                       pool=drain          min=10  max=100  size=10   queue_sz=0
    ALL      fsck info                        pool=fsck           min=2   max=20   size=2    queue_sz=0
    ALL      converter info                   pool=converter      min=8   max=100  size=8    queue_sz=0
    ALL      balancer info                    space=default
    # ------------------------------------------------------------------------------------
    ALL      tracker info                     tracker=balance size=413
    ALL      tracker info                     tracker=convert size=0
    ALL      tracker info                     tracker=drain size=22
    # ------------------------------------------------------------------------------------


We will now discuss every section and their meaning.

.. index::
   pair: Namespace; Statistics
   pair: Namespace; Contention

**Main Section**
 .. code-block:: bash

    # ------------------------------------------------------------------------------------
    ALL      Files                            100000 [booted] (0s)
    ALL      Directories                      66637
    ALL      Total boot time                  0 s
    ALL      Contention                       write: 0.00 % read:0.00 %
    # ------------------------------------------------------------------------------------

**Files**

The `Files` line shows there are 100k files in this namespace and the namespace is up (booted). The time in brackets is the time it took to bring the file namespace up. The part of the boot procedure bringing up the view on all files contains only a very short safety check to see if the stored next free file identifier is correct e.g. there are no higher file ids stored in the namespace already. It is important that the the next free file identifier is correct because otherwise there is a risk to overwrite existing files when new files are created.

**Directories**

The `Directories` line shows the number of directories currently stored in the namespace.

**Total boot time**
Total boot time is the total time to get the namespace into operational state (0s).

**Contention**
Contention expresses the ratio between wait time to obtain the namespace lock for `read` or `write` and the usage time of the namespace lock for `read` or `write` in percent. 100% means, that the average wait time to obtain a mutex is identical to the average time it is used afterwards.

**Replication Section**
 .. code-block:: bash

    # ------------------------------------------------------------------------------------
    ALL      Replication                      is_master=true master_id=mgm.cern.ch:1094
    # ------------------------------------------------------------------------------------

This fields describe if this MGM is currently the master `is_master=true` and output the URL of the currently active MGM node `master_id=mgm.cern.ch:1094`

**Creation Section**
 .. code-block:: bash

    # ------------------------------------------------------------------------------------
    ALL      files created since boot         6
    ALL      container created since boot     0
    # ------------------------------------------------------------------------------------
    ALL      current file id                  22
    ALL      current container id             322347
    # ------------------------------------------------------------------------------------

These fields are self-explaining: number of files and container (directories) created since the MGM started up. The current IDs are the internal IDs for the next created file/directory. With every creation these number is incremented. IDs are never re-used!

.. index::
   pair: Namespace; FUSE Statistics

**FUSE section**
 .. code-block:: bash

    # ------------------------------------------------------------------------------------
    ALL      eosxd caps                       0 c: 0 cc: 0 cic: 0 ic: 0
    ALL      eosxd clients                    1
    ALL      eosxd active clients             0
    ALL      eosxd locked clients             0
    # ------------------------------------------------------------------------------------

These section describes the situation of FUSE client access.
*eosxd caps*

The first line `eosxd caps` is low-level information, which you can ignore in most cases. A `cap` is a per-inode subscription to receive callbacks for remote changes. E.g. if client A appends to file X and client B is using the same file, it needs to receive an update about the changes on file X. The first number (0) is the total number of indodes have currently a subscription. The `c` field is the size of the lookup table to find all caps for a given client (this should be equivalent to the number of clients having any `cap`). The `cic` field is the size of the lookup table to find all inodes with a `cap` for a given client (this should be equivalent to the number of clients having any `cap`). The `ic` field is the size of the lookup table from `inode` to `cap` (this should be equivalent to the number of inodes having a `cap`).

*eosxd clients*

This is the total number of FUSE clients, which are currently connected to this MGM.

*eosxd active clients*

This is the total number of FUSE clients, which were active during the last 5 minutes.

*eosxd locked clients*

This is the total number of FUSE clients, which have any operation which is pending more than 5 minutes. Normally this can indicate either that an access for a given user is stalled, IO is blocked for a given file or a client is dead-locked due to a bug.

.. index::
   pair: Namespace; Cache Statistics

**Cache Section**
 .. code-block:: bash

    # ------------------------------------------------------------------------------------
    ALL      File cache max num               40000000
    ALL      File cache occupancy             13
    ALL      In-flight FileMD                 0
    ALL      Container cache max num          5000000
    ALL      Container cache occupancy        16
    ALL      In-flight ContainerMD            0
    # ------------------------------------------------------------------------------------


The cache section shows the current setting for the maximum number of files and container (directories) in the cache `File cache max num` (number of files) + `Container cache max num` (number of directories) and the current filling of the cache `File cache occupancy` (number of files in the cache) and `Container cache occupancy` (number of directories in the cache).

.. index::
   pair: Namespace; Cache Configuration


Changing the Cache Size
^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of files/container can be modified using:
 .. code-block:: bash

    eos ns cache set -f 100000000 # set the file cache size
    eos ns cache set -d 10000000  # set the container cache size

.. index::
   pair: Namespace; Cache Dropping


Dropping Caches
^^^^^^^^^^^^^^^^^^^^^^^
The caches can be dropped using:
 .. code-block:: bash

    eos ns cache drop -f # drop the file cache
    eos ns cache drop -d # drop the container cache

After a drop operation the occupancy value should go down to 0 and rise from there.

Dropping single files/directories from the cache
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To drop a single file or container one can execute:
 .. code-block:: bash

    eos ns cache drop-single-file <fileid>

 .. code-block:: bash

    eos ns cache drop-single-container <containerid>

.. index::
   pair: Namespace; MGM Latencies

**Latency Sections**

.. code-block:: bash

   # ------------------------------------------------------------------------------------
   ALL      eosViewRWMutex peak-latency      0ms (last) 0ms (1 min) 0ms (2 min) 0ms (5 min)
   # ------------------------------------------------------------------------------------
   ALL      QClient overall RTT              0ms (min)  0ms (avg)  18ms (max)
   ALL      QClient recent peak RTT          1ms (1 min) 1ms (2 min) 1ms (5 min)
   # ------------------------------------------------------------------------------------

*Namespace Peak Latency*
The `eosViewRWMutex peak-latency` values measure the maximum lead time to obtain a write lock on the namespace. The values show the current measurement and the maximum in the last 1,2 or 5 minutes. The value should be most of the time `0ms` but can fluctuate for few hundred milliseconds.

*QClient overall RTT*
This sections shows the measurement of the round-trip time for QuarkDB operations with the minimum, average and maximum values.
For normal operation the average time should be `0ms`, but the maximum value can exceed a second in certain circumstances.

.. index::
   pair: Namespace; QClient Latency

*Qclient recent peak RTT*
This sections shows the maximum value (peak) for a 1,2 and 5 minutes window in the past.

**Resource Section**
This section shows the current usage of memory by the MGM process, the growth of the memory since startup, the number of threads currently used by the MGM, the number of open filedescriptors and the uptime of the MGM process in seconds.

.. code-block:: bash

   # ------------------------------------------------------------------------------------
   ALL      memory virtual                   3.75 GB
   ALL      memory resident                  195.15 MB
   ALL      memory share                     48.03 MB
   ALL      memory growths                   1.48 GB
   ALL      threads                          379
   ALL      fds                              428
   ALL      uptime                           59205
   # ------------------------------------------------------------------------------------


In general the number of threads should be few hundred. If the number of threads reaches the maximum setting for the thread-pool (default is 4k threads), this points to a contention or lock problem in the MGM.

.. index::
   pair: Namespace; Service Information

**Service Section**

The service section shows the thread-pool settings for the `drain`, `fsck` and `converter` engines and the number of entries in their queues `queue_sz`.

.. code-block:: bash

   # ------------------------------------------------------------------------------------
   ALL      drain info                       pool=drain          min=10  max=100  size=10   queue_sz=0
   ALL      fsck info                        pool=fsck           min=2   max=20   size=2    queue_sz=0
   ALL      converter info                   pool=converter      min=8   max=100  size=8    queue_sz=0
   ALL      balancer info                    space=default
   # ------------------------------------------------------------------------------------


.. index::
   pair: Namespace; Tracker Information

**Tracker Section**

Whenever a file is processed by the balancer, conversion or draining engine, it is added to the file tracker for the given engine. This mechanism serves to avoid processing the same file concurrently within these engines.

.. code-block:: bash

   # ------------------------------------------------------------------------------------
   ALL      tracker info                     tracker=balance size=413
   ALL      tracker info                     tracker=convert size=0
   ALL      tracker info                     tracker=drain size=22
   # ------------------------------------------------------------------------------------

.. index::
   pair: Namespace; MD Statistics

Statistics Display
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add detailed statistic counters, one can use:

.. code-block:: bash

   eos ns stat                     # see aggregated counters,rates,timings for all users aggregated
   eos ns stat -m                  # output the previous in key-value monitoring format
   eos ns stat -a                  # see individual user/group measurements
   eos ns stat -a -m               # output the previous in key-value monitoring format
   eos ns stat -a [-m] -n          # don't output user/group names, just IDs


Here is an example output of `eos ns stat`, the header columns are explained here:

.. code-block:: bash

   * who                  : all => aggreation over all users
   * sum                  : number of times this command has been called since instance startup
   * 5s                   : average rate in Hz during the last 5s
   * 1min                 : average rate in Hz during the last minutes
   * 5min                 : average rate in Hz during the last 5 minutes
   * 1h                   : average rate in Hz during the last 1 hour
   * exec(ms)             : average execution time for this command for the last 100 executions
   * sigma(ms)            : standard deviation for the average before
   * 99p(ms)              : 99 percentile for the average before
   * max(ms)              : maximum for the average before
   * cumul                : accumulated execution time for all commands executed since instance start


.. code-block:: bash

   ┌───┬───────────────────────────────────────────┬────────┬────────┬────────┬────────┬────────┬────────┬─────────┬────────┬────────┬────────┐
   │who│command                                    │     sum│      5s│    1min│    5min│      1h│exec(ms)│sigma(ms)│ 99p(ms)│ max(ms)│   cumul│
   └───┴───────────────────────────────────────────┴────────┴────────┴────────┴────────┴────────┴────────┴─────────┴────────┴────────┴────────┘

   all Access                                      612.20 M   989.50  1177.05  1568.29  1745.82     -NA-      -NA-     -NA-     -NA-     -NA-
   all AccessControl                                    283     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all AdjustReplica                                 1.40 K     0.00     0.00     0.00     0.00     1.75      6.97    54.07    44.14       4s
   all AttrGet                                      21.25 M    20.25    33.44    56.27    47.83     0.17      0.46     4.63     0.74       2h
   all AttrLs                                        1.22 G  1131.00  2092.27  2429.77  2607.00     0.21      1.59    15.99     0.13      16h
   all AttrRm                                        2.50 K     0.00     0.00     0.00     0.01     0.21      0.18     1.88     0.49       0s
   all AttrSet                                     101.75 K     0.00     0.07     0.17     0.13     0.35      1.37    13.83     2.25      20s
   all BulkRequestBusiness::getBulkRequest                0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all BulkRequestBusiness::getStageBulkRequest           0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all BulkRequestBusiness::saveBulkRequest               0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Cd                                                15     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Checksum                                           0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Chmod                                       158.80 K     0.00     0.15     0.15     0.14     0.42      1.61    16.39     0.70       1m
   all Chown                                         4.04 K     0.00     0.00     0.01     0.01     -NA-      -NA-     -NA-     -NA-     -NA-
   all Commit                                       37.76 M    13.50   118.27   106.89    89.53     2.33      5.87    23.52    23.25       2d
   all CommitFailedFid                                    0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all CommitFailedNamespace                              0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all CommitFailedParameters                             0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all CommitFailedUnlinked                        347.40 K     0.00     0.00     0.72     0.23     -NA-      -NA-     -NA-     -NA-     -NA-
   all ConversionDone                                     0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all ConversionFailed                                   0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all CopyStripe                                       612     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Delay::threads::101837                        3.74 K     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Delay::threads::102888                        1.55 K     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   ...
   ...
   all DrainCentralFailed                            3.42 K     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all DrainCentralStarted                           1.96 M     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all DrainCentralSuccessful                        1.96 M     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Drop                                         14.62 M     4.25    18.68    28.03    28.37     0.29      0.09     0.56     0.56       1d
   all DropStripe                                       872     0.00     0.00     0.00     0.00     0.29      0.32     2.81     1.41       0s
   all DumpMd                                        8.37 K     0.00     0.00     0.00     0.03     -NA-      -NA-     -NA-     -NA-     -NA-
   all EAccess                                      46.58 M    34.50    54.93    56.34    73.61     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::BEGINFLUSH                        1.93 M     2.75     2.27     1.97     3.27     0.01      0.00     0.02     0.02      22s
   all Eosxd::ext::CREATE                           11.12 M     1.00    47.64    36.13    20.73     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::CREATELNK                       287.03 K     0.00     0.00     0.02     0.07     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::DELETE                            8.56 M     0.00     0.15     2.16     5.81     1.56      1.54    14.19     5.24       6h
   all Eosxd::ext::DELETELNK                       283.06 K     0.00     0.00     0.02     0.06     0.26      0.15     1.60     0.51       1m
   all Eosxd::ext::ENDFLUSH                          1.93 M     2.50     2.24     2.45     3.24     0.01      0.00     0.03     0.03      23s
   all Eosxd::ext::GET                              17.40 M   148.75    30.88    47.78    44.13     0.04      0.04     0.24     0.22       5h
   all Eosxd::ext::GETCAP                          249.79 M   123.00   113.54   217.52   290.05     0.27      0.89     9.16     0.31      23h
   all Eosxd::ext::GETLK                             6.62 K     0.00     0.00     0.01     0.01     0.01      0.00     0.03     0.02       0s
   all Eosxd::ext::LS                              267.49 M   128.50   123.24   230.66   314.07     0.43      0.47     3.03     2.71       2d
   all Eosxd::ext::LS-Entry                          4.93 G  2515.75  2636.56  4773.92  5748.08     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::MKDIR                           860.27 K     0.00     0.49     0.69     2.16     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::MV                               62.39 K     0.00     0.00     0.26     0.06     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::RENAME                            1.62 M     0.00     0.12     0.53     0.85     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::RM                                9.66 M     0.00     0.15     2.30     6.28     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::RMDIR                           817.21 K     0.00     0.00     0.11     0.41     1.20      0.82     4.18     3.78      12m
   all Eosxd::ext::SET                              56.21 M     6.75   119.19    97.08    73.01     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::ext::SETDIR                            3.69 M     0.00     0.78     2.22     4.07     0.27      0.56     5.73     0.60      24m
   all Eosxd::ext::SETFILE                          52.17 M     6.75   118.41    94.85    68.87     0.32      0.41     4.18     0.77      15h
   all Eosxd::ext::SETLK                             1.78 M     0.00     0.07     2.83     1.50     0.01      0.01     0.05     0.04      28s
   all Eosxd::ext::SETLKW                            1.10 M     1.00     1.00     1.15     1.17    11.67      0.26    13.96    12.73       3h
   all Eosxd::ext::SETLNK                          347.46 K     0.00     0.00     0.02     0.08     0.35      0.68     6.98     1.24       2m
   all Eosxd::ext::UPDATE                           42.08 M     5.75    70.93    59.42    48.72     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::int::AuthRevocation                    2.80 M     0.00     0.15     1.68     2.10     0.02      0.03     0.30     0.16      21s
   all Eosxd::int::BcConfig                        204.40 K     0.25     0.24     0.28     0.35     0.01      0.00     0.03     0.02       2s
   all Eosxd::int::BcDeletion                        9.56 M     0.00     0.15     2.30     6.28     0.01      0.01     0.05     0.04      28m
   all Eosxd::int::BcDeletionExt                     3.50 M    20.00     5.76     6.20     4.57     0.01      0.00     0.03     0.02       1m
   all Eosxd::int::BcDropAll                       198.89 K     0.25     0.24     0.28     0.35     0.02      0.00     0.03     0.03       3s
   all Eosxd::int::BcMD                             53.24 M     6.50   118.69    88.87    70.83     0.01      0.00     0.02     0.02       3h
   all Eosxd::int::BcMDSup                         263.14 M     0.00     0.00    25.84    99.11     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::int::BcRefresh                        12.39 M     0.00     0.44     3.83     8.18     0.02      0.02     0.10     0.10      11m
   all Eosxd::int::BcRefreshExt                     27.37 M    80.75    74.95    84.87    68.11     0.01      0.01     0.07     0.06       2h
   all Eosxd::int::BcRefreshExtSup                 108.03 M     0.00     0.00     8.20    28.24     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::int::BcRefreshSup                     47.42 M     0.00     0.00     0.00    20.91     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::int::BcRelease                        12.40 M     0.00     0.44     3.90     8.20     0.01      0.01     0.10     0.05      52m
   all Eosxd::int::BcReleaseExt                     21.32 M    47.25    24.25    71.48    62.38     0.00      0.01     0.05     0.05       5m
   all Eosxd::int::DeleteEntry                      17.59 M     0.00     0.03     1.19     1.24     0.02      0.00     0.04     0.03       5m
   all Eosxd::int::FillContainerCAP                611.91 M   411.75   275.78   586.62   715.28     0.17      0.89     9.05     0.13      14h
   all Eosxd::int::FillContainerMD                   1.12 G  2233.25  1285.92  1171.27  1456.71     0.06      0.20     2.03     0.10       1d
   all Eosxd::int::FillFileMD                        4.38 G   989.25  1651.12  4043.64  4775.40     0.01      0.00     0.02     0.02      16h
   all Eosxd::int::Heartbeat                       211.83 M   273.25   394.36   409.23   552.32     0.03      0.11     1.13     0.12       4h
   all Eosxd::int::MonitorCaps                     759.91 K     1.00     0.95     1.11     1.15    28.66     79.18   281.70   276.66       4h
   all Eosxd::int::MonitorHeartBeat                773.53 K     0.75     0.98     1.15     1.18     8.30     11.54    46.61    41.56      52m
   all Eosxd::int::RefreshEntry                     29.23 M    30.00    84.98    66.38    52.00     0.03      0.01     0.06     0.06      20m
   all Eosxd::int::ReleaseCap                       27.40 M     1.25     0.17     2.98     5.32     0.02      0.01     0.07     0.05       9m
   all Eosxd::int::SendCAP                           5.69 M     0.00     0.00     0.04     5.44     0.02      0.01     0.07     0.05       1m
   all Eosxd::int::SendMD                            8.40 M     0.00     2.08     5.14     7.60     0.01      0.01     0.04     0.03       1m
   all Eosxd::int::Store                           602.26 M   407.50   269.90   504.02   705.10     0.02      0.00     0.03     0.03       4h
   all Eosxd::int::ValidatePERM                      9.38 M     3.50    15.42    10.64     7.14     0.12      0.29     2.37     1.71       3h
   all Eosxd::prot::LS                             590.33 M   673.25   350.66   565.11   748.29     0.20      0.37     2.87     1.61       5d
   all Eosxd::prot::SET                             72.61 M    13.00   124.92   107.26    88.47     0.85      2.49    11.69    11.69       1d
   all Eosxd::prot::STAT                            72.93 M   423.25   113.90   117.07   144.15     0.23      0.17     1.77     0.47       1d
   all Eosxd::prot::evicted                         25.36 K     0.50     0.17     0.08     0.07     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::prot::mount                          198.89 K     0.25     0.24     0.28     0.35     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::prot::offline                         16.10 K     0.00     0.07     0.08     0.07     -NA-      -NA-     -NA-     -NA-     -NA-
   all Eosxd::prot::umount                         186.20 K     0.25     0.31     0.26     0.24     -NA-      -NA-     -NA-     -NA-     -NA-
   all Exists                                       32.53 M   100.50    25.19    22.91    29.91     0.18      0.09     0.54     0.52       2h
   all FileInfo                                     24.22 M    70.75    38.42    41.18    41.16     -NA-      -NA-     -NA-     -NA-     -NA-
   all Find                                        621.96 K     0.25     0.19     0.51     0.45     1.18      2.89    16.82    14.61       1h
   all FindEntries                                   4.60 M     0.50     7.37     6.91     2.97     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse                                               0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Access                                        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Checksum                               483.31 K     0.00     0.14     0.03     1.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Chmod                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Chown                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Mkdir                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Stat                                    44.02 K     0.00     0.07     0.16     0.13     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Statvfs                                  2.56 M     5.00     4.03     5.03     5.46     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-Utimes                                        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Fuse-XAttr                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all GetFusex                                           0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all GetMd                                        81.03 M   306.75   168.15   230.45   167.20     -NA-      -NA-     -NA-     -NA-     -NA-
   all GetMdLocation                                      6     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all HashGet                                      20.51 G 16011.00 56173.54 36789.02 33583.27     -NA-      -NA-     -NA-     -NA-     -NA-
   all HashSet                                       2.90 G  3942.50  4238.42  5160.53  5425.47     -NA-      -NA-     -NA-     -NA-     -NA-
   all HashSetNoLock                                      0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-COPY                                    64.27 K     0.50     0.39     2.65     3.03     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-DELETE                                 133.00 K    20.00     4.68     2.55     1.41     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-GET                                      2.98 M    24.25    31.00     7.44     3.92     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-HEAD                                    98.87 K     2.25     2.86     4.83     6.20     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-LOCK                                         16     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-MKCOL                                   93.92 K     0.00     0.00     0.10     0.09     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-MOVE                                    25.64 K     0.00     0.00     0.02     0.03     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-OPTIONS                                       5     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-POST                                   540.08 K     6.50     6.10    18.93    24.46     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-PROPFIND                                57.52 M    90.75   111.24   157.86   149.35     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-PROPPATCH                                     0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-PUT                                      2.58 M     1.25     2.02    10.12     9.52     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-TRACE                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Http-UNLOCK                                       16     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all IdMap                                         1.19 G  1297.25  1363.63  1452.83  1690.05     0.04      0.01     0.07     0.07       1d
   all LRUFind                                            0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Ls                                            2.82 K     0.00     0.02     0.00     0.01     -NA-      -NA-     -NA-     -NA-     -NA-
   all MarkClean                                          0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all MarkDirty                                          0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Mkdir                                         6.94 M    20.00     5.08     4.94     6.91     0.65      0.68     5.39     3.82       8m
   all Motd                                               7     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all MoveStripe                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Newfind                                            1     0.00     0.00     0.00     0.00  4744.00      0.00  4744.00  4744.00       4s
   all NewfindEntries                                6.40 K     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all NsLockR                                       9.51 G  8018.00  9588.22 12451.45 13669.57     -NA-      -NA-     -NA-     -NA-     -NA-
   all NsLockW                                     163.83 M   109.25   324.59   337.82   271.71     -NA-      -NA-     -NA-     -NA-     -NA-
   all Open                                        119.59 M    91.25   124.71   115.07   132.60     0.65      0.71     7.40     2.36       3d
   all OpenDir                                     482.14 M    39.00   783.88   734.32   715.38     1.30      3.08    16.92    13.28       1d
   all OpenDir-Entry                                12.37 G  1068.25  8288.78 10319.15 26755.20     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedCreate                                   1     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedENOENT                                 352     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedExists                                 156     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedNoUpdate                                 0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedPermission                          3.30 K     0.00     0.00     0.02     0.01     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedQuota                             360.72 K     0.00     0.00     0.04     0.24     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedReconstruct                              0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFailedRedirectLocal                            0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenFileOffline                                  704     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenLayout                                   10.96 K     0.00     0.00     0.01     0.01     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenProc                                    531.80 M   317.75   260.27   476.06   634.66     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenRead                                    104.00 M    83.75    72.64    67.27   100.06     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenRedirectLocal                                  0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenShared                                         0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenStalled                                        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenWrite                                    12.60 M     1.25    49.88    37.67    22.67     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenWriteCreate                               1.65 M     1.25     1.49     7.70     6.12     -NA-      -NA-     -NA-     -NA-     -NA-
   all OpenWriteTruncate                             1.32 M     0.00     0.68     2.71     3.75     -NA-      -NA-     -NA-     -NA-     -NA-
   all Prepare                                          190     0.00     0.00     0.00     0.00     0.49      0.59     5.71     2.08       1s
   all QuarkSyncTimeAccounting                       7.14 M     8.25     5.03     8.28    10.40     4.12      2.43    16.77    13.52       9m
   all QueryResync                                        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Quota                                        93.64 K     0.50     0.07     0.08     0.15     -NA-      -NA-     -NA-     -NA-     -NA-
   all QuotaLockR                                    1.06 G   588.00  1363.17  1974.57  1846.20     -NA-      -NA-     -NA-     -NA-     -NA-
   all QuotaLockW                                   25.96 K     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all ReadLink                                           0     0.00     0.00     0.00     0.00     0.36      2.68    27.05     0.62      40m
   all Recycle                                            0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Redirect                                           0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all RedirectENOENT                                     0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all RedirectENONET                                     0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all RedirectR                                          0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all RedirectR-Master                                   0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all RedirectW                                          0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Rename                                        6.50 M    20.00     4.92     4.32     6.46     1.11      0.36     2.82     2.11       1h
   all ReplicaFailedChecksum                            863     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all ReplicaFailedSize                                180     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Rm                                            9.03 M    20.00     5.27     5.84     7.69     2.39      0.73     5.47     4.70       5h
   all RmDir                                       550.83 K     0.00     0.03     0.09     0.17     0.23      0.21     1.42     1.29       2m
   all Schedule2Balance                              6.66 M    10.25    12.88    16.55    24.56     -NA-      -NA-     -NA-     -NA-     -NA-
   all Schedule2Delete                               2.35 M     2.75     4.02     4.63     4.51     -NA-      -NA-     -NA-     -NA-     -NA-
   all Scheduled2Balance                             6.66 M    10.50    12.90    22.60    24.88     4.79      7.75    50.93    50.46       1d
   all Scheduled2Delete                             14.61 M     8.75    19.58    34.61    33.38     0.49      2.97    21.35    21.25       3h
   all Scheduled2Drain                                    0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all SchedulingFailedBalance                            0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all SchedulingFailedDrain                              0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Stall                                            610     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Stall::threads::104503                           347     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Stall::threads::104562                             5     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   ...
   ...
   all Stat                                          2.39 G  1291.50  4115.97  4238.70  4389.46     0.06      0.03     0.31     0.13      15h
   all Symlink                                       3.03 M    26.50    33.86    12.27    10.12     -NA-      -NA-     -NA-     -NA-     -NA-
   all TapeRestApiBusiness::cancelStageBulkRequest        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all TapeRestApiBusiness::createStageBulkRequest        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all TapeRestApiBusiness::deleteStageBulkRequest        0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all TapeRestApiBusiness::getFileInfo                   0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all TapeRestApiBusiness::getStageBulkRequest           0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all TapeRestApiBusiness::releasePaths                  0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Touch                                         2.65 K     0.00     0.02     0.00     0.00     2.14      0.98     9.60     6.01       2m
   all Truncate                                           0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all TxState                                            0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all VerifyStripe                                     403     0.00     0.00     0.00     0.00     0.81      1.27     8.56     8.49       0s
   all Version                                      13.24 K     0.00     0.05     0.03     0.02     -NA-      -NA-     -NA-     -NA-     -NA-
   all Versioning                                  600.85 K     0.00     0.68     1.92     1.70     0.88      1.11     9.05     6.34      11m
   all ViewLockR                                   604.60 M   812.00   956.05  1060.62  1049.79     -NA-      -NA-     -NA-     -NA-     -NA-
   all ViewLockW                                        916     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-
   all Who                                          29.19 K     0.00     0.20     0.44     0.47     -NA-      -NA-     -NA-     -NA-     -NA-
   all WhoAmI                                        2.64 K     0.00     0.02     0.00     0.00     -NA-      -NA-     -NA-     -NA-     -NA-


When using the `-a` switch, two more sections are added with user and group specific counters for total number of commands per users and rate measurements for 5s,1min,5min and 1 hours:

.. code-block:: bash

  ┌───────────────┬────────┬────────┬────────┬────────┬────────┬────────┐
  │user           │command │     sum│      5s│    1min│    5min│      1h│
  └───────────────┴────────┴────────┴────────┴────────┴────────┴────────┘
   ...

and

.. code-block:: bash

  ┌─────────────────┬────────┬────────┬────────┬────────┬────────┬────────┐
  │group            │command │     sum│      5s│    1min│    5min│      1h│
  └─────────────────┴────────┴────────┴────────┴────────┴────────┴────────┘
  ...

It is recommend in case of many users to use the `eos ns stat -a -n` switch to avoid translating user and group names, which can be slow/problematic for non-existing `uid/gids`.

.. epigraph::

    ===========================================  ======================================================================================
    Counter                                      Desription
    ===========================================  ======================================================================================
    Access                                       internal access function checking permissions
    AccessControl                                usage of 'eos access' commands
    AdjustReplica                                usage of 'eos file adjustreplica' commands
    AttrGet                                      internal get xattr calls
    AttrLs                                       internal list xattr calls
    AttrRm                                       internal delete xattr calls
    AttrSet                                      internal set xattr calls
    BulkRequestBusiness::getBulkRequest          CTA get bulk request calls
    BulkRequestBusiness::getStageBulkRequest     CTA get stage bulk request calls
    BulkRequestBusiness::saveBulkRequest         CTA save bulk request calls
    Cd                                           internal cd calls
    Checksum                                     checksum calls
    Chmod                                        chmod calls
    Chown                                        chown calls
    Commit                                       commit calls (triggered by each replica closed on an FST)
    CommitFailedFid                              internal meta-data mismatch during commit concerning file id
    CommitFailedNamespace                        internal failure when commiting an update to the namespace
    CommitFailedParameters                       commit call misses parameters
    CommitFailedUnlinked                         commit on a file which had already been deleted
    ConversionDone                               successful conversions
    ConversionFailed                             failed conversoins
    CopyStripe                                   calls to copy a stripe from one filesystem to another
    Delay::threads::101090                       number of times a thread for user 101090 has been delayed to enforce a rate limit
    Delay::threads::101981                       as above for user 101981 aso ...
    DrainCentralFailed                           number of failed drain jobs
    DrainCentralStarted                          number of started drain jobs
    DrainCentralSuccessful                       number of successful drain jobs
    Drop                                         calls to drop a replica
    DropStripe                                   calls of function to drop a stripe via 'eos file drop'
    DumpMd                                       calls to dump meta-data (unused when meta-data in fst XATTR)
    EAccess                                      access is forbidden due to some 'eos access' settings
    Eosxd::ext::BEGINFLUSH                       calls indicating a FUSE client starts to flush its journal
    Eosxd::ext::CREATE                           calls to create a file via FUSE
    Eosxd::ext::CREATELNK                        calls to create a symlink via FUSE
    Eosxd::ext::DELETE                           calls to delete a file via FUSE
    Eosxd::ext::DELETELNK                        calls to delete a link via FUSE
    Eosxd::ext::ENDFLUSH                         calls indicating a FUSE client finished flushing its journal
    Eosxd::ext::GET                              calls to retrieve meta-data from a FUSE client
    Eosxd::ext::GETCAP                           calls to retrieve a capability from a FUSE client
    Eosxd::ext::GETLK                            calls to retrieve a lock from a FUSE client
    Eosxd::ext::LS                               calls to retrieve a directory listing from a FUSE client
    Eosxd::ext::LS-Entry                         number of entries retrieved by listing calls from FUSE clients
    Eosxd::ext::MKDIR                            calls to create a directory from FUSE clients
    Eosxd::ext::MV                               calls to move files between directories from FUSE clients
    Eosxd::ext::RENAME                           calls to change a file name within a directory from FUSE clients
    Eosxd::ext::RM                               calls to delete a file from FUSE clients
    Eosxd::ext::RMDIR                            calls to delete a directory from FUSE clients
    Eosxd::ext::SET                              calls to create meta-data from FUSE clients
    Eosxd::ext::SETDIR                           calls to create directory meta-data from FUSE clients
    Eosxd::ext::SETFILE                          calls to create file meta-data from FUSE clients
    Eosxd::ext::SETLK                            calls to create a shared lock from FUSE clients
    Eosxd::ext::SETLKW                           calls to create an exclusive lock from FUSE clients
    Eosxd::ext::SETLNK                           calls to create a symbolic link
    Eosxd::ext::UPDATE                           calls to update meta-data
    Eosxd::int::AuthRevocation                   number of capabilities revoked by FUSE clients
    Eosxd::int::BcConfig                         broadcast calls with FUSE configuration
    Eosxd::int::BcDeletion                       broadcast calls with FUSE deletions triggered by FUSE clients
    Eosxd::int::BcDeletionExt                    broadcast calls with FUSE deletion triggered by non-FUSE clients
    Eosxd::int::BcDropAll                        broadcast calls to drop all capabilities on a FUSE client
    Eosxd::int::BcMD                             broadcast calls to send meta-data to a FUSE client
    Eosxd::int::BcMDSup                          broadcast calls suppressed by suppression rules 'eos fusex conf'
    Eosxd::int::BcRefresh                        broadcast calls to tell a FUSE client to refresh meta-data
    Eosxd::int::BcRefreshExt                     broadcast calls to tell a FUSE client to refresh meta-data triggered by non-FUSE clients
    Eosxd::int::BcRefreshExtSup                  broadcast calls to tell a FUSE client to refresh meta-data triggered by non-FUSE clients suppressed by suppresion rules 'eos fusex conf'
    Eosxd::int::BcRefreshSup                     broadcast calls to tell a FUSE client to refresh meta-data suppressed by suppression rules 'eos fusex conf'
    Eosxd::int::BcRelease                        calls to release a capability to a FUSE client
    Eosxd::int::BcReleaseExt                     calls to release a capability to a FUSE client triggered by non-FUSE clients
    Eosxd::int::DeleteEntry                      calls to delete an entry to a FUSE client
    Eosxd::int::FillContainerCAP                 calls to fill a capability for a given container for a FUSE client
    Eosxd::int::FillContainerMD                  calls to fill container meta-data for a FUSE client
    Eosxd::int::FillFileMD                       calls to fill file meta-data for a FUSE client
    Eosxd::int::Heartbeat                        heartbeats from FUSE clients received
    Eosxd::int::MonitorCaps                      internal calls monitoring (expiring) capabilities of FUSE clients
    Eosxd::int::MonitorHeartBeat                 internal calls monitoring (expiring) FUSE client heartbeats responsible for eviction of clients missing heartbeats
    Eosxd::int::RefreshEntry                     internal refresh entry calls
    Eosxd::int::ReleaseCap                       internal release capability calls
    Eosxd::int::SendCAP                          calls to send a capability to FUSE clients
    Eosxd::int::SendMD                           calls to send meta-data to FUSE clients
    Eosxd::int::Store                            calls to store a capability
    Eosxd::int::ValidatePERM                     calls to validate permissions when an attached capability didn't grant the required permissions
    Eosxd::prot::LS                              protocol calls to for listing
    Eosxd::prot::SET                             protocol calls to set meta-data
    Eosxd::prot::STAT                            protocol calls to stat meta-data
    Eosxd::prot::evicted                         counter marking a client evicted (2 minutes no heart-beat)
    Eosxd::prot::mount                           number clients mount the filesystem
    Eosxd::prot::offline                         counter marking a client as offline (30 seconds no heart-beat)
    Eosxd::prot::umount                          number clients unmount the filesystem
    Exists
    FileInfo
    Find
    FindEntries
    Fuse
    Fuse-Access
    Fuse-Checksum
    Fuse-Chmod
    Fuse-Chown
    Fuse-Mkdir
    Fuse-Stat
    Fuse-Statvfs
    Fuse-Utimes
    Fuse-XAttr
    GetFusex
    GetMd
    GetMdLocation
    HashGet
    HashSet
    HashSetNoLock
    Http-COPY
    Http-DELETE
    Http-GET
    Http-HEAD
    Http-LOCK
    Http-MKCOL
    Http-MOVE
    Http-OPTIONS
    Http-POST
    Http-PROPFIND
    Http-PROPPATCH
    Http-PUT
    Http-TRACE
    Http-UNLOCK
    IdMap
    LRUFind
    Ls
    MarkClean
    MarkDirty
    Mkdir
    Motd
    MoveStripe
    NsLockR
    NsLockW
    Open
    OpenDir
    OpenDir-Entry
    OpenFailedCreate
    OpenFailedENOENT
    OpenFailedExists
    OpenFailedNoUpdate
    OpenFailedPermission
    OpenFailedQuota
    OpenFailedReconstruct
    OpenFailedRedirectLocal
    OpenFileOffline
    OpenLayout
    OpenProc
    OpenRead
    OpenRedirectLocal
    OpenShared
    OpenStalled
    OpenWrite
    OpenWriteCreate
    OpenWriteTruncate
    Prepare
    QuarkSyncTimeAccounting
    QueryResync
    Quota
    QuotaLockR
    QuotaLockW
    ReadLink
    Recycle
    Redirect
    RedirectENOENT
    RedirectENONET
    RedirectR
    RedirectR-Master
    RedirectW
    Rename
    ReplicaFailedChecksum
    ReplicaFailedSize
    Rm
    RmDir
    Schedule2Balance
    Schedule2Delete
    Scheduled2Balance
    Scheduled2Delete
    Scheduled2Drain
    SchedulingFailedBalance
    SchedulingFailedDrain
    Stall
    Stall::threads::112541
    Stall::threads::127773
    Stall::threads::134033
    Stall::threads::152755
    Stall::threads::155709
    Stall::threads::22043
    Stall::threads::52999
    Stall::threads::81826
    Stat
    Symlink
    TapeRestApiBusiness::cancelStageBulkRequest
    TapeRestApiBusiness::createStageBulkRequest
    TapeRestApiBusiness::deleteStageBulkRequest
    TapeRestApiBusiness::getFileInfo
    TapeRestApiBusiness::getStageBulkRequest
    TapeRestApiBusiness::releasePaths
    Touch
    Truncate
    TxState
    VerifyStripe
    Version
    Versioning
    ViewLockR
    ViewLockW
    Who
    WhoAmI
    NsLockRWait:spl
    NsLockRWait:min
    NsLockRWait:avg
    NsLockRWait:max
    NsLockWWait:spl
    NsLockWWait:min
    NsLockWWait:avg
    NsLockWWait:max
    QuotaLockRWait:spl
    QuotaLockRWait:min
    QuotaLockRWait:avg
    QuotaLockRWait:max
    QuotaLockWWait:spl
    QuotaLockWWait:min
    QuotaLockWWait:avg
    QuotaLockWWait:max
    ViewLockRWait:spl
    ViewLockRWait:min
    ViewLockRWait:avg
    ViewLockRWait:max
    ViewLockWWait:spl
    ViewLockWWait:min
    ViewLockWWait:avg
    ViewLockWWait:max
    ===========================================  ======================================================================================


.. index::
   pair: Interfaces; Access
   pair: CLI; eos access

Access Interface
----------------

The access interface `eos access` has the main purpose to define access control rules to an EOS instance.

.. index::
   pair: Access; Restricting

Restricting access by users,group, host or domain name
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The access interface allows to

* manage blacklists of users, groups, hosts, domains or tokens which cannot access the instance via `eos ban` and `eos unban`
* manage whitelists of users, groups, hosts, domains or tokens which can access the instance via `eos allow` and `eos unallow`

If you create a whitelist, only the listed entities are able to connect, otherwise receive an EPERM error. If you have a entry in a whitelist you can still configure a ban on that entry and it will stop access for this entity.

A ban entry returns for any request which is not FUSE or http access a *WAIT* (`come back in 5 minutes`) response, which means banned clients will hang for 5 minutes and might retry - they are not failed with EPERM.

.. index::
   pair: Access; Allowing

Creating *allow_ entries* (whitelist)
""""""""""""""""""""""""""""""""""""""
If we want to allow user `foo` and group `bar` we do:

.. code-block:: bash

   eos allow user foo
   eos allow group bar


If we want to allow only a particular host we do:

.. code-block:: bash

   eos allow host myhost.my.domain


If we want to allow only a particular domain we do:

.. code-block:: bash

   eos allow my,domain


.. WARNING::

   Keep in mind, that if you put an allow rule for a particular token, all other token don't work anymore!

.. index::
   pair: Access; Info

Displaying access rules
""""""""""""""""""""""""
The current access definitions can be show using:

.. code-block:: bash

   eos access ls
   # ....................................................................................
   # Allowd Users ...
   # ....................................................................................
   [ 01 ] foo
   # ....................................................................................
   # Allowed Groups...
   # ....................................................................................
   [ 01 ] bar
   # ....................................................................................
   # Allowed Hosts ...
   # ....................................................................................
   [ 01 ] myhost.my.domain
   # ....................................................................................
   # Allowed Domains ...
   # ....................................................................................
   [ 01 ] my.domain


Removing *allow* entries (whitelist)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Remove entry after `allow`:

.. code-block:: bash

  eos access unallow user foo
  eos accces unallow group bar
  eos access unallow host myhost.my.domain
  eos access unallow domain my.domain


.. note::

   Access is open for all users,groups,hosts or domains without an _allow_ entry!

Creating *ban* entries (blacklist)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*ban* entries define a blacklist.

To ban a user, group, host, domain or token you use the `ban` subcommand:

.. code-block:: bash

  eos access ban user foo
  eos access ban group bar
  eos access ban host myhost.my.domain
  eos access ban domain my.domain
  eos access ban token 1079aad2-927c-11ed-be62-0071c2181e97

Clients matching any of these rules will be stalled 5 minutes (receive a *WAIT* response).

.. note::

  If you need to ban a token, the token voucher ID which you specifiy to block it, is shown as logid in the MGM logfile when the _IdMap_ function has been called. If you have the token available you can see the voucher ID by running

  `eos token --token zteos64:...`.

The defined blacklists are shown using `eos access ls`:

.. code-block::

   eos access ls
   # ....................................................................................
   # Banned Users ...
   # ....................................................................................
   [ 01 ] foo
   # ....................................................................................
   # Banned Groups...
   # ....................................................................................
   [ 01 ] bar
   # ....................................................................................
   # Banned Hosts ...
   # ....................................................................................
   [ 01 ] myhost.my.domain
   # ....................................................................................
   # Banned Domains ...
   # ....................................................................................
   [ 01 ] my.domain
   # ....................................................................................
   # Banned Tokens ...
   # ....................................................................................
   [ 01 ] 1079aad2-927c-11ed-be62-0071c2181e97
   # ....................................................................................

Removing *ban* entries (blacklist)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To remove a ban rule, you use the `unban` subcommand:

.. code-block:: bash

  eos access unban user foo
  eos access unban group bar
  eos access unban host myhost.my.domain
  eos access unban domain my.domain
  eos access unban token 1079aad2-927c-11ed-be62-0071c2181e97


.. note::
    It is not supported to use wildcards in any of the rule definitions for performance reasons!

.. index::
   pair: Access; Redirection

Redirecting Requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
It is possible to redirect all requests to a new target. Additionally a *hold* time can be specified, which means that before a redirection happens a request is hold for *hold* ms. This mechanism can be used to deploy a throttling front-end.

Add/Remove a redirects
""""""""""""""""""""""
Redirect all requests to `foo.bar:1094`:

.. code-block:: bash

  eos access set redirect foo.bar:1094

Redirect all requests to `foo.bar:1094` with a 10ms delay:

.. code-block:: bash

  eos access set redirect foo.bar:1094:10


Remove a redirection rule:

.. code-block:: bash

  eos access rm redirect

.. index::
   pair: Access; Thread Limiting


Thread Limiting Rules
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The EOS *MGM* process provides a thread-pool to handle user requests. The default setting is to have 8 threads always ready and to dynamically expand the thread-pool to 256 threads if needed. These values are defined in `/etc/xrd.cf.mgm` for the old and `/etc/eos/config/mgm/mgm` for the new configuration `xrd.sched mint 8 maxt 256 idle 64`. *mint* is the minimum number of threads ready, *maxt* is the maximum number of threads running, *idle* is the time after which unused threads are retired. To avoid that a single user (who is running many clients doing more parallel requests than *maxt*) hijacks the MGM thread pool and consumes all resources, it is possible to set the maximum number of threads a single user can use at the same time.
To define the maximum number of threads per user for any user you do:

.. code-block:: bash

  eos access set limit 500 threads:*

To define the maximum number of threads for a specific user *foo* you do:

.. code-block:: bash

  eos access set limit 100 threads:foo

To remove a wildcard rule you do:

.. code-block:: bash

  eos access rm limit threads:*

or a specific:

.. code-block:: bash

  eos access rm limit threads:foo

Hint: the state of the thread pool and if limits are applied is shown at the end of the `eos ns stat` output:

.. code-block:: bash

   eos ns stat

   ┌────────┬───────┬────────┬─────┬──────┬─────────┬────────────────┐
   │     uid│threads│sessions│limit│stalls│stalltime│          status│
   └────────┴───────┴────────┴─────┴──────┴─────────┴────────────────┘
       28944       1        1   100      0         1          user-OK
       80912       8        0    10      0         1          user-OK


* *id* - user id
* *threads* - number of active threads in this moment
* *sessions* - number of client sessions
* *limit* - current limit applying to this user (can be a catch all or specific limit)
* *stalls* - number of times this user has received a stall due to thread-pool excess
* *stalltime* - time in *seconds* requests are stalles currently for this user
* *status* - `user-OK`:no limit applies `user-LIMIT`: using more than 90% of allowed `user-OL`: exceeding the allowed limit

A user request hitting a thread-pool (with exceeded user limit!) is not served but receives a *WAIT* response: this request is stalled for *stalltime* seconds and the client can retry the request after the given period has passed.

*Hint:* thread-limits can lead to longer client starvation periods in some cases of long overload periods because
previous stalls of requests don't change the probability that a client is repeatedly stalled!

.. index::
   pair: Access; Rate Limiting

Rate Limiting Rules
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
It is possible to define a maximum *operations/s* rate for individual meta-data operations. These limits can be defined for single users and/or for all users (catch-all wildcard limit). First user limits are checked, then catch-all limits for all users. Limits are defined in Hz as natural numbers. A limit of 0 sends a *WAIT 5s* (smeared +- 5s) response to clients for the configured request type. All this is only valid for non-FUSE clients. FUSE clients receive the error EACCES, when a limit is reached to avoid that mounts lock-up!

Examples:

.. code-block:: bash

   access set limit 100  rate:user:*:OpenRead     : Limit the open for read rate to a frequency of 100 Hz for all users
   access set limit 0    rate:user:ab:OpenRead    : Limit the open for read rate for the ab user to 0 Hz, to continuously stall it
   access set limit 2000 rate:group:zp:Stat       : Limit the stat rate for the zp group to 2kHz


The MGM enforces the given operation rate by delaying responses to match the nominal *operation/s* setting for a given operation/user.
The behavior changes when a limit is reached and a users exhausts his allowed thread limit or the global thread pool is saturated: the MGM does not regulate anymore the rate but sends *WAIT* responses.

.. note:: 1. When the *WAIT* response mechanism is triggered, it triggers for any kind of operation until no single limit is exceeded anymore! To summarize, if a user excesses his open/s rate and the thread pool, also his deletion commands will get stalled.

.. note:: 2. All limits starting with *Eosxd::* (these are operation counters for FUSE clients!) trigger the severe *WAIT* response mechanism only for the given operation, they have no impact on other operations.

White or Blacklisting Hosts which can be stalled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For certain use cases we want to exclude limits depending on the client host issuing the commands. An example are for example protocol gateways (e.g. CIFS server), which we don't want to throttle or a backup machinery, which can easily trigger rate limits.

To allow this, one can define a whitelist (which hosts can actually be stalled) or a blacklist (which hosts should never be stalled).

Examples to define these are shown here:

.. code-block:: bash

   access stallhosts add stall foo*.bar           : Add foo*.bar to the list of hosts which are stalled by limit rules (white list)
   access stallhosts remove stall foo*.bar        : Remove foo*.bar from the list of hosts which are stalled by limit rules (white list)
   access stallhosts add nostall foo*.bar         : Add foo*.bar to the list of hosts which are never stalled by limit rules (black list)
   access stallhosts remove nostall foo*.bar      : Remove foo*.bar from the list of hosts which are never stalled by limit rules (black list)


.. index::
   pair: Interfaces; Space Attributes


 Space Attributes
-----------------

Space attributes allow to define a set of exteneded attributes which appear in attribute listing of any directory linked to a given space. A directory reference space attributes by *sys.forced.space* or if not defined it will reference attributes in the *default* space. Space attributes allow to reduce the directory-meta data size because attributes have not to be stored with each directory individually. A specialized space attribute is *sys.acl*, which is explained in detail in the permission (ACL) system section. *sys.acl* attributes are syntax checked and provide left- and right-positioned ACL extensions. All other space attributes can either overwrite/define attributes appearing in each space referencing directory or they can be defined as optional default attribute, which is used only if a directory does not define the attribute.

Space attributes are defined like:

.. code-block:: bash

                # define adler32 for all directories in that space
                eos space config default space.attr.sys.forced.checksum=adler32

Space attribute are removed like:

.. code-block:: bash

                # remove checksum settings in the default space
                eos sapce config rm default space.attr.sys.forced.checksum

Space attributes are listed in the usual manner:

.. code-block:: bash

                # show configuration of the default space
                eos space status default



.. index::
   pair: Interfaces; Space policies
   pair: Interfaces; Appication policies

Space and Application Policies
----------------------------------

Space policies are set using the space configuration CLI.

The following policies can be configured

.. epigraph::

    =================== =================================================
    key                 values
    =================== =================================================
    space               default,...
    altspaces           [space1[,space2[,space3]]]
    layout              plain,replica,raid5,raid6,raiddp,archive,qrain
    nstripes            1..255
    checksum            adler,md5,sha1,crc32,crc32c
    blockchecksum       adler,md5,sha1,crc32,crc32c
    blocksize           4k,64k,128k,512k,1M,4M,16M,64M
    bandwidth:r|w       IO limit in MB/s for reader/writer
    iotype:r|w          io flavour [ direct, sync, csync, dsync ]
    iopriority:r|w      io priority [ rt:0...rt:7,be:0,be:7,idle ]
    schedule:r|w        fair FST scheduling [1 or 0]
    localredirect       redirect clients to a local filesystem (0,1,never,optional,always)
    readconversion      read-tiering: read files only from the given layout (space:hex-layout)
    updateconversion    write-tiering: update files only with the given layout (space:hex-layout)
    =================== =================================================


Setting space policies
^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   # configure raid6 layout
   eos space config default space.policy.layout=raid6

   # configure 10 stripes
   eos space config default space.policy.nstripes=10

   # configure adler file checksumming
   eos space config default space.policy.checksum=adler
   # configure crc32c block checksumming
   eos space config default space.policy.blockchecksum=crc32c

   # configure 1M blocksizes
   eos space config default space.policy.blocksize=1M

   # configure a global bandwidth limitation for all streams of 100 MB/s in a space
   eos space config default space.policy.bandwidth=100

   # configure FST fair thread scheduling for readers
   eos space config default space.policy.schedule:r=1

   # configure default FST iopriority for writers
   eos space config default space.policy.iopriority:w=be:4

   # configure default FST iotype
   eos space config default space.policy.iotype:w=direct

   # configure local redirects for all clients
   space config default space.policy.localredirect=always

   # configure local redirects for all clients, which have a '#sharedfs' appended to the application
   # e.g. eoscp#cephfs to redirect applications anchoring the 'cephfs' named shared filesystem
   space config default space.policy.localredirect=optional

   # disable local redirects for all clients
   space config default space.policy.localredirect=never

Setting user,group and application policies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

IO policies as iotype,iopriority,bandwidth and schedule can be scoped to
a group,user or an application

.. code-block:: bash

   # configure an application specific bandwidth limitations for all reading streams in a space
   eos space config default space.bandwidth:r.app:myapp=100 # reading streams tagged as ?eos.app=myapp are limited to 100 MB/s

   eos space config default space.iotype:w.user:root=direct # use direct IO for writing streams by user root

   eos space config default space.iopriority:r.group:adm=rt:1 # use IO priority realtime level 1 for the adm group when reading


The evaluation order is by space (lowest), by group, by user, by app
(highest). Finally IO policies can be overwritten by extended
**sys.forced** attributes (see the following).

Policy Selection, Scopes and alternative Spaces
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Clients can select the space ( and its default policies ) by adding
`eos.space=<space>` to the CGI query of an URL, otherwise the space is
taken from **space.policy.space** in the default space or if undefined
it uses the **default** space to set space policies.

Examples:

.. code-block:: bash

   ##############
   # Example 1  #
   ##############
   # files uploaded without selecting a space will end up in the replica space unless there is a forced overwrite in the target directory

   # point to the replica space as default policy
   eos space config default space.policy.space=replica
   # configure 2 replicas in the replica space
   eos space config replica space.policy.nstripes=2
   eos space config replica space.policy.layout=replica


   ##############
   # Example 2  #
   ##############
   # files uploaded selecting the rep4 space will be stored with 4 replicas, if non space is selected they will get the default for the target directory or the default space

   # define a space with 4 replica policy
   eos space config rep4 space.policy.nstripes=4
   eos space config rep4 space.policy.layout=replica


   ##############
   # Example 3  #
   ##############
   # When the policy is consulted for a write operation, alternative storage spaces can be defined as fallbacks.
   # These alternatives are used when the available space in the primary storage, as determined by the policy, is exceeded.
   # For instance, if files are initially placed in an NVME storage space and it becomes full, the system can fall back
   # to an alternative space, such as an HDD. If the HDD space is also exhausted, another fallback, like OLDHDD, can be tried.
   # If all alternative spaces are exhausted, the policy will revert to the initially selected space, and the placement
   # operation will fail due to insufficient space.
   # It is important that all spaces have a proper nominalsize configuration since this is used to determine which one can
   # be used. It is also important to have the full layout configurations for each alternative space because they cannot be taken from
   # directory based sys.forced.* attributes, some for IO policies.


   # define as default the NVME space
   eos space config default space.policy.space=NVME

   # define the HDD space as alternative to the NVME space
   eos space config NVME space.policy.altspaces=HDD,OLDHDD

   # define several alternative spaces HDD,OLDHDD if the NVME space has no nominal bytes left
   # HDD is tried first and if there are nominal bytes left that one is taken
   eos space config NVME space.policy.altspaces=HDD,OLDHDD

Example 3 describes the configuration of alternative spaces. An alternative space is selected if the configured target space for a
file has no space left and there is an *space.policy.altspaces* entry in the configured target space. Read the comment on top of this example
for more information!

Storage Tiering
^^^^^^^^^^^^^^^
Currently we support two tiering scenarios:

1) conversion on read

.. code-block:: bash

   flash <= disk

When a file is stored on **flash**, it should be read there, when a file is stored on **disk**, it should before
the read moved to the **flash** space before the file is opened by the application. Since this applies to existing files
the policy is not defined on the space level but on parent directories via an extended attribute.

An example configuration who move the file to **flash** with a single replica layout would be:

.. code-block:: bash

   attr set sys.forced.readconversion=flash:00650012

This can be combined with an **LRU** policy which moves files of certain sizes and access times to the **disk** tier.
As a result you have a dynamic tiering setup where **active** files are on the **flash** space, while
cold data is on the **disk** tier (which could be for example erasure encoded).

2) conversion on update  :

.. code-block:: bash

   flash <= disk(ec)

When a file is stored on an erasure coded space **disk**, it is not possible to update files. This tiering
option allows to convert a file from an erasure coded layout to a replica based layout e.g. on the **flash** space and
files get updated there. Also this policy is defined on the parent directory via an extended attribute:

.. code-block:: bash

   attr set sys.forced.updateconversion=flash:00650012

This can be again combined with an **LRU** policy which move certain files (based on size, extension, access time) to
an erasure coded **disk** space.

.. ::note

   Both methods use the EOS converter micro service. The client triggers with an **open** request a conversion
   based on the given policy and receives a WAITRESP in the XRootD protocol. Once the conversion jobs finishes,
   the client receives a response and retries the open. Due to the missing logic in HTTP clients, this works only
   with XRootD protocol!

Local Overwrites
^^^^^^^^^^^^^^^^

The space polcies are overwritten by the local extended attribute
settings of the parent directory


.. epigraph::

    ================= =======================================================
    key               local xattr
    ================= =======================================================
    layout            sys.forced.layout, user.forced.layout
    nstripes          sys.forced.nstripes, user.forced.nstripes
    checksum          sys.forced.checksum, user.forced.checksum
    blockchecksum     sys.forced.blockchecksum, user.forced.blockchecksum
    blocksize         sys.forced.blocksize, user.forced.blocksize
    iopriority        sys.forced.iopriority:r\|w
    iotype            sys.forced.iotype:r\|w
    bandwidth         sys.forced.bandwidth:r\|w
    schedule          sys.forced.schedule:r\|w
    ================= =======================================================


Deleting space policies
^^^^^^^^^^^^^^^^^^^^^^^

Policies are deleted by setting a space policy with
[value=remove]{.title-ref} e.g.

.. code-block:: bash

   # delete a policy entry
   eos space config default space.policy.layout=remove

   # delete an application bandwidth entry
   eos space config default space.bw.myapp=remove


Displaying space policies
^^^^^^^^^^^^^^^^^^^^^^^^^

Policies are displayd using the `space status` command:

.. code-block:: bash

   eos space status default

   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   autorepair                       := off
   ...
   policy.blockchecksum             := crc32c
   policy.blocksize                 := 1M
   policy.checksum                  := adler
   policy.layout                    := replica
   policy.nstripes                  := 2
   policy.bandwidth:r               := 100
   policy.bandwidth:w               := 200
   policy.iotype:w                  := direct
   policy.iotype:r                  := direct
   ...
   bw.myapp                         := 100
   bw.eoscp                         := 200
   ...


.. index::
   pair: Interfaces; Conversion policies

Automatic Conversion Policies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Automatic policy conversion policies allow to trigger a conversion job
under two conditions:

*   a new file is created with a complete layout (all required
    replicas/stripes are created) (use case TIERING/IO optimization)
*   an existing file is injected with a complete layout (all required
    replicas/stripes are created) (use case TAPE recall)
*   an existing file is accessed  (use case TIERING)

Automatic conversion policy hooks are triggered by the
ReplicationTracker. You find conversions triggerd in the
**ReplicationTracker.log** logfile.

To use automatic conversion hooks one has to enable policy conversion in
the **default** space:

.. code-block:: bash

   eos space config default space.policy.conversion=on


To disable either remove the entry or set the value to off:

.. code-block:: bash

   #remove
   eos space config default space.policy.conversion=remove
   #or disable
   eos space config default space.policy.conversion=off

It takes few minutes before the changed state takes effect!

To define a policy conversion whenever a file is uploaded for a specific
space you configure:

.. code-block:: bash

   # whenever a file is uploaded to the space **default** a conversion is triggered into the space **replicated** using a **replica::2** layout.
   eos space config default space.policy.conversion.creation=replica:2@replicated

   # alternative declaration using a hex layout ID
   eos space config default space.policy.conversion.creation=00100112@replicated

Also make sure that the converter is enabled:

.. code-block:: bash

   # enable the converter
   eos space config default space.converter=on


To define a policy conversion whenever a file is injected into a
specific space you configure:

.. code-block:: bash

   # whenever a file is injected to the space **ssd* a conversion is triggered into the space **spinner** using a **raid6:10** layout.
   eos space config ssd space.policy.conversion.injection=raid6:10@spinner

   # alternative declaration using a hex layout ID: replace raid6:10 with the **hex layoutid** (e.g. see file info of a file).


.. warning:: You cannot change the file checksum during a conversion job! Make sure source and target layout have the same checksum type!

You can define a policy when a file has been accessed (optional with certain size constraints) in a given space:

   # whenever a file is accessed in the space **ec** a conversion is triggered into the space **ssd** using a **replica:2** layout.
   eos space config ssd space.policy.conversion.injection=replica:2@ssd

You can define a minimum or maximum size criteria to apply automatic
policy conversion depending on the file size.

.. code-block:: bash

   # convert files on creation only if they are at least 100MB
   eos space config ssd space.policy.conversion.creation.size=>100000000

   # convert files on creation only if they are smaller than 1024 bytes
   eos space config ssd space.policy.conversion.creation.size=<1024

   # convert files on injection only if they are bigger than 1G
   eos space config ssd space.policy.conversion.injection.size=>1000000000

   # convert files on injection only if they are smaller than 1M
   eos space config ssd space.policy.conversion.injection.size=<1000000

   # convert files on access only if they are bigger than 1G
   eos space config ec space.policy.conversion.access.size=>1000000000

   # convert files on access only if they are smaller than 1M
   eos space config ec space.policy.conversion.access.size=<1000000

.. index::
   pair: Shared Filesystem; Redirection


Shared Filesystem Redirection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When all FSTs in a space store data into a shared filesystem and clients
might have access to all the data for reading, one can enable the
redirection to a local filesystem:

.. code-block:: bash

   # define the local redirection policy in the given space called 'nfs'
   eos space config nfs space.policy.localredirect=1
   # or
   eos space config nfs space.policy.localredirect=always

   # define local redirection on a per directory basis
   eos attr set sys.forced.localredirect.nfs=1
   # or
   eos attr set sys.forced.localredirect.nfs=always

Please note: a space defined policy overwrites any directory policy.

Local redirection is currently supported for single replica files. It is
disabled for PIO access with *eoscp* (default)), but works with *xrdcp*
and *eoscp -0*. If the client does not see the shared filesystem, the
client will fall back to the MGM and read with the FST (due to an XRootD bug
this is currently broken). If the client
sees the shared filesystem but cannot read it, the client will fail.

One can manually select/disable local redirection using a CGI tag:

.. code-block:: bash

   # enable local redirection via CGI
   root://localhost//eos/shared/file?eos.localredirect=1

   # disable local redirection via CGI
   root://localhost//eos/shared/file?eos.localredirect=0

Redirections are accounted in the *eos ns stat* accounting as failed and
successful redirection on open:

.. code-block:: bash

   eos ns stat | grep RedirectLocal
   all OpenFailedRedirectLocal             0     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-
   all OpenRedirectLocal                  14     0.00     0.00     0.00     0.00     -NA-      -NA-     -NA-     -NA-

.. index::
   pair: Interfaces; IO Priorities

If you have certain machines which have access to the shared filesystem, but not all of them, you can configure local redirection as 'optional'.

.. code-block:: bash

   # define the local redirection policy in the given space called 'nfs' as optional
   eos space config nfs space.policy.localredirect=optional

Then local redirection will occur only on clients which have the XRD_APPNAME or EOSAPP environment variable (for xrootd and eos transfers, respectively)
set to '...#<sharedfsname>', which must match the 'sharedfs' property defined on FSTs which use a shared filesystem for backend storage.
E.g. if you have a shared filesystem called nfs1 only clients with application tag '...#nfs1' will receive a local redirect!

IO Priorities
--------------

IO priorities are currently only supported by devices using the CFQ
(CentOS7) or BFQ (Centos8) scheduler for reads and direct writes. You
can figure out which scheduler is used by inspecting:

.. code-block:: bash

   cat /sys/block/*/queue/scheduler


Supported IO Priorities
^^^^^^^^^^^^^^^^^^^^^^^^
.. epigraph::

    ================== =============
    name               level
    ================== =============
    real-time          0-7
    best-effort        0-7
    idle               0
    ================== =============

.. index::
   pair: IO Priorities;  Real-time


Real-time Class
"""""""""""""""

This is the real-time I/O class. This scheduling class is given higher
priority than any other class: processes from this class are given first
access to the disk every time. Thus, this I/O class needs to be used
with some care: one I/O real-time process can starve the entire system.
Within the real-time class, there are 8 levels of class data (priority)
that determine exactly how much time this process needs the disk for on
each service. The highest real-time priority level is 0; the lowest is
7. The priority is defined in EOS f.e. as *rt:0* or *rt:7*.

.. index::
   pair: IO Priorities;  Best-Effort

Best-Effort Class
"""""""""""""""""""
This is the best-effort scheduling class, which is the default for any
process that hasn\'t set a specific I/O priority.The class data
(priority) determines how much I/O bandwidth the process will get.
Best-effort priority levels are analogous to CPU nice values. The
priority level determines a priority relative to other processes in the
best-effort scheduling class. Priority levels range from 0 (highest) to
7 (lowest). The priority is defined in EOS f.e. as *be:0* or *be:4*.

.. index::
   pair: IO Priorities;  Idle

Idle Class
"""""""""""""""

This is the idle scheduling class. Processes running at this level get
I/O time only when no one else needs the disk. The idle class has no
class data, but the configuration requires to configure it in EOS as
*idle:0* . Attention is required when assigning this priority class to a
process, since it may become starved if higher priority processes are
constantly accessing the disk.

.. index::
   pair: IO Priorities;  Setting Priorities

Setting IO Priorities
^^^^^^^^^^^^^^^^^^^^^^

IO priorities can be set in various ways:

.. code-block:: bash

   # via CGI if the calling user is member of the operator role e.g. make 99 member of operator role
   eos vid set membership 99 -uids 11
   # use URLs like "root://localhost//eos/higgs.root?eos.iopriority=be:0"

   # as a default space policy for readers
   eos space config default space.policy.iopriority:r=rt:0
   # as a space policy
   eos space config erasure space.policy.iopriority:w=idle:0

   # as a default application policy e.g. for application foo writers
   eos space config default space.iopriority:w.app:foo=be:4

   # as a space application policy e.g. for application bar writers
   eos space config erasure space.iopriority:w.app:bar=be:7


The CGI (if allowed via the operator role) is overruling any other
priority configuration. Otherwise the order of evaluation is shown as in
the block above.

For handling of policies in general (how to show, configure and delete)
refer to Space Policies.

.. index::
   pair: Interfaces; Quota
   pair: CLI; eos quota

Quota
-----

The EOS quota system provides user, group and project quota similiar to
filesystems like EXT4, XFS ... e.g. quota is expressed as max. number of
inodes(=files) and maximum volume. The implementation of EOS quota uses the
given inode limit as hard quota, while volume is applied as soft quota e.g.
it can be slightly exceeded.

Quota is attached to a so called 'quota node'. A quota node defines the
quota rules and counting for a subtree of the namespace. If the subtree
contains another quota node in a deeper directory level quota is rooted
on the deeper node.

As an example we can define two quota nodes:

.. epigraph::

   ============ =======================
   Node         Path
   ============ =======================
   Quota Node 1 /eos/lhc/raw/
   Quota Node 2 /eos/lhc/raw/analysis/
   ============ =======================

A file like ``/eos/lhc/raw/2013/raw-higgs.root`` is accounted for in the first
quota node, while a file ``/eos/lhc/raw/analysis/histo-higgs.root`` is
accounted for in the second quota node.

.. index::
   pair: Quota; Listing Quotas


The quota system is easiest explained lookint at the output of
a **quota** command in the EOS shell:

.. code-block:: bash

   eosdevsrv1:# eos quota
   # _______________________________________________________________________________________________
   # ==> Quota Node: /eos/dev/2rep/
   # _______________________________________________________________________________________________
   user       used bytes logi bytes used files aval bytes aval logib aval files filled[%]  vol-status ino-status
   adm        2.00 GB    1.00 GB    8.00 -     2.00 TB    1.00 TB    1.00 M-    0.00       ok         ok

The above configuration defines user quota for user ``adm`` with 1 TB of volume
quota and 1 Mio inodes under the directory subtree ``/eos/dev/plain``.
As you may notice EOS distinguishes between logical bytes and (physical) bytes.
Imagine a quota node subtree is configured to store 2 replica for each file,
then a 1 TB quota allows you effectivly to store 2 TB of raw data.

.. important::
   All quotas set via the 'quota set' command define volume in raw bytes
   by default, and EOS displays both logical and raw bytes values, based
   on the layout definition on the quota node. The environment variable
   'EOS_MGM_QUOTA_SET_BY_LOGICAL', when set, changes this default to be
   in terms of logical bytes instead.

.. important::
   If a quota node contains files with mixed layout, say 2 replica and RAID 6,
   the raw and logical bytes usage will reflect it accordingly, i.e., you'd see
   less than twice the logical bytes as raw bytes used. The quota system
   enforces the *logical bytes usage*, such that in a directory with RAID6 layout,
   adding files of 2 replica layout may cause the raw usage to exceed the amount
   set in the quota node without actually running out of quota. You only run out
   of quota when the logical space is exceeded.

The volume and inode status is displayed as 'ok' if there is quota left for
volume/inodes. If there is less than **5%** left, 'warning' is displayed,
if there is none left 'exceeded'. If volume and/or inode quota is set to 0
'ignored' is displayed. In this case a quota setting of 0 signals not to apply
the quota however if both are '0' the referenced UID/GID has no quota.

There are three types of quota defined in EOS: user, group & project quota!

.. index::
   pair: Quota; User Quota

User Quota
^^^^^^^^^^

User quota defines volume/inode quota based on user id  UID.
It is possible to combine user and group quota on a quota node.
In this case both have to 'ok' e.g. provide enough space for a file placmment.

.. index::
   pair: Quota; Group Quota

Group Quota
^^^^^^^^^^^
Group quota defines volume/inode quota based on group id GID.
As described before it is possible to combine group and user quota.
In this case both have to allow file placement.

.. index::
   pair: Quota; Project Quota

Project Quota
^^^^^^^^^^^^^
Project quota books all volume/inode usage under the project subtree to a single
project account. E.g. the recycle bin uses this quota type to measure a subtree
size. In the EOS shell interface project quota is currently defined setting
quota for group 99:

.. code-block:: bash

   eosdevsrv1:# eos set -g 99 -p /eos/lhc/higgs-project/ -v 1P -i 100M

.. index::
   pair: Quota; Space Quota

Space Quota
^^^^^^^^^^^
It is possible to set a physical space restriction using the space parameter **nominalsize**

.. code-block:: bash

   # restrict the physical space usage to 1P
   eosdevsrv1:# eos space config default space.nominalsize=1P

The restriction is only used, if the connected user is not in the **sudoer** list. The current usage and space setting is
cached for 30s e.g. it might take up to 30s until any change may take effect.

.. index::
   pair: Quota; Quota Enforcement

Quota Enforcement
^^^^^^^^^^^^^^^^^
Quota enforcement is applied when new files are placed and when files in RW mode
are closed e.g. EOS can reject to store a file if the quota exceeds during an
upload. If user and group quota is defined, both are applied.

Quota Command Line Interface
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. index::
   pair: Quota; List Quota

List Quota
""""""""""
To see your quota as a user use:

.. code-block:: bash

   eosdevsrv1:# eos quota

To see quota of all users (if you are an admin):

.. code-block:: bash

   eosdevsrv1:# eos quota ls

To see the quota node for a particular directory/subtree:

.. code-block:: bash

   eosdevsrv1:# eos quota ls /eos/lhc/higgs-project/

 .. index::
   pair: Quota; Set Quota

Set Quota
""""""""""

The syntax to set quota is:

.. code-block:: bash

   eos quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] -p <path>

The <uid>, <gid> parameter can be numerica or the real name. Volume and Inodes
can be specified as **1M**, **1P** etc. or a plain number.

.. ::note

   To set project quota use GID 99!

.. index::
   pair: Quota; Delete Quota

Delete Quota
"""""""""""""

A quota setting can be removed using:

.. code-block:: bash

   eos quota rm -u <uid> |-g <gid> -p <path>

One has to specify to remove the user or the group quota, it is not possible
to remove both with a single command.

.. index::
   pair: Quota; Delete Quota Node

Delete Quota Node
"""""""""""""""""
Sometimes it is necessary to remove completely a quota node.
This can be done via:

.. code-block:: bash

   eos quota rmnode -p <path>

The command will ask for a security code. Be aware the quota is not recalculated
from scratch if the deletion of a node would now leave the accounting to an
upstream node.


.. index::
   pair: Interfaces; Permissions
   pair: CLI; eos chmod
   pair: CLI; eos chown
   pair: CLI; eos acl


Permissions
-----------

Overview
^^^^^^^^

The EOS permission system is based on a combination of **ACLs**  and **POSIX** permissions.

There are two major differences to traditional storage systems:

#. Files don't carry their permissions (only the ownership for quota accounting). They inherit the permissions from the parent directory.
#. Permissions are only checked in the direct parent, EOS is not walking through the complete directory hierarchy.
#. For ACLs, a directory ACL applies to all files, except those who have their own ACL.

.. index::
   pair: Permissions; UNIX Permissions

UNIX Permissions

EOS allows to set user, group and other permissions for read write and browsing defined
by ``'r'(4), 'w'(2), 'x'(1)``, e.g. ``777 ='rwx'``.

Unlike in POSIX the S_ISGID (2---) indicates that any new directory created should automatically inherit all the
extended attributes defined at creation time from the parent directory.

If the parent attributes are changed after creation, they are not automatically
applied to its children. The inheritance bit is always added to any *chmod* automatically. >

All modes for *chmod* have to be given in octal format. For details see **eos chmod --help**.

The S_ISVTX bit (1---) is displayed whenever a directory has any extended attribute defined.

.. index::
   pair: Permissions; ACLs

ACLs
^^^^
ACLs are defined on the directory or file level via the extended attributes.

.. code-block:: bash

   sys.acl=<acllist>
   user.acl=<acllist>

.. note::

   For efficiency, file-level ACLs should only be used sparingly, in favour of directory-level ACLs.

The sys.acl attribute can only be defined by SUDO members or by users which have an 'A' grant in the existing ACL of the same directory/file.
The user.acl attribute can be defined by the **owner** or SUDO members. It is only evaluated if the sys.eval.useracl attribute is set.

The sys.acl/user.acl attributes are inherited from the parent at the time a directory is created. Subsequent changes to a directory's ACL
do not automatically apply to child directories.

<acllist> is defined as a comma separated list of rules:

.. code-block:: bash

   <acllist> = <rule1>,<rule2>...<ruleN>

A rule is defined in the following way:

.. code-block:: bash

   <rule> = u:<uid|username>|g:<gid|groupname>|egroup:<name>|z::{rwxXomqciaA(!d)(+d)(!u)(+u)}

A rule has three colon-separated fields. It starts with the type of rule:
User (u), Group (g), eGroup (egroup) or all (z). The second field specifies the name or
the unix ID of user/group rules and the eGroup name for eGroups
The last field contains the rule definition.


.. index::
   pair: ACLs; Rule Syntax


The following tags compose a rule:

.. epigraph::

   === =========================================================================
   tag definition
   === =========================================================================
   r   grant read permission
   w   grant write permission
   x   grant browsing permission
   m   grant change mode permission
   !m  forbid change mode operation
   !d  forbid deletion of files and directories
   +d  overwrite a '!d' rule and allow deletion of files and directories
   !u  forbid update of files
   +u  overwrite a '!u' rule and allow updates for files
   q   grant 'set quota' permissions on a quota node
   c   grant 'change owner' permission on directory children
   i   set the immutable flag
   a   grant archiving permission
   A   grant sys.acl modification permissions
   X   grant sys.* modification permissions (does not include sys.acl!)
   === =========================================================================




Actually, every single-letter permission with the exception of change owner (c) can
be explicitely denied ('!'), e.g. '!w!r, re-granted ('+'). Change owner
permission is only explicitly enabled on grant, so it is denied by default.
Denials persist after all other rules have been evaluated, i.e. in 'u:fred:!w!r,g:fredsgroup:wrx' the user "fred"
is denied reading and writing although the group he is in has read+write access.
Rights can be re-granted (in sys.acl only) even when denied by specyfing e.g. '+d'. Hence,
when sys.acl='g:admins:+d' and then user.acl='z:!d' are evaluated,
the group "admins" is granted the 'd' right although it is denied to everybody else.

A complex example is shown here:

.. code-block:: bash

   sys.acl="u:300:rw!u,g:z2:rwo,egroup:eos-dev:rwx,u:dummy:rwm!d,u:adm:rwxmqc"

   # user id 300 can read + write, but not update
   #
   # group z2 can read + write-once (create new files but can't delete)
   #
   # members of egroup 'eos-dev' can read & write & browse
   #
   # user name dummy can read + write into directory and modify the permissions
   # (chmod), but cannot delete directories inside which are not owned by him.
   #
   # user name adm can read,write,browse, change-mod, set quota on that
   # directory and change the ownership of directory children

.. note::

   Write-once and '!d' or '!u' rules remove permissions which can only be regained
   by a second rule adding the '+u' or '+d' flag e.g. if the matching user ACL
   forbids deletion it is not granted if a group rule does not forbid deletion!


It is possible to write rules, which apply to everyone:

.. code-block:: bash

   sys.acl="z:i"

   # this directory is immutable for everybody


The user.acl (if defined) is evaluated after the sys.acl, e.g. If we have:

.. code-block:: bash

    sys.acl=’g:admins:+d’ and user.acl=’z:!d’

i.e., the group “admins” is granted the 'd' right although it is denied to everybody else in the user.acl.


Finally the ACL can be set via either of the following 2 commands, see `eos acl --help` or `eos attr set --help`. From the operational perspective one may prefer the former command as it acts specifically on the element we change (egroup, user ... etc.) instead of re-specifying the whole permission set of rules (`eos attr set` case). `eos acl` set of commands also allow for specific position to place the rule in when creating or modifying a rule. By default rules are appended at the end of the acl, `--front` flag allows to place a rule at the front, and an integer position starting from 1 (which is equivalent to `--front`) can also be used to explicitly move a rule to a specific position via the `--position` argument.

.. note::

   From EOS version 5.1.14 and later the behavior of recursive ACL set has changed, keeping in view of very large directory trees. Previously any recursive ACL set command ensures that no directory creation happens during the ACL set, while this is synchronous for very large trees with millions of directories, one can end up blocking everything else. This is moved to fine-grained locks, with the downside that a directory created during the time ACLs are applied may not see the ACLs being applied, in case their parent hasn't inherited yet. The old synchronous behavior can be restored with a `--with-synchronous-write-lock` flag, though it is really not recommended for very large tree hierarchies.

.. code-block:: bash

   eos attr set sys.acl=<rule_a>,<rule_b>.. /eos/mypath
   eos acl --sys <rule_c> /eos/mypath
   eos acl --front <rule_d> /eos/mypath
   eos acl --position 2 <rule_f> /eos/mypath

The ACLs can be listed by either of these commands as well:

.. code-block:: bash

   eos attr ls /eos/mypath
   eos acl -l /eos/mypath


If the operator uses the `eos acl --sys <rule> /eos/mypath` command, the `<rule>` is composed as follows:
`\[u|g|egroup\]:<identifier>\[:|=\]<permission>` . The second delimiter `[:|=]` can be a `:` for modifying permissions
or "=" for setting/overwriting permission. Finally a <permission> itself can be added using the "+" or removed using the "-" operators.

For example:

.. code-block:: bash

   $ eos attr ls /eos/mypath
   sys.acl="u:99999:rw,egroup:mygroup:rw"
   #
   # if you try to set the deletion permission using ':' modification sign:
   $ eos acl --sys 'egroup:mygroup:!d' /eos/mypath
   #
   # you will get an error since there is no deletion permission defined yet in the original ACL (i.e. nothing to be modified), but
   # one can add this new !d permission to the existing ACLs by the '+' operator:
   $ eos acl --sys 'egroup:mygroup:+!d' /eos/mypath
   #
   -->
   #
   $ eos attr ls /eos/mypath
   sys.acl="egroup:mygroup:rw!d,u:99999:rw"
   #
   # one can also remove this permission by the '-' operator:
   $eos acl --sys 'egroup:mygroup:-!d' /eos/mypath
   -->
   #
   $ eos attr ls /eos/mypath
   sys.acl="u:99999:rw,egroup:mygroup:rw"
   #
   # or set completely new permission, overwriting all by '=':
   eos acl --sys 'egroup:mygroup=w' /eos/mypath
   -->
   #
   $ eos attr ls /eos/mypath
   sys.acl="u:99999:rw,egroup:mygroup:w"
   # append a new rule to the end
   $ eos acl --sys u:1002=\!w /eos/mypath
   $ eos attr ls /eos/mypath
   sys.acl="u:99999:rw,egroup:mygroup:rw,u:1002:!w"

   # Move a rule to the front, the full rule needs to be specified
   $ eos acl --front egroup:mygroup=rw /eos/mypath
   $ eos attr ls /eos/mypath
   sys.acl="egroup:mygroup:rw,u:99999:rw,u:1002:!w"

   # Add a new rule at a specific position
   $ eos acl --position 2  egroup:mygroup2=rwx /eos/mypath
   $ eos attr ls /eos/mypath
   sys.acl="egroup:mygroup:rw,egroup:mygroup2:rwx,u:99999:rw,u:1002:!w"


.. note::

   * The "-r 0 0" can be used to map your account with the sudoers role. This has to be assigned to your account on the EOS instance by the service manager, see `eos vid ls`), e.g. `eos -r 0 0 acl --sys 'egroup:mygroup:!d' /eos/mypath`.
   * If no '--sys' or '--user' is specified, by default the `eos acl` sets '--sys' permissions.

.. index::
   pair: Permissions; Validity
   pair: Permissions; Ordering

Validity of Permissions
^^^^^^^^^^^^^^^^^^^^^^^

File Access
"""""""""""
A file ACL (if it exists), or the directory's ACL is evaluated
for access rights.

A user can read a file if the ACL grants 'r' access
to the user's uid/gid pair. If no ACL grants the access,
[the directory's] UNIX permissions are evaluated for a matching 'r' permission bit.

A user can create a file if the parent directory grants 'w' access via the ACL
rules to the user's uid/gid pair. A user cannot overwrite a file if the ACL
grants 'wo' permission. If the ACL does not grant the access, UNIX permissions
are evaluated for a matching 'w' permission bit.

.. note::

   The root role (uid=0 gid=0) can always read and write any file.
   The daemon role (uid=2) can always read any file.

File Deletion
""""""""""""""

A file can be deleted if the parent directory grants 'w' access via the ACL
rules to the user's uid/gid pair. A user cannot delete a file,
if the ACL grants 'wo' or '!d' permission.

.. note::

   The root role (uid=0 gid=0) can
   always delete any file.

File Permission Modification
""""""""""""""""""""""""""""

File permissions cannot be changed, they are automatically inherited from the
parent directory.

File Ownership
""""""""""""""

A user can change the ownership of a file if he/she is member of the SUDO group.
The root, admin user and admin group role can always change the ownership of a
file. See **eos chown --help**  for details.

Directory Access
""""""""""""""""

A user can create a directory if they have the UNIX 'wx' permission, or the ACL
rules grant the 'w' or 'wo' permission. The root role can always create any directory.

A user can list a directory if the UNIX permissions grant 'rx' or the ACL
grants 'x' rights.

.. note::

   The root, admin user and admin group role can always
   browse directories.

Directory Deletion
""""""""""""""""""

A user can delete a directory if he/she is the owner of the directory.
A user can delete a directory if he/she is not the owner of that directory
in case 'UNIX 'w'permission are granted and '!d' is not defined by a matching
ACL rule.

.. note::

   The root role can always delete any directory.

.. warning::

   Deletion only works if directories are empty!

Directory Permission Modification
""""""""""""""""""""""""""""""""""

A user can modify the UNIX permissions if they are the owner of the file
and/or the parent directory ACL rules grant the 'm' right.

.. note::

   The root, admin
   user and admin group role can always modify the UNIX permissions.

Directory ACL Modification
""""""""""""""""""""""""""

A user can modify a directory's system ACL, if they are a member of the SUDO group or the have an A grant.
A user can modify a directory's user ACL, if they are the owner of the directory or
a member of the SUDO group.

Directory Ownership
""""""""""""""""""""

The root, admin user and admin group role can always change the directory
owner and group.
A normal user can change the directory owner if the system ACL allows this, or if the user ACL allows it *and* they change the owner to themselves.

.. warning::

   Otherwise, only priviledged users can alter the ownership.

Quota Permission
""""""""""""""""

A user can do 'quota set' if he is a sudoer, has the 'q' ACL permission set on
the quota node or on the proc directory ``/eos/<instance>/proc``.

Richacl Support
""""""""""""""""

On systems where "richacl"s (a more sophisticated ACL model derived from NFS4 ACLs) are supported, e.g. CentOS7,
the translation between EOS ACLs and richacls is by nature incomplete and not always two-ways:

an effort is made for example to derive a file's or directory's :D: (RICHACL_DELETE) right from the parent's 'd' right,
whereas the :d: (RICHACL_DELETE_CHILD) right translates to the directory's own 'd'.
This helps applications like samba; however, setting
:D: (RICHACL_DELETE) on a directory does not affect the directory's parent as permissions for individual
objects cannot be expressed in EOS ACLs;

the EOS 'm' (change mode) right becomes :CAW: (RICHACE_WRITE_ACL|RICHACE_WRITE_ATTRIBUTES|RICHACE_WRITE_NAMED_ATTRS);

the EOS 'u' (update) right becomes :p: (RICHACE_APPEND_DATA), although this is not really equivalent. It implies that :w: (RICHACE_WRITE_DATA) only grants writing of new files,
not rewriting parts of existing files.

Richacls are created and retrieved using the {get,set}richacl commands and the relevant richacl library functions
on the fusex-mounted EOS tree. Those utilities act on the user.acl attribute and ignore sys.acl.


How to setup a shared scratch directory
"""""""""""""""""""""""""""""""""""""""

If a directory is group writable one should add an ACL entry for this group
to forbid the deletion of files and directories to non-owners and allow
deletion to a dedicated account:

E.g. to define a scratch directory for group 'vl' and the deletion
user 'prod' execute:

.. code-block:: bash

   eos attr set sys.acl=g:vl:!d,u:prod:+d /eos/dev/scratchdisk

The default unix way is to use the VTX bit using the `chmod` interface!

How to setup a shared group directory
""""""""""""""""""""""""""""""""""""""

A directory shared by a <group> with variable members should be setup like this:

.. code-block:: bash

   chmod 550 <groupdir>
   eos attr set sys.acl="egroup:<group>:rw!m"

.. index::
   pair: Permissions; Sticky Ownership

Sticky Ownership
""""""""""""""""

The ACL tag sys.owner.auth allows to tag clients acting as the owner of a directory. The value normally is composed by the authentication method and the user name or can be a wildcard.
If a wild card is specified, everybody resulting in having write permission can use the sticky ownership and write into a directory on behalf of the owner e.g. the file is owned by the directory
owner and not by the authenticated client and quota is booked on the directory owner.

.. code-block:: bash

   eos attr set sys.owner.auth="krb5:prod"
   eos attr set sys.owner.auth="*"


.. index::
   pair: Permissions; Sys Masks


Permission Masks
""""""""""""""""

A permission mask which is applied on all chmod requests for directories can be defined via:

.. code-block:: bash

   sys.mask=<octal-mask>

Example:

.. code-block:: bash

   eos attr set sys.mask="770"
   eos chmod 777 <dir>
   success: mode of file/directory <dir> is now '770'

When the mask attribute is set the !m flag is automatically disabled even if it is given in the ACL.

.. index::
   pair: Permissions; eos acl

Space ACLs
""""""""""

It is possible to define an extra set of ACLs on the space level, which applies to all directories referencing this space via *sys.forced.space*. If no space is referenced in a directory, ACLs from the default space will be added.

ACLs have 4 add-on modes:
* '=<' first evaluated (position left)
* '=>' last evaluated (position right)
* '=|'use if no other sys.acl is present in a directory
* '=' always overwrite directory sys.acl

For example ACLs are configured in a space like:

.. code-block:: bash

                # insert space ACL on the left position (first evaluated)
                space config default space.attr.sys.acl=<u:poweruser:rwxqmcXA
                # insert space ACL on the right position (last evalutated)
                space config default space.attr.sys.acl=>u:poweruser:rwxqmcXA
                # user space ACL if there is no sys.acl on the referenced directory
                space config default space.attr.sys.acl=|u:poweruser:rwxqmcXA
                # overwrite all directory ACLs with the space ACL
                space config default space.attr.sys.acl=u:poweruser:rwxqmxcXA

Space ACLs are removed in the usual manner:

.. code-block:: bash

                # remove space ACL
                space config rm default space.attr.sys.acl

Space ACLs are shown using:

.. code-block:: bash

                # show space configuration
                space status default

ACL CLI
"""""""

To provide atomic add,remove and replacement of permissions one can take advantage of the ``eos acl`` command instead of modifying directly the `sys.acl` attribute:

.. code-block:: bash

   Usage: eos acl [-l|--list] [-R|--recursive][--sys|--user] <rule> <path>

       --help           Print help
   -R, --recursive      Apply on directories recursively
   -l, --lists          List ACL rules
       --user           Set user.acl rules on directory
       --sys            Set sys.acl rules on directory
   <rule> is created based on chmod rules.
   Every rule begins with [u|g|egroup] followed with : and identifier.

   Afterwards can be:
   = for setting new permission .
   : for modification of existing permission.

   This is followed by the rule definition.
   Every ACL flag can be added with + or removed with -, or in case
   of setting new ACL permission just enter the ACL flag.

.. index::
   pair: Permissions; Anonymous Access
   pair: Permissions; Public Access

Anonymous Access
""""""""""""""""

Anonymous access can be allowed by configuring unix authentication (which maps by default everyone to user nobody). If you want to restrict anonymous access to a certain domain you can configure this via the ``access`` interface:

.. code-block:: bash

   eos access allow domain nobody@cern.ch

As an additional measure you can limit the deepness of the directory tree where anonymous access is possible using the ``vid`` interface e.g. not more than 4 levels:

.. code-block:: bash

   eos vid publicaccesslevel 4


The default value for the publicaccesslevel is 1024.


.. index::
   pair: Interfaces; Routing
   pair: CLI; eos route


Routing
-------

The EOS route system provides a method to redirect paths within an existing namespace to an external namespace.
It can be thought of as symbolic links that allow clients to connect to another EOS instance.

This can be used to create a parent MGM that contains references to other EOS instances in a tree like structure,
or to connect EOS namespaces together in a mesh like manner.

`vid` policy and other access control still applies as if a user were connecting directly.

A route is defined as a path, that maps to a remote hostname and port, that is the MGM of a remote EOS namespace.
Access to this path is via a redirect at the HTTP or xrootd level, and will incur some latency.

When the latency penalty of the redirect is not desired, it's better to cache the redirect or use an autofs(8)
or similar automounting solution for the paths.

The link always links to the root of the connected namespace.

As an example we can define three routes:

.. epigraph::

   ====================================== =======================
    Path                                   Destination
   ====================================== =======================
   /eos/test-namespace-1                   test-mgm-1:1094:8000
   /eos/test-namespace-2                   test-mgm-2:1094:8000
   /eos/test-namespace-1/test-namespace-3  test-mgm-3:1094:8000
   ====================================== =======================

Changing directory to `/eos/test-namespace-1`, would be akin to connecting directly to the mgm at `test-mgm-1:1094`.

.. code-block:: bash

    EOS Console [root://localhost] |/> route link /eos/test-namespace-1 test-mgm-1:1094:8000
    EOS Console [root://localhost] |/> route link /eos/test-namespace-2 test-mgm-2:1094:8000
    EOS Console [root://localhost] |/> route link /eos/test-namespace-1/test-namespace-3 test-mgm-3:1094:8000
    EOS Console [root://localhost] |/> route ls
    /eos/test-namespace-1/ => test-mgm-1:1094:8000
    /eos/test-namespace-1/test-namespace-3/ => test-mgm-3:1094:8000
    /eos/test-namespace-2/ => test-mgm-2:1094:8000


The above configuration defines defines the path configuration in the example above.

If a port combination is not specified, the route assumes a xrootd port of 1094, and a http port of 8000.

Creating a link
^^^^^^^^^^^^^^^

A link is created using the `route link` command. It takes the option of a path and a destination host. Optional
specification includes the MGM's xrootd port, and the MGM's http port. Unspecified, they default to 1094 and 8000
respectively.

.. code-block:: bash

    EOS Console [root://localhost] |/> route link /eos/test-path eosdevsrv2:1094:8000
    EOS Console [root://localhost] |/> route ls
    /eos/test-path/ => eosdevsrv2:1094:8000


Removing a link
^^^^^^^^^^^^^^^^

A link is removed using the `route unlink` command. Only a path needs to be specified.

.. code-block:: bash

    EOS Console [root://localhost] |/> route unlink /eos/test-namespace-1


Displaying current links
^^^^^^^^^^^^^^^^^^^^^^^^

The `route ls` command shows current active links. An asterix is displayed in
front of the MGM node which acts as a master for that paricular mapping.

.. code-block:: bash

    EOS Console [root://localhost] |/> route ls
    /eos/test-namespace-1/ => *test-mgm-1:1094:8000
    /eos/test-namespace-1/test-namespace-3/ => *test-mgm-3:1094:8000
    /eos/test-namespace-2/ => *test-mgm-2:1094:8000


Making links visible to clients
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

EOS will not display the link in a directory listing. This means it's possible to have an invisible link, and
stat or fileinfo commands will fail against the link path.

Creating a directory for the path will make it visible to clients, but accounting information will not be accurate until
a client changes into the path and queries again.

Connecting clients
^^^^^^^^^^^^^^^^^^^

HTTP and xrootd clients can effectively connect to the top level MGM and will automatically follow redirects. It is
recommended for performance reasons to connect FUSE clients via an automounter directly to each MGM, and use either
path mounting or bind mounts to replicate the tree structure.

Automounting
^^^^^^^^^^^^

It is possible to convert the output of `route ls` and place it into a map for an automounter or other process to use.

.. index::
   pair: Interfaces; Archiver


Archiver
--------

Archiving allows to store part of the meta-data and data stored on an EOS instance in an external archive.

The *archive daemon* is managing the transfer of files to/from EOS from/to a
remote location offering an XRootD interface.

Before starting the service there are a few configuration parameters that need to be set.

Daemon Configuration
^^^^^^^^^^^^^^^^^^^^
First of all, one needs to set the user account under which the daemon will be
running by modifying the script located at ``/etc/sysconfig/eosarchived``. The
daemon needs to create a series of temporary files during the transfers which
will be saved at the location pointed by the environment variable **EOS_ARCHIVE_DIR**.
Also, the location where the daemon log file is saved can be changed by modifying
the variable **LOG_DIR**.

If none of the above variables is modified then the default configuration is as
follows:

.. code-block:: bash

  export USER=eosarchi
  export GROUP=c3
  export EOS_ARCHIVE_DIR=/var/eos/archive/
  export LOG_DIR=/var/log/eos/archive/

The variables set in ``/etc/sysconfig/eosarchived`` are general ones that apply
to both the daemon process and the individual tranfer proceses spawned later on.

Another file which holds further configurable values is
``/etc/eosarchived.conf``. The advantage of this file is that it can be modified
while the daemon is running and newly submitted transfers will pick-up the new
configuration without the need of a full daemon restart.

The **LOG_LEVEL** variable is set in ``/etc/eosarchived.conf`` and  must be a
string corresponding to the syslog loglevel. The **eosarchived** daemon logs
are saved by default in ``/var/log/eos/archive/eosarchived.log``.

MGM Configuration
^^^^^^^^^^^^^^^^^^

The configuration file for the **MGM** node contains a new directive called
**mgmofs.archivedir** which needs to point to the same location as the
**EOS_ARCHIVE_DIR** defined earlier for the **eosarchived** daemon. The two
locations must match because the **MGM** and the **eosarchived** daemons
communicate between each other using ZMQ and this is the path where any common
ZMQ files are stored.

.. code-block:: bash

  mgmofs.archivedir /var/eos/archive/  # has to be the same as EOS_ARCHIVE_DIR from eosarchived

Another variable that needs to be set for the **MGM** node is the location where
all the archived directories are saved. Care should be taken so that the user
name under which the **eosarchived** daemon runs, has the proper rights to read
and write files to this remote location. This envrionment variables can be set in
the ``/etc/sysconfig/eos`` file as follows:

.. code-block:: bash

  export EOS_ARCHIVE_URL=root://castorpps.cern.ch//castor/cern.ch/archives/

Keytab file generation
""""""""""""""""""""""

Assuming that the **eosarchived** daemon is running under the account *eosarchi*,
then one has to make sure the following files are present at the **MGM**. First of
all, the eos-server.keytab file must include a new entry for the **eosarchi**:

.. code-block:: console

     [root@dev doc]$ ls -lrt /etc/eos-server.keytab
     -r--------. 1 daemon daemon 137 Mar 22  2012 /etc/eos-server.keytab

     [root@dev ~]# xrdsssadmin list /etc/eos-server.keytab
     Number Len Date/Time Created Expires  Keyname User & Group
     ------ --- --------- ------- -------- -------
          2  32 09/17/14 19:25:01 -------- archive eosarchi c3
          1  32 09/17/14 19:24:47 -------- eosinst daemon daemon

The next file that needs to be present is the eos-archive.keytab file which is
going to be used by the **eosarchived** daemon.

.. code-block:: console

     [root@dev ~]# ls -lrt /etc/eos-archive.keytab
     -r--------. 1 eosarchi c3 133 Sep 18 09:48 /etc/eos-archive.keytab

     [root@dev ~]# xrdsssadmin list /etc/eos-archive.keytab
     Number Len Date/Time Created Expires  Keyname User & Group
     ------ --- --------- ------- -------- -------
          2  32 09/17/14 19:25:01 -------- archive eosarchi c3

Some important notes about the **eos-archive.keytab** file:

- if renamed or saved in a different location then the variable **XrdSecSSSKT**
  from ``/etc/sysconfig/eosarchived`` needs to be updated to point to the new
  location/name
- the permissions on this keytab file must match the identity under which the
  eoarchived daemon is running

Futhermore, the **eosarchi** user needs to be added to the **sudoers** list in
EOS so that it can perform any type of operation while creating or transfering
archives.

.. code-block:: console

    EOS Console [root://localhost] |/eos/> vid set membership eosarchi +sudo

As far as the **xrd.cf.mgm** configuration file is concerned, one must ensure
that **sss** authentication has precedence over **unix** when it comes to local
connections:

.. code-block:: bash

   sec.protbind localhost.localdomain sss unix
   sec.protbind localhost             sss unix



.. index::
   pair: Interfaces; Recycle Bin

Recycle Bin
-----------


Overview
^^^^^^^^

The EOS recycle bin allows to define a FIFO policy for delayed file deletion.
This feature is available starting with EOS BERYL.

The recycling bin is time-based and volume based e.g. the garbage directory
performs final deletion after a configurable time delay. The volume in the
recycle bin is limited using project quota. If the recycle bin is full no
further deletion is possible and deletions fails until enough space is available.

The recycling bin supports individual file deletions and recursive bulk
deletions (referenced as object deletions in the following).

The owner of a deleted file or subtree deletion can restore files into the
original location from the recycle bin if he has the required quota. If the
original location is 'occupied' the action is rejected. Using the '-f' flag
the existing location is renamed and the deleted object is restored to the
original name.

If the parent tree of the restore location is incomplete the user is asked
to first recreate the parent directory structure before objects are restored.

If the recycle bin is applicable for a deletion operation the quota is
immediately removed from the original quota node and added to the recycle
quota. Without recycle bin, quota is released once files are physically deleted!

Command Line Interface
^^^^^^^^^^^^^^^^^^^^^^^
If you want to get the current state and configuration of the recycle bin you
run the recycle command:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle

   # _______________________________________________________________________________________________
   # used 0.00 B out of 100.00 GB (0.00% volume / 0.00% inodes used) Object-Lifetime 86400 [s]
   # _______________________________________________________________________________________________

The values are self-explaining.

Dump entire recycle bin configuration
""""""""""""""""""""""""""""""""""""""

If you want to list all the parameter affecting the recycle bin you running
**recycle config --dump**:

.. code-block:: bash

   EOS Console: [root://localhost] |/eos/> recycle config --dump
   enforced=on
   dry_run=yes
   keep_time_sec=172800
   space_keep_ratio=0.95
   low_space_watermark=0
   low_inode_watermark=0
   collect_interval_sec=20
   remove_interval_sec=10

Enable/Disable the recycle bin
""""""""""""""""""""""""""""""

If you want to enable or disable the recycle bin globally you run
**recycle config --enable <on|off>**:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --enable on


Define the object lifetime
""""""""""""""""""""""""""

If you want to configure the lifetime of objects in the recycle bin you run
**recycle config --lifetime <lifetime>**:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --lifetime 86400

<lifetime> can be e.g. just a number 3600, 3600s  (seconds) or 60min
(60 minutes) 1d (one day), 1w (one week), 1mo (one month), 1y (one year) aso.

The lifetime has to be at least 60 seconds!

Define the recycle bin size
"""""""""""""""""""""""""""

If you want to configure the size of the recycle bin you run
**recycle config --size <size>**:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --size 100G

<size> can be e.g. just a number 100000000000, 100000M (mega byte) or 100G (giga byte), 1T (one terra) aso.

The size has to be at least 100G!

Define the inode size of the recycle bin
""""""""""""""""""""""""""""""""""""""""

If you want to configure the number of inodes in the recycle bin you run
**recycle config --inodes <value>[K|M|G]**:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --inodes 1M

<value> can be a number or suffixed with K (1000), M (1.000.0000) or G (1.000.000.000).

It is not mandatory to define the number of inodes to use a recycle bin.

Define an optional threshold ratio
""""""""""""""""""""""""""""""""""

If you want to keep files as long as possible you can set a keep ratio on the
recycle bin:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --ratio 0.8

In this example the recycle bin can be filled up-to 80%. Once it reaches the
watermark it will clean all files matching the given lifetime policy. The
cleaning will free 10% under the given watermark. This option is not
mandatory and probably not always the desired behaviour.

Define dry-run mode for the recycle cleanup
"""""""""""""""""""""""""""""""""""""""""""

If you want the recycle bin to just select and print the entries that would
be deleted according to the given policy enforced, then you can enable the
"dry-run" mode:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --dry-run


In this case the recycle bin would not delete any files that are selected
for clean-up bases either on the size or lifetime policies.

Define a recycle project for a subtree
"""""""""""""""""""""""""""""""""""""""

By default the recycle bin functionality works on user identities. Therefore,
once a user deletes a certain file, that file will end up in the recycle bin
corresponding to the original owner of the file. There are certain situations
where this behaviour makes things hard to restore, for example in project
spaces that are shared by multiple users.

In this case, the admin of the project can set up a so-called "recycle project"
by using the command **reyclce project <path> --acl <optional_acls>**. This
will create a new extended attribute attached to the **<path>** directory
that contains the recycle project identifier. Example:

.. code-block:: bash

   EOS Console [root://localhost] |/eos> recycle project /eos/dev/plain --acl u:1234:rx

Listing the extended attributes of the given path will display a new pair
called **sys.forced.recycleid** that coresponds to the container identifier
of the path.

.. code-block:: bash

   [root://localhost] |/eos> eos attr ls  /eos/dev/plain | grep recycle
   sys.forced.recycleid="1007"
   [root://localhost] |/eos> eos fileinfo /eos/dev/plain | grep Fid
   CUid: 0 CGid: 0 Fxid: 000003ef Fid: 1007 Pid: 3 Pxid: 00000003

This identifier is now used to group all the deletions coming from this
sub-tree inside the recycle bin, in a dedicated location.

.. code-block:: bash
   [root://localhost] |/eos> eos ls -lrt /eos/dev/proc/recycle | grep 1007
   drwx-----+   1 root     root                0 Nov 24 22:37 rid:1007

This identifier is called the "recycle id" and can be used as input for the
various recycle commands that accept such a value.

Bulk deletions
""""""""""""""

A bulk deletion using the recycle bin prints how the deleted files can
be restored:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> rm -r tree

   success: you can recycle this deletion using 'recycle restore 00000000000007cf'

Add recycle policy on a subtree
"""""""""""""""""""""""""""""""

If you want to set the policy to use the recycle bin in a subtree of the
namespace run:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle config --add-bin /eos/dev/2rep/subnode/tree

   success: set attribute 'sys.recycle'='../recycle' in directory /eos/dev/2rep/subnode/tree/

Remove recycle policy from a subtree
""""""""""""""""""""""""""""""""""""

To remove the recycle bin policy in a subtree run:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle config --remove-bin /eos/dev/2rep/subnode/tree

   success: removed attribute 'sys.recycle' from directory /eos/dev/2rep/subnode/tree/

@todo(esindril) Review this part!
Enforce globally usage of a recycle bin for all deletions
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The global policy is enforced in the default space:

.. code-block:: bash

   # enable
   EOS Console [root://localhost ]/ space config default space.policy.recycle=on

   # disable
   EOS Console [root://localhost ]/ space config default space.policy.recycle=off

   # remove policy
   EOS Console [root://localhost ]/ space config default space.policy.recycle=remove

List files in the recycle bin
"""""""""""""""""""""""""""""

If you want to list the objects that can be restored from the recycle bin you run:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle ls
   # Deletion Time            UID      GID      TYPE          RESTORE-KEY            RESTORE-PATH                  DTRACE
   # =====================================================================================================================
   Thu Mar 21 23:02:22 2013   apeters  z2       recursive-dir pxid:00000000000007cf /eos/dev/2rep/subnode/tree     {}

By default this command displays all user private restorable objects.
If you have the root role or are member of the admin group, you can add the
``--all`` flag to list the objects that can be restored for all users.


For performance and maintainability reasons the list is truncated after 100k
entries. The DTRACE entry contains authentication information of the client
who actually deleted the entry (if available).

For the so-called "recycle projects" the user needs to use the "recycle id"
when doing the listing:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> eos recycle ls --rid 1007
   # Deletion Time            UID   GID   SIZE    TYPE   RESTORE-KEY           RESTORE-PATH                DTRACE
   # ============================================================================================================
   Fri Dec  5 15:31:55 2025   adm   adm   1294    file   fxid:0000000000002070 /eos/dev/plain/file1.dat    {}

Restoring Objects
"""""""""""""""""

Objects are restored using **recycle restore <restore-key>**.
The <restore-key> is shown by **recycle ls**.

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle restore 00000000000007cf
   error: to recycle this file you have to have the role of the file owner: uid=755 (errc=1) (Operation not permitted)

You can only restore an object if you have the same uid/gid role
like the object owner:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> role 755 1395
   => selected user role ruid=<755> and group role rgid=<1395>

   EOS Console [root://localhost] |/eos/> recycle restore 00000000000007cf
   success: restored path=/eos/dev/2rep/subnode/tree

If the original path has been used in the mean while you will see the following
after a restore command:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle restore 00000000000007cf
   error: the original path is already existing - use '--force-original-name'
          or '-f' to put the deleted file/tree back and rename the file/tree
          in place to <name>.<inode> (errc=17) (File exists)

The file can be restored using the force flag:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle restore -f 00000000000007cf
   warning: renamed restore path=/eos/dev/2rep/subnode/tree to backup-path=/eos/dev/2rep/subnode/tree.00000000000007d6
   success: restored path=/eos/dev/2rep/subnode/tree

For so-called "recycle projects" a user can restore a certain entry if and
only if one of the following conditions holds true:
* they are the owner of the file to be restored
* they are listed in the ACLs attached to the recycle bin project directory as
  being allowed to read the entries

These ACLs can be configured when setting up the "recycle project" space and
passing the ACL option to the **recycle project** command.

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle project --path /eos/dev/plain --acl u:1234=rx
   EOS Console [root://localhost] |/eos/> attr get sys.acl /eos/dev/proc/recycle/rid:1007
   sys.acl="u:58602:rx"


Purging
"""""""

One can force to flush files in the recycle bin before the lifetime policy
kicks in using recycle purge command:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle purge
   success: purged 1 bulk deletions and 0 individual files from the recycle bin!

Notice that purging only removes files of the current uid/gid role.
Running as **root** does not purge the recycle bin of all users by default.
If you want to purge the recycle bin completely add the ``--all`` option.

Implementation
^^^^^^^^^^^^^^

The implementation is hidden to the enduser and is explained to give some
deeper insight to administrators. All the functionality is wrapped as demonstrated before in the CLI using the recycle command.

The recycle bin resides in the namespace under the proc directory under ``/recycle/``.

The structure of the recycle bin is as follows:

``/recycle/<uid>/<year>/<month>/<date>/<index>/<contracted-path>.<hex-inode>`` for files and

``/recycle/<uid>/<year>/<month>/<date>/<index>/<contracted-path>.<hex-inode>.d`` for bulk deletions.

The structure helps to purge the recycle bin easily by date:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle purge 2018/03/01
   success: purged 12 bulk deletions and 0 individual files from the recycle bin!

The internal structure is however not relevant or exported to the end-user.
The contracted path flattens the full pathname replacing '/' with '#:#'.

The ``/recycle/`` directory is configured as a quota node with project space
e.g. all files appearing in there are accounted on a catch-all project quota.

Deletion only succeeds if the recycle quota node has enough space available
to absorb the deletion object.

A dedicated thread inside the MGM uses an optimized logic to follow the entries
in the recycle tree and performs unrecoverable deletion according to the
configured lifetime policy. The lifetime policy is defined via the external
attribute sys.recycle.lifetime tagged on the /recycle directory specifying
the file lifetime in seconds.

File deletions and bulk deletions are moved in the recycle bin if the parent
directory of the deletion object specifies as external attribute ``sys.recycle=../recycle/``.

A restore operation can only succeed if the restore location provides the
needed quota for all objects to be restored.

Note that a tree can have files owned by many individuals and restoration
requires appropriate quota for all of them. As mentioned, the restore operation
has be executed with the role of the file or subtree top-level directory
identity (uid/gid pair).

.. highlight:: wfe

.. index::
      pair: Interfaces; Workflow Engine, WFE
   single:: WFE

WFE Engine
----------
The workflow engine is a versatile event triggered storage process chain. Currently all events are created by file operations.
The policy to emit events is described as extended attributes of a parent directory. Each workflow is named. The default workflow
is named 'default' and used if no workflow name is provided in an URL as `?eos.workflow=default`.

The workflow engine allows to create chained workflows: E.g. one workflow can trigger an event emission to run the next workflow in the chain and so on...

.. epigraph::

   ==================== ==================================================================================================
   Event                Description
   ==================== ==================================================================================================
   sync::create         event is triggered at the MGM when a file is being created (synchronous event)
   open                 event is triggered at the MGM when a 'file open'
                        - if the return of an open call is ENONET a workflow defined stall time is returned
   sync::prepare        event is triggered at the MGM when a 'prepare' is issued (synchronous event)
   sync::abort_prepare  event is triggered at the MGM when xrdfs prepare -f issued (synchronous event)
   sync::offline        event is triggered at the MGM when a 'file open' is issued against an offline file (synchronous
                        event)
   retrieve_failed      event is triggered with an error message at the MGM when the retrieval of a file has failed
   archive_failed       event is triggered with an error message at the MGM when the archival of a file has failed
   closer               event is triggered via the MGM when a read-open file is closed on an FST.
   sync::closew         event is triggered via the FST when a write-open file is closed (it has priority over the asynchronous one)
   closew               event is triggered via the MGM when a write-open file is closed on an FST
   sync::delete         event is triggered at the MGM when a file has been deleted (synchronous event)
   sync::recycle        event is triggered at the MGM when a file has been moved to the recycle bin (synchronous event)
   ==================== ==================================================================================================

Currently the workflow engine implements two action targets. The **bash:shell** target is a powerful target.
It allows you to execute any shell command as a workflow. This target provides a large set of template parameters
which EOS can give as input arguments to the called shell command. When using the **bash::shell** target you should avoid to
use calls to the EOS console CLI since this can lead to deadlock situations. This is described later. The **mail** target
allows to send an email notification to a specified recipient and mostly used for demonstration.

.. epigraph::

   ============= =============================================================================================
   Action Target Description
   ============= =============================================================================================
   bash:shell    run an arbitrary shell command line with template argument substitution
   mail          send an email notification to a provided recipient when such an event is triggered
   notify        asynchronous workflow sending notification via http,activemq,grpc or qclient(redis) protocol
   ============= =============================================================================================

Configuration
^^^^^^^^^^^^^

Engine
""""""

The WFE engine has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.wfe=on
   # disable
   eos space config default space.wfe=off

The current status of the WFE can be seen via:

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   wfe                            := off
   wfe.interval                   := 10
   ...

The interval in which the WFE engine is running is defined by the **wfe.interval**
space variable. The default is 10 seconds if unspecified.

.. code-block:: bash

   # run the LRU scan once a week
   eos space config default space.wfe.interval=10

The thread-pool size of concurrently running workflows is defined by the **wfe.ntx** space variable.
The default is to run all workflow jobs sequentially with a single thread.

.. code-block:: bash

   # configure a thread pool of 16 workflow jobs in parallel
   eos space config default space.wfe.ntx=10

Workflows are stored in a virtual queue system. The queues display the status of each workflow. By default workflows older than 7 days are cleaned up.
This setting can be changed by the **wfe.keeptime** space variable. That is the time in seconds how long workflows are kept in the virtual queue system before
they get deleted.

.. code-block:: bash

   # keep workflows for 1 week
   eos space config default space.wfe.keeptime=604800

Workflow Configuration
""""""""""""""""""""""

The **notify** workflow
```````````````````````
This notification mechanism can be used to inform external service about events on certain files e.g. when a new file is generated to inform a processing service.

The supported notification protocols are:

- http(s) (POST)
- grpc (Notify rpc)
- activeMQ (Message)
- redis (PUBLISH)

The message which is sent upstream contains a JSON document derived from the protobuf defintion of eos::rpc::MDNotification e.g.

.. code-block:: bash

   {
    "fmd": {
        "id": "653201",
        "contId": "12174",
        "uid": "65534",
        "gid": "2",
        "size": "3106",
        "layoutId": 1048578,
        "flags": 416,
        "name": "ei42MA==",
        "ctime": {
            "sec": "1745940835",
            "nSec": "386583069"
        },
        "mtime": {
            "sec": "1745940835",
            "nSec": "693545000"
        },
        "checksum": {
            "value": "jrQu4gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "type": "adler32"
        },
        "xattrs": {
            "sys.utrace": "NWM2MzBjODYtMjUwZi0xMWYwLWI0NzUtZmExNjNlMDdkZTdl",
            "sys.fs.tracking": "KzU=",
            "sys.vtrace": "W1R1ZSBBcHIgMjkgMTc6MzM6NTUgMjAyNV0gdWlkOjY1NTM0W25vYm9keV0gZ2lkOjJbZGFlbW9uXSB0aWRlbnQ6cm9vdC4yMjQ5NDE6NTYyQGxvY2FsaG9zdDYgbmFtZTpkYWVtb24gZG46IHByb3Q6c3NzIGFwcDogaG9zdDpsb2NhbGhvc3Q2IGRvbWFpbjpsb2NhbGRvbWFpbiBnZW86IHN1ZG86MCB0cmFjZTogb25iZWhhbGY6",
            "sys.eos.btime": "MTc0NTk0MDgzNS4zODY1ODMwNjk="
        },
        "path": "L2Vvcy9kZXYvcHVibGljL25vdGlmeS96LjYw",
        "etag": "\"175342308294656:8eb42ee2\"",
        "inode": "175342308294656"
    },
    "role": {
        "uid": "65534",
        "gid": "2",
        "username": "nobody",
        "groupname": "daemon"
    }
   }


The notification is configured on parent directories using the ``sys.workflow.*`` syntax.

Here are some example configurations:

.. code-block:: bash

     # general attribute structure
     #            sys.workflow.<event>.<space> = notify:<protocol>|<uri>|<port>|<queue>|<timeout>

     # HTTP notification - the port and path to do the POST is part of the <uri> tag, <port> & <queue> are empty
     eos attr set sys.workflow.closew.default=notify:http|localhost:5000/notify|||2000

     # REDIS pub/sub notification - the port is given as an extra argument
     eos attr set sys.workflow.closew.default=notify:qclient|localhost|6349|notification|2000

     # REDIS rpush notification - the port is given as an extra argument
     eos attr set sys.workflow.closew.default=notify:redis|localhost|6349|notification|2000

     # GRPC notification - the port is given as an extra argument, <queue> is empty
     eos attr set sys.workflow.closew.default=notify:grpc|localhost|55100||2000


For testing HTTP here is a simple Flask HTTP printing notifications listening on localhost port 5000:

.. code-block:: python

   import json

   from flask import Flask, request, jsonify

   app = Flask(__name__)

   @app.route('/notify', methods=['POST'])
   def handle_post():
   data = request.get_json()
   if not data:
     return jsonify({'error': 'No JSON payload received'}), 400

   pretty_json = json.dumps(data, indent=4)
   print(pretty_json)
   return jsonify({'message': 'Data received successfully'}), 200

   if __name__ == '__main__':
   app.run(debug=True, port=5000)

For testing REDIS here is a simple CLI listener on the notification queue on localhost for REDIS:

.. code-block:: bash

   #start REDIS
   systemctl start redis

   redis-cli SUBSCRIBE notification

The **mail** workflow
`````````````````````
As an example we want to send an email to a mailing list, whenever a file is deposited. This workflow can be specified like this:

.. code-block:: bash

   # define a workflow to send when a file is written
   eos attr set "sys.workflow.closew.default=mail:eos-project.cern.ch: a file has been written!" /eos/dev/mail/

   # place a new file
   eos cp /etc/passwd /eos/dev/mail/passwd

   # eos-project.cern.ch will receive an Email with a subject like: eosdev ( eosdev1.cern.ch ) event=closew fid=000004f7 )
   # and the text in the body : a file has been written!


The **bash:shell** workflow
``````````````````````````````````````````````````

Most people want to run a command whenever a file is placed, read or deleted. To invoke a shell command one configures the **bash:shell** workflow.
As an example consider this simple echo command, which prints the path when a **closew** event is triggered:

.. code-block:: bash

   # define a workflow to echo the full path when a file is written
   eos attr set "sys.workflow.closew.default=sys.workflow.closew.default=bash:shell:mylog echo <eos::wfe::path>" /eos/dev/echo/

The template parameters ``<eos::wfe::path>`` is replaced with the full logical path of the file, which was written. The third parameters ``mylog`` in **bash:shell:mylog** specifies the name of
the log file for this workflow which is found on the MGM under ``/var/log/eos/wfe/mylog.log``

Once one uploads a file into the ``echo`` directory, the following log entry is created in ``/var/log/eos/wfe/mylog.log``

.. code-block:: bash

   ----------------------------------------------------------------------------------------------------------------------
   1466173303 Fri Jun 17 16:21:43 CEST 2016 shell echo /eos/dev/echo/passwd
   /eos/dev/echo/passwd
   retc=0


.. warning:: Please be aware that running a synchronous workflow which calls back to the EOS MGM either using the CLI or other clients can create a dead-lock if the threadpool is exhausted!

The full list of static template arguments is given here:

.. epigraph::

   =========================== =============================================================================================
   Template                    Description
   =========================== =============================================================================================
   <eos::wfe::uid>             user id of the file owner
   <eos::wfe::gid>             group id of the file owner
   <eos::wfe::username>        user name of the file owner
   <eos::wfe::groupname>       group name of the file owner
   <eos::wfe::ruid>            user id invoking the workflow
   <eos::wfe::rgid>            group id invoking the workflow
   <eos::wfe::rusername>       user name invoking the workflow
   <eos::wfe::rgroupname>      group name invoking the workflow
   <eos::wfe::path>            full absolute file path which has triggered the workflow
   <eos::wfe::base64:path>     base64 encoded full absolute file path which has triggered the workflow
   <eos::wfe::turl>            XRootD transfer URL providing access by file id e.g. root://myeos.cern.ch//mydir/myfile?eos.lfn=fxid:00001aaa
   <eos::wfe::host>            client host name triggering the workflow
   <eos::wfe::sec.app>         client application triggering the workflow (this is defined externally via the CGI ``?eos.app=myapp``)
   <eos::wfe::sec.name>        client security credential name triggering the workflow
   <eos::wfe::sec.prot>        client security protocol triggering the workflow
   <eos::wfe::sec.grps>        client security groups triggering the workflow
   <eos::wfe::instance>        EOS instance name
   <eos::wfe::ctime.s>         file creation time seconds
   <eos::wfe::ctime.ns>        file creation time nanoseconds
   <eos::wfe::mtime.s>         file modification time seconds
   <eos::wfe::mtime.ns>        file modification time nanoseconds
   <eos::wfe::size>            file size
   <eos::wfe::cid>             parent container id
   <eos::wfe::fid>             file id (decimal)
   <eos::wfe::fxid>            file id (hexacdecimal)
   <eos::wfe::name>            basename of the file
   <eos::wfe::base64:name>     base64 encoded basename of the file
   <eos::wfe::link>            resolved symlink path if the original file path is a symbolic link to a file
   <eos::wfe::base64:link>     base64 encoded resolved symlink path if the original file path is a symbolic link to a file
   <eos::wfe::checksum>        checksum string
   <eos::wfe::checksumtype>    checksum type string
   <eos::wfe::event>           event name triggering this workflow (e.g. closew)
   <eos::wfe::queue>           queue name triggering this workflow (e.g. can be 'q' or 'e')
   <eos::wfe::workflow>        workflow name triggering this workflow (e.g. default)
   <eos::wfe::now>             current unix timestamp when running this workflow
   <eos::wfe::when>            scheduling unix timestamp when to run this workflow
   <eos::wfe::base64:metadata> a full base64 encoded meta data blop with all file metadata and parent metadata including extended attributes
   <eos::wfe::vpath>           the path of the workflow file in the virtual workflow directory when the workflow is executed
                               - you can use this to attach messages/log as an extended attribute to a workflow if desired
   =========================== =============================================================================================


Extended attributes of a file and it's parent container can be read with dynamic template arguments:

.. epigraph::

   ================================ ========================================================================================
   Template                         Description
   ================================ ========================================================================================
   <eos::wfe::fxattr:<key>>         Retrieves the value of the extended attribute of the triggering file with name <key>
                                    - sets UNDEF if not existing
   <eos::wfe::fxattr:base64:<key>>  Retrieves the base64 encoded value of the extended attribute of the triggering file with name <key>
                                    - sets UNDEF if not existing
   <eos::wfe::cxattr:<key>>         Retrieves the value of the extended attribute of parent directory of the triggering file
                                    - sets UNDEF if not existing
   ================================ ========================================================================================



Here is an  example for a dynamic attribute:

.. code-block:: bash

   # define a workflow to echo the meta blob and the acls of the parent directory when a file is written
   eos attr set "sys.workflow.closew.default=bash:shell:mylog echo <eos::wfe::base64:metadata> <eos::wfe::cxattr:sys.acl>" /eos/dev/echo/


Configuring retry policies for  **bash:shell** workflows
````````````````````````````````````````````````````````

If a **bash:shell** workflow failes e.g. the command returns rc!=0 and no retry policy is defined, the workflow job ends up in the **failed** queue. For each
workflow the number of retries and the delay for retry can be defined via extended attributes. To reschedule a workflow after a failure the shell command has to return **EAGAIN** e.g. ``exit(11)``.
The number of retries for a failing workflow can be defined as:

.. code-block:: bash

   # define a workflow to return EAGAIN to be retried
   eos attr set "sys.workflow.closew.default=bash:shell:fail '(exit 11)'" /eos/dev/echo/

   # set the maximum number of retries
   eos attr set "sys.workflow.closew.default.retry.max=3" /eos/dev/echo/

The previous workflow will be scheduled three times without delay. If you want to schedule a retry at a later point in time, you can define the delay for retry for a particular workflow like:

.. code-block:: bash

   # configure a workflow retry after 1 hour
   eos attr set "sys.workflow.closew.default.retry.delay=3600" /eos/dev/echo/


Returning result attributes
````````````````````````````

if a **bash::shell** workflow is used, the STDERR of the command is parsed for return attribute tags, which are either tagged on the triggering file (path) or the virtual workflow entry (vpath):

.. epigraph::

   ============================================== =====================================================================================
   Syntax                                         Resulting Action
   ============================================== =====================================================================================
   <eos::wfe::path::fxattr:<key>>=base64:<value>  set a file attribute <key> on <eos::wfe::path> to the base64 decoded value of <value>
   <eos::wfe::path::fxattr:<key>>=<value>         set a file attribute <key> on <eos::wfe::path> to <value> (value can not contain space)
   <eos::wfe::vpath::fxattr:<key>>=base64:<value> set a file attribute <key> on <eos::wfe::vpath> to the base64 decoded value of <value>
   <eos::wfe::vpath::fxattr:<key>>=:<value>       set a file attribute <key> on <eos::wfe::vpath> to <value> (value can not contain space)
   ============================================== =====================================================================================

Virtual /proc Workflow queue directories
""""""""""""""""""""""""""""""""""""""""

The virtual directory structure for triggered workflows can be found under ``/eos/<instance>/proc/workflow``.

Here is an example:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/> eos find /eos/dev/proc/workflow/
   /eos/dev/proc/workflow/20160617/d/
   /eos/dev/proc/workflow/20160617/d/default/
   /eos/dev/proc/workflow/20160617/d/default/1466171933:000004f7:closew
   /eos/dev/proc/workflow/20160617/d/default/1466173303:000004fd:closew
   /eos/dev/proc/workflow/20160617/f/
   /eos/dev/proc/workflow/20160617/f/default/
   /eos/dev/proc/workflow/20160617/f/default/1466171873:000004f4:closew
   /eos/dev/proc/workflow/20160617/f/default/1466173183:000004fa:closew
   /eos/dev/proc/workflow/20160617/q/
   /eos/dev/proc/workflow/20160617/q/default/1466173283:000004fb:closew

The virtual tree is organized with entries like ``<proc>/workflow/<year-month-day>/<queue>/<workflow>/<unix-timestamp>:<fid>:<event>``.
Workflows are scheduled only from the **q** and **e** queues. All other entries describe a ``finale state`` and will be expired as configured by the cleanup policy described in the beginning.

The existing queues are described here:

.. epigraph::

   =========================== ========================================================================================
   Queue                       Description
   =========================== ========================================================================================
   ../q/..                     all triggered asynchronous workflows appear first in this queue
   ../s/..                     scheduled asynchronous workflows and triggered synchronous workflows appear in this queue
   ../r/..                     running workflows appear in this queue
   ../e/..                     failed workflows with retry policy appear here
   ../f/..                     failed workflows without retry appear here
   ../g/..                     workflows with 'gone' files or some global misconfiguration appear here
   ../d/..                     successful workflows with 0 return code
   =========================== ========================================================================================


Synchronous workflows
``````````````````````

The **deletion** and **prepare** workflow are synchronous workflows which are executed in-line. They are stored and tracked as asynchronous workflows in the proc filesystem. The emitted event on deletion is **sync::delete**, the emitted event on prepare is **sync::prepare**.

Workflow log and return codes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The return codes and log information is tagged on the virtual directory entries in the proc filesystem as extended attributes:

.. code-block:: bash

   sys.wfe.retc=<return code value>
   sys.wfe.log=<message describing the result of running the workflow>

Devices Interface
-----------------


eos devices
^^^^^^^^^^^

The devices interface `eos devices` has the main purpose to track storage devices in EOS. The backend of the devices interfaces is a background thread, which by default every 15 minutes decodes S.M.A.R.T information published every 15 minutes from FST nodes.

.. index::
   pair: Devices; Storage Devices


The MGM extraction interval used by the async. thread can only be modified in the sysconfig file by definining `EOS_MGM_DEVICES_PUBLISHING_INTERVAL` in seconds.

The CLI interface by default creates overview statistic about all storage devices configured in the instance:

.. code-block:: bash

   eos devices ls
   # Fri Sep 22 14:50:05 2023
   ┌───────────────────┬──────────────┬────────┬─────┬───────┐
   │model              │avg:age[years]│   bytes│count│  hours│
   ├───────────────────┴──────────────┴────────┴─────┴───────┤
   │TOSHIBA:MG07ACA14TE           0.19 56.00 TB     4 6.55 Kh│
   └─────────────────────────────────────────────────────────┘

   The first line prints the extraction time of the information.

The fields are:

.. code-block:: bash

   model          : name of storage device (blanks are replaced with :)
   avg:age[years] : average age of all devices for a given storage model
   bytes          : storage capacity of all devices for a given storage model
   hours          : power-on-hours for all devices for a given storage model


The long option of the devices command prints all devices ordered by space. The space name is shown in the top left header

.. code-block:: bash

   eos devices ls -l

   # Fri Sep 22 14:50:05 2023
   ┌────────────┬───────────────────┬────────────┬────┬────────┬────┬──────────┬─────────────┬─────────┬────────┬────┬────┐
   │     default│model              │serial      │type│capacity│rpms│poweron[h]│temp[degrees]│S.M.A.R.T│if      │rla │wc  │
   ├────────────┴───────────────────┴────────────┴────┴────────┴────┴──────────┴─────────────┴─────────┴────────┴────┴────┤
   │           1 TOSHIBA:MG07ACA14TE 23S0A0MBF94G sat  14.00 TB 7200    1.64 Kh            31     noctl 6.0:Gb/s true true│
   │           2 TOSHIBA:MG07ACA14TE 23S0B0MBF94G sat  14.00 TB 7200    1.64 Kh            31     noctl 6.0:Gb/s true true│
   │           3 TOSHIBA:MG07ACA14TE 23S0C0MBF94G sat  14.00 TB 7200    1.64 Kh            31     noctl 6.0:Gb/s true true│
   │           4 TOSHIBA:MG07ACA14TE 23S0D0MBF94G sat  14.00 TB 7200    1.64 Kh            31     noctl 6.0:Gb/s true true│
   └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

   ┌───────────────────┬──────────────┬────────┬─────┬───────┬───────┬──────────┬───────┬────────────┬──────────┬──────────┬────────────┐
   │model              │avg:age[years]│   bytes│count│  hours│smrt:ok│smrt:noctl│smrt:na│smrt:failing│smrt:check│smrt:inval│smrt:unknown│
   ├───────────────────┴──────────────┴────────┴─────┴───────┴───────┴──────────┴───────┴────────────┴──────────┴──────────┴────────────┤
   │TOSHIBA:MG07ACA14TE           0.19 56.00 TB     4 6.55 Kh       0          4       0            0          0          0            0│
   └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

.. code-block:: bash

   1st column     : EOS filesystem ID
   model          : model name
   type           : storage HW type
   capacity       : human readable device capacity
   rpms           : RPMs in case of HDDs
   poweron[h]     : hours device has been powerd on
   S.M.A.R.T.     : device status ({"ok", "noctl","na","failing","check","inval","unknown"})
   if             : connection interface speed
   rla            : read-look-ahead (enabled=true,disabled=false)
   wc             : write-cache (enabled=true, disabled=false)

The devices command supports monitoring and JSON format including per device and device statistics.

.. code-block:: bash

   # key:val output for monitoring
   eos devices ls -m

   # json output for monitoring
   eos --json devices ls


proc/devices
^^^^^^^^^^^^

The devices async thread creates for each `serialnumber.filesystem-id` combination an entry in `/eos/.../proc/devices/`. The birth time of these entries is the first time a serialnumber/filesystem-id combination has been stored. Each of these empty file entries carries an extended attribute `sys.smart.json`, which stores the last JSON output provided by `smartctl` running on FSTs. These entries allow on the long term to trace the appearance and disappearance of devices.
