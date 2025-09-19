.. highlight:: rst

.. index::
   single: Drain System

.. _draining:

Drain System
============

Overview
--------

The EOS drain system provides a fully automatic mechanism to drain (empty)
filesystems under certain error conditions. A file system drain is triggered
by an IO error on a file system or manually by an operator setting a
filesystem in drain mode.

The drain engine makes use of the GeoTreeEngine component to decide where to move the drained replicas.
The drain processes are spawned on the MGM and represent simple XRootD third-party-copy transfers.

FST Scrubber
------------

Each FST run's a dedicated thread doing scrubbing. Scrubbing is running if the
file system configuration is at least **wo** ( e.g. in write-only or read-write mode),
the file system is in **booted** state and the label of the
filesystem ``<mountpoint>/.eosfsid + <mountpoint>/.eosfsuuid`` is readable.
If the label is not readable the Scrubber broadcasts an IO error for filesystems
in **ro**, **wo** or **rw** mode and **booted** state with the error text
"filesystem seems to be not mounted anymore".

The FST scrubber follows the filling size of a disk and writes test pattern
files at 0%, 10%, 20% ... 90% filling with the goal to do tests equally
distributed over the physical size of the disk. At each 10% filling position
the scrubber creates a write-once file to be re-read in each scrubbing pass
and a re-write file which is re-written and re-read in each scrubbing pass.
The following pattern is written into the test files:

.. code-block:: bash

   scrubPattern[0][i]=0xaaaa5555aaaa5555ULL;
   scrubPattern[0][i+1]=0x5555aaaa5555aaaaULL;
   scrubPattern[1][i]=0x5555aaaa5555aaaaULL;
   scrubPattern[1][i+1]=0xaaaa5555aaaa5555ULL;

Pattern 0 or pattern 1 is selected randomly. Each test file has 1MB size and
the scrub file names are ``<mountpoint>/scrub.write-once.[0-9]`` and
``<mountpoint>/scrub.re-write.[0-9]``.

In case an error is detected, the FST broadcasts an EIO to the MGM with the
error text "filesystem probe error detected".

You can see filesystems in error state and the error text on the MGM node doing:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -e
   #...............................................................................................
   #                   host #   id #     path #       boot # configstatus #      drain #... #errmsg
   #...............................................................................................
        lxfsrk51a02.cern.ch   3235    /data05  opserror            empty      drained   5 filesystem seems to be
                                                                                          not mounted anymore
        lxfsrk51a04.cern.ch   3372    /data19  opserror            empty      drained   5 filesystem probe error detected


Central File System View and State Machine
------------------------------------------

Each filesystem in EOS has a configuration, boot state and drain state.

The possible configuration states are self explaining:

.. epigraph::

   ============= ======================================================================================
   state          definition
   ============= ======================================================================================
   rw            filesystem set in read write mode
   wo            filesystem set in write-once mode
   ro            filesystem set in read-only mode
   drain         filesystem set in drain mode
   off           filesystem set disabled
   empty         filesystem is empty e.g. contains no files any more
   ============= ======================================================================================

File systems involved in any kind of IO need to be in boot state booted.

The configured file systems are shown via:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls

   #.........................................................................................................................
   #                   host (#...) #   id #           path #     schedgroup #       boot # configstatus #      drain # active
   #.........................................................................................................................
        lxfsra02a05.cern.ch (1095)      1          /data01        default.0       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      2          /data02       default.10       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      3          /data03        default.1       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      4          /data04        default.2       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      5          /data05        default.3       booted             rw      nodrain   online

As shown each file system has also a drain state. Drain states can be:

.. epigraph::

   ================ ==============================================================================================================================================================================
   state            definition
   ================ ==============================================================================================================================================================================
   nodrain          file system is currently not draining
   prepare          the drain process is prepared - this phase lasts 60 seconds
   wait             the drain process either waits for the namespace to be booted or it is waiting that the graceperiod has passed (see below)
   draining         the drain process is enabled - nodes inside the scheduling group start to pull transfers to drop replicas from the filesystem to drain
   stalling         in the last 5 minutes there was noprogress of the drain procedure. This happens if the files to transfer are very huge or there are only files left which cannot be replicated.
   expired          the time defined by the drainperiod variable has passed and the drain process is stopped. There are files left on the disk which couldn't be drained.
   drained          all files have been drained from the filesystem.
   failed           the drain activity is finished but there are still files on file system that could not be drained and require a manual inspection.
   ================ ==============================================================================================================================================================================

The final state can be one of the following: expired, failed or drained.

The drain and grace periods are defined as a space variables (e.g. automatically
applied to all filesystems in that space when they are moved into or registered).

One can see the settings via the space command:

.. code-block:: bash

   EOS Console [root://localhost] |/> space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   balancer                         := on
   balancer.node.ntx                := 10
   balancer.node.rate               := 10
   balancer.threshold               := 1
   drainer.node.ntx                 := 10
   drainer.node.rate                := 25
   drainperiod                      := 3600
   graceperiod                      := 86400
   groupmod                         := 24
   groupsize                        := 20
   headroom                         := 0.00 B
   quota                            := off
   scaninterval                     := 1

They can be modified by setting the *drainperiod* or *graceperiod* variable in
number of seconds:

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default space.drainperiod=86400
   success: setting drainperiod=86400

   EOS Console [root://localhost] |/> space config default space.graceperiod=86400
   success: setting graceperiod=86400

.. warning::
   This defines the variables only if filesystems are registered or moved into that space.

If you want to apply this setting to all filesystems in that space,
you have additionally to call:

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default fs.drainperiod=86400
   EOS Console [root://localhost] |/> space config default fs.graceperiod=86400

If you want a global overview about running drain processes, you can get the
number of running drain transfers by space, by group, by node and by filesystem:

.. code-block:: bash

   EOS Console [root://localhost] |/> space ls --io
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #     name # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------
   default       0.01        32.00        17.00        862         15         14      9      9      6.97 TB    347.33 TB      20.42 M     16.97 G          0         10

   EOS Console [root://localhost] |/> group  ls --io
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #           name # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   default.0              0.00         0.00         0.00        952        217        199      0      0    338.31 GB     15.97 TB     952.65 k    780.14 M          0          0
   default.1              0.00         0.00         0.00        952        217        199      0      0    336.07 GB     15.97 TB     927.18 k    780.14 M          0          0
   default.10             0.00         0.00         0.00        952        217        199      0      0    332.23 GB     15.97 TB     926.45 k    780.14 M          0          0
   default.11             0.00         0.00         0.00        952        217        199      0      0    325.14 GB     15.97 TB     948.02 k    780.14 M          0          0
   default.12             0.00         0.00         0.00        833        180        179      0      0     22.39 GB     13.97 TB     898.40 k    682.62 M          0          0
   default.13             0.00         0.00         1.00        952        217        199      0      0    360.30 GB     15.97 TB     951.05 k    780.14 M          0          0
   default.14             0.99        96.00       206.00        952        217        199     31     30    330.45 GB     15.97 TB     956.50 k    780.14 M          0         36
   default.15             0.00         0.00         0.00        952        217        199      0      0    308.26 GB     15.97 TB     939.26 k    780.14 M          0          0
   default.16             0.00         0.00         0.00        833        188        184      0      0    327.76 GB     13.97 TB     899.97 k    682.62 M          0          0
   default.17             0.87       100.00       202.00        952        217        199     16     28    368.09 GB     15.97 TB     933.95 k    780.14 M          0         31
   default.18             0.00         0.00         0.00        952        217        199      0      0    364.27 GB     15.97 TB     953.94 k    780.14 M          0          0
   default.19             0.00         0.00         0.00        952        217        199      0      0    304.66 GB     15.97 TB     939.24 k    780.14 M          0          0
   default.2              0.00         0.00         0.00        952        217        199      0      0    333.64 GB     15.97 TB     920.26 k    780.14 M          0          0
   default.20             0.00         0.00         0.00        952        217        199      0      0    335.00 GB     15.97 TB     957.02 k    780.14 M          0          0
   default.21             0.00         0.00         0.00        952        217        199      0      0    335.18 GB     15.97 TB     921.75 k    780.14 M          0          0
   default.3              0.00         0.00         0.00        952        217        199      0      0    319.06 GB     15.97 TB     919.02 k    780.14 M          0          0
   default.4              0.00         0.00         0.00        952        217        199      0      0    320.18 GB     15.97 TB     826.62 k    780.14 M          0          0
   default.5              0.00         0.00         0.00        952        217        199      0      0    320.12 GB     15.97 TB     924.60 k    780.14 M          0          0
   default.6              0.00         0.00         0.00        952        217        199      0      0    333.56 GB     15.97 TB     920.32 k    780.14 M          0          0
   default.7              0.00         0.00         0.00        952        217        199      0      0    333.42 GB     15.97 TB     922.51 k    780.14 M          0          0
   default.8              0.00         0.00         0.00        952        217        199      0      0    335.67 GB     15.97 TB     925.39 k    780.14 M          0          0
   default.9              0.00         0.00         0.00        952        217        199      0      0    325.37 GB     15.97 TB     957.84 k    780.14 M          0          0
   test                   0.00         0.00         0.00          0          0          0      0      0       0.00 B       0.00 B         0.00        0.00          0          0

   EOS Console [root://localhost] |/> node  ls --io
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #               hostport # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   eosdevsrv1.cern.ch:1095       0.00         0.00         0.00          0          0          0      0      0       0.00 B       0.00 B         0.00        0.00          0          0
   lxfsra02a02.cern.ch:1095       0.10        19.00        55.00        119         37         20      7      8    935.18 GB     41.92 TB       2.54 M      2.05 G          0         10
   lxfsra02a05.cern.ch:1095       0.06         5.00        53.00        119         30          5      1     10    968.03 GB     43.92 TB       2.71 M      2.15 G          0         10
   lxfsra02a06.cern.ch:1095       0.05         0.00        50.00        119         16          0      0      6    872.91 GB     43.92 TB       2.84 M      2.15 G          0          6
   lxfsra02a07.cern.ch:1095       0.05        33.00        10.00        119         23         33      6      7    882.25 GB     43.92 TB       3.03 M      2.15 G          0          8
   lxfsra02a08.cern.ch:1095       0.09        41.00        56.00        119         45         42      9      9    947.68 GB     43.92 TB       2.78 M      2.15 G          0         10
   lxfsra04a01.cern.ch:1095       0.09        15.00       101.00        119         29         15      2      8    818.77 GB     41.92 TB       2.02 M      2.05 G          0         10
   lxfsra04a02.cern.ch:1095       0.09        27.00        83.00        119         37         27      2     10    837.91 GB     43.92 TB       2.30 M      2.15 G          0         10
   lxfsra04a03.cern.ch:1095       0.05        56.00         1.00        119          0         57     20      0    746.40 GB     43.92 TB       2.21 M      2.15 G          0          0

   EOS Console [root://localhost] |/> fs ls --io

   #.................................................................................................................................................................................................................
   #                     hostport #  id #     schedgroup # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #.................................................................................................................................................................................................................

   ...

   lxfsra04a02.cern.ch:1095   109       default.14       0.21         0.00        15.00        119         21          0      0      8     59.35 GB      2.00 TB     102.85 k     97.52 M          0          8

   ...



Drain Threads MGM
-----------------

Each filesystem shown in the drain view in a non-final state has a thread on the
MGM associated to it.

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra02a05.cern.ch (1095)     20          /data20      prepare            0         0.00       0.00 B          24

A drain thread is steering the drain of each filesystem in non-final state and
is responsible of spawning drain processes directly on the MGM node. These logical
drain jobs use the GeoTreeEngine to select the destination file system are queued
in case the limits per node are reached. The drain parameters can be configured at
the space level:

.. code-block:: bash

   EOS Console [root://localhost] |/> space status default

   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ..

   drainer.node.nfs                 := 10
   drainer.fs.ntx                   := 10
   drainperiod                      := 3600
   graceperiod                      := 86400
   ..

By default max 5 file systems per node can be drained in parallel with max 5
parallel transfers per file system.

The values can be modified via:

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default space.drainer.node.nfs=20
   EOS Console [root://localhost] |/> space config default space.drainer.fs.ntx=50


Example Drain Process
---------------------

We need to drain filesystem 20. However the file system is still fully operational
hence we use status drain.

.. code-block:: bash

   EOS Console [root://localhost] |/> fs config 20 configstatus=drain
   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra02a05.cern.ch (1095)     20          /data20      prepare            0         0.00       0.00 B          24

After 60 seconds a drain filesystem changes into state draining if the drain
mode was manually set. If a graceperiod is defined, it will stay in status
waiting for the length of the grace period.

In this example the defined drain period is 1 day:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra04a03.cern.ch (1095)    20           /data20     draining            5        75.00     37.29 GB       86269

   When the drain has successfully completed, the output looks like this:

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra02a05.cern.ch (1095)     20          /data20      drained            0         0.00       0.00 B           0


If the drain can not complete you will see this after the drain period has passed:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   l
   lxfsra04a03.cern.ch (1095)     20          /data20      expired           56        34.00     27.22 GB       86050

You can now investigate the origin by doing:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs status 20

   ...

   # ....................................................................................
   # Risk Analysis
   # ....................................................................................
   number of files                  :=         34 (100.00%)
   files healthy                    :=          0 (0.00%)
   files at risk                    :=          0 (0.00%)
   files inaccessbile               :=         34 (100.00%)
   # ------------------------------------------------------------------------------------

Here all remaining files are inaccessible because all replicas are down.

In case files are claimed to be accessible you have to look directoy at the remaining files:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs dumpmd 20 -path
   path=/eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root
   path=/eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/2/8.root
   path=/eos/dev/2rep/sub12/lxplus406.cern.ch_4/0/1/0.root
   path=/eos/dev/2rep/sub12/lxplus403.cern.ch_43/0/2/8.root
   ...

Check these files using 'file check':

.. code-block:: bash

   EOS Console [root://localhost] |/> file check /eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root
   path="/eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root" fid="0002d989" size="291241984" nrep="2" checksumtype="adler" checksum="0473000100000000000000000000000000000000"
   nrep="00" fsid="20" host="lxfsra02a05.cern.ch:1095" fstpath="/data08/00000012/0002d989" size="291241984" checksum="0473000100000000000000000000000000000000"
   nrep="01" fsid="53" host="lxfsra04a01.cern.ch:1095" fstpath="/data09/00000012/0002d989" size="291241984" checksum="0000000000000000000000000000000000000000"

In this case the second replica didn't commit a checksum and cannot be read.

This you might fix like this:

.. code-block:: bash

   EOS Console [root://localhost] |/> file verify /eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root -checksum -commitchecksum


If you just want to force the remove of files remaining on a non-drained filesystem,
you can drop all files on a particular filesystem using **eos fs dropfiles**.
If you use the '-f' flag all references to these files will be removed immediately
and EOS won't try to delete any file anymore.

.. code-block:: console

   EOS Console [root://localhost] |/> fs dropfiles 170 -f
   Do you really want to delete ALL 24 replica's from filesystem 170 ?
   Confirm the deletion by typing => 1434841745
   => 1434841745

   Deletion confirmed
