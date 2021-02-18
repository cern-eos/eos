.. highlight:: rst

.. index::
   single: Fsck


Enable FST Scan
---------------

To enable the FST scan you have to set the variable **scaninterval** on the space and
on all file systems:

.. code-block:: bash

   # set it on the space to inherit a value for all new filesystems in this space every 14 days (time has to be in seconds)
   space config default space.scaninterval=1209600

   # set it on an existing filesystem (fsid 23) to 14 days (time has to be in seconds)
   fs config 23 space.scaninterval=1209600

   # set the scaninterval for all the existing file systems already registered in the given space
   space config default fs.scaninterval=1209601

.. note::

   The *scaninterval* time has to be given in seconds!


Caveats
-------

For FSCK engine to function correctly, FSTs must be able to connect to QuarkDB directly (and to the MGM).


Overview
========

High level summary
------------------

#) error collection happens in the FST in defined intervals, no action/trigger by MGM is required for this

#) the locally saved results will be collected by the fsck collection thread of fsck engine

#) if the fsck repair thread is  enabled, the mgm will trigger repair actions (i.e. create / delete replica)
as required (based on collected error data)

Intervals and config parameters for file systems(FS)
-----------------------------------------------------

These values are set as global defaults on the space. A file system should get the values from the space when it is newly created.
Below you can find a brief description of the parameters influencing the scanning procedure.

===================  ===============   ===========================================================
Name                 Default           Description
===================  ===============   ===========================================================
scan_disk_interval   14400 [s] (4h)    interval at which files in the FS should be scanned, by the FST itself
scan_ns_interval     259200 [s] (3d)   interval at which files in the FS are compares against the
                                       namespace information from QuarkDB
scaninterval         604800 [s] (7d)   target interval at which all file should be scanned
scan_ns_rate         50 [Hz]           rate limit the requests to QuarkDB for the namespace scans
scanrate             100 [MB/s]        rate limit bandwidth used by the scanner when reading files
                                       from disk
===================  ===============   ===========================================================

**scan_disk_interval** and **scan_ns_interval** are skewed by a random factor per FS so that not all disks become busy at the same time.

The scan jobs are started with a lower IO priority class (using Linux ioprio_set) within EOS to decrease the impact on normal filesystem access, i.e. check logs for set io priority to 7 (lowest best-effort).

.. code-block:: bash

   210211 12:41:40 time=1613043700.017295 func=RunDiskScan              level=NOTE
   logid=1af8cd9e-6c5e-11eb-ae37-3868dd2a6fb0 unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f98bebff700 source=ScanDir:446
   tident=<service> sec=   uid=0 gid=0 name= geo="" msg="set io priority to 7(lowest best-effort)" pid=221712


Scan duration
-------------

The first scan of a larger (fuller) FS can take several hours. Following scans will be much faster, within minutes (10-30min).
Subsequent scans will only look at file that have not been scanned since scaninterval . i.e. each scan iteration will only look at a fraction of the files on disk, compare the logs for such a scan. (see the last line “scannedfiles” vs “skippedfiles” and the scanduration of 293s.)

.. code-block:: bash

   210211 12:49:44 time=1613044184.957472 func=RunDiskScan              level=NOTE  logid=1827f5ea-6c5e-11eb-ae37-3868dd2a6fb0    unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f993afff700 source=ScanDir:504                    tident=<service> sec=      uid=0 gid=0 name= geo="" [ScanDir] Directory: /srv/data/data.01 files=147957 scanduration=293 [s] scansize=23732973568 [Bytes] [ 23733 MB ] scannedfiles=391 corruptedfiles=0 hwcorrupted=0 skippedfiles=147557

Error types detected by fsck
-----------------------------

(in decreasing priority)

=============  ====================================================  ==============
Error          Description                                           Fixed by
=============  ====================================================  =============
d_mem_sz_diff  disk and reference size mismatch                      FsckRepairJob
m_mem_sz_diff  MGM and reference size mismatch                       inspecting all the replicas or saved for manual inspection
d_cx_diff      disk and reference checksum mismatch                  FsckRepairJob
m_cx_diff      MGM and reference checksum mismatch                   inspecting all the replicas or saved for manual inspection
unreg_n        unregistered file / replica                           (i.e. file on FS that has no entry in MGM) register replica if metadata match or drop if not needed
rep_missing_n  missing replica for a file                            replica is registered on mgm but not on disk - FsckRepairJob
rep_diff_n     replica count is not nominal (too high or too low)    fixed by dropping replicas or creating new ones through FsckRepairJob
orphans_n      orphan files (no record for replica/file in mgm)      no action at the MGM, files not referenced by MGM at all, moved to to .eosorphans directory on FS mountpoint
=============  ====================================================  ========


Configuration
=============

space
-----

Some config items on the space are global, some are defaults (i.e. for newly created filesystems), see https://eos-docs.web.cern.ch/configuration/autorepair.html

To enable the FST scan you have to set the variable **scaninterval** on the space and on all file systems.

The intervals other than `scaninterval` are defaults for newly created filesystems. For an explanation. of the intervals see above.


.. code-block:: bash

   [root@mgm-1 ~]# eos space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   autorepair                       := on
   [...]
   scan_disk_interval               := 14400
   scan_ns_interval                 := 259200
   scan_ns_rate                     := 50
   scaninterval                     := 604800
   scanrate                         := 100
   [...]



filesystem / FS
---------------

To enable the FST scan you have to set the variable `scaninterval` on the space and on all file systems

.. code-block:: bash

   [root@mgm-1 ~]# eos fs status 1
   # ------------------------------------------------------------------------------------
   # FileSystem Variables
   # ------------------------------------------------------------------------------------
   bootcheck                        := 0
   bootsenttime                     := 1612456466
   configstatus                     := rw
   host                             := fst-1.eos.grid.vbc.ac.at
   hostport                         := fst-1.eos.grid.vbc.ac.at:1095
   id                               := 1
   local.drain                      := nodrain
   path                             := /srv/data/data.00
   port                             := 1095
   queue                            := /eos/fst-1.eos.grid.vbc.ac.at:1095/fst
   queuepath                        := /eos/fst-1.eos.grid.vbc.ac.at:1095/fst/srv/data/data.00

   [...] defaults for these are taken from MGM, scanterval must be set!
   scan_disk_interval               := 14400
   scan_ns_interval                 := 259200
   scan_ns_rate                     := 50
   scaninterval                     := 604800
   scanrate                         := 100

   [...] various stat values reported back by the FST
   stat.fsck.blockxs_err            := 1
   stat.fsck.d_cx_diff              := 0
   stat.fsck.d_mem_sz_diff          := 0
   stat.fsck.d_sync_n               := 148520
   stat.fsck.m_cx_diff              := 0
   stat.fsck.m_mem_sz_diff          := 0
   stat.fsck.m_sync_n               := 148025
   stat.fsck.mem_n                  := 148526
   stat.fsck.orphans_n              := 497
   stat.fsck.rep_diff_n             := 5006
   stat.fsck.rep_missing_n          := 0
   stat.fsck.unreg_n                := 5003
   [...]


Ffsck settings
-------------

With the settings above, stats are collected on the FST (and reported in fs status) but no further action is taken. To setup of the fsck mechanism, see the eos fsck subcommands:

`fsck stat`
-----------

Gives a quick status of error stats collection and if the repair thread is active. The `eos fsck toggle-repair` and `toggle-collect` are really toggles. Use **eos fsck stat** to verify the correctness of your settings!

.. code-block:: bash

   [root@mgm-1 ~]# eos fsck stat
   Info: collection thread status -> enabled
   Info: repair thread status     -> enabled
   210211 15:54:09 1613055249.712603 Start error collection
   210211 15:54:09 1613055249.712635 Filesystems to check: 252
   210211 15:54:10 1613055250.769177 blockxs_err                    : 118
   210211 15:54:10 1613055250.769208 orphans_n                      : 92906
   210211 15:54:10 1613055250.769221 rep_diff_n                     : 1226274
   210211 15:54:10 1613055250.769224 rep_missing_n                  : 6
   210211 15:54:10 1613055250.769231 unreg_n                        : 1221521
   210211 15:54:10 1613055250.769235 Finished error collection
   210211 15:54:10 1613055250.769237 Next run in 30 minutes

The collection thread will interrogate the FSTs for locally collected error stats at configured intervals (default: 30 minutes).

`fsck report`
-------------

For a more comprehensive error report, use **eos fsck report** this will only contain data once the error collection has started (also note the switch -a to show errors per filesystem FS)

.. code-block:: bash

   [root@mgm-1 ~]# eos fsck report
   timestamp=1613055250 tag="blockxs_err" count=43
   timestamp=1613055250 tag="orphans_n" count=29399
   timestamp=1613055250 tag="rep_diff_n" count=181913
   timestamp=1613055250 tag="rep_missing_n" count=4
   timestamp=1613055250 tag="unreg_n" count=180971


Repair
=======

Most of the repair operations are implemented using the DrainTransferJob functionality.

Operations
===========

Inspect FST local error stats
-----------------------------

Use **eos-leveldb-inspect** command to inspect the contents of the local database on the FSTs.
The local database contains all information (fxid, error type, etc) that will be collected
by the mgm (compare the eos fs status <fsid> output).

.. code-block:: bash

   [root@fst-9 ~]# eos-leveldb-inspect  --dbpath /var/eos/md/fmd.0225.LevelDB --fsck
   Num. entries in DB[mem_n]:                     148152
   Num. files synced from disk[d_sync_n]:         148150
   Num, files synced from MGM[m_sync_n]:          147723
   Disk/referece size missmatch[d_mem_sz_diff]:   0
   MGM/reference size missmatch[m_mem_sz_diff]:   140065
   Disk/reference checksum missmatch[d_cx_diff]:  0
   MGM/reference checksum missmatch[m_cx_diff]:   0
   Num. of orphans[orphans_n]:                    427
   Num. of unregistered replicas[unreg_n]:        5078
   Files with num. replica missmatch[rep_diff_n]: 5081
   Files missing on disk[rep_missing_n]:          0

Check fsck repair activity
--------------------------

See if the fsck repair thread is active and how log its work queue is (cross check with log activity on mgm):

.. code-block:: bash

   [root@mgm-1 ~]# eos ns | grep fsck
   ALL      fsck info                        thread_pool=fsck min=2 max=20 size=20 queue_size=562
   ALL      tracker info                     tracker=fsck size=582
   compare namespace stats for total count of fsck operations:


   [root@mgm-1 ~]# eos ns stat | grep -i fsck
   ALL      fsck info                        thread_pool=fsck min=2 max=20 size=20 queue_size=168
   ALL      tracker info                     tracker=fsck size=188
   all FsckRepairFailed              71.58 K     0.00     0.03     1.35     0.87     -NA-      -NA-
   all FsckRepairStarted             63.19 M   857.75  1107.25  1112.05   918.32     -NA-      -NA-
   all FsckRepairSuccessful          63.12 M   857.75  1106.88  1110.64   917.44     -NA-      -NA-

Log examples
============

Startup of FST service and initializing fsck threads:


.. code-block:: bash

    210211 12:41:39 time=1613043699.997897 func=ConfigScanner level=INFO  logid=1af5b7a8-6c5e-11eb-ae37-3868dd2a6fb0
    unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f99497ff700 source=FileSystem:159 tident=<service> sec= uid=0 gid=0
    name= geo="" msg="started ScanDir thread with default parameters" fsid=238

   # NS scanner thread with random skew
   210211 12:41:50 time=1613043710.000322 func=RunNsScan  level=INFO  logid=1af62382-6c5e-11eb-ae37-3868dd2a6fb0
   unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f98e6bfe700 source=ScanDir:224 tident=<service> sec= uid=0 gid=0
   name= geo="" msg="delay ns scan thread by 38889 seconds" fsid=239 dirpath="/srv/data/data.14"


systemd ScanDir results
-----------------------

These logs are also written to /var/log/eos/fst/xrdlog.fst

.. code-block:: bash

   Feb 11 12:41:33 fst-9.eos.grid.vbc.ac.at eos_start.sh[220738]: Using xrootd binary: /opt/eos/xrootd/bin/xrootd
   Feb 11 12:49:44 fst-9.eos.grid.vbc.ac.at scandir[220738]: skipping scan w-open file: localpath=/srv/data/data.01/000006e3/010d045d fsid=226 fxid=010d045d
   Feb 11 12:49:44 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.01 files=147957 scanduration=293 [s] scansize=23732973568 [Bytes] [ 23733 MB ] scanned...iles=147557
   Feb 11 13:07:55 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.18 files=148074 scanduration=263 [s] scansize=17977114624 [Bytes] [ 17977.1 MB ] scann...iles=147730
   Feb 11 13:08:36 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.22 files=147905 scanduration=258 [s] scansize=19978055680 [Bytes] [ 19978.1 MB ] scann...iles=147498
   Feb 11 13:14:56 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.27 files=147445 scanduration=249 [s] scansize=15998377984 [Bytes] [ 15998.4 MB ] scann...iles=147119
   fsck repairs. success/failure on MGM

   210211 13:58:17 time=1613048297.294157 func=RepairReplicaInconsistencies level=INFO  logid=cf14c90e-6c68-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd53bff700 source=FsckEntry:689                  tident=<service> sec=      uid=0 gid=0 name= geo="" msg="file replicas consistent" fxid=0028819b
   210211 13:58:17 time=1613048297.294294 func=RepairReplicaInconsistencies level=INFO  logid=cf14c54e-6c68-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd51bfb700 source=FsckEntry:689                  tident=<service> sec=      uid=0 gid=0 name= geo="" msg="file replicas consistent" fxid=00ef5955
   210211 13:59:18 time=1613048358.345753 func=RepairReplicaInconsistencies level=ERROR logid=cf14c7ce-6c68-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd523fc700 source=FsckEntry:663                  tident=<service> sec=      uid=0 gid=0 name= geo="" msg="replica inconsistency repair failed" fxid=0079b4d0 src_fsid=244


No repair action, file is being deleted
---------------------------------------

The file has an FsckEntry i.e. is marked from repair, and was previously listed on the collected errors, but

.. code-block:: bash

   210211 16:27:45 time=1613057265.418302 func=Repair                   level=INFO  logid=b077de7c-6c7d-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd95bff700 source=FsckEntry:773                  tident=<service> sec=      uid=0 gid=0 name= geo=""
   msg="no repair action, file is being deleted" fxid=00033673
   The file is noted as “being deleted” as its container (directory) does not exist anymore, i.e.


   [root@mgm-1 ~]# eos fileinfo fxid:00033673
   File: 'fxid:00033673'  Flags: 0600  Clock: 1662bb7c74f01d9f
   Size: 0
   Modify: Fri Jul 24 11:32:15 2020 Timestamp: 1595583135.037235673
   Change: Fri Jul 24 11:32:15 2020 Timestamp: 1595583135.037235673
   Birth: Fri Jul 24 11:32:15 2020 Timestamp: 1595583135.037235673
   CUid: 12111 CGid: 11788 Fxid: 00033673 Fid: 210547 Pid: 0 Pxid: 00000000
   XStype: adler    XS: 00 00 00 00    ETAGs: "56518279954432:00000000"
   Layout: raid6 Stripes: 7 Blocksize: 1M LayoutId: 20640642 Redundancy: d0::t0
   #Rep: 0
   *******
   error: cannot retrieve file meta data - Container #0 not found (errc=0) (Success)


Discrepancy reported errors
-----------------------------------------------------------------------------------------

... between fsck report summary / per filesystem and fsck stat.
EOS fsck report is giving different numbers for total report and per filesystem summary. This is expected.

Per filesystem reports may contain error counts for individual replicas of a single file stored in EOS.
 **eos fsck stat** will reflect the per replica count, **eos fsck report** will show lower numbers,
not counting per each replica of a file.

**example script**

.. code-block:: bash

   echo "summed up by filesystem"
   ERR_TYPES="blockxs_err orphans_n rep_diff_n rep_missing_n unreg_n"
   for ETYPE in $ERR_TYPES; do
   echo -n "$ETYPE: "
   eos fsck report -a | grep $ETYPE  | awk '{print $4;}' | awk 'BEGIN{ FS="="; total=0}; { total=total+$2; } END{print total;}'
   done

   echo ""

   echo "eos fsck summary report"
   eos fsck report

**output example**

.. code-block:: bash

   [root@mgm-1 ~]# ./eos_fsck_miscount.sh
   summed up by filesystem
   blockxs_err: 115
   orphans_n: 95056
   rep_diff_n: 1251566
   rep_missing_n: 30
   unreg_n: 1246475

   eos fsck summary report
   timestamp=1613069473 tag="blockxs_err" count=43
   timestamp=1613069473 tag="orphans_n" count=29602
   timestamp=1613069473 tag="rep_diff_n" count=181913
   timestamp=1613069473 tag="rep_missing_n" count=28
   timestamp=1613069473 tag="unreg_n" count=180998
