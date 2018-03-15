:orphan:

.. highlight:: rst

.. index::
   single: Citrine-Release


Citrine Release Notes
======================

``Version 4 Citrine``

Introduction
------------
This release is based on XRootD V4 and IPV6 enabled.

``v4.2.16 Citrine``
===================

2018-03-02

Bug
---

* [EOS-2142] - eosfstregister fails to get mgm url in CentOS 7
* [EOS-2370] - EOSATLAS crashed while creating the output for a recursive attr set
* [EOS-2382] - FUSEX access with concurrency creates orphaned files
* [EOS-2386] - Vectored IO not accounted by "io" commands
* [EOS-2387] - FST crash in eos::fst::ReedSLayout::AddDataBlock

Task
----

* [EOS-2383] - eosxd: segfault in inval_inode

Improvement
-----------

* [EOS-1565] - RFE: turn off SIGSEGV handler on non-MGM EOS components


``v4.2.15 Citrine``
===================

2018-02-22

Bug
---

* [EOS-2353] - git clone with 2GB random reading creates read amplification
* [EOS-2359] - Deadlock in proto wfe
* [EOS-2361] - MGM crash after enabling ToggleDeadlock
* [EOS-2362] - eosfusebind (runuser) broken on slc6


``v4.2.14 Citrine``
===================

2018-02-20

Bug
----

* [EOS-2153] - consistent eosd memory leak
* [EOS-2348] - ns shows wrong value for resident memory (shows virtual)
* [EOS-2350] - eosd returns Numerical result out of range when talking to a CITRINE server and out of quota


``v4.2.13 Citrine``
===================

2018-02-19

Bug
----

* [EOS-2057] - Wrong conversion between IEC and Metric multiples
* [EOS-2299] - WFE can't be switched off
* [EOS-2309] - Possible memleak in FuseServer::Caps::BroadcastReleaseFromExternal
* [EOS-2310] - eosadmin wrapper no longer sends role
* [EOS-2330] - Usernames with 8 characters are wrongly mapped
* [EOS-2335] - Crash around XrdOucString::insert
* [EOS-2339] - "eos" shell crash around "eos_console_completion","eos_entry_generator"
* [EOS-2340] - "eos" crash around "AclHelper::CheckId"
* [EOS-2337] - autofs-ed fuse mounts not working for mountpoint names with matched entries under "/"

Task
----

* [EOS-2329] - protect MGM against memory exhaustion caused by a globbing ls

Improvement
-----------

* [EOS-2321] - Quota report TiB vs. TB
* [EOS-2323] - citrine mgm crash
* [EOS-2336] - Default smart files in the proc filesystem

Configuration Change
-------------------+

* [EOS-2279] - eosfusebind error message at login

Incident
-------+

* [EOS-2298] - EOS MGM memory leak



``v4.2.12 Citrine``
===================

2018-02-01

Bug
---

* Fix deadlock observerd in EOSATLAS between gFsView.ViewMutex and pAddRmFsMutex from the
  scheduling part.
* Fix bug on the FST realted to the file id value going beyond 2^32-1
* [EOS-2275] - Possible data race in ThreadPool
* [EOS-2290] - increase shutdown timeout for the FSTs

New Feature
----------+

* Add skeleton for new "fs" command using protobuf requests
* Add skeleton for CTA integration
* Enhance the mutex deadlock detection mechanism


``v4.2.11 Citrine``
===================

2018-01-25

Bug
---

* [EOS-2264] - Fix possible insertion of an empty FS in FSView
* [EOS-2270] - FSCK crashed booting namespace
* [EOS-2271] - EOSPUBLIC deadlocked
* [EOS-2261] - "eos node ls <node>" with the monitoring flag does not apply the node filter
* [EOS-2267] - EOSPublic has crashed while recusively setting ACLs
* [EOS-2268] - Third party copying (on the same instance) fails with big files

Improvement
-----------

* [EOS-2283] - Double unlock in CITRINE code

Task
----

* [EOS-2244] - Understand EOSATLAS configuration issue


``v4.2.10 Citrine``
===================

2018-01-24

Bug
---

* [EOS-2264] Fix possible insertion of an empty FS in FSView
* [EOS-2258] If FST has qdb cluster configuration then to the dumpmd directly against QuarkDB
* [EOS-2277] fixes 'fake' truncation failing eu-strip in rpm builds of eos

Improvements
------------

* Refactoring of includes to speed up compilation, various build improvements
* avoid to call IsKnownNode to discover if an FST talks to the MGM, rely on sss + daemon user
* use (again) a reader-preferring mutex for the filesystem view


``v4.2.9 Citrine``
===================

2018-01-18

Bug
---

* [EOS-2228] Crash around forceRefreshSched related to pFsId2FsPtr

New Feature
-----------

* Filter out xrdcl.secuid/xrdcl.secgid tags on the FSTs to avoid triggering a
  bug on the xrootd client implementation

Improvements
------------

* [EOS-2253] Small writes should be aggregated with the journal
* Refactoring of the includes to speed up compilation


``v4.2.8 Citrine``
===================

2018-01-16

Bug
---

* [EOS-2184] - "eos ls -l" doesn't display the setgid bit anymore
* [EOS-2186] - eos ns reports wrong number of directory
* [EOS-2187] - Authproxy port only listens on IPv4
* [EOS-2211] - CITRINE deadlocks on namespace mutex
* [EOS-2216] - "binary junk" logged in func=RemoveGhostEntries (FID?)
* [EOS-2224] - selinux denials with eosfuse bind.
* [EOS-2229] - files downloaded with scp show 0 byte contents
* [EOS-2230] - read-ahead inefficiency
* [EOS-2231] - ioflush thread serializes file closeing and leads to memory aggregation
* [EOS-2241] - Directory TREE mv does not invalidate source caches

New Feature
-----------

* [EOS-2248] - FUSEX has to point ZMQ connection to active master

Improvement
-----------

* [EOS-2238] - Print a warning for 'node ...' functions when an FST is seen without a GEO tag

Support
-------
* [EOS-2208] - EOS MGM (new NS) aborts with "pure virtual method called" on update (restart?)


``v4.2.7 Citrine``
===================

2017-12-18

Bug
---

* [EOS-2207] - Work-around via environment variable to avoid loading too big no-replica sets (export EOS_NS_QDB_SKIP_UNLINKED_FILELIST)

* Many improvements and fixes for eosxd
  - fixing gateway mount options to work as NFS exports
  - fixing access function which was not refreshing caps/md objects

``v4.2.6 Citrine``
===================

2017-12-18

Bug
---

* [EOS-2150] - Repair option for d_mem_sz_diff error files
* [EOS-2202] - Lock-order-inversion between gAccessMutex and ViewMutex

* Many improvements and fixes for eosxd

``v4.2.5 Citrine``
===================

2017-12-12

Bug
---

* [EOS-2142] - eosfstregister fails to get mgm url in CentOS 7
* [EOS-2146] - symlinks have to show the size of the target string
* [EOS-2147] - listxattr creates SEGV on OSX
* [EOS-2148] - eosxd on OSX creates empty file when copying with 'cp'
* [EOS-2159] - An owner of a directory has to get always chmod permissions
* [EOS-2161] - rm -rf on fusex mount fails to remove all files/subdirectories
* [EOS-2167] - new file systems added go to 'spare.0'
* [EOS-2174] - Running out of FDs when using a user mount
* [EOS-2175] - eos ns command takes 10s on EOSPPS
* [EOS-2179] - calling verifychecksum issue
* [EOS-2180] - Unable to access quota space <filename> Read-only file system

* Many improvements and fixes for esoxd
* Performance improvements and fixes for the namespace and QuarkDB

``v4.2.4 Citrine``
===================

2017-11-28

Bug
----

* [EOS-2123] - eosxd renice's to lowest possible priority
* [EOS-2130] - segv while compiling eos
* [EOS-2137] - JSON output doesn't work anymore

Improvements
------------

* Many improvements and fixes for eosxd
* Many improvements and fixes for the namespace on QuarkDB


``v4.2.3 Citrine``
===================

2017-11-17

New features
------------

* New centralized draining implementation
* mgmofs.qdbcluster option in the configuration of the MGM to connect QuarkDB cluster

Improvements
------------

* Use the flusher also in the quota view of the new namespace
* Use prefetching for TPC transfers

Bug
---
* [EOS-2117] - mount.eosx should filter invalid options
* Fix ns stat statistics


``v4.2.2 Citrine``
===================

2017-11-14

Improvements
------------

* Many fixes for the eosxd fuse module
* Add eos_dump_proto_md tool to dump object metada info from QuarkDB
* Clean-up and improvements of the eos_ns_conversion tool for the new namespace
* Fix ns stat command not displaying ns info in monitoring format


``v4.2.1 Citrine``
===================

2017-11-10

Bug
---

* [EOS-2017] - MGM crash caused by FSCK
* [EOS-2061] - converter error in  "file adjustreplica" on raid6/archive layouts
* [EOS-2050] - Scheduling problem with adjustreplica and draining filesystem
* [EOS-2066] - xrdcp "Error [3005]" trying to transfer a "degraded" archive/raid6 file
* [EOS-2068] - Archive should use root identity when collecting files/dirs
* [EOS-2073] - MGM (citrine 4.1.30) unable to load configuration due to #iostat::udptargets with empty value
* [EOS-2092] - Auth proxy crashes
* [EOS-2093] - eos file convert from raid6/archive to replica:2 seems to not work.
* [EOS-2094] - JSON Return 0 instead of "NULL" when space.nominalsize is not defined

Task
----
* [EOS-1998] - Allow FST to login even when client traffic is stalled

Improvement
-----------

* [EOS-2101] - Report logical used-space when using xrootd commands
* A lot of improvements on the fusex side


``v4.2.0 Citrine``
===================

2017-10-23

Bug
----

* [EOS-1971] - EOS node listing crash
* [EOS-2015] - Table engine display values issue
* [EOS-2057] - Wrong conversion between IEC and Metric multiples
* [EOS-2060] - XrdMgmOfsFile SEGV out of bounds access

New Feature
-----------

* [EOS-2030] - Add '.' and '..' directories to file listings
* Prototype for the new fuse implementation i.e fusex
* Refactor of the ns command to use ProtoBuf-style commands

Task
----

* [EOS-2033] - quota id mapping for non-existing users

Bug
----

* [EOS-2016] - avoid SEGV when removing ghost entries on FST
* [EOS-2017] - avoid creating NULL object in map when resetting draining
* DOC: various corrections - use solar template with new WEB colour scheme


``v4.1.31 Citrine``
===================

2017-09-19

Bug
----

* [EOS-2016] - avoid SEGV when removing ghost entries on FST
* [EOS-2017] - avoid creating NULL object in map when resetting draining
* DOC: various corrections - use solar template with new WEB colour scheme

``v4.1.30 Citrine``
====================

2017-09-15

Bug
----
* [EOS-1978] - Preserve converted file ctime and ctime (CITRINE)
* FUSE: fix significant leak when returning a cached directory listing
* MGM: Enforce permission check when utime is executed
* MGM: Fix uid/gid overflow and comparison issues
* HTTP: fix ipv4/6 connection2ip function


``v4.1.29 Citrine``
===================

2017-09-08

Bug
----
* Mask the block checksum for draining and balancing when there is layout
  requesting blockchecksum for replica files.
* Add protection in case the proxys or the firewalleps vectors are not
  properly populated and we try to access a location beyond the size of the
  vector which leads to undefined behaviour.
* Multiple fixes to the Schedule2Drain code
* [EOS-1893] - EOS configuration can end up empty or truncated
* [EOS-1989] - eos file verify <path> -checksum is broken
* [EOS-1991] - eos-fuse rpm package broken dependency
* [EOS-1996] - space ls geo output is wrongly formatted

``v4.1.28 Citrine``
===================

2017-08-30

Bug
---
* [EOS-1991] - eos-fuse rpm package broken dependency

``v4.1.27 Citrine``
===================

2017-08-28

Bug
---
* [EOS-1976] - EOSD client memory leak
* [EOS-1986] - EOSPUBLIC: Crash when deleting a file entry
* [EOS-1984] - MGM: only show available fs on geosched show state latency and penalties tables.
* [EOS-1974] - NS: add missing initialization of pData (might lead to a SEGV during compaction if mmapping is disabled)

Improvement
-----------
* [EOS-1791] - RFE: attempt to auto-unmount on eos-fuse-core updates
* [EOS-1968] - eosd: always preload libjemalloc.so.1
* [EOS-1983] - Built-in http server should be dual-stack

New features
------------

* New accounting command - "eos accounting".

``v4.1.26 Citrine``
===================

2017-08-07

Bug
---
* [EOS-558] - "eos fileinfo" should better indicate non-active machines
* [EOS-1895] - MGM Crash when the groupscheduler can't place file
* [EOS-1897] - /var/log/eos/archive/eosarchived.log is world-writeable, should not
* [EOS-1906] - Incorrect GeoTree engine information
* [EOS-1936] - EOS ATLAS lost file due to balancing

Story
-----
* [EOS-1919] - Bug visible when creating YUM repositories on the FUSE mount in CITRINE instances

Improvement
------------
* [EOS-1159] - renaming a "quota node" directory gets rid of the quota setting?
* [EOS-1345] - documentation update - eos fs help
* [EOS-1875] - RFE: isolate eos client from LD_LIBRARY_PATH via RPATH

* Plus all the fixes from the 0.3.264 and 0.3.265 release form the bery_aquamarine branch.


``v4.1.25 Citrine``
===================

2017-06-29

Bugfix
------
* [EOS-542] - eos file version filename version modify the permissions of the file
* [EOS-1259] - MGM eos node ls display
* [EOS-1292] - "eos" hangs for 5min without EOS_MGM_URL - give verbose error message instead
* [EOS-1317] - command to drop/refresh UID / GID cache is not documented?
* [EOS-1762] - "eos attr link origin target" with a non-existent origin prevents listing of target's atrributes
* [EOS-1887] - Link back with the dynamic version of protobuf3
* [EOS-1889] - file verify command fails when specifyng fsid on a one-replica file
* [EOS-1893] - EOS configuration can end up empty or truncated
* [EOS-1888] - FSs wrongly reported as Unavailable by the GeoTreeEngine
* [EOS-1892] - File copy is scheduled on a full FS

New Feature
-----------
* [EOS-1872] - "Super" graceful FST shutdown
* There is a new dependency on protobuf3 packages both at build time and run time.
  These packages can be downloaded from the citrine-depend yum repository:
  http://storage-ci.web.cern.ch/storage-ci/eos/citrine-depend/el-7/x86_64/

Improvement
-----------
* [EOS-1581] - RFE: better error messages from the eos client, remove 'error: errc=0 msg=""'


``v4.1.24 Citrine``
===================

2017-06-14

Bugfix
------
* [EOS-162] - RFE: auto-refill spaces from "spare", up to "nominalsize"
* [EOS-455] - RFE: drop either fid: or fxid:, use the other consistently
* [EOS-1299] - MGM node and fs printout with long hostname
* [EOS-1716] - MGM: typo/missing whitespace in "client acting as directory owner" message
* [EOS-1859] - PPS crash while listing space
* [EOS-1877] - eos file drop does not accept fid:XXXX
* [EOS-1881] - List quota info not working anymore on EOSLHCB
* Fix fsck bug mixing information from different types of issues

Task
-----
* [EOS-1851] - mount.eos assumes sysv or systemd present

Improvement
-----------
* [EOS-1875] - RFE: isolate eos client from LD_LIBRARY_PATH via RPATH

Support
-------
* [EOS-1064] - get the year information for EOS file


``v4.1.23 Citrine``
===================

2017-05-17

Bugfix
------
* MGM: Take headroom into account when scheduling for placement
* MGM: Add protection in case the bookingsize is explicitly set to 0
* ARCHIVE: Use the MgmOfsAlias consistently otherwise the newly generated archive file will contain invalid JSON lines.


``v4.1.22 Citrine``
===================

2017-05-15

Bugfix
------
* Fix response for xrdfs query checksum to display "adler32" instead of "adler" as checksum type
* Fix launch of the follower thread for the MGM slave


``v4.1.21 Citrine``
===================

2017-05-12

Bugfix
------
* [EOS-1833] - eosfuse.cc uses a free'd fuse_req_t -> segfault
* [EOS-1781] - MGM crash in GeoBalancer
* [EOS-1642] - "Bad address" on EOS FUSE should be "Permission denied"
* [EOS-1830] - Recycle bin list crash when doing full scan (need protection)


Task
----
* [EOS-1848] - selinux error when uninstalling eos-fuse-core

User Documentation
------------------
* [EOS-1826] - Missing dependencies on the front page

Suggestion
----------
* [EOS-1827] - Ancient version of zmq.hpp causing issues when compiling with new zmq.h (taken from system)
* [EOS-1828] - Utils.hh in qclient #include cannot find header
* [EOS-1831] - CMAKE, microhttpd, and client
* [EOS-1832] - Bug in console/commands/com_fuse.cc with handling of environment variable EOS_FUSE_NO_MT


``v4.1.3 Citrine``
==================

2016-09-15

Bugfix
-------

* [EOS-1606] - Reading root files error when using eos 4.1.1
* [EOS-1609] - eos -b problem : *** Error in `/usr/bin/eos': free():


``v0.4.31 Citrine``
===================

2016-07-22

Bugfix
-------

- FUSE: when using krb5 or x509, allow both krb5/x509 and unix so that authentication
        does not fail on the fst (using only unix) when using XRootD >= 4.4


``v0.4.30 Citrine``
===================

2016-07-21

Bugfix
-------

- SPEC: Add workaround in the %posttrans section of the eos-fuse-core package
        to keep all the necessary files and directories when doing an update.
- CMAKE: Remove the /var/eos directory from the eos-fuse-core package and fix
        type in directory name.

``v0.4.29 Citrine``
===================

Bugfix
-------

- MGM: add monitoring switch to space,group status function
- MGM: draing mutex fix and fix double unlock when restarting a drain job
- MGM: fixes in JSON formatting, reencoding of non-http friendly tags/letters like <>?@
- FST: wait for pending async requests in the close method
- SPEC: remove directory creation scripting from spec files

New Features
------------

- RPM: build one source RPM which creates by default only client RPMs with less dependencies
