:orphan:

.. highlight:: rst

.. index::
   pair: Releases; Diopside


Diopside Release Notes
===========================

``Version 5 Diopside``

Introduction
------------

This release is based on XRootD V5.


``v5.2.32 Diopside``
====================

2025-04-17

Bug
----

* COMMON: Mapping: use getgrouplist to simplify fetching secondary groups


``v5.2.31 Diopside``
====================

2024-12-18

Bug
---

* MGM: Track the last update timestamp of the balancer statistics per space to
  avoid interference when we have several spaces balancing at the same time
  which can lead to starvation of some spaces.


``v5.2.30 Diopside``
====================

2024-12-17

Bug
----

* AUTH: Expose also the XrdSfsFileSystem XRootD API that is needed for the EOS HTTP plugin
* MGM: Make sure the FsBalancer runs only on the current MGM master


``v5.2.29 Diopside``
====================

2024-12-09

Bug
----

* AUTH: Fix bug in authentication daemons so that they can communicate with the MGM.


``v5.2.28 Diopside``
====================

2024-10-17

Bug
----

* [EOS-6065] - MGM memory increase/leak (EOSHOMEs)
* [EOS-6217] - eosxd looping in async open during write recovery


``v5.2.27 Diopside``
====================

2024-10-01

Note
-----

* This release is targeted for the CTA use-case as it's built with eos-xrootd/xrood 5.7.1
  that contains some HTTP header passing functionality required for CTA.
* Built with eos-xrootd/xrootd 5.7.1


``v5.2.26 Diopside``
====================

2024-10-01

Bug
----

* [EOS-6205] - FUSEX: timing-related access issue (initial "No such file or directory" (Kerberos, ACRON)
* [EOS-6207] - eos fusex crash
* [EOS-6211] - fst segfault or hang, async close triggered during XrdFstOfsFile destructor

New feature
------------

* [EOS-6200] - MGM - HTTP Take into account OpenWriteCreate limit


``v5.2.25 Diopside``
====================

2024-07-05

Note
----

* This EOS release is based on eos-xrootd-5.6.11 which itself bring important fixes like
  - memory leaks in the XRootD python bindings
  - fixes to crashes seen in production with EOS etc.

Bug
----

* [EOS-6087] - [eoscp] Intermittent segmentation faults in LHCb datamovers
* [EOS-6155] - Touch should NOT require 10737418240 bytes as booking size
* [EOS-6172] - man eos-ls wrong formatting
* [EOS-6197] - Report: Undefined behavior in constructor if sec.host is an empty string (deletion)
* [EOS-6126] - Recovery OpenAsync cannot open file anymore in eosxd


``v5.2.24 Diopside``
====================

2024-05-23

Bug
---

* [EOS-6112] - Remove reliance on 'errno' from _dropallstripes() and other functions MGM(CTA)
* [EOS-6148] - Too many levels of symbolic links unexpectedly reported on eosxd mounted fs

New Feature
------------

* [EOS-6150] - Print archive metadata in eoscta report MGM(CTA)
* Add new eos-mgm-monitoring package containing a series of helper scripts for monitoring.

Improvement
------------

* [EOS-6139] - MGM - HTTP GET issues 2 consecutive stats instead of only one


``v5.2.23 Diopside``
====================

2024-04-30

Note
----

* Update eos-xrootd dependency to 5.6.10 - this version includes important
  optimizations for the use of OpenSSL 3.

Bug
----

* [EOS-5972] - rising "HB is stuck" time, apparent deadlock wait_upstream/mdcflush
* [EOS-6109] - Rename - Deadlock with concurrent renames
* [EOS-6120] - deadlock during EosFuse::mkdir

Improvement
------------

* ALL: Many compilation warning fixes


``v5.2.22 Diopside``
====================

2024-04-09

Bug
----

* [EOS-6116] - FUSEX: fix eosxd callback handler when a file is moved on top of an existing file
* [EOS-6115] - FUSEX: fix invisible directories if the name had been put into the ENOENT cache
* [EOS-6111] - FST: mark readV errors as read IO errors in the report log
* [EOS-6110] - MGM: fix loop in devices thread in non-master MGMs
* FST - fix interface speed reading


Improvement
------------

* [EOS-6117] - FST: ErrorReports are suppressed on FSTs when over 4 Hz to 1Hz + marker
* [EOS-6114] - FUSEX: eosxd and MGM share the same assumption, that as an owner of directory you can delete a file of another person even if !d was specified for the group


``v5.2.21 Diopside``
====================

2024-03-25

BUG

* [EOS-6105] - fix credential validation in ALMA9 container under chroot environments

``v5.2.20 Diopside``
====================

2024-03-21

Bug
---

* [EOS-6091] - Update PersistentSharedHash before publishing updates
* [EOS-6101] - fs rm no longer sends a notification to the FST


``v5.2.19 Diopside``
====================

2024-03-12


Note
----

* Update dependency to xrootd/eos-xrootd 5.6.9

Bug
----

* [EOS-6085] - EOSPUBLIC mgm crash during BroadcastDeletionFromExternal in rename
* [EOS-6088] - MGM aborts with "what():  std::bad_alloc" under eos::mgm::FuseServer::Caps::BroadcastDeletionFromExternal


``v5.2.18 Diopside``
====================

2024-03-07

Bug
----

* [EOS-6075] - [eoscp] memory leaks and context errors
* [EOS-6078] - eos archive segv in xrootd prepare
* [EOS-6079] - Credential validation fails in chroot container with non local jail lookup
* [EOS-6080] - "eos find --purge atomic" can lock up namespace
* [EOS-6081] - "eos find --purge atomic" can cause slow restarts (FSCK loads one big hash at startup)
* [EOS-6082] - MGM crash from early "eos ns stat" command (under eos::common::ThreadPool::GetInfo)
* [EOS-6084] - "Scheduler is not yet initialized" from early setDiskStatus() (possible: drain?)


New Feature
------------

* [EOS-6045] - Monitor number or kworker processes with 'eos node ls --sys'


Improvement
------------

* [EOS-5185] - FUSEX can not write to logical quotas <= 5GB (hardcoded limit)
* [EOS-5835] - MGM: remove internal redirect for "/" to port 8443


``v5.2.17 Diopside``
====================

2024-02-29

Note
----

* Update dependency XRootD/eos-xrootd to 5.6.8


Bug
----

* [EOS-6061] - Disk drain failure, replicas are on disk, but adjustreplica fails to replicate
* [EOS-6062] - MGM: "fs mv" randomly "forgets" filesystems
* [EOS-6064] - MGM stuck (namespace locking)
* [EOS-6066] - eos cp -r (recursive copy) uses "find", does not work on redirection (?)
* [EOS-6070] - FST aborts with "what():  basic_string::_S_construct null not valid" under eos::fst::ScanDir::CheckFile()
* [EOS-6074] - Crash in FlatScheduler

Improvement
------------

* [EOS-6048] - RFE: FST should not "check for Fmd xattr conversion" at boot


``v5.2.16 Diopside``
====================

2024-02-16

Bug
----

* [EOS-6051] - MGM: fix crash in FSScheduler caused by edgecases at boot time


``v5.2.15 Diopside``
====================

2024-02-15

Bug
----

* [EOS-6044] - FUSEX: fix 0-pointer access into data object map - fixes EOS-6044
* [EOS-6046] - MGM: flat scheduler know honours configuration changes on filesystems immediately

New Feature
-----------

* MGM - return EBUSY and HTTP::CONFLICT when opening a file locked via the xattr interface (collaborative editing)

  ``v5.2.14 Diopside``
====================

2024-02-13

Bug
----

* [EOS-6009] - FUSEX: don't overwrite FILE:/!tmp locations as KRB5 default location
* NS: Catch exception in FutureVectorIterator destructor


``v5.2.13 Diopside``
====================

2024-02-12

Bug
----

* [EOS-3898] - EOS permissions system incorrectly requires an explicit '+u' privilege for the root user
* [EOS-4763] - ACL set argument 'foo:foo:+d' does not work
* [EOS-4796] - Not consistent behaviour when setting user.acl with attr set and acl --user
* [EOS-6009] - FUSEX: fix retrieval of default kerberos crednetial location if not under FILE:/tmp/
* [EOS-6013] - FUSEX: fix hash function used to cache connections to distinguish container credentials using identical internval paths
* [EOS-6016] - MGM crash during shutdown in eos::mgm::ConverterDriver::ScheduleJob()
* [EOS-6025] - MGM: accumulating "atomic" version files (from sync client) if out of volume quota
* [EOS-6029] - MGM (subprocess?) crash in qclient::FollyFutureHandler::stage()
* [EOS-6038] - MGM misses broadcast message to deal with renames
* MGM: fix 'find --fileinfo --cache'
* FST: fix publishing of 'xrootd' version in 'node ls --sys'
* CONSOLE: fix broken 'eos report' for reads


New Feature
------------

* [EOS-5614] - FUSEX: bypass deletion through recycle bin, if a file is deleted while still open for writign
* [EOS-5879] - [eoscp] Add the possibility to see the version of the command
* [EOS-5956] - Implement default XRootD Attribute functions for xrootd prefixes
* [EOS-6040] - GRPC: implement reycle bin listing with date/index filter
* FUSEX: code refactoring allowing to re-use functionality of eosxd authentication in eoscfsd
* CFSD: adding POSIX passthrough filesystem implementation packaged in new RPM eos-cfsd

Improvement
------------

* [EOS-2373] - Inconsistent handling of linked attributes in attr_ls and attr_get
* [EOS-5614] - Fuse skip recycle bin for known broken files
* [EOS-5717] - [eos-archive] Review the workflow + files with no checksum on destination make the tool crash

Reverted
--------

* MGM/CONSOLE: reverted removing 'eos old find' implementation


``v5.2.12 Diopside``
=========================

2024-02-11

Bug
---

* FST: Fix overflow when reading file larger than 4GB during rain-check
* FST: Fix reading of the network speed value
* MGM: avoid parallel computation of the currently used physical space and cache for 2 minutes
* REVERT: COMMON: RWMutex: lock the mutex name map before finding items


``v5.2.11 Diopside``
=========================

2024-02-06

Note
----

* Update eos-xrootd/xrootd dependency to 5.6.7

Bug
----

* [EOS-6028] - EOS: ACL command help displays wrong option


``v5.2.10 Diopside``
=========================

2024-02-02

Bug
----

* [EOS-6022] - mkdir -p does not broadcast properly to eosxd clients


``v5.2.9 Diopside``
=========================

2024-02-02

Bug
----

* [EOS-6012] - Fix crash in eos::mgm::ConversionJob::Merge() when logging error message


``v5.2.8 Diopside``
=========================

2024-01-29

Bug
----

* MGM: Add legacy find command implementaiton for old clients.


``v5.2.7 Diopside``
=========================

2024-01-26

Note
----

* Update eos-xrootd/xrootd dependency to 5.6.6

Bug
---

* [EOS-5770] - "eos node ls --sys" - messed-up formatting (newline after "sockets"?)
* [EOS-5877] - MGM crash while registering new FST
* [EOS-5934] - FST "failed to parse metadata info" for existing filenames prevents EA conversion
* [EOS-5949] - undrainable "fuse::needsflush" file - outdated "mgmsize" does not match on-disk size
* [EOS-5986] - Add support for long filename (> 2kB) for Getfmd requests
* [EOS-5987] - RWMutex: concurrent modification of the Mutex Name map
* [EOS-5988] - MGM: concurrent modification of sync Time Accounting class
* [EOS-5989] - concurrent modification of RWMutex at configure stage
* [EOS-5993] - MGM: do not log SYMKEY on start
* [EOS-5998] - FST crash under eos::fst::RainMetaLayout::Open()
* [EOS-5999] - Connection Idle timeouts create broken FUSE replicas
* [EOS-6006] - EOS MGM lockup/unresponsive on EOSPROJECT-I00

New Feature
-----------

* [EOS-5970] - Implement scitags in EOS for HTTP transfers
* [EOS-5971] - Add RX/TX errors and dropped pack errors to FST monitoring
* [EOS-6010] - CLI: Remove eos oldfind from the console

Task
----

* [EOS-6003] - eos: sched ls output doesn't list all disks
* [EOS-6004] - eos: scheduler: active status not taken into consideration

Improvement
-----------

* [EOS-5744] - Forbid archival of directories that contain symlinks
* [EOS-5745] - Forbid archival of directories with 0 size files
* [EOS-5982] - Skip checksumming files with FUSE
* [EOS-5990] - Add FSCK reset


``v5.2.6 Diopside``
==========================

2024-01-15

Bug
---

* [EOS-5977] - NS: Double check md object is not null before constructing md locked object



``v5.2.5 Diopside``
==========================

2024-01-09

Bug
---

SPEC: Fix missing target when building in client mode only


``v5.2.4 Diopside``
==========================

2023-12-18

Note
----

* Update eos-xrootd/xrootd dependency to 5.6.4
* Update eos-rocksdb dependency to 8.8.1


Bug
----

* [EOS-5657] - Overreplication in EC preventing reading files
* [EOS-5937] - Fix 'EOS command 'evict'/'stagerrm' not deleting files on FST'
* [EOS-5965] - FUSEX: TSAN data race on setting pid in shared mdx object
* CONSOLE/MGM: Fix EOS command evict/stagerrm not deleting files on FSTs [CTA]

New Feature
------------

* [EOS-5511] - suggestion: rate limit on errors


Improvement
------------

* [EOS-5718] - Fsck request to repair overreplicated files in EC
* [EOS-5919] - Disable fallocate on FSTs when filesystem != XFS by default


``v5.2.3 Diopside``
==========================

2023-12-13

Bug
----

* FST: Http chunk upload - avoid infinite loop for misbehaving clients


``v5.2.2 Diopside``
==========================

2023-11-08

Bug
----

* MGM: Make sure token information is passed to all namespace operations
* MGM: Avoid re-entrant lock in space ls
* SPEC: Add eos-grpc-gateway as an explicit requirement


``v5.2.1 Diopside``
==========================

2023-11-06

Bug
----

* [EOS-5849] - MGM crash, possibly around eos::QuarkHierarchicalView::getUriInternal()
* [EOS-5858] - FlatScheduler: groups are not retried
* [EOS-5861] - MGM crash (corrupted free memory?)
* [EOS-5862] - Files with strange state after editing on two places at the same time via FUSE
* [EOS-5866] - Invalid NS entry when a file is renamed on top of a hard-link with recycle bin enabled
* [EOS-5872] - NS: IFileMD::unlinkLocation() takes a read lock instead of a write lock
* [EOS-5895] - MGM memory increase (EOSHOMEs)
* [EOS-5902] - XrdHttp access throws 500 when file name contains a '#'
* [EOS-5903] - Left over fst.ioping.XXXX files on FSTs
* [EOS-5904] - Fix unsafe modification in Qdb Master logging
* [EOS-5906] - 5.2 FST don't start because of benchmark files irritating LevelDB check code

Improvement
------------

* [EOS-5792] - Document the possibility of moving fs between nodes in the help and the eos official documentation
* [EOS-5894] - MGM memory increase with agressive parameters for balancing


``v5.2.0 Diopside``
==========================

2023-10-10

Note
----

* Update dependency to eos-xrootd-5.6.2 that matches XRootD-5.6.2.
* New eos-grpc-1.56.1 dependency that obsoletes any previous eos-protobuf3 packages.


Bug
----

* [EOS-5429] - [TAPE REST API] Modify STAGE polling (GET) logic to take into account files not queued on CTA
* [EOS-5680] - MQ overloaded when deleting a large number of EC files
* [EOS-5687] - CtaUtils: GCC12 FTBS
* [EOS-5694] - chunked upload fails on EOS5 + XrdHTTP
* [EOS-5699] - request retries discarded on RAIN layout
* [EOS-5700] - readv errors ReedSLayout claims corrupted but file is ok
* [EOS-5704] - RAIN layouts don't enable XrdIo read-ahead
* [EOS-5732] - removexattr fails with ENOENT when trying to remove any of the extended attributes from a created file
* [EOS-5784] - /etc/cron.d/eos-reports : do not use "bc"
* [EOS-5791] - Force physical space info for xrdfs spaceinfo command not working
* [EOS-5798] - FST abort() on "no manager name" shutdown: "terminate called without an active exception"
* [EOS-5825] - eosxd heartbeat stuck, duration slowly rising (maybe mdcflush deadlock)
* [EOS-5826] - eosxd rising heartbeat time, suspected mdx left locked by exited thread
* [EOS-5832] - FUSEX crash around cap::capx::lifetime(this=0x0)
* [EOS-5842] - FUSEX: throw in data::datax::attach
* [EOS-5843] - Wrong quota checks when recycling directories with EC files
* [EOS-5855] - Cannot remove access limits already introduced by username

New Feature
------------

* [EOS-5613] - Store in xattr who deleted a file
* [EOS-5716] - [eoscp] Create JSON output in addition to the text output
* [EOS-5857] - Add support for HTTP REST API via grpc-gateway


Task
----

* [EOS-5530] - Send fid as string to CTA
* [EOS-5856] - Libmicrohttpd support disabled by default

Improvement
------------

* [EOS-5537] - RS layouts don't use read-ahead anymore
* [EOS-5703] - Modifications to eos `evict`/`stagerrm` command
* [EOS-5707] - eos-config-inspect dump: allow to choose a particular config backup
* [EOS-5734] - eos recycle -m, revert usage of underscore on keys
* [EOS-5739] - RFE: honour sys.app.lock also when serving flock operations via FUSE
* [EOS-5779] - EOS: server rpm upgrades shouldn't affect quarkdb
* [EOS-5819] - Forbid quota set cli on recycle bin
* [EOS-5831] - Add Birthtime vs Accesstime distributions to inspector output
* [EOS-5840] - Add 'du' command to CLI


``v5.1.30 Diopside``
==========================

2023-09-27

Bug
---
* [EOS-5834] - Corrected MGM Namespace mutex tracking

New feature
-----------

* MGM: add 'eos ns benchmark' command to run inside the MGM a multithreaded benchmark

``v5.1.29 Diopside``
==========================

2023-09-14

Bug
----

* [EOS-5771] - HTTP transfers of a file with no disk replicas create a zero-length file
* [EOS-5813] - Show physical space info for xrdfs spaceinfo query
* [EOS-5818] - FST crash in eos::fst::FmdConverter::ConvertFS

Improvement
-----------

* [EOS-5530] - Send fid as string to CTA
* [EOS-5822] - Implement JSON output for eoscp command


``v5.1.28 Diopside``
==========================

2023-09-01

New Feature
-----------

* [EOS-5803] - Introduce New groupbalancer engine - freespace which balances on
  absolute freespace Additionally blocklisting groups is now supported in this
  engine.

``v5.1.27 Diopside``
==========================

2023-08-04

Note
----

* Pin down the eos-grpc dependency package version to 1.41.0 to better control the update process in the future.

Bug
---

* [EOS-5763] - eosxd: occasional very large max-inode-lock-ms reported
* [EOS-5776] - Blocked IO measurement can be wrong in case of multithreaded readers on same inode
* [EOS-5768]: File write recovery can lead to file loss
* FUSEX: put back md-cache auto-cleanup on umount, which was removed since 5.1.25


``v5.1.26 Diopside``
==========================

2023-07-26

Bug
---

* FUSEX: protect against inserting md objects with ino=0
* FUSEX: check the md err code of entries returned by the server before using
* FUSEX: add sanity check to not dump a swapped-out meta-data object which is in the LRU list
* FUSEX: avoid writing into swapped-out MD objects
* FUSEX: remove dead code deleting old cache entries


``v5.1.25 Diopside``
==========================

2023-07-20

Bug
----

* [EOS-5753] - Crash in LRU remove function
* [EOS-5754] - cp -a gives "preserving times for .. : Invalid argument" - negative accesstime?
* [EOS-5748] - MGM: Disable TPC timeout estimates as this can lead to corruption of RAIN
  stripes for slow transfers - temporary workaround.


``v5.1.24 Diopside``
==========================

2023-07-14

Bug
----

* [EOS-5652] - eosxd abrtd reports from lxplus
* [EOS-5480] - eosxd crash under count() / metad::lookup() / EosFuse::lookup()
* [EOS-5486] - eosxd crash with SIGABRT
* [EOS-5667] - eosxd abtrd reports from lxplus705
* [EOS-5668] - Input/output error on FUSE mount, client ok
* FUSEX: don't return EFAULT with invalid statvfs responses
* FUSEX: avoid some further concurrent access to md attr field


``v5.1.23 Diopside``
==========================

Bug
----

* [EOS-5695] - some Fsts not booting into EOS after upgrade to 5
* [EOS-5696] - Allow 0-sized CTA files to be deleted from EOS namespace
* [EOS-5699] - request retries discarded on RAIN layout

New Feature
------------

* [EOS-5697] - [eoscp] Add checksum comparison between source and destination


``v5.1.22 Diopside``
==========================

2023-05-24

Bug
----

* COMMON: Serialize calls to setgrent/getgrent/endgrent since they are not thread-safe and can cause a crash


``v5.1.21 Diopside``
==========================

2023-05-24

Bug
----

* COMMON: Fix handling of eos token when passed as HTTPS bearer authorization header


``v5.1.20 Diopside``
==========================

2023-05-10

This release is based on eos-xrootd-5.5.10/xrootd-5.5.5

Bug
---
* This release updates to using eos-xrootd-5.5.10 which includes
a fix for a regression when higher fdlimits are needed


``v5.1.19 Diopside``
==========================

2023-05-10

This release is based on eos-xrootd-5.5.9/xrootd-5.5.5

Bug
---
* MGM: Do special handling for HEAD requests

Improvement
------------
* [EOS-5658] - support external host/port alias for FSTs


``v5.1.18 Diopside``
==========================

2023-05-08

Bug
----

* SPEC: Fix dependency to point to eos-xrootd-5.5.9/xrootd-5.5.5


``v5.1.17 Diopside``
==========================

2023-05-08

Bug
---

* [EOS-5515] - EC file with undrained stripes that looks fine
* [EOS-5612] - Recycle bin setting change disables cleanup
* [EOS-5633] - Eos inspector: Considers a space already deleted
* [EOS-5601] - eos cp: Fix memory leaks in eos_roles_opaque
* FUSEX: fix permission denied errors for slow MGM requests
* FUSEX: fix ctime setting in eosxd3, enable write-back cache
* FUSEX: fix blocked statistic output when backen-end waits for a flush

Improvement
------------
* [EOS-5563] - add monitoring format to `eos fsck stat`
* [EOS-5626] - Converter - Rain file failed to convert (100GB)
* [EOS-5641] - Have Macaroons take into account vid VOMS mapping when determining client identit
* DOC: refactor documentation for Diopside releases


``v5.1.16 Diopside``
==========================

2023-04-04

Bug
----

* COMMON: Don't reset the current vid identity when handling KEYS mapping
  unless we actually have a hit in the map. This was breaking the vid mapping
  for gsi/http with voms extensions that have the endorsements field in the
  XrdSecEntity populated and this was interpreted as a key.


``v5.1.15 Diopside``
=========================

2023-03-27


Note
----

* Update dependency to eos-xrootd-5.5.8 which also matches XRootD-5.5.4

Bug
----

* [EOS-5577] - MGM crash in eos::mgm::GrpcWncServer::RunWnc()
* [EOS-5587] - jwt::decode might throw an exception
* [EOS-5600] - eos group ls outputs wrong filled stats


New Feature
------------

* [EOS-5588] - Allow HTTPS gateway functionality to use key entries

Task
----

* [EOS-5522] - Drain status stays in `expired` after setting fs in rw.
* [EOS-5530] - Send fid as string to CTA

Improvement
-----------

* [EOS-5578] - Balancer/Drainer/Recycler: reduce sleep info logging
* [EOS-5592] - Disabling oauth did not actually disabled it


``v5.1.14 Diopside``
=========================

2023-03-14

Bug
----

* [EOS-2520] - FST abort (coredump) on shutdown, "EPoll: Bad file descriptor polling for events"
* [EOS-5554] - Deadlock while setting acls recursive

New Feature
------------

* [EOS-5571] - Add atime to eos-ns-inspect tool
* [EOS-5573] - Show if namespace is locked-up
* [EOS-5576] - MGM: fileinfo -j does not output the file' status


``v5.1.13 Diopside``
=========================

2023-03-06

Bug
----

* [EOS-5546] - MGM: IoStat fprintf() stuck
* [EOS-5555] - FST segfaults around qclient::QSet::srem
* [EOS-5559] - EOS HTTP REST API - no JSON output if authentication is done with Bearer token

New features
------------
* [EOS-5561] - Create "eos df" command


``v5.1.12 Diopside``
=========================

2023-02-28

Bug
----

* [EOS-5526] - User Sessions count seems to be wrong
* [EOS-5534] - LRU should not walk down the recycle bin and apply policies
* [EOS-5535] - LRU tries to delete all directories having an empty deletion policy
* [EOS-5542] - Error when accessing directories with wildcards

Improvement
------------

* [EOS-5536] - LRU code has still in-memory namespace code


``v5.1.11 Diopside``
=========================

2023-02-15


Bug
----

* [EOS-5516] - Dangling files (possibly) after container is removed
* [EOS-5520] - eos CLI group resolution changed - INC3372876
* [EOS-5523] - eosxd recovery failing

Improvement
------------

* [EOS-5524] - Allow https gateway nodes to provide x-forwarded-for headers


``v5.1.10 Diopside``
=========================

2023-02-07

Note
----

* Update dependency to eos-xrootd-5.5.7 which also matches XRootD-5.5.2

Bug
----

* [EOS-5386] - iostat reports are not processed fast enough

Improvements
------------

* MGM: Make central balancer configurable at runtime
* FST: Chunk fsck requests to at most 50k entries per request
* MGM: enable hide-version also when heartbrate has been changed


``v5.1.9 Diopside``
=========================

2023-01-24


Bug
----

* [EOS-5487] - sticky bit on version folders makes Recycler not able to clean the files on the recycle bin.
* [EOS-5488] - New Year's crashes on all projects and homes
* [EOS-5489] - PropFind fails when namespace mappings should apply
* [EOS-5494] - eosxd looping when cleaning write queue
* [EOS-5495] - FST crashing while doing LevelDB->ext_attr conversion on a (not) broken (enough) disk
* [EOS-5498] - All 0 size files are marked as missing when using xattr fmd


New Feature
------------

* [EOS-5209] - Fsck removal should just move stripes to a quarantine directory


Improvement
------------

* [EOS-5501] - Allow black and whitelisting of token vouchers (ids)


``v5.1.8 Diopside``
=========================

2022-12-14

Note
----

* Update dependency eos-xrootd-5.5.5
* Includes an important fix for HTTP TPC PULL transfers.

Bug
----

* [EOS-5467] - Inspector aggregates results instead of reseting the current scan
* MGM: Add regfree in FuseServer regex usage to avoid memory leak
* MGM: Unlock the Access mutex when delaying a client to not get problems to get a write lock


Improvement
-----------

* [EOS-5478] - Invert Stall logic to check first user limits and then catch-all rules


``v5.1.7 Diopside``
=========================

2022-12-12

Bug
----

* [EOS-5474] - Conversion breaks files with FMD info in xattrs

Improvement
------------

* [EOS-5469] - Allow to select secondary groups with kerberos authentication and implement AC checks for secondorary groups
* [EOS-5471] - Add atime to EOS
* [EOS-5458] - Setting a namespace xattr might fail for wopi


``v5.1.6 Diopside``
=========================

2022-12-05

Bug
----

* [EOS-5467] - Inspector aggregates results instead of reseting the current scan

Improvement
------------

* [EOS-5465] - Shoe FUSE application name in 'fusex ls'
* [EOS-5466] - Add Stall / NoStall host lists to access interface


``v5.1.5 Diopside``
=========================

2022-12-02

Bug
----

* MGM: Fix MGM crash when the balancer is configured

Improvement
-----------

* [EOS-5452] - New metric: Provide I/O errors per transfer in report logs
* [EOS-5453] - New metric: Namespace contention calculation in ns stat command
* [EOS-5131] - RFE: honour XRD_APPNAME for xrdcp
* [EOS-5444] - Provide number of stripes in the inspector command
* [EOS-5454] - EOS inspector: Provide layout_id in the list output per fxid
* [EOS-5455] - eos node ls monitoring - Improve sys.uptime value format
* [EOS-5459] - MGM: avoid blocking cleanup ops while user mapping
* [EOS-5464] - Have TPC transfers respect the client tpc.ttl value


``v5.1.4 Diopside``
=========================

2022-11-22

Bug
----

* [EOS-5442] - eosxd crash (on shutdown) under ShardedCache destructor
* [EOS-5446] - Failures in setting thread names


``v5.1.3 Diopside``
=========================

2022-11-16

Bug
----

* [EOS-5162] - Setting ACL does not work when dir ends with whitespace
* [EOS-5433] - GroupBalancer: crash when conversions are scheduled before Converter
* [EOS-5436] - Origin Restriction does not work as expected
* [EOS-5437] - Fix potential leaks in Mapping::getPhysicalIds

New Feature
------------

* [EOS-5145] - Extending lock support
* [EOS-5438] - Don't stall clients when thread pool is exhausted and a rate limit is reached

Improvement
------------

* [EOS-5231] - Allow eos attr set to operate on CIDs
* [EOS-5344] - eos recycle -m: show inode used / max numbers
* [EOS-5401] - Return the inode number in FMD responses for GRPC
* [EOS-5412] - add qclient performance metrics on monitoring format.
* [EOS-5413] -  QClient performance: have last 5m, last 1m, etc metrics
* [EOS-5439] - Add eosxd3 to all builds when fuse3 is available and ship in the RPM


``v5.1.2 Diopside``
=========================

2022-10-04

Bug
----

* [EOS-5399] - FST: Segfaults in FmdConverter
* [EOS-5400] - FST crash in AccountMissing due to null Fmd object

Improvement
------------

* [EOS-3297] - Print the deviation used for the group balancer

New features
------------

* MGM: Add implementation for central group balancer using TPC


``v5.1.1 Diopside``
=========================

2022-09-15

Note
-----

* Update dependency to eos-xrootd-5.5.1
* eosd is now deprecated and there are no more RPM packages provided for it

Bug
----

* [EOS-5347] - EOS token not usable via eosxd
* [EOS-5369] - Occasional error during eoscta test "mismatch between requested fid/fsid and retrieved ones"
* [EOS-5371] - Fix crash of the MGM when listing container entries due to invalidated
               iterators to the ContainerMap/FileMap objects.
* FST: eos-xrootd-5.5.1 fixes a bug in XRootD related to async close functionality
  where the FST would crash if it received another requests for a file which was in
  the process of being closed.

New features
------------

* CTA: Enhance/extend EOS report messages for CTA prepare workflow


``v5.1.0 Diopside``
=========================

2022-09-02

Note
----

* This release comes with XRootD/eos-xrootd 5.5.0 as dependency

Bug
----

* [EOS-5377] - Unhandled exception in the GeoBalancer code
* [EOS-5367] - Fix IoStat intialization when there is no prior data in QuarkDB
* MGM: Fsck: correct the calculation of expected number of stripes in RepairFstXsSzDiff


Improvement
-----------

* [EOS-5380] - Qclient: handle folly warnings
* [EOS-5381] - Fix potential format overflows
* [EOS-5378] - Fix compilation warnings
* FUSEX: Add support for json statistics output

New features
-------------

* FST: Add support for storing file metadata info as extended attributes
  of the raw files on disk rather than using the LevelDB on disk.
  Disabled by default for the moment.


``v5.0.31 Diopside``
=======================

2022-08-12

Bug
----

* FST: Properly detect HTTP transfers and skip async close functionality in
  such cases
* [EOS-5359] - use after free in fusex::client::info
* [EOS-5358] - WNC GRPC unserialized global options


``v5.0.30 Diopside``
=======================

2022-08-11

Bug
---

* [EOS-5355] - System ACLs evaluation overruling logic is incorrect


New Feature
------------

* [EOS-5342] - CREATE cta workflow not triggered when new file created using fusex - DELETE workflow is also missing


Improvement
-----------

* [EOS-5343] - Better enforcement of the scattered placement policy


``v5.0.29 Diopside``
=======================

2022-07-29

Bug
----

* Fix /usr/bin/python dependency on EL8(S) which is no longer provided by any package,
  therefore we need to explicitly use /usr/bin/python3


``v5.0.28 Diopside``
=======================

2022-07-26

Note
----

* This version of EOS is based on an internal release of XRootD namely eos-xrootd-5.4.7

Bug
---

* [EOS-5336] - Lot of EOS FST crash (SIGSEGV) in the EOSALICE instance
* [EOS-5308] - MGM: Potential double free in LDAP initialize
* [EOS-5334] - LDAP connection socket leak
* [EOS-5335] - MGM crash in Fileinfo.cc:97


``v5.0.27 Diopside``
=======================

2022-06-30


Bug
---

* [EOS-5296] - FST segfault around XrdXrootdProtocol::Process2
* [EOS-5314] - segfault around "XrdCl::CopyProcess::CleanUpJobs"
* [EOS-5302] - Iostat domain accounting is broken
* [EOS-5303] - Shared filesystem file registration feature
* [EOS-5308] - MGM: Potential double free in LDAP initialize

Improvement
------------

* [EOS-5317] - Crash in AssignLBHandler with asan
* [EOS-5321] - Allow to define which errors the fsck repair thread works on
* [EOS-5305] - Tape REST API - V1 with an option to deactivate STAGE


``v5.0.26 Diopside``
=======================

2022-06-21


Note
----

* XRootD: Based on eos-xrootd-5.4.5 which fixes a couple for important bugs
  on the xrootd client side.

Bug
----

* [EOS-5302] - Iostat domain accounting is broken
* [EOS-5303] - Shared filesystem file registration feature

Improvements
------------

* MGM: Make fsck start up and shutdown more responsive
* MGM: Add fsck repair procedure for m_mem_sz_diff errors


``v5.0.25 Diopside``
=======================

2022-06-09

Bug
----

* [EOS-5278] - Segmentation fault around eos::mgm::GroupDrainer::scheduleTransfer
* [EOS-5284] - GroupBalancer: spurious logs when no transfers can be scheduled
* [EOS-5286] - Physical quota is not updated when we set EC conversion
* [EOS-5288] - Wrong layout id after conversion operation leading to wrong physical size
* [EOS-5218] - Infinite loop in XrdCl::XRootDMsgHandler::Copy
* MGM: The initial behaviour of xrdfs prepare -s/-a/-e and xrdfs query prepare have been restored

Improvement
------------

* [EOS-5277] - Add LockMonitor class wrapping standard mutex
* [EOS-5282] - Allow converter configuration to persist on restarts
* [EOS-5285] - GroupDrainer: Allow all transfers to be reset
* [EOS-5289] - File truncate can be slow especially for RAIN layouts
* [EOS-5290] - File close operation for RAIN layouts can trigger client timeouts
* MGM: Tape REST API v0.1 release - Support for ArchiveInfo and Release
  functionality + discovery endpoint
* MISC: Allow the eos-iam-mapfile tool to deal with DNs containing commas


``v5.0.24 Diopside``
=======================

2022-05-27

Bug
---

* [EOS-3713] - sys.eos.mdino should not use old-style inodes
* [EOS-5230] - Keep xattrs when restoring versions
* [EOS-5269] - Certain FSes not picked up by the group drainer

Improvement
-----------

* [EOS-5263] - groupmod is hard limited to 256 groups
* [EOS-5267] - Provide timestamp in eos convert list failed errors


``v5.0.23 Diopside``
=======================

2022-05-16

Note
----

* This release uses eos-xrootd-5.4.4 which is based on XRootD-5.4.3-rc3.

Bug
----

* [EOS-5246] - replica show 'error_label=none' while having checksum mismatch.

Improvement
------------

* [EOS-5184] - Add RedirectCollapse to XrdMgmOfs::Redirect responses
* [EOS-5198] - Add few log lines to MasterLog


``v5.0.22 Diopside``
=======================

2022-05-06

Improvements
------------

FUSEX: Refactoring async response handling


``v5.0.21 Diopside``
=======================

2022-05-06

Notes
------

* Note: this is a scratch build on top of XRootD-5.4.3-RC1 trying to test a bug fix concerning vector reads
* Update dependency to XRootD-5.4.3-RC1



``v5.0.20 Diopside``
=======================

2022-05-03

Improvements
------------

MGM: Improve fsck handling for rain files with rep_diff_n errors
MGM: Add extra logging in fsck and be more defensive when handling
unregistered stripes
MGM: Group drainer prune transfers only once every few minutes
FST: Silence stat errors for TPC transfers during preparation stages


``v5.0.19 Diopside``
=======================

2022-05-02

Bug
---

* MGM: Fix race condition in Converter which can lead to wrong metadata stored
  in leveldb for converted files.
* MGM: Fix wrong computation of number of stripes for RAIN layout
* [EOS-5199] - Metadata (xattrs) is lost when creating new versions
* [EOS-5219] - eos fsck report json output does not reflect command line options -l and -i
* [EOS-5224] - No update is perfomed when adding a new member to an e-group in EOSATLAS


New Feature
-----------

* [EOS-5178] - Implement Group Drain
* [EOS-5225] - Have a useful GroupDrain Status


``v5.0.18 Diopside``
=======================

2022-04-22

Bug
----

* [EOS-5197] - Deleting an xattr via console does not delete the key
* [EOS-5199] - Metadata (xattrs) is lost when creating new versions
* MGM: Fix crash in debug message when Env object is null for Access method

New Feature
------------

* [EOS-5215] - Fsck handle stripe size inconsistencies for RAIN layouts


Improvement
------------

* [EOS-4955] - Add project quota tests as a part of CI
* MGM: Iostat performance improvements for summary output
* MGM: Iostat make extra tables optional by default and add separate
  flag for displaying them.


``v5.0.17 Diopside``
=======================

2022-04-13

Note
----

* This version includes add the fixes up to 4.8.82.

Improvement
------------

* [EOS-5201] - Allow for more fine grained IO policies
* [EOS-5204] - Only create files  via FUSEX if there is inode and volume quota and physical space available
* [EOS-5205] - Distinguish writable space and total space
* [EOS-5206] - Don't allow to set quota volume lower than the minimum fuse quota booking size


``v5.0.16 Diopside``
=======================

2022-03-29

Bug
----

* [EOS-5181] - Slave to Master redirection creates IO errors on FUSEx mounts
* [EOS-5176] - Make OAuth tolerant to self-signed//invalid certificates used by identity provider

Improvement
-----------

* MGM: Add protection against multi-source retry for RAIN layouts
* MGM: Rewrite of the IoStat implementation for better accuracy
* MGM: Remove dependency on eos-scitokens and use the library provided by XRootD framework
* DOC: Update documentation concerning the MGM configuration for SciTokens support
* NS: QuarkSyncTimeAccounting - removed namespace lock usage

New feature
-----------

* MGM: Add support for eos tokens over https


``v5.0.15 Diopside``
=======================

2022-03-22

Note
-----

* Includes all the changes from 4.8.79

Bug
----

* FUSEX: never keep the deletion mutex when distroying an upload proxy because
  the destructor still needs a free call back thread to use HandleResponse
* [EOS-5153] - EC file written via FUSEx - mismatching checksum
* [EOS-5167] - MGM segv in a non-tape enabled instance



``v5.0.14 Diopside``
=======================

2022-03-14

Bug
----

* [EOS-5090] - convert clear is not a admin command
* [EOS-5133] - node ls -b does not remove the domain names
* FUSEX: Fix deadlocks and race-conditions reported by TSAN

Improvement
------------

* [EOS-5108] - workaround: drop forced automount expiry on FUSEX updates
* [EOS-5126] - [eos-ns-inspect] Complement `stripediff` ouput


``v5.0.13 Diopside``
=======================

2022-02-15

Note
----

* Includes all the changes from 4.8.76

Bug
---

* [EOS-5110] - Consolidate Access control in GRPC MD, MDSTreaming
* [EOS-5116] - Workaround for XrdOucBuffPool bug
* [EOS-5118] - eos-ns-inspect scan is initializing maxdepth to 0, even if not used
* [EOS-5119] - Deadlock scenario in eosxd

Improvement
-----------

* [EOS-5111] - Groupbalancer: newly introduced fields may not have a sane value
* [EOS-5120] - io stat tag totals


``v5.0.12 Diopside``
=======================

2022-02-04

Note
----

* Identical to 5.0.11 but re-tagged due to Koji issues


``v5.0.11 Diopside``
=======================

2022-02-04

Bug
----

* [EOS-5105] - eosxd crash in cap::quotax::dump


``v5.0.10 Diopside``
=======================

2022-02-02

Note
-----

* This release includes all the changes from 4.8.74 release

Bug
----

* [EOS-5069] - filesystem status in "rw + failed"
* [EOS-5070] - Access::ThreadLimit creates re-entrant lock of the access mutex
* [EOS-5095] - Re-entrant lock triggered by out of quota warning

Improvement
------------

* [EOS-5065] - Add create-if-not-exists option in GRPC
* [EOS-5076] - Extend iotype interfaces to be space/directory defined
* MGM: Fix missing support for cid/cxid and error output for convert command
* WNC: Replaced auxiliary ACL function for fileinfo command

New features
------------

* WNC: Implemented support for EOS-wnc token, convert, fsck and new find commands
* WNC: Changed GRPC streaming mechanism for find, ls and transfer commands


``v5.0.9 Diopside``
=======================

2022-01-12

Bug
----

* COMMON: Avoid segv due to mutex object set to nullptr in RWLock printout
* [EOS-4850] - eosxd crash in destructor under metad::pmap::retrieveWithParentTS()
* [EOS-5057] - Volume quota dispatched to FUSE clients mixes logical and physical bytes


``v5.0.8 Diopside``
=======================

2022-01-06

Note
----

* Note: This release includes all the changes to the 4.8.70 release

Bug
----

* [EOS-5039] - Threads with parens in their name cannot access EOS

Improvement
-----------

* [EOS-5029] - Allow to apply rate limiting in recursive (server side) command.
* [EOS-5048] - Support direct IO for high performance read/write use cases


``v5.0.7 Diopside``
=======================

2021-12-01

Note
----

* Release based on XRootD-5.3.4


New features
------------

* WNC: Implemeneted support for EOS-wnc member, backup, map and archive command



``v5.0.6 Diopside``
=======================

2021-11-16

Note
-----

* Release based on XRootD-5.3.3 which fixes a critical bug concerning "invalid responses"


Bug
----

* ARCHIVE: Avoid trying to set extended attributes which are empty
* [EOS-4995] MGM/CONSOLE: add '-c' option to CLI ls to show also the checksum for a listing
* CTA: Fixed FST crash when connecting to misconfigured ctafrontend endpoint


``v5.0.5 Diopside``
=======================

2021-11-04

Bug
----

OSS: Avoid leaking file descriptors for xsmap files which are deleted in the meantime
MGM: Skip applying fsck config changes at the slave as these will be properly


``v5.0.4 Diopside``
=======================

2021-10-27


Bug
----

* SPEC: Make sure both libproto* and libXrd* requirements are excluded when
  building the eos packages since these come from internally build rpms like
  eos-xrootd and eos-protobuf3 which don't expose the library so names so that
  they can be installed on a machine along with the official rpms for the
  corresponding packages if they exist.
* MGM: Avoid that a slave MGM applies an fsck configuration change in a loop

Improvements
------------

* EOS-4967: Add ARM64 support for blake3


``v5.0.3 Diopside``
=======================

2021-10-27


Note
----

* This version is based on XRootD 5.3.2 that addresses some critical bug observed
  in the previous version for XRootD.

Bug
----

* MGM: Fix GRPC IPv6 parsing
* [EOS-4963] - FST: Reply with 206(PARTIAL_CONTENT) for partial content responses
* [EOS-4962] - MGM: Return FORBIDDEN if there is a public access restriction in PROFIND requests
* [EOS-4950] - FUSEX: fix race conditions in async callbacks with respect to proxy object deletions
*

New features
------------

* [EOS-4670] - FUSEX: implement file obfuscation and encryption


``v5.0.2 Diopside``
=======================

2021-09-06

Bug
----

* [EOS-4809] - Make eos5 work with XrdMacaroons from XRootD5
* Includes all the fixes from 4.8.65

Improvements
------------

* WNC: Improvements to the EOS-Drive for fileinfo & health command


``v5.0.1 Diopside``
=======================

2021-08-16

New features
-------------

* Comtrade WNC contribution for the server side
* Includes all the fixes from the 4.8.60 release


``v5.0.0 Diopside``
=======================

2021-06-11

Major changes
--------------

* Based on XRootD 5.2.0
* Drop support for in-memory namespace
* Drop support for file based configuration
* Drop support for old high-availability setup
* Make fusex classes compatible with the latest protobuf library
* Integrate QuarkDB as part of the eos release process
