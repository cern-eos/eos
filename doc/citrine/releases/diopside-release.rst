:orphan:

.. highlight:: rst

.. index::
   single: Diopside-Release


Diopside Release Notes
=====================

``Version 5 Diopside``

Introduction
------------

This release is based on XRootD V5.


``v5.1.16 Diopside``
====================

2023-04-04

Bug
----

* COMMON: Don't reset the current vid identity when handling KEYS mapping
  unless we actually have a hit in the map. This was breaking the vid mapping
  for gsi/http with voms extensions that have the endorsements field in the
  XrdSecEntity populated and this was interpreted as a key.


``v5.1.15 Diopside``
====================

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

* [EOS-5522] - Drain status stays in `expired`after setting fs in rw.
* [EOS-5530] - Send fid as string to CTA

Improvement
-----------

* [EOS-5578] - Balancer/Drainer/Recycler: reduce sleep info logging
* [EOS-5592] - Disabling oauth did not actually disabled it


``v5.1.14 Diopside``
====================

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
====================

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
====================

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
====================

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
====================

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
====================

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
====================

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
====================

2022-12-12

Bug
----

* [EOS-5474] - Conversion breaks files with FMD info in xattrs

Improvement
------------

* [EOS-5469] - Allow to select secondary groups with kerberos authentication and implement AC checks for secondary groups
* [EOS-5471] - Add atime to EOS
* [EOS-5458] - Setting a namespace xattr might fail for wopi


``v5.1.6 Diopside``
====================

2022-12-05

Bug
----

* [EOS-5467] - Inspector aggregates results instead of resetting the current scan

Improvement
------------

* [EOS-5465] - Shoe FUSE application name in 'fusex ls'
* [EOS-5466] - Add Stall / NoStall host lists to access interface


``v5.1.5 Diopside``
====================

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
====================

2022-11-22

Bug
----

* [EOS-5442] - eosxd crash (on shutdown) under ShardedCache destructor
* [EOS-5446] - Failures in setting thread names


``v5.1.3 Diopside``
====================

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
====================

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
====================

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
====================

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
===================

2022-08-12

Bug
----

* FST: Properly detect HTTP transfers and skip async close functionality in
  such cases
* [EOS-5359] - use after free in fusex::client::info
* [EOS-5358] - WNC GRPC unserialized global options


``v5.0.30 Diopside``
===================

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
===================

2022-07-29

Bug
----

* Fix /usr/bin/python dependency on EL8(S) which is no longer provided by any package,
  therefore we need to explicitly use /usr/bin/python3


``v5.0.28 Diopside``
===================

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
===================

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
===================

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
===================

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
===================

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
===================

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
===================

2022-05-06

Improvements
------------

FUSEX: Refactoring async response handling


``v5.0.21 Diopside``
===================

2022-05-06

Notes
------

* Note: this is a scratch build on top of XRootD-5.4.3-RC1 trying to test
a bug fix concerning vector reads
* Update dependency to XRootD-5.4.3-RC1


``v5.0.20 Diopside``
===================

2022-05-03

Improvements
------------

MGM: Improve fsck handling for rain files with rep_diff_n errors
MGM: Add extra logging in fsck and be more defensive when handling
unregistered stripes
MGM: Group drainer prune transfers only once every few minutes
FST: Silence stat errors for TPC transfers during preparation stages


``v5.0.19 Diopside``
===================

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
===================

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
===================

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
===================

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
===================

2022-03-22

Note
-----

* Includes all the changes from 4.8.79

Bug
----

* FUSEX: never keep the deletion mutex when destroying an upload proxy because
  the destructor still needs a free call back thread to use HandleResponse
* [EOS-5153] - EC file written via FUSEx - mismatching checksum
* [EOS-5167] - MGM segv in a non-tape enabled instance



``v5.0.14 Diopside``
===================

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
===================

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
===================

2022-02-04

Note
----

* Identical to 5.0.11 but re-tagged due to Koji issues


``v5.0.11 Diopside``
===================

2022-02-04

Bug
----

* [EOS-5105] - eosxd crash in cap::quotax::dump


``v5.0.10 Diopside``
===================

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
===================

2022-01-12

Bug
----

* COMMON: Avoid segv due to mutex object set to nullptr in RWLock printout
* [EOS-4850] - eosxd crash in destructor under metad::pmap::retrieveWithParentTS()
* [EOS-5057] - Volume quota dispatched to FUSE clients mixes logical and physical bytes


``v5.0.8 Diopside``
===================

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
===================

2021-12-01

Note
----

* Release based on XRootD-5.3.4


New features
------------

* WNC: Implemeneted support for EOS-wnc member, backup, map and archive command



``v5.0.6 Diopside``
===================

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
===================

2021-11-04

Bug
----

OSS: Avoid leaking file descriptors for xsmap files which are deleted in the meantime
MGM: Skip applying fsck config changes at the slave as these will be properly


``v5.0.4 Diopside``
===================

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
===================

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
===================

2021-09-06

Bug
----

* [EOS-4809] - Make eos5 work with XrdMacaroons from XRootD5
* Includes all the fixes from 4.8.65

Improvements
------------

* WNC: Improvements to the EOS-Drive for fileinfo & health command


``v5.0.1 Diopside``
===================

2021-08-16

New features
-------------

* Comtrade WNC contribution for the server side
* Includes all the fixes from the 4.8.60 release


``v5.0.0 Diopside``
===================

2021-06-11

Major changes
--------------

* Based on XRootD 5.2.0
* Drop support for in-memory namespace
* Drop support for file based configuration
* Drop support for old high-availability setup
* Make fusex classes compatible with the latest protobuf library
* Integrate QuarkDB as part of the eos release process
