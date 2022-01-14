:orphan:

.. highlight:: rst

.. index::
   single: Citrine-Release


Citrine Release Notes
=====================

``Version 4 Citrine``

Introduction
------------
This release is based on XRootD V4 and IPV6 enabled.

``v4.8.71 Citrine``
===================

2022-01-14

Bug
----

* COMMON: Avoid segv due to mutex object set to nullptr in RWLock printout    
* [EOS-4850] - eosxd crash in destructor under metad::pmap::retrieveWithParentTS()
* [EOS-5057] - Volume quota dispatched to FUSE clients mixes logical and physical bytes


``v4.8.70 Citrine``
===================

2022-01-06

Bug
----

* [EOS-5033] - missing drainperiod from `eos -j fs ls`
* [EOS-5034] - eos-server missing dependency on perl(Time::HiRes)
* [EOS-5052] - Repeated open/close sequence leads to failed file state
* [EOS-5039] - Threads with parens in their name cannot access EOS

Improvement
-----------

* [EOS-5027] - Handle eviction for multiple staging requests on the same file
* [EOS-5029] - Allow to apply rate limiting in recursive (server side) command
* [EOS-5048] - Support direct IO for high performance read/write use cases


``v4.8.69 Citrine``
===================

2021-11-24

Improvements
------------

* FST: allow to disable any iopriority settings in FSTs using env EOS_FST_NO_IOPRIORITY


``v4.8.68 Citrine``
===================

2021-11-23

Bug
----

* [EOS-5015] - FSTs running versions older than 4.8.67 cannot connect to MQ
  running version 4.8.67

Improvement
-----------

* [EOS-5004] - Support sys.acl for file ACLs for RA protocols
* [EOS-5013] - Make oAuth userinfo configurable
* [EOS-5018] - Allow to set extended attributes on version folders


``v4.8.67 Citrine``
===================

2021-11-17

Bug
----

* [EOS-4934] - ASAN: fusex: enoent use after free
* [EOS-4941] - FSCK toogle-repair multiple time crashes MGM
* [EOS-4952] - Unify the various string split interfaces
* [EOS-4963] - FST returns 200 status code for Partial Content request instead of 206
* [EOS-4976] - Fix activity field passed from EOS to CTA
* [EOS-4986] - eos CLI aborts with "basic_string::_S_construct null not valid"
* [EOS-4992] - FST crashes upon SSI exception

Improvement
------------

* [EOS-4945] - Use timestamp for saving the stack trace
* [EOS-4995] - Add flag to 'ls' to add checksum printout in long listing
* [EOS-5002] - Add a '-c' option to set an extended attribute only if it does not exist altready


``v4.8.66 Citrine``
===================

2021-10-05

Bug
----

* [EOS-4936] - GETLK returns EAGAIN instead of lock information
* [EOS-4937] - Fix reporting for written bytes for RAIN layouts
* [EOS-4938] - Store report info only in the current MGM master

Improvement
-----------

* [EOS-4930] - Add support for async writes for replica layout


``v4.8.65 Citrine``
===================

2021-09-29

Bug
---

* MGM: Fix quota accounting for the sum of all groups


``v4.8.64 Citrine``
===================

2021-09-27

Bug
----

* [EOS-4779] - Dead lock in parity computation for RAIN
* [EOS-4896] - queuing for archive should use MgmOfsAlias instead of mgm.manager
* [EOS-4912] - fst - read lock held for 10s seconds
* [EOS-4922] - SEGV on config after shutdown was initiated
* [EOS-4924] - FST service restarts after calls to std::future, eos::fst::Storage::Publish
* [EOS-4925] - Typo in mgm/proc/user/Archive.cc
* [EOS-4926] - discrepancy of accounting report and quota

New Feature
-----------

* [EOS-4903] - Add new configuration to setup redirection with Master/Slave QuarkDB Configuration


Improvement
-----------

* [EOS-4889] - Make EOS-CTA tape garbage collector compatible with MGM master/slave configuration


``v4.8.63 Citrine``
===================

2021-09-10

Bug
===

* [EOS-4904] MGM: block proxy headers in XrdHttp by default (add env file + fix typo)
* [EOS-4905] MGM: pass CGI 'query' to the access function used in XrdHttpMgm to allow token access
* [EOS-4901] MGM: check for invalid paths before scoping them
* MGM/CONSOLE: Fix acl command to accept the "a" archive flag
* FST: Make sure to skip checksum if asked to ignore it
* MGM: Reduce load on the configuration backups when moving a files systems between groups/spaces

Improvements
============
* CI: Add ApMon build/publish job for Centos Stream 8
* DOC: various documentation improvements

``v4.8.62 Citrine``
===================

2021-08-25

Bug
----

* [EOS-4327] - FST still misses the required capability key - symkey empty
* [EOS-4852] - Race condition when accounting running console commands
* [EOS-4878] - Balancing RAIN files stores wrong size in local DB

Improvement
------------

* [EOS-4858] - Add fsck check for RAIN layout to spot disk size corruptions
* [EOS-4863] - make eos-hashbench run a single benchmark at a time
* [EOS-4875] - mgm: Mapping: avoid double lookups on maps


``v4.8.61 Citrine``
===================

2021-08-21

Bug
----

* Revert "COMMON: drop 'sudo' role after sudo mapping - fixes EOS-4781"


``v4.8.60 Citrine``
===================

2021-08-11

Bug
----

* [EOS-4480] - HA issue: GridFTP transfers with checksum testing are failing when
  the DNS alias is not pointing towards the active MGM node
* [EOS-4633] - 'eos' manpage is empty, rest is missing
* [EOS-4683] - MGM LRU crash
* [EOS-4690] - HA: transition to master crashes the future master
* [EOS-4696] - eos config dump <name> does not work for backup configs
* [EOS-4803] - FST node status not remaining offline when service is stopped
* [EOS-4814] - Restore of a version does not work
* [EOS-4818] - EOSAMS02 crash in DrainTransferJob
* [EOS-4835] - Strange remdir unformatted lines...
* [EOS-4843] - Wrong quota after a ns update_quotanode command
* [EOS-4847] - group translation failing in EOSHOME for def-cg
* [EOS-4779] FST: reduce file-local dead lock condition after parity computation error
* [EOS-4835] MGM: fix ever growing '/' in remdir

Improvement
------------

* [EOS-4411] - disk health check for Linux DM multipath devices
* [EOS-4586] - RFE" remove "pre-configuring default route" warning for fully-qualified instance+path
* [EOS-4749] - Remove the extra-output display in eos rm command
* [EOS-4783] - Size differs only in MGM [WIP in fsck dev]
* [EOS-4784] - [rep_diff_n] and [rep_missing_n]; Overreplicated file, faulty replica was commited to MGM
* [EOS-4838] - Check health status refinement
* [EOS-4839] - Improve balancer shutdown to clean what it was balancing from the tracker queue
* [EOS-4682] - MGM crash in LRU.hh:252 eos::MetadataProviderShard::retrieveFileMD
* [EOS-4827] MGM: implement GRAB version functionality in GRPC
* [EOS-4759] MGM: allow set space specific scheduling and iopriority parameter defaults


``v4.8.59 Citrine``
===================

2021-07-22

Bug
---

* [EOS-4824] MGM: avoid SEGV when loading quota nodes with certain configurations

Improvements
------------

* [EOS-4823] MGM: eosxd creations support now linked attributes describing file layouts etc.
* [EOS-4825] COMMON: allow static mappping to local accounts from 'sub' using 'vid set map -oauth2 sub:xyz vuid:localuid'


``v4.8.58 Citrine``
===================

2021-07-19

Bug
---

* [EOS-4775] NS: fixing SearchNode expansion decision taking mechanism
* [EOS-4779] FST: fix dead lock in parity computation for RAIN
* [EOS-4806] MGM: protect newfind command against crashes on malformed/buggy input for regex match --name filters
* MGM: directory listing (XrdMgmOfsDirectory) always checks now ACLs for denials e.g. an ACL denial can superseed a POSIX allow

Improvements
------------

* [EOS-4819]  MGM: adding server side bandwidth limitation, which can be defined either as a space policy (policy.bandwidth) or by application per space. The key for an empty application is 'space.bw.default' and the limits are given in MB/s
* [EOS-4781] COMMON: drop 'sudo' role after sudo mapping
* [EOS-4746] MGM/CONSOLE/GRPC: support ACL positions
* DOC: improvements of fsck,permission and policy documentation
* [EOS-4805] MGM: implement negative ACLs for read/write/delete operations


``v4.8.57 Citrine``
===================

2021-06-30

Bug
---

* MGM: silence 'no token' error message in Acl class
* MGM: silence error message in CommitHelper for atomic versioning, if no file has been versioned

``v4.8.56 Citrine``
===================

2021-06-27

Bug
---

* [EOS-4764] COMMON:  fix overlap function used in token macro for CLI commands creating a SEGV when doing certain 'file mv' operations

Improvements
------------

* [EOS-4766] MGM: Don't block HTTP access with EOS tokens in the HTTP bridge code - this allows to mix SciTokens and EosTokens inside the same instance

New Feature
------------

* [EOS-4762] MGM: add new filesystem active status online - overload - offline
* [EOS-4760] FST: implement round-robin scheduling
* [EOS-4759] FST: add 'eos.iopriority' to stear BFQ/CFQ priorities


``v4.8.55 Citrine``
===================

2021-06-22

BUG
----

* MGM: silence fprintf statements in InFlightTracker
* [EOS-4756] MGM: keep recusrive deletions exactly as configured by the recycle bin time policy

New Feature
------------

* FUSEX: allow to define 'sparse ratio' to disable read-ahead for good if a file has been seen to be sparse read - normally read-ahead can get re-enabled
* CI: allow ASAN builds to be manually triggered


``v4.8.54 Citrine``
===================

2021-06-18

Bug
----

* [EOS-4755] MGM: fix concurrency issues leading to SEGV in FuseServer/Caps (Imply)


``v4.8.53 Citrine``
===================

2021-06-18

Improvement
------------

* MGM: support tokens for EOS CLI commands and basic xrdfs functions like mkdir/rmdir/rm
* MGM: introduce thread pool limits by user and global using 'eos access' and show usage in 'eos ns [stat]'
* MGM: improve performance of eosxd broadcasts and use a standard mutex to protect the caps objects


``v4.8.51 Citrine``
===================

2021-06-10

Bug
----

* [EOS-4740] MGM: Make sure only the master MGM propagates changes to the configuration engine.
* SPEC: Fix ownership of archive directories
* CONSOLE: Prevent to print out twice an error in selected proto commands


``v4.8.50 Citrine``
===================

2021-06-07

Bug
----

* [EOS-4725] - Unknown lock held for many seconds
* [EOS-4730] - Fix FST crash during shutdown
* [EOS-4736] - Memory leak when parsing diskstat on CentOS8
* [EOS-4737] - File systems blocked in booting during mass boot with --syncmgm
* [EOS-4740] - Inconsistent FsView maps after removing/changing file system

Improvement
------------

* [EOS-4724] - Support HTTP chunked uploads
* [EOS-4727] - Add fsck subcommand to cleanup orphans
* [EOS-4728] - Improve the refresh of fsck stats
* [EOS-4729] - Improve remove detached for entries with deleted parents
* [EOS-4735] - Make Egroup queries for non existing users / groups cacheable


``v4.8.49 Citrine``
===================

2021-05-24

Bug
----

* FUSEX: properly support also KERYRING:persisten:%{UID} as default krb5 CCCAHCE


Improvement
-----------

* [EOS-4709] - [eos-ns-inspect] adding --maxdepth to scanning functionality
* MGM/CONSOLE: allow to scan quota in a subtree for a given uid or gid using
  e.g. 'eos update_quotanode /eos/tree uid:123'
* MGM: enhance eosnobody squashfs check to distinguish three instead of two cases:
  result eosnobody can only stat via eosxd and access squashfs image files, nothing else


``v4.8.48 Citrine``
===================

2021-05-18

Bug
----

* [EOS-4715] - Segv in jemalloc during PathRouting
* MGM: add by-pass for squashfs sss 'eosnobody' file access without ACL entries
* FUSEX: allow to open a squashfs image file client side even if we don't have R mode on the parent directory


``v4.8.47 Citrine``
===================

2021-05-17

Bug
---

* [EOS-4716] - quota zeroes the counters of used bytes/files from the quota node


New Feature
------------

* [EOS-4712] - Support LOCK_MAND in eosxd


``v4.8.46 Citrine``
===================

2021-05-07

Bug
----

* FST: Don't free internal jerasure structs, these will be cleaned up when the FST is shutdown


``v4.8.45 Citrine``
===================

2021-05-06

Bug
----

* [EOS-4695] - Select default KRB5 token location
* [EOS-4697] - LRU uses wrong prefetch type
* [EOS-4699] - Screen both mappings (uid,gid) in vid set before setting any config value
* [EOS-4700] - Space policies interfere with conversion jobs
* [EOS-4702] - Don't redirect to FSTs if not enough locations are available in EC layouts
* [EOS-4704] - Memory leak when using the jerasure library

New Feature
------------

* [EOS-4705] - Block multi-source reading for EC files

Task
-----

* [EOS-4684] - Make the "file archived" GC aware of different EOS spaces

Improvement
-----------

* [EOS-4691] - Improve the locking primitives in FuseServer caps


``v4.8.44 Citrine``
===================

2021-04-30

Bug
---

* FST: fix bug introduced with a checksum reset in case of non-sequential writing


``v4.8.43 Citrine``
===================

2021-04-21

Bug
---

* [EOS-4669] - eos file verify need to be triggered twice in order to work
* [EOS-4674] - Empty FSCK report seemingly after FST slow upgrade
* [EOS-4676] - Crash when checking for recursive deletion
* [EOS-4677] - FST deadlock when updating the scanner config
* [EOS-4678] - MGM crash when removing a file system
* Fix interference between master-slave setup and various internal services
  like LRU, drainer and converter that should only run in a master MGM.

Improvements
------------

* Add fileTruncateAsync API to the file IO interface


``v4.8.42 Citrine``
===================

2021-04-14


Bug
----

* [EOS-4545] Option for eosxd mounts to block symlinks walking up the hierarchie

Improvements
------------

* Drop the use of folly concurrent map and use internal implementation
* Add job for CentOS8 Stream packages


``v4.8.41 Citrine``
===================

2021-04-14


Bug
----

* [EOS-4607] - The commad eos node config does not accept 'off' when using configstatus
* [EOS-4627] - FSCK collected time changed after restart
* [EOS-4629] - Checksum not recomputed after certain truncation operations
* [EOS-4657] - File in draining with both FST checksums to 0x00
* [EOS-4659] - Debug command broken
* [EOS-4653] - Krb5 memory leak in CredentialValidator
* [EOS-4660] - Potential cross-site scripting vulnerability in the EOS-HTTP
* [EOS-4639] - Fix possible memory leak when using dense_hash_set objects
* [EOS-4635] - Failure to share with egroups containing underscore
* FST: Avoid early return in case of HTTP partial content like for example for range requests

New Feature
-----------

* [EOS-4623] - Create an utils script to setup a development environment on CentOS7/8
* [EOS-4062] - Centos8: support "KCM" Kerberos cache
* [EOS-4609] - Support for excess replicas/stripes

Improvements
------------

* [EOS-4575] - Error on eos find command when tmp file cannot be created
* [EOS-4617] - Quota option to provide only the quota of the specified quota node
* [EOS-4658] - EOS workflow engine should not insist on the W_OK mode bit
* Fsck improvements when dealing with detached files in general and also hadling
  wired cases where a file is detached but its parent id is not properly marked as 0


``v4.8.40 Citrine``
===================

2021-02-03

Bug
----

* [EOS-4506] - Slowness when changing fs configurations when using eos space
* [EOS-4540] - FST flips status from online to offline and back when cfg.status=off
* [EOS-4582] - investigate far-in-the-future mtime, robustify "eos fileinfo"
* MGM: Fix drain for RAIN 0-size files

Improvements
-------------

* MGM/HTTP: Allow running XrdHttp without the need for token authentication
* ALL: Improve logging functionality to avoid the long tail of performance

Note
----

* Upgrade to XRootD-4.12.8


``v4.8.39 Citrine``
===================

2021-02-08

Bug
----

* [EOS-4539] - FST crash on shutdown in eos::common::DbMapT::iterate()
* [EOS-4574] - Crash in HandleVOMS when role is not present

Improvement
------------

* Improve buffering and memory operations for RAIN layouts
* [EOS-4525] - Include in acl man page the difference between sys.acl and user.acl
* [EOS-4534] - Check compatibility of libXrdVoms.so with the HTTP interface
* [EOS-4541] - Add a log message when a `ns recompute_quotanode` finishes

Note
----

* Update to XRootD-4.12.7


``v4.8.38 Citrine``
===================

2021-02-02

Bug
----

* [EOS-4573] - ZMQ threads jump into eternal parsing error state
* COMMON: Compensate for the missing protocol info for HTTP transfers also in the SecEntity::ToKey method
* SPEC: Make sure the debug info is not stripped from the binaries
* MGM: Avoid to refresh directory MD all the time after a deletion

Improvements
------------

* FST: Allow XRootD env variables to override default XrdCl timeouts in EOS
* Deal with a list of VOMS roles/groups


``v4.8.37 Citrine``
===================

2021-01-19

Improvements
-------------

ALL: Improve the logging info evaluation which is now done only if the log line
  is to be actually printed.
MGM: Add hex dump of ZMQ messages received from the FUSEX clients


``v4.8.36 Citrine``
===================

2021-01-18

Bug
---

* NS: Make sure the dense_hash_maps used for storing file ids for the file systems
  don't grow forever and call resize(0) to reclaim memory when elements are deleted.
* MGM: inherit file ACLs when overwriting existing files and add instance test cases


``v4.8.35 Citrine``
===================

2021-01-07

Bug
----

* FST: Fix logic when enabling/disabling async close
* FST: Properly align the writes for PUT requests
* CONSOLE: Fix memory corruption issues with eos cp
* MGM: fix webdav free quota bytes computation

New Feature
------------

* [EOS-4545] - Option for eosxd mounts to block symlinks walking up the hierarchie


``v4.8.34 Citrine``
===================

2020-12-17

Note
----

* Fix spurius errno triggering an exception in proc/mgm/Fusex


``v4.8.33 Citrine``
===================

2020-12-14

Note
----

* This version is built aginst XRootD-4.12.6 which contains some important fixes for
  HTTP TPC transfers.


``v4.8.32 Citrine``
===================

2020-12-11


Bug
----

* [EOS-4499] - EOSHOME-i04 crash in eos::fusex::cap::clientuuid ()
* [EOS-4504] - Persistent ESTAB connections on the FUSEX port from 'bogus' clients
* [EOS-4536] - SIGSEGV around eos::mgm::FuseServer::Caps::Store


``v4.8.31 Citrine``
===================

2020-12-07

Bug
---

* MGM: Reduce scope of eos::mgm::FuseServer::Client write lock to avoid deadlock
* MGM: Skip quota updates on the slaves as this might corrupt the ns
* EOS-4520 MGM: fix treesize changes when moving directory trees via FUSE

Improvements
------------

* MGM: Add namespace stats entry for newfind


``v4.8.30 Citrine``
===================

2020-12-03

Bug
----

* [EOS-4498] - MGM slowness in eoshome-i02
* [EOS-4500] - EOSHOME-i01 (Apparently - Deadlock)
* [EOS-4519] - Namespace deadlock (EOSPUBLIC)
* [EOS-4524] - EOSCMS unresponsive
* MGM: Prevent the prefetcher from bypassing the limits on the number of results
 returned when using by the find functionality
* MGM: enforce eos access interface being only for admins


``v4.8.29 Citrine``
===================

2020-12-01

Bug
----

* [EOS-4505] - Cannot xrdfs prepare -s in EOS with no write access`
* [EOS-4515] - HTTP PUT stores corrupted file
* [EOS-4521] - MQ: Crash in the XrdMqOfs::stat method

Improvements
-------------

* MGM: Improve FuseXCast notifications sent during the rename operation
* MGM/FUSE: Make the mutex for Caps and Client objects blocking
* MGM: TGC now uses tgc.freebytesscript if set and not empty


``v4.8.28 Citrine``
===================

2020-11-13

Improvements
------------

* MGM: Modified RealTapeGcMgm::getSpaceStats() to give the exact same result as `eos space ls spinner -m`
* FUSEX: decouple stat mutex from disk activiy - reduce mutex scopres in .stats file thread producing statistics output
* MQ: Do broadcast all stat.* params as some are needed back on the FST side


``v4.8.27 Citrine``
===================

2020-11-12

Bug
----

* [EOS-4410] - intermittent mgm failover and offline FST
* [EOS-4482] - Converter always uses default.0 as scheduling group
* [EOS-4484] - Http in/out traffic accounting is broken
* [EOS-4487] - LRU add switch for the new converter
* [EOS-4488] - LRU requires the converter to update ctime of converted files
* [EOS-4492] - Fix ns locking used in the LRU
* [EOS-4494] - New converter uses only default.0 as scheduling group

Improvement
-----------

* [EOS-4486] - LRU refresh once the interval is changed
* [EOS-4489] - Add basic unit tests for the ConvertInfo class
* [EOS-4490] - Archive should evict files from disk cached after a successful recall


``v4.8.26 Citrine``
===================

2020-11-02

Bug
----

* MGM: Fix crash when accessing file system which is null when iterating over
  file systems in a group/space.

Improvement
-----------

* [EOS-4481] - Tape garbage collector should notice file conversion jobs and also open for read requests
* Enforce check for QuarkDB 0.4.2 minimum version


``v4.8.25 Citrine``
===================

2020-10-27


Bug
----

* MGM: Fix quota refresh initialization
* [EOS-4466] - eos newfind still bogus with "-f/-d" filters
* [EOS-4477] - 'eos ls' bypasses permission check when result is cached

New feature
-----------

* FST: Tool to create readv pattern and check the result of readv requests done
  against different endpoints. Used to check for RAIN readv correctness.


``v4.8.24 Citrine``
===================

2020-10-20

Note
----

* Release based on XRootD 4.12.5 which addresses a couple of issues in the XrdHttp component

Improvement
------------

* [EOS-4464] - Latency Investigations on EOSHOME with v 4.8.22
* [EOS-4468] - Allow open for read requests to trigger implicit prepare requests for offline files
* [EOS-4470] - EOSCTA prepare logic within the MGM should use mgmofs.alias if set
* Debug symbols are no longer stripped as this was leading to a crash in gdb and
  consequently the eos-debuginfo package is no longer created.


``v4.8.23 Citrine``
===================

2020-10-09

Bug
----

* [EOS-4405] - mgm crash on eos::mgm::Stat::PrintOutTotal ()
* [EOS-4449] - Deadlock triggered when changing eos fs configstatus in a new machine
* [EOS-4457] - FST: Crash when scanning list of unlinked files
* [EOS-4460] - MGM does not correctly reply to Xrd config query for TPC delegation
* [EOS-4461] - FST exception not caught in RequestRateLimit

Improvement
-----------

* FST: Remove transaction directory/functionality
* FST: Properly align XrdHttp and EosHtpp buffers during PUT requests

New Feature
-----------

* MGM: Add QClient RTT statistics displayed in the "eos ns" command


``v4.8.22 Citrine``
===================

2020-09-30

Bug
---

* SPEC: adding missing mount helper scripts (packaging issue)
* SPEC: Avoid richacl for CentOS 8 until RPMs are provided"
* MGM/FST: Stop the libmicrohttp daemon in the destuctor of the MGM/FST HttpServer
  derived classes otherwise the Handler method might still be called after the
  derived classes are destructed (but before MHD_stop_daemon is called in the
  common HttpServer) causing a SEGV due to "pure virtual method called" EOS-4438

Improvements
------------

*  MGM: Speed up the shutdown of the routing thread


``v4.8.21 Citrine``
===================

2020-09-29

Bug
---

* COMMON: Fix bug in thread pool implementation


Improvements
------------

* MGM/FUSEX: Add prefetching of namespace metadata where necessary
* MGM: Fsck - don't mark 0-size files without replicas as rep_missing_n
* MGM: Fsck - improve handling of m_mem_sz_diff errors
* MGM/FST: Move debug command out of MQ and use XRootD query command to modify the log level
* MGM: Move fsck command out of MQ and use XRootD query command to collect the fsck responses
* MGM/FST: Move resync command out of MQ and use XRootD query to send such requests
* MGM/FST: Move rtlog command out of MQ and use XRootD query to send such requests
* MGM/FST: Move deletion scheduling out of MQ and implement it using XRootD query commands
* MGM/FST: Move verify command out of MQ and use XRootD query command for such requests
* BUILD: new way to build SELINUX policies

New Feature
------------

* [EOS-4431] - 'rm -rf' return directory not empty if query exceeds default user limit of 100k files
* [EOS-4442] - Add a '-0' option to file touch



``v4.8.20 Citrine``
===================

2020-09-22

Bug
---

* MGM: unlimited scope of added missing Access mutex in PROC_BOUNCE_NOT_ALLOWED macro creates mutex inversions

``v4.8.19 Citrine``
===================

2020-09-21

Bug
---

* COMMON: fix XRootd 4.12.4 user name masking (WARNING: supports now uids only up to 1M)

``v4.8.18 Citrine``
===================

2020-09-17

Bug
---

* MGM: add missing mutex in access rejection macros

Improvement
-----------

* MGM: improve mutex contention in Access commmands (particular in combination with QDB Config)
* MGM: adding Prefetcher in various places

``v4.8.17 Citrine``
===================

2020-09-16

Bug
---

COMMON: adapt to new * => _ mapping of xrootd connection names for FUSE ID mapping

``v4.8.16 Citrine``
===================

2020-09-16

Bug
---

MGM: fix bug where a FuseX broadcast is run while the namespace write lock is held
SELINUX: add missing rules for 'mount' to work with default SE settings

Improvement
------------

* [EOS-4424] - Parse a second local eosxd configuration file
* [EOS-4427] - Show where in the code a mutex is held after exceeding a given threshold


``v4.8.15 Citrine``
===================

2020-09-09

Improvement
------------

* Release based on XRootD 4.12.4


``v4.8.14 Citrine``
===================

2020-09-09

Bug
----

* Release based on XRootD 4.12.3
* [EOS-4399] - Fusex repair functionality corrupts files


``v4.8.13 Citrine``
===================

2020-09-01

Bug
----

* [EOS-4412] - reduce latency due to scheduling deletions (long lasting view read locking)
* [EOS-4407] - block volume EDQUOT client-side with the first occurence of EDQUOT on a directory
* [EOS-4364] - prefer EEXIST over EACCESS in eosxd mkdir
* NS: fix command executed by drop-empty-cid

Improvement
-----------

* [EOS-4408] - add option to hide 'eos.*' attributes in eosxd listxattr
* FUSEX: load OAUTH ticket file when creating a trusted credential to have the proper jail prefixes used with containerizat
* MGM: make LRU engine less chatty
* NS: Implement ns-inspect command to drop empty directories


``v4.8.12 Citrine``
===================

2020-08-25

Bug
----

* [EOS-4389] - EOS does not install on Macs
* [EOS-4390] - EOS for Mac is missing libssl.1.0.0.dylib
* [EOS-4391] - EOS for Mac is missing libXrdSecProt.so
* [EOS-4400] - mgm crash in n __gnu_cxx::__verbose_terminate_handler()

Task
-----

* [EOS-3998] - Modifying the content of a file only changes mtime (not ctime)

``v4.8.11 Citrine``
===================

2020-08-05

Bug
----

* [EOS-3711] - XrdMgmOfs::mkdir does not honor mode parameter
* [EOS-3843] - Avoid to accept "unacceptable" block sizes (sys.forced.blocksize)
* [EOS-3991] - Trying to stat symbolic links in Recycle bin
* [EOS-4153] - Misleading error for lock order check when using timed locks
* [EOS-4279] - MGM restart corrupts mtime in a directory after mkdir + quota node creation
* [EOS-4319] - eos-ns-inspect reports wrong value for some extended attributes
* [EOS-4367] - eoscp check if hierarchy exists before attempting to create it
* [EOS-4369] - eos commands try to follow (non-EOS) symlinks

Task
-----

* [EOS-3775] - Rename stat.drain.* and friends to local.drain.*
* [EOS-4280] - User with no files and no quota limit should be removed from the list regardless of MGM restart?
* [EOS-4293] - Add JSON format for `eos who`

Improvement
------------

* [EOS-4308] - Update documentation for migrating to QDB config
* [EOS-4318] - Include extended attributes in eos-ns-inspect print
* [EOS-4371] - "eos file info inode": give error on "hex" input


``v4.8.10 Citrine``
===================

2020-07-24

Bug
----

* FUSEX: fix the real problem of EOS-4338 which is the destruction of the object before all read-ahead calls had been collected

Improvement
-----------

* FUSEX: add 'trace' option and enable all debug levels in the xattr interface
* FUSEX: trace 'slow' flush operations if they take more than 2000ms


``v4.8.9 Citrine``
==================

2020-07-20

Bug
----

* MGM: suppress commit of left-over entry-gateway replica happening during eosxd recovery - fixes EOS-4340
* FUSEX: bypass recursive rm detection by default if it is not enabled.
* FUSEX: avoid SEGV when read-ahead callback comes and didn't get a buffer - fixes EOS-4338
* FUSEX: fix repair when a write error occurs after the file is larger than the pre-fetch size and the first journal was not yet flushed
* FUSEX: remove 'return' short cut to see timings of readlink


``v4.8.8 Citrine``
==================

2020-07-07

Bug
----

* FUSEX: check in journalcache::reset if there is actually an open journal - fixes EOS-4322
* FUSEX: disable FST checksum checks for all reads in general, which can break recovery if not

Improvement
-----------

* FUSEX: close read-only files async in IO flush - fixes EOS-4328


``v4.8.7 Citrine``
===================

2020-07-06

Improvements
------------

* FUSEX: don't print 'IO blocked' for the root inode, since this frequently happens after wake-up
* FUSEX: print some user information if GETCAP results in EPERM
* FUSEX: print some debug information if journal()->reset() fails
* SPEC: Disable running spec scriplets if file /etc/eos/yum_with_noscripts is present


``v4.8.6 Citrine``
===================

2020-07-02

Bug
----

* MGM: don't place new replicas for read if filesize=0 and a replica is offline


``v4.8.5 Citrine``
===================

2020-07-01

Bug
----

* [EOS-4317] - Don't use repairOnClose for eosxd clients
* [EOS-3994] - MGM should not require mgmofs.configdir if config is stored in QDB

Improvement
------------

* [EOS-4311] - filesystem move is slow with in-QDB config and the lock taken triggers high node heartbeats
* [EOS-4312] - Allow to move a filesystem to a diffrent node via a command
* [EOS-4313] - _find should only prefetch container metadata if no_files is set


``v4.8.4 Citrine``
===================

2020-06-24

Bug
----

* [EOS-4305] - _remdir sends fusex notifications under namespace lock

Improvement
------------

* [EOS-3851] - do not `drainwait` group balancing on terminate drain statuses
* [EOS-4306] - Add namespace mutex acqusition latency stats to "eos ns"
* Add option to store the LevelDB on the data disk rather than root partition


``v4.8.3 Citrine``
===================

2020-06-19

Bug
----

* [EOS-4295] - Folder remove fails while deleting child version files (with Operation not permitted)
* MGM: remove timeordered caps entries if there insertion time has passed, don't rely on the cap
  validity beause it can be updated in the meanwhile
* MGM: default max children for eosxd listing to 128k not 128M

New feature
------------

MGM: Implement helper method for relocating filesystem to different FST

Improvement
------------

* Build on top of XRootD 4.12.3 that fixes some HTTP crashes
* XRootD5 compatibility
* SCITOKENS: Build libEosAccSciTokens.so as part of the eos release
* FST: Provide digest information if want-digest header present according to RFC3230
* [EOS-4299] - ResyncFileFromQdb error after FST upgrade to 4.8.2


``v4.8.2 Citrine``
===================

2020-06-11

Bug
----

* [EOS-4037] - eosxd gets SIGBUS in journalcache::read_journal()
* [EOS-4083] - eosxd abort() with "std::bad_alloc" under journalcache::get_chunks
* [EOS-4276] - Add extra checks while updating the directory e-tag on 0-size file updates
* [EOS-4282] - eos-client-4.7.16-1 requires xrootd-server-libs
* [EOS-4286] - Cannot set `eos.mtime` using xrdcp opaque query
* [EOS-4288] - `eos file adjustreplica` : error: invalid argument for file placement (errc=22) (Invalid argument)
* [EOS-4289] - Replicas dropped after a conversion of a non-healthy file

Improvement
------------

* [EOS-4284] - Allow automatic layout conversion hooks for file injection and file creation
* [EOS-4285] - negative cache entries are not served from eosxd cache


``v4.8.1 Citrine``
===================

2020-06-02

Bug
----

* SPEC: Fix CentOS8 Koji build
* MGM: Exclude tape locations from the converter merge procedure


``v4.8.0 Citrine``
===================

2020-06-02

Bug
----

* [EOS-3966] - Fix prefetching especially for RAIN and make it adaptive
* [EOS-4035] - FST service not starting (timeout) if there are too many log files
* [EOS-4214] - eos file convert behaviour
* [EOS-4259] - eosxd crash under metad::add_sync() /  EosFuse::create()
* [EOS-4260] - eosxd crash data::dmap::ioflush()

Task
----

* [EOS-3976] - The converter does not honour the source file checksum if sys.forced.checksum is set on /eos/<instance>/proc/conversion


``v4.7.16 Citrine``
===================

2020-05-18

Bug
---

* [EOS-4203] - reading empty missing replica file triggers commit & mtime update
* [EOS-4215] - ns time printing broken in fileinfo command

Improvements
-------------

* CMAKE: Refactor and simplify the cmake code to move to a target based approach


``v4.7.15 Citrine``
===================

2020-05-14

Bug
---

* [EOS-4299] Fix stat counters update frequency
* MGM: Add missing lock to MgmStats in the stall functionality
* MGM: stat.st_nlink is an UNSIGNED integer.  Replaced dangerous -1 logic with safe usigned logic


``v4.7.14 Citrine``
===================

2020-05-11

Bug
---

* [EOS-4210] - `eos fs ls -d` shows disks which are actually not in drain (stat.drain is empty)

New Feature
-------------

* [EOS-4205] - Be able to hide .sys.v# like folder/files to users

Improvement
------------

* [EOS-4197] - Show available redundancy in 'ls -y '
* [EOS-4207] - Add Quota (ls) comand to GRPC interface
* [EOS-4212] - Review POSIX permission behaviour in eosxd & enable overlay behaviour


``v4.7.13 Citrine``
===================

2020-05-08

Bug
----

* [EOS-4084] - 'eos fs mv'  returns 0 even in case of errors
* [EOS-4171] - GDB seg faults when taking backtraces of EOS daemons
* [EOS-4182] - FUSEX: 'Invalid argument' instead of 'Permission denied' on non-cached access to restricted directory
* [EOS-4183] - eosxd: unable to delete, temporary I/O error on directory
* [EOS-4187] - MGM: fs commands return random "return codes"
* [EOS-4188] - Crash in XrdMgmOfsFile::open
* [EOS-4189] - EOSHOME-I00 crash on XrdMgmOfsFile::open
* [EOS-4209] - MGM: sys.acl does not accept denial of some permissions

Improvement
------------

* [EOS-4113] - log: add fs number to the MGM logs for FST redirections
* [EOS-4169] - Missing fsids in file info -m and json when NA context (it is not the case in normal file info)


``v4.7.12 Citrine``
===================

2020-04-29

Bug
----

* [EOS-4178] - use 'x' bits from ACL+POSIX for directories, while only from POSIX for files

``v4.7.11 Citrine``
===================

2020-04-28

Bug
----

* [EOS-3867] - MGM redirecting to itself
* [EOS-4110] - `eos fs mv` not working properly for multi-fst instances
* [EOS-4122] - `eos file touch` does not create a file if it not exists
* [EOS-4131] - MGM: Broken logic in fs add leads to various inconsistencies
* [EOS-4133] - MGM: Deadlock when booting the in memory namespace
* [EOS-4137] - MQ: Exceeded message backlog never recovers
* [EOS-4139] - eosxd sees EIO when rate limiter sends stalls
* [EOS-4140] - Allow the eos command-line tool to modify the disk layout of a "tape only" file
* [EOS-4150] - MGM: Acl should check for update flag present
* [EOS-4151] - Broken shutdown sequence for EOS daemons
* [EOS-4168] - rename & move of symlinks not supported in FuseServer

New Feature
------------

* [EOS-3415] - feature: `eos status` view

Improvement
------------

* [EOS-4011] - Allow "eos rm" by fid for weird cases
* [EOS-4091] - Add LRU caching to XrdMgmOfsDirectory class
* [EOS-4092] - Add LRU caching to proc::ls function
* [EOS-4129] - Add STAT equivalent functionality to GRPC
* [EOS-4142] - Only set filesize in MGM when eosxd has opened a file on FSTs
* [EOS-4152] - MGM: GroupBalancer improve cancellation/cleanup by using std::thread
* [EOS-4166] - Enforce wait-for-flush behaviour on file creation for a list of given executables
* [EOS-4167] - Enhance fsck repair to take an fsid and error type


``v4.7.10 Citrine``
===================

2020-04-17

Bug
----

* [EOS-4103] - FUSEX marks as 0600 file as "executable"
* [EOS-4112] - Deadlock between mdstackfree and data::unlink
* HTTP/FST: Fix crash by replying with 411 when a PUT without Content-Length is attempted

Improvement
------------

* [EOS-4108] - Merge tape replicas in conversion jobs
* [EOS-3913] - eos report is reporting deletion of files that were never transferred in the first place
* [EOS-4104] - Allow to select, O_DIRECT O_SYNC O_DSYNC via CGI


``v4.7.9 Citrine``
===================

2020-04-08

Bug
----

* [EOS-4095] - MGM crash in `eos::common::Logging::log`
* [EOS-4096] - Crash due to missing args in FuseServer error message

Improvement
------------

* NS: Use std::mutex in the NS LRU implementation instead of eos::common::RWMutex
  for better performance
* [EOS-4003] - Export sys xattr to trusted machines through FUSEX


``v4.7.8 Citrine``
===================

2020-04-06

Bug
---

* [EOS-4082] MGM: remove sym link files from the file view directly
* FST: Fix misuse of [] operator on map which can lead to crashes
* COMMON: Make sure we use the same shared_mutex implementation (cv)
  everywhere and update qclient

Improvement
------------

* COMMON: Encapsulate VOMS mapping functionality and reuse it for both gsi
   and http authentication
* [EOS-3960] - eos-ns-inspect improvements


``v4.7.7 Citrine``
===================

2020-04-01

Bug
---

* MGM: fix lock order violation in FuseServer file creation
* NS: Fix inverted condition when calculating etag for md5
* Fixes bit-flip error when setting rsp.is_on_tape


Improvements
-------------

* MGM: disable fusex versioning on rename - can by defining  xattr 'sys.fusex.versioning'
* MGM: clone/hard links/recycle bin
* MGM: Made tape-aware GC persistent between MGM restarts
* MGM/FST The sys.cta.archive.objectstore.id xattr of a file is now set when it is queued for archival to tape


``v4.7.6 Citrine``
===================

2020-03-30

Bug
----

* [EOS-4063] - Error creating version folder
* [EOS-4069] - Git clone does not work


``v4.7.5 Citrine``
===================

2020-03-23

Bug
----

* This only fixes a Koji build issue otherwise it's identical to 4.7.4


``v4.7.4 Citrine``
===================

2020-03-23

Bug
----

* [EOS-4013] - EOSBACKUP "FST still misses the required capability key"
* [EOS-4046] - sync client re-downloading files

New Feature
------------

* [EOS-4057] - Allow fine-graned stall rules for eosxd access and restic bypass

Improvement
------------

* [EOS-4056] - Make the TPC key validity configurable


``v4.7.3 Citrine``
===================

2020-03-12

Bug
----

* [EOS-4042] Cannot see the content of a version


``v4.7.2 Citrine``
===================

2020-03-09

Bug
----

* [EOS-3920] - eosxd crash in EosFuse::DumpStatistic()
* [EOS-4016] - FUSEX: file content mixup / data corruption
* [EOS-4025] - utimes call does not set cookie in disk cache
* [EOS-4031] - fst crash in eos::fst::FileSystem::UpdateInconsistencyInfo() while
  registering fss
* [EOS-3605] - FUSEX crash in metad::pmap::lru_add()
* [EOS-4029] - eosxd abort() in Json::Value::isMember - "Json::Value::find(key, end, found): requires objectValue or nullValue"

Improvement
------------

* [EOS-3745] - Allow static mapping of HTTP access to a non-root user


``v4.7.1 Citrine``
===================

2020-03-06

Bug
----

* FST: Disable async close functionality that triggers a bug in XRootD due to memory corruption - seen in EOSPROJECT
* EOS-4027: RAIN file chunk dropped when chunk drain fails due to node being offline - seen in EOSALICEDAQ


``v4.7.0 Citrine``
===================

2020-02-21

New Feature
------------

* Provide backup-clone functionality
* Provide tape garbage collector base-line implementation
* [EOS-3956] - Provide the expected checksum per block in the namespace in RAIN files

Bug
----

* [EOS-3377] - find -b shows wrong accounting for RAIN files
* [EOS-3867] - MGM redirecting to itself
* [EOS-3912] - Balancing prevented for RAIN files
* [EOS-3917] - SetNodeConfigDefault might be called before gOFS->mMaster has been initialized
* [EOS-3954] - eos documentation guides people towards an insecure QDB deployment
* [EOS-3969] - Bug in NextInodeProvider raises possibility of creating two containers with colliding IDs
* [EOS-4000] - Spurious errors of fusex-benchmark test 13

Task
-----

* [EOS-3819] - Create automatically the missing directories when recovering files

Improvement
------------

* [EOS-3370] - RFE: "eos file check" , "eos file info" should show 'user.eos.filecxerror' status for full-replica
* [EOS-3967] - Extend redirection URL length accepted by the MGM


``v4.6.8 Citrine``
===================

2020-01-22

Bug
---

* FUSEX: fix writer starvation triggered by EDQUOT errors
* [EOS-3872] - FST should delete file on WCLOSE when archive request cannot not be queued
* [EOS-3873] - Coredump in jerasure_matrix_to_bitmatrix
* [EOS-3885] - Add "tape enabled" configuration attribute to /etc/xrd.cf.mgm
* [EOS-3915] - FUSEX uses std::stoll instead of std::stoull to parse inodes, breaking new inode encoding scheme

Improvement
-----------

* FUSEX: support oauth token files - see OS-9604
* FUSEX: allow to track write buffers using 'eos fusex evict UUID sendlog'
* FUSEX: add CERN automount script/configs and update SELINUX policies accordingly supporting SquashFS mounting
* FST: support ISA-L accelerated adler/crc32c checksum
* FST: add generic eos-checksum command
* FST: support xxhash64,crc64 and sha256 as checksums
* ALL: Add basic support for Macaroons and SciTokens


``v4.6.7 Citrine``
===================

2019-12-16

Bug
---

* [EOS-3854] - Fixed SELinux policy regression bug which installed wrong file on SLC6

Improvement
-----------

* [EOS-3886] - Enrich eosreport in the context of TPC

``v4.6.6 Citrine``
===================

2019-12-09

Bug
---

* FUSEX: avoid starvation due to no quota error during open in flush-nolock
* APMON: bump to latest version

Improvement
-----------

* [EOS-3879] - Adding a field that reports free writable bytes
* [EOS-3882] - eos report is not reporting deletion timestamp
* CONSOLE: Suppress routing information for 'quota ls -m' requests

``v4.6.5 Citrine``
===================

2019-12-05

Bug
---

* [EOS-3611] - MGM unresponsive, does not appear to recover on its own
* [EOS-3715] - fst offline: Publisher cycle exceeded
* [EOS-3827] - MGM Upgrade: After restarts prevent storage node heartbeats to increase
* [EOS-3858] - ARCHIVE: Broken due to utimes silent error
* [EOS-3864] - unable to boot filesystem after eos fs add
* MGM: Remove sys.cta.objectstore.id xattr on successful retrieve

Improvement
------------

* [EOS-3860] - Allow lock-free iteration over long directory listings
* [EOS-3862] - eos client: hardcode RPM dependency on 'zeromq'
* [EOS-3875] - Drop use of std::ptr_fun, std::not1
* [EOS-3880] - RaftReplicator pipelines way too many pending batches inside QClient


``v4.6.4 Citrine``
===================

2019-12-03

Bug
---

* [EOS-3854] MISC: Version SELinux policy files for targeted platforms (SLC6 and CC7)


``v4.6.3 Citrine``
===================

2019-11-20

Bug
---

* [EOS-3717] FUSEX: fix lru_xyz SEGV in eosxd
* [EOS-3853] NS: more options to filter with inspect command
* FUSEX: fix WR buffer exhaustion triggered by out-of-quota writes

New Feature
-----------

* allow IPC connections via ZMQ to bypass xrd-threadpool for admin commands - usage 'eos ipc:// ...'
* make the maximum number of listable entries by eosxd configurable: EOS_MGM_FUSEX_MAX_CHILDREN=32768


``v4.6.2 Citrine``
===================

2019-11-18

Bug
---

* fix eosxd messaging for renames, commits, versioning
* avoid spurious entries in quota map
* [EOS-3692] print critical messages when FUSEx throws runtime_errors
* [EOS-3793] prefix recycle restore keys with fxid: and pxid: to avoid ambiguities
* [EOS-3798] suppress atomic/versioning for 'verify --commit' workflows
* [EOS-3808] broadcast externally versioned files into fusex network
* [EOS-3822] avoid SEGV in FUSEx recovery
* [EOS-3823] avoid infinite loop unlinkAllLoctions
* [EOS-3829] parsing problem
* [EOS-3833] avoid SEGV when logfile is not opened
* [EOS-3834] console char replacement
* [EOS-3839] avoid deadlock in lock order violation
* [EOS-3845] create barrier in FST creation to avoid race condition under file creation from two clients
* [EOS-3848] store exception in future
* [EOS-3850] avoid SEGV in FUSEx deletion of non-existant objects

New Feature
-----------

* cta add-ons for multi-space usage
* make fsck thread-pool configurable
* json response format for xrdfs query prepare
* stall logic for prepares
* more options in eos-ns-inspect
* decrease noserver FUSEx timeouts to 15/2 minutes (r/w)


``v4.6.1 Citrine``
===================

2019-10-31

Bug
---

* Fix wrong linking in the eos-client package
* General restructuring of the link dependencies


``v4.6.0 Citrine``
===================

2019-10-30

Bug
----

* [EOS-2990] - FSCK on QuarkDB causes higher latency
* [EOS-3437] - FST crash around eos::common::DbMapTypes::Tlogentry::~Tlogentry()
* [EOS-3469] - no replica information on file check but the physical file is there
* [EOS-3470] - eos verify: unable to verify ... no local MD stored
* [EOS-3497] - Avoid ghost entries to fail the draining of a disk
* [EOS-3689] - MGM crashed in XrdCl::Utils::CheckTPCLite()
* [EOS-3726] - FST crash in eos::fst::Adler::Add (negative "length")
* [EOS-3736] - FST registration causing locking issue
* [EOS-3743] - 'eos fs rm' triggers the following error: "cannot set net parameters on filesystem"
* [EOS-3751] - weird behavior of the geoscheduler when some FSTs changed the geotag
* [EOS-3783] - Miniconda2-latest-Linux-x86_64.sh - no exec bit for 'python' from archive
* [EOS-3790] - MGM gets stuck when using local QuarkDB MD lock
* [EOS-3791] - Transfers timeout on EOS\CERNBox home folders A G J K W
* [EOS-3792] - eos quota not redirecting to proper home
* [EOS-3799] - XrdMgmOfs::Emsg() calls strerror() which is NOT thread safe
* [EOS-3802] - eos acl not setting acl's
* [EOS-3803] - FUSEX client says "Directory not empty" on removal (bad caching?)
* [EOS-3805] - EOS client links against system XRootD instead of eos-xrootd
* [EOS-3806] - eoscp won't copy the file if the 'extra' stripes are missing

Task
----

* [EOS-3583] - Repair logs (useful metadata)
* [EOS-3591] - 'file info' resolves symlinks and displays info of the referenced file
* [EOS-3710] - TPC from castor/ceph to EOS not working

Improvement
-----------

* [EOS-3371] - RFE: update "user.eos.filecxerror" on FST checksum verification failures
* [EOS-3750] - Change error message for adjustreplica


``v4.5.13 Citrine``
===================

2019-11-15

Bug
----

* [EOS-3839] MGM: Fix lock inversion leading to deadlock when calling getmdlocation
* [EOS-3729] FUSEX: fix bug in wait_flush method leading to a mix-up of rename/unlink records
* MGM/FUSEX: Fix faulty assumption that getFile would raise an exception (had been
  changed when Qdb was introduced) - fixes spurious EIO errors and 'Attempt to add
  an existing file' messages.


``v4.5.12 Citrine``
===================

2019-10-28
==========

* [EOS-3792] - eos quota not redirecting to proper home

Improvement
-----------

* [EOS-3800] - Routing mechanism of proto commands


``v4.5.11 Citrine``
===================

2019-10-22

Bug
----

* MGM: fix rare lockups observed due to wrong expectation of an exception thrown


``v4.5.10 Citrine``
===================

2019-10-16

Bug
----

* [EOS-3736] - FST registration causing locking issue
* [EOS-3737] - Possible eos file verify commands causing deadlock while restarting mgm
* [EOS-3710] - TPC from castor/ceph to EOS not working
* [EOS-3774] - FUSEX: fix recovery problem when files are truncated to 0 size
* FUSEX: fix rc=EPERM for setxattr if not called by uid=0
* FUSEX: fix possible out-of-memory scenario when applications keep writing on fatal
  error conditions like out-of-quota


``v4.5.9 Citrine``
===================

2019-09-11

Bug
----

* MGM: Update rights 'u' are implicit in 'w'
* EOS-3721: Slave MGMs in old-implementation master-slave should refuse to boot on QDB-namespaces


``v4.5.8 Citrine``
===================

2019-09-10

Bug
----

* FST: Fix FST metadata synchronization with the MGM info when delay is not respected

Improvement
-----------

* FUSEX: Enable safe mode by default - when a file is created the client always gets
  feedback if the FST open didn't work.


``v4.5.7 Citrine``
===================

2019-09-09

Bug
----

* Fix bug in the MgmSync process which could crash the FST
* [EOS-3633] - Many new commands are not compatible with old server version
* [EOS-3696] - shell: "cd ../../" does nothing?
* [EOS-3705] - Error when updating eos-archive
* [EOS-3703] - FST not starting if mountpoint not present
* [EOS-3684] - eosxd crash in debug() in EosFuse::readdir()
* [EOS-3608] - Wrong help for space policy and no error message

Improvement
------------

* [EOS-2725] - Missing usage example for some space parameters
* [EOS-3694] - Add eos-fusex-tests to the pipeline
* [EOS-3706] - Add 1m,1w,daily timebins to versioning similiar to DFS
* GRPC: Add version command implementation and other ns related operations


``v4.5.6 Citrine``
===================

2019-08-26

Bug
----

* [EOS-3315] - eos file adjustreplica selects bad replica for replication
* [EOS-3572] - Crash while reloading the config in eoslhcb
* [EOS-3575] - EOSCMS - killed by SIGSEGV (around eos::mgm::GeoTreeEngine::applyBranchDisablings)
* [EOS-3624] - eosxd SEGV eraseTS
* [EOS-3669] - Wrong Routing when target path ends as <path>/.
* [EOS-3678] - space define command doesn't set groupmod
* [EOS-3680] - Space set subcommand affects all groups and nodes
* [EOS-3687] - getQuotaNode throws an exception when called on a detached container, instead of returning nullptr
* [EOS-3700] - eosxd SEGV apply
* [EOS-3701] - eosxd SEGV lookup
* [EOS-3704] - rename/stat/open handling of trailing '/'

New Feature
------------

* [EOS-3682] - gRPC container insert does not inherit extended attributes

Improvement
------------

* [EOS-3474] - GroupBalancer logging


``v4.5.5 Citrine``
===================

2019-08-07

Bug
---

* [EOS-3536] - fix hard-link cleanup problems seen with 'rm -rf' on git repositories
* [EOS-3644] - adjust eosxd cache path filename hashing for physical inodes
* [EOS-3643] - avoid ghost entries when files are overwritten and support reycle bin for those


Improvements
------------

* [EOS-3638] - introduce file info detached field
* speed-up shutdown for drain jobs
* implement ns-reserve-id command
* don't print byte-range locks per client ( get it with '-k' option )
* filesytem class refactoring
* clean-up empty eosxd cache directories
* support proc results larger than 2G
* timeout eosxd connections after 24h


``v4.5.4 Citrine``
===================

2019-08-01

Bug
---

* [EOS-3622] - eoscp is not propagating the error code.
* [EOS-3629] - Provide fallback for the quota command to old implementation
* [EOS-3631] - port flag is ignored on eosfstregister script
* [EOS-3632] - mv on FUSEX deterministically loose data
* [EOS-3633] - Many new commands are not compatible with old server version

Question
---------

* [EOS-3626] - eos mgm cannot contact to external eos instance via eos route


``v4.5.3 Citrine``
===================

2019-07-25

Bug
---

* [EOS-455] - RFE: drop either fid: or fxid:, use the other consistently
* [EOS-3577] - Crash in ReplicationTracker
* [EOS-3579] - io stat shows negative values (overflow?)
* [EOS-3585] - eosxd crash below cap::capflush() / metad::cleanup()
* [EOS-3604] - Apply path mapping for eos rm command
* [EOS-3609] - Wrong json format in file info when & are in pathnames
* Fix bug related to interference between logrotation and QdbMaster setup for
  high-availability observed at JRC.

Improvements
------------

* Extend ns cache drop command to drop individual entries
* Move the following commands to the protobuf implementation: access, quota,
  config, node and space.
* [EOS-3602] - Drop automatic conversion attempt from default output to JSON for
  protobuf commands with JSON flag on. Each proto command will be
  responsible of providing valid JSON output.
* [EOS-3606] - Add birth time to a file's metadata when it is created/born


``v4.5.2 Citrine``
===================

2019-06-27

Bug
---

* if eosxd is compiled without ROCKSDB support, it should not touch mdcachedir e.g. it has to stay empty - fixes EOS-3558
* require eos-rocksdb on SLC6 and EL7 to have support for swapping inodes

``v4.5.1 Citrine``
===================

2019-06-25

Bug
---

* [ EOS-3546 ] Apply remote quota updates if q-node has no file open

New Feature
-----------

* [ EOS-3548 ] Replication Tracker class (see docs/configuration/tracker)

``v4.5.0 Citrine``
===================

2019-06-21

Bug
---

* [ EOS-3495 ] Handle out-of-quota open correctly in eosxd
* [ EOS-1755 ] Don't irritate du with . entry size
* [ EOS-3536 ] Fix hardlink deletion logic to avoid hidden entries after all references have been removed
* [EOS-3279] - eos fs dumpmd RC wrong
* [EOS-3396] - File with two 'bad' replicas: one has size mismatch, the other xsum mismatch
* [EOS-3499] - eos-ns-inspect: Include again the libprotobuf dependency
* [EOS-3522] - 'eos config dump --vid' prints dummy "mgm.vid.key=<key>", cannot  "eos vid rm'
* [EOS-3526] - eosxd crash in EosFuse::readlink(), NULL 'md' pointer
* [EOS-3533] - eos find doesnt work with --fid and -0

New Feature
-----------

* [EOS-3532] - Allow default placement policies per space

Improvement
-----------

* Provide optional GRPC service in MGM
* Documentation improvements
* Swap-in-out eosxd inodes with lru table into rocksdb DB
* Block only running file drains from parallel draining
* CTA GC monitoring in 'eos ns'
* [ EOS-3514 ] Implement orphan detection in eos-ns-inspec
* [ EOS-3490 ] Support printing mctime, ctime in eos-ns-inspec
* [EOS-3409] - 'bind mount' FUSEX, no credentials: "No such file or directory"
  instead of "Permission denied"
* [EOS-3519] - Add the possibility to do attr ls with the fid/pid
* [EOS-3520] - add pid to the json output of file info
* [EOS-2020] - Use Table Formatter for geosched show tree and snapshot commands output
* [EOS-3513] - Provide an exception when eos dumpmd <fsid> --path is not really empty
* [EOS-3527] - FSCK dection tool: Classify size errors for not orphan files
* [EOS-3531] - FSCK detection: Ignore size 0 files in the namespace in replica error detection
* Move the "group" command to the Protobuf implementation
* Move the "io" command to the Protobuf implementation
* Move the "debug" command to the Protobuf implementation


``v4.4.47 Citrine``
===================

2019-05-17

Bug
---

* freeze client RPATH to XRootD location used during build

Improvement
-----------

* CTA module v 0.41
* Extended 'prepare' for XRoot 4.4.10 (abort etc.)
* Report detached files in 'eos-fsck-fs'
* [ EOS-3483 ] - add container id in output of stripediff option
* [ EOS-3484 ] - add location to output of stripediff option
* [ EOS-3532 ] - introduce space default placement policies ( obsoletes per directory extended attributes for default placement policy)
* use eos-protobuf3 eos-xrootd only on EL7 for tags like x.y.z-0, otherwise only eos-protouf3 on EL7 builds


``v4.4.46 Citrine``
===================

2019-05-15

Bug
---

* Fix FST conversion from NS proto to Fmd
* Fix RPATH configuration to force linker locations

Improvement
-----------
* Implement 'eos fsck search' to forward FSCK from NS to FSTs
* Expose 'eos resync' and 'eos verify -resync' to force FMD resynchronization on FSTs
* Refactor ScanDir code

``v4.4.45 Citrine``
===================

2019-05-14


Bug
---

* Introduce obsoletes statement in spec file for eos-protobuf3/eos-xrootd

Improvement
-----------

FST: Refactor the ScanDir code and add simple unit tests
FST: Encapsulate the rate limiting code into its own method
FST: Start publishing individual fs stats
NS: Add etag, flags to eos-ns-inspect output

``v4.4.44 Citrine``
===================

2019-05-08

Bug
---

* FST: fix dataloss bug introduced in 4.4.35 when an asynchronous replication fails (adjustreplica cleaning up also the source)


``v4.4.43 Citrine``
===================

2019-05-08

Improvements
------------
* FUSEX: add compatiblity mode for older server which cannot return getChecksum by file-id
* CI: build with ubuntu bionic
* NS: Add mtime, ctime, unlinked locations, and link name to eos-ns-inspect printing
* CTA: configuration parameters for tapeaware garbage collector

``v4.4.42 Citrine``
===================

2019-05-07

Improvements
------------

* FUSEX: lower default IO buffer size to 128M
* MGM: remove unnecessary plug-incall
* NS: implement subcmd to change fid attributes

``v4.4.41 Citrine``
===================

2019-05-07


Bug
---
* [EOS-3462] - FUSEX: suppress concurrent read errors for unrecoverable errors
* MGM: Fix monitoring output for eos fusex ls -m

Improvements
------------

* NS: Implement inspect subcommand to run through all file/directory metadata
* [EOS-3463] - implement stripediff functionality in inspect tool
* MGM: optimize quota accounting to correct for the given default layout when queried for quota via 'xrdfs ... space query /'
* FUSEX: if a logfile exceeds 4G, we shrink it back to 2G
* CTA: various cta related fixes (see commits)

``v4.4.40 Citrine``
===================

2019-05-03


Bug
---

* FUSEX: avoid hanging call-back threads whnen a files is not attached and immedeatly unlinke
* FUSE:  allow unauthenticated stats on the mount point directory ( for autofs )
* FUSEX: silence mdstrackfree messages to debug mode
* [EOS-3446] - CONSOLE: Return errno if set otherwise the XRootD client shell code approximation
* FST: Don't report RAIN files as d_mem_sz_diff in the fsck output
* FUSEX: allow setting 'eos.*' attributes by silently ignoring them
* NS: add detection for container names '.' and '..'


Improvements
-------------

* NS: Report any errors found by ContainerScanner or FileScanner in check-naming-conflicts
* Adding ' eos-leveldb-inspect' tool
* MGM: Refactor Fsck


``v4.4.39 Citrine``
===================

2019-04-30


Bug
---

* [EOS-3313] - ns master other output looks incorrect
* [EOS-3378] - double draining into same destination gives corrupted or empty replica
* [EOS-3407] - Schedule2Balance reports long lasting read locks
* [EOS-3414] - EOS config file could not be loaded
* [EOS-3439] - rw filesystems shown with 'fs ls -d'
* Fix for draining of RAIN file when parity information was not stored back on disk.
* Enforce checksum verification for all replication operations.

Documentation
-------------

* Add documentation for EOS on Kubernetes deployment


``v4.4.38 Citrine``
===================

2019-04-24

Bug
----

* Fix LRU which was looping and taking the FsView lock when disabled
* [EOS-3427] - getUriFut can overwhelm the folly executor pool, causing slowness and potential deadlocks
* [EOS-3432] - MGM crash in eos::NamespaceExplorer::buildDfsPath

Improvement
------------

* [EOS-3431] - MGM: make "func=performCycleQDB" log (much) less


``v4.4.37 Citrine``
===================

2019-04-16

Bug
---

* Fix deadlock in the folly executor introduced when using a single folly
  executor for the entire namespace.

Improvements
-------------

* Add env variable to control the master-slave transition lease validity.
  EOS_QDB_MASTER_INIT_LEASE_MS


``v4.4.36 Citrine``
===================

2019-04-16


Bug
----

* Fix deadlock in the Iostat class introduced in the previous release.
* [EOS-2477] - MGM lockedup after enabling LRU - Citrine with new namespace
* [EOS-3337] - MGM crash around XrdMgmOfs::OrderlyShutdown() on "orderly" shutdown
* [EOS-3405] - MGM switches drain filesystems to empty

Improvement
------------

* [EOS-3356] - RFE: shut up the 'verbose' recursive "chown" under /var/eos
* [EOS-3389] - review "error: no drain started for the given fs": do not trigger this or do not log
* [EOS-3402] - "eos node ls": double 'status' column, white-on-white text
* [EOS-3412] - silence "failed to stat recycle path" error on rename+remove?
* [EOS-3421] - Flood of "SOM Listener new notification" messages in the log since 77cfb51213


``v4.4.35 Citrine``
===================

2019-04-11

Bug
---
* [EOS-3400] - don't commit any replica with write errors
* [EOS-3399] - never drop all replicas in reconstruction or injectino failure scenarios
* [EOS-3398] +
* [EOS-3237] - never wipe local MD in eosxd with LEASE messages
* [EOS-3410] - catch JSON exception produced by empty strings
* [EOS-3408] - fixs prefetch logic in fileReadAsync(XrdIo)
* fix fading heart-beat problem: re-enable a queue in MQ if a client has cleared backlog

Improvement
-----------

* add 'eos-fsck-fs' command to run standalone fsck on FSTs
* add read-ahead test for XrdIo
* [EOS-3391] - make geotag propagation less verbose
* [EOS-3406] - move some log messages from error to debug
* [EOS-3390] - suppress UDP target missing message
* [EOS-3401] - if scanner is diabled don't even scan files a first time
* avoid FuseXCasts when _rem is called in FuseServer with recycle bin enabled

Refactoring
-----------

* fix some more fid/fxid log messages to use the hex format
* drop use of BackendClient in MetadataProvider

``v4.4.34 Citrine``
===================

2019-04-05

Bug
---

* [EOS-3394] - automount might fail due to race condition in ShellExecutor/ShellCmd test

Improvement
-----------

* RAIN placement uses round-robin algorithm to define the entry server

``v4.4.33 Citrine``
===================

2019-04-04

Bug
----

* Disable prefetching for TPC transfers which might corrupt the data.
* Put the mgm.checksum opaque info for drain jobs in the unencrypted part of
  the URL otherwise the checksum check is not enforced.
* [EOS-3367] - "eos file verify --checksum" does not update FMD checksum or ext.attribute
* [EOS-3372] - MGM "autorepair" for corrupted replicas is not working
* [EOS-3382] - Network monitoring always shows 0 on newer kernel versions

Improvement
------------

* [EOS-3359] - Graceful cancelation of drain jobs
* [EOS-3375] - Use eos/conversion as io stat tag

Refactoring
-----------

* Introduce NamespaceGroup

``v4.4.32 Citrine``
===================

2019-03-26

Bug
---

* [EOS-3347] - Fix slave follower problem with new mutex implementation due to unlock_shared vs unlock calls
* [EOS-3348] - openSize used in XrdFstOfsFile::open
* [EOS-3350] - Fusex lists duplicate items
* [EOS-3352] - RAIN upload is not failed if a stripe cannot be opened for creation
* [EOS-3354] - MGM deadlock while loading the configuration


Refactoring
-----------

* Rename VirtualIdentity_t to Virtualidentity
* Replace Fs2UuidMap maps with FilesystemMapper, drop unused 'nextfsid' global configuration

Improvements
------------

* Allow to disable partition scrubbing by creating /.eosscrub on the FST partition
* Add warning messages containing timing information about delayed heartbeat messaging


``v4.4.31 Citrine``
===================

2019-03-21

Bug
---

* HTTP: Extend lifetime of variable pointed to from the XrdSecEntity object
* CONSOLE: Refactor the RecycleHelper for easier testing. EOS-3345
* MGM: Display real geotag field in FileInfo JSON format. Additionally, display forcegeotag field when available
* FST: Fix default geotag to be less than 8 chars
* FST: Add a check for Geotag length limit. Fixes EOS-3208
* MGM: Fail file placement if a forced scheduling group is provided and the

Refactoring
-----------

* MGM: Implement method to allocate new fsid based on uuid in FilesystemUuidMapper
* MISC: Remove any kinetic reference
* CONSOLE
* ALL: enum class for filesystem status - strongly typed

Improvements
------------

* MGM: add BackUpExists flag for files on CTA
* MGM: Add estimate for drain TPC copy timeout based on the size of the file and a
* MGM: Check geotag limit also on fs config forcegeotag command
* MISC: Basic bash completion script. Fixes EOS-3252
* MGM: Add tracking for in-flight requests in the MGM code for cleaner master-slave
* ARCHIVE: Increase the TPC transfer timeout to 1 hour


``v4.4.30 Citrine``
===================

2019-03-18

Bug
---

* FUSEX/MGM: allow all combinations of client/server versions by considering the
  config entry if 'mdquery' is supported or not
* FUSEX: fix return code of eos-ioverify in case of any IO error

Improvements
------------

*  ALL: Drop "drainstatus" from the persistent config and use "stat.drain" to
   hold the current status of the draining for a filesystem. This reduces also
   the number of configuration save operations triggered by the draining and
   we rely only on "configstatus" to decide whether or not draining should
   be enabled. Note: all "stat.*" are filtered out from the persistent config.


``v4.4.29 Citrine``
===================

2019-03-14

Bug
----
* Release built on top of XRootD 4.8.*


``v4.4.28 Citrine``
===================

2019-03-12

Bug
----

* Fix bug in the namespace conversion tool when computing the quota nodes
* Fix bug in the QuotaNodeCode copy constructor which was preventing a quota
  node recomputation
* [EOS-3316] - Namespace conversion tool suffers from high lock contention on releases 4.4.26, 4.4.27

Improvements
------------

* Refactor the FuseServer code into various functional pieces
* Use std::mutex for conversion tool rather than RWMutex which hinders performance


``v4.4.27 Citrine``
===================

2019-03-07

Bug
----

* [EOS-3200] Fix crash in zmq::context_t constructor due to PGM_TIMER env variable
* [EOS-3308] Drain status shown but machine is in configstatus rw
* Put back fflush in Logging class to check

Improvements
------------

* MGM/CONSOLE/DOC: extend LRU engine to specify policies by age and size limitations
  like 'older than a week and larger then 50G' or 'older than a week and smaller than 1k'
* NS: Add sharding to MetadataProvider to ease lock contention


``v4.4.26 Citrine``
===================

2019-03-04

Bug
----

* [EOS-3246] - IPv6 addresses parsing broken
* [EOS-3256] - Add XRootD connection pool to the MGM
* [EOS-3257] - interactive 'eos' CLI aborts around eos::common::SymKeyStore::~SymKeyStore()
* [EOS-3261] - EOSBACKUP locked up
* [EOS-3263] - eosxd does not support seekdir/telldir
* [EOS-3265] - Node config values never removed
* [EOS-3266] - First MGM boot on clean namespace does not setup "/", "/eos", etc if EOS_USE_QDB_MASTER is set
* [EOS-3267] - Dump files on CERN FSTs goes into a file named /var/eos/mdso.fst.dump.lxfsre10b04.cern.ch:109
* [EOS-3276] - Inconsistent behavior (and doc) for "eos fs config" and "eos node config"
* [EOS-3296] - eoscp crash while copying 'opaque_info' data
* [EOS-3299] - Workaround for XRootD TPC bug in Converter which leads to data loss.
               This is not a definitive fix.
* [EOS-3280] - Logrotate rpm dependency missing for eos-server package
* [EOS-3303] - Implement InheritChildren method for the QuarkContainerMD which otherwise
               crashes the MGM for commands like "eos --json fileinfo /path/to/dir/".

Improvement
------------

* [EOS-3249] - Add "flag" file for master status
* [EOS-3251] - Expose Central drain thread pool status in monitoring format
* [EOS-3269] - path display in `eos file check` output
* [EOS-3295] - Allow MGMs to retrieve stacktraces and log files from eosxd at runtime

Note
-----

Starting with this version one can control the xrootd pool of physical connections
by using the following two env variables:
EOS_XRD_USE_CONNECTION_POOL - enable the xrootd connection pool
EOS_XRD_CONNECTION_POOL_SIZE - max number of unique phisical connection
towards a particular host.
This can be use in the MGM daemon to control connection pool for TPC transfers
used in the Converter and the Central Draining, but also on the FST side for
FST to FST transfers.

The following two env variables that proided similar functionality only on the
FST side are now obsolete:
EOS_FST_XRDIO_USE_CONNECTION_POOL
EOS_FST_XRDIO_CONNECTION_POOL_SIZE


``v4.4.25 Citrine``
===================

2019-02-12

* [EOS-3152] - FUSEX: crash below data::datax::peek_pread


``v4.4.24 Citrine``
===================

2019-02-11

Bug
----

* [EOS-3240] - EOSBACKUP crash related somehow to ThreadPool
* FUSEX: fix logical error in read overlay logic - fixes EOS-3253
* FUSEX: fix datamap entry leak whenever a file is truncated by name and not via file descriptor
* FUSEX: fix ugly kernel deadlock appearing in consumer-producer workloads

Improvement
------------

* FUSEX: reduce the default wr/ra buffer to 256 MB if ram>=2G otherwise ram/8


``v4.4.23 Citrine``
===================

2019-01-31

Bug
----

* [EOS-3231] - Update is not anymore implicit in ACL:w permissions - non-fuse fix
* FUSE: Stop returning reference to temporary

Improvement
-----------

* FUSEX: When the unmount handler catches a signal, re-throw in the same thread
  so that abort handler print a meaningful trace


``v4.4.22 Citrine``
===================

2019-01-24

Bug
----

* [EOS-3231] - Update is not anymore implicit in ACL:w permissions
* [EOS-3215] - drainstatus not reseted when disk put back to rw
* [EOS-3227] - Missing eosarch python module
* [EOS-3230] - CmdHelper does not always print error stream as provided by the MGM


``v4.4.21 Citrine``
===================

2019-01-21

Bug
----

* [EOS-3203] - recycle config --size
* [EOS-3204] - CLI: "eos acl" is broken
* [EOS-3205] - Problem with the draining of zero size file
* [EOS-3209] - central draining fails on paths containing question marks ('?')


Improvement
------------

* [EOS-2678] - converter/groupbalancer "recycles" files found in recycle-enabled directories


``v4.4.20 Citrine``
===================

2019-01-17

Bug
----

* [EOS-3202] - Instance degradation due to client concurrancy and quota refresh
* MGM: Improve drain source selection by giving priority to replicas of files on other
  file systems rather than the one currently being drained.
* [EOS-3198] - Json output from the httpd interface escapes redundant double
  quotes on values of attr queries
* [EOS-1733] - eosd segfault in unlink around "fileystem::is_toplevel()"

Improvement
------------

* [EOS-3197] - Improve directory rename/move inside the same quota node
* MGM: Add command to control the number of threads used in the central draining:
  eos ns max_drain_thread <num>
* MGM: Add support for ACLs for single files


``v4.4.19 Citrine``
===================

2018-12-18

Bug
----

* FUSEX: fix race/dead-lock condition when create and delete are racing

Improvements
------------

* FUSEX: Put 256k as file start cache size
* FUSEX: Add ignore-containerization flag
* MGM: Refactor and add unit tests to the Access method
* UNIT_TEST: Add quarkdb unit tests to the Gitlab pipeline
* MGM/MQ: Various improvements and fixes to the QuarkDB master-slave setup
* MGM: Various improvements and refactoring of the WFE functionality related
       to CTA.


``v4.4.18 Citrine``
===================

2018-12-07

Bug
----

* [EOS-2636] - VERY high negative cache value = 1987040
* [EOS-2969] - central drain/config: "eos fs config XYZ configstatus=drain" hangs
* [EOS-2974] - EOS new NS (EOSPPS) sudden memory increase → OOM
* [EOS-3129] - Error following symlink while "eos cp"
* [EOS-3162] - File reported successfully written despites IO errors
* [EOS-3163] - FuseServer confuses file ID with inode when prefetching under lock
* [EOS-3168] - "eos recycle config --remove-bin" not working anymore
* [EOS-3170] - Data race in FuseServer when handling client statistics

Improvement
-----------

* [EOS-2923] - Improve and rationalize Egroup class
* [EOS-2968] - central drain/config: skip/ignore attempts to set the same configstatus twice (instead of hanging)
* [EOS-3037] - RFE: draining - randomize order for to-be-drained files on a filesystem
* [EOS-3138] - RPM packaging: depend on the EPEL repo definitions
* [EOS-3153] - Reduce MGM shutdown time
* [EOS-3155] - Write mtime multi-client propagation testsuite
* [EOS-3166] - Allow chown always if the owner does not change


``v4.4.17 Citrine``
===================

2018-11-29

Bug
---

* [EOS-3151] - fix OpenAsync in async flush thread in case of recovery

Improvement
-----------

* Support REFRESH callback to force an update individual metadata records, not only bulk by directory


``v4.4.16 Citrine``
===================

2018-11-28

Bug
---

* [EOS-3137] - Add additional permission check when following a symbolic link in XrdOfsFile::open
* [EOS-3139] - eos chown -r uid:gid follows links
* [EOS-3144] - Cannot auth with unix with fusex
* [EOS-3145] - FUSEX: repeated WARN messages about "doing XOFF"

Improvement
-----------

* [EOS-3050] - Add calling process ID and process name possibly to each client and server side log-entry for FUSE
* [EOS-3096] - Show mount point in 'fusex ls'

``v4.4.15 Citrine``
===================

2018-11-27

Bug
---

* CONSOLE: Add fallback to old style recycle command for old servers
* MGM: Fix possible memory leak in capability generation


``v4.4.14 Citrine``
===================

2018-11-20

Bug
---

* [EOS-3089] - Inflight-buffer exceeds maximum number of buffers in flight
* [EOS-3110] - Looping Open in EOSXD
* [EOS-3114] - corrupted file cache on eosxd in SWAN
* [EOS-3116] - FUSEX-4.4.13 - 'zlib' selftest failure on SLC6
* [EOS-3117] - FUSEX logs "missing quota node for pino=" (and "high rate error messages suppressed")
* [EOS-3121] - MQ: Heap-use-after-free on XrdMqOfsFile::close
* [EOS-3120] - Add eosxd support for persistent kerberos keyrings
* [EOS-3123] - Parsing issue with "eos recycle -m"
* [EOS-3125] - git clone fails with "fatal: remote-curl: fetch attempted without a local repo"
* [EOS-3134] - fix journalcache memory leak

New Feature
-----------

* [EOS-3126] - FUSE: ability to tag traffic with custom tag
* [EOS-3128] - eosxd usability

Improvement
-----------

* [EOS-3108] - Move recycle command to protobuf implementation - keep server support for 'old' clients
* [eos-3113] - Don't stall mount when no read-ahead buffer is available
* [EOS-3119] - Make eosxd auth subsystem more debuggable for users
* [EOS-3120] - Add eosxd support for persistent kerberos keyrings
* [EOS-3122] - Add XrdCl fuzzing
* improve shutdown behaviour of server
* move all pthread to std::thread
* FST no longer sends proto events for sync::closew if file comes from a tape server retrieve operation


``v4.4.13 Citrine``
===================

2018-11-19

Bug
---

* [EOS-3101] - fix EEXIST logic in FuseServer open to race condition and remove double parent lookup

Improvements
------------

* NS: Add metadata-entries-in-flight to NS cache information


``v4.4.12 Citrine``
===================

2018-11-16

Bug
---

* [EOS-2172] - eosxd aborted, apparently due to diskcache missing xattr
* [EOS-2865] - Lost some mount points
* [EOS-3090] - Encoding problems in TPC/Draining
* [EOS-3069] Use logical quota in prop find requests (displayed by CERNBOX client)
* [EOS-3092] Don't require an sss keytable for a fuse mount if 'sss' is not configured as THE auth protocol to use

Improvements
------------

* [EOS-3095] Fail all write access even from localhost in MGM while booting -
  properly tag RO/WR access in proto buf requests
* [EOS-3091] allow to ban eosxd clients (=> EPERM)
* [EOS-3047] add defaulting routing to recycle command
* Refactor fsctl includes into functions
* enable eosxd authentication in docker container

New Feature
-----------

* [EOS-3094] - Access to eos in a container


``v4.4.11 Citrine``
===================

2018-11-14

Bug
---

* [EOS-3044] Fusex quota update blocks the namespace
* [EOS-3065] Ubuntu/Debian packaging: "/etc/fuse.conf.eos" conflicts between "eos-fuse" and "eos-fusex"
* [EOS-3079] MGM Routing Macro should stop bouncing clients to same targets if the target was already tried
* [EOS-3068] fix to catch missing exception in find, avoid FUSE client heartbeat waiving creating DOS
* [EOS-3054] add missing '&' separator in deletion reports
* [EOS-3052] fix typo in report log description
* [EOS_3048] create group readable reports directory structure
* [EOS-3045] fix wrong heart-beat interval logic creating tight-loops and default to 0.1Hz
* [EOS-3043] avoid creating .xsmap files
* [EOS-3041] add timeout to query in SendMessage, add timeout to open and stat requests
* [EOS-3033] fix wrong etag in JSON fileinfo response
* [EOS-3029] disable backward stacktrace in eosd by default possibly creating SEGVs when a long standing mutex is discovered
* [EOS-3025] fix checksum array reset in Commit operation
* [EOS-2989] take fsck enable intereval into account
* [EOS-2872] modify mtime modification in write/truncate/flush to preserve the order of operations in EOSXD
* [EOS-2599] fix ACLs by key and fully supported trusted and signle ID shared sss mounts supporting endorsement keys
* [CTA-312]  propagate protobuf call related errors messages through back to clients
* Don't call 'system' implying fork in FST code
* Fix Fmd object constructor to use 64-bit file ids

Improvements
------------

* [EOS-3073] auto-scale IO buffers according to available client memory
* [EOS-3072] add number of open files to the eosxd statistics output
* [EOS-3027] allow 'fusex evict' without calling abort handler by default e.g.
  to force a client mount with a newer version
* [EOS-2576] add support for clientDNs formatted according to RFC2253
* FUSEX: Add client IO counter and rates in EOSXD stats file and 'fusex ls -l' output
* FUSEX: Manage the negative cache actively from eosxd - saves many remote
  lookups in case of unfound libraries in library lookup path on fuse mount
* FUSEX: Improve tracebility in FuseServer logging to log by client credential
  (remove the _static_ log entries)
* Support deny ACL entries, RICHACL_DELETE from parent
* CTA: Rename tape gc variable names
* FST: Use RAII for XrdCl::Buffer response objects in FST code


``v4.4.10 Citrine``
===================

2018-10-25

Bug
---

* [EOS-2500] fix shutdown procedure which might send a kill signal to process id=1 when the watchdog becomes a zombie process
* [EOS-3015] deal with OpenAsync timeouts in the ioflush thread
* [EOS-3016] Properly handle URL sources (eg.: starting with root://) in eos cp
* [EOS-3021] Make function executed by thread noexcept so that we get a proper stack if it throws an exception
* [EOS-3022] Use uint64_t for storing file ids in the archive command
* fixes for file ids > 2^31 (int->long long in FST)


Improvements
------------

* update file sizes for ongonig writes in eosxd by default every 5s and as long as the cap is valid

``v4.4.9 Citrine``
==================

2018-10-22

Bug
---

* [EOS-2947] - MGM crash near eos::HierarchicalView::findLastContainer
* [EOS-2981] - DrainJob destructor: Thread attempts to join with itself
* [EOS-3009] - -checksum argument of fileinfo not supported anymore
* MGM: Fix master-slave propagation of container metadata


``v4.4.8 Citrine``
==================

2018-10-19

Bug
---

* [EOS-3001] - fix clients seeing deleted CWDs after few minutes


``v4.4.7 Citrine``
==================

2018-10-18

Bug
---

* [EOS-2992],[EOS-2994],[EOS-2967] - clients shows empty file list after caps expired
* [EOS-2997] - GIT usage broken since hard-links are enabled by default

``v4.4.6 Citrine``
==================

2018-10-10

Bug
---

* [EOS-2816] - eos cp issues
* [EOS-2894] - FUSEX: "xauth -q -" gets stuck in "D" state
* [EOS-2992] - aiadm: Lost all files in EOS home
* FUSEX: Various fixes


Task
----

* [EOS-2988] - Login hangs forever (with HOME=/eos/user/l/laman)


``v4.4.5 Citrine``
==================

2018-10-10

Bug
---

* [EOS-2931] - Operation confirmation value isn't random
* [EOS-2962] - table in documentation badly displayed on generated website
* [EOS-2964] - Heap-use-after-free on new master / slave when booting
* [EOS-2970] - "fs mv" not persisted in config file
* MGM: Disable by default the QdbMaster implementation and use the env variable
    EOS_USE_QDB_MASTER to enable it when the QDB namespace is used
* MGM: Enable broadcast before loading the configuration in the QdbMaster so
    that the MGM collects broadcast replies from the file systems
* MGM: Fix possible deadlock at startup when a file system needs to be put
    in kDrainWait state during configuration loading
* MGM: Various improvements to the shutdown procedure for a clean exit
* MQ: Fix memory leak of RSA Objects

Improvement
------------

* [EOS-2901] - RFE: "slow" lock debug - print more info on single line, or disable printing?
* [EOS-2966] - FUSEX: hardcode RPM dependency on 'zeromq'


``v4.4.4 Citrine``
==================

2018-10-09

Bug
----

* [EOS-2951] - FST crashes while MGM is down
* MGM: Fix find crash when a broken symlink exists along side a directory with
  the same name
* MGM: Fix creation of directories that have the same name as a broken link

Improvement
-----------

* MGM: Improve shutdown of the MGM and cleanup of threads and resources


``v4.4.3 Citrine``
==================

2018-10-04

Bug
----

* [EOS-2944] - Central Drain Flaws
* [EOS-2945] - Disks ends up in wrong state with leftover files when central drain is active
* [EOS-2946] - slave mq seen as down by the master MGM

Improvement
-----------

* [EOS-2940] - Error message if wrong params for 'eos file info'


``v4.4.2 Citrine``
==================

2018-10-03

Bug
----

* FST: Fix populating the vector of replica URL which can lead to a crash


``v4.4.1 Citrine``
==================

2018-10-03

Bug
----

* [EOS-2936] - configuration file location change
* [EOS-2937] - eossync does not cope with the change in the config path
* MGM: Fix http port used for redirection to the FSTs


``v4.4.0 Citrine``
==================

2018-10-02

Bug
----

* [EOS-1952] - eosd crash in FileAbstraction::WaitFinishWrites
* [EOS-2743] - "eosd" segfault .. error 4 in libpthread-2.17.so[...+17000]
* [EOS-2801] - Heap-use-after-free in LayoutWrapper::WaitAsyncIO
* [EOS-2836] - Sain file cannot be downloaded when one FS is not present
* [EOS-2914] - git repo on EOS corruption
* [EOS-2922] - eos-server.el6 package requires /usr/bin/bash (not provided by any package in SLC6)
* [EOS-2926] - MGM deadlock due to fusex capability delete operation
* [EOS-2930] - Core dump in rename path sanity check
* [EOS-2933] - createrepo fails on large repo

New Feature
------------

* [EOS-2928] - FUSEX interference from user deletion and generic removal protection (g:z5:!d)

Task
----

* [EOS-2721] - UNIX permissions not propagated to the slave (until a slave restart or failover)

Improvement
------------

* [EOS-2696] - eosarchived systemd configuration
* [EOS-2799] - eosdropboxd: document, add "--help", "-h" options -- or hide outside of default path
* [EOS-2853] - Make background scan rate configurable like scaninterval
* [EOS-2906] - Add "fstpath" to the message written in MGM's report log
* [EOS-2921] - Support client defined LEASE times

User Documentation
-------------------

* [EOS-1723] - Instruction how to migrate to quarkdb namespace


``v4.3.14 Citrine``
===================

2018-09-26

Bug
---

* [EOS-2759] - FST crash on NULL value for stat.sys.keytab, right after machine boot
* [EOS-2821] - FST has lots of FS' stuck in "booting" state
* [EOS-2904] - eos-client: manpages empty/missing on SLC6
* [EOS-2912] - FuseServer does not update namespace store after addFile
* [EOS-2913] - "newfind --count" displays empty lines for each entry found
* [EOS-2916] - Missing server side check for inode quota and wrong eosxd client behaviour
* [EOS-2917] - Central draining crash ?

Task
-----

* [EOS-2832] - FST aborts (coredump) if it cannot launch a transferjob ("Not able to send message to child process")


``v4.3.13 Citrine``
===================

2018-09-19

Bug
---

* [EOS-2892] - FUSE: Initialize XrdSecPROTOCOL before issuing kXR_query to check MGM features
* [EOS-2895] - MGM: fix locking when waiting for a booted namespace
* [EOS-2989] - MGM: Fix queueing logic in Egroup class
* fix wrong checksum validation for chunked OC uploads from the secondary replicas
* let FUSEX writes fail after 60s otherwise we can get stuck pwrite calls/hanging forever


``v4.3.12 Citrine``
===================

2018-09-13

Bug
---

* [EOS-2793] - removexattr fails to remove attribute from mgm metadata
* [EOS-2800] - Relocate check for sys.eval.useracl from fuse client to the Fuseserver
* [EOS-2850] - avoid directory move into itself when going via symlinks
* [EOS-2870] - faulty scheduling on offline machine (regression)
* [EOS-2873] - fix chmod/chown behaviour on executing EOSXD client
* [EOS-2874] - fix 'adjustreplica' for files continaing an '&' sign
* Thread sanitizer fixes in EOSXD
* Fix snooze time in WFE

Improvements
------------

* Default fd limit for shared EOSXD mounts is now 512k
* Don't open journals for file reads in EOSXD ( divides by 2 number of fds)
* Add 'fs dropghosts <fsid>' call to get rid of illegal entries in filesystem view without any corresponding meta data object (undrainable filesystems)
* Use filesystem name as default cache subdirectory in EOSXD (not default)
* Improve locking in EOSXD notification path - release ns mutex in most places before notifying - add timing counters to all EOSXD counters


``v4.3.11 Citrine``
===================

2018-09-05

Bug
---

* MGM: Fix slots leak of proc commands for which the initial client disconnected
  before receiving the response
* MGM/FUSE: Add support for all possible encodings between EOSXD and MGM
* FUSEX: Fix stack corruption when doing recovery and remove leaking proxy object
  after recovery
* FUSEX: Add 'sss' as a possible authentication scheme for eosxd

Improvements
------------

CI: Add script for promoting tag releases from the testing to the stable repo


``v4.3.10 Citrine``
===================

2018-08-31

Bug
---

* [EOS-2138] - Handling of white spaces in eos commands
* [EOS-2722] - filR state not propagated to parent branches in a snapshot
* [EOS-2787] - Fix filesystem ordering for FUSE file creation by geotag, then fsid
* [EOS-2838] - WFE background thread hammering namespace, running find at 100 Hz
* [EOS-2839] - Central draining is active on slave MGM
* [EOS-2843] - FUSEX crash in metad::get(), pmd=NULL.
* [EOS-2847] - FUSEX: Race between XrdCl::Proxy destructor and OpenAsyncHandler::HandleResponseWithHosts
* [EOS-2849] - Memeory Leaks in FST code

Task
----

* [EOS-2825] - FUSEX (auto-)unmount not working?

Improvement
-----------

* [EOS-2852] - MGM: hardcode RPM dependency on 'zeromq'
* [EOS-2856] - EOSXD marks CWD deleted when invalidating a CAP subscription


``v4.3.9 Citrine``
==================

2018-08-23

Bug
---

* [EOS-2781] - MGM crash during WebDAV copy
* [EOS-2797] - FUSE aborts in LayoutWrapper::CacheRemove, ".. encountered inode which is not recognized as legacy"
* [EOS-2798] - FUSE uses inconsistent datatypes to handle inodes
* [EOS-2808] - Symlinks on EOSHOME have size of 1 instead of 0
* [EOS-2817] - eosxd crash in metad::cleanup
* [EOS-2826] - Cannot create a file via emacs on EOSHOME topdir
* [EOS-2827] - log/tracing ID has extra '='


``v4.3.8 Citrine``
==================

2018-08-14

Bug
---

* [EOS-2193] - Eosd fuse crash around FileAbstraction::GetMaxWriteOffset
* [EOS-2292] - eosd crash around "FileAbstraction::IncNumOpenRW (this=0x0)"
* [EOS-2772] - ns compact command doesn't do repairs
* [EOS-2775] - TPC failing in IPV4/6 mixed setups
* Fix quota accounting for touched files


New Feature
-----------

* [EOS-2742] - Add reason when we change the status for file systems and node


``v4.3.7 Citrine``
==================

2018-08-07

Bug
---

* Fix possible deadlock when starting the MGM with more than the maximum allowed
  number of draining file systems per node.


``v4.3.6 Citrine``
==================

2018-08-06

Bug
---

* [EOS-2752] - FUSE: crashes around "blockedtracing" getStacktrace()
* [EOS-2758] - SLC6 FST crashes on getStacktrace()

Task
----

* [EOS-2757] - The 4.3.6 pre-release generates FST crashes (SEGFAULT)

Improvement
-----------

* [EOS-2753] - Logging crashing


``v4.3.5 Citrine``
==================

2018-07-26

Bug
---

* [EOS-2692] - Lock-order-inversion between FsView::ViewMutex and ConfigEngine::mMutex
* [EOS-2698] - XrdMqSharedObjectManager locks the wrong mutex
* [EOS-2701] - FsView::SetGlobalConfig corrupts the configuration file during shutdown
* [EOS-2718] - Commit.cc assigns zero-sized filename during rename, corrupting the namespace queue
* [EOS-2723] - user.forced.placementpolicy overrules sys.forced.placementpolicy
* Fix S3 access configuration not getting properly refreshed

Improvement
-----------

* [EOS-2691] - FUSEX abort in ShellException("Unable to open stdout file")
* [EOS-2684] - Allow uuid identifier in 'fs boot' command
* [EOS-2679] - Display xrootd version in 'eos version -m' and 'node ls --sys' commands
* Documentation for setting up S3 access [Doc > Configuration > S3 access]
* More helpful error messages for S3 access

``v4.3.4 Citrine``
==================

2018-07-04

Bug
---

* [EOS-2686] - DrainFs::UpdateProgress maxing out CPU on PPS
* Fix race conditions and crashes while updating the global config map
* Fix lock order inversion in the namespace prefetcher code leading to deadlocks

New feature
-----------

* FUSEX: Add FIFO support

Improvement
-----------

* Remove artificial sleep when generating TPC drain jobs since the underlying issue
  is now fixed in XRootD 4.8.4 - it was creating identical tpc keys.
* Replace the use of XrdSysTimer with std::this_thread::sleep_for


``v4.3.3 Citrine``
==================

2018-06-29

Improvement
-----------

* FUSEX: Fix issues with the read-ahead functionality
* MGM: Extended the routing functionality to detect online and master nodes with
  automatic stalling if no node is available for a certain route.
* MGM: Fix race condition when updating the global configuration map


``v4.3.2 Citrine``
==================

2018-06-26

Bug
---

* FUSEX: encode 'name' in requests by <inode>:<name>
* MGM: decode 'name' in requests by <inode>:<name>
* MGM: decode routing requests from eosxd which have an URL encoded path name


``v4.3.1 Citrine``
==================

2018-06-25

Bug
---

* FUSEX: make the bulk rm the default
* FUSEX: by default use 'backtace' handler, fusermount -u and emit received signal again.
* FUSEX: use bulk 'rm' only if the '-rf' flag and not verbose option has been selected
* FUSEX: avoid possible dead-lock between calculateDepth and invalidation callbacks


``v4.3.0 Citrine``
==================

2018-06-22

Bug
---

* [EOS-1132] - eosarchived.py, write to closed (log) file?
* [EOS-2401] - FST crash in eos::fst::ScanDir::CheckFile (EOSPPS)
* [EOS-2513] - Crash when dumping scheduling groups for display
* [EOS-2536] - FST
* [EOS-2557] - disk stats displaying for wrong disks
* [EOS-2612] - Probom parsing options in "eos fs ls"
* [EOS-2621] - Concurrent access on FUSE can damage date information (as shown by ls -l)
* [EOS-2623] - EOSXD loses kernel-md record for symbolic link during kernel compilation
* [EOS-2624] - Crash when removing invalid quota node
* [EOS-2654] - Unable to start slave with invalid quota node
* [EOS-2655] - 'eos find' returns different output for dirs and files
* [EOS-2656] - Quota rmnode should check if there is quota node before deleting and not afater
* [EOS-2659] - IO report enabled via xrd.cf but not collecting until enabled on the shell
* [EOS-2661] - space config allows fs.configstatus despite error message

New Feature
-----------

* [EOS-2313] - Add queuing in the central draining


Improvement
-----------

* [EOS-2297] - MGM: "boot time" is wrong, should count from process startup
* [EOS-2460] - MGM should not return
* [EOS-2558] - Fodora 28 rpm packages
* [EOS-2576] - http: x509 cert mapping using legacy format
* [EOS-2589] - git checkout slow
* [EOS-2629] - Make VST reporting opt-in instead of opt-out
* [EOS-2644] - Possibility to configure #files and #dirs on MGM with quarkdb


``v4.2.26 Citrine``
===================

2018-06-20

Bug
---

* [EOS-2662] - ATLAS stuck in stacktrace due to SETV in malloc in table formatter
* [EOS-2415] - Segmentation fault while building the quota table output


``v4.2.25 Citrine``
===================

2018-06-14

Bug
---

* Put back option to enable external authorization library


``v4.2.24 Citrine``
===================

2018-06-13

Bug
----

* [EOS-2081] - "eosd" segfault in sscanf() / filesystem::stat() / EosFuse::lookup
* [EOS-2600] - Clean FST shutdown wrongly marks local LevelDB as dirty

New Feature
-----------

* Use std::shared_timed_mutex for the implementation of RWMutex. This is by default disabled and can be enabled by setting the EOS_USE_SHARED_MUTEX=1 environment var.

Improvement
-----------

* The FSTs no longer do the dumpmd when booting.


``v4.2.23 Citrine``
===================

2018-05-23

Bug
----

* [EOS-2314] - Central draining traffic is not tagged properly
* [EOS-2318] - Slave namespace failed to boot (received signal 11)
* [EOS-2465] - adding quota node on the master kills the slave (which then bootloops trying to apply the same quota)
* [EOS-2537] - Balancer sheduler broken
* [EOS-2544] - Setting recycle bin size changes inode quota to default.
* [EOS-2564] - CITRINE MGM does not retrieve anymore error messages from FSTs in error.log
* [EOS-2574] - enabling accounting on the slave results in segfault shortly after NS booted
* [EOS-2575] - used space on /eos/<instance>/proc/conversion is ever increasing
* [EOS-2579] - Half of the Scheduling groups are selected for  new file placement
* [EOS-2580] - 'find -ctime' actually reads and compares against 'mtime'
* [EOS-2582] - Access command inconsistencies
* [EOS-2585] - EOSFUSE inline-repair not working
* [EOS-2586] - The client GEOTAG is not taken into account when performing file placement

New Feature
------------

* [EOS-2566] - Enable switch to propagate uid only via fuse

Task
----

* [EOS-2119] - Implement support in central drain for RAIN layouts + reconstruction
* [EOS-2587] - Fix documentation for docker deployment

Improvement
-----------

* [EOS-2462] - improve eos ns output
* [EOS-2571] - Change implementation of atomic uploads`
* [EOS-2588] - Change default file placement behaviour in case of clients with GEOTAG


``v4.2.22 Citrine``
===================

2018-05-03

Bug
----

* [EOS-2486] - eosxd stuck, last message "recover reopened file successfully"
* [EOS-2512] - FST crash around eos::fst::XrdFstOfsFile::open (soon after start, "temporary fix"?)
* [EOS-2516] - "eosd" aborts with std::system_error "Invalid argument" on shutdown (SIGTERM)
* [EOS-2519] - Segmentation fault when receiving empty opaque info
* [EOS-2529] - eosxd: make renice =setpriority() optional, req for unprivileged containers
* [EOS-2541] - (eosbackup halt): wrong timeout and fallback in FmdDbMapHandler::ExecuteDumpmd
* [EOS-2543] - Unable to read 0-size file created with eos touch

New Feature
-----------

* [EOS-1811] - RFE: support for "hard links" in FUSE
* [EOS-2505] - RFE: limit number of inodes for FUSEX cache, autoclean
* [EOS-2518] - EOS WfE should log how long it takes to execute an action
* [EOS-2542] - Group eossync daemons in eossync.target

Improvement
-----------

* [EOS-2114] - trashbin behaviour for new eos fuse implementation
* [EOS-2423] - EOS_FST_NO_SSS_ENFORCEMENT breaks writes
* [EOS-2532] - Enable recycle bin feature on FUSEX
* [EOS-2545] - Report metadata cache statistics through "eos ns" command

Question
--------

* [EOS-2458] - User quota exceeted and user can write to this directory
* [EOS-2497] - Repeating eos fusex messages all over

Incident
--------

* [EOS-2381] - File lost during fail-over ATLAS


``v4.2.21 Citrine``
===================

2018-04-18

Bug
----

* [EOS-2510] - eos native client is not working correctly against eosuser

New
----

* XrootD 4.8.2 readiness and required

``v4.2.20 Citrine``
===================

2018-04-17

Improvements
------------

FST: make the connection pool configurable by defining EOS_FST_XRDIO_USE_CONNECTION_POOL
FUSE: avoid that FUSE calls open in a loop for every write in the outgoing write-back cache if the file open failed
FUSE: remove 'dangerous' recovery functionality which is unnecessary with xrootd 4
FUSE: Try to re-use connections towards the MGM when using the same credential file


``v4.2.19 Citrine``
===================

2018-04-10

Bug
----

* [EOS-2440] - `eos health` is broken
* [EOS-2457] - EOSPPS: several problems with `eos node ls -l`
* [EOS-2466] - 'eos rm' on a file without a container triggers an unhandled error
* [EOS-2475] - accounting: storagecapacity should be sum of storageshares

Task
----

* [EOS-1955] - .xsmap file still being created (balancing? recycle bin?), causes "corrupted block checksum"


``v4.2.18 Citrine``
===================

2018-03-20

Bug
----

* [EOS-2249] - Citrine generation of corrupted configuration
* [EOS-2288] - headroom is not propagated from space to fs
* [EOS-2334] - Failed "proto:" workflow notifications do not end up in either the ../e/.. or ../f/.. workflow queues
* [EOS-2360] - FST aborts with "pure virtual method called", "terminate called without an active exception" on XrdXrootdProtocol::fsError
* [EOS-2413] - Crash while handling a protobuf reply
* [EOS-2419] - Segfault around TableFormatter (when printing FSes)
* [EOS-2424] - proper automatic lock cleanups
* [EOS-2428] - draining jobs create .xsmap files on the source and destination FSTs
* [EOS-2429] - FuseServer does not grant SA_OK permission if ACL only allows to be a writer
* [EOS-2432] - eosapmond init script for CC7 sources /etc/sysconfig/eos
* [EOS-2433] - Wrong traffic accounting for TPC/RAIN/Replication
* [EOS-2436] - FUSEX: permission problem in listing shared folder
* [EOS-2438] - FUSEX: chmod +x does not work
* [EOS-2439] - FUSEX: possible issue with sys.auth=*
* [EOS-2442] - TPC of 0-size file fails

Improvement
-----------

* [EOS-2423] - EOS_FST_NO_SSS_ENFORCEMENT breaks writes
* [EOS-2430] - fusex cache should not use /var/eos

Question
--------

* [EOS-2431] - fusex cache cleanup


``v4.2.17 Citrine``
===================

2018-03-15

Bug
---

* [EOS-2292] - eosd 4.2.4-1 segmentation fault in SWAN
* [EOS-2322] - eosd 4.2.4-1 segmentation fault on swan003
* [EOS-2388] - Fuse::utimes only honours posix permissions, but not ACLs
* [EOS-2402] - FST abort in eos::fst::FmdDbMapHandler::ResyncAllFromQdb (EOSPPS)
* [EOS-2403] - eosd 4.2.4-1 SegFaults on swan001
* [EOS-2404] - eosd 4.2.4-1 SegFaults on swan002

Improvement
-----------

* [EOS-2389] - Classify checksum errors during scan
* [EOS-2398] - Apply quota settings relativly quick in time on the FUSEX clients
* [EOS-2408] - Proper error messages for user in case of synchronous workflow failure


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
--------

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
* [EOS-1609] - eos -b problem : \*\*\* Error in `/usr/bin/eos: free():`


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
