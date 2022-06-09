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

* FUSEX: never keep the deletion mutex when distroying an upload proxy because
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
