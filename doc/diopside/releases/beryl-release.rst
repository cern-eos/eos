:orphan:

.. highlight:: rst

.. index::
   pair: Releases; Beryl
   


Beryl Release Notes
===================

``V0.3.270 Aquamarine``
=======================

New Features
============

- MGM: add the 'eos fusex' interface and new FUSE client server side support (beta status - be careful with using this)
- MGM/CONSOLE: add new 'access allow|unallow|ban domain <domain>'
- NS: add hopscotch map/hash

Bug Fixes
+++++++++

- NS: use murmurhash as string hash function
- COMMON: fix dead-lock im common/Mapping.cc

``V0.3.268 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: Mask the block checksum for draining and balancing when there is a layout
       requesting blockchecksum for replica files. This was blocking all draining,
       balancing or conversion jobs.

``V0.3.267 Aquamarine``
=======================

Bug Fixes
+++++++++

- AUTH: Set the ZMQ_LINGER option on the socket so that messages are not retransmitted
- NS: add missing initialization of pData leading to random compaction crashes/failures
- MGM: fix race in mkdir which could return EEXIST
- MGM: fix race in rm 
- FUSE: fix memory-leak when read-ahead gets disabled during an open/read/close sequence

Improvement
+++++++++++

- MGM: Return ENETUNREACH in case no diskservers are available (implies different client behavior)
- MGM: allow recursive deletes for the http bridge using XrdOfs::remdir with ?mgm.option=r
- MGM: add two new space variables to modify scheduler behaviour
       "space.scheduler.skip.overloaded=off" - by default we don't skip anymore overloaded eth-out nodes)
       "space.min.weight=0.1" - the minimum probability to select an disk or eth-out overloaded node
- MGM: Collect response time statistics for the authentication front-ends
- MGM: make the recycle bin work with symbolic links


``V0.3.266 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: avoid recreating block-xs files in balancing and draining due to wrong mask used
- MGM: avoid increasing number of replicas when balancing very empty groups
- AUTH: Avoid replay of requests for ZMQ sockets which are deleted. This avoid the 0-size
  files in the namespace bug.

``V0.3.265 Aquamarine``
=======================

Bug Fixes
+++++++++
- Fix issue in EOSATLAS where files where disappearing from the namespace after being confirmed
  to the client. This is correlated which exceptionally long scheduling times (~ 5min). This in
  turn is due to the scheduling not finding a suitable node to place the file. When this happens
  the default XRootD client will try to recover the initial open requests and this leads to a
  race condition.
- [EOS-1948] - FST crash with "terminate called after throwing an instance of 'std::bad_alloc'"
- [EOS-1949] - Strange correlated crash in EOSATLAS

Improvement
+++++++++++
- [EOS-1947] - Improve error message when trying to delete a directory attached to a quota node


``V0.3.264 Aquamarine``
=======================

Bug Fixes
+++++++++

- [EOS-1936] - EOS ATLAS lost file due to balancing
- ARCHIVE: Fix archive endpoint which was constructed only if the MGM node was a master.
           This approach fails when we have a master slave failover as we never set up
           the archive endpoint for the slave. Use the same ZMQ contect for both the
           archive and authentication services.
- FUSE: Make configurable the maximum number of retries in case a synchronous
        open operation fails.
- DOC: update documentatino of wfe's


``V0.3.263 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: re-establish 2nd path of 'deleteOnClose' functionality broken since 0.3.295

``V0.3.262 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: fix computation of wake-up time for the recycle bin - old code slept too long before waking up

``V0.3.261 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: re-establish 'deleteOnClose' functionality broken since 0.3.295



``V0.3.260 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: call 'unlinkAllLocations' instead of 'clearLocations' when trying to re-place an empty already placed file, which didn't reomve entries from the filesystem view leaving files forever undrainable

``V0.3.259 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: Don't drop a file if an FST calls a drop replica on a not committed replica


``V0.3.258 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: Protect if the namespace throws an exception without setting an error number in the readlink functionality


``V0.3.257 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: protect against 0 pointer access if not local fmd is available for a scanned file

``V0.3.256 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM/CONSOLE: revive 'file layout' command and 'find -layoutstripes'

``V0.3.255 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: treat attributes not prefixed as sys. like user. attributes (don't allow to set them if we are not the object owner)
- MGM: many bug fixes/improvements in the AUTH service


``V0.3.248 Aquamarine``
=======================

Bug Fixes
+++++++++

- MGM: fix recycle bin restore function to forbid to recycle files by fxid/pxid which are not in the recycle bin. Allow to explicitly restore a file or directory (they might overlap in the inode space) by prefixing the key with fxid: or pxid:


``V0.3.246 Aquamarine``
=======================

Bug Fixes
+++++++++

- FUSE: fix shutdown crash by properly canceling/joining the cache cleaner thread
- NS: fix gcc 4.4. compilation problem
- MGM: reschedule empty files if current replicas are unavailable
- MGM: add authentication front-end (backport from CITRINE)


``V0.3.244 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: don't block Fmd access for an unitialized filecxerror value (after Resync was called and filecxerror=-1)


``V0.3.243 Aquamarine``
=======================

Bug Fixes
+++++++++

- NS: fix memory allocation bug in Buffer class


``V0.3.242 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: fix logical error when to call auto-repair (don't call it for unregsistered files)
- FUSE: fix double response when returning entries from internal directory cache
- MGM: fix protection when listing too large recycle bins with 'recycle ls' (> 1Gb output)

``V0.3.241 Aquamarine``
=======================

Bug Fixes
+++++++++

- FUSE: fix memory leak in opendir function not cleaning dirbuf struct

``V0.3.240 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: implement fdellocate function for non-XFS detected filesystems (which used posix_fallocate)

``V0.3.239 Aquamarine``
=======================

Bug Fixes
+++++++++

- NS: fix resolution of multiple ../ path changes like ../../XYZ
- COMMON: fix resolution of multipeo ../ path changes like /X/Y/Z/../../Z

``V0.3.238 Aquamarine``
=======================

Bug Fixes
+++++++++

- FST: avoid SEGV during startup when calling RemoveGhostEntries (.eosscan exists on data path)

``V0.3.237 Aquamarine``
=======================

Bug Fixes
+++++++++

- NS: fix slave follower attachment issue leading to invisible files
- MGM: fix the logic when to show a slave as booted

New Feature
+++++++++++

- NS: add 'pending' counter to show if there are updates on the slave, which cannot be attached
- NS: show follower progress during the initial scan phase and not only after


``V0.3.236 Aquamarine``
=======================

Bug Fixes
+++++++++

- NS: set 'pData' pointer to 0 in munmap function to switch back to traditional read function

``V0.3.235 Aquamarine``
=======================

New Feature
+++++++++++

- NS: compile with devtoolset-2 on SLC6
- NS: make part of boot process parallel (gain 3-6x in boottime) [ enable with export EOS_NS_BOOT_PARALLEL=1 ]
- NS: mmap changelog files during first scan phase to avoid performance limitation by too many syscalls [ disable with export EOS_NS_BOOT_NOMMAP=1 ]
- NS: implement pread function for namespace file following using read-ahead caching to avoid too many syscalls
- NS: allow to disable CRC32 on boot (e.g. when using BTRS/ZFS) [ enable with export EOS_NS_BOOT_NORCRC32=1 ]
- NS: use murmurhash3 for the main flat indexes avoiding serious performance degradation for high id's in google::dense_hash_map
- NS: make treesize and tree modification time atomic variables if gcc >=4.8
- FST: limit 'file open for writing' messages in Verify to once per minute
- FST: limit 'writer error' message to only once per open/write/close file sequence
- COMMON: add generic lambda function to run parallel for loops Parallel::For ()
- UTILS: add yum packages to install devtoolset-2 to compile with gcc 4.8

New documentation of namespace variables: http://eos.readthedocs.io/en/latest/configuration/namespace.html

Bug Fixes
+++++++++

- NS: fix various bugs in slave follower losing directories, not showing proper treesize aso.
- NS: start 'eossync' in slave2master transition
- MGM: avoid Converter::ResetMasterJobs on slaves
- MGM: don't run slaves in auto-repair mode when scanning the changelog file
- FUSE: fix 'bad address' errors and show proper 'permission denied' messages when a client has not credential or is forbidden to talk to certain EOS instances
- CONSOLE: fix 'treesize' output in 'fileinfo'


``V0.3.234 Aquamarine``
=======================

- NS: avoid that the main indexes ever shrink
- MGM: don't follow symlinks when stating recycle bin entries
- FUSE/FST: add read-ahead cache consistency to FUSE client and make kernel cache invalidation work properly
- FST: allow to define the network speed via an environment variable since 'ip route' and ethtool are not equivalent on SLC6/EL7

``V0.3.233 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: remove falsely committed debug return statement disabling stale cache file detection from previous fix
- FST: extending '.eosscan' functionality to cleanup ghost entries which are neither on disk or memory but can normally only be removed by wiping the local database and rebuild from scratch

``V0.3.232 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix stale kernel cache contents problem if file contents changed but not the file size
- FUSE: fix stale directory/file attributes for lookup/getattr of cached files/directories (apply attr lifetime)
- FST: avoid to try to call forever an old master in commit/drop calls which specified an explicit call-back manager - use the broadcasted MGM name after 60 attempts

``V0.3.231 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: stall/redirect access by fid:fxid before trying to translate to a real path (can crash boot procedure)

``V0.3.230 Aquamarine``
=======================

Bug Fix
+++++++

- FST: deal with unregistered files with the correct replica count in the same way as with orphans when .eosscan is enabled on an FST mount

>>>>>>> beryl_aquamarine
``V0.3.229 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix bug introduced with retry 'query' mechanism doing double deletes
- FUSE: fix bug in AuthId manager doing a double lock when session id != process id
- FUSE: set the link count for files/links to 1 to make applications like gzip work
- MGM: fix subtree accounting in the slave follower
- FST: add an .eosorphans directory to each FST mount point and allow to isolate orphans into this directory by creating a tag file  <mnt>/.eosscan. The .eosscan file removes any smearing and sleep time between scans. The original location is tagged as an exteneded attribute after during the move

``V0.3.228 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix locking strategy bug in the proc cache usage where entries were not locked anymore when used

``V0.3.227 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: fix failover procedure: slave stays forever booting until master sees a change
- MGM: safte in failover procedure: don't failover if the slave did not follow the changelog to the end
- MGM: show bytes left to follow in 'ns master' on slave
- NS: avoid infinite loop in slave follower when looking for a quota node
- FUSE: fix bug leaving files open when a file was inline repaired
- DAV: fix webdav bug when a symbolic link is present in a directory listing leading to an error response
- MGM: fix 'access rm' implementation to remove ENOENT and ENONET redirection
- DAV: take into account sys.owner.auth when looking for webdav quota

``V0.3.226 Aquamarine``
=======================

Bug Fix
+++++++

- COMMON: make ShellExecutor thread/interrupt safe
- FST: reset checksum error flags also after correct 'verify -checksum'
- FUSE: fix ping timeouts and dependencies, allow sss mounts
- NS: remove ns file archiving process by default in SLAVE->MASTER transition and fix too early enabling of the namespace for write

New Feature
+++++++++++

- MGM: add REST API for 'fileinfo'


``V0.3.225 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: fix vulnerability for http GET of '/./' via eos::common::Path
- COMMON: make '/' the full and parent path of /. /.. /./ /../

``V0.3.224 Aquamarine``
=======================

New Feature
+++++++++++

- FST: allow 'eos.checksum=ignore' for file uploads to avoid checksum computation
- FST: fix 'eoscp -a' and add 'eoscp -A <offset>' to upload a file to a certain offset

``V0.3.223 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix foreground option for eosd
- FUSE: shard proc cache to keep memory footprint low for high MAX_PID settings and run AuthId cleanup every 5 minutes
- FUSE: don't pick up root credentials inside eosd
- COMMON: fix syslog logging interface using wrong argument list


``V0.3.222 Aquamarine``
=======================

Bug Fix
+++++++

- MQ: fix race condition multiplexed/non-multiplexed set
- FST: fix race condition in filesystem mutex map
- FUSE: fix wrong default values for query retry sleep time
- MGM: protect scheduling against scheduling in a space without filesystems
- MGM: fix 'fileinfo by inode'

New Feature
+++++++++++

- FUSE: use proc map sharding to avoid too large mutex maps for machines with high max proc ID settings
- FUSE: allow to run eosd as a foreground process when specified in /etc/sysconfig/eos

``V0.3.221 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: don't hold (timeout) HTTP requests during compacting
- FST: fix mutex race condition
- FUSE: fix memory issues and remove unreachable code
- FUSE: avoid SEGV on empty XRootD buffer responses
- FUSE: restructure read-buffer handling and clean-up not used read-buffers in CacheCleanup function - avois significant memory leaking under parallel access

``V0.3.220 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: avoid triggering recreation of xsmap files during draining/balancing for replica layouts
- FUSE/FST: fix 'critical' bug in async write implementation not collecting async writes errors when flush is called and file exceeds the cache size
- FUSE: always wait for asynchronous writes in case of file modifications


Feature
+++++++

- COMMON: allow to duplicate EOS log to syslog via export EOS_LOG_SYSLOG=1


Bug Fix
+++++++

- COMMON/FUSE: fix base64 encoding of not-string buffers
- FUSE: fix memory leak in proc cache
- FUSE: use FORKHANDLER in XrdCl and check mgm before forking the FUSE daemon
- FUSE: fix shutdown behaviour after MGM ping failure
- MGM: fix 'fileinfo' for high inode numbers


``V0.3.218 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix a bug in auth cache when sid process of a calling pid does not exist anymore


``V0.3.217 Aquamarine``
=======================

Bug Fix
+++++++

- FST: cleanup checksum error flags after "file verify -checksum"

``V0.3.216 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: fix OC upload complete condition


``V0.3.215 Aquamarine``
=======================

Bug Fix
+++++++

- ETC: fix typoe introduced by MALLOC_CONF_VARNAME

``V0.3.214 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: fix geobalancer default variable names (were geotagbalancer)

New Feature
+++++++++++

- MGM: bounce checksum & open requests without an attached replica to an alive master
- MGM: add heap profiler

``V0.3.213 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: Fix condition in ShellExecutor leading to deadlock in MGM startup
- TEST: Adapt the eos-instance test give the modifications done to the default "replica" layout i.e. drop of the blockchecksum

``V0.3.212 Aquamarine``
=======================

Bug Fix
+++++++

- FST: Fix race condition in TPC implementation
- FST: convert some critical errors to warnings
- COMMON: add an alarm timer for the ShellExecutor forked process to die on its own if the parent process disappears
- MGM: fix miscounting quote bug when deleteOnClose is triggered
- MGM: fix bug introduced by commit 089803efe0b0cde882ed655788985eb166eb4546  triggering a SEGV under load due to out-of-lock access
- MGM: fix balancer bug which was in case of N full and M empty boxes balancing the M times more from first box instead of all N equally


New Feature
+++++++++++

- FST: add a connection pool to avoid bottleneck due to slow close blocking other opens to the same target FST - the connection pool size is by default 64 and can be changed by the variable EOS_FST_XRDIO_CONNECTION_POOL_SIZE
- MGM: add an environment variable allowing read-write-modify to all all users on MGM for RAIN layouts (define EOS_ALLOW_RAIN_RWM)
- MGM: relax OC chunked upload order restriction - order is irrelevant and retries but the last chunk terminates an upload

``V0.3.211 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: don't set the truncate flag in OpenAsync to avoid increment of inode when async open is done
- NS: fix copy constructor not duplicationg the pTreeSize variable

``V0.3.210 Aquamarine``
=======================

Bug Fix
+++++++

- FST: fix 'ScanDir' funcionality to deal properly with files which get opened during a scan for update and don't flag them as checksum error files
- FST: ignore flagged checksum errors when updating a file

``V0.3.209 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: move from passive cache expiration to active write-back cache cleaen-up (by thread) - the maximum allowed default size of wb-file caches is 512 MB
- MGM: fix acl check if client sends base64 encoded acl values (as EOS 4.X does)
- FST: fix memory and fd leak triggered by deleteOnClose on files with block checksums
- FST: silence "probably already unlinked" message in XrdFstOss::Unlink

``V0.3.208 Aquamarine``
=======================

Bug Fix
+++++++

- FST: enable blockchecksums againf for plain layouts if there is an .xsmap file - this avoids bogus errors and still checks the blockchecksum files if they are available
- CONSOLE: adjust the console command to not add block checksum for "attr set default=replica"

``V0.3.207 Aquamarine``
=======================

Bug Fix
+++++++

- FST: put back the posix_fallocate call since xfs pre-allocation slows down when a truncate is called and produces contention in the Oss::Close handle where xrootd uses a global lock
- COMMON: disable block checksums for plain and replica layouts by force

``V0.3.206 Aquamarine``
=======================

Bug Fix
+++++++

- FST: avoid bogus mgm/disk size errors due to still uninitialized disk size values

``V0.3.205 Aquamarine``
=======================

Bug Fix
+++++++

- FST: avoid double deletion in Fmd code

``V0.3.204 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: protect accessing a 0 pointer in opendir
- FUSE: store all invisble items in the FUSE stat cache although they are not visible in the listing

``V0.3.203 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: refactor opendir/readdir/closedir consistency and directory caching


``V0.3.202 Aquamarine``
=======================

Bug Fix
+++++++

- FST: fix return code handling of xfs pre-allocation in CheckSum.cc


``V0.3.201 Aquamarine``
=======================


Bug Fix
+++++++

- FST: always reset the disk checksum in the meta data db when a file has been modified
- FST: consider only flagged file/blockchecksum errors to prevent to return meta data objects
- FST: set /var partition RO threshold to 95% full
- FUSE: swap lines to avoid valgrind warning about use after erase
- MGM: return json responses with json response tag
- DOC: fix commit message for release number


``V0.3.200 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix out of lock scope iterator used in error message
- FUSE: give no validity to attributes coming as fuse-replies to a create call (since uid/gid can be different on MGM side from uid/gid of the caller)
- FST: prevent deleteOnClose when clients retried an open e.g. open | open | write| close (the XRootD client might replay an open with a new connection and this can lead to file loss)
- FST: switch filesystems to RO when /var parition is 90% full
- FST: make deleteOnClose a warning on client disconnect

``V0.3.199 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix wrong lock scope when readdir buffers are retrieved

``V0.3.198 Aquamarine``
=======================

Bug Fix
+++++++

- HTTP: drop FileClose handler to avoid SEGVs due to inteference between FileClose and Complete handler
- NS: avoid failing compaction when a slave was promoted to be master due to virtual root entry with 0 offset in changelog file
- ARCHIVE: use MGM alias to reference instances in archives
- FST: protect against 0-size buffer response bug in XRootD 3.3.6

New Feature
+++++++++++

- MGM: add some more information about the currently in-use file/container-id and the id's created since last boot
- MGM: allow update of 0-size RAIN files to allow lazy-open with RAIN layouts


``V0.3.197 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: return correct (also overlayed) mode bits after file creation

``V0.3.196 Aquamarine``
=======================

Bug Fix
+++++++

- NS: fix slave follower getQuotaNode exception preventing quota accounting on slave
- FUSE: swap unlock and pool-fd push (which is protected by the same file abstraction rwmutex)


New Feature
+++++++++++

- MGM: add 'Treesize' to the output of 'file info'

``V0.3.195 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix possible size inconsistency after utimes call storing size=0 in kernel cache

New Features
++++++++++++

- TEST: adding eos-fuse-test suite to eos-test RPM (use eos-fuse-test to display individual test categories)

``V0.3.194 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix truncate bug putting a stall file size after truncate into the kernel cache

New Features
++++++++++++

- TEST: add test for truncate bug to eos-fuse-test

``V0.3.193 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: add monitoring switch to space,group status function
- MGM: draing mutex fix and fix double unlock when restarting a drain job
- MGM: fixes in JSON formatting, reencoding of non-http friendly tags/letters like <>?@
- MGM: fix possible lock problem in 'eos find' mgm iplementation
- MGM: fix memory leak in fs.Ping (xrootd3 mem leak)
- MGM: fix bug when revoking sudo priviledges
- MGM: decode all base64 prefixed attr values before storing in attr_set
- MGM: return base64 encoded attributes in attr_get when called via FUSE
- NS:  handled uncatched exception in the slave follower when looking for a quota node
- FST: wait for pending async requests in the close method
- SPEC: remove directory creation scripting from spec files
- FUSE: fix bug in 'setxattr' function
- FUSE: protect against missing response buffer

``V0.3.192 Aquamarine``
=======================

Bug Fix
+++++++

- FST: fix regression from bug fix in 191
- FUSE: fix getxattr return value as ENOATTR if attribute not found


``V0.3.191 Aquamarine``
=======================

Bug Fix
+++++++

- FST: honour (rare) xrootd XOFF send on open to retry after <n> seconds to open a file due to contention on xrootd tables

``V0.3.190 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix memory leak when returning readdir from in-memory cache

New Features
++++++++++++

- FUSE: update SELINUX policies
- FUSE: create /var/run/eosd and /var/log/eos/fuse/ directories in eos-fuse-core
- MGM: allow to change the find query limitations (by default 100k/50k files/dirs) via the 'access' interface. See 'eos access -h'.

``V0.3.189 Aquamarine``
=======================

New Features
++++++++++++

- MGM: add JSONP response object format when 'callback=...' is specified in a query URL

``V0.3.188 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: wake up the recycle thread if there is a change of the recycle policy
- MGM: don't cache unresolved uid/gid with their number, since sssd translation is not 100% successful
- MGM: allow underscore in user/group names (ACL parsing)
- MGM: forward errors from find (like query limitation etc.)
- MGM: don't keep the Stat mutex when translating uid/gids
- MGM: fix slave follower bug when moving a subtree
- MGM: fix recursive accounting on slave
- MGM: resolve symlink when opening a file via non-FUSE clients to resolve to the right quota node
- MGM: fix bug in creation of shared URLs after introduction of URL encoding
- CONSOLE: fix recursive copy bug in eos cp

New Features
++++++++++++

- FUSE: refactor FUSE rpms into eos-fuse-core & eos-fuse-sysv. The core has only mount scripts and not sysv scripts anymore
- FUSE: add SELINUX policies in the eos-fuse-core postinstall script
- MGM: add JSON output formatting for all REST commands

Documentation
+++++++++++++

- WFE: document workflow engine
- REST: document rest api for space, node, group and fs calls

``V0.3.187 Aquamarine``
=======================

- FUSE: forward correct errno from XrdCl::Open failures
- FUSE: fix wrong map deletion when unlink/rmdir fails (visible with rsync  --delay-updates)
- FUSE: add mknod implementation to allow kernel NFS exports
- MGM: fix SEGV when looking at the changelog file

``V0.3.186 Aquamarine``
=======================

- FUSE: fix inode mapping after repair and follow new inode
- FUSE: avoid to force a file open for a utimes setattr call
- MGM: fix 'map' interface to work with encoded FUSE paths
- CONSOLE: update 'fs dropdeletion' and deprecate 'fs dropfiles' and MGM redirection behaviour for 'fs dropdeletion'

``V0.3.185 Aquamarine``
=======================

- FST: correct error codes in eoscp to flag target errors in tranfser queue jobs
- MGM: allow 'xrd.*' to be present in proc commands (used by FUSE repair)


``V0.3.184 Aquamarine``
=======================

- FUSE: report 1k as maximum file name length in statvfs
- FUSE: don't trigger recovery if a file is deleted before it is actually written
- MGM: update directory mtime when a replica drop leads to a file remove
- FST: don't give a checksum error if a not yet fully created file is read by a second FUSE client



``V0.3.183 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix lock bug visible since 0.3.182 in the WriteBack cache as a dead-lock (responsible for many previous changes)
- FUSE: close inconsistent mtime window present during release file (vim editor problem)

``V0.3.182 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix bug introduced in 0.3.181 to force creation of a file before a read open can proceed
- FUSE: use a standard mutex instead of a rw mutex to protect wb cache map
- FUSE: fix open(update) wrong mtime behaviour observed when using vim ona a file without local caps
- COMMON: fix performance relevant ShellCmd::Wait() function to use exponential backoff starting at 1ms to discover if a subprocess has terminated. This has a drastic effect on balancing and draining jobs which was limited to 1Hz due to this implementation
- FST: when running multiple FST instances store the eoscp log for each instance in their private log directory
- FST: fix missing tpcClose when a target TPC operation had been terminated
- MGM: use conditional/scoped lock monitor to avoid any path in the code where the quota mutex could stay read-locked and no new quota node can be created/listed


New Features
++++++++++++

- MGM: by default don't do a risk analysis for 'fs status' since it can take significant amount of time when millions of files are on a filesystem - previous behaviour using 'fs status -r'
- MGM: extend 'schedule2balance' call to directly return a balance job to the FST instead of sending it through the asynchronous queue (FST equivalent part is still not committed)
- FUSE: add an environment variable to simulate slow backend behaviour in the asynchronous part of FUSE (EOS_FUSE_LAZY_LAG=<ms>)

``V0.3.181 Aquamarine``
=======================

Bug Fix
+++++++

- FST: fix double unlock leading to an abort if a file checksum was found
- FUSE: fix race condition in locking scheme when adding pieces to the writeback cache
- FUSE: avoid several memory leaks induced by open/write/close/delete sequences
- FUSE: avoid possible order inversion of Open[create] file / Open[read] file

``V0.3.180 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: fix particular geo scheduling case which could return ENOSPACE
- MGM: avoid dead-lock in SetQuota calls

``V0.3.179 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix SEGV introduced by XrdIo memory leak fix in 0.3.177

``V0.3.178 Aquamarine``
=======================

Bug Fix
+++++++

- MGM: fix geotag scheduling when exact switch is enabled/disabled (try always first with exact geo matching, then relax the requirement)
- FUSE: fix SEGV on krb5 recovery redirection
- COMMON: fix eternal loop for esoteric .././.././../ path combinations

``V0.3.177 Aquamarine``
=======================

Bug Fix
+++++++

- FST: reduce lock contention on Sqlite mutex
- FST: use one Sqlite lock per filestem instead of a global lock for all filesystems
- ETC: fix use of default mount dir in eosd scripts
- FUSE: fix invalid modtime calculation disabling directory caching
- FUSE: fix memory leak in XrdIo when a file was deleted before it was ever opened
- HTTP: add mutex to avoid parallel loading of grid-map file and possible memory SEGV when parsing
- NAMESPACE: don't cancel follower threads on the Slave in active code (avoids exceptions on pthread_join)

New Feature
+++++++++++

- FUSE: add support to compile eosd3 using libfuse3

``V0.3.176 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: unset KRB5CCNAME only when run as a shared fuse mount ( prevented krb5 for single user mounts via 'eos fuse mount'
- FUSE: fix XRootD 3.3.6 memory leaks in every synchronous call (AnyObject leak) - not present anymore in XRootD 4.X
- FUSE: add clean-up to filesystem destructor to clean valgrind reports
- MGM: remove tight lock on namespace boot in HTTP service

New Feature
+++++++++++

- FUSE: by default hide all special files from version/atomic/backup - enable with env EOS_FUSE_SHOW_SPECIAL_FILES=1
- FUSE: by default configure a 64M shared write-back cache for shared and single-user mounts
- FUSE: use a blocking flush if the write-back size is larger than the in-memory cache - in this case there is no recovery possible so it is better to see possible errors on the application layer via the flush call

``V0.3.175 Aquamarine``
=======================

Bug Fix
+++++++

- FUSE: fix memory leaks and missing mutex - remove w-open tracking map

``V0.3.174 Aquamarine``
=======================

New Feature
+++++++++++

- FUSE: add 'restore' functionality which recovers file write errors on client side transparently if all the writes are still in the local in-memory cache
- FUSE: add the option do do an asynchronous open after a lazy open call (by default disabled - still WIP)

Bug Fix
+++++++

- MGM: print fid as decimal number in 'file info'
- MGM: redirect new 'Redirect' fuse call on the MGM always to a master
- MGM: keep the replica chain in the same order for FUSE updates (cl=>rep1=>rep2) doing identical scheduling
- FST: fix 'tried' CGI to append to a list and not overwrite previous tried add-ons

``V0.3.173 Aquamarine``

New Feature
+++++++++++

- FUSE: deal properly with security/system.posix_acl attributes in (cp -a errors)
- FUSE: reduce significantly memory footprint for tight file creation loops - default in-memory cache reduced from 1M to 4k
- FUSE: cleanup in-memory caches of deleted files immediatly
- FUSE: use asynchronous writes in release call and gain 25% performance
- FUSE: prefer readlocks when submitting a piece to the wb-cache and refresh iterator if mutex upgrade from r->w is needed
- WebDAV: return logical bytes as quota
- RPMS: add dependency for JEMALLOC at runtime for eos-server and eos-fuse rpms

Bug Fix
+++++++

- FUSE: fix bug bypassing the directory cache all the time when doing ls,ls -l ...
- FUSE: detect meta data updates on directories and refresh the client cache accordingly

``V0.3.172 Aquamarine``

New Feature
+++++++++++

- reduce default write-back page size to 256k (was 4M)
- make the page size configurable via env EOS_FUSE_CACHE_PAGE_SIZE (in bytes)


``V0.3.171 Aquamarine``

Bug Fixes
+++++++++

- fix 'd' via ACL for OC access

``V0.3.170 Aquamarine``
=======================

New Feature
+++++++++++

- remove 'chown -R' on FST paritions which was used to compensate a bug visible in 0.3.137 since it might introduce large unnecessary boot times when updating from versions < 0.3.137

``V0.3.169 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix exclusive lock held around fallocate delaying all writes and opens during an fallocate call (FST)
- fix SEGV in readlink call when an errno is returned (FUSE)
- fix OC access permission string to include writable for ACL shared directories (MGM)
- fix race condition when FUSE write-back cache is full - JIRA EOS-1455
- don't report symlinks as zero replica files
- fix SEGV in enforced geo placement where no location is available

New Features
++++++++++++

- add new FUSE config flags to enable automatic repair of a broken replica if one is still readable - default enabled until 256MB files
  - export EOS_FUSE_INLINE_REPAIR=1
  - export EOS_FUSE_MAX_INLINE_REPAIR_SIZE=268435456
- bypass authentication requirements for 'eos version' call (e.g. when getting the supported features)
- add IO error simulation for open on FSTs

``V0.3.168 Aquamarine``
=======================

Bug Fixes
+++++++++

- initialize container mtime by default with ctime if not defined


``V0.3.167 Aquamarine``
=======================

Bug Fixes
+++++++++

- add responses for custom namespaces (for new Owncloud clients) HTTP
- fix race condition for stat after close in FUSE
- gcc 6.0 warnings
- don't version module libraries anymore (as done by newer cmake)

New Features
++++++++++++

- introduction of 'sys.mask' attribute to apply a default mask to all chmod calls on directories (attribute disables !m in acls)

``V0.3.166 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix 'dumpmd' response for files with empty checksum, which cannot be parsed by the FST
- convert r=>w lock in FUSE (dir_cache_sync) to fix crashes in readdir
- protect 'recycle ls' to exceed string size limitation when listing millions of entries - stops at 1GB of console output and displays warning message

New Features
++++++++++++

- by default use FUSE in async mode e.g. fsync is not a blocking call - enable sync behaviour via sysconfig EOS_FUSE_SYNC=1
- by default use new FST fast boot option and disable WAL journaling of SQLITE db - the pedantic boot behaviour can be enforced via sysconfig EOS_FST_NO_FAST_BOOT=1
- add 'service eos clean fst' and 'service eos resync fst' to enforce a start behaviour (no resync or resync)

``V0.3.165 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix race condition on google_hash_map in FUSE leading

New Features
++++++++++++

- don't set/get xattr with "security.*' keys in FUSE

``V0.3.164 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix serious bug when moving directory subtress (as used by recycle bin) attaching moved trees after a reboot to the source location

.. warning:: it is highly recommended to update the MGM, if possible purge all recursive deletes before reboot from the recycling bin

``V0.3.163 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix dual side/dual fs exact placement
- fix 'eosd status' script

``V0.3.162 Aquamarine``
=======================

Bug Fixes
+++++++++

- mask all special mode bits in FUSE (was breaking writes via CIFS server if no group-w bit set)
- fix missing lock in TPC handling function in storage nodes
- apply removed sudoer priviledged in running instance

New Features
++++++++++++

- add 'service eosd killall' command and fix 'service eosd condrestart'


``V0.3.161 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix race condition originating in use of iterator outside locked section for setattr(utime)
- fix check for encoding support in FUSE client

``V0.3.160 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix file magic in various startup scripts
- place (u)mount.eos in /sbin
- fix eosd script and mount script to be compatible with autofs on EL6/7 and systemd
- fix geo placement for minimal geo case of two sites/two filesystems and 1 replica

New Features
++++++++++++

- add new encoding feature allowing full supoort of all characters via FUSE
- remove global locks around XrdCl calls in FUSE for better parallelism and less lock contention
- add version/fsctl call to discover available (FUSE) features of an MGM service
- add convenience RPMs to configure EOS repositories for YUM installation

``V0.3.159 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix SEGV in directory rename in FUSE
- fix read-after-write short-read for not aligned read crossing local-cache/remote border in FUSE
- make '.' and '..' visible in FUSE (again)

New Features
++++++++++++

- find honours now also ACLs in all recursive directories

``V0.3.158 Aquamarine``
=======================

- protect against failing inode reverse lookup

``V0.3.157 Aquamarine``
=======================

- add mount scripts to eos-fuse RPM

``V0.3.156 Aquamarine``
=======================

New Features
++++++++++++

- high speed directory listing in FUSE (enhanded protocol returning stat information with readdir - backward compatible)
- changing ETAG definition for directories to ino(hex):mtime(s).mtime(ms)
- allowing arbitrary remote path to local path mounting (no matching prefixes needed)
- allow to give a mount directory to 'mount -t eos <instance> <local-dir>'
- documentation for geotags and new fuse features added
- add 'find --xurl' to get XROotD urls as output
- refactor FUSE in pure C++
- use only eosd for single user mounts and shared mounts (fix eosfsd grep in any operation script)
- generate mtime timestamps locally
- auto-detect LAZY open capability of mounted server

Bug Fixes
+++++++++

- fix single user mount 'eos fuse mount' prefix
- removing deprecated env variables in FUSE
- track open inodes to prevent publishing stall size information from directory/stat cache
- fix 'mkdir -p' in CLI
- fix sync time propagation in Commit call
- fix '-h' behaviour of all shell commands
- protect against namespace crash with 'file touch /'
- fix sync time propagation in mkdir and setTMTime
- fix rm level protection
- don't report symbolic links a zero-replica files
- fix SEGV in PIO mode when an error is returned in FUSE client
- fix FUSE rename
- fix FUSE utime/mtime behaviour
- fix FUSE daemonize behaviour killing systemd on EL7

``V0.3.155 Aquamarine``
=======================

.. warning:: The FUSE implementation in this release is broken in various places. The sync time propagation in this release is broken. Don't use this version in production on client and server side!

Bug Fixes
+++++++++

- fix FUSE memory leak
- fix esod start-script typo
- fix HTTP PropFind requests for owncloud - unencoded paths in PropFind request to check quota & access permissions

``V0.3.154 Aquamarine``
=======================

New Features
++++++++++++

- disintiguish OC propfind and 'normal' propfind requests to report sync time or modification time of a directory
- fix 409 ERROR for HTTP PUT on non-existant path
- don't commit anymore mtime from FSTs for FUSE clients - let the FUSE client execute utime during close
- encode mtime.tv_nsec in the XRootD stat responses (inside device id) to track mtime with ns precision on open files
- protect plain-layout read-ahead mechamism with respect to size changing files
- FUSE: implementation refactoring (will break mtime consistency when used against old instances)
- => use negative stat cache of the kernel
- => add temporary and size limited in-memory rw cache per file to avoid waiting for flush of not written out pieces
- => add creator capability mechanism to assign local cache capability of a newly created file for a limited time to the local FUSE cache
- => retrieve mtime in ns precision for wopen files from the FST. commit last mtime on FST to MGM in asynchronous close operation
- => hide write latency completely in asynchronous write chain where open(MGM)=sync, open(FST1..X)=async, write(FST1)=async,flush=async,close=async
- => print FUSE settings on startup into log file
- => remove deprecated FUSE options, add new FUSE options to example files and verbose output on startup
- => point an unconfigured FUSE target url to localhost instead of eosdev
- => modify default values of FUSE configuration (enable lazy-open-w)

``V0.3.153 Aquamarine``
=======================

New Features
++++++++++++

- console add 'rm -rF' allow only root to use the bypass of the recycling policy
- console revert to use by default host+domain names and add a '-b,--brief' option to all fs,node,group commands to get short hostnames

``V0.3.152 Aquamarine``
=======================

Bug Fixes
+++++++++

- reenable FUSE concurrent opens and close
- fix FUSE lazy open and negative stat cache broken in the previous release
- fix wrong timestamping of symlinks

``V0.3.151 Aquamarine``
=======================

Bug Fixes
+++++++++

- synchronize with CITRINE FUSE implementation

``V0.3.150 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix wrong mount-prefix handling for deepness>1

``V0.3.149 Aquamarine``
=======================

New Features
++++++++++++

- import the CITRINE FUSE implementation and build this one
- making big writes and local mtime consistency the default behaviour in FUSE

``V0.3.148 Aquamarine``
=======================

New Features
++++++++++++

- add progress report on TTY console for all boot steps and estimate of boot time
- automatically store version in the recyle bin and allow to recall using 'recycle restore -r <key>'

Bug Fixes
+++++++++

- fix FUSE daemonize to work properly with autofs


``V0.3.147 Aquamarine``
=======================

New Features
++++++++++++

- shorten hostnames (remove domain) in all view functions besides monitoring format
- add support for multi-delegated proxy certificates

``V0.3.146 Aquamarine``
=======================

Bug Fixes
+++++++++

- fix http upload implementation for large body uploads
- allow to disable block checksumming via opaque tag
- use aggregation size in the WebDAV quota response and not the quota accounting
- track file size to avoid FUSE write-cache flushing on stat and listing
- merge no-quota-error in xrootd errors response into e-nospace to avoid the client reporting an io error

``V0.3.145 Aquamarine``
=======================

Bug Fixes
+++++++++

- add option to exclude all xattrs from being applied on the destination dirs by using the wildcard "*".
- clean-up the python cmake modules and simplify the use of Python related variables
- remove only the leading "eos" string when building the proc path for the MGM

``V0.3.144 Aquamarine``
=======================

Bug Fixes
+++++++++

- source sysconfig file inside MGM before running service scripts

``V0.3.142 Aquamarine``
=======================

New Features
++++++++++++

- add service alias example in eos.example how to run with systemd

``V0.3.141 Aquamarine``
=======================

Bug Fixes
+++++++++
- don't ship fuse.conf on EL7 in eos-fuse RPM
- fix reporting of subtree copying in 'eos cp'

``V0.3.140 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix missing object in drain lock helper mutex
- distinguish client and FST methods to prevent having FSTs calling a booting slave with namespace modifications
- add min/maxfilesize check during the open function, to block too large uploads immediately

``V0.3.139 Aquamarine``
=======================

Bug Fixes
+++++++++
- automatically chown files on FST partitions once (to compensate to bug introduced in 0.3.137)
- make the XRD stream timeout configurable and increase the default to 5 minutes

``V0.3.138 Aquamarine``
=======================

New Features
++++++++++++
- allow to specify the network interface to monitor on the FST via environment variable
- run the FST and MGM again as daemon/daemon and switch only the monitoring thread in ShellCmd to enable ptrace for all spawned sub commands

``V0.3.137 Aquamarine``
=======================

Bug Fixes
+++++++++
- don't scan partial read files when also if no blockchecksums are configured
- fix recursive copy command allowing spaces in path names

``V0.3.136 Aquamarine``
=======================

New Features
++++++++++++
- implement 'eos ls -lh' for readable sizes
- add extended attributes on files
- add 'file tag' command to manually set/remove locations
- allow 'file injection' to upload contents into an existing file
- add optional namespace subtree aggregation and introduce the concept of sync time
- implement <oc::size> and <oc::permissions> in PROPFIND requests
- run MGM/FST with effective user ID of root and filesystem ID of daemon/daemon


Bug Fixes
+++++++++
- avoid default auto-repair trigger if not configured
- fix high system time bug in ShellCmd class
- don't use fork when doing a stack trace, use ShellCmd class
- use always the current configured manager from global configuration to avoid eternal looping in case of certain failover scenarios
- avoid rescheduling of files on a location still in the deletion list

``V0.3.134 Aquamarine``
=======================

Bug Fixes
+++++++++
- avoid 'fork' calls in the namespace library using the 'ShellCmd' class

``V0.3.133 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix wrong EXITSTATUS() macro preventing clean Slave2Master transitions

``V0.3.132 Aquamarine``
=======================

Bug Fixes
+++++++++
- revert faulty bug fix introduced in 0.3.130 preventing a slave to boot the file namespace

``V0.3.131 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix comparison between FQDN and hostname when registering FSTs with the MGM
- forward errno to client console when archive/backup command fails
- fix accidental deletion of opaque info at the MGM for fsctl commands
- various FUSE bugfixes

New Features
++++++++++++
- add queuing functionality to the archive/backup tool

``V0.3.130 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix eternally booting slave and crazy boot times

``V0.3.129 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix for memory leak by ShellCmd not joining properly threads

``V0.3.128 Aquamarine``
=======================

Bug Fixes
+++++++++
- avoid to call pthread_cancel after pthread_join (SEGV) in ShellCmd class
- fix startup script to align with change in grep on CC7
- fix gcc 5.1 warning

``V0.3.127 Aquamarine``
=======================

Bug Fixes
+++++++++
- several compilation and build fixes (spec) for i386 and CC7
- fix fuse base64 encoding to not break URL syntax

``V0.3.126 Aquamarine``
=======================

New Features
++++++++++++
- major improvements in automatic error recovery for read and writes
- a failed create due to a faulty disk server is recovered transparently
- a failed read due to a faulty disk server is recovered transparently
- an update on a file where not all replicas are available triggers an inline repair if (<1GB) and if configured via attributes an async repair via the configure - FUSE has been adapted to deal with changing inodes during a repaired open
- distinguish scheduling policies for read and write via `geo.access.policy.read.exact` `geo.access.policy.write.exact` - if `on` for **write** then only groups matching the geo policy and two-site placement policy will be selected for placement and data will flow through the close fst - if `on` for **read** the replica in the same geo location will always be chosen

``V0.3.125 Aquamarine``
=======================

New Features
++++++++++++
- allow to disable 'sss' enforcement on FSTs (see /etc/sysconfig/eos.example) - each FST need a prot bind entry on the MGM config file when enabled
- show the current debug setting in 'node status <node>' as debug.state variable
- add support for multi-session FUSE connections with uid<1024*1024 and gid<65536 sid<256
- introduce vid.app, avoid stalling of 'fuse' clients and report application names in 'who -a'
- implement 'sys.http.index' attribute to allow for static index pages/redirection and support URLs a symbolic link targets
- follow the 'tried=<>' advice given by the XRootD client not to redirect again to a broken target

Bug Fixes
+++++++++
- fix 'eos <cmd>' bug where <cmd> is not executed if it has 3 letters and is a local file or directory (due to XrdOucString::endswith bug)
- update modification for intermediate directories created by MKPATH option of 'xrdcp'
- fix 'vid rm <key>'
- revert 'rename' function to apply by default overwrite behaviour
- allow arbitrary symbolic link targets (relative targets etc.)
- disable readahead for files that have rd/wr operations
- allow clean-up via the destructor for chunked upload files
- fix directory listing ACL bug
- avoid timing related dead-lock in asynchronous backend flush

``V0.3.121 Aquamarine``
=======================

New Features
++++++++++++
- support ALICE tokens in gateway transfers
- allow to disable enforced authentication for submitted transfers
- disable direct_io flag on ZFS mounts to avoid disabling filesystems due to scrubbing errors

Bug Fixes
+++++++++
- replacing system(fork) commands with ShellCmd class fixing virtual memory and fd cloning

``V0.3.120 Aquamarine``
=======================

Bug Fixes
+++++++++
- symlink fixes
- fix round-robin behaviour of scheduler for single and multi-replace placements

``V0.3.119 Aquamarine``
=======================

New Features
++++++++++++
- add support symbolic links for files and directories
- add convenient short console commands for 'ln', 'info', 'mv', 'touch'

``V0.3.118 Aquamarine``
=======================

New Features
++++++++++++
- add console broadcasts for important MGM messages

Bug Fixes
+++++++++

- use correct lock type (write) for merge,attr:set calls
- resolve locking issue when new SpaceQuota objects have to be created
- implement a fast and successful shutdown procedure for the MGM
- implement saveguard for the manager name configuration in FSTs

``V0.3.117 Aquamarine``
=======================

New Features
++++++++++++
- enable read-ahead in FUSE clients to boost performance (default is off - see /etc/sysconfig/eos.example)


``V0.3.116 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix asynchronous egroup refresh query

``V0.3.115 Aquamarine``
=======================

Bug Fixes
+++++++++
- reduce verbosity of eosfsd logging
- support OC special header removing the location header from a WebDAV MOVE response

Bug Fixes
+++++++++
- fix temporary ro master situation when slave reloads namespace when indicated from compacted master (due to stat redirection)

``V0.3.114 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix temporary ro master situation when slave reloads namespace when indicated from compacted master (due to stat redirection)

``V0.3.112 Aquamarine``
=======================

New Features
++++++++++++

- add support for nested EGROUPS
- add 'member' CLI to check egroup membership

Bug Fixes
+++++++++
- fix logical quota summary accounting bug
- fix not working 'file version' command for directories with 'sys.versioning=1' configured
- fix order violation bug in 'Drop' implementation which might lead to SEGV

``V0.3.111 Aquamarine``
=======================

Bug Fixes
+++++++++
- redirect "file versions' to the master

``V0.3.110 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix copy constructor of ContainerMD impacting slave following (hiding directory contents on slave)
- fix temp std::string assignment bugs reported by valgrind

``V0.3.109 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix timed read/write locks to use absolute times

``V0.3.108 Aquamarine``
=======================

Bug Fixes
+++++++++
- update Drain/Balancer configuration at least every minute to allow following master/slave failover and slot reconfiguration

New Features
++++++++++++
- support for OC-Checksum field in GET/PUT requests

``V0.3.107 Aquamarine``
=======================

New Features
++++++++++++
- support for secondary group evaluation in ACLs (enable secondary groups via /etc/sysonfig/eos:export EOS_SECONDARY_GROUPS=1

``V0.3.106 Aquamarine``
=======================

Bug Fixes
+++++++++
- update MIME types to reflect most recent mappings for office types

``V0.3.104 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix custom namespace parsing for PROPPATCH requests
- allow 'eos cp' to copy files/dirs with $
- fix missing unlock of quota mutex in error return path
- fix mutex inversion in STATLS function

``V0.3.102 Aquamarine``
=======================

Bug Fixes
+++++++++
- fix 'attr' get' function if no attribute links are used
- use '_attr_ls' consistently instead of directy namespace map (to enable links everywhere)
- fix PROPPATCH response to be 'multi-status' 207

``V0.3.101 Aquamarine``
=======================

Bug Fixes
+++++++++
- avoid negative sleep times in scrub loops induced by very slow disks
- apply ANDROID patch for chunked uploads only if 'cbox-chunked-android-issue-900' special header has been added by NGINX proxy
- make MIME type detection case-insensitive

``V0.3.100 Aquamarine``
=======================

New Features
++++++++++++
- add online compaction for directories selectable via 'ns compact' (see help)
- support for symbolic attributes 'attr link', 'attr unlink', 'attr fold' to reduce directory memory footprint

Bug Fixes
+++++++++
- fix bug leading to wrong dual master detection after online compaction was running on the master

``V0.3.99 Aquamarine``
======================

New Features
++++++++++++
- allow 'sys.owner.auth=*' to have sticky uid/gids for such directories
- new FST proxy redirection to send file IO through a proxy frontend
- recursive 'rm -r' protection in fuse
- add MIME type suffix detection

Bug Fixes
+++++++++
- remove PrivGuards from Transfer cmds enabling krb5/x509 delegation
- fix HTTP return codes for Put and Range Requests

``V0.3.97 Aquamarine``
======================

New Features
++++++++++++
- forbid 'rm -r' & 'rm -rf' on a predefined tree deepness

Bug Fixes
+++++++++
- various fixes in archive daemon
- improve speed of HTTP HEAD requests with trailing /
- store proxy and client identity properly in VID structure

``V0.3.96 Aquamarine``
======================

Bug Fixes
+++++++++
- fix -1 bug in 'chown'

New Features
++++++++++++
- add dummy responses for LOCK,UNLOCK,PROPPATCH enabling OSX & Windows WebDAV clients
- allow to modifiy only group ownership in chown

``V0.3.95 Aquamarine``
======================

Bug Fixes
+++++++++
- balancing: seal '&' in capabilities
- draining: seal '&' in capabilities
- encode all '&' in meta data synchronization
- propagate 'disableChecksum' to all replicas during chunked uploads
- make 'console log' e.g. /var/log/eos/mgm/error.log working again
- fix substantial memory leak in PUT requests on FSTs
- fix 's3' lower-case headers
- disable 'delete-on-close & repair-on-close' for chunked uploads to allow for single chunk retry
- fix '\n' encoding for FUSE listing
- require 'targetsize' in standard HTTP PUT
- fix documentation of attributes for max/minsize in 'attr help'
- fix sealing of empty checksum FMD info
- fix double mapping of propfind requests
- enable re-entrant https mapping as required by HTTPS Webdav gateways
- fix JSON format for fsck reports
- swap HTTP/ROOT share url
- fix return codes for chunked uploads for cases like no quota etc.
- add 'open' serialization for identical file paths to avoid open errors using HTTP protocol
- don't send redirect on FST put's to avoid incomplete files
- fix missing targetsize for standard oc PUTs to avoid acceptance of incomplete files
- fix and use atomic CLOEXEC flag in various places
- add PAM module to NGINX
- fix PUT error handling (will break connection for all errors happening after 100-continue on FST)
- various improvements to backup functionality
- enforce order in chunked uploads
- disable scanning of w-open files
- fix 'geotag' client mapping
- fix 'recycle restore' for overlapping file/directory keys
- advertise MKCOL,PUT in OPTIONS for WebDAV write access
- fix SEGV due to illegal mtime settings for HTTP GETs
- fix copy constructor of Container objects

New Features
++++++++++++
- 'find --purge atomic' to clean-up atomic left-over garbage
- allow 'file check fxid:.... | fid:...'
- add 'recycle config --ratio < 0 .. 1.0 >' to set a threadshold based keep ratio in the recycle bin

``V0.3.75 Aquamarine``
======================

- add support for archive interface to stage-out and migrate a frozen subtree in the namespace to any XRootD enabled archive storage

``V0.3.57 Beryl``
=================

New Features
++++++++++++
- adding libmicrohttpd build directory
- support threadpool with EPOLL for embedded http server

Bug Fixes
+++++++++
- balancing: was never starting
- scheduler: was skipping scheduling group when one node >95% network-out loaded
- nginx: don't forward PUT payload to MGM
- microhttpd: fix virtual memory leaking due to fragmentation
- http: let HTTP clients see errors on PUT

``V0.3.53 Beryl``
=================

New Features
++++++++++++
- [webdav] add possibility to exclude directory syncs via 'sys.allow.oc.sync'
- [webdav] add support to do path replacments provdided by two special header flosg CBOX_CLIENT_MAPPING & CBOX_SERVER_MAPPING

``V0.3.51 Beryl``
=================

Bug Fixes
+++++++++
- fix gdb stacktrace getting stuck if too much output is produced - stacktrace is stored in /var/eos/md/stacktrace and then reported back into the log
- fix wrong network traffic variable used in the scheduling implementation (used always 0 instead of real traffic)

``V0.3.49 Beryl``
=================

Bug Fixes
+++++++++
- rename: allow whitespace names, fix subpath check, fix encofing in HTTP move
- various HTTP/DAV related return code fixes

Consolidation
+++++++++++++
- the 'eos' shell by default does not run in 'pipe mode' e.g. no background agent

New Features
++++++++++++
- allow FUSE_OPT in /etc/sysconfig/eos e.g. to set a FUSE mount read-only use export FUSE_OPT="ro"
- enable MacOSX build and add packing script for DMG

``V0.3.47 Beryl``
=================

Bug Fixes
+++++++++
- bugfixes in HTTP daemon configuration/startup
- many bugfixes for owncloud/atomic/version support
- many bugfixes for mutex order violations
- fix BUG in FUSE making the mount hang easily
- fix BUG in FUSE showing alternating mtimes and showing stale directory listings
- fix BUG in stalling drain/balance
- fix BUG in drain reset
- fix FD leak in Master
- add monitor lock to getpwXXX calls to deal with SSSD dead-lock on SLC6
- disable FMD size/checksum checks for RAIN files

Consolidation
+++++++++++++
- FST don't clean-up transactions if their replica is registered in the MGM
- make all HTTP header tags case-insensitive
- HEAD becomes a light-weight operation on large directories
- new unit tests for owncloud/atomic/version support
- improve 'quota ls' performance and bypass uid/gid translations as much as possible
- avoid lock contention in uid/gid translations
- limit the 'gdb' stack trace to maximum 120s to avoid service lock-up in case of a stuck GDB process
- FST never give up in calling a manager for errors allowing a retry

New Features
++++++++++++
- update 'eos-deploy' to be able to install from beryl, beryl-testing, aquamarine and citrine YUM repositories
- adjust 'file adjustreplica' and 'file verify' for RAIN files (file verify made RAIN file inaccessible)
- extend 'space reset' command

``V0.3.37 Beryl``
=================

- add support for Owncloud chunked upload
- add support for immutable namespace directories
- fix drain/balancing stalls
- fix memory leak introcuded by asynchronous XrdCl messaging
- fix node/fs/group unregistering bug
- make atomic uploads and versioning real 'atomic' operations (no visible state gap between target file exchange)
- add 'file versions' command to show and recall a previous version
- fix tight thread locking delaying start-up

``V0.3.35``
===========

Bug Fixes
+++++++++

- modify behaviour on FST commit timeouts - cleanup transaction and keep the replica to avoid unacknowledged commits (replica loss)
- fix output of 'vst ls --io'
- add option 'vst --upd target --self' to publish only the local instance VST statistics to InfluxDB

``V0.3.34``
===========

New Features
++++++++++++
- add global VST monitoring support - by default all running EOS instances are visible with some basic parameters using the 'vst' command
- add support to feed VST informatino using UDP into InfluxDB for vizualisation with Grafana
- add global-mq config file to run a global VST broker
- support 'mtime' propagation as needed by OwnCloud sync client to optimize the sync process
- better support OwnCloud sync clients
- restrict OwnCloud sync tree requiring 'sys.allow.oc.sync=1' on the entry directory
- add support for atomic file uploads - files are visible with the target name when they are complete - disabled for FUSE
- support LDAP authentication (basic HTTP authentication) in NGINX proxy on port 4443 (by default)
- add 'file info' command for directories
- implement 'fsck repair --adjust-replica-nodrop' for safe repair (nothing get's removed - only added)
- allow 'grep'-like functionality in 'fs ls' commands
- support encoding models like UTF-8 (set export EOS_UTF8=1 in /etc/sysconfig/eos)
- accept any checksum configuration in 'xrootd.chksum' config file

Consolidation
+++++++++++++
- FUSE (cache) refactoring & FUSE unit tests
- send all 'monitoring'-like messages purely in async mode (not waiting) for any response e.g. all shared hash states

Bug Fixes
+++++++++
- fix PWD mapping for names starting with numbers
- fix Windows compliance for WebDAV implementation (allprop request)
- fix iterator issue in GeoBalancer and GroupBalancer
- fix balancing starvation bug
- fix 'range requests' in HTTP implementation
- fix embedded HTTP server configuration (thread-per-client model using poll)
- fix S3 escaping for signature checks (make Cyberduck compatible)

``V0.3.28``
-----------

New Features
++++++++++++
- allow FUSE mounts against Master and Slave MGM implementing a new stat function and mkdir/create returning the new inode numbers
- add ETAG to FST GET & PUT requests
- allow to 'grep' for several view objects in fs,node,group,space ls function

Consolidation
+++++++++++++
- improve/fix master/slave failover behaviour
- display the correct boot state during slave startup
- improve stack trace to extract responsible stacktrace thread and print again in the end of a log file
- let hotfile display files age and expire
- don't allow to remove nodes which are currently sending heartbeats or have not drained filesystems

Bug Fixes
+++++++++
- fix leak in HTTP access leaving files open
- fix krb5 keytab permission for xrootd 3.3.6-CERN and eos-deploy
- fix sync startup in Slave2Master transition


``V0.3.25``
===========

New Features
++++++++++++
- allow to match hostnames in VID interface for gateway machines e.g. vid add gateway lxplus* https
- broadcast hotfile list per filesystem to the MGM and add interface to this list via ``io ns -f``
- use inode+checksum for file ETAGs in HTTP, otherwise inode+mtime time - for directories use inode+mtime
- add support for file versioning using attribute ``sys.versioning`` or via shell interface ``file version ..``
- make ApMon more flexible to match individual mountpoints via environment match variable ``APMON_STORAGEPATH`` (try df | grep $APMON_STORAGEPATH).
- eos-deploy script is added to the repository allowing RPM installation of (possibly ALICE enabled) EOS instances with a dual MGM and multi FST setup via a single command
- allow to list files at risk/offline via ``fs status -l <fs-id>``

Consolidation
+++++++++++++
- add space reset to documentation
- add release notes to documentation
- restrict daemon account to read everything but no write permission
- propagate ban/unban/sudo setting from Master to Slave MGM
- map the root user on a shared FUSE mount to daemon
- delete space,group,node objects if they contained no filesystem when rm is issued on them
- add space/group/node create/delete tests
- make krb5 keytab file accessible to EOS MGM (required by XROOTD 3.6/CERN and 4.0)
- allow for new TPC protocol where destination's open arrives before the source TPC key is deposited
- use xrdfs in eos-instance-test instead of xrd
- add a check for missing fusermount execution permissions to the user FUSE daemon eosfsd
- add an explicit message to the MGM log AFTER a file is successfully deleted
- allow to select user and group ID as user and group names e.g. user foo and group bar ``eos -b foo bar``
- add the node information given by ``ls --sys`` to the monitoring output ``ls -m``

Bug Fixes
+++++++++
- make krb5 keytab file accessible to EOS MGM
- fix lock from rw to wr-lock when a space/node group is defined or created
- fix boradcasting and value application on slave filesystem view
- add the eos-test RPM to the MGM installation done via eos-deploy
- fix path reparsing for .. to allow filenames like ..myfile
- use path filter function in the Attr shell interface to support attr ls . etc.
- make RAIN recovery/draining usable
- forbid renaming of a directory into an existing file
- add browse permission of local drop box directory
- if no strong auth is available use sss authentication in transfer jobs
- remove two obsolete tests from eos-instance-test and add bc to RPM dependency of eos-test
- fix eos-uninstall script
- don't block slave/master transitions if eosha is enabled
- start recycle thread only when the namespace is fully booted
