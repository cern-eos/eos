.. highlight:: rst

.. index::
   single: Beryl(-Aquamarine)-Release


Beryl Release Notes
===================

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
-----------------------

New Feature
+++++++++++

- remove 'chown -R' on FST paritions which was used to compensate a bug visible in 0.3.137 since it might introduce large unnecessary boot times when updating from versions < 0.3.137

``V0.3.169 Aquamarine``
-----------------------

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
-----------------------

Bug Fixes
+++++++++

- initialize container mtime by default with ctime if not defined


``V0.3.167 Aquamarine``
-----------------------

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
-----------------------

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
-----------------------

Bug Fixes
+++++++++

- fix race condition on google_hash_map in FUSE leading 

New Features
++++++++++++

- don't set/get xattr with "security.*' keys in FUSE

``V0.3.164 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- fix serious bug when moving directory subtress (as used by recycle bin) attaching moved trees after a reboot to the source location

.. warning:: it is highly recommended to update the MGM, if possible purge all recursive deletes before reboot from the recycling bin

``V0.3.163 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix dual side/dual fs exact placement
- fix 'eosd status' script

``V0.3.162 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- mask all special mode bits in FUSE (was breaking writes via CIFS server if no group-w bit set)
- fix missing lock in TPC handling function in storage nodes
- apply removed sudoer priviledged in running instance 

New Features
++++++++++++

- add 'service eosd killall' command and fix 'service eosd condrestart'


``V0.3.161 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- fix race condition originating in use of iterator outside locked section for setattr(utime)
- fix check for encoding support in FUSE client 

``V0.3.160 Aquamarine``
-----------------------

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
-----------------------

Bug Fixes
+++++++++

- fix SEGV in directory rename in FUSE
- fix read-after-write short-read for not aligned read crossing local-cache/remote border in FUSE
- make '.' and '..' visible in FUSE (again)

New Features
++++++++++++

- find honours now also ACLs in all recursive directories

``V0.3.158 Aquamarine``
-----------------------

- protect against failing inode reverse lookup

``V0.3.157 Aquamarine``
-----------------------

- add mount scripts to eos-fuse RPM

``V0.3.156 Aquamarine``
-----------------------

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
-----------------------

.. warning:: The FUSE implementation in this release is broken in various places. The sync time propagation in this release is broken. Don't use this version in production on client and server side!

Bug Fixes
+++++++++

- fix FUSE memory leak
- fix esod start-script typo
- fix HTTP PropFind requests for owncloud - unencoded paths in PropFind request to check quota & access permissions

``V0.3.154 Aquamarine``
-----------------------

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
-----------------------

New Features
++++++++++++

- console add 'rm -rF' allow only root to use the bypass of the recycling policy
- console revert to use by default host+domain names and add a '-b,--brief' option to all fs,node,group commands to get short hostnames

``V0.3.152 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- reenable FUSE concurrent opens and close
- fix FUSE lazy open and negative stat cache broken in the previous release
- fix wrong timestamping of symlinks

``V0.3.151 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- synchronize with CITRINE FUSE implementation 

``V0.3.150 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- fix wrong mount-prefix handling for deepness>1

``V0.3.149 Aquamarine``
-----------------------

New Features
++++++++++++

- import the CITRINE FUSE implementation and build this one
- making big writes and local mtime consistency the default behaviour in FUSE

``V0.3.148 Aquamarine``
-----------------------

New Features
++++++++++++

- add progress report on TTY console for all boot steps and estimate of boot time
- automatically store version in the recyle bin and allow to recall using 'recycle restore -r <key>'

Bug Fixes
+++++++++

- fix FUSE daemonize to work properly with autofs


``V0.3.147 Aquamarine``
-----------------------

New Features
++++++++++++

- shorten hostnames (remove domain) in all view functions besides monitoring format
- add support for multi-delegated proxy certificates

``V0.3.146 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- fix http upload implementation for large body uploads
- allow to disable block checksumming via opaque tag
- use aggregation size in the WebDAV quota response and not the quota accounting
- track file size to avoid FUSE write-cache flushing on stat and listing
- merge no-quota-error in xrootd errors response into e-nospace to avoid the client reporting an io error

``V0.3.145 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- add option to exclude all xattrs from being applied on the destination dirs by using the wildcard "*".
- clean-up the python cmake modules and simplify the use of Python related variables
- remove only the leading "eos" string when building the proc path for the MGM

``V0.3.144 Aquamarine``
-----------------------

Bug Fixes
+++++++++

- source sysconfig file inside MGM before running service scripts

``V0.3.142 Aquamarine``
-----------------------

New Features
++++++++++++

- add service alias example in eos.example how to run with systemd

``V0.3.141 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- don't ship fuse.conf on EL7 in eos-fuse RPM
- fix reporting of subtree copying in 'eos cp'

``V0.3.140 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix missing object in drain lock helper mutex
- distinguish client and FST methods to prevent having FSTs calling a booting slave with namespace modifications
- add min/maxfilesize check during the open function, to block too large uploads immedeatly

``V0.3.139 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- automatically chown files on FST partitions once (to compensate to bug introduced in 0.3.137)
- make the XRD stream timeout configurable and increase the default to 5 minutes

``V0.3.138 Aquamarine``
-----------------------

New Features
++++++++++++
- allow to specify the network interface to monitor on the FST via environment variable
- run the FST and MGM again as daemon/daemon and switch only the monitoring thread in ShellCmd to enable ptrace for all spawned sub commands

``V0.3.137 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- don't scan partial read files when also if no blockchecksums are configured
- fix recursive copy command allowing spaces in path names

``V0.3.136 Aquamarine``
-----------------------

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
-----------------------

Bug Fixes
+++++++++
- avoid 'fork' calls in the namespace library using the 'ShellCmd' class

``V0.3.133 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix wrong EXITSTATUS() macro preventing clean Slave2Master transitions

``V0.3.132 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- revert faulty bug fix introduced in 0.3.130 preventing a slave to boot the file namespace

``V0.3.131 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix comparison beteen FQDN and hostname when registering FSTs with the MGM
- forward errno to client console when archive/backup command fails
- fix accidental deletion of opaque info at the MGM for fsctl commands
- various FUSE bugfixes

New Features
++++++++++++
- add queuing functionality to the archive/backup tool

``V0.3.130 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix eternally booting slave and crazy boot times

``V0.3.129 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix for memory leak by ShellCmd not joining properly threads

``V0.3.128 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- avoid to call pthread_cancel after pthread_join (SEGV) in ShellCmd class
- fix startup script to align with change in grep on CC7
- fix gcc 5.1 warning

``V0.3.127 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- several compilation and build fixes (spec) for i386 and CC7
- fix fuse base64 encoding to not break URL syntax 

``V0.3.126 Aquamarine``
-----------------------

New Features
++++++++++++
- major improvements in automatic error recovery for read and writes
- a failed create due to a faulty disk server is recovered transparently
- a failed read due to a faulty disk server is recovered transparently
- an update on a file where not all replicas are available triggers an inline repair if (<1GB) and if configured via attributes an async repair via the configure - FUSE has been adapted to deal with changing inodes during a repaired open
- distinguish scheduling policies for read and write via `geo.access.policy.read.exact` `geo.access.policy.write.exact` - if `on` for **write** then only groups matching the geo policy and two-site placement policy will be selected for placement and data will flow through the close fst - if `on` for **read** the replica in the same geo location will always be chosen

``V0.3.125 Aquamarine``
-----------------------

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
-----------------------

New Features
++++++++++++
- support ALICE tokens in gateway transfers
- allow to disable enforced authentication for submitted transfers
- disable direct_io flag on ZFS mounts to avoid disabling filesystems due to scrubbing errors

Bug Fixes
+++++++++
- replacing system(fork) commands with ShellCmd class fixing virtual memory and fd cloning

``V0.3.120 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- symlink fixes
- fix round-robin behaviour of scheduler for single and multi-repliace placements

``V0.3.119 Aquamarine``
-----------------------

New Features
++++++++++++
- add support symbolic links for files and directories
- add convenient short console commands for 'ln', 'info', 'mv', 'touch'

``V0.3.118 Aquamarine``
-----------------------

New Features
++++++++++++
- add console broadcasts for important MGM messages

Bug Fixes
+++++++++

- use correct lock type (write) for merge,attr:set calls
- resolve locking issue when new SpaceQuota objects have to be created
- implement a fast and successfull shutdown procedure for the MGM
- implement saveguard for the manager name configurationi in FSTs

``V0.3.117 Aquamarine``
-----------------------

New Features
++++++++++++
- enable read-ahead in FUSE clients to boost performance (default is off - see /etc/sysconfig/eos.example)


``V0.3.116 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix asynchronous egroup refresh query 

``V0.3.115 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- reduce verbosity of eosfsd logging
- support OC special header removing the location header from a WebDAV MOVE response

Bug Fixes
+++++++++
- fix temporary ro master situation when slave reloads namespace when indicated from compacted master (due to stat redirection)

``V0.3.114 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix temporary ro master situation when slave reloads namespace when indicated from compacted master (due to stat redirection)

``V0.3.112 Aquamarine``
-----------------------

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
-----------------------

Bug Fixes
+++++++++
- redirect "file versions' to the master

``V0.3.110 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix copy constructor of ContainerMD impacting slave following (hiding directory contents on slave)
- fix temp std::string assignment bugs reported by valgrind

``V0.3.109 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix timed read/write locks to use absolute times

``V0.3.108 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- update Drain/Balancer configuration atleast every minute to allow following master/slave failover and slot reconfiguration

New Features
++++++++++++
- support for OC-Checksum field in GET/PUT requests

``V0.3.107 Aquamarine``
-----------------------

New Features
++++++++++++
- support for secondary group evaluation in ACLs (enable secondary groups via /etc/sysonfig/eos:export EOS_SECONDARY_GROUPS=1

``V0.3.106 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- update MIME types to reflect most recent mappings for office types

``V0.3.104 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix custom namespace parsing for PROPPATCH requests
- allow 'eos cp' to copy files/dirs with $
- fix missing unlock of quota mutex in error return path
- fix mutex inversion in STATLS function

``V0.3.102 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- fix 'attr' get' function if no attribute links are used
- use '_attr_ls' consistently instead of directy namespace map (to enable links everywhere)
- fix PROPPATCH response to be 'multi-status' 207

``V0.3.101 Aquamarine``
-----------------------

Bug Fixes
+++++++++
- avoid negative sleep times in scrub loops induced by very slow disks
- apply ANDROID patch for chunked uploads only if 'cbox-chunked-android-issue-900' special header has been added by NGINX proxy
- make MIME type detection case-insensitive

``V0.3.100 Aquamarine``
-----------------------

New Features
++++++++++++
- add online compaction for directories selectable via 'ns compact' (see help)
- support for symbolic attributes 'attr link', 'attr unlink', 'attr fold' to reduce directory memory footprint

Bug Fixes
+++++++++
- fix bug leading to wrong dual master detection after online compaction was running on the master

``V0.3.99 Aquamarine``
----------------------

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
----------------------

New Features
++++++++++++
- forbid 'rm -r' & 'rm -rf' on a predefined tree deepness

Bug Fixes 
+++++++++
- various fixes in archive daemon
- improve speed of HTTP HEAD requests with trailing /  
- store proxy and client identity properly in VID structure

``V0.3.96 Aquamarine``
----------------------

Bug Fixes
+++++++++
- fix -1 bug in 'chown' 

New Features
++++++++++++
- add dummy responses for LOCK,UNLOCK,PROPPATCH enabling OSX & Windows WebDAV clients 
- allow to modifiy only group ownership in chown

``V0.3.95 Aquamarine``
----------------------

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
----------------------

- add support for archive interface to stage-out and migrate a frozen subtree in the namespace to any XRootD enabled archive storage

``V0.3.57 Beryl``
-----------------

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
-----------------

New Features
++++++++++++
- [webdav] add possibility to exclude directory syncs via 'sys.allow.oc.sync'
- [webdav] add support to do path replacments provdided by two special header flosg CBOX_CLIENT_MAPPING & CBOX_SERVER_MAPPING

``V0.3.51 Beryl``
-----------------

Bug Fixes
+++++++++
- fix gdb stacktrace getting stuck if too much output is produced - stacktrace is stored in /var/eos/md/stacktrace and then reported back into the log
- fix wrong network traffic variable used in the scheduling implementation (used always 0 instead of real traffic)

``V0.3.49 Beryl``
-----------------

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
-----------------

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
-----------------

- add support for Owncloud chunked upload
- add support for immutable namespace directories
- fix drain/balancing stalls
- fix memory leak introcuded by asynchronous XrdCl messaging
- fix node/fs/group unregistering bug
- make atomic uploads and versioning real 'atomic' operations (no visible state gap between target file exchange)
- add 'file versions' command to show and recall a previous version
- fix tight thread locking delaying start-up

``V0.3.35``
-----------

Bug Fixes
+++++++++

- modify behaviour on FST commit timeouts - cleanup transaction and keep the replica to avoid unacknowledged commits (replica loss)
- fix output of 'vst ls --io'
- add option 'vst --upd target --self' to publish only the local instance VST statistics to InfluxDB

``V0.3.34``
-----------

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

``V0.3.28`
----------

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
-----------

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



