:orphan:

.. highlight:: rst

.. index::
   single: Beryl(-Aquamarine)-Release

Beryl Release Notes
===================

``Version V0.3.134 Beryl_Aquamarine``
---------
- avoid 'fork' calls in the namespace library using the 'ShellCmd' class

``Version V0.3.133 Beryl_Aquamarine``

Bug Fixes
---------
- fix wrong EXITSTATUS() macro preventing clean Slave2Master transitions

``Version V0.3.132 Beryl_Aquamarine``

Bug Fixes
---------
- revert faulty bug fix introduced in 0.3.130 preventing a slave to boot the file namespace

``Version V0.3.131 Beryl_Aquamarine``

Bug Fixes
---------
- fix comparison beteen FQDN and hostname when registering FSTs with the MGM
- forward errno to client console when archive/backup command fails
- fix accidental deletion of opaque info at the MGM for fsctl commands
- various FUSE bugfixes

New Features
------------
- add queuing functionality to the archive/backup tool

``Version V0.3.130 Beryl-Aquamarine``

Bug Fixes
---------
- fix eternally booting slave and crazy boot times

``Version V0.3.129 Beryl-Aquamarine``

Bug Fixes
---------
- fix for memory leak by ShellCmd not joining properly threads

``Version V0.3.128 Beryl-Aquamarine``

Bug Fixes
---------
- avoid to call pthread_cancel after pthread_join (SEGV) in ShellCmd class
- fix startup script to align with change in grep on CC7
- fix gcc 5.1 warning

``Version V0.3.127 Beryl-Aquamarine``

Bug Fixes
---------
- several compilation and build fixes (spec) for i386 and CC7
- fix fuse base64 encoding to not break URL syntax

``Version V0.3.126 Beryl-Aquamarine``

New Features
------------
- major improvements in automatic error recovery for read and writes
- a failed create due to a faulty disk server is recovered transparently
- a failed read due to a faulty disk server is recovered transparently
- an update on a file where not all replicas are available triggers an inline repair if (<1GB) and if configured via attributes an async repair via the configure - FUSE has been adapted to deal with changing inodes during a repaired open
- distinguish scheduling policies for read and write via `geo.access.policy.read.exact` `geo.access.policy.write.exact` - if `on` for **write** then only groups matching the geo policy and two-site placement policy will be selected for placement and data will flow through the close fst - if `on` for **read** the replica in the same geo location will always be chosen

``Version V0.3.125 Beryl-Aquamarine``

New Features
------------
- allow to disable 'sss' enforcement on FSTs (see /etc/sysconfig/eos.example) - each FST need a prot bind entry on the MGM config file when enabled
- show the current debug setting in 'node status <node>' as debug.state variable
- add support for multi-session FUSE connections with uid<1024*1024 and gid<65536 sid<256
- introduce vid.app, avoid stalling of 'fuse' clients and report application names in 'who -a'
- implement 'sys.http.index' attribute to allow for static index pages/redirection and support URLs a symbolic link targets
- follow the 'tried=<>' advice given by the XRootD client not to redirect again to a broken target

Bug Fixes
---------
- fix 'eos <cmd>' bug where <cmd> is not executed if it has 3 letters and is a local file or directory (due to XrdOucString::endswith bug)
- update modification for intermediate directories created by MKPATH option of 'xrdcp'
- fix 'vid rm <key>'
- revert 'rename' function to apply by default overwrite behaviour
- allow arbitrary symbolic link targets (relative targets etc.)
- disable readahead for files that have rd/wr operations
- allow clean-up via the destructor for chunked upload files
- fix directory listing ACL bug
- avoid timing related dead-lock in asynchronous backend flush

``Version V0.3.121 Beryl-Aquamarine``

New Features
------------
- support ALICE tokens in gateway transfers
- allow to disable enforced authentication for submitted transfers
- disable direct_io flag on ZFS mounts to avoid disabling filesystems due to scrubbing errors

Bug Fixes
---------
- replacing system(fork) commands with ShellCmd class fixing virtual memory and fd cloning

``Version V0.3.120 Beryl-Aquamarine``
Bug Fixes
---------
- symlink fixes
- fix round-robin behaviour of scheduler for single and multi-repliace placements

``Version V0.3.119 Beryl-Aquamarine``
New Features
------------
- add support symbolic links for files and directories
- add convenient short console commands for 'ln', 'info', 'mv', 'touch'

``Version V0.3.118 Beryl-Aquamarine``

New Features
------------
- add console broadcasts for important MGM messages

Bug Fixes
---------

- use correct lock type (write) for merge,attr:set calls
- resolve locking issue when new SpaceQuota objects have to be created
- implement a fast and successfull shutdown procedure for the MGM
- implement saveguard for the manager name configurationi in FSTs

``Version V0.3.117 Beryl-Aquamarine``

New Features
------------
- enable read-ahead in FUSE clients to boost performance (default is off - see /etc/sysconfig/eos.example)


``Version V0.3.116 Beryl-Aquamarine``

Bug Fixes
---------
- fix asynchronous egroup refresh query

``Version V0.3.115 Beryl-Aquamarine``

Bug Fixes
---------
- reduce verbosity of eosfsd logging
- support OC special header removing the location header from a WebDAV MOVE response

Bug Fixes
---------
- fix temporary ro master situation when slave reloads namespace when indicated from compacted master (due to stat redirection)

``Version V0.3.114 Beryl-Aquamarine``

Bug Fixes
---------
- fix temporary ro master situation when slave reloads namespace when indicated from compacted master (due to stat redirection)

``Version V0.3.112 Beryl-Aquamarine``

New Features
------------

- add support for nested EGROUPS
- add 'member' CLI to check egroup membership

Bug Fixes
---------
- fix logical quota summary accounting bug
- fix not working 'file version' command for directories with 'sys.versioning=1' configured
- fix order violation bug in 'Drop' implementation which might lead to SEGV

``Version V0.3.111 Beryl-Aquamarine``

Bug Fixes
---------
- redirect "file versions' to the master

``Version V0.3.110 Beryl-Aquamarine``

Bug Fixes
---------
- fix copy constructor of ContainerMD impacting slave following (hiding directory contents on slave)
- fix temp std::string assignment bugs reported by valgrind

``Version V0.3.109 Beryl-Aquamarine``

Bug Fixes
---------
- fix timed read/write locks to use absolute times

``Version V0.3.108 Beryl-Aquamarine``

Bug Fixes
---------
- update Drain/Balancer configuration atleast every minute to allow following master/slave failover and slot reconfiguration

New Features
------------
- support for OC-Checksum field in GET/PUT requests

``Version V0.3.107 Beryl-Aquamarine``

New Features
------------
- support for secondary group evaluation in ACLs (enable secondary groups via /etc/sysonfig/eos:export EOS_SECONDARY_GROUPS=1

``Version V0.3.106 Beryl-Aquamarine``

Bug Fixes
---------
- update MIME types to reflect most recent mappings for office types

``Version V0.3.104 Beryl-Aquamarine``

Bug Fixes
---------
- fix custom namespace parsing for PROPPATCH requests
- allow 'eos cp' to copy files/dirs with $
- fix missing unlock of quota mutex in error return path
- fix mutex inversion in STATLS function

``Version V0.3.102 Beryl-Aquamarine``

Bug Fixes
---------
- fix 'attr' get' function if no attribute links are used
- use '_attr_ls' consistently instead of directy namespace map (to enable links everywhere)
- fix PROPPATCH response to be 'multi-status' 207

``Version V0.3.101 Beryl-Aquamarine``

Bug Fixes
---------
- avoid negative sleep times in scrub loops induced by very slow disks
- apply ANDROID patch for chunked uploads only if 'cbox-chunked-android-issue-900' special header has been added by NGINX proxy
- make MIME type detection case-insensitive

``Version V0.3.100 Beryl-Aquamarine``

New Features
------------
- add online compaction for directories selectable via 'ns compact' (see help)
- support for symbolic attributes 'attr link', 'attr unlink', 'attr fold' to reduce directory memory footprint

Bug Fixes
---------
- fix bug leading to wrong dual master detection after online compaction was running on the master

``Version V0.3.99 Beryl-Aquamarine``

New Features
------------
- allow 'sys.owner.auth=*' to have sticky uid/gids for such directories
- new FST proxy redirection to send file IO through a proxy frontend
- recursive 'rm -r' protection in fuse
- add MIME type suffix detection

Bug Fixes
---------
- remove PrivGuards from Transfer cmds enabling krb5/x509 delegation
- fix HTTP return codes for Put and Range Requests

``Version V0.3.97 Beryl-Aquamarine``

New Features
------------
- forbid 'rm -r' & 'rm -rf' on a predefined tree deepness

Bug Fixes
---------
- various fixes in archive daemon
- improve speed of HTTP HEAD requests with trailing /
- store proxy and client identity properly in VID structure

``Version V0.3.96 Beryl-Aquamarine``

Bug Fixes
---------
- fix -1 bug in 'chown'

New Features
------------
- add dummy responses for LOCK,UNLOCK,PROPPATCH enabling OSX & Windows WebDAV clients
- allow to modifiy only group ownership in chown

``Version V0.3.95 Beryl-Aquamarine``

Bug Fixes
---------
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
------------
- 'find --purge atomic' to clean-up atomic left-over garbage
- allow 'file check fxid:.... | fid:...'
- add 'recycle config --ratio < 0 .. 1.0 >' to set a threadshold based keep ratio in the recycle bin

``Version V0.3.75 Beryl-Aquamarine``

- add support for archive interface to stage-out and migrate a frozen subtree in the namespace to any XRootD enabled archive storage

``Version V0.3.57 Beryl``

New Features
------------
- adding libmicrohttpd build directory
- support threadpool with EPOLL for embedded http server

Bug Fixes
---------
- balancing: was never starting
- scheduler: was skipping scheduling group when one node >95% network-out loaded
- nginx: don't forward PUT payload to MGM
- microhttpd: fix virtual memory leaking due to fragmentation
- http: let HTTP clients see errors on PUT

``Version V0.3.53 Beryl``

New Features
------------
- [webdav] add possibility to exclude directory syncs via 'sys.allow.oc.sync'
- [webdav] add support to do path replacments provdided by two special header flosg CBOX_CLIENT_MAPPING & CBOX_SERVER_MAPPING

``Version V0.3.51 Beryl``

Bug Fixes
---------
- fix gdb stacktrace getting stuck if too much output is produced - stacktrace is stored in /var/eos/md/stacktrace and then reported back into the log
- fix wrong network traffic variable used in the scheduling implementation (used always 0 instead of real traffic)

``Version V0.3.49 Beryl``

Bug Fixes
---------
- rename: allow whitespace names, fix subpath check, fix encofing in HTTP move
- various HTTP/DAV related return code fixes

Consolidation
-------------
- the 'eos' shell by default does not run in 'pipe mode' e.g. no background agent

New Features
------------
- allow FUSE_OPT in /etc/sysconfig/eos e.g. to set a FUSE mount read-only use export FUSE_OPT="ro"
- enable MacOSX build and add packing script for DMG

``Version V0.3.47 Beryl``

Bug Fixes
---------
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
-------------
- FST don't clean-up transactions if their replica is registered in the MGM
- make all HTTP header tags case-insensitive
- HEAD becomes a light-weight operation on large directories
- new unit tests for owncloud/atomic/version support
- improve 'quota ls' performance and bypass uid/gid translations as much as possible
- avoid lock contention in uid/gid translations
- limit the 'gdb' stack trace to maximum 120s to avoid service lock-up in case of a stuck GDB process
- FST never give up in calling a manager for errors allowing a retry

New Features
------------
- update 'eos-deploy' to be able to install from beryl, beryl-testing, aquamarine and citrine YUM repositories
- adjust 'file adjustreplica' and 'file verify' for RAIN files (file verify made RAIN file inaccessible)
- extend 'space reset' command

``Version V0.3.37 Beryl``

- add support for Owncloud chunked upload
- add support for immutable namespace directories
- fix drain/balancing stalls
- fix memory leak introcuded by asynchronous XrdCl messaging
- fix node/fs/group unregistering bug
- make atomic uploads and versioning real 'atomic' operations (no visible state gap between target file exchange)
- add 'file versions' command to show and recall a previous version
- fix tight thread locking delaying start-up

``Version V0.3.35``

Bug Fixes
---------

- modify behaviour on FST commit timeouts - cleanup transaction and keep the replica to avoid unacknowledged commits (replica loss)
- fix output of 'vst ls --io'
- add option 'vst --upd target --self' to publish only the local instance VST statistics to InfluxDB

``Version V0.3.34``

New Features
------------
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
-------------
- FUSE (cache) refactoring & FUSE unit tests
- send all 'monitoring'-like messages purely in async mode (not waiting) for any response e.g. all shared hash states

Bug Fixes
---------
- fix PWD mapping for names starting with numbers
- fix Windows compliance for WebDAV implementation (allprop request)
- fix iterator issue in GeoBalancer and GroupBalancer
- fix balancing starvation bug
- fix 'range requests' in HTTP implementation
- fix embedded HTTP server configuration (thread-per-client model using poll)
- fix S3 escaping for signature checks (make Cyberduck compatible)

``Version V0.3.28``

New Features
------------
- allow FUSE mounts against Master and Slave MGM implementing a new stat function and mkdir/create returning the new inode numbers
- add ETAG to FST GET & PUT requests
- allow to 'grep' for several view objects in fs,node,group,space ls function

Consolidation
-------------
- improve/fix master/slave failover behaviour
- display the correct boot state during slave startup
- improve stack trace to extract responsible stacktrace thread and print again in the end of a log file
- let hotfile display files age and expire
- don't allow to remove nodes which are currently sending heartbeats or have not drained filesystems

Bug Fixes
---------
- fix leak in HTTP access leaving files open
- fix krb5 keytab permission for xrootd 3.3.6-CERN and eos-deploy
- fix sync startup in Slave2Master transition


``Version V0.3.25``

New Features
------------
- allow to match hostnames in VID interface for gateway machines e.g. vid add gateway lxplus* https
- broadcast hotfile list per filesystem to the MGM and add interface to this list via ``io ns -f``
- use inode+checksum for file ETAGs in HTTP, otherwise inode+mtime time - for directories use inode+mtime
- add support for file versioning using attribute ``sys.versioning`` or via shell interface ``file version ..``
- make ApMon more flexible to match individual mountpoints via environment match variable ``APMON_STORAGEPATH`` (try df | grep $APMON_STORAGEPATH).
- eos-deploy script is added to the repository allowing RPM installation of (possibly ALICE enabled) EOS instances with a dual MGM and multi FST setup via a single command
- allow to list files at risk/offline via ``fs status -l <fs-id>``

Consolidation
-------------
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
---------
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
