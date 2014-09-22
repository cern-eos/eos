.. highlight:: rst

.. index::
   single: Beryl(-Aquamarine)-Release

Beryl Release Notes
===================
``Version V0.3.42 Beryl-Aquamarine``
- add support for archive interface to stage-out and migrate a frozen subtree in the namespace to any XRootD enabled archive storage

``Version V0.3.43 Beryl``
- FST don't clean-up transactions if their replica is registered in the MGM
- bugfixes in HTTP daemon configuration/startup
- make all HTTP header tags case-insensitive
- HEAD becomes a light-weight operation on large directories
- many bugfixes for owncloud/atomic/version support
- many bugfixes for mutex order violations
- new unit tests for owncloud/atomic/version support

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



