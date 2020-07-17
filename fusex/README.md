eosxd
=====

Warning
-------

To have *eosxd* working properly with many writers you have to modify the MGM configuration file ```/etc/xrd.cf.mgm``` with the nolock option: ```all.export / nolock```

Configuration File
------------------
The configuration file name for an unnamed instance is ```/etc/eos/fuse.conf```.
The configuration file for a named instance is ```/etc/eos/fuse.<name>.conf```.

You can select a named instance adding ```'-ofsname=<name>'``` to the argument list.

This

```
{
  "name" : "",
  "hostport" : "localhost:1094",
  "remotemountdir" : "/eos/",
  "localmountdir" : "/eos/",
  "statisticfile" : "stats",
  "mdcachedir" : "/var/cache/eos/fusex/md",
  "mdzmqtarget" : "tcp://localhost:1100",
  "mdzmqidentity" : "eosxd",
  "appname" : "",
  "options" : {
    "debug" : 1,
    "debuglevel" : 4,
    "backtrace" : 1,
    "libfusethreads" : 0,
    "hide-versions" : 1,
    "md-kernelcache" : 1,
    "md-kernelcache.enoent.timeout" : 0,
    "md-backend.timeout" : 86400,
    "md-backend.put.timeout" : 120,
    "data-kernelcache" : 1,
    "rename-is-sync" : 1,
    "rmdir-is-sync" : 0,
    "global-flush" : 0,
    "flush-wait-open" : 1, // 1 = flush waits for open when updating - 2 = flush waits for open when creating - 0 flush never waits
    "flush-wait-open-size" : 262144 , // file size for which we force to wait that files are opened on FSTs
    "flush-wait-umount" : 120, // seconds to wait for write-back data to be flushed out before terminating the mount - 0 disables waiting for flush
    "flush-nowait-executables" : [ "/tar", "/touch" ],
    "global-locking" : 1,
    "fd-limit" : 524288,
    "no-fsync" : [ ".db", ".db-journal", ".sqlite", ".sqlite-journal", ".db3", ".db3-journal", "*.o" ],
    "overlay-mode" : "000",
    "rm-rf-protect-levels" : 0,
    "rm-rf-bulk" : 0,
    "show-tree-size" : 0,
    "cpu-core-affinity" : 1,
    "no-xattr" : 1,
    "no-link" : 0,
    "nocache-graceperiod" : 5,
    "leasetime" : 300,
    "write-size-flush-interval" : 10,
    "submounts" : 0,
    "inmemory-inodes" : 16384  
  },
  "auth" : {
    "shared-mount" : 1,
    "krb5" : 1,
    "gsi-first" : 0,
    "sss" : 1,
    "ssskeytab" : "/etc/eos/fuse.sss.keytab",
    "oauth2" : 1,
    "environ-deadlock-timeout" : 100,
    "forknoexec-heuristic" : 1
  },
  "inline" : {
    "max-size" : 0,
    "default-compressor" : "none"
  },
  "fuzzing" : {
    "open-async-submit" : 0,
    "open-async-return" : 0,
    "open-async-submit-fatal" : 0,
    "open-async-return-fatal" : 0,
    "read-async-submit" : 0
  }
}
```

You also need to define a local cache directory (location) where small files are cached and an optional journal directory to improve the write speed (journal).

```
  "cache" : {
    "type" : "disk",
    "size-mb" : 512,
    "size-ino" : 65536,
    "journal-mb" : 2048,
    "journal-ino" : 65536,
    "clean-threshold" : 85.0,
    "location" : "/var/cache/eos/fusex/cache/",
    "journal" : "/var/cache/eos/fusex/journal/",
    "read-ahead-strategy" : "static",
    "read-ahead-bytes-nominal" : 262144,
    "read-ahead-bytes-max" : 2097152,
    "read-ahead-blocks-max" : 16,
    "max-read-ahead-buffer" : 134217728,
    "max-write-buffer" : 134217728
  }

```

The available read-ahead strategies are 'dynamic', 'static' or 'none'. Dynamic read-ahead doubles the read-ahead window from nominal to max if the strategy provides cache hits. The default is a dynamic read-ahead starting with 512kb and using 2,4,8,16 blocks resizing blocks up to 2M.

The daemon automatically appends a directory to the mdcachedir, location and journal path and automatically creates these directory private to root (mode=700).

You can modify some of the XrdCl variables, however it is recommended not to change these:

```
  "xrdcl" : {
    "TimeoutResolution" : 1,
    "ConnectionWindow": 10,
    "ConnectionRetry" : 0,
    "StreamErrorWindow" : 60,
    "RequestTimeout" : 30,
    "StreamTimeout" : 60,
    "RedirectLimit" : 3,
    "LogLevel" : "None"
  },

```

The recovery settings are defined in the following section:

```
   "recovery" : {
     "read-open" : 1,
     "read-open-noserver" : 1,
     "read-open-noserver-retrywindow" : 15,
     "write-open" : 1,
     "write-open-noserver" : 1,
     "write-open-noserver-retrywindow" : 15
   }
```

Configuration default values and avoiding configuration files
-------------------------------------------------------------

Every configuration value has a corresponding default value .
As explained the configuration file name is taken from the fsname option given on the command line:

```
root> eosxd -ofsname=foo loads /etc/eos/fuse.foo.conf
root> eosxd              loads /etc/eos/fuse.conf

user> eosxd -ofsname=foo loads $HOME/.eos/fuse.foo.conf
```

One can avoid to use configuration files if the defaults are fine providing the remote host and remote mount directory via the fsname:

```
root> eosxd -ofsname=eos.cern.ch:/eos/ $HOME/eos # mounts the /eos/ directory from eos.cern.ch shared under $HOME/eos/

user> eosxd -ofsname=user@eos.cern.ch:/eos/user/u/user/ $home/eos # mounts /eos/user/u/user from eos.cern.ch private under $HOME/eos/
```

If this is a user-private mount the syntax 'foo@cern.ch' should be used to distinguish private \
mounts of individual users in the 'df' output

Please note, that root mounts are by default shared mounts with kerberos configuration, user mounts are private mounts with kerberos configuration

Statistics File
---------------

The *stat* file contains rate and execution average time counting.

```
bash> cat /var/log/eos/fusex/fuse.stats
ALL      Execution Time                   0.00 +- 0.00
# -----------------------------------------------------------------------------------------------------------
who      command                          sum             5s     1min     5min       1h exec(ms) +- sigma(ms)
# -----------------------------------------------------------------------------------------------------------
ALL        :sum                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        access                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        create                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        flush                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        forget                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        fsync                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        getattr                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        getxattr                                    0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        listxattr                                   0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        lookup                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        mkdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        mknod                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        open                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        opendir                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        read                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        readdir                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        readlink                                    0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        release                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        releasedir                                  0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        removexattr                                 0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        rename                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        rm                                          0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        rmdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        setattr                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        setattr:chmod                               0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        setattr:chown                               0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        setattr:truncate                            0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        setattr:utimes                              0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        setxattr                                    0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        statfs                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        symlink                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        unlink                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-
ALL        write                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
# -----------------------------------------------------------------------------------------------------------
ALL        inodes              := 1
ALL        inodes-todelete     := 0
ALL        inodes-backlog      := 0
ALL        inodes-ever         := 1
ALL        inodes-ever-deleted := 0
ALL        inodes-open         := 0
ALL        inodes-vmap         := 1
ALL        inodes-caps         := 0
# -----------------------------------------------------------------------------------------------------------
ALL        threads             := 17
ALL        visze               := 336.41 Mb
All        rss                 := 53.10 Mb
All        wr-buf-inflight     := 0 b
All        wr-buf-queued       := 0 b
All        ra-buf-inflight     := 0 b
All        ra-buf-queued       := 0 b
All        rd-buf-inflight     := 0 b
All        rd-buf-queued       := 0 b
All        version             := 4.2.11
ALl        fuseversion         := 28
All        starttime           := 1517583072
All        uptime              := 1
All        instance-url        := apeters.cern.ch
# -----------------------------------------------------------------------------------------------------------
```

Mounting with configuration files
---------------------------------

```
# mount on /eos/
mount -t fuse eosxd /eos/

# umount /eos/
umount -f /eos/

# run the default mount in foreground mode
eosxd -f

# run the default mount to a different mount directory from the JSON configuration
eosxd -f /other/

# run the default mount in background mode
eosxd

# mount without configuration files and default values
mount -t fuse -ofsname=eos.cern.ch:/eos/scratch /eos/scratch

# run without configuration files in foreground
eosxd -ofsname=eos.cern.ch:/eos/scratch /eos/scratch

# run a usermount without configuration in background
eosxd -ofsname=me@eos.cern.ch:/eos/user/m/me/ $HOME/eos/

```

Mounting with sss credentials
-----------------------------

Use these authentication directives in the config file:
```
  "auth" : {
    "shared-mount" : 1,
    "krb5" : 0,
    "gsi-first" : 1,
    "sss" : 1
  }
```
The mount daemon uses /etc/fuse/fuse.sss.keytab as default keytab when running as a shared mount. The user mount default is $HOME/.eos/fuse.sss.keytab. Unlike Kerberos it is not possible in XRootD to use different keytabs for individual users. If you want to create a 'trusted' mount mapping local users to their local username, you have to create an sss keytab entry for user **anybody** and group **anygroup**. Otherwise you can create an sss keytab for a given application user.
The mount also supports to forward sss endorsements, which are forwarded to the server. These endorsement can be used server-side to define an ACL entry by key e.g. sys.acl="k:9c2bd333-5331-4095-8fcd-28726404742f:rwx". This would provide access to all sss clients having this key in their environment even if the mapped sss user/group wouldn't have access.


AUTOFS Configuration
--------------------

Make sure you have in /etc/autofs.conf :
```
browse_mode = yes
```
Add this line to /etc/auto.master to configure automount for the directory /eos/ :
```
/eos/  /etc/auto.eos
```
Create the directory /eos (should be empty).

Create the file /etc/auto.eos to mount f.e. from instance eos.cern.ch the path /eos/user/ under /eos/scratch :
```
scratch -fstype=eosx,fsname=eos.cern.ch:/eos/user/ :eosxd
```


Web/NFS/Samba Gateway Configuration
-----------------------------------

To run eosxd for gateways you can specify the gateway type. eosxd will optimize internal settings for the referenced gateway type.

```
# WebServer, NFS etc...
eosxd -ofsname=gw@eos.cern.ch:/eos/user/ /eos/user/

# CIFS(Samba)
eosxd -ofsname=smb@eos.cern.ch:/eos/user/ /eos/user/
```


Client Interaction with a FUSE mount
------------------------------------

eosxd provides a command line interface to interact with mounts (see eosxd -h):

```
# eosxd -h 
usage CLI   : eosxd get <key> [<path>]

                     eos.btime <path>                   : show inode birth time
                     eos.ttime <path>                   : show lastest mtime in tree
                     eos.tsize <path>                   : show size of directory tree
                     eos.dsize <path>                   : show total size of files inside a directory
		     eos.checksum <path>                : show path checksum if defined
                     eos.name <path>                    : show EOS instance name for given path
                     eos.md_ino <path>                  : show inode number valid on MGM 
                     eos.hostport <path>                : show MGM connection host + port for given path
                     eos.mgmurl <path>                  : show MGM URL for a given path
                     eos.stats <path>                   : show mount statistics
                     eos.stacktrace <path>              : test thread stack trace functionality
                     eos.quota <path>                   : show user quota information for a given path
                     eos.reconnect <mount>              : reconnect and dump the connection credentials
                     eos.reconnectparent <mount>        : reconnect parent process and dump the connection credentials
                     eos.identity <mount>               : show credential assignment of the calling process
                     eos.identityparent <mount>         : show credential assignment of the executing shell

 as root             system.eos.md  <path>              : dump meta data for given path
                     system.eos.cap <path>              : dump cap for given path
                     system.eos.caps <mount>            : dump all caps
                     system.eos.vmap <mount>            : dump virtual inode translation table

usage CLI   : eosxd set <key> <value> [<path>]

 as root             system.eos.debug <level> <mount>   : set debug level with <level>=notice|info|debug
                     system.eos.dropcap - <mount>       : drop capability of the given path
                     system.eos.dropcaps - <mount>      : drop call capabilities for given mount
                     system.eos.resetstat - <mount>     : reset the statistic counters
                     system.eos.log <mode> <mount>      : make log file public or private with <mode>=public|private

usage FS    : eosxd -ofsname=<host><remote-path> <mnt-path>
                     eosxd -ofsname=<config-name> <mnt-path>
                        with configuration file /etc/eos/fuse.<config-name>.conf
                     mount -t fuse eosxd -ofsname=<host><remote-path> <mnt-path>
                     mount -t fuse eosxd -ofsname=<config-name> <mnt-path>

usage HELP  : eosxd [-h|--help|help]                    : get help

```

The CLI uses the following extended attribute interfaces internally:

To change the log configuration do as root:
```
# setfattr -n system.eos.debug -v info <path>
# setfattr -n system.eos.debug -v debug <path>
# setfattr -n system.eos.debug -v notice <path>
```

To display the local meta data record do as root
```
# getfattr --only-values -n system.eos.md <path>
```

To display a capability on a path do as root
```
# getfattr --only-values -n system.eos.cap <path>
```

To display a list of all capabilities on a path do as root
```
# getfattr --only-values -n system.eos.caps <any-path>
```

To display a list of local to remote inode translations
```
# getfattr --only-values -n system.eos.vmap <any-path>
```

To drop a capability on a path do as root
```
# setfattr -n system.eos.dropcap <path>
```

To drop all capabilities on a mount do as root
```
# setfattr -n system.eos.dropallcap <any-path>
```

Show all hidden system attributes on a given path
```
# getfattr -d -m - <path>
```

Inspect which set of credentials have been assigned to the calling process:
```
getfattr --only-values -n eos.identity <any-path>
```

Inspect which set of credentials have been assigned to the parent of the calling process,
in this case this would be the shell:
```
getfattr --only-values -n eos.identityparent <any-path>
```

Invalidate current set of credentials assigned to the calling process, and reconnect
while printing a detailed log of what's happening:
```
getfattr --only-values -n eos.reconnect <any-path>
```

Invalidate current set of credentials assigned to the parent of the calling process
(in this case, the shell), and reconnect while printing a detailed log of what's happening.
```
getfattr --only-values -n eos.reconnectparent <any-path>
```

Reset the statis counters on a mount as root

# setfattr -n system.eos.resetstat <any-path>

Virtual extended attributes on a FUSE mount
-------------------------------------------

Display instance name
```
# getfattr --only-values -n eos.name /eos/
```

Display MGM hostname+port
```
# getfattr --only-values -n eos.hostport /eos/
```

Display MGM url
```
# getfattr --only-values -n eos.mgmurl /eos/
```

Display Quota Information for a given path
```
# getfattr --only-values -n eos.quota <path>
```

Display Checksum value for a given path
```
# getfattr --only-values -n eos.checksum <path>
```

Server Interaction with a FUSE mount
------------------------------------


EOS Console [root://localhost] |/eos/dev/fusetest/workspace/senf/> fusex
usage: fusex ls [-c] [-n] [-z] [-a] [-m] [-s]                        :  print statistics about eosxd fuse clients
                -c                                                   -  break down by client host
                -a                                                   -  print all
                -s                                                   -  print summary for clients
                -m                                                   -  print in monitoring format <key>=<value>

       fuxex evict <uuid> [<reason>]                                 :  evict a fuse client
                                                              <uuid> -  uuid of the client to evict
                                                            <reason> -  optional text shown to the client why he has been evicted

       fusex dropcaps <uuid>                                         :  advice a client to drop all caps

       fusex droplocks <uuid>                                        :  advice a client to drop all locks

       fusex caps [-t | -i | -p [<regexp>] ]                         :  print caps
                -t                                                   -  sort by expiration time
                -i                                                   -  sort by inode
                -p                                                   -  display by path
                -t|i|p <regexp>>                                     -  display entries matching <regexp> for the used filter type
examples:
           fusex caps -i ^0000abcd$                                  :  show caps for inode 0000abcd
           fusex caps -p ^/eos/$                                     :  show caps for path /eos
           fusex caps -p ^/eos/caps/                                 :  show all caps in subtree /eos/caps


Allow traversing of directories without 'x' mode
-------------------------------------------------

To allow on a mount to cd into directories without 'x' mode bit for the user you can define 'overlay-mode' to allow the access & stat function to work for directories without 'x' bit.
This is necessary e.g. on a Samba mount to reach a subfolder, which you made accessibla to another person without granting 'x' mode on all parent folders. It is enough to use "1" as overlay mode.
