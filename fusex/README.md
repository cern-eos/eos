eosxd
=====

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
  "mdcachedir" : "/var/eos/fusex/md",
  "mdzmqtarget" : "tcp://localhost:1100",
  "mdzmqidentity" : "eosxd",

  "options" : {
    "debug" : 1,
    "debuglevel" : 4,
    "libfusethreads" : 0,
    "md-kernelcache" : 1,
    "md-kernelcache.enoent.timeout" : 0.01,
    "md-backend.timeout" : 86400, 
    "md-backend.put.timeout" : 120, 
    "data-kernelcache" : 1,
    "mkdir-is-sync" : 1,
    "create-is-sync" : 1,
    "symlink-is-sync" : 1,
    "rename-is-sync" : 1,
    "rmdir-is-sync" : 0,
    "global-flush" : 0,
    "flush-wait-open" : 1,
    "global-locking" : 1, 
    "fd-limit" : 65536,
    "no-fsync" : [ ".db", ".db-journal", ".sqlite", ".sqlite-journal", ".db3", ".db3-journal", "*.o" ], 
    "overlay-mode" : 000, 
    "rm-rf-protect-levels" : 1,
    "show-tree-size" : 0,
    "free-md-asap" : 1,
    "cpu-core-affinity" : 1,
    "no-xattr" : 1 
  },
  "auth" : {
    "shared-mount" : 1,
    "krb5" : 1,
    "environ-deadlock-timeout" : 100, 
    "forknoexec-heuristic" : 1
  },
}
```

You also need to define a local cache directory (location) where small files are cached and an optional journal directory to improve the write speed (journal).

```
  "cache" : {
    "type" : "disk",
    "size-mb" : 1000,
    "location" : "/var/eos/fusex/cache/",
    "journal" : "/var/eos/fusex/journal/",
    "read-ahead-strategy" : "dynamic",
    "read-ahead-bytes-nominal" : 1048576,
    "read-ahead-bytes-max" : 8388608
  }

```

The available read-ahead strategies are 'dynamic', 'static' or 'none'. Dynamic read-ahead doubles the read-ahead window from nominal to max if the strategy provides cache hits.

The daemon automatically appends a directory to the mdcachedir, location and journal path and automatically creates these directory private to root (mode=700).

You can modify some of the XrdCl variables, however it is recommended not to change these:

```
  "xrdcl" : {
    "TimeoutResolution" : 1,
    "ConnectionWindow": 10,
    "ConnectionRetry" : 0,
    "StreamErrorWindow" : 30,
    "RequestTimeout" : 15,
    "StreamTimeout" : 30,
    "RedirectLimit" : 3,
    "LogLevel" : "None"
  },

```

The recovery settings are defined in the following section:

```
   "recovery" : {
     "read-open" : 1,
     "read-open-noserver" : 1,
     "read-open-noserver-retrywindow" : 86400,
     "write-open" : 1,
     "write-open-noserver" : 1,
     "write-open-noserver-retrywindow" : 86400
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

To run as an export gateway one needs to configure 'stable inodes'. In the configuration file one can specify the 'mdcachedir' directive pointing to a directory where a ROCKSDB database will be stored. There is however a much simpler method to get an WebServer/NFS ready mount. Just use the normal mount or AUTOFS configuration but prefix '-ofsname=eos.cern.ch' like '-ofsname=gw@eos.cern.ch', which will automatically enable the stable inodes option. To work around an issue with Sambe and EOS Acls, use '-ofsname=smb@eos.cern.ch' to enable additionally the overlay-mode flag. You CANNOT enable the kernel meta data cache when using the NFS kernel daemon because the invalidation kernel upcall creates permission denied errors on NFS client side.

```
# WebServer, NFS etc...
eosxd -ofsname=gw@eos.cern.ch:/eos/user/ /eos/user/

# CIFS(Samba) 
eosxd -ofsname=smb@eos.cern.ch:/eos/user/ /eos/user/
```


Client Interaction with a FUSE mount
------------------------------------

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