eosxd
=====

Configuration File
------------------

```
{
  "name" : "",
  "hostport" : "localhost:1094",
  "remotemountdir" : "/eos/",
  "localmountdir" : "/eos/",
  "statisticfile" : "stats",
  "mdcachehost" : "localhost",
  "mdcacheport" : 6379,
  "options" : {
    "debug" : 1,
    "lowleveldebug" : 0,
    "debuglevel" : 6
  }
}
```


Statistics File
---------------

The *stat* file contains rate and execution average time counting.

```
bash> cat /var/log/eos/fusex/fuse.stats
 
ALL      Execution Time                   0.02 +- 0.01
# -----------------------------------------------------------------------------------------------------------
who      command                          sum             5s     1min     5min       1h exec(ms) +- sigma(ms) 
# -----------------------------------------------------------------------------------------------------------
ALL        access                                      0     0.00     0.00     0.00     0.00  0.00316 +- 0.00401   
ALL        create                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        flush                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        forget                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        fsync                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        getattr                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        getxattr                                    0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        listxattr                                   0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        lookup                                      0     0.00     0.00     0.00     0.00  0.01622 +- 0.00867   
ALL        mkdir                                       0     0.00     0.00     0.00     0.00  0.03578 +- 0.01038   
ALL        mknod                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        open                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        opendir                                   100     0.00     0.00     0.00     0.03  0.02275 +- 0.00703   
ALL        read                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        readdir                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        readlink                                    0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        release                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        releasedir                                 12     0.00     0.00     0.00     0.00  0.00650 +- 0.00328   
ALL        removexattr                                 0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        rename                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        rm                                          0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        rmdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        setattr                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        setxattr                                    0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        statfs                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        symlink                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        unlink                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-      
ALL        write                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-
```


Startup
-------

```
# mount on /eos/
eosxd /eos/
```


