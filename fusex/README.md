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

Startup
-------

```
# mount on /eos/
eosxd /eos/
```


