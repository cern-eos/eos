.. index::
   pair: Blog; New Features

.. highlight:: rst

.. _features:


=====
BLOG
=====

New Features
-------------

This blog is used to track new features added to EOS.

September 2023
^^^^^^^^^^^^^^

* FSTs now publish regulary S.M.A.R.T information and the MGM can provide statistics by storage device or model categories including age, capacity etc.
  The information is available using the new devices interface:

  `eos devices ls [-l]`

  All the information is also available as JSON output using `eos --json devices ls`! The feature will be available with EOS 5.2!

* `eos df` supports now JSON output e.g. `eos --json df` and prints the performance capacity ratio in GB/s per TB
  
August 2023
^^^^^^^^^^

* The EOS group balancer had been rewritten during 2023 and now supports a 'free space engine'
  Instead of balancing groups to the same usage level (percentage),
  groups are balanced to have the same amount of free space. This allows
  to avoid significant performance degradation when an EOS instance is
  close to be full. 

June 2023
^^^^^^^^^

* The EOS MGM provides now a meta-data benchmark command to measure MGM performance
   Example:

   `eos ns benchmark 100 10 10 # see the help in the CLI`

   The tool got added when validating the shared mutex implementation of the Abseil library
   as a new namespace mutex implementation, which performs much better than the
   standard C++ implementation. The new mutex will be available with EOS 5.2!
   
May 2023
^^^^^^^^

* Support external FST hostname (port) aliases by defining
   * sysconfig: EOS_FST_ALIAS=externalhostname.domain
   * sysconfig: EOS_FST_PORT_ALIAS=externalport (normally the same)

  The aliases are shown when using
   * `eos fs ls -l`
   * `eos fs ls -m`

.. note:: When an alias is removed on an FST, the MGM and FST have to be shutdown and restarted to get rid of the aliases


March 2023
^^^^^^^^^^^^^

* HTTPS gateways can use now a shared key to act as gateway for an EOS instance. This simplifies deployment and removes the need
  to specify the IPV6 addresses of each gateway node using the vid interface.

  Example:
  * `vid set map -https key:e9d2011d-942a-4ec1-9f78-3643316ed336 vuid:100`
  * `vid set map -https key:e9d2011d-942a-4ec1-9f78-3643316ed336 vgid:100`
  * If a client sends the header "x-gateway-authorization:  e9d2011d-942a-4ec1-9f78-3643316ed336" he will be authenticated as user 100 in this example.
  * If user 100 is a sudoer, the client can select the effective user ID specifying a "remote-user: ..." header.

* `newfind` has now improved performance and new usability features.

  Examples:
  * `eos newfind --format uid,gid,checksum,fxid,atime,btime /eos/ #select output format`
  * `eos newfind --count -d /eos/dev/ #better performance`
  * `eos newfind --treecount /eos/dev/ #aggregate number of dirs and files`
  * `eos newfind --cache ... #store items in in-memory MD cache, by default we don't cache meta-data to avoid trashing`


January 2023
^^^^^^^^^^^^^


* Token identity mapping can now be controlled using `eos vid tokensudo always|strong|encrypted|never`
   * `always` - identity in the token is always taken into account
   * `strong` - identity in the token is not taken into account for unix authenticated clients
   * `encrypted` - identity in the token is only taken into account for encrypted connections
   * `never` - identity in the token is never taken into account

* It is now possible to black-/whitelist EOS tokens using the _access__ interface. This allows e.g. to prevent arbitrary token generation by users and implement an approval process. When using the white list mode, all user created tokens appear in the logfile `/var/log/eos/mgm/TokenCmd.log` e.g.

.. code-block:: bash 

   230113 11:17:19 WARN  TokenCmd:218                   creating voucher=7630eb7e-932b-11ed-8d40-0071c2181e97 path=/eos/foo/ owner=123 group=123 perm=rx expires=1673605339 token:'{ "token": {  "permission": "rx",  "expires": "1673605339",  "owner": "bar",  "group": "bar",  "generation": "1",  "path": "/eos/foo/",  "allowtree": true,  "vtoken": "",  "origins": [] },}'

An admin can now whitelist this token by issuing:
.. code-block:: bash 

   eos access allow token 7630eb7e-932b-11ed-8d40-0071c2181e97

In blacklist mode it is possible to disable token usage if required using:

.. code-block:: bash 

   eos access ban 7630eb7e-932b-11ed-8d40-0071c2181e97


December 2022
^^^^^^^^^^^^^

* The file inspector daemon now reports access time and birth time distributions:

.. code-block:: bash 

    inspector -l
    ...
    ======================================================================================
     Access time distribution of files
     0s                               : 1613 (1.59%)
     24h                              : 6 (0.01%)
     7d                               : 1 (0.00%)
     30d                              : 1 (0.00%)
     2y                               : 5 (0.00%)
     5y                               : 100.02 k (98.40%)
    ======================================================================================
     Access time volume distribution of files
     0s                               : 81.31 MB (98.73%)
     24h                              : 15.09 kB (0.02%)
     7d                               : 0 B (0.00%)
     30d                              : 1.00 MB (1.21%)
     2y                               : 10.49 kB (0.01%)
     5y                               : 24.27 kB (0.03%)
    ======================================================================================
     Birth time distribution of files
     0s                               : 1619 (1.59%)
     24h                              : 6 (0.01%)
     7d                               : 100.00 k (98.39%)
     90d                              : 1 (0.00%)
     5y                               : 13 (0.01%)
    ======================================================================================
     Birth time volume distribution of files
     0s                               : 81.32 MB (98.74%)
     24h                              : 1.01 MB (1.23%)
     7d                               : 25 B (0.00%)
     90d                              : 2769 B (0.00%)
     5y                               : 21.48 kB (0.03%)
    --------------------------------------------------------------------------------------
    
    inspector -m
    key=last layout=00000000 type=plain nominal_stripes=1 checksum=none blockchecksum=none blocksize=4k locations=0 nolocation=12 repdelta:-1=12 unlinkedlocations=0 volume=20480 zerosize=7
    key=last layout=00100002 type=plain nominal_stripes=1 checksum=adler32 blockchecksum=none blocksize=4k locations=101628 nolocation=1 repdelta:-1=1 repdelta:0=101628 unlinkedlocations=0 volume=82338570 zerosize=100003
    kay=last tag=accesstime::files 0=1613 86400=6 604800=1 2592000=1 63072000=5 157680000=100015
    key=last tag=accesstime::volume 0=81309191 86400=15090 604800=0 2592000=1000000 63072000=10495 157680000=24274
    kay=last tag=birthtime::files 0=1619 86400=6 604800=100002 7776000=1 157680000=13


------------

* It is now possible to enable access time tracking e.g. with 1h precision:

.. code-block:: bash 

   eos space config default atime=3600

------------

* Supporting now secondary group permission evaluation with sysconfig setting `EOS_SECONDARY_GROUPS=1`

------------

* `eos register` is a new command which can be used to _inject_ meta-data into EOS

.. code-block:: bash 

   Usage: register [-u] <path> {tag1,tag2,tag3...}
              :  when called without the -u flag the parent has to exist while the basename should not exist
           -u :  if the file exists this will update all the provided meta-data of a file
    
           tagN is optional, but can be one or many of:
                 size=100
                 uid=101 | username=foo
                 gid=102 | username=bar
                 checksum=abcdabcd
                 layoutid=00100112
                 location=1 location=2 ...
                 mode=777
                 btime=1670334863.101232
                 atime=1670334863.101232 
                 ctime=1670334863.110123
                 mtime=1670334863.11234d
                 attr="sys.acl=u:100:rwx"
                 attr="user.md=private"
                 path="/eos/newfile"   # can be used instead of the regular path argument of the path

* `eos ns` reports now a read and write contention value 

.. code-block:: bash 

    eos ns stat:
    ALL      Contention                  :     write:42.11% read:0.00%
    
    eos ns stat -m | grep contention
    uid=all gid=all ns.contention.read=42.11
    uid=all gid=all ns.contention.write=0


------------


November 2022
^^^^^^^^^^^^^

* Added a sharded cache for the ID mapping interface to get better parallelism

------------

* Shipping now *eosxd* based on libfuse2 and *eosxd3* on libfuse3
   * eosxd3 can be started using `-o clone_fd` to have one FUSE connection per thread

------------

* Support for the POSIX VTX bit has been added (e.g. as it is used in /tmp/)
