attr
----

.. code-block:: text

   '[eos] attr ..' provides the extended attribute interface for directories in EOS.
   attr [OPTIONS] ls|set|get|rm ...
   Options:
   attr [-r] ls <path> :
      : list attributes of path
    -r : list recursive on all directory children
   attr [-r] set <key>=<value> <path> :
      : set attributes of path (-r recursive)
   attr [-r] set default=replica|raiddp|raid6|archive <path> :
      : set attributes of path (-r recursive) to the EOS defaults for replicas,dual-parity-raid (4+2), raid-6 (4+2) or archive layouts (5+3).
    -r : set recursive on all directory children
   attr [-r] get <key> <path> :
      : get attributes of path (-r recursive)
    -r : get recursive on all directory children
   attr [-r] rm  <key> <path> :
      : delete attributes of path (-r recursive)
.. code-block:: text

    -r : delete recursive on all directory children
   attr [-r] link <origin> <path> :
      : link attributes of <origin> under the attributes of <path> (-r recursive)
    -r : apply recursive on all directory children
   attr [-r] unlink <path> :
      : remove attribute link of <path> (-r recursive)
    -r : apply recursive on all directory children
   attr [-r] fold <path> :
      : fold attributes of <path> if an attribute link is defined (-r recursive)
      all attributes which are identical to the origin-link attributes are removed locally
    -r : apply recursive on all directory children
   If <key> starts with 'sys.' you have to be member of the sudoer group to see this attributes or modify.
   Administrator Variables:
      sys.forced.space=<space>              : enforces to use <space>    [configuration dependend]
      sys.forced.group=<group>              : enforces to use <group>, where <group> is the numerical index of <space>.<n>    [configuration dependend]
      sys.forced.layout=<layout>            : enforces to use <layout>   [<layout>=(plain,replica)]
      sys.forced.checksum=<checksum>        : enforces to use file-level checksum <checksum>
      <checksum> = adler,crc32,crc32c,md5,sha
      sys.forced.blockchecksum=<checksum>   : enforces to use block-level checksum <checksum>
      <checksuM> = adler,crc32,crc32c,md5,sha
      sys.forced.nstripes=<n>               : enforces to use <n> stripes[<n>= 1..16]
      sys.forced.blocksize=<w>              : enforces to use a blocksize of <w> - <w> can be 4k,64k,128k,256k or 1M
      sys.forced.nouserlayout=1             : disables the user settings with user.forced.<xxx>
      sys.forced.nofsselection=1            : disables user defined filesystem selection with environment variables for reads
      sys.forced.bookingsize=<bytes>        : set's the number of bytes which get for each new created replica
      sys.forced.minsize=<bytes>            : set's the minimum number of bytes a file to be stored must have
      sys.forced.maxsize=<bytes>            : set's the maximum number of bytes a file to be stored can have
      sys.forced.atomic=1                   : if present enforce atomic uploads e.g. files appear only when their upload is complete - during the upload they have the name <dirname>/.<basename>.<uuid>
      sys.mtime.propagation=1               : if present a change under this directory propagates an mtime change up to all parents until the attribute is not present anymore
      sys.allow.oc.sync=1                   : if present, OwnCloud clients can sync pointing to this subtree
      sys.force.atime=<age>                 : enables atime tagging under that directory. <age> is the minimum age before the access time is stored as change time.
      sys.lru.expire.empty=<age>            : delete empty directories older than <age>
      sys.lru.expire.match=[match1:<age1>,match2:<age2>..]
      : defines the rule that files with a given match will be removed if
      they havn't been accessed longer than <age> ago. <age> is defined like 3600,3600s,60min,1h,1mo,y ...
      sys.lru.watermark=<low>:<high>        : if the watermark reaches more than <high> %, files will be removed
      until the usage is reaching <low> %.
      sys.lru.convert.match=[match1:<age1>,match2:<age2>...]
      defines the rule that files with a given match will be converted to the layouts defined by sys.conversion.<match> when their access time reaches <age>.
      sys.stall.unavailable=<sec>           : stall clients for <sec> seconds if a needed file system is unavailable
      sys.heal.unavailable=<tries>          : try to heal an unavailable file for atleast <tries> times - must be >= 3 !!
      - the product <heal-tries> * <stall-time> should be bigger than the expect replication time for a given filesize!
      sys.redirect.enoent=<host[:port]>     : redirect clients opening non existing files to <host[:port]>
      => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eog/public
      sys.redirect.enonet=<host[:port]>     : redirect clients opening unaccessible files to <host[:port]>
      => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eog/public
      sys.recycle=....                      : define the recycle bin for that directory - WARNING: never modify this variables via 'attr' ... use the 'recycle' interface
      sys.recycle.keeptime=<seconds>        : define the time how long files stay in a recycle bin before final deletions taks place. This attribute has to defined on the recycle - WARNING: never modify this variables via 'attr' ... use the 'recycle' interface
      sys.recycle.keepratio=< 0 .. 1.0 >    : ratio of used/max quota for space and inodes in the recycle bin under which files are still kept in the recycle bin even if their lifetime has exceeded. If not defined pure lifetime policy will be applied
      sys.versioning=<n>                    : keep <n> versions of a file e.g. if you upload a file <n+10> times it will keep the last <n+1> versions
      sys.acl=<acllist>                     : set's an ACL which is honored for open,rm & rmdir operations
      => <acllist> = <rule1>,<rule2>...<ruleN> is a comma separated list of rules
      => <rule> = u:<uid|username>|g:<gid|groupname>|egroup:<name>|z:{irwxomqc(!d)(+d)(!u)(+u)}
      e.g.: <acllist="u:300:rw,g:z2:rwo:egroup:eos-dev:rwx,u:500:rwm!d:u:600:rwqc"
      => user id 300 can read + write
      => group z2 can read + write-once (create new files but can't delete)
      => members of egroup 'eos-dev' can read & write & browse
      => user id 500 can read + write into and chmod(m), but cannot delete the directory itself(!d)!
      => user id 600 can read + write and administer the quota node(q) and can change the directory ownership in child directories(c)
      '+d' : this tag can be used to overwrite a group rule excluding deletion via '!d' for certain users
      '+u' : this tag can be used to overwrite a rul excluding updates via '!u'
      'c'  : this tag can be used to grant chown permissions
      'q'  : this tag can be used to grant quota administrator permissions
      e.g.: sys.acl='z:!d' => 'z' is a rule for every user besides root e.g. nobody can delete here'b
      sys.acl='z:i' => directory becomes immutable
      sys.eval.useracl                      : enables the evaluation of user acls if key is defined
      sys.owner.auth=<owner-auth-list>      : set's additional owner on a directory - open/create + mkdir commands will use the owner id for operations if the client is part of the owner authentication list
      sys.owner.auth=*                      : every person with write permission will be mapped to the owner uid/gid pair of the parent directory and quota will be accounted on the owner uid/gid pair
      => <owner-auth-list> = <auth1>:<name1>,<auth2>:<name2  e.g. krb5:nobody,gsi:DN=...
      sys.attr.link=<directory>             : symbolic links for attributes - all attributes of <directory> are visible in this directory and overwritten/extended by the local attributes
      sys.http.index=<path>                 : show a static page as directory index instead of the dynamic one
      => <path> can be a relative or absolute file path!
   User Variables:
      user.forced.space=<space>             : s.a.
      user.forced.layout=<layout>           : s.a.
      user.forced.checksum=<checksum>       : s.a.
      user.forced.blockchecksum=<checksum>  : s.a.
      user.forced.nstripes=<n>              : s.a.
      user.forced.blocksize=<w>             : s.a.
      user.forced.nouserlayout=1            : s.a.
      user.forced.nofsselection=1           : s.a.
      user.forced.atomic=1                  : s.a.
      user.stall.unavailable=<sec>          : s.a.
      user.acl=<acllist>                    : s.a.
      user.versioning=<n>                   : s.a.
      user.tag=<tag>                        : Tag <tag> to group files for scheduling and flat file distribution. Use this tag to define datasets (if <tag> contains space use tag with quotes)
   
   --------------------------------------------------------------------------------
   Examples:
   ...................
   ....... Layouts ...
   ...................
   - set 2 replica as standard layout ...
      |eos> attr set default=replicae /eos/instance/2-replica
   --------------------------------------------------------------------------------
   - set RAID-6 4+2 as standard layout ...
      |eos> attr set default=raid6 /eos/instance/raid-6
   --------------------------------------------------------------------------------
   - set ARCHIVE 5+3 as standard layout ...
      |eos> attr set default=archive /eos/instance/archive
   --------------------------------------------------------------------------------
   - re-configure a layout for different number of stripes (e.g. 10) ...
      |eos> attr set sys.forced.stripes=10 /eos/instance/archive
   ................
   ....... ACLs ...
   ................
   - forbid deletion and updates for group xx in a directory ...
      |eos> attr set sys.acl=g:xx::!d!u /eos/instance/no-update-deletion
   .....................
   ....... LRU Cache ...
   .....................
   - configure a volume based LRU cache with a low/high watermark 
      e.g. when the cache reaches the high watermark it cleans the oldest files untile low-watermark is reached ...
      |eos> quota set -g 99 -v 1T /eos/instance/cache/                           # define project quota on the cache
      |eos> attr set sys.lru.watermark=90:95  /eos/instance/cache/               # define 90 as low and 95 as high watermark
      |eos> attr set sys.force.atime=300 /eos/dev/instance/cache/                # track atime with a time resolution of 5 minutes
   --------------------------------------------------------------------------------
   - configure clean-up of empty directories ...
      |eos> attr set sys.lru.expire.empty="1h" /eos/dev/instance/empty/          # remove automatically empty directories if they are older than 1 hour
   --------------------------------------------------------------------------------
   - configure a time based LRU cache with an expiration time ...
      |eos> attr set sys.lru.expire.match="*.root:1mo,*.tgz:1w"  /eos/dev/instance/scratch/
      # files with suffix *.root get removed after a month, files with *.tgz after one week
      |eos> attr set sys.lru.expire.match="*:1d" /eos/dev/instance/scratch/      # all files older than a day are automatically removed
   --------------------------------------------------------------------------------
   - configure automatic layout conversion if a file has reached a defined age ...
      |eos> attr set sys.lru.convert.match="*:1mo" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next
      |eos> attr set sys.conversion.*=20640542 /eos/dev/instance/convert/          # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2
   --------------------------------------------------------------------------------
   - configure automatic layout conversion if a file has not been used during the last 6 month ...
      |eos> attr set sys.force.atime=1w /eos/dev/instance/cache/                   # track atime with a time resolution of one week
      |eos> attr set sys.lru.convert.match="*:6mo" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next
      |eos> attr set sys.conversion.*=20640542  /eos/dev/instance/convert/         # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2
   --------------------------------------------------------------------------------
   .......................
   ....... Recycle Bin ...
   .......................
   - configure a recycle bin with 1 week garbage collection and 100 TB space ...
      |eos> recycle config --lifetime 604800                                     # set the lifetime to 1 week
      |eos> recycle config --size 100T                                           # set the size of 100T
      |eos> recycle config --add-bin /eos/dev/instance/                          # add's the recycle bin to the subtree /eos/dev/instance
   .......................
   .... Atomic Uploads ...
   .......................
      |eos> attr set sys.forced.atomic=1 /eos/dev/instance/atomic/
   .......................
   .... Attribute Link ...
   .......................
      |eos> attr set sys.attr.link=/eos/dev/origin-attr/ /eos/dev/instance/attr-linked/
