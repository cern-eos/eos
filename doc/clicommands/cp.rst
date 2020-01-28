cp
--

.. code-block:: text

  cp [--async] [--atomic] [--rate=<rate>] [--streams=<n>] [--depth=<d>] [--checksum] [--no-overwrite|-k] [--preserve|-p] [--recursive|-r|-R] [-s|--silent] [-a] [-n] [-S] [-d[=][<lvl>] <src> <dst>
  '[eos] cp ..' provides copy functionality to EOS.
    <src>|<dst> can be root://<host>/<path>, a local path /tmp/../ or an eos path /eos/ in the connected instance
  Options:
    --async         : run an asynchronous transfer via a gateway server (see 'transfer submit --sync' for the full options)
    --atomic        : run an atomic upload where files are only visible with the target name when their are completely uploaded [ adds ?eos.atomic=1 to the target URL ]
    --rate          : limit the cp rate to <rate>
    --streams       : use <#> parallel streams
    --depth         : depth for recursive copy
    --checksum      : output the checksums
    -a              : append to the target, don't truncate
    -p              : create destination directory
    -n              : hide progress bar
    -S              : print summary
    -d | --debug          : enable debug information (optional <lvl>=1|2|3)
    -s | --silent         : no output outside error messages
    -k | --no-overwrite   : disable overwriting of files
    -P | --preserve       : preserves file creation and modification time from the source
    -r | -R | --recursive : copy source location recursively
.. code-block:: text

  Remark: 
    If you deal with directories always add a '/' in the end of source or target paths e.g. if the target should be a directory and not a file put a '/' in the end. To copy a directory hierarchy use '-r' and source and target directories terminated with '/' !
  Examples: 
    eos cp /var/data/myfile /eos/foo/user/data/                   : copy 'myfile' to /eos/foo/user/data/myfile
    eos cp /var/data/ /eos/foo/user/data/                         : copy all plain files in /var/data to /eos/foo/user/data/
    eos cp -r /var/data/ /eos/foo/user/data/                      : copy the full hierarchy from /var/data/ to /eos/foo/user/data/ => empty directories won't show up on the target!
    eos cp -r --checksum --silent /var/data/ /eos/foo/user/data/  : copy the full hierarchy and just printout the checksum information for each file copied!
  S3:
    URLs have to be written as:
    as3://<hostname>/<bucketname>/<filename> as implemented in ROOT
    or as3:<bucketname>/<filename> with environment variable S3_HOSTNAME set
    and as3:....?s3.id=<id>&s3.key=<key>
    The access id can be defined in 3 ways:
    env S3_ACCESS_ID=<access-id>          [as used in ROOT  ]
    env S3_ACCESS_KEY_ID=<access-id>      [as used in libs3 ]
    <as3-url>?s3.id=<access-id>           [as used in EOS transfers ]
    The access key can be defined in 3 ways:
    env S3_ACCESS_KEY=<access-key>        [as used in ROOT ]
    env S3_SECRET_ACCESS_KEY=<access-key> [as used in libs3 ]
    <as3-url>?s3.key=<access-key>         [as used in EOS transfers ]
    If <src> and <dst> are using S3, we are using the same credentials on both ends and the target credentials will overwrite source credentials!
