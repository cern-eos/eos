quota
-----

.. code-block:: text

  Usage:
    quota [<path>]                                                       : show personal quota for all or only the quota node responsible for <path>
    quota ls [-n] [-m] [-u <uid>] [-g <gid>] [[-p] <path>]               : list configured quota and quota node(s)
    quota rm -u <uid>|-g <gid> [-v] [-i] [[-p] <path>]                   : remove configured quota type(s) for uid/gid in path
    quota rmnode [-p] <path>                                             : remove quota node and every defined quota on that node
    quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] [[-p] <path>] : set volume and/or inode quota by uid or gid
    General options:
    -m : print information in monitoring <key>=<value> format
    -n : don't translate ids, print uid and gid number
    -u/--uid <uid> : print information only for uid <uid>
    -g/--gid <gid> : print information only for gid <gid>
    -p/--path <path> : print information only for path <path> - this can also be given without -p or --path
    -v/--volume <bytes> : refer to volume limit in <bytes>
    -i/--inodes <inodes> : refer to inode limit in number of <inodes>
    Notes:
    => you have to specify either the user or the group identified by the unix id or the user/group name
    => the space argument is by default assumed as 'default'
    => you have to specify at least a volume or an inode limit to set quota
    => for convenience all commands can just use <path> as last argument omitting the -p|--path e.g. quota ls /eos/ ...
    => if <path> is not terminated with a '/' it is assumed to be a file so it won't match the quota node with <path>/ !
