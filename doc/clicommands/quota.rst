quota
-----

.. code-block:: text

   usage: quota [<path>]                                                                              : show personal quota for all or only the quota node responsible for <path>
      quota ls [-n] [-m] [-u <uid>] [-g <gid>] [-p <path> ]                                       : list configured quota and quota node(s)
      quota ls [-n] [-m] [-u <uid>] [-g <gid>] [<path>]                                           : list configured quota and quota node(s)
      quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] -p <path>                            : set volume and/or inode quota by uid or gid
      quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] <path>                               : set volume and/or inode quota by uid or gid
      quota rm  -u <uid>|-g <gid> -p <path>                                                       : remove configured quota for uid/gid in path
      quota rm  -u <uid>|-g <gid> <path>                                                          : remove configured quota for uid/gid in path
      -m                  : print information in monitoring <key>=<value> format
      -n                  : don't translate ids, print uid+gid number
      -u/--uid <uid>      : print information only for uid <uid>
      -g/--gid <gid>      : print information only for gid <gid>
      -p/--path <path>    : print information only for path <path> - this can also be given without -p or --path
      -v/--volume <bytes> : set the volume limit to <bytes>
      -i/--inodes <inodes>: set the inodes limit to <inodes>
      => you have to specify either the user or the group identified by the unix id or the user/group name
      => the space argument is by default assumed as 'default'
      => you have to specify at least a volume or an inode limit to set quota
      => for convenience all commands can just use <path> as last argument ommitting the -p|--path e.g. quota ls /eos/ ...
      => if <path> is not terminated with a '/' it is assumed to be a file so it want match the quota node with <path>/ !
      quota rmnode -p <path>                                                                      : remove quota node and every defined quota on that node
