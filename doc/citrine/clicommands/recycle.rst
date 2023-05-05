recycle
-------

.. code-block:: text

  recycle [ls|purge|restore|config ...]
    provides recycle bin functionality
    recycle [-m]
    print status of recycle bin and config status if executed by root
    -m     : display info in monitoring format
.. code-block:: text

    recycle ls [-g|<date>] [-m] [-n]
    list files in the recycle bin
    -g     : list files of all users (if done by root or admin)
    <date> : can be <year>, <year>/<month> or <year>/<month>/<day>
    e.g.: recycle ls 2018/08/12
    -m     : display info in monitoring format
    -n     : display numeric uid/gid(s) instead of names
    recycle purge [-g|<date>]
    purge files in the recycle bin
    -g     : empties the recycle bin of all users (if done by root or admin)
    <date> : can be <year>, <year>/<month> or <year>/<month>/<day>
    recycle restore [-f|--force-original-name] [-r|--restore-versions] <recycle-key>
    undo the deletion identified by the <recycle-key>
    -f : move deleted files/dirs back to their original location (otherwise
    the key entry will have a <.inode> suffix)
    -r : restore all previous versions of a file
    recycle config [--add-bin|--remove-bin] <sub-tree>
    --add-bin    : enable recycle bin for deletions in <sub-tree>
    --remove-bin : disable recycle bin for deletions in <sub-tree>
    recycle config --lifetime <seconds>
    configure FIFO lifetime for the recycle bin
    recycle config --ratio <0..1.0>
    configure the volume/inode keep ratio. E.g: 0.8 means files will only
    be recycled if more than 80% of the volume/inodes quota is used. The
    low watermark is by default 10% below the given ratio.
    recycle config --size <value>[K|M|G]
    configure the quota for the maximum size of the recycle bin.
    If no unit is set explicitly then we assume bytes.
    recycle config --inodes <value>[K|M|G]
    configure the quota for the maximum number of inodes in the recycle
    bin.
