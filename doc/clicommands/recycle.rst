recycle
-------

.. code-block:: text

   recycle ls|purge|restore|config ...
   '[eos] recycle ..' provides recycle bin functionality to EOS.
   Options:
   recycle :
      print status of recycle bin and if executed by root the recycle bin configuration settings.
   recycle ls :
      list files in the recycle bin
   recycle purge :
      purge files in the recycle bin
   recycle restore [--force-original-name|-f] <recycle-key> :
      undo the deletion identified by <recycle-key>
      --force-original-name : move's deleted files/dirs back to the original location (otherwise the key entry will have a <.inode> suffix
   recycle config --add-bin <sub-tree>:
      configures to use the recycle bin for deletions in <sub-tree>
   recycle config --remove-bin <sub-tree> :
      disables usage of recycle bin for <sub-tree>
   recycle config --lifetime <seconds> :
      configure the FIFO lifetime of the recycle bin
   recycle config --ratio < 0 .. 1.0 > :
      configure the volume/inode keep ratio of the recycle bin e.g. 0.8 means files will only be recycled if more than 80% of the space/inodes quota is used. The low watermark is 10% under the given ratio by default e.g. it would cleanup volume/inodes to be around 70%.
   recycle config --size <size> :
      set the size of the recycle bin
   'ls' and 'config' support the '-m' flag to give monitoring format output!
   'ls' supports the '-n' flag to give numeric user/group ids instead of names!
