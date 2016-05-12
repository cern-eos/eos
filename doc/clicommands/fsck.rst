fsck
----

.. code-block:: text

   usage: fsck stat                                                  :  print status of consistency check
      fsck enable [<interval>]                                   :  enable fsck
      <interval> :  check interval in minutes - default 30 minutes       fsck disable                                               :  disable fsck
      fsck report [-h] [-a] [-i] [-l] [--json] [--error <tag> ]  :  report consistency check results                                                               -a :  break down statistics per filesystem
      -i :  print concerned file ids
      -l :  print concerned logical names
      --json :  select JSON output format
      --error :  select to report only error tag <tag>
      -h :  print help explaining the individual tags!
      fsck repair --checksum
      :  issues a 'verify' operation on all files with checksum errors
      fsck repair --checksum-commit
      :  issues a 'verify' operation on all files with checksum errors and forces a commit of size and checksum to the MGM
      fsck repair --resync
      :  issues a 'resync' operation on all files with any error. This will resync the MGM meta data to the storage node and will clean-up 'ghost' entries in the FST meta data cache.
      fsck repair --unlink-unregistered
      :  unlink replicas which are not connected/registered to their logical name
      fsck repair --unlink-orphans
      :  unlink replicas which don't belong to any logical name
      fsck repair --adjust-replicas[-nodrop]
      :  try to fix all replica inconsistencies - if --adjust-replicas-nodrop is used replicas are only added but never removed!
      fsck repair --drop-missing-replicas
      :  just drop replicas from the namespace if they cannot be found on disk
      fsck repair --unlink-zero-replicas
      :  drop all files which have no replica's attached and are older than 48 hours!
      fsck repair --all                                          :  do all the repair actions besides <checksum-commit>
