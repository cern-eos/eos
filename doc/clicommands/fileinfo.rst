fileinfo
--------

.. code-block:: text

   usage: fileinfo <path> [--path] [--fxid] [--fid] [--size] [--checksum] [--fullpath] [-m] [--silent] [--env] :  print file information for <path>
      fileinfo fxid:<fid-hex>                                           :  print file information for fid <fid-hex>
      fileinfo fid:<fid-dec>                                            :  print file information for fid <fid-dec>
      fileinfo inode:<fid-dec>                                          :  print file information for inode (decimal)>
      --path  :  selects to add the path information to the output
      --fxid  :  selects to add the hex file id information to the output
      --fid   :  selects to add the base10 file id information to the output
      --size  :  selects to add the size information to the output
      --checksum :  selects to add the checksum information to the output
      --fullpath :  selects to add the full path information to each replica
      -m     :  print single line in monitoring format
      --env  :  print in OucEnv format
      -s     :  silent - used to run as internal command
