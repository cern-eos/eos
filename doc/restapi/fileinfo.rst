.. highlight:: rst

.. index::
   single: Fileinfo API



fileinfo
========

Get meta data information about files and directories. 

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/user/ | root://<host>//proc/user/
     ?mgm.cmd=fileinfo
     &mgm.path=/eos/
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json

CLI syntax
++++++++++

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



