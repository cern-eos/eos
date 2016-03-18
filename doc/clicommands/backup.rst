backup
------

.. code-block:: text

   usage: backup <src_url> <dst_url> [options] 
    
    optional arguments: 
    --ctime|mtime <val>s|m|h|d use the specified timewindow to select entries for backup
    --excl_xattr val_1[,val_2]...[,val_n] extended attributes which are not enforced and
      also not checked during the verification step
