rm
--

.. code-block:: text

  rm [-r|-rf|-rF] [--no-recycle-bin|-F] [<path>|fid:<fid-dec>|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>]
    -r | -rf : remove files/directories recursively
    - the 'f' option is a convenience option with no additional functionality!
    - the recursive flag is automatically removed it the target is a file!
.. code-block:: text

   --no-recycle-bin|-F : remove bypassing recycling policies
    - you have to take the root role to use this flag!
    -rF | Fr : remove files/directories recursively bypassing recycling policies
    - you have to take the root role to use this flag!
    - the recursive flag is automatically removed it the target is a file!
