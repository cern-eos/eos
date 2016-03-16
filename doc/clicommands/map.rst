map
---

.. code-block:: text

   '[eos] map ..' provides a namespace mapping interface for directories in EOS.
   map [OPTIONS] ls|link|unlink ...
   Options:
   map ls :
      : list all defined mappings
   map link <source-path> <destination-path> :
      : create a symbolic link from source-path to destination-path
   map unlink <source-path> :
      : remove symbolic link from source-path
