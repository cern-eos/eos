.. highlight:: rst

.. index::
   single: Namespace Configuration

Namespace Configuration
==========================

The BERYLL release supports to run two MGM's in master/slave namespaces with 
realtime switching from slave=>master role. 

Since Version 0.3.235 there are various performance oriented optimizations available to speed-up the boot process.


Parallel Boot Configuration
---------------------------

.. code-block:: bash
 
   export EOS_NS_BOOT_PARALLEL=1

Run a multi-threaded boot procedure using the maximum number of avilable core's of a machine. By default an MGM uses a sequential boot running on a single core.

Disable CRC32 Checksumming
---------------------------

.. code-block:: bash
 
   export EOS_NS_BOOT_NOCRC32=1

If you use a filesystem which has internal checksummung there is no need to let the MGM do record checksumming during a boot. Be carefule with this option since it removes another consistency check.

Disable mmaped changelog files
------------------------------

.. code-block:: bash
 
   export EOS_NS_BOOT_NOMMAP=1


Since version 0.3.235 the MGM mmap's changel files in the first phase until a compaction mark is detected. If you are short in memory, you can disable this mmap functionality. Mmapping removes a bottleneck of doing many ::pread calls for small lengths, which bottlenecks the boot performance. 


Enable subtree accounting
-------------------------

.. code-block:: bash
 
   export EOS_NS_ACCOUNTING=1


This will enable subtree accounting for the namespace. When you use a 'fileinfo' command on a directory there is a special field displayed 'Treesize' which is the sum of all logical file sizes in this tree of the namespace. If subtree accounting is enabled, 'ls -l' shows the tree size instead of the directory size.

Enable sync time propagation
----------------------------

-- code-block:: bash

   export EOS_SYNCTIME_ACCOUNTING=1

Each directory which has the extended attribute 'sys.mtime.propagation=1' set, will propagate its modification time into parent directory sync time. The parent directory sync time is updated if the propagated modification time is newer than the last stored sync time. This meachnism is used to find quickly directories which have modifications as used by Owncloud clients or backup scripts. The 'fileinfo' command displays for directories besides'Change", "Modify" time a third field with the propagated "Sync" time.


Namespace Size Preset Variables
-------------------------------

-- code-block:: bash

   # Set Namespace Preset size
   export EOS_NS_DIR_SIZE=15000000
   export EOS_NS_FILE_SIZE=62000000

It is possible to resize hashmaps to the expected maximum size at the start of the boot process. There is no other adavantage besides that the MGM process never needs to resize the hashmap during normal operation ( locking the namespace for several seconds). The boot time of the namespace stays unchanged by these settings.




