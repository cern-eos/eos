.. highlight:: rst

.. index::
   single:: LRU Policies
   see: Polcies; LRU Policies

LRU Engine
==========
The LRU engine scans in a defined interval the full directory hierarchy and applies 
so called LRU policies. 

.. epigraph::

   ===================================================================================== =====================
   Policy                                                                                Basis 
   ===================================================================================== =====================
   Volume based LRU cache with low and high watermark                                    volume/threshold/time
   Automatic time based cleanup of empty directories                                     ctime
   Time based LRU cache with expiration time settings                                    ctime
   Automatic time based layout conversion if a file reaches a defined age                ctime
   Automatic time based layout conversion if a file has not been used for specified time mtime
   ===================================================================================== =====================

Configuration
-------------

Engine
++++++
The LRU engine has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.lru=on  
   # disable
   eos space config default space.lru=off

The current status of the Converter can be seen via:

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   lru                            := off
   lru.interval                   := 0
   ...

The interval in which the LRU engine is running is defined by the **lru.interval**
space variable:

.. code-block:: bash

   # run the LRU scan once a week
   eos space config default space.lru.interval=604800

Policy
++++++

Volume based LRU cache with low and high watermark
``````````````````````````````````````````````````
To configure an LRU cache with low and high watermark it is necessary to define
a quota node on the cache directory, set the high- and low watermark and to enable  
the **atime** feature updating the creation times of files with the current 
access time. 

When the cache reaches the high watermark it cleans the oldest files untile low-watermark is reached ...

.. code-block:: bash

   # define project quota on the cache
   eps quota set -g 99 -v 1T /eos/instance/cache/ 

   # define 90 as low and 95 as high watermark            
   eos attr set sys.lru.watermark=90:95  /eos/instance/cache/

   # track atime with a time resolution of 5 minutes
   eos attr set sys.force.atime=300 /eos/dev/instance/cache/  


Automatic time based cleanup of empty directories 
``````````````````````````````````````````````````
Configure automatic clean-up of empty directories which have a minimal age.
The LRU scan deletes directories with the largest deepness first to be able 
to remove complete empty subtrees in the namespace.

.. code-block:: bash

   # remove automatically empty directories if they are older than 1 hour
   eos attr set sys.lru.expire.empty="1h" /eos/dev/instance/empty/ 


Time based LRU cache with expiration time settings 
``````````````````````````````````````````````````
This policy allows to match files by name with a defined age to be deleted.

.. code-block:: bash
 
   # files with suffix *.root get removed after a month, files with *.tgz after one week
   eos attr set sys.lru.expire.match="*.root:1mo,*.tgz:1w"  /eos/dev/instance/scratch/
   
   # all files older than a day are automatically removed                                                                       
   eos attr set sys.lru.expire.match="*:1d" /eos/dev/instance/scratch/      

Automatic time based layout conversion if a file reaches a defined age
``````````````````````````````````````````````````````````````````````
This policy allows to convert a file from the current layout into a defined layout.

.. code-block:: bash

   # convert all files older than a month to the layout defined next
   eos attr set sys.lru.convert.match="*:1mo" /eos/dev/instance/convert/    

   # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 
   eos attr set sys.conversion.*=20640542 /eos/dev/instance/convert/                                    


The hex layout ID contains also the checksum and blocksize settings. The best is
to create a file with the desired layout and get the hex layout ID using 
**eos** **file** **info** **<path>**.

Automatic time based layout conversion if a file has not been used for specified time
`````````````````````````````````````````````````````````````````````````````````````
This policy allows to convert a file from the current layout into a different layout 
if a file was not accessed for a defined interval. To use this feature one has 
also to enable the **atime** feature where the access time is stored as the new 
file creation time.

.. code-block:: bash
    
     # track atime with a time resolution of one week
     eos attr set sys.force.atime=1w /eos/dev/instance/convert/     

     # convert all files older than a month to the layout defined next              
     eos attr set sys.lru.convert.match="*:6mo" /eos/dev/instance/convert/ 

     # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2    
     eos attr set sys.conversion.*=20640542 /eos/dev/instance/convert/                                  

Manual File Conversion
----------------------
It is possible to run an asynchronous file conversion using the **EOS CLI**.

.. code-block:: bash

   # convert the referenced file into a file with 3 replica
   EOS Console [root://localhost] |/eos/dev/proc/conversion/> file convert /eos/dev/2rep/passwd replica:3
   info: conversion based layout+stripe arguments
   success: created conversion job '/eos/dev/proc/conversion/0000000000059b10:default#00650212'

.. code-block:: bash

   # convert the referenced file into a RAID6 file with 6 stripes
   EOS Console [root://localhost] |/eos/dev/2rep/> file convert /eos/dev/2rep/passwd raid6:6
   info: conversion based layout+stripe arguments
   success: created conversion job '/eos/dev/proc/conversion/0000000000064f61:default#20650542'
   EOS Console [root://localhost] |/eos/dev/2rep/> file info passwd
   File: '/eos/dev/2rep/passwd'  Size: 2458
   Modify: Wed Oct 30 17:03:35 2013 Timestamp: 1383149015.384602000
   Change: Wed Oct 30 17:03:36 2013 Timestamp: 1383149016.243563000
     CUid: 0 CGid: 0  Fxid: 00064f63 Fid: 413539    Pid: 1864   Pxid: 00000748
   XStype: adler    XS: 01 15 4b 52 
   raid6 Stripes: 6 Blocksize: 4M LayoutId: 20650542
     #Rep: 6
   <#> <fs-id> #...................................................................................................................................
               #                   host  #     schedgroup #           path #     boot # configstatus #      drain # active #                 geotag
               #...................................................................................................................................
     0     102      lxfsra04a03.cern.ch        default.11          /data12     booted             rw      nodrain   online           eos::cern::mgm
     1     116      lxfsra02a05.cern.ch        default.11          /data12     booted             rw      nodrain   online           eos::cern::mgm
     2      94      lxfsra04a02.cern.ch        default.11          /data12     booted             rw      nodrain   online           eos::cern::mgm
     3      65      lxfsra02a07.cern.ch        default.11          /data12     booted             rw      nodrain   online           eos::cern::mgm
     4     108      lxfsra02a08.cern.ch        default.11          /data12     booted             rw      nodrain   online           eos::cern::mgm
     5      77      lxfsra04a01.cern.ch        default.11          /data13     booted             rw      nodrain   online           eos::cern::mgm
   *******


Log Files 
---------
The LRU engine has a dedicated log file under ``/var/log/eos/mgm/LRU.log``
which shows triggered actions based on scanned policies. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info


