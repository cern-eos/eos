.. highlight:: rst

.. index::
   single:: File Inspector

File Inspector
===================

The File Inspector is a slow agent scanning all files in a namespace and collects statistics per layout type. Additionally it adds statistic about replication inconsistencies per layout. The target interval to scan all files is user defined. The default cycle is 4 hours, which can create a too high load in large namespaces and should be adjusted accordingly.

Configuration
-------------

File Inspector
++++++++++++++
The File Inspector has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.inspector=on  
   # disable
   eos space config default space.inspector=off

By default Replication Tracking is disabled.

The current status of the Tracker can be seen via:

.. code-block:: bash

   eos space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   inspector                        := off
   ...


Inspector Interval
------------------

The default inspector interval to scan all files is 4 hours. The interval can be set using:

.. code-block:: bash

   # set interval to 1d
   eos space config default space.inspector.interval=86400


Inspector Status
----------------

You can get the inspector status and an estimate for the run time using

.. code-block:: bash

   eos space inspector

   # or 

   eos inspector

   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:38:24Z
   # 28 % done - estimate to finish: 2575 seconds
   # ------------------------------------------------------------------------------------

Inspector Output
----------------

You can see the current statistics of the inspector run using

.. code-block:: bash

   eos inspector -c 
   eos inspector --current

   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:39:55Z
   # 28 % done - estimate to finish: 2574 seconds
   # current scan: 2019-07-12T08:25:42Z
    not-found-during-scan            : 0
   ======================================================================================
   layout=00000000 type=plain         checksum=none     blockchecksum=none     blocksize=4k  

   locations                        : 0
   nolocation                       : 223004
   repdelta:-1                      : 223004
   unlinkedlocations                : 0
   zerosize                         : 223004
   
   ======================================================================================
   layout=00100001 type=plain         checksum=none     blockchecksum=none     blocksize=4k  

   locations                        : 2
   repdelta:0                       : 2
   unlinkedlocations                : 0
   volume                           : 3484
  
   ...


The reports tags are:

.. code-block:: bash 

   locations         : number of replicas (or stripes) in this layout categorie
   nolocation        : number of files without any location attached
   repdelta:-N       : number of files with -N replicas missing
   repdelta:0        : number of files with correct replicat count
   repdelate:+N      : number of files with +N replicas in excess
   zerosize          : number of files with 0 size
   volume            : logical bytes stored in this layout type
   unlinkedlocations : number replicas still to be deleted
   shadowdeletions   : number of files with a replica pointing to a not configured filesystem for deletion
   shodowlocation    : number of files with a replica pointing to a not configured filesystem


You can get the statistics of the last completed run using

.. code-block:: bash

   eos inspector -l
   eos inspector --last

You can print the current and last run statistics in monitoring format:

.. code-block:: bash

   eos inspector -c -m 
   ...

   eos inspector -l -m 

   key=last layout=00100002 type=plain checksum=adler32 blockchecksum=none blocksize=4k locations=638871 repdelta:+1=1 repdelta:0=638869 unlinkedlocations=0 volume=10802198338 zerosize=550002
   key=last layout=00100012 type=replica checksum=adler32 blockchecksum=none blocksize=4k locations=42 repdelta:0=42 unlinkedlocations=0 volume=21008942
   key=last layout=00100014 type=replica checksum=md5 blockchecksum=none blocksize=4k locations=1 repdelta:0=1 unlinkedlocations=0 volume=1701
   key=last layout=00100015 type=replica checksum=sha1 blockchecksum=none blocksize=4k locations=1 repdelta:0=1 unlinkedlocations=0 volume=1701
   key=last layout=00100112 type=replica checksum=adler32 blockchecksum=none blocksize=4k locations=44 repdelta:0=22 unlinkedlocations=0 volume=10506283
   key=last layout=00640112 type=replica checksum=adler32 blockchecksum=none blocksize=1M locations=2 repdelta:0=1 unlinkedlocations=0 volume=1783
   key=last layout=20640342 type=raid6 checksum=adler32 blockchecksum=crc32c blocksize=1M locations=0 nolocation=6 repdelta:-4=6 unlinkedlocations=0 zerosize=6
   key=last layout=3b9ac9ff type=none checksum=none blockchecksum=none blocksize=illegal unfound=0

The list of file ids with an inconsistency can be extracted using:

.. code-block:: bash

   # print the list of file ids
   eos inspector -c -p #current run

   fxid:00140237 repdelta:-1
   fxid:001410ff repdelta:-1
   fxid:00141807 repdelta:-1
   fxid:0013da42 repdelta:-4
   fxid:0013da43 repdelta:-4
   fxid:0013da44 repdelta:-4
   fxid:0013da45 repdelta:-4
   fxid:0013da57 repdelta:-4
   fxid:0013da68 repdelta:-4
   ...


   eos inspector -l -p #last run
   ...

   # export the list of file ids on the mgm
   eos inspector -c -e #current run
   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:53:14Z
   # 100 % done - estimate to finish: 0 seconds
   # file list exported on MGM to '/var/log/eos/mgm/FileInspector.1562921594.list'
   # ------------------------------------------------------------------------------------

   eos inspector -l -e #last run
   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:53:33Z
   # 100 % done - estimate to finish: 0 seconds
   # file list exported on MGM to '/var/log/eos/mgm/FileInspector.1562921613.list'
   # -----------------------------------------------------------------------   


Log Files
---------
The File Inspector has a dedicated log file under ``/var/log/eos/mgm/FileInspector.log``
which shows the scan activity and potential errors. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info


