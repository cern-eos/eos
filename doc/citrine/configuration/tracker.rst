.. highlight:: rst

.. index::
   single:: Replica Tracker

Replication Tracker
===================

The Replication Tracker follows the workflow of file creations. For each created file a virtual entry is created in the ``proc/tracker`` directory. Entries are removed once a layout is completely commited. The purpose of this tracker is to find inconsistent files after creation and to remove atomic upload relicts automatically after two days.

Configuration
-------------

Tracker
+++++++
The Replication Tracker has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.tracker=on  
   # disable
   eos space config default space.tracker=off

By default Replication Tracking is disabled.

The current status of the Tracker can be seen via:

.. code-block:: bash

   eos space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   tracker                        := off
   ...


Automatic Cleanup
-----------------

When the tracker is enabled, an automatic thread inspects tracking entries and takes care of cleanup of tracking entries and the time based tracking directory hierarchy. Atomic upload files are automatically cleaned after 48 hours when the tracker is enabled.


Listing Tracking Information
----------------------------

You can get the current listing of tracked files using:

.. code-block:: bash

   eos space tracker 

   # ------------------------------------------------------------------------------------
   key=00142888 age=4 (s) delete=0 rep=0/1 atomic=1 reason=REPLOW uri='/eos/test/creations/.sys.a#.f.1.802e6b70-973e-11e9-a687-fa163eb6b6cf'
   # ------------------------------------------------------------------------------------

   

The displayed reasons are:

* REPLOW - the replica number is too low
* ATOMIC - the file is an atomic upload
* KEEPIT - the file is still in flight
* ENOENT - the tracking entry has no corresponding namespace entry with the given file-id
* REP_OK - the tracking entry is healthy and can be removed - FUSE files appear here when not replica has been committed yet

There is convenience command defined in the console:

.. code-block:: bash

   eos tracker # instead of eos space tracker



Log Files
---------
The Replication Tracker has a dedicated log file under ``/var/log/eos/mgm/ReplicationTracker.log``
which shows the tracking entries and related cleanup activities.
To get more verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info


