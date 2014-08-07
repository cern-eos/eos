.. highlight:: rst 

.. index::
   single: Archive


Archiver interface
================================

The archiver daemon is managing the transfer of files to/from EOS from/to a remote location offering an XRootD 
interface. Before starting the new service there are a few configuration parameters that need to be set.

Configuration
+++++++++++++
First of all, one can set the user under which the daemon will be running by modifying the script located 
at ``/etc/sysconfig/eosarchiverd``. The daemon needs to create a series of temporary files during the 
transfers which will be saved at the location pointed by the environment variable **EOS_ARCHIVE_DIR**.
Also, the location where the log files are saved can be changed by modifying the variable **LOG_DIR**.

If none of the above variables is modified then the default configuration is as follows:

.. code-block:: bash 

  export USER="xrootd"
  export EOS_ARCHIVE_DIR="/var/eos/archive/"
  export LOG_DIR="/var/log/eos/archive/"
  export LOG_LEVEL="debug"

The **LOG_LEVEL** variable must be a string corresponding to the syslog loglevel.

Log File
++++++++

The logs form the **eosarchvierd** daemon are saved by default at ``/var/log/eos/archive/eosarchiver.log``

MGM Configuration
+++++++++++++++++

The configuration file for the **MGM** node contains a new directive called **mgmofs.archivedir** which needs
to point to the same location as the **EOS_ARCHIVE_DIR** defined earlier for the **eosarchiverd** daemon. 
The two locations must match because the **MGM** and the **eosarchiverd** daemons communicate between each 
other using ZMQ and this is the path where any common ZMQ files are saved.

.. code-block:: bash

  mgm.archivedir /var/eos/archive/  # has to be the same as EOS_ARCHIVE_DIR from eosarchiverd

Another variables that needs to be set for the **MGM** node is the location where all the archived directories
are saved. Care should be taken so that the user name under which the **eosarchiverd** daemon runs, has the 
proper rights to read and write files to this remote location. This envrionment variables can be set in the 
``/etc/sysconfig/eos`` file as follows:

.. code-block:: bash

  export EOS_ARCHIVE_URL=root://castordev.cern.ch//castor/cern.ch/dev/archives/




