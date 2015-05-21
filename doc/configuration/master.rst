.. highlight:: rst

.. index::
   single: Master/Slave Configuration

Master/Slave Configuration
==========================

The BERYLL release supports to run two MGM's in master/slave configuration with 
realtime switching from slave=>master role. 
This can be combined with namespace file online compactification (OC).

Single MGM/MQ as master
-----------------------

To run an existing instance with a single MGM/MQ and the master/slave code there is one necessary configuration step to be done after an update.

At least once you have to tag (your previous or new) master machine to start 
as a master. This is done via the service scripts:

.. code-block:: bash
 
   service eos master mgm

   service eos master mq

Then you can startup the instance as usual doing:

.. code-block:: bash

   service eos start 
   service eos status mgm 
   xrootd for role: mgm (pid 23290) is running (as slave) ... 
   service eos status mq 
   xrootd for role: mq (pid 14930) is running (as master) ...

By default every old or fresh installed MGM is configured as a slave. 
You can force MGM or MQ into a slave role using service eos slave mgm and service eos slave mq. 

You con use the **eosha** service to keep the MGM services alive or install a cron job doing ``service eos ...``

Configuration of an MGM/MQ master/slave pair
--------------------------------------------

The simplest configuration uses an alias to point round-robin to master and slave machine e.g. 
configure a static alias ``eosdev.cern.ch`` resolving to ``eosdevsrv1.cern.ch`` and ``eosdevsrv2.cern.ch``. 
This name can be used in the FST configuration to define the broker URL and can 
be used by clients to talk to the instance independent of read or write access.
 
If only failover is desired without read-write load share one can use a static 
alias pointing only to either of the two machines.

Declare one machine to be the master for MGM and MQ, 
declare the second machine to be the slave for MGM and MQ:

.. code-block:: bash

   eosdevsrv1:# service eos master mgm
   eosdevsrv1:# service eos master mq
   eosdevsrv2:# service eos slave mgm
   eosdevsrv2:# service eos slave mq

Start the MQ and SYNC service on both machines.

.. code-block:: bash 
  
   eosdevsrv1:# service eos start mq
   eosdevsrv2:# service eos start mq
   eosdevsrv1:# service eos start sync
   eosdevsrv2:# service eos start sync
   eosdevsrv1:# service eossync start 
   eosdevsrv2:# service eossync start 

After checking the healthy status of 'eossync'

.. code-block:: bash

   eosdevsrv1:# service eossync status

you can bring up the MGM service on both machines doing:

.. code-block:: bash

   eosdevsrv1:# service eos start mgm

   eosdevsrv2:# service eos start mgm

Now veryify the running state of the two MGM's:

.. code-block:: bash

   eosdevsrv1:# eos -b ns 

   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ALL      Files                            227 [booted] (0s)
   ALL      Directories                      572
   # ....................................................................................
   ALL      Replication                      mode=master-rw state=master-rw master=eosdevsrv1.cern.ch configdir=/var/eos/config/eosdevsrv2.cern.ch/ config=default active=true mgm:eosdevsrv1.cern.ch=ok mgm:mode=ro-slave mq:eosdevsrv1.cern.ch:1097=ok
   # ....................................................................................
   ALL      File Changelog Size              18.13 MB
   ALL      Dir  Changelog Size              86.39 kB
   # ....................................................................................
   ALL      avg. File Entry Size             79.85 kB
   ALL      avg. Dir  Entry Size             151.00 B
   # ------------------------------------------------------------------------------------
   ALL      memory virtual                   269.70 MB
   ALL      memory resident                  57.52 MB
   ALL      memory share                     5.96 MB
   ALL      memory growths                  -0.00 B
   ALL      threads                          28
   # ------------------------------------------------------------------------------------

   eosdevsrv2:# eos -b ns

   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ALL      Files                            227 [booted] (0s)ALL      Directories                      572
   # ....................................................................................
   ALL      Replication                      mode=slave-ro state=slave-ro master=eosdevsrv1.cern.ch configdir=/var/eos/config/eosdevsrv2.cern.ch/ config=default active=true mgm:eosdevsrv1.cern.ch=ok mgm:mode=rw-master mq:eosdevsrv1.cern.ch:1097=ok
   ALL      Namespace Latency                0.00 += 0.00 ms
   # ....................................................................................
   ALL      File Changelog Size              18.13 MB
   ALL      Dir  Changelog Size              86.39 kB
   # ....................................................................................
   ALL      avg. File Entry Size             79.85 kB
   ALL      avg. Dir  Entry Size             151.00 B 
   # ------------------------------------------------------------------------------------
   ALL      memory virtual                   270.75 MB
   ALL      memory resident                  67.02 MB
   ALL      memory share                     6.09 MB
   ALL      memory growths                   1.05 MB
   ALL      threads                          26
   # ------------------------------------------------------------------------------------
   eosdevsrv2:# access ls
   # ....................................................................................
   # Redirection Rules ...
   # ....................................................................................
   [ 01 ]                         ENOENT:* => eosdevsrv1.cern.ch
   [ 02 ]                              w:* => eosdevsrv1.cern.ch


Run a master<=>slave MGM role change procedure
----------------------------------------------

.. code-block:: bash

   # switch the master MGM to RO mode 
   eosdevsrv1:# eos -b ns master eosdevsrv2.cern.ch

   # switch the slave MGM to master mode
   eosdevsrv2:# eos -b ns master eosdevsrv2.cern.ch

   # switch the RO mode master MGM to slave mode
   eosdevsrv1:# eos -b ns master eosdevsrv2.cern.ch 
   


Master/Slave eos shell interface
--------------------------------

.. code-block:: bash

   eos -b ns : shows the current state of slave/master MGM/MQ and the current configuration
   eos -b ns master: shows the log file of any master/slave transition command including the initial boot
   eos -b ns master --log-clear : clean the in-memory logfile
   eos -b ns master --disable : disable the heartbeat-supervisor thread modifying redirection/stall variables
   eos -b ns master --enable: enable the heartbeat-supervisor thread modifying redirection/stall variables


Bounce the MQ Service
---------------------

To bounce the MQ service you should make the slave machine also to a master and then declare the other as slave e.g. to move from eosdevsrv1 to eosdevsrv2 you do

.. code-block:: bash

   eosdevsrv2:# service eos master mq
   eosdevsrv1:# service eos slave mq

It is important to never declare both machines as slaves at the same time! 
While it should work well if the broker alias points to both machines it is 
probably more efficient to use a dedicated alias for the MQ broker and always 
point only to one box. This has to be tested.

Configure Online Compactification
---------------------------------

On the MGM master running in RW mode one can configure online compactificiation 
to compact the namespace once or in defined intervals. The configuration of 
online compacting is for the moment not persistent e.g. after a service restart 
online compactificiation is always disabled. For the rare event of a change-log file corruption
it is possible to add a '-repair' to the compactification type e.g. 'all-repair', 'all-files', 'all-directories'. 
The repair skips broken records up to 1kb, otherwise 'eos-log-repair' has to be used offline.

The interface for online compactification is

.. code-block:: bash

   eos -b ns compact on : schedules online compactification for files immedeatly. Immedeatly means that the compactification starts within the next minute.
   eos -b ns compact on 100 : schedules online compactificiation for files with a delay of 100 seconds. The compactification starts with a delay of 100 to max. 160 seconds.
   eos -b ns compact on 1 86400 : schedules online compactification for files with a delay of 1 seconds. The compactification is rescheduled always one day later automatically.
   eos -b ns compact on 60 0 all : schedule online compactification for files and directories once in one minute.
   eos -b ns compact on 60 0 files : schedule online compactification for files once in one minute.
   eos -b ns compact on 60 0 directories : schedule online compactification for files once in one minute.
   eos -b ns compact on 60 0 all-repair : schedule online compactification for files and directories with auto-repair once in one minute.
   
The RW MGM signals a RO slave when the compactification starts and when it is 
finished and triggers a reload of the namespace on the RO MGM once the 
compacted file is fully resynchronized.

During compactification the namespace is set into RO mode for a short time 
and then locked with a write lock for short moment to update the offset 
table pointing to the compacted namespace file.  

The various stages of compactification can be traced with 

.. code-block:: bash

   eos -b ns 
   eos -b ns master
   EOS Console [root://localhost] |/> ns
   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ...
   # ....................................................................................
   ALL      Compactification                 status=off waitstart=0 interval=0 ratio=0.0:1
   # ....................................................................................

When the compactification has been enabled:

.. code-block:: bash

   EOS Console [root://localhost] |/> ns
   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ...
   # ....................................................................................
   ALL      Compactification                 status=starting waitstart=0 interval=0 ratio=0.0:1
   # ....................................................................................

When the compactification is running:

.. code-block:: bash

   EOS Console [root://localhost] |/> ns
   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ...
   # ....................................................................................
   ALL      Compactification                 status=compacting waitstart=0 interval=0 ratio=0.0:1
   # ....................................................................................
   EOS Console [root://localhost] |/> ns

When the compactification is waiting for the next scheduling interval to run:

.. code-block:: bash

   EOS Console [root://localhost] |/> ns
   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ...
   # ....................................................................................
   ALL      Compactification                 status=wait waitstart=85430 interval=86400 ratio=3.4:1
   # ....................................................................................

The ratio parameter in the output shows the namespace file compression factor 
achieved during the last compactification run.

If compactification fails for any reason the namespace boot status is failed !

When the namespace is not in RW mode compacting is blocked:

.. code-block:: bash

   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ...
   # ....................................................................................
   ALL      Compactification                 status=blocked waitstart=0 interval=0 ratio=0.0:1
   # ....................................................................................

In case records have been repaired with auto-repair enabled, they are reported in the master log. 
