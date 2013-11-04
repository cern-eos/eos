.. highlight:: rst

.. index::
   single: Auto Repair

Auto Repair
===========

You can configure the FSTs to regulary scan all files for file/block checksum errors.
If you enable the auto repair function the FSTs will call to trigger an asynchronous 
**rewrite** of the corrupted file using the :doc:`converter`.

Enable
------

To enable auto repair:

.. code-block:: bash

   [eosdevsrv1]# eos space config default space.autorepair=on

.. note::
   
   Auto Repair has to be configured globally only on the **default** space.

Disable
-------

To disable auto repair:

.. code-block:: bash

   [eosdevsrv1]# eos space config default space.autorepair=off

Status
------

To check the status use:

.. code-block:: bash

   [eosdevsrv1]# eos space status default

    ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   autorepair                       := on
   balancer                         := off
   balancer.node.ntx                := 4
   balancer.node.rate               := 120
   balancer.threshold               := 0.1
   converter                        := on
   converter.ntx                    := 100
   drainer.node.ntx                 := 10
   drainer.node.rate                := 100
   drainperiod                      := 3600
   geobalancer                      := on
   geobalancer.ntx                  := 100
   geobalancer.threshold            := 0.5
   graceperiod                      := 86400
   groupbalancer                    := off
   groupbalancer.ntx                := 100
   groupbalancer.threshold          := 0.1
   groupmod                         := 22
   groupsize                        := 22
   headroom                         := 0.00 B
   lru                              := off
   lru.interval                     := 604800
   quota                            := on
   scaninterval                     := 40000

Enable FST Scan
---------------

To enable the FST scan you have to set the variable **scaninterval** on the space and
on all file systems:

.. code-block:: bash

   # set it on the space to inherit a value for all new filesystems in this space every 14 days (time has to be in seconds)
   space config default space.scaninterval=1209600

   # set it on all existing filesystems to 14 days (time has to be in seconds)
   space config default space.scaninterval=1209600

.. note::
   
   The *scaninterval* time has to be given in seconds!

