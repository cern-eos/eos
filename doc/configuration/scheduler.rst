.. highlight:: rst

.. index::
   single: Scheduler

Converter
=========

The scheduler distinguishes two algorithms, one for placement, one for access. 


Placement
-------------
Placements selects the primary location round-robin over scheduling groups and within scheduling groups. In case filesystems have GEO tags, the scheduler tries to place the first and second replica in different locations. By default the selection of a filesystem is a weighted selection where the weight is computed by current load parameters (network,disk IO). For multiple GEO locations it makes sense to enforce a pre-defined policy. An exact GEO selection/placement can be enforced with the following space variables, which are separated for read and write operations:

.. code-block:: bash

   # enable
   eos space config default space.geo.access.policy.write.exact=on
   eos space config default space.geo.access.policy.read.exact=on
   # disable
   eos space config default space.geo.access.policy.write.exact=off
   eos space config default space.geo.access.policy.read.exact=off

The current status of policy can be seen here (no entry means the policy is off):

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   geo.access.policy.write.exact   := on
   geo.access.policy.read.exact    := on
   ...

