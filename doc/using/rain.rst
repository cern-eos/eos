.. highlight:: rst

RAIN
====

ECC Layout Types
----------------

EOS supports three types of RAIN layouts:

.. epigraph::

   ========== ============= ================================ ====================================
   name       redundancy    algorithm                        description
   ========== ============= ================================ ====================================
   raid5      N+1           single parity raid               can lose 1 disk without data loss
   raiddp     4+2           dual parity raid                 can lose 2 disks without data loss
   raid6      N+2           Erasure Code (Jerasure library)  can lose 2 disks without data loss
   archive    N+3           Erasure Code (Jerasure library)  can lose 3 disks without data loss
   qrain      N+4           Erasure Code (Jerasure library)  can lose 4 disks without data loss
   ========== ============= ================================ ====================================

The layout is set in a namespace tree via ``eos attr -r set default=<name> <tree>``.

The default layout can be defined using default space policies. See :ref:`space-policies`.
