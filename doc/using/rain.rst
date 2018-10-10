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
   raiddp     4+2           dual parity raid                 can lose 2 disks without data loss
   raid6      N+2           Erasure Code (Jerasure library)  can lose 2 disks without data loss
   archive    N+3           Erasure Code (Jerasure library)  can lose 3 disks without data loss
   ========== ============= ================================ ====================================
