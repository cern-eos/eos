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
   raiddp     4+2           dual parity raid                 can loose 2 disks without dataloss
   raid6      N+2           Erasure Code (Jerasure library)  can loose 2 disks without dataloss
   archive    N+3           Erasure Code (Jerasure library)  can loose 3 disks without dataloss
   ========== ============= ================================ ====================================