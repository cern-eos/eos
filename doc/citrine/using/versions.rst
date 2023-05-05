.. highlight:: rst

.. _versioning:

File Versioning
===============

File versioning can be triggerd as a per directory policy using the extended attribute ``sys.versioning=<n>`` or via the ``eos file version`` command.

The paramter ``<n>`` in the extended attribute describes the maximum number of versions which should be kept according to a FIFO policy.

Additionally to the simple FIFO policy where the oldest versions are deleted once ``<n>`` versions are reached there are 11 predefined timebins, for which additional versions exceeding the versioning parameter ``<n>`` are kept.

Versions are kept in a hidden directory (visible with ``ls -la``) which is composed by ``.sys.v#.<basename>``

.. code-block:: bash

   eos ls -la 
   drwxrwxrwx   1 root     root                0 Aug 29 15:33 .sys.v#.myfile
   -rw-r-----   1 root     root             1824 Aug 29 15:33 myfile

The 11 time bins are defined as follows:

.. epigraph::

   ============= ===================================================
   bin           deletion policy
   ============= ===================================================
   age<1d        the first version entering this bin survives
   1d<=age<2d    the first version entering this bin survives
   2d<=age<3d    the first version entering this bin survives
   3d<=age<4d    the first version entering this bin survives
   4d<=age<5d    the first version entering this bin survives
   5d<=age<6d    the first version entering this bin survives
   6d<=age<1w    the first version entering this bin survives
   1w<=age<2w    the first version entering this bin survives
   2w<=age<3w    the first version entering this bin survives
   3w<=age<1mo   the first version entering this bin survives
   ============= ===================================================



Configuration of automatic versioning
-------------------------------------

Configure each directory which should apply versioning using the extended attribute ``sys.versioning``:

.. code-block:: bash

   # force 10 versions (FIFO)
   eos attr set sys.versioning=10 version-dir
  
   # upload initial file
   eos cp /tmp/file /eos/version-dir/file

   # versions are created on the fly with each upload - now 1 version
   eos cp /tmp/file /eos/version-dir/file

   # versions are created on the fly with each upload - now 2 versions
   eos cp /tmp/file /eos/version-dir/file

   # aso ....



Creating new versions
---------------------

.. code-block:: bash

   # force a new version 
   eos file version myfile
 
   # force a new version with max 5 versions
   eos file version myfile 5

List existing versions
----------------------

.. code-block:: bash

   eos file versions myfile
   -rw-r-----   1 root     root             1824 Aug 29 15:17 1567084675.0014ede6
   -rw-r-----   1 root     root             1824 Aug 29 15:33 1567085591.0014ede7
   -rw-r-----   1 root     root             1824 Aug 29 15:33 1567085591.0014ede8
   -rw-r-----   1 root     root             1824 Aug 29 15:33 1567085592.0014ede9



Purging existing versions
-------------------------

.. code-block:: bash

   # remove all versions
   eos file purge myfile 0

   # keep 5 versions (FIFO)
   eos file purge myfile 5
