.. highlight:: rst

.. index::
   single: Recycle Bin

Recycle Bin
===========


Overview
--------

The EOS recycle bin allows to define a FIFO policy for delayed file deletion. 
This feature is available starting with EOS BERYL.

The recycling bin is time-based and volume based e.g. the garbage directory 
performs final deletion after a configurable time delay. The volume in the 
recycle bin is limited using project quota. If the recycle bin is full no 
further deletion is possible and deletions fails until enough space is available.

The recycling bin supports individual file deletions and recursive bulk 
deletions (referenced as object deletions in the following). 

The owner of a deleted file or subtree deletion can restore files into the 
original location from the recycle bin if he has the required quota. If the original location is 'occupied' the action is rejected. Using the '-f' flag the existing location is renamed and the deleted object is restored to the original name.

If the parent tree of the restore location is incomplete the user is asked 
to first recreate the parent directory structure before objects are restored.

If the recycle bin is applicable for a deletion operation the quota is 
immedeatly removed from the original quota node and added to the recycle quota. Without recycle bin quota is released once files are physically deleted!
   
Command Line Interface 
----------------------
If you want to get the current state and configuration of the recycle bin you 
run the recycle command:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle

   # _______________________________________________________________________________________________
   # used 0.00 B out of 100.00 GB (0.00% volume / 0.00% inodes used) Object-Lifetime 86400 [s]
   # _______________________________________________________________________________________________

The values are self-explaining.

Define the object lifetime
++++++++++++++++++++++++++

If you want to configure the lifetime of objects in the recycle bin you run r
**recycle config --lifetime <lifetime>**:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --lifetime 86400

<lifetime> can be e.g. just a number 3600, 3600s  (seconds) or 60min 
(60 minutes) 1d (one day), 1w (one week), 1mo (one month), 1y (one year) aso.

The lifetime has to be at least 60 seconds!

Define the recycle bin size
+++++++++++++++++++++++++++

If you want to configure the size of the recycle bin you run 
**recycle config --size <size>**:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/> recycle config --size 100G

<size> can be e.g. just a number 100000000000, 100000M (mega byte) or 100G (giga byte), 1T (one terra) aso.

The size has to be atleast 100G !

Bulk deletions
++++++++++++++
A bulk deletion using the recycle bin prints how the deleted files can 
be restored:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> rm -r tree

   success: you can recycle this deletion using 'recycle restore 00000000000007cf'

Add recycle policy on a subtree
+++++++++++++++++++++++++++++++

If you want to set the policy to use the recycle bin in a subtree of the 
namespace run:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle config --add-bin /eos/dev/2rep/subnode/tree

   success: set attribute 'sys.recycle'='../recycle' in directory /eos/dev/2rep/subnode/tree/

Remove recycle policy from a subtree
++++++++++++++++++++++++++++++++++++

To remove the recycle bin policy in a subtree run:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle config --remove-bin /eos/dev/2rep/subnode/tree

   success: removed attribute 'sys.recycle' from directory /eos/dev/2rep/subnode/tree/

List files in the recycle bin
+++++++++++++++++++++++++++++++++++

If you want to list the restorable objects from the recycle bin you run: 

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle ls
   # Deletion Time            UID      GID      TYPE          RESTORE-KEY      RESTORE-PATH                                                    
   # ==============================================================================================================================
   Thu Mar 21 23:02:22 2013   apeters  z2       recursive-dir 00000000000007cf /eos/dev/2rep/subnode/tree

Executed as a non-root this command displays all user private restorable objects. 
If running as root it shows restorable objects of all users!

For manageability reasons the list is truncated after 100k entries.

Restoring Objects
+++++++++++++++++

Objects are restored using recycle restore <restore-key>. 
The <restore-key> is shown by **recycle ls**.

.. code-block:: bash
   EOS Console [root://localhost] |/eos/> recycle restore 00000000000007cf

   error: to recycle this file you have to have the role of the file owner: uid=755 (errc=1) (Operation not permitted)

You can only restore an object if you have the same uid/gid role 
like the object owner:

.. code-block:: bash
   
   EOS Console [root://localhost] |/eos/> role 755 1395 
   => selected user role ruid=<755> and group role rgid=<1395>

   EOS Console [root://localhost] |/eos/> recycle restore 00000000000007cf
   success: restored path=/eos/dev/2rep/subnode/tree

If the original path has been used in the mean while you will see the following 
after a restore command:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle restore 00000000000007cf
   error: the original path is already existing - use '--force-original-name' or '-f' to put the deleted file/tree back and rename the file/tree in place to <name>.<inode> (errc=17) (File exists)

The file can be restored using the force flag:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle restore -f 00000000000007cf
   warning: renamed restore path=/eos/dev/2rep/subnode/tree to backup-path=/eos/dev/2rep/subnode/tree.00000000000007d6
   success: restored path=/eos/dev/2rep/subnode/tree

Purging
+++++++

One can force to flush files in the recycle bin before the lifetime policy 
kicks in using recycle purge:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/2rep/subnode/> recycle purge
   success: purged 1 bulk deletions and 0 individual files from the recycle bin!

Notice that purging only removes files of the current uid/gid role. 
Running as **root** does not purge the recycle bin of all users!

Implementation
----------------
The implementation is hidden to the enduser and is explained to give some 
deeper insight to administrators. All the functionality is wrapped as demonstrated before in the CLI using the recycle command. 

The recycle bin resides in the namespace under the proc directory under ``/recycle/``.

Each deleted objects is moved into

``/recycle/<gid>/<uid>/<contracted-path>.<hex-inode>`` for files and

``/recycle/<gid>/<uid>/<contracted-path>.<hex-inode>.d`` for bulk deletions.

The internal structure is however not relevant or exported to the end-user. 
The contracted path flattens the full pathname replacing '/' with '#:#'.

The ``/recycle/`` directory is configured as a quota node with project space 
e.g. all files appearing in there are accounted on a catch-all project quota.

Deletion only succeeds if the recycle quota node has enough space available 
to absorb the deletion object.

A dedicated thread inside the MGM uses an optimized logic to follow the entries 
in the recycle tree and performs unrecoverable deletion according to the 
configured lifetime policy. The lifetime policy is defined via the external 
attribute sys.recycle.lifetime tagged on the /recycle directory specifying 
the file lifetime in seconds.

File deletions and bulk deletions are moved in the recycle bin if the parent 
directory of the deletion object specifies as external attribute ``sys.recycle=../recycle/``.

A restore operation can only succeed if the restore location provides the 
needed quota for all objects to be restored.

Note that a tree can have files owned by many individuals and restoration 
requires appropriate quota for all of them. As mentioned the restore operation 
has be executed with the role of the file or subtree top-level directory 
identity (uid/gid pair).
