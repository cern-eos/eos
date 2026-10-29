.. highlight:: rst

.. index::
   single: Quota System

Quota System
============

The EOS quota system provides user, group and project quota similar to
filesystems like EXT4, XFS ... e.g. quota is expressed as max. number of
inodes(=files) and maximum volume. The implementation of EOS quota uses the
given inode limit as hard quota, while volume is applied as soft quota e.g.
it can be slightly exceeded.

Quota is attached to a so called 'quota node'. A quota node defines the
quota rules and counting for a subtree of the namespace. If the subtree
contains another quota node in a deeper directory level quota is rooted
on the deeper node.

As an example we can define two quota nodes:

.. epigraph::

   ============ =======================
   Node         Path
   ============ =======================
   Quota Node 1 /eos/lhc/raw/
   Quota Node 2 /eos/lhc/raw/analysis/
   ============ =======================

A file like ``/eos/lhc/raw/2013/raw-higgs.root`` is accounted for in the first
quota node, while a file ``/eos/lhc/raw/analysis/histo-higgs.root`` is
accounted for in the second quota node.

The quota system is easiest explained lookint at the output of
a **quota** command in the EOS shell:

.. code-block:: bash

   eosdevsrv1:# eos quota
   # _______________________________________________________________________________________________
   # ==> Quota Node: /eos/dev/2rep/
   # _______________________________________________________________________________________________
   user       used bytes logi bytes used files aval bytes aval logib aval files filled[%]  vol-status ino-status
   adm        2.00 GB    1.00 GB    8.00 -     1.00 TB    0.5 TB     1.00 M-    0.00       ok         ok

The above configuration defines user quota for user ``adm`` with 1 TB of volume
quota and 1 Mio inodes under the directory subtree ``/eos/dev/plain``.
As you may notice EOS distinguishes between logical bytes and (physical) bytes.
Imagine a quota node subtree is configured to store 2 replica for each file,
then a 1 TB quota allows you effectively to store 0.5 TB (aval logib = 0.5 TB!).

.. warning::

   All quota set via the 'quota set' command is defining the (physical) bytes
   and EOS displays the logical bytes value based on the layout definition on
   the quota node.

The volume and inode status is displayed as 'ok' if there is quota left for
volume/inodes. If there is less than **5%** left, 'warning' is displayed,
if there is none left 'exceeded'. If volume and/or inode quota is set to 0
'ignored' is displayed. In this case a quota setting of 0 signals not to apply
the quota however if both are '0' the referenced UID/GID has no quota.

There are three types of quota defined in EOS: user, group & project quota!

User Quota
----------

User quota defines volume/inode quota based on user id  UID.
It is possible to combine user and group quota on a quota node.
In this case both have to 'ok' e.g. provide enough space for a file placmment.

Group Quota
-----------
Group quota defines volume/inode quota based on group id GID.
As described before it is possible to combine group and user quota.
In this case both have to allow file placement.

Project Quota
-------------
Project quota books all volume/inode usage under the project subtree to a single
project account. E.g. the recycle bin uses this quota type to measure a subtree
size. In the EOS shell interface project quota is currently defined setting
quota for group 99:

.. code-block:: bash

   eosdevsrv1:# eos set -g 99 -p /eos/lhc/higgs-project/ -v 1P -i 100M

Space Quota
-----------
It is possible to set a physical space restriction using the space parameter **nominalsize**

.. code-block:: bash

   # restrict the physical space usage to 1P
   eosdevsrv1:# eos space config default space.nominalsize=1P

The restriction is only used, if the connected user is not in the **sudoer** list. The current usage and space setting is
cached for 30s e.g. it might take up to 30s until any change may take effect.

Quota Enforcement
-----------------
Quota enforcement is applied when new files are placed and when files in RW mode
are closed e.g. EOS can reject to store a file if the quota exceeds during an
upload. If user and group quota is defined, both are applied.

Quota Command Line Interface
----------------------------

List Quota
++++++++++
To see your quota as a user use:

.. code-block:: bash

   eosdevsrv1:# eos quota

To see quota of all users (if you are an admin):

.. code-block:: bash

   eosdevsrv1:# eos quota ls

To see the quota node for a particular directory/subtree:

.. code-block:: bash

   eosdevsrv1:# eos quota ls /eos/lhc/higgs-project/

Add ``-r`` (``--recursive``) to list every quota node at or below the given
path instead of only the one responsible for it. Without a path, ``quota ls``
already lists every quota node:

.. code-block:: bash

   eosdevsrv1:# eos quota ls -r /eos/lhc/

Add ``-s`` (``--summary``) to print one line per selected quota node and a
total line instead of the full tables. Combined with ``-r`` this gives an
overview of a subtree:

.. code-block:: bash

   eosdevsrv1:# eos quota ls -r -s /eos/lhc/

The columns follow ``quota ls``: used bytes (physical), used logical bytes and
used files, then the corresponding limits, the filled percentage of the logical
volume and the volume and inode status. For a project quota node the project
quota is shown, otherwise the sum of all user quotas or, if there is no user
quota, the sum of all group quotas; the **type** column tells which one. Limits
which are not defined are shown as ``-``.

Since every file is accounted only to the deepest quota node above it, the
usage in the total line is the sum of all listed quota nodes. A limit in the
total line is only shown if every listed quota node defines it with the same
type, otherwise it is shown as ``-`` and the type as ``mixed``.

With ``-u`` or ``-g`` the summary shows the quota of that user or group on
every selected quota node, skipping the nodes where it has neither quota nor
usage. This shows where a user or group consumes quota in a subtree:

.. code-block:: bash

   eosdevsrv1:# eos quota ls -r -s -u alice /eos/lhc/

Add ``-m`` to get the same information in monitoring ``<key>=<value>`` format,
or use the global ``--json`` option:

.. code-block:: bash

   eosdevsrv1:# eos quota ls -r -s -m /eos/lhc/
   quota=summary space=/eos/lhc/raw/ type=project usedbytes=... usedlogicalbytes=... usedfiles=... maxbytes=... maxlogicalbytes=... maxfiles=... percentageusedbytes=... statusbytes=ok statusfiles=ok
   ...
   quota=total space=/eos/lhc/ type=mixed usedbytes=... usedlogicalbytes=... usedfiles=... maxbytes=0 maxlogicalbytes=0 maxfiles=0 percentageusedbytes=... statusbytes=ignored statusfiles=ignored

   eosdevsrv1:# eos quota ls -r -s -m -u alice /eos/lhc/
   quota=summary uid=alice space=/eos/lhc/raw/ usedbytes=... ...

Set Quota
+++++++++

The syntax to set quota is:

.. code-block:: bash

   eos quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] -p <path>

The <uid>, <gid> parameter can be numerica or the real name. Volume and Inodes
can be specified as **1M**, **1P** etc. or a plain number.

.. ::note

   To set project quota use GID 99!

Delete Quota
+++++++++++++

A quota setting can be removed using:

.. code-block:: bash

   eos quota rm -u <uid> |-g <gid> -p <path>

One has to specify to remove the user or the group quota, it is not possible
to remove both with a single command.


Delete Quota Node
+++++++++++++++++
Sometimes it is necessary to remove completely a quota node.
This can be done via:

.. code-block:: bash

   eos quota rmnode -p <path>

The command will ask for a security code. Be aware the quota is not recalculated
from scratch if the deletion of a node would now leave the accounting to an
upstream node.
