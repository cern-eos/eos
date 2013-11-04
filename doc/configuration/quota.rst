.. highlight:: rst

.. index::
   single: Quota System

Quota System
============

The EOS quota system provides user, group and project quota similiar to 
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

   eosdevsrv1:# eos -b quota
   # _______________________________________________________________________________________________
   # ==> Quota Node: /eos/dev/2rep/
   # _______________________________________________________________________________________________
   user       used bytes logi bytes used files aval bytes aval logib aval files filled[%]  vol-status ino-status
   adm        2.00 GB    1.00 GB    8.00 -     1.00 TB    0.5 TB     1.00 M-    0.00       ok         ok'

The above configuration defines user quota for user ``adm`` with 1 TB of volume 
quota and 1 Mio inodes under the directory subtree ``/eos/dev/plain``. 
As you may notice EOS distinguishes between logical bytes and (physical) bytes. 
Imagine a quota node subtree is configured to store 2 replica for each file, 
then a 1 TB quota allows you effectivly to store 0.5 TB (aval logib = 0.5 TB!). 

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

.. code-block::
   
   eosdevsrv1:# eos -b set -g 99 -p /eos/lhc/higgs-project/ -v 1P -i 100M

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

   eosdevsrv1:# eos -b quota

To see quota of all users (if you are an admin)::

.. code-block:: bash
 
   eosdevsrv1:# eos -b quota ls 

To see the quota node for a particular directory/subtree:

.. code-block:: bash

   eosdevsrv1:# eos -b quota ls /eos/lhc/higgs-project/
 
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

