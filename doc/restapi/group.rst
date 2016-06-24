.. highlight:: rst

.. index::
   single: Group API



group
=====

group ls
--------

List configured groups.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=group
     &mgm.subcmd=ls
     &eos.ruid=0
     &eos.rgid=0
     [&mgm.outformat=l|m|io|IO]
     [&mgm.outhost=brief]
     [&mgm.selection=<match>]

CLI syntax
++++++++++

.. code-block:: text

   group ls [-s] [-b|--brief] [-m|-l|--io|--IO] [<group>]           : list groups or only <group>. <group> is a substring match and can be a comma seperated list
    -s : silent mode
    -b,--brief : display host names without domain names
    -m : monitoring key=value output format
    -l : long output - list also file systems after each group
    --io : print IO statistics for the group
    --IO : print IO statistics for each filesystem

group rm
--------

Delete a group.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=group
     &mgm.subcmd=rm
     &eos.ruid=0
     &eos.rgid=0
     &mgm.group=<group>

CLI syntax
++++++++++

.. code-block:: text

   group rm <group-name>                                         : remove group

group set
--------

Activate/Deactivate a group.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=group
     &mgm.subcmd=set
     &eos.ruid=0
     &eos.rgid=0
     &mgm.group=<group>
     &mgm.group.state=on|off

CLI syntax
++++++++++

.. code-block:: text

   group set <group-name> on|off                                 : activate/deactivate group
     => when a group is (re-)enabled, the drain pull flag is recomputed for all filesystems within a group
     => when a group is (re-)disabled, the drain pull flag is removed from all members in the group
