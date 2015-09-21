group
-----

.. code-block:: text

   usage: group ls                                                      : list groups
   usage: group ls [-s] [-m|-l|--io] [<group>]                          : list groups or only <group>. <group> is a substring match and can be a comma seperated list
      -s : silent mode
      -m : monitoring key=value output format
      -l : long output - list also file systems after each group
      --io : print IO statistics for the group
      --IO : print IO statistics for each filesystem
      group rm <group-name>                                         : remove group
      group set <group-name> on|off                                 : activate/deactivate group
      => when a group is (re-)enabled, the drain pull flag is recomputed for all filesystems within a group
      => when a group is (re-)disabled, the drain pull flag is removed from all members in the group
