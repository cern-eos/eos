.. highlight:: rst

.. index::
   single: Namespace API



ns
===

ns stat
-------

Print namespace statistics.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=ns
     &mgm.subcmd=stat
     &eos.ruid=0
     &eos.rgid=0
     &mgm.option=[anr]m

CLI syntax
++++++++++

.. code-block:: text

       ns stat [-a] [-m] [-n]                                     :  print namespace statistics
                -a                                                   -  break down by uid/gid
                -m                                                   -  print in <key>=<val> monitoring format
                -n                                                   -  print numerical uid/gids
                --reset                                              -  reset namespace counter (option r)
