.. highlight:: rst

.. index::
   single: Who API



who
===

Show statistics about active users.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/user/ | root://<host>//proc/user/
     ?mgm.cmd=who
     &eos.ruid=0
     &eos.rgid=0
     &mgm.option=[cnzas]m

CLI syntax
++++++++++

.. code-block:: text

   who [-c] [-n] [-z] [-a] [-m] [-s]                             :  print statistics about active users (idle<5min)
                -c                                                   -  break down by client host
                -n                                                   -  print id's instead of names
                -z                                                   -  print auth protocols
                -a                                                   -  print all
                -s                                                   -  print summary for clients
                -m                                                   -  print in monitoring format <key>=<value> 

