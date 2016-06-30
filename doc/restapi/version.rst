.. highlight:: rst

.. index::
   single: Version information



version
=======


Show version information.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/usre/ | root://<host>//proc/user/
     ?mgm.cmd=version
     &mgm.option=m
     &mgm.format=json
     &eos.ruid=0
     &eos.rgid=0

CLI syntax
++++++++++

.. code-block:: text

   version [-f] [-m]                                             :  print EOS version number
                -f                                                   -  print the list of supported features
                -m                                                   -  print in monitoring format
