.. highlight:: rst

.. index::
   single: Group Balancer

Group Balancer
==============================

The group balancer uses the :doc:`converter` mechanism to move files from groups 
above the average filling state to groups under the average filling state. To 
avoid oscillations a threshold parameter defines when group balancing stops e.g.
the deviation from the average in a group is less then the threshold parameter.

Configuration
-------------
Group balancing uses the relative filling state of a group and not absolute byte
values.

Groupbalancing is enabled/disabled by space:

.. code-block:: bash

   # enable
   eos space config default space.groupbalancer=on  
   # disable
   eos space config default space.groupbalancer=off

The curent status of Group Balancing can be seen via

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   groupbalancer                    := off
   groupbalancer.ntx                := 0
   groupbalancer.threshold          := 0.1
   ...

The number of concurrent transfers to schedule is defined via the **groupbalancer.ntx**
space variable:

.. code-block:: bash

   # schedule 10 transfers in parallel
   eos space config default space.groupbalancer.ntx=10

The threshold in percent is defined via the **groupbalancer.threshold** variable:

.. code-block:: bash

   # set a 5 percent threshold
   eos space config default space.groupbalancer.threshold=5

Make sure that you have enabled the converter and the **converter.ntx** space
variable is bigger than **groupbalancer.ntx** :

.. code-block:: bash
  
   # enable the converter
   eos space config default space.converter=on
   # run 20 conversion transfers in parallel
   eos space config default space.converter.ntx=20

One can see the same settings and the number of active conversion transfers
(scroll to the right):

.. code-block:: bash
   
   eos space ls 
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #     type #           name #  groupsize #   groupmod #N(fs) #N(fs-rw) #sum(usedbytes) #sum(capacity) #capacity(rw) #nom.capacity #quota #balancing # threshold # converter #  ntx # active #intergroup
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   spaceview           default           22           22    202       123          2.91 T       339.38 T      245.53 T          0.00     on        off        0.00          on 100.00     0.00         off

Log Files 
---------
The Group Balancer has a dedicated log file under ``/var/log/eos/mgm/GroupBalancer.log``
which shows basic variables used for balancing decisions and scheduled transfers. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info
