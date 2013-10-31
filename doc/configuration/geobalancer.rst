.. highlight:: rst

GEO Balancer
==============================

The geo balancer uses the :doc:`converter` mechanism to redistribute files according 
to their geographical location. Currently it is only moving files with replica 
layouts. To avoid oscillations a threshold parameter defines when geo balancing stops e.g.
the deviation from the average in a group is less then the threshold parameter.

Configuration
-------------
GEO balancing uses the relative filling state of a geo tag and not absolute byte
values.

GEO balncing is enabled/disabled by space:

.. code-block:: bash

   # enable
   eos space config default space.geobalancer=on  
   # disable
   eos space config default space.geobalancer=off

The curent status of GEO Balancing can be seen via

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   geobalancer                    := off
   geobalancer.ntx                := 0
   geobalancer.threshold          := 0.1
   ...

The number of concurrent transfers to schedule is defined via the **geobalancer.ntx**
space variable:

.. code-block:: bash

   # schedule 10 transfers in parallel
   eos space config default space.geobalancer.ntx=10

The threshold in percent is defined via the **geobalancer.threshold** variable:

.. code-block:: bash

   # set a 5 percent threshold
   eos space config default space.geobalancer.threshold=5

Make sure that you have enabled the converter and the **converter.ntx** space
variable is bigger than **geobalancer.ntx** :

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

:: ::warning
   You have to configure geo mapping for clients, atleast for the MGM machine,
   otherwise EOS does not apply the geoplacement/scheduling algorithm and GEO
   Balancing does not give the expected results!

Log Files 
---------
The GEO Balancer has a dedicated log file under ``/var/log/eos/mgm/GeoBalancer.log``
which shows basic variables used for balancing decisions and scheduled transfers. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info
