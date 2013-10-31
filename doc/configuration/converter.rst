.. highlight:: rst

Converter
=========

The converter serves two purposes: For :doc:`groupbalancer` and :doc:`geobalancer`
it is used to rewrite files with to a new location. For the LRU policy converter
is used to rewrite a file with a new layout e.g. rewrite a file with 2 replica 
into a RAID-6 like RAIN layout with the benefit of space savings.
Internally the converter uses the XRootD third party copy mechanism and consumes
one thread in the **MGM** for each running conversion transfer.

Configuration
-------------
The Converter is enabled/disabled by space:

.. code-block:: bash

   # enable
   eos space config default space.converter=on  
   # disable
   eos space config default space.converter=off

The current status of the Converter can be seen via:

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   converter                       := off
   converter.ntx                   := 0
   ...

The number of concurrent transfers to run is defined via the **converter.ntx**
space variable:

.. code-block:: bash

   # schedule 10 transfers in parallel
   eos space config default space.converter.ntx=10

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

The Converter has a dedicated log file under ``/var/log/eos/mgm/Converter.log``
which shows scheduled conversions and errors of conversion jobs. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info