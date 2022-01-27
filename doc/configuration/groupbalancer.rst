.. highlight:: rst

.. index::
   single: Group Balancer

Group Balancer
==============================

The group balancer uses the :doc:`converter` mechanism to move files from groups
above a given threshold filling state to groups under the threshold filling
state. Once the groups fall within the threshold they no longer participate in
balancing and thus prevents further oscillations, once the groups are in a
settled state.

Group Balancer Engine
---------------------

From EOS 4.8.74 2 different balancer engines are supported which can be switched
at runtime. A brief description of the various engines and their features are
described below. Please note that only one engine can be configured to run at a
time.

Std
~~~

This is the default engine, which uses deviation from the average groups filled
to decide which groups are the outliers to be balanced. Both the deviation from
the left and right can be configured individually to further fine tune how the
groups are picked for balancing. The parameter is to be entered as percent value
as deviation from average. Groups within the threshold values will not
participate in balancing. Files from groups above the threshold will be picked
at random within constraints (see `min/max_file_size` config below) and moved to
groups below threshold. The parameters expected for the engine are
`max_threshold` and `min_threshold`, groups above max_threshold deviation from
average and below min_threshold deviation from average will be the participating
groups. For compatibility the currently ``groupbalancer.threshold`` will be as a
default value in case both ``groupbalancer.min_threshold`` and
``groupbalancer.max_threshold`` aren't provided. It is recommended to explicitly
configure as this option may be removed in a future release.

MinMax
~~~~~~

This engine can be used as a stop gap engine to balance outliers, unlike the
std. engine no averages are computed, this engine takes static min & max
threshold values which are absolute `%` of groups fill ratio. Groups with usage
above the `max_threshold` (for eg 90%) will be chosen for filling to groups with
usage below `min_threshold`. While for almost all common use cases std. engine
should fit the bill, when needing to do targetted balancing only on certain
outliers this engine can be used as a temporary measure. This engine is only
recommended as a quick fix to balance outliers and then it is recommended to run
the std. engine to balance for longer periods of time.


Configuration
-------------
Groupbalancing is enabled/disabled by space:

.. code-block:: bash

   # enable
   eos space config default space.groupbalancer=on  
   # disable
   eos space config default space.groupbalancer=off

The current configuration of Group Balancing can be seen via

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   groupbalancer                    := on
   groupbalancer.engine             := std
   groupbalancer.file_attempts      := 50
   groupbalancer.max_file_size      := 20000000000
   groupbalancer.min_file_size      := 1000000000
   groupbalancer.max_threshold      := 5
   groupbalancer.min_threshold      := 5
   groupbalancer.ntx                := 1500
   groupbalancer.threshold          := 1  # Deprecated, this value will not be used if min/max thresholds are set
   ...

The ``max_file_size`` and ``min_file_size`` parameter decides the size of files
to be picked for transfer. The ``file_attempts`` is the number of attempts the
random picker will use to try to find a file within those sizes. For really
sparse file systems, where the probability of finding a file within the size
might be lower, it is possible to tweak this number. The number of
concurrent transfers to schedule is defined via the **groupbalancer.ntx** space
variable:

.. code-block:: bash

   # schedule 10 transfers in parallel
   eos space config default space.groupbalancer.ntx=10

Configure the groupbalancer engine:

.. code-block:: bash

   # configure the goupbalancer engine
   eos space config default space.groupbalancer.engine=std

The threshold in percent is defined via the **groupbalancer.min_threshold** &
**groupbalancer.max_threshold** variable. For std. balancer engine this is a
percent deviation from average:

.. code-block:: bash

   # set a 3 percent min threshold & 5 percent max threshold
   eos space config default space.groupbalancer.min_threshold=3
   eos space config default space.groupbalancer.max_threshold=5

In case you want to run the minmax balancer engine, here the values are
absolute values

   # set a 3 percent min threshold & 5 percent max threshold
   eos space config default space.groupbalancer.engine=minmax
   eos space config default space.groupbalancer.min_threshold=60
   eos space config default space.groupbalancer.max_threshold=80


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


Status
------

Status of the groupbalancer engine can be viewed with

.. code-block:: bash

   $ eos space groupbalancer status default
   Engine configured          : Std
   Current Computed Average   : 0.397366
   Min Deviation Threshold    : 0.03
   Max Deviation Threshold    : 0.05
   Total Group Size: 25
   Total Groups Over Threshold: 8
   Total Groups Under Threshold: 12
   # Detailed view of groups available with `--detail` switch
   $ eos space groupbalancer status default --detail
   engine configured          : Std
   Current Computed Average   : 0.397258
   Min Deviation Threshold    : 0.03
   Max Deviation Threshold    : 0.05
   Total Group Size: 25
   Total Groups Over Threshold: 8
   Total Groups Under Threshold: 12
   Groups Over Threshold
   ┌──────────┬──────────┬──────────┬──────────┐
   │Group     │ UsedBytes│  Capacity│    Filled│
   ├──────────┴──────────┴──────────┴──────────┤
   │default.8      2.75 T     6.00 T       0.46│
   │default.6      5.34 T     6.00 T       0.89│
   │default.5      2.78 T     6.00 T       0.46│
   │default.12     2.74 T     6.00 T       0.46│
   │default.11     2.77 T     6.00 T       0.46│
   │default.10     2.74 T     6.00 T       0.46│
   │default.3      2.83 T     6.00 T       0.47│
   │default.0      5.36 T     6.00 T       0.89│
   └───────────────────────────────────────────┘

   Groups Under Threshold
   ┌──────────┬──────────┬──────────┬──────────┐
   │Group     │ UsedBytes│  Capacity│    Filled│
   ├──────────┴──────────┴──────────┴──────────┤
   │default.9      2.19 T     6.00 T       0.36│
   │default.7      2.18 T     6.00 T       0.36│
   │default.24     1.78 T     6.00 T       0.30│
   │default.21     2.20 T     6.00 T       0.37│
   │default.2      1.47 G     6.00 T       0.00│
   │default.18     1.86 T     6.00 T       0.31│
   │default.17     2.17 T     6.00 T       0.36│
   │default.20     1.81 T     6.00 T       0.30│
   │default.15     1.80 T     6.00 T       0.30│
   │default.14     6.10 G     6.00 T       0.00│
   │default.13     2.15 T     6.00 T       0.36│
   │default.1      1.75 T     6.00 T       0.29│
   └───────────────────────────────────────────┘

For MinMax engines these numbers are absolute percent (for eg this was configured with 45 & 85)

.. code-block:: bash

   $ eos space groupbalancer status default
   Engine configured: MinMax
   Min Threshold    : 0.45
   Max Threshold    : 0.85
   Total Group Size: 25
   Total Groups Over Threshold: 9
   Total Groups Under Threshold: 4

There is a 60s cache for values, so if values are reconfigured

Traffic from the groupbalancer is tagged as ``eos/groupbalancer`` and visible in iostat

.. code-block:: bash

   eos io stat -x
    io │             application│    1min│    5min│      1h│     24h
   └───┴────────────────────────┴────────┴────────┴────────┴────────┘
   out        eos/groupbalancer  86.41 G 190.89 G   2.95 T  19.15 T
   out          eos/replication        0   1.49 G  52.96 G  52.96 G
   out                    other      605   1.33 K  10.77 K  64.73 K
   in         eos/groupbalancer  18.91 G  85.30 G   2.83 T  19.04 T
   in           eos/replication        0   1.43 G  52.90 G  52.90 G
   in                     other      605   1.33 K  10.77 K  64.73 K


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
