.. index::
   pair: MGM; Microservices

.. highlight:: rst

.. _microservices:


MGM Microservices
=================

The EOS MGM service incorporates several embedded sub-services, many of them are disabled by default.
Most of them are implement in an asynchronous thread running as part of the meta-data service.

.. index::
   pair: MGM; Converter


Converter
---------

The converter functionality serves several purposes: 



.. index::
   pair: Converter; Engine

Converter Engine
^^^^^^^^^^^^^^^^

The Converter Engine is responsible for scheduling
and performing file conversion jobs. A conversion job means rewriting a file
with a different storage parameter: layout, replica number, space
or placement policy. The functionality is used for serveral purposes: For the Balancer
it is used to rewrite files to achieve a new placement. For the LRU policy converter
is used to rewrite a file with a new layout e.g. rewrite a file with 2 replica 
into a RAID-6 like RAIN layout with the benefit of space savings.
Internally the converter uses the XRootD third party copy mechanism and consumes
one thread in the **MGM** for each running conversion transfer.

The Converter Engine is split into two main components:
*Converter Driver* and *Converter Scheduler*.


.. index::
   pair: Converter; Driver

Converter Driver
"""""""""""""""""

The Converter Driver is the component responsible for performing the actual
conversion job. This is done using XRootD third party copy between the FSTs.

The Converter Driver keeps a threadpool available for conversion jobs.
Periodically, it queries QuarkDB for conversion jobs, in batches of 1000. 
The retrieved jobs are scheduled, one per thread, up to a configurable 
runtime threads limit. After each scheduling, a check is performed 
to identify completed or failed jobs.
  
Successful conversion jobs:
  - get removed from the QuarkDB pending jobs set
  - get removed from the MGM in-flight jobs tracker

Failed conversion jobs:
  - get removed from the QuarkDB pending jobs set
  - get removed from the MGM in-flight jobs tracker
  - get updated to the QuarkDB failed jobs set
  - get updated to the MGM failed jobs set

Within QuarkDB, the following hash sets are used:

.. code-block:: bash

  eos-conversion-jobs-pending
  eos-conversion-jobs-failed

Each hash entry has the following structure: *<fid>:<conversion_info>*.

.. index::
   pair: Conversion; Info

Conversion Info
~~~~~~~~~~~~~~~

A conversion info is defined as following:

.. code-block:: bash

  <fid(016hex)>:<space[.group]>#<layout(08hex)>[~<placement>]

    <fid>       - 16-digit with leading zeroes hexadecimal file id
    <space>     - space or space.group notation
    <layout>    - 8-digit with leading zeroes hexadecimal layout id
    <placement> - the placement policy to apply

The job info is parsed by the Converter Driver before creating 
the associated job. Entries with invalid info are simply discarded 
from the QuarkDB pending jobs set.

.. index::
   pair: Conversion; Job

Conversion Job
~~~~~~~~~~~~~~

A conversion job goes through the following steps:
  - The current file metadata is retrieved
  - The TPC job is prepared with appropriate opaque info
  - The TPC job is executed
  - Once TPC is completed, verify the new file has all fragments according to layout
  - Verify initial file hasn't changed (checksum is the same)
  - Merge the conversion entry with the initial file
  - Mark conversion job as completed

If at any step a failure is encountered, the conversion job
will be flagged as failed.

.. index::
   pair: Converter; Scheduler

Converter Scheduler
"""""""""""""""""""

The Converter Scheduler is the component responsible for creating conversion jobs,
according to a given set of conversion rules. A conversion rule is placed
on a namespace entry (file or directory), contains optional filters
and the target storage parameter.

- When a conversion rule is placed on a file, an immediate conversion job is created
  and pushed to QuarkDB.
- When a conversion rule is placed on a directory, a tree traversal is initiated
  and all files which pass the filtering criteria will be scheduled for conversion.

.. index::
   pair: Converter; Configuration


Configuration
^^^^^^^^^^^^^
The Converter is enabled/disabled by space:

.. code-block:: bash

   # enable
   eos space config default space.converter=on  
   # disable
   eos space config default space.converter=off

.. warning:: Be aware that you have to grant project quota in the converter directory if your instances has quota enabled, otherwise
	     the converter cannot write files because the same quota restrictions apply

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


.. index::
   pair: Converer; Log Files

Log Files
^^^^^^^^^

The Converter has a dedicated log file under ``/var/log/eos/mgm/Converter.log``
which shows scheduled conversions and errors of conversion jobs. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM


.. index::
   pair: MGM; Balancing

Balancing
---------

The rebalacing system is made out of three services:

.. epigraph::
  
   ========================= ======================================================================
   Name                      Responsability
   ========================= ======================================================================
   Filesystem Balancer       Balance relative usage between all filesystem within a group
   Group Balancer            Balance relative usage between groups
   GEO Balancer              Balance relative usage between geographic locations
   ========================= ======================================================================

.. index::
   pair: Balancer; File System Balancer

Filesystem Balancer
^^^^^^^^^^^^^^^^^^^

Overview
"""""""""

The filesystem balancing system provides a fully automated mechanism to balance the 
volume usage across a scheduling group. Hence currently the balancing system 
does not balance between scheduling groups!

The balancing system is made up by the cooperation of several components:

* Central File System View with file system usage information and space configuration
* Centrally running balancer thread steering the filesystem balancer process by computing averages and deviations
* Balancer Thread on each FST pulling workload to pull files locally to balance filesystems

.. ::note

   Balancing is en-/disabled in each space seperatly!

.. index::
   pair: Balancer; Info

Balancing View and Configuration
"""""""""""""""""""""""""""""""""

Each filesystem advertises the used volume and the central view allows to see 
the deviation from the average filesystem usage in each group.

.. code-block:: bash

   EOS Console [root://localhost] |/> group ls
   #---------------------------------------------------------------------------------------------------------------------
   #     type #           name #     status #nofs #dev(filled) #avg(filled) #sig(filled) #balancing #  bal-run #drain-run
   #---------------------------------------------------------------------------------------------------------------------
   groupview  default.0                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.1                  on     8         0.28         0.10         0.12 idle                0          0
   groupview  default.10                 on     8         0.29         0.10         0.13 idle                0          0
   groupview  default.11                 on     8         0.29         0.10         0.13 idle                0          0
   groupview  default.12                 on     7         0.28         0.11         0.14 idle                0          0
   groupview  default.13                 on     8         0.28         0.12         0.14 idle                0          0
   groupview  default.14                 on     8         0.29         0.10         0.13 idle                0          0
   groupview  default.15                 on     8         0.30         0.10         0.13 idle                0          0
   groupview  default.16                 on     7         0.26         0.12         0.13 idle                0          0
   groupview  default.17                 on     8         0.28         0.12         0.14 idle                0          0
   groupview  default.18                 on     8         0.30         0.10         0.14 idle                0          0
   groupview  default.19                 on     8        12.42         4.76         6.80 idle                0          0
   groupview  default.2                  on     8         0.48         0.16         0.23 idle                0          0
   groupview  default.20                 on     8        14.03         5.43         7.62 idle                0          0
   groupview  default.21                 on     8         0.48         0.16         0.23 idle                0          0
   groupview  default.3                  on     8         0.28         0.10         0.12 idle                0          0
   groupview  default.4                  on     8         0.26         0.11         0.13 idle                0          0
   groupview  default.5                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.6                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.7                  on     8         0.27         0.09         0.12 idle                0          0
   groupview  default.8                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.9                  on     8         0.30         0.11         0.14 idle                0          0


The decision parameters to enable balancing in a group is the maximum deviation 
of the filling state (given in %). 
In this example two groups are unbalanced (12 + 14 %).

The balancing is configured on the space level and the current configuration 
is displayed using the 'space status' command:

.. code-block:: bash

   EOS Console [root://localhost] |/> space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   balancer                         := off
   balancer.node.ntx                := 10
   balancer.node.rate               := 10
   balancer.threshold               := 1
   ...

.. index::
   pair: Balancer; Configuration

The configuration variables are:

.. epigraph::
  
   ========================= ======================================================================
   variable                  definition
   ========================= ======================================================================
   balancer                  can be off or on to disable or enable the balancing
   balancer.node.ntx         number of parallel balancer transfers running on each FST
   balancer.node.rate        rate limitation for each running balancer transfer in MB/s
   balancer.threshold        percentage at which balancing get's enabled within a scheduling group
   ========================= ======================================================================
 
If balancing is enabled ....

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default space.balancer=on
   success: balancer is enabled!

Groups which are balancing are shown via the **eos group ls** command:

.. code-block:: bash

   EOS Console [root://localhost] |/> group ls
   #---------------------------------------------------------------------------------------------------------------------
   #     type #           name #     status #nofs #dev(filled) #avg(filled) #sig(filled) #balancing #  bal-run #drain-run
   #---------------------------------------------------------------------------------------------------------------------
   groupview  default.0                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.1                  on     8         0.28         0.10         0.12 idle                0          0
   groupview  default.10                 on     8         0.29         0.10         0.13 idle                0          0
   groupview  default.11                 on     8         0.29         0.10         0.13 idle                0          0
   groupview  default.12                 on     7         0.28         0.11         0.14 idle                0          0
   groupview  default.13                 on     8         0.28         0.12         0.14 idle                0          0
   groupview  default.14                 on     8         0.29         0.10         0.13 idle                0          0
   groupview  default.15                 on     8         0.30         0.10         0.13 idle                0          0
   groupview  default.16                 on     7         0.26         0.12         0.13 idle                0          0
   groupview  default.17                 on     8         0.28         0.12         0.14 idle                0          0
   groupview  default.18                 on     8         0.30         0.10         0.14 idle                0          0
   groupview  default.19                 on     8        12.42         4.76         6.80 balancing          10          0
   groupview  default.2                  on     8         0.48         0.16         0.23 idle                0          0
   groupview  default.20                 on     8        14.03         5.43         7.62 balancing          12          0
   groupview  default.21                 on     8         0.48         0.16         0.23 idle                0          0
   groupview  default.3                  on     8         0.28         0.10         0.12 idle                0          0
   groupview  default.4                  on     8         0.26         0.11         0.13 idle                0          0
   groupview  default.5                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.6                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.7                  on     8         0.27         0.09         0.12 idle                0          0
   groupview  default.8                  on     8         0.27         0.10         0.12 idle                0          0
   groupview  default.9                  on     8         0.30         0.11         0.14 idle                0          0

The current balancing can also be viewed by space or node:

.. code-block:: bash

   EOS Console [root://localhost] |/> space ls --io
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #     name # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------
   default       0.02        66.00        66.00        862         57         60     31     22      1.99 TB    347.33 TB     805.26 k     16.97 G         51          0

   EOS Console [root://localhost] |/> node ls --io
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #               hostport # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   lxfsra02a02.cern.ch:1095       0.08        41.00         0.00        119          0         41     23      0    825.47 GB     41.92 TB     298.80 k      2.05 G          0          0
   lxfsra02a05.cern.ch:1095       0.03        19.00         0.00        119          0         19      2      0    832.01 GB     43.92 TB     152.14 k      2.15 G          0          0
   lxfsra02a06.cern.ch:1095       0.01         0.00        11.00        119         12          0      0      6     70.05 GB     43.92 TB      54.77 k      2.15 G         10          0
   lxfsra02a07.cern.ch:1095       0.01         0.00        11.00        119          9          0      0      3     79.95 GB     43.92 TB      75.91 k      2.15 G         10          0
   lxfsra02a08.cern.ch:1095       0.01         0.00        11.00        119          9          0      0      2     52.01 GB     43.92 TB      61.25 k      2.15 G          8          0
   lxfsra04a01.cern.ch:1095       0.01         0.00        10.00        119          9          0      0      1     72.12 GB     41.92 TB      60.92 k      2.05 G          8          0
   lxfsra04a02.cern.ch:1095       0.01         0.00        10.00        119          9          0      0      7     52.32 GB     43.92 TB      86.72 k      2.15 G         10          0
   lxfsra04a03.cern.ch:1095       0.01         0.00        10.00        119          9          0      0      5     10.53 GB     43.92 TB      14.80 k      2.15 G          5          0

To see the usage difference within the group, one can inspect all the group filesystems via **eos group ls --IO** e.g.

.. code-block:: bash

   EOS Console [root://localhost] |/> group ls --IO default.20
   #---------------------------------------------------------------------------------------------------------------------
   #     type #           name #     status #nofs #dev(filled) #avg(filled) #sig(filled) #balancing #  bal-run #drain-run
   #---------------------------------------------------------------------------------------------------------------------
   groupview  default.20                 on     8        13.71         5.48         7.47 balancing          37          0
   #.................................................................................................................................................................................................................
   #                     hostport #  id #     schedgroup # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #.................................................................................................................................................................................................................
   lxfsra02a05.cern.ch:1095    17       default.20       0.47        12.00         0.00        119          0         21      1      0    383.17 GB      2.00 TB      59.33 k     97.52 M          0          0
   lxfsra02a06.cern.ch:1095    35       default.20       0.08         0.00         6.00        119         10          0      0      6     26.56 GB      2.00 TB       6.23 k     97.52 M          7          0
   lxfsra04a01.cern.ch:1095    57       default.20       0.13         0.00         6.00        119          9          0      0      4     25.01 GB      2.00 TB       6.11 k     97.52 M          4          0
   lxfsra02a08.cern.ch:1095    77       default.20       0.08         0.00         6.00        119         11          0      0      5     27.36 GB      2.00 TB       6.64 k     97.52 M          8          0
   lxfsra04a02.cern.ch:1095    99       default.20       0.07         0.00         4.00        119         10          0      0      3     26.57 GB      2.00 TB       7.75 k     97.52 M          6          0
   lxfsra02a02.cern.ch:1095   121       default.20       1.00        22.00         0.00        119          0         41     21      0    351.07 GB      2.00 TB      59.80 k     97.52 M          0          0
   lxfsra02a07.cern.ch:1095   143       default.20       0.10         0.00         7.00        119          9          0      0      2     28.57 GB      2.00 TB       7.46 k     97.52 M          7          0
   lxfsra04a03.cern.ch:1095   165       default.20       0.12         0.00         6.00        119         10          0      0      5      7.56 GB      2.00 TB       2.96 k     97.52 M          5          0

 
The scheduling activity for balancing can be monitored with the **eos ns ls** command:

.. code-block:: bash

   EOS Console [root://localhost] |/> ns stat
   # ------------------------------------------------------------------------------------
   # Namespace Statistic
   # ------------------------------------------------------------------------------------
   ALL      Files                            682781 [booted] (12s)
   ALL      Directories                      1316
   # ....................................................................................
   ALL      File Changelog Size              804.27 MB
   ALL      Dir  Changelog Size              515.98 kB
   # ....................................................................................
   ALL      avg. File Entry Size             1.18 kB
   ALL      avg. Dir  Entry Size             392.00 B
   # ------------------------------------------------------------------------------------
   ALL      Execution Time                   0.40 +- 1.12
   # -----------------------------------------------------------------------------------------------------------
   who      command                          sum             5s     1min     5min       1h exec(ms) +- sigma(ms)
   # -----------------------------------------------------------------------------------------------------------
   ALL        Access                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-     
    ....
   ALL        Schedule2Balance                         6423    11.75    10.81    10.71     1.78     -NA- +- -NA-     
   ALL        Schedule2Drain                              0     0.00     0.00     0.00     0.00     -NA- +- -NA-     
   ALL        Scheduled2Balance                        6423    11.75    10.81    10.71     1.78     4.20 +- 0.57 
   ALL        SchedulingFailedBalance                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-

   
The relevant counters are:

.. epigraph::
   
   ============================== =====================================================================
   state                          definition
   ============================== =====================================================================
   Schedule2Balance               counter/rate at which all FSTs ask for a file to balance
   ScheduledBalance               counter/rate of balancing transfers which have been scheduled to FSTs
   SchedulingFailedBalance        counter/rate of scheduling requests which could not get any workload
                                  (e.g. no file matches the target machine)
   ============================== =====================================================================

.. index::
   pair: Balancer; Group Balancer

Group Balancer
^^^^^^^^^^^^^^

The group balancer uses the converter mechanism to move files from groups
above a given threshold filling state to groups under the threshold filling
state. Once the groups fall within the threshold they no longer participate in
balancing and thus prevents further oscillations, once the groups are in a
settled state.


.. index::
   pair: Group Balancer; Engine


Group Balancer Engine
"""""""""""""""""""""

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

.. index::
   pair: Group Balancer; Configuration

Freespace
~~~~~~~~~

This engine can be used in case groups have non uniform total capacities and you
want to make the absolute free space equal in all groups. The geoscheduler picks
groups in a round robin fashion, so having absolute freespace equal makes it
easy to keep groups in balance after. The same parameters `max_threshold` and
`min_threshold` can be used to tweak the spread of total freespace allowed. Additionally a list of groups that do not need to participate in balancing activity can be configured via the key ``groupbalancer.blocklist``. For adding removing the same key needs to be set again to the new value.


Configuration
"""""""""""""
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
might be lower, it is possible to tweak this number. The number of concurrent
transfers to schedule is defined via the **groupbalancer.ntx** space variable,
this is the number of transfers in every cycle of groupbalancer scheduling,
which is every 10s. Hence it is recommended to set a min value in the hundreds
or around 1000 (and watch the progress occasionally with eos io stat) if the
groups are really unbalanced:

.. code-block:: bash

   # schedule 10 transfers in parallel
   eos space config default space.groupbalancer.ntx=1000

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


Configure blocklisting, ie. groups that do not participate. (Only used in freespace engine currently)

.. code-block:: bash

   # blocklist groups default.2, default.8 in participating
   eos space config default space.groupbalancer.blocklist=default.2, default.8

.. index::
   pair: Group Balancer; Info

Status
"""""""

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

.. index::
   pair: Group Balancer; Log Files

Log Files
""""""""""
The Group Balancer has a dedicated log file under ``/var/log/eos/mgm/GroupBalancer.log``
which shows basic variables used for balancing decisions and scheduled transfers. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info

.. index::
   pair: Balancer; GEO Balancer


GEO Balancer
^^^^^^^^^^^^

The GEO Balancer uses the converter mechanism to redistribute files according 
to their geographical location. Currently it is only moving files with replica 
layouts. To avoid oscillations a threshold parameter defines when geo balancing stops e.g.
the deviation from the average in a group is less then the threshold parameter.

.. index::
   pair: GEO Balancer; Configuration

Configuration
"""""""""""""
GEO balancing uses the relative filling state of a geo tag and not absolute byte
values.

GEO balancing is enabled/disabled by space:

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

.. warning::
   You have to configure geo mapping for clients, at least for the MGM machine,
   otherwise EOS does not apply the geoplacement/scheduling algorithm and GEO
   Balancing does not give the expected results!

.. index::
   pair: GEO Balanacer; Log Files

Log Files 
"""""""""
The GEO Balancer has a dedicated log file under ``/var/log/eos/mgm/GeoBalancer.log``
which shows basic variables used for balancing decisions and scheduled transfers. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info

.. index::
   pair: MGM; Draining

Draining
--------

The drain system contains two engines:

* Filesystem Draining
* Group Draining

.. index::
   pair: Draining; Filesystem Draining

Filesystem Draining
^^^^^^^^^^^^^^^^^^^

Overview
""""""""

The EOS drain system provides a fully automatic mechanism to drain (empty)
filesystems under certain error conditions. A file system drain is triggered
by an IO error on a file system or manually by an operator setting a
filesystem in drain mode.

The drain engine makes use of the GeoTreeEngine component to decide where
to move the drained replicas. The drain proccesses are spawned on the MGM and
represent simple XRootD third-party-copy transfers.

.. index::
   pair: FST; Scrubber


FST Scrubber
~~~~~~~~~~~~

Each FST run's a dedicated thread doing scrubbing. Scrubbing is running if the
file system configuration is at least **wo** ( e.g. in write-only or read-write mode),
the file system is in **booted** state and the label of the
filesystem ``<mountpoint>/.eosfsid + <mountpoint>/.eosfsuuid`` is readable.
If the label is not readable the Scrubber broadcasts an IO error for filesystems
in **ro**, **wo** or **rw** mode and **booted** state with the error text
"filesystem seems to be not mounted anymore".

The FST scrubber follows the filling size of a disk and writes test pattern
files at 0%, 10%, 20% ... 90% filling with the goal to do tests equally
distributed over the physical size of the disk. At each 10% filling position
the scrubber creates a write-once file to be re-read in each scrubbing pass
and a re-write file which is re-written and re-read in each scrubbing pass.
The following pattern is written into the test files:

.. code-block:: bash

   scrubPattern[0][i]=0xaaaa5555aaaa5555ULL;
   scrubPattern[0][i+1]=0x5555aaaa5555aaaaULL;
   scrubPattern[1][i]=0x5555aaaa5555aaaaULL;
   scrubPattern[1][i+1]=0xaaaa5555aaaa5555ULL;

Pattern 0 or pattern 1 is selected randomly. Each test file has 1MB size and
the scrub file names are ``<mountpoint>/scrub.write-once.[0-9]`` and
``<mountpoint>/scrub.re-write.[0-9]``.

In case an error is detected, the FST broadcasts an EIO to the MGM with the
error text "filesystem probe error detected".

You can see filesystems in error state and the error text on the MGM node doing:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -e
   #...............................................................................................
   #                   host #   id #     path #       boot # configstatus #      drain #... #errmsg
   #...............................................................................................
        lxfsrk51a02.cern.ch   3235    /data05  opserror            empty      drained   5 filesystem seems to be
                                                                                          not mounted anymore
        lxfsrk51a04.cern.ch   3372    /data19  opserror            empty      drained   5 filesystem probe error detected


.. index::
   pair: Filesystem; Statemachine
   pair: Filesystem; View

Central File System View and State Machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each filesystem in EOS has a configuration, boot state and drain state.

The possible configuration states are self explaining:

.. epigraph::

   ============= ======================================================================================
   state          definition
   ============= ======================================================================================
   rw            filesystem set in read write mode
   wo            filesystem set in write-once mode
   ro            filesystem set in read-only mode
   drain         filesystem set in drain mode
   off           filesystem set disabled
   empty         filesystem is empty e.g. contains no files any more
   ============= ======================================================================================

File systems involved in any kind of IO need to be in boot state booted.

The configured file systems are shown via:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls

   #.........................................................................................................................
   #                   host (#...) #   id #           path #     schedgroup #       boot # configstatus #      drain # active
   #.........................................................................................................................
        lxfsra02a05.cern.ch (1095)      1          /data01        default.0       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      2          /data02       default.10       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      3          /data03        default.1       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      4          /data04        default.2       booted             rw      nodrain   online
        lxfsra02a05.cern.ch (1095)      5          /data05        default.3       booted             rw      nodrain   online

As shown each file system has also a drain state. Drain states can be:

.. epigraph::

   ================ ==============================================================================================================================================================================
   state            definition
   ================ ==============================================================================================================================================================================
   nodrain          file system is currently not draining
   prepare          the drain process is prepared - this phase lasts 60 seconds
   wait             the drain process either waits for the namespace to be booted or it is waiting that the graceperiod has passed (see below)
   draining         the drain process is enabled - nodes inside the scheduling group start to pull transfers to drop replicas from the filesystem to drain
   stalling         in the last 5 minutes there was noprogress of the drain procedure. This happens if the files to transfer are very huge or there are only files left which cannot be replicated.
   expired          the time defined by the drainperiod variable has passed and the drain process is stopped. There are files left on the disk which couldn't be drained.
   drained          all files have been drained from the filesystem.
   failed           the drain activity is finished but there are still files on file system that could not be drained and require a manual inspection.
   ================ ==============================================================================================================================================================================

The final state can be one of the following: expired, failed or drained.

The drain and grace periods are defined as a space variables (e.g. automatically
applied to all filesystems in that space when they are moved into or registered).

One can see the settings via the space command:

.. code-block:: bash

   EOS Console [root://localhost] |/> space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   balancer                         := on
   balancer.node.ntx                := 10
   balancer.node.rate               := 10
   balancer.threshold               := 1
   drainer.node.ntx                 := 10
   drainer.node.rate                := 25
   drainperiod                      := 3600
   graceperiod                      := 86400
   groupmod                         := 24
   groupsize                        := 20
   headroom                         := 0.00 B
   quota                            := off
   scaninterval                     := 1

They can be modified by setting the *drainperiod* or *graceperiod* variable in
number of seconds:

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default space.drainperiod=86400
   success: setting drainperiod=86400

   EOS Console [root://localhost] |/> space config default space.graceperiod=86400
   success: setting graceperiod=86400

.. warning::
   This defines the variables only if filesystems are registered or moved into that space.

If you want to apply this setting to all filesystems in that space,
you have additionally to call:

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default fs.drainperiod=86400
   EOS Console [root://localhost] |/> space config default fs.graceperiod=86400

If you want a global overview about running drain processes, you can get the
number of running drain transfers by space, by group, by node and by filesystem:

.. code-block:: bash

   EOS Console [root://localhost] |/> space ls --io
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #     name # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------
   default       0.01        32.00        17.00        862         15         14      9      9      6.97 TB    347.33 TB      20.42 M     16.97 G          0         10

   EOS Console [root://localhost] |/> group  ls --io
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #           name # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   default.0              0.00         0.00         0.00        952        217        199      0      0    338.31 GB     15.97 TB     952.65 k    780.14 M          0          0
   default.1              0.00         0.00         0.00        952        217        199      0      0    336.07 GB     15.97 TB     927.18 k    780.14 M          0          0
   default.10             0.00         0.00         0.00        952        217        199      0      0    332.23 GB     15.97 TB     926.45 k    780.14 M          0          0
   default.11             0.00         0.00         0.00        952        217        199      0      0    325.14 GB     15.97 TB     948.02 k    780.14 M          0          0
   default.12             0.00         0.00         0.00        833        180        179      0      0     22.39 GB     13.97 TB     898.40 k    682.62 M          0          0
   default.13             0.00         0.00         1.00        952        217        199      0      0    360.30 GB     15.97 TB     951.05 k    780.14 M          0          0
   default.14             0.99        96.00       206.00        952        217        199     31     30    330.45 GB     15.97 TB     956.50 k    780.14 M          0         36
   default.15             0.00         0.00         0.00        952        217        199      0      0    308.26 GB     15.97 TB     939.26 k    780.14 M          0          0
   default.16             0.00         0.00         0.00        833        188        184      0      0    327.76 GB     13.97 TB     899.97 k    682.62 M          0          0
   default.17             0.87       100.00       202.00        952        217        199     16     28    368.09 GB     15.97 TB     933.95 k    780.14 M          0         31
   default.18             0.00         0.00         0.00        952        217        199      0      0    364.27 GB     15.97 TB     953.94 k    780.14 M          0          0
   default.19             0.00         0.00         0.00        952        217        199      0      0    304.66 GB     15.97 TB     939.24 k    780.14 M          0          0
   default.2              0.00         0.00         0.00        952        217        199      0      0    333.64 GB     15.97 TB     920.26 k    780.14 M          0          0
   default.20             0.00         0.00         0.00        952        217        199      0      0    335.00 GB     15.97 TB     957.02 k    780.14 M          0          0
   default.21             0.00         0.00         0.00        952        217        199      0      0    335.18 GB     15.97 TB     921.75 k    780.14 M          0          0
   default.3              0.00         0.00         0.00        952        217        199      0      0    319.06 GB     15.97 TB     919.02 k    780.14 M          0          0
   default.4              0.00         0.00         0.00        952        217        199      0      0    320.18 GB     15.97 TB     826.62 k    780.14 M          0          0
   default.5              0.00         0.00         0.00        952        217        199      0      0    320.12 GB     15.97 TB     924.60 k    780.14 M          0          0
   default.6              0.00         0.00         0.00        952        217        199      0      0    333.56 GB     15.97 TB     920.32 k    780.14 M          0          0
   default.7              0.00         0.00         0.00        952        217        199      0      0    333.42 GB     15.97 TB     922.51 k    780.14 M          0          0
   default.8              0.00         0.00         0.00        952        217        199      0      0    335.67 GB     15.97 TB     925.39 k    780.14 M          0          0
   default.9              0.00         0.00         0.00        952        217        199      0      0    325.37 GB     15.97 TB     957.84 k    780.14 M          0          0
   test                   0.00         0.00         0.00          0          0          0      0      0       0.00 B       0.00 B         0.00        0.00          0          0

   EOS Console [root://localhost] |/> node  ls --io
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #               hostport # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   eosdevsrv1.cern.ch:1095       0.00         0.00         0.00          0          0          0      0      0       0.00 B       0.00 B         0.00        0.00          0          0
   lxfsra02a02.cern.ch:1095       0.10        19.00        55.00        119         37         20      7      8    935.18 GB     41.92 TB       2.54 M      2.05 G          0         10
   lxfsra02a05.cern.ch:1095       0.06         5.00        53.00        119         30          5      1     10    968.03 GB     43.92 TB       2.71 M      2.15 G          0         10
   lxfsra02a06.cern.ch:1095       0.05         0.00        50.00        119         16          0      0      6    872.91 GB     43.92 TB       2.84 M      2.15 G          0          6
   lxfsra02a07.cern.ch:1095       0.05        33.00        10.00        119         23         33      6      7    882.25 GB     43.92 TB       3.03 M      2.15 G          0          8
   lxfsra02a08.cern.ch:1095       0.09        41.00        56.00        119         45         42      9      9    947.68 GB     43.92 TB       2.78 M      2.15 G          0         10
   lxfsra04a01.cern.ch:1095       0.09        15.00       101.00        119         29         15      2      8    818.77 GB     41.92 TB       2.02 M      2.05 G          0         10
   lxfsra04a02.cern.ch:1095       0.09        27.00        83.00        119         37         27      2     10    837.91 GB     43.92 TB       2.30 M      2.15 G          0         10
   lxfsra04a03.cern.ch:1095       0.05        56.00         1.00        119          0         57     20      0    746.40 GB     43.92 TB       2.21 M      2.15 G          0          0

   EOS Console [root://localhost] |/> fs ls --io

   #.................................................................................................................................................................................................................
   #                     hostport #  id #     schedgroup # diskload # diskr-MB/s # diskw-MB/s #eth-MiB/s # ethi-MiB # etho-MiB #ropen #wopen # used-bytes #  max-bytes # used-files # max-files #  bal-run #drain-run
   #.................................................................................................................................................................................................................

   ...

   lxfsra04a02.cern.ch:1095   109       default.14       0.21         0.00        15.00        119         21          0      0      8     59.35 GB      2.00 TB     102.85 k     97.52 M          0          8

   ...



Drain Threads MGM
""""""""""""""""""

Each filesystem shown in the drain view in a non-final state has a thread on the
MGM associated to it.

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra02a05.cern.ch (1095)     20          /data20      prepare            0         0.00       0.00 B          24

A drain thread is steering the drain of each filesystem in non-final state and
is responsible of spawning drain processes directly on the MGM node. These logical
drain jobs use the GeoTreeEngine to select the destination file system are queued
in case the limits per node are reached. The drain parameters can be configured at
the space level:

.. code-block:: bash

   EOS Console [root://localhost] |/> space status default

   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ..

   drainer.node.nfs                 := 10
   drainer.fs.ntx                   := 10
   drainperiod                      := 3600
   graceperiod                      := 86400
   ..

By default max 5 file systems per node can be drained in parallel with max 5
parallel transfers per file system.

The values can be modified via:

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default space.drainer.node.nfs=20
   EOS Console [root://localhost] |/> space config default space.drainer.fs.ntx=50


Example Drain Process
"""""""""""""""""""""

We need to drain filesystem 20. However the file system is still fully operational
hence we use status drain.

.. code-block:: bash

   EOS Console [root://localhost] |/> fs config 20 configstatus=drain
   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra02a05.cern.ch (1095)     20          /data20      prepare            0         0.00       0.00 B          24

After 60 seconds a drain filesystem changes into state draining if the drain
mode was manually set. If a graceperiod is defined, it will stay in status
waiting for the length of the grace period.

In this example the defined drain period is 1 day:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra04a03.cern.ch (1095)    20           /data20     draining            5        75.00     37.29 GB       86269

   When the drain has successfully completed, the output looks like this:

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   lxfsra02a05.cern.ch (1095)     20          /data20      drained            0         0.00       0.00 B           0


If the drain can not complete you will see this after the drain period has passed:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs ls -d

   #......................................................................................................................
   #                   host (#...) #   id #           path #      drain #   progress #      files # bytes-left #  timeleft
   #......................................................................................................................
   l
   lxfsra04a03.cern.ch (1095)     20          /data20      expired           56        34.00     27.22 GB       86050

You can now investigate the origin by doing:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs status 20

   ...

   # ....................................................................................
   # Risk Analysis
   # ....................................................................................
   number of files                  :=         34 (100.00%)
   files healthy                    :=          0 (0.00%)
   files at risk                    :=          0 (0.00%)
   files inaccessbile               :=         34 (100.00%)
   # ------------------------------------------------------------------------------------

Here all remaining files are inaccessible because all replicas are down.

In case files are claimed to be accessible you have to look directoy at the remaining files:

.. code-block:: bash

   EOS Console [root://localhost] |/> fs dumpmd 20 -path
   path=/eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root
   path=/eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/2/8.root
   path=/eos/dev/2rep/sub12/lxplus406.cern.ch_4/0/1/0.root
   path=/eos/dev/2rep/sub12/lxplus403.cern.ch_43/0/2/8.root
   ...

Check these files using 'file check':

.. code-block:: bash

   EOS Console [root://localhost] |/> file check /eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root
   path="/eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root" fid="0002d989" size="291241984" nrep="2" checksumtype="adler" checksum="0473000100000000000000000000000000000000"
   nrep="00" fsid="20" host="lxfsra02a05.cern.ch:1095" fstpath="/data08/00000012/0002d989" size="291241984" checksum="0473000100000000000000000000000000000000"
   nrep="01" fsid="53" host="lxfsra04a01.cern.ch:1095" fstpath="/data09/00000012/0002d989" size="291241984" checksum="0000000000000000000000000000000000000000"

In this case the second replica didn't commit a checksum and cannot be read.

This you might fix like this:

.. code-block:: bash

   EOS Console [root://localhost] |/> file verify /eos/dev/2rep/sub12/lxplus403.cern.ch_10/0/0/7.root -checksum -commitchecksum


If you just want to force the remove of files remaining on a non-drained filesystem,
you can drop all files on a particular filesystem using **eos fs dropfiles**.
If you use the '-f' flag all references to these files will be removed immediately
and EOS won't try to delete any file anymore.

.. code-block:: console

   EOS Console [root://localhost] |/> fs dropfiles 170 -f
   Do you really want to delete ALL 24 replica's from filesystem 170 ?
   Confirm the deletion by typing => 1434841745
   => 1434841745

   Deletion confirmed

.. index::
   pair: Draining; Group Draining

Group Drainer
^^^^^^^^^^^^^

The group drainer uses the converter mechanism to drain files from groups to target groups.
Failed transfers are retried a configurable number of times before finally reaching either a
drained or drainfail status for a group. It uses an architecture similar to GroupBalancer with a
special Drainer Engine which only looks for groups marked as *drain* as source groups. The target
groups are by default chosen as a threshold below the total group fillness average. Similar to
converter and groupbalancer this is enabled/disabled at a space level.

.. index::  
   pair: Group Drainer; Configuration


Configuration
"""""""""""""

.. code-block:: bash

   # enable/disable
   eos space config space.groupbalancer = <on/off>

   # force a group to drain
   eos group set <groupname> drain



   # The list of various configuration flags supported in the eos cli
   space config <space-name> space.groupdrainer=on|off                   : enable/disable the group drainer [ default=on ]
   space config <space-name> space.groupdrainer.threshold=<threshold>    : configure the threshold(%) for picking target groups
   space config <space-name> space.groupdrainer.group_refresh_interval   : configure time in seconds for refreshing cached groups info [default=300]
   space config <space-name> space.groupdrainer.retry_interval           : configure time in seconds for retrying failed drains [default=4*3600]
   space config <space-name> space.groupdrainer.retry_count              : configure the amount of retries for failed drains [default=5]
   space config <space-name> space.groupdrainer.ntx                      : configure the max file transfer queue size [default=10000]


The `threshold` param by default is a percent threshold below the total computed average of all group fillness. If you want to ignore this and target
every available group, then threshold=0 will do that.
The `group refresh interval` determines how often we refresh the list of groups in the system, since this is not expected to change that often by
default we only do it every 5 minutes (or when any groupdrainer config sees a change)
The `ntx` is the maximum amount of transfers we keep as active, it is okay to set this value higher than converter's ntx so that a healthy queue is maintained
and the converter is kept busy. However if you want to reduce throughput, reducing the ntx will essentially throttle the files we schedule for transfers
The `retry_interval` and `retry_count` determine the amount of retries we do for a failed transfer. By default we try upto 5 times before giving up and
eventually marking the FS as drainfailed. This will need manual intervention similar to handling regular FS drains.

.. index::  
   pair: Group Drainer; Info

Status
"""""""

Currently a very minimal status command is implemented, which only informs about
the total transfers in queue and failed being tracked currently, in addition to
the count of groups in drain state and target groups. This is expected to change
in the future with more information about the progress of the drain.

This command can be accessed via

.. code-block:: bash

   eos space groupdrainer status <spacename>


Recommendations
"""""""""""""""

It is recommended not to drain FS individually within the groups that are marked as in drain state
as the groupdrainer may target the same files targeted by the regular drainer and similarly they
may compete on drain complete statuses.

GroupBalancer only targets groups that are not in drain state, so in groups in drain state will not
be picked as either source or target groups by the GroupBalancer. However if no threshold is configured
then we might end up in scenarios where a file is being targeted by GroupDrainer to a group that is
relatively full eventually forcing the GroupBalancer to also balance. To avoid this it is recommended to
set the threshold so that only groups below average are targeted by GroupDrainer.


Completion
"""""""""""

In a groupdrain scenario:
An individual FS is marked as either drained/drainfailed
- When all the files in the FS are converted ie. transferred to other groups (`drained`)
- There are some files which even after `retry_count` attempts were failing transfer (`drainfailed`)


A groupdrain is marked as complete when all the FSes in a group are in drained or drainfailed mode.
In this scenario the group status is set as `drained` or `drainfailed`, which should be visible in the
`eos group ls` command.

.. index::
   pair: MGM; Inspector


File Inspector
--------------

The File Inspector is a slow agent scanning all files in a namespace and collects statistics per layout type. Additionally it adds statistic about replication inconsistencies per layout. The target interval to scan all files is user defined. The default cycle is 4 hours, which can create a too high load in large namespaces and should be adjusted accordingly.

.. index::  
   pair: Inspector; Configuration

Configuration
^^^^^^^^^^^^^

File Inspector
"""""""""""""""
The File Inspector has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.inspector=on  
   # disable
   eos space config default space.inspector=off

By default Replication Tracking is disabled.

The current status of the Tracker can be seen via:

.. code-block:: bash

   eos space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   inspector                        := off
   ...


Inspector Interval
"""""""""""""""""""

The default inspector interval to scan all files is 4 hours. The interval can be set using:

.. code-block:: bash

   # set interval to 1d
   eos space config default space.inspector.interval=86400


.. index::  
   pair: Inspector; Info


Inspector Status
^^^^^^^^^^^^^^^^

You can get the inspector status and an estimate for the run time using

.. code-block:: bash

   eos space inspector

   # or 

   eos inspector

   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:38:24Z
   # 28 % done - estimate to finish: 2575 seconds
   # ------------------------------------------------------------------------------------

Inspector Output
^^^^^^^^^^^^^^^^

You can see the current statistics of the inspector run using

.. code-block:: bash

   eos inspector -c 
   eos inspector --current

   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:39:55Z
   # 28 % done - estimate to finish: 2574 seconds
   # current scan: 2019-07-12T08:25:42Z
    not-found-during-scan            : 0
   ======================================================================================
   layout=00000000 type=plain         checksum=none     blockchecksum=none     blocksize=4k  

   locations                        : 0
   nolocation                       : 223004
   repdelta:-1                      : 223004
   unlinkedlocations                : 0
   zerosize                         : 223004
   
   ======================================================================================
   layout=00100001 type=plain         checksum=none     blockchecksum=none     blocksize=4k  

   locations                        : 2
   repdelta:0                       : 2
   unlinkedlocations                : 0
   volume                           : 3484
  
   ...


The reports tags are:

.. code-block:: bash 

   locations         : number of replicas (or stripes) in this layout categorie
   nolocation        : number of files without any location attached
   repdelta:-N       : number of files with -N replicas missing
   repdelta:0        : number of files with correct replicat count
   repdelate:+N      : number of files with +N replicas in excess
   zerosize          : number of files with 0 size
   volume            : logical bytes stored in this layout type
   unlinkedlocations : number replicas still to be deleted
   shadowdeletions   : number of files with a replica pointing to a not configured filesystem for deletion
   shodowlocation    : number of files with a replica pointing to a not configured filesystem

.. index::  
   pair: Inspector; Statistics
   pair: Inspector; Access Time Distribution
   pair: Inspector; Birth Time Distribution

You can get the statistics of the last completed run using

.. code-block:: bash

   eos inspector -l
   eos inspector --last

This will additionally include birth and access time distributions:

.. code-block:: bash

    eos inspector -l
    ...
    ======================================================================================
     Access time distribution of files
     0s                               : 1613 (1.59%)
     24h                              : 6 (0.01%)
     7d                               : 1 (0.00%)
     30d                              : 1 (0.00%)
     2y                               : 5 (0.00%)
     5y                               : 100.02 k (98.40%)
    ======================================================================================
     Access time volume distribution of files
     0s                               : 81.31 MB (98.73%)
     24h                              : 15.09 kB (0.02%)
     7d                               : 0 B (0.00%)
     30d                              : 1.00 MB (1.21%)
     2y                               : 10.49 kB (0.01%)
     5y                               : 24.27 kB (0.03%)
    ======================================================================================
     Birth time distribution of files
     0s                               : 1619 (1.59%)
     24h                              : 6 (0.01%)
     7d                               : 100.00 k (98.39%)
     90d                              : 1 (0.00%)
     5y                               : 13 (0.01%)
    ======================================================================================
     Birth time volume distribution of files
     0s                               : 81.32 MB (98.74%)
     24h                              : 1.01 MB (1.23%)
     7d                               : 25 B (0.00%)
     90d                              : 2769 B (0.00%)
     5y                               : 21.48 kB (0.03%)
    --------------------------------------------------------------------------------------

To get access time distributions you have to have the access time tracking enabled in the space configuration:
e.g. with 1h resolution: ``eos space config default atime=3600``
   
You can print the current and last run statistics in monitoring format:

.. code-block:: bash

   eos inspector -c -m 
   ...

   eos inspector -l -m 

   key=last layout=00100002 type=plain checksum=adler32 blockchecksum=none blocksize=4k locations=638871 repdelta:+1=1 repdelta:0=638869 unlinkedlocations=0 volume=10802198338 zerosize=550002
   key=last layout=00100012 type=replica checksum=adler32 blockchecksum=none blocksize=4k locations=42 repdelta:0=42 unlinkedlocations=0 volume=21008942
   key=last layout=00100014 type=replica checksum=md5 blockchecksum=none blocksize=4k locations=1 repdelta:0=1 unlinkedlocations=0 volume=1701
   key=last layout=00100015 type=replica checksum=sha1 blockchecksum=none blocksize=4k locations=1 repdelta:0=1 unlinkedlocations=0 volume=1701
   key=last layout=00100112 type=replica checksum=adler32 blockchecksum=none blocksize=4k locations=44 repdelta:0=22 unlinkedlocations=0 volume=10506283
   key=last layout=00640112 type=replica checksum=adler32 blockchecksum=none blocksize=1M locations=2 repdelta:0=1 unlinkedlocations=0 volume=1783
   key=last layout=20640342 type=raid6 checksum=adler32 blockchecksum=crc32c blocksize=1M locations=0 nolocation=6 repdelta:-4=6 unlinkedlocations=0 zerosize=6
   key=last layout=3b9ac9ff type=none checksum=none blockchecksum=none blocksize=illegal unfound=0
   kay=last tag=accesstime::files 0=1613 86400=6 604800=1 2592000=1 63072000=5 157680000=100015
   key=last tag=accesstime::volume 0=81309191 86400=15090 604800=0 2592000=1000000 63072000=10495 157680000=24274
   kay=last tag=birthtime::files 0=1619 86400=6 604800=100002 7776000=1 157680000=13

The list of file ids with an inconsistency can be extracted using:

.. code-block:: bash

   # print the list of file ids
   eos inspector -c -p #current run

   fxid:00140237 repdelta:-1
   fxid:001410ff repdelta:-1
   fxid:00141807 repdelta:-1
   fxid:0013da42 repdelta:-4
   fxid:0013da43 repdelta:-4
   fxid:0013da44 repdelta:-4
   fxid:0013da45 repdelta:-4
   fxid:0013da57 repdelta:-4
   fxid:0013da68 repdelta:-4
   ...


   eos inspector -l -p #last run
   ...

   # export the list of file ids on the mgm
   eos inspector -c -e #current run
   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:53:14Z
   # 100 % done - estimate to finish: 0 seconds
   # file list exported on MGM to '/var/log/eos/mgm/FileInspector.1562921594.list'
   # ------------------------------------------------------------------------------------

   eos inspector -l -e #last run
   # ------------------------------------------------------------------------------------
   # 2019-07-12T08:53:33Z
   # 100 % done - estimate to finish: 0 seconds
   # file list exported on MGM to '/var/log/eos/mgm/FileInspector.1562921613.list'
   # -----------------------------------------------------------------------   


Log Files
^^^^^^^^^
The File Inspector has a dedicated log file under ``/var/log/eos/mgm/FileInspector.log``
which shows the scan activity and potential errors. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info

.. index::
   pair: MGM; LRU 

LRU Engine
----------

The LRU system serves to apply various conversion or deletion policies. It scans in a defined interval the full directory hierarchy and applies
the following LRU policies:

.. epigraph::

   ===================================================================================== =====================
   Policy                                                                                Basis
   ===================================================================================== =====================
   Volume based LRU cache with low and high watermark                                    volume/threshold/time
   Automatic time based cleanup of empty directories                                     ctime
   Time based LRU cache with expiration time settings                                    ctime
   Automatic time based layout conversion if a file reaches a defined age                ctime
   Automatic size based layout conversion if a file fullfills a given size rule          size
   Automatic time based layout conversion if a file has not been used for specified time mtime
   ===================================================================================== =====================

.. index::  
   pair: LRU; Configuration
   pair: LRU; Engine

Configuration
^^^^^^^^^^^^^

Engine
"""""""
The LRU engine has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.lru=on
   # disable
   eos space config default space.lru=off

The current status of the LRU can be seen via:

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   lru                            := off
   lru.interval                   := 0
   ...

The interval in which the LRU engine is running is defined by the **lru.interval**
space variable:

.. code-block:: bash

   # run the LRU scan once a week
   eos space config default space.lru.interval=604800

.. index::  
   pair: LRU; Policy

Policy
~~~~~~

Volume based LRU cache with low and high watermark
``````````````````````````````````````````````````
To configure an LRU cache with low and high watermark it is necessary to define
a quota node on the cache directory, set the high and low watermarks and to enable
the **atime** feature updating the creation times of files with the current
access time.

When the cache reaches the high watermark it cleans the oldest files untile low-watermark is reached:

.. code-block:: bash

   # define project quota on the cache directory
   eps quota set -g 99 -v 1T /eos/instance/cache/

   # define 90 as low and 95 as high watermark
   eos attr set sys.lru.watermark=90:95  /eos/instance/cache/

   # track atime with a time resolution of 5 minutes (space configuration parameter)
   eos space config default space.atime=300

.. index::  
   pair: LRU; Clean Empty Directories


Automatic time based cleanup of empty directories
`````````````````````````````````````````````````
Configure automatic clean-up of empty directories which have a minimal age.
The LRU scan deletes directories with the largest deepness first to be able
to remove complete empty subtrees in the namespace.

.. code-block:: bash

   # remove automatically empty directories if they are older than 1 hour
   eos attr set sys.lru.expire.empty="1h" /eos/dev/instance/empty/


Time based LRU cache with expiration time settings
``````````````````````````````````````````````````
This policy allows to match files by name with a defined age to be deleted. We
use the following convention when specifying the age interval for the various
"match" options:

 +---------------+---------------+
 | Symbol        | Meaning       |
 +===============+===============+
 | **s/S**       | seconds       |
 +---------------+---------------+
 | **min/MIN**   | minutes       |
 +---------------+---------------+
 | **h/H**       | hours         |
 +---------------+---------------+
 | **d/D**       | days          |
 +---------------+---------------+
 | **w/W**       | weeks         |
 +---------------+---------------+
 | **mo/MO**     | months        |
 +---------------+---------------+
 | **y/Y**       | years         |
 +---------------+---------------+

All the size related symbols refer to the International System of Units, therfore
1K is 1000 bytes.

.. code-block:: bash

   # files with suffix *.root get removed after a month, files with *.tgz after one week
   eos attr set sys.lru.expire.match="*.root:1mo,*.tgz:1w"  /eos/dev/instance/scratch/

   # all files older than a day are automatically removed
   eos attr set sys.lru.expire.match="*:1d" /eos/dev/instance/scratch/

Automatic time based layout conversion if a file reaches a defined age
``````````````````````````````````````````````````````````````````````
This policy allows to convert a file from the current layout into a defined layout.
A *placement policy* can also be specified.

.. code-block:: bash

   # convert all files older than a month to the layout defined next
   eos attr set sys.lru.convert.match="*:1mo" /eos/dev/instance/convert/

   # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2
   eos attr set sys.conversion.*=20640542 /eos/dev/instance/convert/

   # same thing specifying a placement policy for the replicas/stripes
   eos attr set sys.conversion.*=20640542|gathered:site1::rack2 /eos/dev/instance/convert/

The hex layout ID contains also the checksum and blocksize settings. The best is
to create a file with the desired layout and get the hex layout ID using
**eos file info <path>**.

Automatic size based restriction for time based conversion
``````````````````````````````````````````````````````````
This policy addition allows to restrict the time based layout conversion to certain
file sizes.

.. code-block:: bash

   # convert all files smaller than 128m in size [ with units E/e,P/p,T/t,G/g,M/m,K/k ]
   eos attr set sys.lru.convert.match="*:1w:<1M"

   # convert all files bigger than 1G in size
   eos attr set sys.lru.convert.match="*:1w:>1G"


Automatic time based layout conversion if a file has not been used for specified time
``````````````````````````````````````````````````````````````````````````````````````
This policy allows to convert a file from the current layout to a different layout
if the file was not accessed for a defined interval. To use this feature one has
also to enable the **atime** feature where the access time is stored as the new
file creation time. A *placement policy* can also be specified.

.. code-block:: bash

     # track atime with a time resolution of one week ( space configuration parameter )
     eos space config default space.atime=604800

     # convert all files older than a month to the layout defined next
     eos attr set sys.lru.convert.match="*:6mo" /eos/dev/instance/convert/

     # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2
     eos attr set sys.conversion.*=20640542 /eos/dev/instance/convert/

     # same thing specifying a placement policy for the replicas/stripes
     eos> attr set sys.conversion.*=20640542|gathered:site1::rack2 /eos/dev/instance/convert/

.. index::  
   pair: File; Conversion


Manual File Conversion
^^^^^^^^^^^^^^^^^^^^^^
It is possible to run an asynchronous file conversion using the **EOS CLI**.

.. code-block:: bash

   # convert the referenced file into a file with 3 replica
   eos file convert /eos/dev/2rep/passwd replica:3
   info: conversion based layout+stripe arguments
   success: created conversion job '/eos/dev/proc/conversion/0000000000059b10:default#00650212'

   # same thing mentioning target space and placement policy
   eos file convert /eos/dev/2rep/passwd replica:3 default gathered:site1::rack1
   info: conversion based layout+stripe arguments
   success: created conversion job '/eos/dev/proc/conversion/0000000000059b10:default#00650212'~gathered:site1::rack1

.. code-block:: bash

   # convert the referenced file into a RAID6 file with 6 stripes
   eos file convert /eos/dev/2rep/passwd raid6:6
   info: conversion based layout+stripe arguments
   success: created conversion job '/eos/dev/proc/conversion/0000000000064f61:default#20650542'

   # check that the conversion was successful
   eos fileinfo /eos/dev/2rep/passwd
   File: '/eos/dev/2rep/passwd'  Size: 2458
   Modify: Wed Oct 30 17:03:35 2013 Timestamp: 1383149015.384602000
   Change: Wed Oct 30 17:03:36 2013 Timestamp: 1383149016.243563000
     CUid: 0 CGid: 0  Fxid: 00064f63 Fid: 413539    Pid: 1864   Pxid: 00000748
   XStype: adler    XS: 01 15 4b 52
   raid6 Stripes: 6 Blocksize: 4M LayoutId: 20650542
     #Rep: 6
   <#> <fs-id> #.................................................................................................................
               #               host  #    schedgroup #      path #    boot # configstatus #    drain # active #         geotag #
               #.................................................................................................................
     0     102     lxfsra04a03.cern.ch      default.11     /data12    booted             rw    nodrain   online   eos::cern::mgm
     1     116     lxfsra02a05.cern.ch      default.11     /data12    booted             rw    nodrain   online   eos::cern::mgm
     2      94     lxfsra04a02.cern.ch      default.11     /data12    booted             rw    nodrain   online   eos::cern::mgm
     3      65     lxfsra02a07.cern.ch      default.11     /data12    booted             rw    nodrain   online   eos::cern::mgm
     4     108     lxfsra02a08.cern.ch      default.11     /data12    booted             rw    nodrain   online   eos::cern::mgm
     5      77     lxfsra04a01.cern.ch      default.11     /data13    booted             rw    nodrain   online   eos::cern::mgm
   *******

.. index::  
   pair: LRU; Log Files

Log Files
^^^^^^^^^
The LRU engine has a dedicated log file under ``/var/log/eos/mgm/LRU.log``
which shows triggered actions based on scanned policies. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info


.. index::
   pair: MGM; FSCK 
   pair: MGM; Consistency 

FSCK
-----

FSCK (File System Consistency Check) is the service reporting and possibly repairing inconsistencies in an EOS instance.

This section describles how the internal file system consistency checks (FSCK) are configured and work.

.. index::  
   pair: FSCK; FST Scan


Enable FST Scan
^^^^^^^^^^^^^^^

To enable the FST scan you have to set the variable **scaninterval** on the space and
on all file systems:

.. code-block:: bash

   # set it on the space to inherit a value for all new filesystems in this space every 14 days (time has to be in seconds)
   space config default space.scaninterval=1209600

   # set it on an existing filesystem (fsid 23) to 14 days (time has to be in seconds)
   fs config 23 space.scaninterval=1209600

   # set the scaninterval for all the existing file systems already registered in the given space
   space config default fs.scaninterval=1209601

.. note::

   The *scaninterval* time has to be given in seconds!


Caveats
^^^^^^^

For FSCK engine to function correctly, FSTs must be able to connect to QuarkDB directly (and to the MGM).


Overview
^^^^^^^^

High level summary
^^^^^^^^^^^^^^^^^^

#) error collection happens in the FST in defined intervals, no action/trigger by MGM is required for this

#) the locally saved results will be collected by the fsck collection thread of fsck engine

#) if the fsck repair thread is  enabled, the mgm will trigger repair actions (i.e. create / delete replica)
as required (based on collected error data)

Intervals and config parameters for file systems(FS)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These values are set as global defaults on the space. A file system should get the values from the space when it is newly created.
Below you can find a brief description of the parameters influencing the scanning procedure.

===================  ===============   ===========================================================
Name                 Default           Description
===================  ===============   ===========================================================
scan_disk_interval   14400 [s] (4h)    interval at which files in the FS should be scanned, by the FST itself
scan_ns_interval     259200 [s] (3d)   interval at which files in the FS are compares against the
                                       namespace information from QuarkDB
scaninterval         604800 [s] (7d)   target interval at which all files should be scanned
scan_rain_interval   2419200 [s] (4w)  target interval at which all rain files should be scanned
scan_ns_rate         50 [Hz]           rate limit the requests to QuarkDB for the namespace scans
scanrate             100 [MB/s]        rate limit bandwidth used by the scanner when reading files
                                       from disk
===================  ===============   ===========================================================

**scan_disk_interval** and **scan_ns_interval** are skewed by a random factor per FS so that not all disks become busy at the same time.

The scan jobs are started with a lower IO priority class (using Linux ioprio_set) within EOS to decrease the impact on normal filesystem access, i.e. check logs for set io priority to 7 (lowest best-effort).

.. code-block:: bash

   210211 12:41:40 time=1613043700.017295 func=RunDiskScan              level=NOTE
   logid=1af8cd9e-6c5e-11eb-ae37-3868dd2a6fb0 unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f98bebff700 source=ScanDir:446
   tident=<service> sec=   uid=0 gid=0 name= geo="" msg="set io priority to 7(lowest best-effort)" pid=221712


Scan Duration
^^^^^^^^^^^^^

The first scan of a larger (fuller) FS can take several hours. Following scans will be much faster, within minutes (10-30min).
Subsequent scans will only look at file that have not been scanned since scaninterval . i.e. each scan iteration will only look at a fraction of the files on disk, compare the logs for such a scan. (see the last line “scannedfiles” vs “skippedfiles” and the scanduration of 293s.)

.. code-block:: bash

   210211 12:49:44 time=1613044184.957472 func=RunDiskScan              level=NOTE  logid=1827f5ea-6c5e-11eb-ae37-3868dd2a6fb0    unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f993afff700 source=ScanDir:504                    tident=<service> sec=      uid=0 gid=0 name= geo="" [ScanDir] Directory: /srv/data/data.01 files=147957 scanduration=293 [s] scansize=23732973568 [Bytes] [ 23733 MB ] scannedfiles=391 corruptedfiles=0 hwcorrupted=0 skippedfiles=147557

.. index::  
   pair: FSCK; Error Types

Error Types detected by FSCK
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

(in decreasing priority)

=============  ====================================================  ================================================================================================================
Error          Description                                           Fixed by
=============  ====================================================  ================================================================================================================
stripe_err     stripe is unable to reconstruct original file         FsckRepairJob
d_mem_sz_diff  disk and reference size mismatch                      FsckRepairJob
m_mem_sz_diff  MGM and reference size mismatch                       inspecting all the replicas or saved for manual inspection
d_cx_diff      disk and reference checksum mismatch                  FsckRepairJob
m_cx_diff      MGM and reference checksum mismatch                   inspecting all the replicas or saved for manual inspection
unreg_n        unregistered file / replica                           (i.e. file on FS that has no entry in MGM) register replica if metadata match or drop if not needed
rep_missing_n  missing replica for a file                            replica is registered on mgm but not on disk - FsckRepairJob
rep_diff_n     replica count is not nominal (too high or too low)    fixed by dropping replicas or creating new ones through FsckRepairJob
orphans_n      orphan files (no record for replica/file in mgm)      no action at the MGM, files not referenced by MGM at all, moved to to .eosorphans directory on FS mountpoint
=============  ====================================================  ================================================================================================================

.. index::  
   pair: FSCK; Configuration

Configuration
^^^^^^^^^^^^^

Space
"""""

Some config items on the space are global, some are defaults (i.e. for newly created filesystems), see https://eos-docs.web.cern.ch/configuration/autorepair.html

To enable the FST scan you have to set the variable **scaninterval** on the space and on all file systems.

The intervals other than `scaninterval` are defaults for newly created filesystems. For an explanation. of the intervals see above.


.. code-block:: bash

   [root@mgm-1 ~]# eos space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   autorepair                       := on
   [...]
   scan_disk_interval               := 14400
   scan_ns_interval                 := 259200
   scan_ns_rate                     := 50
   scaninterval                     := 604800
   scan_rain_interval               := 2419200
   scanrate                         := 100
   [...]



Filesystem(FS)
"""""""""""""""

To enable the FST scan you have to set the variable `scaninterval` on the space and on all file systems

.. code-block:: bash

   [root@mgm-1 ~]# eos fs status 1
   # ------------------------------------------------------------------------------------
   # FileSystem Variables
   # ------------------------------------------------------------------------------------
   bootcheck                        := 0
   bootsenttime                     := 1612456466
   configstatus                     := rw
   host                             := fst-1.eos.grid.vbc.ac.at
   hostport                         := fst-1.eos.grid.vbc.ac.at:1095
   id                               := 1
   local.drain                      := nodrain
   path                             := /srv/data/data.00
   port                             := 1095
   queue                            := /eos/fst-1.eos.grid.vbc.ac.at:1095/fst
   queuepath                        := /eos/fst-1.eos.grid.vbc.ac.at:1095/fst/srv/data/data.00

   [...] defaults for these are taken from MGM, scanterval must be set!
   scan_disk_interval               := 14400
   scan_ns_interval                 := 259200
   scan_ns_rate                     := 50
   scaninterval                     := 604800
   scan_rain_interval               := 2419200
   scanrate                         := 100

   [...] various stat values reported back by the FST
   stat.fsck.blockxs_err            := 1
   stat.fsck.d_cx_diff              := 0
   stat.fsck.d_mem_sz_diff          := 0
   stat.fsck.d_sync_n               := 148520
   stat.fsck.m_cx_diff              := 0
   stat.fsck.m_mem_sz_diff          := 0
   stat.fsck.m_sync_n               := 148025
   stat.fsck.mem_n                  := 148526
   stat.fsck.orphans_n              := 497
   stat.fsck.rep_diff_n             := 5006
   stat.fsck.rep_missing_n          := 0
   stat.fsck.unreg_n                := 5003
   [...]


FSCK Settings
"""""""""""""""

With the settings above, stats are collected on the FST (and reported in fs status) but no further action is taken. To setup of the fsck mechanism, see the eos fsck subcommands:

`fsck stat`
"""""""""""

Gives a quick status of error stats collection and if the repair thread is active. The `eos fsck toggle-repair` and `toggle-collect` are really toggles. Use **eos fsck stat** to verify the correctness of your settings!

.. code-block:: bash

   [root@mgm-1 ~]# eos fsck stat
   Info: collection thread status -> enabled
   Info: repair thread status     -> enabled
   210211 15:54:09 1613055249.712603 Start error collection
   210211 15:54:09 1613055249.712635 Filesystems to check: 252
   210211 15:54:10 1613055250.769177 blockxs_err                    : 118
   210211 15:54:10 1613055250.769208 orphans_n                      : 92906
   210211 15:54:10 1613055250.769221 rep_diff_n                     : 1226274
   210211 15:54:10 1613055250.769224 rep_missing_n                  : 6
   210211 15:54:10 1613055250.769231 unreg_n                        : 1221521
   210211 15:54:10 1613055250.769235 Finished error collection
   210211 15:54:10 1613055250.769237 Next run in 30 minutes

The collection thread will interrogate the FSTs for locally collected error stats at configured intervals (default: 30 minutes).

`fsck report`
"""""""""""""

For a more comprehensive error report, use **eos fsck report** this will only contain data once the error collection has started (also note the switch -a to show errors per filesystem FS)

.. code-block:: bash

   [root@mgm-1 ~]# eos fsck report
   timestamp=1613055250 tag="blockxs_err" count=43
   timestamp=1613055250 tag="orphans_n" count=29399
   timestamp=1613055250 tag="rep_diff_n" count=181913
   timestamp=1613055250 tag="rep_missing_n" count=4
   timestamp=1613055250 tag="unreg_n" count=180971


.. index::  
   pair: FSCK; Repair

Repair
^^^^^^

Most of the repair operations are implemented using the DrainTransferJob functionality.

Operations
^^^^^^^^^^

Inspect FST local Error Statistics
""""""""""""""""""""""""""""""""""

Use **eos-leveldb-inspect** command to inspect the contents of the local database on the FSTs.
The local database contains all information (fxid, error type, etc) that will be collected
by the mgm (compare the eos fs status <fsid> output).

.. code-block:: bash

   [root@fst-9 ~]# eos-leveldb-inspect  --dbpath /var/eos/md/fmd.0225.LevelDB --fsck
   Num. entries in DB[mem_n]:                     148152
   Num. files synced from disk[d_sync_n]:         148150
   Num, files synced from MGM[m_sync_n]:          147723
   Disk/referece size missmatch[d_mem_sz_diff]:   0
   MGM/reference size missmatch[m_mem_sz_diff]:   140065
   Disk/reference checksum missmatch[d_cx_diff]:  0
   MGM/reference checksum missmatch[m_cx_diff]:   0
   Num. of orphans[orphans_n]:                    427
   Num. of unregistered replicas[unreg_n]:        5078
   Files with num. replica missmatch[rep_diff_n]: 5081
   Files missing on disk[rep_missing_n]:          0

Check fsck repair activity
""""""""""""""""""""""""""

See if the fsck repair thread is active and how log its work queue is (cross check with log activity on mgm):

.. code-block:: bash

   [root@mgm-1 ~]# eos ns | grep fsck
   ALL      fsck info                        thread_pool=fsck min=2 max=20 size=20 queue_size=562
   ALL      tracker info                     tracker=fsck size=582
   compare namespace stats for total count of fsck operations:


   [root@mgm-1 ~]# eos ns stat | grep -i fsck
   ALL      fsck info                        thread_pool=fsck min=2 max=20 size=20 queue_size=168
   ALL      tracker info                     tracker=fsck size=188
   all FsckRepairFailed              71.58 K     0.00     0.03     1.35     0.87     -NA-      -NA-
   all FsckRepairStarted             63.19 M   857.75  1107.25  1112.05   918.32     -NA-      -NA-
   all FsckRepairSuccessful          63.12 M   857.75  1106.88  1110.64   917.44     -NA-      -NA-

Log examples
"""""""""""""

Startup of FST service and initializing fsck threads:


.. code-block:: bash

    210211 12:41:39 time=1613043699.997897 func=ConfigScanner level=INFO  logid=1af5b7a8-6c5e-11eb-ae37-3868dd2a6fb0
    unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f99497ff700 source=FileSystem:159 tident=<service> sec= uid=0 gid=0
    name= geo="" msg="started ScanDir thread with default parameters" fsid=238

   # NS scanner thread with random skew
   210211 12:41:50 time=1613043710.000322 func=RunNsScan  level=INFO  logid=1af62382-6c5e-11eb-ae37-3868dd2a6fb0
   unit=fst@fst-9.eos.grid.vbc.ac.at:1095 tid=00007f98e6bfe700 source=ScanDir:224 tident=<service> sec= uid=0 gid=0
   name= geo="" msg="delay ns scan thread by 38889 seconds" fsid=239 dirpath="/srv/data/data.14"


systemd ScanDir results
~~~~~~~~~~~~~~~~~~~~~~~

These logs are also written to /var/log/eos/fst/xrdlog.fst

.. code-block:: bash

   Feb 11 12:41:33 fst-9.eos.grid.vbc.ac.at eos_start.sh[220738]: Using xrootd binary: /opt/eos/xrootd/bin/xrootd
   Feb 11 12:49:44 fst-9.eos.grid.vbc.ac.at scandir[220738]: skipping scan w-open file: localpath=/srv/data/data.01/000006e3/010d045d fsid=226 fxid=010d045d
   Feb 11 12:49:44 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.01 files=147957 scanduration=293 [s] scansize=23732973568 [Bytes] [ 23733 MB ] scanned...iles=147557
   Feb 11 13:07:55 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.18 files=148074 scanduration=263 [s] scansize=17977114624 [Bytes] [ 17977.1 MB ] scann...iles=147730
   Feb 11 13:08:36 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.22 files=147905 scanduration=258 [s] scansize=19978055680 [Bytes] [ 19978.1 MB ] scann...iles=147498
   Feb 11 13:14:56 fst-9.eos.grid.vbc.ac.at scandir[220738]: [ScanDir] Directory: /srv/data/data.27 files=147445 scanduration=249 [s] scansize=15998377984 [Bytes] [ 15998.4 MB ] scann...iles=147119
   fsck repairs. success/failure on MGM

   210211 13:58:17 time=1613048297.294157 func=RepairReplicaInconsistencies level=INFO  logid=cf14c90e-6c68-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd53bff700 source=FsckEntry:689                  tident=<service> sec=      uid=0 gid=0 name= geo="" msg="file replicas consistent" fxid=0028819b
   210211 13:58:17 time=1613048297.294294 func=RepairReplicaInconsistencies level=INFO  logid=cf14c54e-6c68-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd51bfb700 source=FsckEntry:689                  tident=<service> sec=      uid=0 gid=0 name= geo="" msg="file replicas consistent" fxid=00ef5955
   210211 13:59:18 time=1613048358.345753 func=RepairReplicaInconsistencies level=ERROR logid=cf14c7ce-6c68-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd523fc700 source=FsckEntry:663                  tident=<service> sec=      uid=0 gid=0 name= geo="" msg="replica inconsistency repair failed" fxid=0079b4d0 src_fsid=244


No repair action, file is being deleted
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The file has an FsckEntry i.e. is marked from repair, and was previously listed on the collected errors, but

.. code-block:: bash

   210211 16:27:45 time=1613057265.418302 func=Repair                   level=INFO  logid=b077de7c-6c7d-11eb-becb-3868dd28d0c0 unit=mgm@mgm-1.eos.grid.vbc.ac.at:1094 tid=00007efd95bff700 source=FsckEntry:773                  tident=<service> sec=      uid=0 gid=0 name= geo=""
   msg="no repair action, file is being deleted" fxid=00033673
   The file is noted as “being deleted” as its container (directory) does not exist anymore, i.e.


   [root@mgm-1 ~]# eos fileinfo fxid:00033673
   File: 'fxid:00033673'  Flags: 0600  Clock: 1662bb7c74f01d9f
   Size: 0
   Modify: Fri Jul 24 11:32:15 2020 Timestamp: 1595583135.037235673
   Change: Fri Jul 24 11:32:15 2020 Timestamp: 1595583135.037235673
   Birth: Fri Jul 24 11:32:15 2020 Timestamp: 1595583135.037235673
   CUid: 12111 CGid: 11788 Fxid: 00033673 Fid: 210547 Pid: 0 Pxid: 00000000
   XStype: adler    XS: 00 00 00 00    ETAGs: "56518279954432:00000000"
   Layout: raid6 Stripes: 7 Blocksize: 1M LayoutId: 20640642 Redundancy: d0::t0
   #Rep: 0
   *******
   error: cannot retrieve file meta data - Container #0 not found (errc=0) (Success)


Discrepancy reported errors
^^^^^^^^^^^^^^^^^^^^^^^^^^^

... between fsck report summary / per filesystem and fsck stat.
EOS fsck report is giving different numbers for total report and per filesystem summary. This is expected.

Per filesystem reports may contain error counts for individual replicas of a single file stored in EOS.
**eos fsck stat** will reflect the per replica count, **eos fsck report** will show lower numbers,
not counting per each replica of a file.

**example script**

.. code-block:: bash

   echo "summed up by filesystem"
   ERR_TYPES="blockxs_err orphans_n rep_diff_n rep_missing_n unreg_n"
   for ETYPE in $ERR_TYPES; do
   echo -n "$ETYPE: "
   eos fsck report -a | grep $ETYPE  | awk '{print $4;}' | awk 'BEGIN{ FS="="; total=0}; { total=total+$2; } END{print total;}'
   done

   echo ""

   echo "eos fsck summary report"
   eos fsck report

**output example**

.. code-block:: bash

   [root@mgm-1 ~]# ./eos_fsck_miscount.sh
   summed up by filesystem
   blockxs_err: 115
   orphans_n: 95056
   rep_diff_n: 1251566
   rep_missing_n: 30
   unreg_n: 1246475

   eos fsck summary report
   timestamp=1613069473 tag="blockxs_err" count=43
   timestamp=1613069473 tag="orphans_n" count=29602
   timestamp=1613069473 tag="rep_diff_n" count=181913
   timestamp=1613069473 tag="rep_missing_n" count=28
   timestamp=1613069473 tag="unreg_n" count=180998

.. index::  
   pair: Tracker; Replication Tracker


Replication Tracker
-------------------

The Replication Tracker follows the workflow of file creations. For each created file a virtual entry is created in the ``proc/tracker`` directory. Entries are removed once a layout is completely commited. The purpose of this tracker is to find inconsistent files after creation and to remove atomic upload relicts automatically after two days.


.. warning:: Please note that using the tracker will increase the meta-data operation load on the MGM! 

.. index::  
   pair: Tracker; Configuration

Configuration
^^^^^^^^^^^^^

Tracker
"""""""
The Replication Tracker has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.tracker=on  
   # disable
   eos space config default space.tracker=off

By default Replication Tracking is disabled.

The current status of the Tracker can be seen via:

.. code-block:: bash

   eos space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   tracker                        := off
   ...


Automatic Cleanup
^^^^^^^^^^^^^^^^^

When the tracker is enabled, an automatic thread inspects tracking entries and takes care of cleanup of tracking entries and the time based tracking directory hierarchy. Atomic upload files are automatically cleaned after 48 hours when the tracker is enabled.

.. index::  
   pair: Tracker; Info

Listing Tracking Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can get the current listing of tracked files using:

.. code-block:: bash

   eos space tracker 

   # ------------------------------------------------------------------------------------
   key=00142888 age=4 (s) delete=0 rep=0/1 atomic=1 reason=REPLOW uri='/eos/test/creations/.sys.a#.f.1.802e6b70-973e-11e9-a687-fa163eb6b6cf'
   # ------------------------------------------------------------------------------------

   

The displayed reasons are:

* REPLOW - the replica number is too low
* ATOMIC - the file is an atomic upload
* KEEPIT - the file is still in flight
* ENOENT - the tracking entry has no corresponding namespace entry with the given file-id
* REP_OK - the tracking entry is healthy and can be removed - FUSE files appear here when not replica has been committed yet

There is convenience command defined in the console:

.. code-block:: bash

   eos tracker # instead of eos space tracker


.. index::  
   pair: Tracker; Log Files

Log Files
^^^^^^^^^
The Replication Tracker has a dedicated log file under ``/var/log/eos/mgm/ReplicationTracker.log``
which shows the tracking entires and related cleanup activities. To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info
