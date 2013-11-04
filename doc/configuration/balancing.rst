.. highlight:: rst

.. index::
   single: Balancing System

Balancing System
================

Overview
--------

The EOS balancing system provides a fully automated mechanism to balance the 
volume usage across a scheduling group. Hence currently the balancing system 
does not balance between scheduling groups! See :doc:`groupbalancer`!

The balancing system is made up by the cooperation of several components:

* Central File System View with file system usage information and space configuration
* Centrally running balancer thread steering the filesystem balancer process by computing averages and deviations
* Balancer Thread on each FST pulling workload to pull files locally to balance filesystems

.. ::note

   Balancing is en-/disabled in each space seperatly!

Balancing View and Configuration
--------------------------------

Each filesystem advertises the used volume and the central view allows to see 
the deviation from the average filesystem usage in each group.

.. code-block::bash

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

The configuration variables are:

.. epigraph::
  
   ========================= ======================================================================
   variable                  definition
   ========================= ======================================================================
   balancer                  can be off or on to disable or enable the balancing
   balancer.node.ntx         number of parallel balancer transfers running on each FST
   balancer.node.rate        rate limitation for each running balancer transfer
   balancer.threshold        percentage at which balancing get's enabled within a scheduling group
   ========================= ======================================================================
 
If balancing is enabled ....

.. code-block:: bash

   EOS Console [root://localhost] |/> space config default space.balancer=on
   success: balancer is enabled!

Groups which are balancing are shown via the **eos group ls** command:

.. code-block:; bash

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

.. code-block::bash

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