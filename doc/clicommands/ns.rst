ns
--

.. code-block:: text

   ns                                                         :  print basic namespace parameters
      ns stat [-a] [-m] [-n]                                     :  print namespace statistics
      -a                                                   -  break down by uid/gid
      -m                                                   -  print in <key>=<val> monitoring format
      -n                                                   -  print numerical uid/gids
      --reset                                              -  reset namespace counter
      ns mutex                                                   :  manage mutex monitoring
      --toggletiming                                       -  toggle the timing
      --toggleorder                                        -  toggle the order checking
      --smplrate1                                          -  set the timing sample rate at 1%   (default, almost no slow-down)
      --smplrate10                                         -  set the timing sample rate at 10%  (medium slow-down)
      --smplrate100                                        -  set the timing sample rate at 100% (severe slow-down)
      ns compact on <delay> [<interval>] [<type>]                   -  enable online compactification after <delay> seconds
      -  if <interval> is >0 the compactifcation is repeated automatically after <interval> seconds!
      -  <type> can be 'files' 'directories' or 'all'. By default only the file changelog is compacted!
      -  the repair flag can be indicated by adding '-repair': 'files-repair', 'directories-repair', 'all-repair'
      ns compact off                                                -  disable online compactification
      ns master <master-hostname>|[--log]|--log-clear            :  master/slave operation
      ns master <master-hostname>                                   -  set the host name of the MGM RW master daemon
      ns master                                                     -  show the master log
      ns master --log                                               -  show the master log
      ns master --log-clear                                         -  clean the master log
      ns master --disable                                           -  disable the slave/master supervisor thread modifying stall/redirection variables
      ns master --enable                                            -  enable  the slave/master supervisor thread modifying stall/redirectino variables
