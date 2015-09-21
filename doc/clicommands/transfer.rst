transfer
--------

.. code-block:: text

   transfer submit|cancel|ls|enable|disable|reset|clear|resubmit|log ..'[eos] transfer ..' provides the transfer interface of EOS.
   Options:
   transfer submit [--rate=<rate>] [--streams=<#>] [--group=<groupname>] [--sync] [--noauth] <URL1> <URL2> :
      transfer a file from URL1 to URL2
      <URL> can be root://<host>/<path> or a local path /eos/...
      --rate          : limit the transfer rate to <rate>
      --streams       : use <#> parallel streams
.. code-block:: text

      --group         : set the group name for this transfer
      --noauth        : disable authentication protocol enforcement for the copy job
   transfer cancel <id>|--group=<groupname>
      cancel transfer with ID <id> or by group <groupname>
      <id>=*          : cancel all transfers (only root can do that)
   transfer ls [-a] [-m] [s] [--group=<groupname>] [id] 
      -a              : list all transfers not only of the current role
      -m              : list all transfers in monitoring format (key-val pairs)
      -s              : print transfer summary
      --group         : list all transfers in this group
      --sync          : follow the transfer in interactive mode (like interactive third party 'cp')
      <id> : id of the transfer to list
   transfer enable
      : start the transfer engine (you have to be root to do that)
   transfer disable
      : stop the transfer engine (you have to be root to do that)
   transfer reset [<id>|--group=<groupname>]
      : reset all transfers to 'inserted' state (you have to be root to do that)
   transfer clear 
      : clear's the transfer database (you have to be root to do that)
   transfer resubmit <id> [--group=<groupname>]
      : resubmit's a transfer
   transfer kill <id>|--group=<groupname>
      : kill a running transfer
   transfer purge [<id>|--group=<groupname>]
      : remove 'failed' transfers from the transfer queue by id, group or all if not specified
