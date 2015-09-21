dropbox
-------

.. code-block:: text

   dropbox add|rm|start|stop|add|rm|ls ...
   '[eos] dropbox ...' provides dropbox functionality for eos.
   Options:
   dropbox add <eos-dir> <local-dir>   :
      add drop box configuration to synchronize from <eos-dir> to <local-dir>!
   dropbox rm <eos-dir>                :
      remove drop box configuration to synchronize from <eos-dir>!
   dropbox start [--resync]             :
      start the drop box daemon for all configured dropbox directories! If the --resync flag is given, the local directory is resynced from scratch from the remote directory!
   dropbox stop                        :
      stop the drop box daemon for all configured dropbox directories!
   dropbox ls                          :
      list configured drop box daemons and their status
