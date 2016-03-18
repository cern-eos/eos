io
--

.. code-block:: text

   usage: io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x]               :  print io statistics
      -l                                                   -  show summary information (this is the default if -t,-d,-x is not selected)
      -a                                                   -  break down by uid/gid
      -m                                                   -  print in <key>=<val> monitoring format
      -n                                                   -  print numerical uid/gids
      -t                                                   -  print top user stats
      -d                                                   -  break down by domains
      -x                                                   -  break down by application
      io enable [-r] [-p] [-n] [--udp <address>]                 :  enable collection of io statistics
      -r    enable collection of io reports
      -p    enable popularity accounting
      -n    enable report namespace
      --udp <address> add a UDP message target for io UDP packtes (the configured targets are shown by 'io stat -l'
      io disable [-r] [-p] [-n]                                       :  disable collection of io statistics
      -r    disable collection of io reports
      -p    disable popularity accounting
      --udp <address> remove a UDP message target for io UDP packtes
      -n    disable report namespace
      io report <path>                                           :  show contents of report namespace for <path>
      io ns [-a] [-n] [-b] [-100|-1000|-10000] [-w] [-f]         :  show namespace IO ranking (popularity)
      -a    don't limit the output list
      -n :  show ranking by number of accesses
      -b :  show ranking by number of bytes
      -100 :  show the first 100 in the ranking
      -1000 :  show the first 1000 in the ranking
      -10000 :  show the first 10000 in the ranking
      -w :  show history for the last 7 days
      -f :  show the 'hotfiles' which are the files with highest number of present file opens
