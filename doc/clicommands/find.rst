find
----

.. code-block:: text

   usage: find [--childcount] [--purge <n> ] [--count] [-s] [-d] [-f] [-0] [-1] [-ctime +<n>|-<n>] [-m] [-x <key>=<val>] [-p <key>] [-b] [-c %tags] [-layoutstripes <n>] <path>
      -f -d :  find files(-f) or directories (-d) in <path>
      -x <key>=<val> :  find entries with <key>=<val>
      -0 :  find 0-size files
      -g :  find files with mixed scheduling groups
      -p <key> :  additionally print the value of <key> for each entry
      -b :  query the server balance of the files found
      -c %tags  :  find all files with inconsistencies defined by %tags [ see help of 'file check' command]
      -s :  run as a subcommand (in silent mode)
      -ctime +<n> :  find files older than <n> days
      -ctime -<n> :  find files younger than <n> days
      -layoutstripes <n> :  apply new layout with <n> stripes to all files found
      --maxdepth <n> :  descend only <n> levels
      -1 :  find files which are atleast 1 hour old
      --stripediff :  find files which have not the nominal number of stripes(replicas)
      --faultyacl :  find directories with illegal ACLs
      --count :  just print global counters for files/dirs found
      --childcount :  print the number of children in each directory
      --purge <n> | atomic
      :  remove versioned files keeping <n> versions - to remove all old versions use --purge 0 ! To apply the settings of the extended attribute definition use <n>=-1! To remove all atomic upload left-overs older than a day user --purge atomic
      default :  find files and directories
      find [--nrep] [--nunlink] [--size] [--fileinfo] [--online] [--hosts] [--partition] [--fid] [--fs] [--checksum] [--ctime] [--mtime] [--uid] [--gid] <path>   :  find files and print out the requested meta data as key value pairs
      path=file:...  :  do a find in the local file system (options ignored) - 'file:' is the current working directory
      path=root:...  :  do a find on a plain XRootD server (options ignored) - does not work on native XRootD clusters
      path=as3:...   :  do a find on an S3 bucket
      path=...       :  all other paths are considered to be EOS paths!
