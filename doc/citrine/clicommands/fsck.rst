fsck
----

.. code-block:: text

  fsck [stat|config|report|repair]
    control and display file system check information
.. code-block:: text

    fsck stat
    print summary of consistency checks
    fsck config <key> [<value>]
    configure the fsck with the following possible options:
    toggle-collect       : enable/disable error collection thread, <value> represents
    the collection interval in minutes [default 30]
    toggle-repair        : enable/disable repair thread, no <value> required    show-dark-files      : yes/no [default no]
    show-offline         : yes/no [default no]
    show-no-replica      : yes/no [default no]
    max-queued-jobs      : maximum number of queued jobs
    max-thread-pool-size : maximum number of threads in the fsck pool
    fsck report [-a] [-h] [-i] [-l] [-j|--json] [--error <tag1> <tag2> ...]
    report consistency check results, with the following options
    -a         : break down statistics per file system
    -i         : display file identifiers
    -l         : display logical file name
    -j|--json  : display in JSON output format
    --error    : display information about the following error tags
    fsck repair --fxid <val> [--async]
    repair the given file if there are any errors
    --fxid  : hexadecimal file identifier
    --async : job queued and ran by the repair thread if enabled
  
