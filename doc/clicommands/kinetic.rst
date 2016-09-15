kinetic
-------

.. code-block:: text

  ------------------------------------------------------------------------------------------------
  usage: kinetic config [--publish | --upload ] [--space <name>] [ --security | --cluster | --location] [--file <local-json-file>
    kinetic config [--space <name> ] [--security | --cluster | --location]
    shows the currently deployed kinetic configuration - by default 'default' space
    --security, --cluster, --location selects only the relevant entry for the given fieldn
    kinetic config --publish [--space <name>]
    publishes the configuration files under <mgm>:/var/eos/kinetic/ to all currently
    existing FSTs in default or referenced space
    kinetic config --upload {--cluster | --security| --location} --file <local-json-file>  [--space <name>]
    uploads the local configuration file as current version <mgm>:/var/eos/kinetic/ for the dedicated space and security,cluster or location description
  usage: kinetic --id <name> <operation> [OPTIONS] 
    --id <name>
    the name of target cluster (see kinetic config)
    <operation>
    count  : count number of keys existing in the cluster
    scan   : check keys and display their status information
    repair : check keys, repair as required, display key status information
    reset  : force remove keys (Warning: Data will be lost!)
    status : show health status of cluster.
    OPTIONS
    --target data|metadata|attribute|indicator
    Operations can be limited to a specific key-type. Setting the 'indicator' type will
    perform the operation on keys of any type that have been marked as problematic. In
    most cases this is sufficient and much faster. Use full scan / repair operations
    after a drive replacement or cluster-wide power loss event.
    --threads <number>
    Specify the number of background io threads used for a scan/repair/reset operation.
    --space <name>
    Use the kinetic configuration for the referenced space - by default 'default' space
    is used (see kinetic config).
    --bench <number>
    Only for status operation. Benchmark put/get/delete performance for each  connection
    using <number> 1MB keys to get rough per-connection throughput.
    -m : monitoring key=value output format
  ------------------------------------------------------------------------------------------------
