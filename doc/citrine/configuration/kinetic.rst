Kinetic
=======================

Preconditions
+++++++++++++
The following standard packages are required

.. code-block:: bash

   yum install json-c libuuid

Additionally, you will need to install the packages available at https://dss-ci-repo.web.cern.ch/dss-ci-repo/kinetic/. If you have afs access, this can be done by

.. code-block:: bash

   yum install /afs/cern.ch/project/dss_ci/repo/kinetic/*


Please refer to the installation docs on how to install eos. Make sure not to install a version without kinetic support.

Kinetic Drive Setup
+++++++++++++++++++++
If you need to administrate individual kinetic drives (e.g. reformat drives, set up security roles and passwords), one solution is to use https://github.com/Seagate/kinetic-java-tools. The tool accepts lists of drives to perform admin operations on. For the syntax see the github documentation.


EOS Kinetic Cluster Configuration
++++++++++++++++++++++++++++++++++++++

For each eos space you wish to host kinetic clusters in, you need to generate the following configuration files in /var/eos/kinetic/

 + kinetic-location-spacename.json  -  Location information (ip / dns name)
 + kinetic-security-spacename.json  -  Security information (user id, password)
 + kinetic-cluster-spacename.json   -  Definitions of clusters as well as overall library configuration.

The directory will already contain example json files that demonstrate the syntax and can be used to get started. In the following the configuration options are explained in detail.

LOCATION:
 - wwn: The world wide name of the drive. While each drive exports a world wide name that can be used for this field, an arbitrary value can be used as long as it is unique.
 - inet4: The ip addresses for both interfaces of the kinetic drive. If you only want to use a single interface you may list it twice.
 - port: The port to connect to. The standard port for Kinetic services is 8123.

SECURITY:
 - wwn: The world wide name of the drive.
 - userId: The user id to use when establishing a connection.
 - key: The secret key / the password to use when establishing a connection.
CLUSTER:
 - clusterID: The cluster identifier. As the drive wwn it can be freely chosen but has to be unique. It may not contain the ':' or '/' symbols.
 - numData: The number of data chunks that will be stored in a data stripe (required to be >=1).
 - numParity: Defines the redundancy level of this cluster (required to be >=0). Metadata and attribute keys will be stored with numParity replication. Data will be stored in (numData,numParity) erasure coded stripes (unless numData is defined as 1 in which case replication will be used).
 - chunkSizeKB: The maximum size of data chunks in KB (required to be min. 1 and max. 1024). A value of 1024 is optimal for Kinetic drive performance.
 - timeout: Network timeout for cluster operations in seconds.
 - minReconnectInterval: The minimum time / rate limit in seconds between reconnection attempts.
 - drives: A list of wwn identifiers for all drives associated with the cluster. The order of the drives is important and may not be changed after data has been written to the cluster. If a drive is replaced, the new drive wwn has to replace the old drive wwn at the same position.

LIBRARY:
 - cacheCapacityMB: The maximum cache size in MB. The cache is used to hold data for currently executing operations. It has to be at least the stripe size times the maximum number of concurrent data streams. E.g. for a setup with 16-4 erasure coding configuration, 1 MB chunkSize and an expected 20 concurrent data streams the cache capacity should be 20MB*20Streams=400MB. Larger cache capacities are beneficial as they allow higher concurrency for writing (asynchronous flushes of multiple data blocks per stream) as well as more prefetching and data retention for read scenarios.
 - maxBackgroundIoThreads: The maximum number of background IO threads. If > 0 it sets the limit for concurrent I/O operations (put, get, del). For 10G EOS nodes, 12 to 16 seems to achieve good performance.
 - maxBackgroundIoQueue: The maximum number of IO operations queued for execution. If set to 0, background threads will not be held in a pool but use one-shot threads spawned on-demand. For normal operation a value of ~3 times the number of background threads works well.
 - maxReadaheadWindow: limit the maximum readahead to set number of data stripes. Note that the maximum readahead will only be reached if the access pattern is very predictable and there is no cache pressure.


EOS Kinetic Cluster Administration
++++++++++++++++++++++++++++++++++
The eos kinetic command line tools provide a variety of admin functions. In the following, some of the most important functions are described. You can view all available functionality by typing *eos kinetic -h*

.. code-block:: bash

  eos kinetic --space <spacename> config

Display the currently loaded configuration for a space


.. code-block:: bash

  eos kinetic --space <spacename> config --publish

Load the configuration specified in the json files for the space into eos. This can be done during normal operation. While library wide configuration changes are immediate, changes to cluster configuration as well as drive location or security will only apply to files opened after the configuration has been published. Files that already had been opened at that time will continue using the configuration that was current when they were opened.

.. code-block:: bash

  eos kinetic --space <spacename> --id <cluster-id> status

Display the cluster status / health. This will list healthy & failed connections to kinetic drives of the cluster as well as if any indicator keys on any drive. If none exist, you can assume the data stored to be complete. If indicator keys do exist, a repair operation should be attempted as soon as failed connections can be reestablished.


.. code-block:: bash

  eos kinetic --space <spacename> --id <cluster-id> count indicator

Count the number of existing indicators. This can be helpful to estimate runtime of scan and / or repair operations.

.. code-block:: bash

  eos kinetic --space <spacename> --id <cluster-id> scan indicator

Check if repair operations are possible for the  metadata / attribute / data keys with indicators.

.. code-block:: bash

  eos kinetic --space <spacename> --id <cluster-id> repair indicator

Attempt to repair all metadata / attribute / data keys on the cluster that have an indicator.









