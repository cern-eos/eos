.. highlight:: rst

.. index::
   pair: Storage and Client geo tagging; GEOTAG


GeoTag Configuration for client and storage server
==================================================

In a geographically distributed storage system it is important to tag each storage server with a geographical location.
You should group servers into LAN groups e.g. all servers which are close in term of network latency shared a GeoTag.
GeoTags are just arbitrary strings (for the time beeing). Each FST defines in ``/etc/sysconfig/eos`` a GeoTag:

Storage Node Tagging
--------------------

.. code-block:: bash
   
   # The EOS host geo location tag used to sort hosts into geographical (rack) locations 
   export EOS_GEOTAG="CERN"

Client Tagging
--------------

To optimise the network path it is not enough to tag only the storage server. EOS allows you to define IP prefixes to match a client
to a certain location. This is done via the **vid** command:

.. code-block:: bash
   eos vid -h
   ... 
   vid set geotag <IP-prefix> <geotag>  : add to all IP's matching the prefix <prefix> the geo location tag <geotag>
                                          N.B. specify the default assumption via 'vid set geotag default <default-tag>'

As an example we could define the default location to be CERN:


.. code-block:: bash

   eos vid set geotag default CERN


While we want to assign all clients with 111.222.x.x. to CANADA:

.. code-block:: bash
   
   eos vid set 111.122. CANADA

Scheduling
----------

The scheduling takes this settings now into account in different ways when you write a file and when you read a file.

Writing
+++++++

The EOS scheduler picks one of your filesystem groups round-robin and tries to match one storage location with your client GeoTag. 
The client will connect to this location. The scheduler picks now any other location in the same storage group which does not have
this GeoTag. The client writes to the close first location, the data is replicated asynchronously on **write** from the close storage server 
to the remote storage server and synchronizes on **close** . Assuming that you have both locations available, the default configuration does
not guarantee that in 100% of cases the first location shares the same GeoTag because the scheduler uses a weighted selection (95% probability on idle system). If you 
want to enforce exact placement for the first location, you can enforce this by configuring:

.. code-block:: bash

   eos space config default space.geo.access.policy.write.exact=on 

Reading
+++++++

For reading the scheduler tries to match the client GeoTag with a replica location GeoTag. Also here it is by default not guaranteed that the closest
replica is always selected (95% probability on idle system). If you want to enforce lcoation matching you can configure:

.. code-block:: bash

   eos space config default space.geo.access.policy.read.exact=on 

.. note::

   Consult the help text ``eos space -h``. 


N-Site Configuration
++++++++++++++++++++
EOS Aquamarine is tuned for dual site setups. If you want to setup a system with more sites, you can enforce N-site replication by grouping one filesystem of each site into exact one group.
If the replication count is N you will have exactly one replica on each site. 

.. warning::

   If one disk becomes unavailable in such a configuration the group is skipped for writing until all disks are again writable.




