.. highlight:: rst

.. index::
   pair: Storage and Client geo tagging; GEOTAG


GeoTags
=======

In a geographically distributed storage system it is important to tag each storage server with a geographical location.
You should group servers into LAN groups e.g. all servers which are close in term of network latency shared a GeoTag.
GeoTags are just arbitrary strings (for the time being). Each FST defines in ``/etc/sysconfig/eos`` a GeoTag:

Storage Node Tagging
--------------------

.. code-block:: bash
   
   # The EOS host geo location tag used to sort hosts into geographical (rack) locations 
   export EOS_GEOTAG="CERN::513::1"

.. highlight:: Each portion of the GeoTag string must be delimited by "::" and have a maximun length of 8 characters

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

