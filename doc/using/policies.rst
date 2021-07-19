.. highlight:: rst

.. _space-policies:

Space and Application Policies
==============================

Space policies are set using the space configuration CLI.

The following policies can be configured

.. epigraph::

   ============= ==============================================
   key           values
   ============= ==============================================
   space         default,...
   layout        plain,replica,raid5,raid6,raiddp,archive,qrain           
   nstripes      1..255           
   checksum      adler,md5,sha1,crc32,crc32c        
   blockchecksum adler,md5,sha1,crc32,crc32c           
   blocksize     4k,64k,128k,512k,1M,4M,16M,64M           
   bandwidth     IO limit in MB/s
   ============= ==============================================


Setting space policies
----------------------


.. code-block:: bash

   # configure raid6 layout   
   eos space config default space.policy.layout=raid6

   # configure 10 stripes
   eos space config default space.policy.nstripes=10

   # configure adler file checksumming
   eos space config default space.policy.checksum=adler

   # configure crc32c block checksumming
   eos space config default space.policy.blockchecksum=crc32c

   # configure 1M blocksizes
   eos space config default space.policy.blocksize=1M

   # configure a global bandwidth limitation for all streams of 100 MB/s in a space
   eos space config default space.policy.bandwidth=100


Setting application policies
-------------------------------------

Application bandwidht policies apply for all read and write streams.

   # configure an application specific bandwidth limitations for all streams in a space
   eos space config default space.bw.myapp=100 # streams tagged as ?eos.app=myapp are limited to 100 MB/s

   eos space config default space.bw.eoscp=200 # limit untagged eoscp to 200 MB/s

Policy Selection and Scopes
----------------------------

Clients can select the space ( and its default policies ) by adding ``eos.space=<space>`` to the CGI query of an URL, otherwise the space is taken from **space.policy.space** in the default space or if undefined it uses the **default** space to set space policies.

Examples:

.. code-block:: bash

   ##############
   # Example 1  #
   ##############
   # files uploaded without selecting a space will end up in the replica space unless there is a forced overwrite in the target directory

   # point to the replica space as default policy
   eos space config default space.policy..space=replica
   # configure 2 replicas in the replica space
   eos space config replica space.policy.nstripes=2
   eos space config replica space.policy.layout=replica


   ##############
   # Example 2  #
   ##############
   # files uploaded selecting the rep4 space will be stored with 4 replicas, if non space is selected they will get the default for the target directory or the default space

   # define a space with 4 replica policy
   eos space config rep4 space.policys.nstripes=4
   eos space config rep4 space.policy.layout=replica


Local Overwrites
----------------

The space polcies are overwritten by the local extended attribute settings of the parent directory

.. epigraph::

   ============= ===================================================
   key           local xattr
   ============= ===================================================
   layout        sys.forced.layout, user.forced.layout
   nstripes      sys.forced.nstripes, user.forced.nstripes
   checksum      sys.forced.checksum, user.forced.checksum
   blockchecksum sys.forced.blockchecksum, user.forced.blockchecksum   
   blocksize     sys.forced.blocksize, user.forced.blocksize
   ============= ===================================================


Deleting space policies
-----------------------

Policies are deleted by setting a space policy with `value=remove` e.g.

.. code-block:: bash

   # delete a policy entry
   eos space config default space.policy.layout=remove

   # delete an application bandwidth entry
   eos space config default space.bw.myapp=remove


Displaying space policies
-------------------------

Policies are displayd using the ``space status`` command:

.. code-block:: bash

   eos space status default

   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   autorepair                       := off
   ...
   policy.blockchecksum             := crc32c
   policy.blocksize                 := 1M
   policy.checksum                  := adler
   policy.layout                    := replica
   policy.nstripes                  := 2
   policy.bandwidth                 := 100
   ...
   bw.myapp                         := 100
   bw.eoscp                         := 200
   ...

Automatic Conversion Policies
-----------------------------

Automatic policy conversion policies allow to trigger a conversion job under two conditions:

* a new file is created with a complete layout (all required replicas/stripes are created)        (use case IO optimization)
* an existing file is injected with a complete layout (all required replicas/stripes are created) (use case TAPE recall)

Automatic conversion policy hooks are triggered by the ReplicationTracker. You find conversions triggerd in the **ReplicationTracker.log** logfile.

To use automatic conversion hooks one has to enable policy conversion in the **default** space:

.. code-block:: bash

   eos space config default space.policy.conversion=on

To disable either remove the entry or set the value to off:

.. code-block:: bash

   #remove
   eos space config default space.policy.conversion=remove
   #or disable
   eos space config default space.policy.conversion=off

It takes few minutes before the changed state takes effect!


To define a policy conversion whenever a file is uploaded for a specific space you configure:

.. code-block:: bash

   # whenever a file is uploaded to the space **default** a conversion is triggered into the space **replicated** using a **replica::2** layout.
   eos space config default space.policy.conversion.creation=replica:2@replicated

   # alternative declaration using a hex layout ID
   eos space config default space.policy.conversion.creation=00100112@replicated

Also make sure that the converter is enabled:

.. code-block:: bash

   # enable the converter
   eos space config default space.converter=on

To define a policy conversion whenever a file is injected into a specific space you configure:

.. code-block:: bash

   # whenever a file is injected to the space **ssd* a conversion is triggered into the space **spinner** using a **raid6:10** layout.
   eos space config ssd space.policy.conversion.injection=raid6:10@spinner

   # alternative declaration using a hex layout ID: replace raid6:10 with the **hex layoutid** (e.g. see file info of a file).

.. warning::
   You cannot change the file checksum during a conversion job! Make sure source and target layout have the same checksum type!

You can define a minimum or maximum size criteria to apply automatic policy conversion depending on the file size.

.. code-block:: bash

   # convert files on creation only if they are atleast 100MB
   eos space config ssd space.policy.conversion.creation.size=>100000000

   # convert files on creation only if they are smaller than 1024 bytes
   eos space config ssd space.policy.conversion.creation.size=<1024

   # convert files on injection only if they are bigger than 1G
   eos space config ssd space.policy.conversion.injection.size=>1000000000

   # convert files on injection only if they are smaller than 1M
   eos space config ssd space.policy.conversion.injection.size=<1000000



 
