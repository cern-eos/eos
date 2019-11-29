.. index::
   single: Docker Image

.. _eos_base_docker_image:

.. _docker: https://docs.docker.com/

EOS Docker Installation
=======================

.. image:: docker.jpg
   :scale: 50 %
   :align: center   

A docker installation is the easiest way to go if you want to try EOS and get up a test instance in short time. 
We provide EOS docker images with all the necessary components installed and ready to use.


Preparation
-----------

.. note::

   Make sure you have docker_ installed and the docker daemon started on your system.

.. code-block:: bash

   // Example on CentOS 7
   yum install docker
   systemctl start docker


Run EOS in docker
-----------------

Checkout the `eos-docker <https://gitlab.cern.ch/eos/eos-docker>`_ project:

.. code-block:: bash

   git clone https://gitlab.cern.ch/eos/eos-docker.git
   cd eos-docker

To start a small instance with 6 storage servers (**FST**), 1 namespace server (**MGM**), 1 messaging borker (**MQ**), 1 client and 1 Kerberos **KDC** containers ready to use,
all you have to do is to use the `start_services <https://gitlab.cern.ch/eos/eos-docker/blob/master/scripts/start_services.sh>`_ script.

The arguments to provide are the name of the image to use with the **-i** and the number of FST containers (default is 6) with the **-n** flag.

The containers will reside on the same network knowing about each other and are configured to be working out-of-the-box.

.. code-block:: bash

   scripts/start_services.sh -i gitlab-registry.cern.ch/dss/eos:4.2.16 -n 6

To connect to EOS using the *eos* shell CLI running in the MGM container you can do:

.. code-block:: bash

   docker exec -i eos-mgm1 eos
   EOS Console [root://localhost] |/> whoami
   whoami
   Virtual Identity: uid=0 (2,99,3,0) gid=0 (99,4,0) [authz:sss] sudo* host=localhost

.. code-block:: bash

   EOS Console [root://localhost] |/> version
   version
   EOS_INSTANCE=eosdockertest
   EOS_SERVER_VERSION=4.2.16 EOS_SERVER_RELEASE=1
   EOS_CLIENT_VERSION=4.2.16 EOS_CLIENT_RELEASE=1

.. code-block:: bash

   EOS Console [root://localhost] |/> node ls
   node ls
   ┌──────────┬─────────────────────────────────────┬────────────────┬──────────┬────────────┬──────┬──────────┬────────┬────────┬────────────────┬─────┐
   │type      │                             hostport│          geotag│    status│      status│  txgw│ gw-queued│  gw-ntx│ gw-rate│  heartbeatdelta│ nofs│
   └──────────┴─────────────────────────────────────┴────────────────┴──────────┴────────────┴──────┴──────────┴────────┴────────┴────────────────┴─────┘
    nodesview  eos-fst1.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1
    nodesview  eos-fst2.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1
    nodesview  eos-fst3.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1
    nodesview  eos-fst4.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1
    nodesview  eos-fst5.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1
    nodesview  eos-fst6.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2

You can mount EOS to the client container using FUSE and KRB5 authentication.

.. code-block:: bash

   docker exec -i eos-cli1 env EOS_MGM_URL=root://eos-mgm1.eoscluster.cern.ch eos fuse mount /eos
   docker exec -it eos-cli1 bash

   .... trying to create ... /eos
   ===> Mountpoint   : /eos
   ===> Fuse-Options : max_readahead=131072,max_write=4194304,fsname=eos-mgm1.eoscluster.cern.ch,url=root://eos-mgm1.eoscluster.cern.ch//eos/
   ===> fuse readahead        : 1
   ===> fuse readahead-window : 1048576
   ===> fuse debug            : 0
   ===> fuse low-level debug  : 0
   ===> fuse log-level        : 5 
   ===> fuse write-cache      : 1
   ===> fuse write-cache-size : 67108864
   ===> fuse rm level protect : 1
   ===> fuse lazy-open-ro     : 0
   ===> fuse lazy-open-rw     : 1
   ==== fuse multi-threading  : true
   info: successfully mounted EOS [root://eos-mgm1.eoscluster.cern.ch] under /eos

.. code-block:: bash

   [root@testmachine eos-docker]# docker exec -it eos-cli1 bash 
   ls -la /eos/
   total 4
   drwxrwxr-x.  1 root root    0 Jan  1  1970 .
   drwxr-xr-x. 18 root root 4096 Mar 14 10:16 ..
   drwxrwxr-x.  1 root root    0 Jan  1  1970 dockertest

Or by running the EOS instance testsuite

.. code-block:: bash

   docker exec -i eos-mgm1 eos-instance-test


Delete and clean
-------------------

You can use the  `shutdown_services <https://gitlab.cern.ch/eos/eos-docker/blob/master/scripts/shutdown_services.sh>`_ script to remove these EOS containers from your system.

.. code-block:: bash

   scripts/shutdown_services.sh


Image Repository
-------------------

You can get the images for each automatic build and for each release.
The release images are tagged with the release version. Regular images are tagged with the build id of their originating pipeline.

Docker images are accessible from the project's `registry <https://gitlab.cern.ch/dss/eos/container_registry>`_.


.. code-block:: bash

   docker pull gitlab-registry.cern.ch/dss/eos:<tag>

Example for a build

.. code-block:: bash

   docker pull gitlab-registry.cern.ch/dss/eos:206970

Example for the latest release

.. parsed-literal::

   docker pull gitlab-registry.cern.ch/dss/eos:|version| 

Selfmade images
---------------

In case you would like to create a different setup, you are welcome to browse and reuse the provided scripts under
the `image_scripts <https://gitlab.cern.ch/eos/eos-docker/tree/master/image_scripts>`_ directory to get an idea on how to do it.
