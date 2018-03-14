.. index::
   single: Docker Image

.. _eos_base_docker_image:

.. _docker: https://docs.docker.com/

EOS Docker Installation
=======================

If you want to try EOS and get up a test instance in two minutes, a docker installation is the way to do that.

We rovide EOS docker images with all the necessary components installed and ready to use.


Preparation
-----------

.. note::

   Make sure you have docker_ installed and the docker daemon started on your system.

.. code-block:: text

   // Example on CentOS 7
   yum install docker
   systemctl start docker


Run EOS in docker
-----------------

Checkout the `eos-docker <https://gitlab.cern.ch/eos/eos-docker>`_ project:

.. code-block:: text

   git clone https://gitlab.cern.ch/eos/eos-docker.git
   cd eos-docker

To start a small instance with 6 storage servers (FST), 1 namespace server (MGM), 1 messaging borker (MQ), 1 client and 1 Kerberos KDC containers ready to use,
all you have to do is to use the `start_services <https://gitlab.cern.ch/eos/eos-docker/blob/master/scripts/start_services.sh>`_ script.
You have to give the name of the image to use with the -i and the number of FST containers (default is 6) with the -n flags.

The containers will reside on the same network knowing about each other and configured to be working out-of-the-box.

.. parsed-literal::

   scripts/start_services.sh -i gitlab-registry.cern.ch/dss/eos:|version| -n 6

To connect to EOS using the EOS shell on the MGM container you can do:

.. code-block:: text

   docker exec -i eos-mgm-test eos
   EOS Console [root://localhost] |/> whoami
   whoami
   Virtual Identity: uid=0 (2,99,3,0) gid=0 (99,4,0) [authz:sss] sudo* host=localhost

   EOS Console [root://localhost] |/> version
   version
   EOS_INSTANCE=eosdockertest
   EOS_SERVER_VERSION=4.2.16 EOS_SERVER_RELEASE=1
   EOS_CLIENT_VERSION=4.2.16 EOS_CLIENT_RELEASE=1

   EOS Console [root://localhost] |/> node ls
   node ls
   ┌──────────┬─────────────────────────────────────┬────────────────┬──────────┬────────────┬──────┬──────────┬────────┬────────┬────────────────┬─────┐
   │type      │                             hostport│          geotag│    status│      status│  txgw│ gw-queued│  gw-ntx│ gw-rate│  heartbeatdelta│ nofs│
   └──────────┴─────────────────────────────────────┴────────────────┴──────────┴────────────┴──────┴──────────┴────────┴────────┴────────────────┴─────┘
    nodesview  eos-fst1-test.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1 
    nodesview  eos-fst2-test.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1 
    nodesview  eos-fst3-test.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1 
    nodesview  eos-fst4-test.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1 
    nodesview  eos-fst5-test.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2     1 
    nodesview  eos-fst6-test.eoscluster.cern.ch:1095      docker-test     online           on    off          0       10      120                2    

You can mounting EOS to the client container using FUSE and KRB5 authentication.

.. code-block:: text

   docker exec -i eos-client-test env EOS_MGM_URL=root://eos-mgm-test.eoscluster.cern.ch eos fuse mount /eos
   docker exec -i eos-client-test bash

   .... trying to create ... /eos
   ===> Mountpoint   : /eos
   ===> Fuse-Options : max_readahead=131072,max_write=4194304,fsname=eos-mgm-test.eoscluster.cern.ch,url=root://eos-mgm-test.eoscluster.cern.ch//eos/
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
   info: successfully mounted EOS [root://eos-mgm-test.eoscluster.cern.ch] under /eos

   [root@testmachine eos-docker]# docker exec -i eos-client-test bash 
   ls -la /eos/
   total 4
   drwxrwxr-x.  1 root root    0 Jan  1  1970 .
   drwxr-xr-x. 18 root root 4096 Mar 14 10:16 ..
   drwxrwxr-x.  1 root root    0 Jan  1  1970 dockertest

Or by running the the EOS instance testsuite

.. code-block:: text

   docker exec -i eos-mgm-test eos-instance-test

You can use the  `shutdown_services <https://gitlab.cern.ch/eos/eos-docker/blob/master/scripts/shutdown_services.sh>`_ script to remove these EOS containers from your system.

.. code-block:: text

   scripts/shutdown_services.sh


Image Repository
-------------------

You can get the images for each automatic build and for each release.
The release images are tagged with the release version. Regular images are tagged with the build id of their origin.

Docker images are accessible from the project's `registry <https://gitlab.cern.ch/dss/eos/container_registry>`_.


.. code-block:: text

   docker pull gitlab-registry.cern.ch/dss/eos:<tag>

Example for a build

.. code-block:: text

   docker pull gitlab-registry.cern.ch/dss/eos:206970

Example for the latest release

.. code-block:: text

   docker pull gitlab-registry.cern.ch/dss/eos: |version| 

Selfmade images
---------------

In case you would like to create a different setup, you are welcome to browse and reuse the provided scripts under
the `image_scripts <https://gitlab.cern.ch/eos/eos-docker/tree/master/image_scripts>`_ directory to get an idea on how to do it.


