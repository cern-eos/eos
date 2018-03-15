.. highlight:: rst

.. index::
   pair: Quarkdb

.. _quarkdb:



QuarkDB Configuration
=====================

`QuarkDB <https://quarkdb.web.cern.ch/docs/master>`_ is a highly available datastore that implements a small subset of the redis command set. It is built on top of rocksdb, an embeddable, transactional key-value store. High availability is achieved through multiple replicated nodes and the raft distributed consensus algorithm. 

The EOS CITRINE version allows to persist the namespace in QuarkDB avoiding the high memory footprint of the in-memory namespace. QuarkDB is considered like a database as an external component which you need to install, configure and manage separatly.

Preparation
-----------

QuarkDB has the best performance when storing KV data on an SSD. To guarantee high-availability with predictable performance you should have three homogeneous nodes. In theory it also possible to have only one high-performance node with an SSD, but there is currently no way to set a leader preference. 

Installation
------------

We currently provide RPMs for QuarkDB on CentOS 7. Configure first the repository file `/etc/yum.repos.d/quarkdb.repo`:

.. code-block:: bash

   [quarkdb-stable]
   name=QuarkDB stable repository
   baseurl=https://storage-ci.web.cern.ch/storage-ci/quarkdb/tag/el7/$basearch
   enabled=1
   gpgcheck=0
   protect=1

Install the relevant RPMs on all three nodes:

.. code-block:: bash

   yum install quarkdb quarkdb-debuginfo redis

.. note::

   QuarkDB has a dependency on `XRootD <http://xrootd.org>`_ (see :ref:`eos_base_setup_repos` or use the EPEL repository).
   The **redis** package is installed to get access to the **redis-cli** command.

Configuration
-------------

On each of the nodes we have to create a DB directory using `quarkdb-create`. Each node in the cluster has to use an agreed `cluster-id` to allow to peer between the three QuarkDB cluster nodes. The `cluster-id` can be the instance name or a UUID.

.. code-block:: bash

   //  node 1 
   quarkdb-create --path /ssd/quarkdb/node-1 --clusterID eosfoo.bar --nodes node1foo.bar:7777,node2foo.bar:7777,node3foo.bar:7777

QuarkDB runs as a protocol plugin inside `XRootD <http://xrootd.org>`_. To start QuarkDB as an XRootD service you have first to create one configuration file `/etc/xrootd/xrootd-quarkdb.cf` per machine:

.. code-block:: bash

   # xrootd@quarkdb node 1
   xrd.port 7777
   xrd.protocol redis:7777 libXrdQuarkDB.so
   redis.mode raft
   redis.database /ssd/quarkdb/node-1
   redis.myself node1.foo.bar:7777

.. code-block:: bash

   # xrootd@quarkdb node 2
   xrd.port 7777
   xrd.protocol redis:7777 libXrdQuarkDB.so
   redis.mode raft
   redis.database /ssd/quarkdb/node-1
   redis.myself node2.foo.bar:7777

.. code-block:: bash

   # xrootd@quarkdb node 3
   xrd.port 7777
   xrd.protocol redis:7777 libXrdQuarkDB.so
   redis.mode raft
   redis.database /ssd/quarkdb/node-1
   redis.myself node3.foo.bar:7777

Service Management - start and stop
-----------------------------------

The QuarkDB service is managed via **systemd** on CentOS7:

.. code-block:: bash

   // start
   systemctl start xrootd@quarkdb

   // stop 
   systemctl stop  xrootd@quarkdb

   // status
   systemctl status xrootd@quarkdb

   // restart
   systemctl restart xrootd@quarkdb

Checking your cluster
-----------------------

Using the raft algorith the available nodes elect a leader, when at least two out of three nodes are availab. 

You can verify the state of each QuarkDB node using the redis-cli:

.. code-block:: bash 

   redis-cli -p 7777
  
   127.0.0.1:7777> raft-info
    1) TERM 6
    2) LOG-START 0
    3) LOG-SIZE 21
    4) LEADER qdb-test-1.cern.ch:7777
    5) CLUSTER-ID ed174a2c-3c2d-4155-85a4-36b7d1c841e5
    6) COMMIT-INDEX 20
    7) LAST-APPLIED 20
    8) BLOCKED-WRITES 0
    9) LAST-STATE-CHANGE 155053 (1 days, 19 hours, 4 minutes, 13 seconds)
   10) ----------
   11) MYSELF node1.foo.bar:7777
   12) STATUS LEADER
   13) ----------
   14) MEMBERSHIP-EPOCH 0
   15) NODES node1.foo.bar:7777,node2.foo.bar:7777,node3.foo.bar:7777
   16) OBSERVERS
   17) ----------
   18) REPLICA node2.foo.bar:7777 ONLINE | UP-TO-DATE | NEXT-INDEX 21
   19) REPLICA node3.foo.bar:7777 ONLINE | UP-TO-DATE | NEXT-INDEX 21

The above output yields, that node1.foo.bar is currently the leader. All redis commands are issued against the leader.

You can verify that your cluster is operational:

.. code-block:: bash

   // on the leader
   redis-cli -p 7777 
   node1.foo.bar:7777> set testkey hello
   OK
   node1.foo.bar:7777> get testkey
   "hello"

Security
--------

Currently QuarkDB is deployed without TLS. To make sure no third party access or tampers your KV storage you should configure the firewall accordingly that only MGM and FST nodes have direct access to QuarkDB (by default on port 7777).

This will change in the future.

Source Code
-----------

QuarkDB is OpenSource and available on `GitHUB <https://gitlab.cern.ch/eos/quarkdb>`_ and `GitLAB@CERN <https://gitlab.cern.ch/eos/quarkdb>`_.

To build QuarkDB manually do

.. code-block:: bash
    
   git clone https://gitlab.cern.ch/eos/quarkdb && cd quarkdb
   git submodule update --recursive --init

   mkdir build && cd build
   cmake ..
   make -j 4
   ./test/quarkdb-tests

Build dependencies can be installed using/running `utils/el7-packages.sh`.

