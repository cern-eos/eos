.. highlight:: rst

Introduction
=======================

History
-------
The `EOS <http:://eos.cern.ch>`_ project was started in April 2010 in the IT DSS group. The first production version of EOS was running in 2011.


Goal
----

The main goal of the project is to provide fast and reliable disk only storage technology for CERN LHC use cases. The following picture demonstrates the principle use case at CERN:

.. image:: eos-usage-cern.jpg
   :scale: 50 %
   :align: center



Software Versions
-----------------
The phasing-out production versions is called **Beryl/Aquamarine** v0.3.X.

The stable production version called **Citrine** is currently v4.2.X

License
-------
Since release version 0.1.1 EOS is available under GPL v3 license. 


Architecture
------------

EOS is made by three components:

* MGM - metadata server
* FST - storage server
* MQ - message broker for asynchronous messaging

.. image:: eos-base-arch.jpg
   :scale: 50 %
   :align: center

The initial architecture is using an in-memory namespace implementation with a master-slave high-availability model. This implementation provides very low-latency. Since the CITRINE release the architecture has been modified to provide optinal an in-memory namespace cache and a KV store for persistency. This was necessary to overcome the scalability limitation of the meta-data service given by the maximum available RAM of MGM nodes.

.. image:: eos-architecture.jpg
   :scale: 50 %
   :align: center

Storage Concepts
----------------

EOS uses a storage index concept to identify where and how files are stored. These information is stored inside the meta data of each file. 

Files are stored with a layout. The following layouts are supported

* plain - a file is stored as a plain file in one filesystem
* replica - a file is stored with a variable number of replicas in `n` filesystems
* rain - 
  
  * raid6 - a file is chunked into blocks and stored in `n-2` filesystems for data and `2` filesystems for parity blocks
  * archive - a file is chunked into blcoks and stored in `n-3` filesystems for data and `3` filesystems for parity blocks


EOS groups storage resources into three logical categories:

* spaces
* groups
* filesystems

A **space** is used to reference a physical location when files are placed by the storage system. **spaces** are made by placement **groups**. **groups** consist of one or many filesystems. The EOS scheduler selects a **group** to store all replicas or chunks of a file are stored within a single **group**. Therefore the filesystems within a group should never be on the same node to guarantee availability with node failures.

Protocols and Clients
---------------------

EOS is based on the `XRootD Framework <https://xrootd.org>`_.  The native protocol is the **xrootd** protocol. The second native protocol is **http/webdav** currently implemented using `libmicrohttpd`. 

EOS can be used like a filesystem using FUSE clients. There are two implementations of FUSE available:

* **eosd** - available for BERYL and CITRINE - limited POSIX conformity
* **eosxd** - available for CITRINE - improved POSIX conformity 

EOS has been extended to work simliar to `Owncloud <owncloud.org>`_ as a sync and share platform. The branded product is called `CERNBox <https://cernbox.web.cern.ch>`_. 

.. IMAGE:: cernbox.jpg
   :scale: 50%
   :align: center


Architecture Roadmap
--------------------

The target architecture for the next major release version is shown in the following figure:

.. IMAGE:: roadmap-2018.jpg
   :scale: 50%
   :align: center

The goal is to reach full scalability and high-availability of all service components and to embedd better external storage systems.






