.. index::
   single: Hardware

.. highlight:: rst

.. _hardwareinstallation:

Hardware Requirements
=====================

.. image:: cerncc.jpg
   :align: center
   :width: 800px

.. index::
   pair: Hardware; MGM

MGM Node
---------
MGM run heavily multithreaded code. It can be run on a single CPU core, we recommend at least 6-8 CPU cores for a production system. The memory consumption is tunable via thread pool settings and the number of cached namespace entries. The absolute minimum is 8 GB, we recommend at least 32 GB. The disk requirements for an MGM node are to store log files and file access reports. The required size depends essentially on the usage pattern of your EOS instance. HDDs are ok, however we experienced in production systems, that it is better to use SSDs since they don't lead to hick-ups and D state processes during log-rotation.

+-------------+----------+--------+----------------------+
| Type        |  CPU     | MEMORY |  DISK                |
+=============+==========+========+======================+
| Minimum     | 1 core   | 8 GB   | 8 GB HDD             |  
+-------------+----------+--------+----------------------+
| Recommended | 6-8 core | 32 GB  | 128 GB HDD           |
+-------------+----------+--------+----------------------+
| Performance | 32 core  | 128 GB | 512 GB SSD           |
+-------------+----------+--------+----------------------+

Example configurations from large CERN deployments:

.. code-block:: 

   Intel(R) Xeon(R) Silver 4216 CPU @ 2.10GHz (32 core), 386 GB RAM (2933 MHz DDR4), 1 TB /var partition

.. index::
   pair: Hardware; QDB

QDB Node
--------

QDB nodes run RocksDB as a KV store. A QDB node requires IOPS for a cold start, so it should be deployed using SSDs/NVMEs. A rule of thumb is to calculate 0.1-0.2 GB of space per 1M namespace entries e.g. a namespace with up to 1 Billion entries should have 100-200GB of disk space.
To run backups there is not much extra space needed. The memory footprint of the QDB daemon is tiny. RAM on a QDB node is useful to cache RocksDB files (RAFT journals) in the buffer cache. Ideally you have as much memory as the maximum expected volume of QDB. 

+-------------+----------+--------+----------------------+
|             | CPU      | MEMORY | DISK                 |
+=============+==========+========+======================+
| Minimum     | 1 core   | 8 GB   | 0.1-0.2  GB/Million  |
+-------------+----------+--------+----------------------+
| Performance | 4 core   | DB size| 0.1-0.2  GB/Million  |
+-------------+----------+--------+----------------------+

To save resources you can run MGM and QDB nodes co-located (both daemons on one node) taking into account the disk and memory requirements of both daemons.

Example configurations from large CERN deployments:

.. code-block:: 

   Intel(R) Xeon(R) Silver 4216 CPU @ 2.10GHz (32 core), 386 GB RAM (2933 MHz DDR4), 2x 1.8 TB /var partition (INTEL SSDSC2KB01)


.. index::
   pair: Hardware; FST

FST Node
--------

Diskspace in EOS is provided via mounted filesystems on FST nodes. The requirements to the mounted filesystem is to have **extended attribute support**! **atime** can be disabled.

### Supported Back-end Filesystems
Examples of filesystems suitable as storage volumes:

* **XFS** on **JBOD** HDD (physical drive)
* **XFS** on **RAID** array (hardware or software RAID)
* **XFS** on **RBD** (virtual drive)
* **CephFS**, **Lustre**, **GPFS**

**ZFS** is possible, but **not recommended** due to worse random IO performance.

For installations with very few physical nodes, we recommend to use a conventional **RAID6** configuration. 

If there is a sufficient number of nodes available (8), we recommend to use **EOS erasure coding** to provide high-availability and durability of data. 

If you run EOS in an **OpenStack** environment, you might use virtual drives or a distributed filesystem underneath. Be aware, that virtualized setups might show unexpected intrinsic IO bottlenecks (IOPS,Bandwidth).

Hardware
~~~~~~~~

For JBOD storage we recommend  1GB of RAM per disk. The memory serves mainly to improve performance using the LINUX buffer cache.

The CPU usage on FST nodes is low, if replication is used. For erasure coding a modern CPU with SIMD extension should be available.

Example configuration of a **small FST** with replication and JBODs:

.. code-block:: 

   Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz, 128 GB RAM (2400 MHz, DDR4), 48x TOSHIBA MG04ACA6(6TB)

Example configuration of a **large FST** for erasure-coding and JBODs:

.. code-block:: 

   2x AMD EPYC 7302 16-Core Processor, 128 GB RAM (3200 MHz, DDR4), 96x TOSHIBA MG07ACA1 (14TB)



.. index::
   single: Installation

Installation
============
EOS is usually installed from package repositories. For installation from sources, follow :ref:`Develop`.

Supported Platforms
-------------------

EOS for Client+Server
~~~~~~~~~~~~~~~~~~~~~

The following platforms are supported to run EOS client and server:
* CentOS 7
* Alma 8
* Alma 9
* Fedora Core 38

RPM Repository Configuration
""""""""""""""""""""""""""""

You have to configure a dependency repository and either the tag or commit repository (master branch).

.. figure:: yum.jpg
   :align: center
   :figwidth: 480px

.. index::
   pair: YUM; Packages


**Dependency Repository for Tag and Commit Releases**

+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Platform        | Setup                                                                                                              |
+=================+====================================================================================================================+
| CentOS7         | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-7/x86_64/"``     |
+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Alma 8          | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-8/x86_64/"``     |
+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Alma 9          | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-9/x86_64/"``     |
+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Fedora 38       | https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/fc-38/                                               |
+-----------------+--------------------------------------------------------------------------------------------------------------------+

**Tag Releases**

+-----------------+-----------------------------------------------------------------------------------------------------------------------+
| Platform        |  Setup                                                                                                                |
+=================+=======================================================================================================================+
| CentOS7         | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-7/x86_64/"``   |
+-----------------+-----------------------------------------------------------------------------------------------------------------------+
| Alma 8          | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-8/x86_64/"``   |
+-----------------+-----------------------------------------------------------------------------------------------------------------------+
| Alma 9          | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"``   |
|                 |                                                                                                                       |
+-----------------+-----------------------------------------------------------------------------------------------------------------------+
| CentOS9 Stream  | https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/fc-38/x86_64/                                      |
+-----------------+-----------------------------------------------------------------------------------------------------------------------+


**Commit Releases**

+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Platform        |  Setup                                                                                                             |
+=================+====================================================================================================================+
| CentOS7         | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/commit/el-7/x86_64/"``     |
+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Alma 8          | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/commit/el-8/x86_64/"``     |
+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Alma 9          | ``yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/commit/el-9/x86_64/"``     |
+-----------------+--------------------------------------------------------------------------------------------------------------------+
| Fedora 38       | https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/fc-38/x86_64/                                   |
+-----------------+--------------------------------------------------------------------------------------------------------------------+

Client+FUSE RPM Installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   yum install eos-client eos-fusex

Server RPM Installation
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   yum install eos-server

