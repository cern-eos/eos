.. index::
   single: Uboxed

.. _eos_base_uboxed:

.. _uboxed: https://github.com/cernbox/uboxed

UBoxed Installation
=======================

`UBoxed <https://github.com/cernbox/uboxed` is a self-contained docker demo configuration for scientific and general purpose use. It encapsulates four compontents:

- `EOS <https://eos.cern.ch>` - storage and namespace server
- `CERNBox <https://cernbox.web.cern.ch>` - dropbox-like add-on for sync-and-share service on top of EOS
- `SWAN <https://swan.web.cern.ch>` - Service for Web based ANalsysi - Jupyter notebook interface
- `CVMFS <https://cvmfs.web.cern.ch` - CernVM file system - a scalable software distribution service


Preparation
-----------

The setup scripts will install all required packages.

.. note::

   Make sure you have no other web server listening on the standard ports.


Quick Setup
-----------

Checkout the `uboxed <https://github.com/cernbox/uboxed`_ project:

.. code-block:: bash

   git clone https://github.com/cernbox/uboxed
   cd uboxed

Install Services
++++++++++++++++

The platform dependent installation script will pull required software and install docker images for the four service components. This will take few minutes depending on your environment.

.. code-block:: bash
  // CentOS 7
  ./SetupInstall-Centos7.sh

  // Ubuntu
  ./SetupInstall-Ubuntu.sh

Setup and Initialize Services
+++++++++++++++++++++++++++++

The setup host script will configure and start all four service components and their corresponding containers.

.. code-block:: bash

   ./SetupHost.sh


Run a Self Test
+++++++++++++++

.. code-block:: bash

   ./TestHost.sh


Connect to your services
++++++++++++++++++++++++

Open https://localhost in a local browser or connect to your docker installation machine with a remote browser. 

Stop Services
-------------

If you started the self test container, first do:

.. code-block:: bash

   docker stop selftest
   docker rm selftest

To stop all Uboxed services do:

.. code-block:: bash

   ./StopBox.sh

Cleanup docker images and volumes
---------------------------------

If you want to remove all Uboxed images and volumes from your local docker installation, you do:

.. warning::
   This will delete all created user data!


.. code-block:: bash

   docker rmi cernbox cernboxgateway eos-controller eos-storage ldap swan_cvmfs swan_eos-fuse swan_jupyterhub selftest cernphsft/systemuser:v2.10 cern/cc7-base:20170920

.. code-block:: bash

   docker volume rm cernbox_shares_db ldap_config ldap_database eos-fst1 eos-fst1_userdata eos-fst2 eos-fst2_userdata eos-fst3 eos-fst3_userdata eos-fst4 eos-fst4_userdata eos-fst5 eos-fst5_userdata eos-fst6 eos-fst6_userdata eos-mgm eos-mq
