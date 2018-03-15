.. index::
   single: Uboxed

.. _eos_base_uboxed:

.. _uboxed: https://github.com/cernbox/uboxed

Scientific Services Installation: EOS, CERNBox, SWAN and CVMFS
==============================================================

We have bundled a demonstration setup of four CERN developed cloud and anlysis platform services called `UBoxed <https://github.com/cernbox/uboxed>`_. It encapsulates four compontents:

- `EOS <http://eos.cern.ch>`_ - scalable storage platform with data, metadata and messaging server components
- `CERNBox <https://cernbox.web.cern.ch>`_ - dropbox-like add-on for sync-and-share services on top of EOS
- `SWAN <https://swan.web.cern.ch>`_ - service for web based interactive analysis with jupyter notebook interface
- `CVMFS <https://cvmfs.web.cern.ch>`_ - CernVM file system - a scalable software distribution service


Preparation
-----------

The setup scripts will install all required packages. 

.. note::
   Make sure you have no other web server listening on the standard ports. Make sure you have atleast 30GB of free space under ``/var/lib/docker/``.

.. note::
   The installation requires atleast docker version 17.03 - if you have an older one we recommend to uninstall it let
   the setup script take care of pulling a newer version.
   
.. note::
   In certain environments docker container cannot resolve external domain addresses because nameserver accesss to the default nameserver 8.8.8.8 is blocked. To fix this create a daemon configuration file ``etc/docker/daemon.json`` and restart the docker daemon
   For the CERN DNS server that would be e.g.

.. code-block:: bash

   cat /etc/docker/daemon.json
   {
     "dns" : ["137.138.17.5", "137.138.17.5"]
   }

   // el7
   systemctl restart docker

Quick Setup
-----------

Checkout the `uboxed <https://github.com/cernbox/uboxed>`_ project:

.. code-block:: bash

   git clone https://github.com/cernbox/uboxed
   cd uboxed

Install Services
++++++++++++++++

The platform dependent installation script will pull required software and install docker images for the four service components. The procedure is validated on CentOS 7 and Ubuntu platforms. The installation will take few minutes depending on your environment.

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

Open https://localhost in a local browser or connect to your docker host machine with with a remote browser and HTTPS. You will land on the **Uboxed** main page which directs you to documentation and how to try the individual services running in your container setup.


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
