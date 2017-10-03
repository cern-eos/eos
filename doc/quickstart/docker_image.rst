.. index::
   single: Docker Image

.. _eos_base_docker_image:

.. _docker: https://docs.docker.com/

Docker images for EOS
======================

EOS also provides docker images with all the necessary components installed and ready to use.

You can get the image both for each build (mostly for automated testing and debugging purposes) and for each release.
The release images are tagged with the release version and the regular images are tagged with the build id they source from.

The images are accessible from the project's `registry <https://gitlab.cern.ch/dss/eos/container_registry>`_.

Download the image
-------------------

Make sure you have docker_ installed and started on your system.

.. code-block:: text

   docker pull gitlab-registry.cern.ch/dss/eos:<tag>

Example for a build

.. code-block:: text

   docker pull gitlab-registry.cern.ch/dss/eos:206970

Example for a release

.. code-block:: text

   docker pull gitlab-registry.cern.ch/dss/eos:4.1.31

Run an instance
-------------------

There is already `project <https://gitlab.cern.ch/eos/eos-docker>`_ which can help you use this image and run EOS components inside containers.

To start a small instance with 6 FST, 1 MGM, 1 MQ, 1 client and 1 Kerberos KDC containers ready to use,
all you have to do is to use the `start_services <https://gitlab.cern.ch/eos/eos-docker/blob/master/scripts/start_services.sh>`_ script.
You have to give the name of the image to use with the -i and the number of FST containers (default is 6) with the -n flags.

The containers will reside on the same network knowing about each other and configured to be working out-of-the-box.

.. code-block:: text

   start_services.sh -i gitlab-registry.cern.ch/dss/eos:4.1.31 [-n 6]

You can then start using the instance, for example by mounting EOS to the client container using FUSE and KRB5 authentication.

.. code-block:: text

   docker exec -i eos-client-test env EOS_MGM_URL=root://eos-mgm-test.eoscluster.cern.ch eos fuse mount /eos

Or by running test scripts

.. code-block:: text

   docker exec -i eos-mgm-test eos-instance-test

There is also a `shutdown_services <https://gitlab.cern.ch/eos/eos-docker/blob/master/scripts/shutdown_services.sh>`_ script for removing these containers from your system.

.. code-block:: text

   shutdown_services.sh

In case you would like to create a different setup, you are welcome to browse and reuse the provided scripts under
the `image_scripts <https://gitlab.cern.ch/eos/eos-docker/tree/master/image_scripts>`_ directory to get an idea on how to do it.