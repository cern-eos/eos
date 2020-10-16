.. index::
   single: Ubuntu

.. _eos_ubuntu_install:

Debian/Ubuntu installation
==========================

The EOS client gets automatically built for recent Ubuntu releases,
currently "bionic" and "focal"

.. note::
   You need to add the XRootD and EOS repositories to your ``/etc/apt/sources.list`` (change distribution name as required)

.. code-block:: text

	deb [arch=amd64] http://storage-ci.web.cern.ch/storage-ci/debian/xrootd/ bionic release
	deb [arch=amd64] http://storage-ci.web.cern.ch/storage-ci/debian/eos/citrine/ bionic tag


Install EOS client via apt
--------------------------

.. code-block:: text

   curl -sL http://storage-ci.web.cern.ch/storage-ci/storageci.key | sudo apt-key add -
   sudo apt update
   sudo apt install eos-client eos-fusex

In case EOS access as filesystem is wanted, EOS-FUSEX needs then to be
configured as per
https://gitlab.cern.ch/dss/eos/-/blob/master/fusex/README.md
