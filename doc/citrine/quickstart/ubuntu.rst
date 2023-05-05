.. index::
   single: Ubuntu

.. _eos_ubuntu_install:

Debian/Ubuntu installation
==========================

The EOS client gets automatically built for recent Ubuntu releases,
currently "focal" and "jammy"

You **might** need to have the following packages installed to setup the installation of eos client.

.. code-block:: text

    apt update && apt upgrade
    apt install -y sudo lsb-release curl gnupg2 

.. note::
    You need to add the XRootD and EOS repositories to your ``/etc/apt/sources.list``.

    .. code-block:: text

        echo "deb [arch=$(dpkg --print-architecture)] http://storage-ci.web.cern.ch/storage-ci/debian/xrootd/ $(lsb_release -cs) release" | sudo tee -a /etc/apt/sources.list.d/cerneos-client.list > /dev/null
        echo "deb [arch=$(dpkg --print-architecture)] http://storage-ci.web.cern.ch/storage-ci/debian/eos/diopside/ $(lsb_release -cs) tag" | sudo tee -a /etc/apt/sources.list.d/cerneos-client.list > /dev/null

    The above snippet will automatically get "arch" and "release" information for your machine (otherwise, just change arch and distribution name as required).

    e.g., for a "amd64" machine with ubuntu "jammy" that would be

    .. code-block:: text

        deb [arch=amd64] http://storage-ci.web.cern.ch/storage-ci/debian/xrootd/ jammy release
        deb [arch=amd64] http://storage-ci.web.cern.ch/storage-ci/debian/eos/diopside/ jammy tag

.. note::
    Also, to avoid possible conflicts with other releases you need to version-lock xrootd dependency packages (this will we softened in future releases).

    e.g, as of eos version 5.1.5, you need to version-lock xrootd to 5.5.1:

    .. code-block:: text

        echo -e "Package: xrootd* libxrd* libxrootd*\nPin: version 5.5.1\nPin-Priority: 1000" > /etc/apt/preferences.d/xrootd.pref

Install EOS client via apt
--------------------------

Once the repository are properly configured, you can simply run

.. code-block:: text
   curl -sL http://storage-ci.web.cern.ch/storage-ci/storageci.key > /tmp/storageci.key
   gpg --no-default-keyring --keyring ./storageci_keyring.gpg --import /tmp/storageci.key
   gpg --no-default-keyring --keyring ./storageci_keyring.gpg --export | sudo tee /etc/apt/trusted.gpg.d/storageci.gpg > /dev/null
   sudo apt update
   sudo apt install eos-client eos-fusex

In case EOS access as filesystem is wanted, EOS-FUSEX needs then to be
configured as per
https://gitlab.cern.ch/dss/eos/-/blob/master/fusex/README.md
