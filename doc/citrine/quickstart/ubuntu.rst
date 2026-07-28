.. index::
   single: Ubuntu

.. _eos_ubuntu_install:

Ubuntu installation
===================

The EOS client and FUSE packages are built and published for recent Ubuntu
releases:

.. list-table::
   :header-rows: 1

   * - Release
     - Codename
     - Architectures
   * - Ubuntu 22.04 LTS
     - ``jammy``
     - ``amd64``
   * - Ubuntu 24.04 LTS
     - ``noble``
     - ``amd64``, ``arm64``
   * - Ubuntu 26.04 LTS
     - ``resolute``
     - ``amd64``

.. note::
   Only Ubuntu packages are provided, there are no packages for Debian. For
   RPM based distributions see :ref:`eos_base_setup_repos`.

Repository layout
-----------------

The packages live in an APT repository rooted at
``http://storage-ci.web.cern.ch/storage-ci/ubuntu/eos/diopside``. There is one
*distribution* per Ubuntu release, named after its codename, and within a
distribution the packages are split into *components* by build type and by the
major.minor version of the EOS packages:

.. code-block:: text

   <codename>/tag/<major.minor>       official tagged releases
   <codename>/commit/<major.minor>    builds of individual commits
   <codename>/deps/<major.minor>      dependencies (XRootD, ...) for that series

Several EOS series are published side by side, so a client selects the one it
wants by listing the corresponding components. For example ``jammy/tag/5.4``
holds the 5.4 releases for Ubuntu 22.04, while ``jammy/tag/5.3`` is left
untouched by 5.4 publications.

.. note::
   The codename is repeated inside the component name on purpose: the pool is
   laid out under ``pool/<component>/`` and the same EOS version built for two
   Ubuntu releases produces identically named ``.deb`` files with different
   content, which cannot share one pool directory.

Setup the APT repository
------------------------

Install the few utilities needed to configure the repository:

.. code-block:: text

   sudo apt update
   sudo apt install -y curl gpg lsb-release

Import the GPG key used to sign the repository:

.. code-block:: text

   curl -sL http://storage-ci.web.cern.ch/storage-ci/storageci.key | \
     sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/storage-ci.gpg

Create the repository configuration. The snippet below picks up the codename
and the architecture of the machine automatically and subscribes to the tagged
5.4 releases together with their dependencies:

.. code-block:: text

   EOS_SERIES=5.4
   CODENAME=$(lsb_release -cs)
   echo "deb [arch=$(dpkg --print-architecture)] http://storage-ci.web.cern.ch/storage-ci/ubuntu/eos/diopside ${CODENAME} ${CODENAME}/tag/${EOS_SERIES} ${CODENAME}/deps/${EOS_SERIES}" | \
     sudo tee /etc/apt/sources.list.d/eos-client.list > /dev/null

For an ``amd64`` machine running Ubuntu 22.04 this expands to:

.. code-block:: text

   deb [arch=amd64] http://storage-ci.web.cern.ch/storage-ci/ubuntu/eos/diopside jammy jammy/tag/5.4 jammy/deps/5.4

Replace ``EOS_SERIES`` to follow a different series, or use the ``commit``
component instead of ``tag`` to track the packages built for every commit.

Install the EOS client
----------------------

.. code-block:: text

   sudo apt update
   sudo apt install -y eos-client eos-fusex

Mounting EOS with FUSE
----------------------

Create the local directory holding the mount points:

.. code-block:: text

   sudo mkdir -p /eos/

Each EOS instance to be accessed is described by its own configuration file in
``/etc/eos/``, named ``fuse.<name>.conf``, whose contents must be a valid JSON
object:

.. code-block:: text

   {"name": "<name>", "hostport": "<mgm host>", "remotemountdir": "<remote path>"}

For example, at CERN, for a user account ``userx`` whose data is stored in
CERNBox, ``/etc/eos/fuse.home-u.conf``:

.. code-block:: text

   {"name": "home-u", "hostport": "eoshome-u.cern.ch", "remotemountdir": "/eos/user/u/userx/"}

for a project ``asdf`` stored in the EOSPROJECT instance,
``/etc/eos/fuse.project-a.conf``:

.. code-block:: text

   {"name": "project-a", "hostport": "eosproject-a.cern.ch", "remotemountdir": "/eos/project/a/asdf/"}

and for the EOSCMS instance, ``/etc/eos/fuse.cms.conf``:

.. code-block:: text

   {"name": "cms", "hostport": "eoscms.cern.ch", "remotemountdir": "/eos/cms/"}

Managing the mounts with autofs
-------------------------------

With the configuration files in place, ``autofs`` can take care of mounting and
unmounting on demand:

.. code-block:: text

   sudo apt install -y autofs
   sudo systemctl status autofs

Create ``/etc/auto.eos`` listing one entry per mount point, referring to the
configuration files created above by their ``fsname``:

.. code-block:: text

   home-a    -fstype=eosx,fsname=home-a    :eosxd
   # ... one line per user letter
   home-z    -fstype=eosx,fsname=home-z    :eosxd
   project-a -fstype=eosx,fsname=project-a :eosxd
   # ... one line per project letter
   project-z -fstype=eosx,fsname=project-z :eosxd
   cms       -fstype=eosx,fsname=cms       :eosxd

Hook it into the autofs master map and restart the service:

.. code-block:: text

   echo "/eos /etc/auto.eos" | sudo tee /etc/auto.master.d/eos.autofs > /dev/null
   sudo systemctl restart autofs

The mount points are now managed automatically by the autofs daemon. They all
appear inside ``/eos/`` and are reached by appending the first column of
``/etc/auto.eos`` to it, so with the above configuration accessing
``/eos/home-u/`` displays the CERNBox contents of ``userx``.

Authentication
--------------

Kerberos is commonly used to authenticate against the mount points. Install the
Kerberos client:

.. code-block:: text

   sudo apt install -y krb5-user

and use ``kinit`` to obtain a ticket before accessing the mount points.

For the remaining configuration options of the EOS FUSE mount points see
https://gitlab.cern.ch/dss/eos/-/blob/master/fusex/README.md
