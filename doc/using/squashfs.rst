.. highlight:: rst

.. _squashfs:

Using SquashFS images for software distribution
===============================================

EOS provides support for SquashFS image files, which can be automatically mounted when the image path is traversed. This functioality requires an appropriate automount configuration.

To create SquashFS images a client needs the EOS shell and a local mount with an identical path prefix as inside the client shell.
This means e.g. both commands as shown here point to the same directory:

.. code-block:: bash

   # access inside the shell
   eos ls -la /eos/foo/bar
   # access using the FUSE mount
   ls -la /eos/foo/bar


To really have read-only access to the  contents of SquashFS images, clients have to install the package **cern-eos-autofs-squashfs**.

All functionality of the SqashFS CLI is displays using the help option:

.. code-block:: bash

   eos squash -h


The functionality can be grouped into two categories:

* Simple SquashFS packages
* Release SquashFS packages


Simple SquashFS Packages
------------------------

A simple SquashFS package consists of a symbolic link under the package path and a hidden package file in the same directory as the symbolic link.

The workflow to create a SquashFS package is shown here:

Create a new package
++++++++++++++++++++
.. code-block:: bash

   [root@dev ]# eos mkdir -p /eos/dev/squash/
   [root@dev ]# eos squash new /eos/dev/squash/mypackage
   info: ready to install your software under '/eos/dev/squash/mypackage'
   info: when done run 'eos squash pack /eos/dev/squash/mypackage' to create an image file and a smart link in EOS!

   # see what happend - we have created a symbolic link in EOS with the package pathname and the link points to a local stage directory in /var/tmp/...
   [root@dev ]# eos ls -la /eos/dev/squash/
   drwxrwxrw+   1 root     root               59 May 27 13:32 .
   drwxrwxrw+   1 root     root       4751231651 May 27 13:32 ..
   lrwxrwxrwx   1 nobody   nobody             59 May 27 13:32 mypackage -> /var/tmp/root/eosxd/mksquash/..eos..dev..squash..mypackage/


Install software into a package
+++++++++++++++++++++++++++++++

.. code-block:: bash

   # install software into the package, de facto we work on the local disk under /var/tmp/...
   [root@dev ]# cd /eos/dev/squash/mypackage/
   [root@dev ]# touch HelloWorld

Pack a new package
++++++++++++++++++

.. code-block:: bash

   # pack the new package
   [root@dev ]# eos squash pack /eos/dev/squash/mypackage

   # see what happend - the symlink in EOS with the package pathname points to an encoded loction for the hidden package file .mypackage.sqsh
   [root@dev ]# eos ls -la /eos/ajp/squash/
   drwxrwxrw+   1 root     root             4161 May 27 13:38 .
   drwxrwxrw+   1 root     root       4751235753 May 27 13:32 ..
   -rw-r--r--   2 nobody   nobody           4096 May 27 13:38 .mypackage.sqsh
   lrwxrwxrwx   1 nobody   nobody             65 May 27 13:38 mypackage -> /eos/squashfs/ajp.cern.ch@---eos---ajp---squash---.mypackage.sqsh


If you try to use or access a package on a diffrent client machine before you call **eos squash pack** you will get errors on clients, because the symbolic link points to a non-existing local directory as long as a package is not closed.

In general you have to treat SquashFS packages as write-once archives. There is the possiblity to unpack a packed archive, modify and re-pack, however this is problematic if a package is already accessed on other clients using the automount mechanism. They won't remount an updated package automatically unless the mount is removed by idle timeouts and re-mounted later.


Package information
++++++++++++++++++


For completeness here are the commands to get information about a package:

.. code-block:: bash

   [root@dev ]# eos squash info /eos/dev/squash/mypackage
   info: '/eos/dev/squash/.mypackage.sqsh' has a squashfs image with size=4096 bytes
   info: squashfs image is currently packed - use 'eos squash unpack /eos/dev/squash/mypackage' to open image locally


Unpackaging
+++++++++++

As mentioned you can unpack an existing package:


.. code-block:: bash

   [root@dev ]# eos squash unpack /eos/ajp/squash/mypackage
   ...
   info: squashfs image is available unpacked under '/eos/dev/squash/mypackage'
   info: when done with modifications run 'eos squash pack /eos/dev/squash/mypackage' to create an image file and a smart link in EOS!


And pack it again:

.. code-block:: bash

   # pack the new package
   [root@dev ]# eos squash pack /eos/dev/squash/mypackage

Deleting a package
++++++++++++++++++

To delete a SquashFS package you run:

.. code-block:: bash

   # delete a package
   [root@dev ]# eos squash rm /eos/dev/squash/mypackage


Relabeling a package
++++++++++++++++++++

If a SquashFS package and/or package files has been moved around in the namespace e.g. by doing this ...

.. code-block:: bash

   [root@dev ]# eos mv /eos/dev/squash/ /eos/dev/newsquash/

the package links are broken. In this case one has to relabel the package doing:

.. code-block:: bash

   [root@dev ]# eos squash relabel /eos/dev/newsquash/mypackage


Remote web installation of packages
++++++++++++++++++++++++++++++++++++

The CLI provides a convenience function to install a .tar.gz package from a web URL:

.. code-block:: bash

   [roo@dev ]# eos squash install --curl=https://root.cern/download/root_v6.24.00.Linux-centos7-x86_64-gcc4.8.tar.gz /eos/dev/newsquash/root

After successful execution the software package is ready for use and no further packaging commands are required.

If you have the automounter RPM installed on your client you are ready to use the software:

.. code-block:: bash

   cd /eos/dev/newsquash/root/
   ...


Release SquashFS Packages
-------------------------

The **simple** package functionality is sufficient, if properly used. Many times you want to deal with updates and new release/versions of software. In this case the **release** functionality is preferable.

Creating a new release package
++++++++++++++++++++++++++++++

Release package management is illustrated in the following:

.. code-block:: bash

   [root@dev ]# eos squash new-release /eos/dev/release/mypackage
   info: ready to install your software under '/eos/dev/release/mypackage/.archive/mypackage-20210527135506'
   info: when done run 'eos squash pack /eos/dev/release//mypackage/.archive/mypackage-20210527135506' to create an image file and a smart link in EOS!
   info: install the new release under '/eos/dev/release/mypackage/next'


This new release is now locally available under **/eos/dev/release/mypackage/next**. You can install your software to this location and then call

Packing a new release package
+++++++++++++++++++++++++++++++

.. code-block:: bash

   [root@dev ]# eos squash pack-release /eos/dev/release/mypackage
   ...
   info: new release available under '/eos/ajp/squash/mypackage/current'

Now we have published the latest version of our release under **/eos/dev/release/mypcakge/current**. Our package name is in the release management mode a directory containing a **current** link, if there is an open new release a **next** link and a hidden **.archive** directory, where all versions of a release are stored.

By default a release is created with the unix timestamp during **new-release**. For most people it might be more convenient to specify a version number. In this case you call:

.. code-block:: bash

   [root@dev ]# eos squash new-release /eos/dev/release/mypackage v1.0.0
   ...
   [root@dev ]# eos squash pack-release /eos/dev/release/mypackage
   [root@dev ]# eos squash new-release /eos/dev/release/mypackage v1.1.0
   ...
   [root@dev ]# eos squash pack-release /eos/dev/release/mypackage

Release Package Information
+++++++++++++++++++++++++++

You can obtain information about all available versions/releases doing:

.. code-block:: bash

   [root@dev ]# eos squash info-release /eos/dev/release/mypackage
   ---------------------------------------------------------------------------
   - releases of '/eos/ajp/squash/mypackage'
   ---------------------------------------------------------------------------
   /eos/dev/squash/mypackage/.archive/mypackage-v1.0.0
   /eos/dev/squash/mypackage/.archive/mypackage-v1.1.0
   /eos/dev/squash/mypackage/current
---------------------------------------------------------------------------

The output shows two versions in the **archive** and the **current** link.

Trimming Release Packages
+++++++++++++++++++++++++++

If you regulary build software releases, you want to limit the number of versions, which are kept.

You can trim your softare releases using:

.. code-block:: bash

   [root@dev ]# eos squash trim-release /eos/dev/release/mypackage 100

This commmand will keep only versions not older than 100 days.

Additionally you can specifiy the maximum number of versions to keep:

.. code-block:: bash

   [root@dev ]# eos squash trim-release /eos/dev/release/mypackage 100 10

In this case we don't want to keep more than the 10 most recents versions, not older than 100 days.

Deleting Release Packages
+++++++++++++++++++++++++

For completeness, there is a command to cleanup a release packge. Be aware, that this will deleted all your release versions!

.. code-block:: bash

   [root@dev ]# eos squash rm-release /eos/dev/release/mypackage
   ---------------------------------------------------------------------------
   - releases of '/eos/dev/release/mypackage'
   ---------------------------------------------------------------------------
   /eos/dev/release/mypackage/.archive/mypackage-v1.0.0
   /eos/dev/release/mypackage/.archive/mypackage-v1.1.0
   /eos/dev/release/mypackage/current
   ---------------------------------------------------------------------------
   info: wiping squashfs releases under '/eos/dev/release/mypackage'
   info: wiping links current,next ...
   info: wiping archive ...

The main difference between simple and release packages is, that you can create new release while the previous one is in use on any other client.
