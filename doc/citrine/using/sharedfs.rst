.. highlight:: rst

.. _sharedfs:

Using a shared filesystem as FST backend
========================================

The EOS FST server can be configured to store data on any (shared) filesystem as storage device which supports extended attributes. To avoid that filesystems sharing a device or a shared filesystem are accounted multiple times in the space and node aggregation, one can label each filesystem with a shared filesystem name to indicated that all devices using this name share the same hardware resource.

The shared filesystem name can be configured when a filesystem is added e.g. a CephFS filesystem names cephfs1

.. code-block:: bash

   fs add 7a41781f-62dc-4f18-8f64-375e57487578 foo.cern.ch /cephfs/ default rw cephfs1

If filesystems are already registered or the filesystem name has changed one can use the filesystem config command:

.. code-block:: bash

   fs config 1 sharedfs=cephfs1

