.. index::
   single: Installation

.. _eos_base_install:

RPM installation
================

.. image:: rpm.jpg
   :scale: 50%
   :align: left

|
|


.. note::
   You need to add the XRootD and EOS repositories.
   Please follow instruction from :ref:`eos_base_setup_repos`



Install EOS via yum
-------------------

For client

.. code-block:: text

   yum install eos-client

For server

.. code-block:: text

   yum install eos-server eos-client eos-testkeytab eos-fuse jemalloc

For HTTP server

.. code-block:: text

   yum install eos-nginx

For GridFTP server

   yum install xrootd-dsi

For EOS test on MGM (namespace node)

.. code-block:: text

   yum install eos-test
