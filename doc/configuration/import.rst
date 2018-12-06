.. highlight:: rst

.. index::
      triple: Import; HTTP; S3

Importing files
===============

EOS can use multiple protocols when communicating with a storage device.
Most commonly, the data is stored on local disks. However, data can also be
stored on an external storage system, such as an S3, WebDAV or XRootD endpoint.
More information can be found `here <configuration/logicalpath>`__.

Importing files works with any underlying storage device
as file importation is a filesystem operation.
This means the endpoint must be mapped and accessible
through an EOS filesystem.

The importation procedure does a scan of the remote path,
recursively traversing all directories from this point on.
All discovered files will be imported into the namespace,
together with their size and time of creation/modification.
Upon registering the files into the namespace,
they can be managed like any other EOS file.

.. note::
   File importation is only a metadata operation.
   The files themselves will not be moved.

Configuration
-------------

Preconditions
+++++++++++++

To set-up the importation procedure, a filesystem must be
associated with that particular endpoint.

Registering a filesystem which uses an external endpoint for storage
is done the same way. For example, to import from ``s3://s3.cern.ch/bucket``,
we would do the following:

.. code-block:: bash

   eos fs add [-m 1] <uuid> s3-fst.cern.ch:1095 s3://s3.cern.ch/bucket/ s3 rw

Once registered, make sure the filesystem is online:

.. code-block:: bash

   ┌────────────────────────┬────┬──────┬──────────────────────────────────────────┬────────────────┬────────────────┬────────────┬──────────────┬────────────┬────────┬────────────────┐
   │host                    │port│    id│                                      path│      schedgroup│          geotag│        boot│  configstatus│ drainstatus│  active│          health│
   └────────────────────────┴────┴──────┴──────────────────────────────────────────┴────────────────┴────────────────┴────────────┴──────────────┴────────────┴────────┴────────────────┘
    s3-fst.cern.ch           1095      1                    s3://s3.cern.ch/bucket/             s3.0              xdc       booted             rw      nodrain   online              N/A

Filesystem set-up
+++++++++++++++++

Although not mandatory, configuring the filesystem to use `logicalpath <configuration/logicalpath>`__
will be needed in most cases.

.. code-block:: bash

   eos fs config <fsid> logicalpath=1

Depending on the remote endpoint, additional configuration might be needed.

S3
##

FSTs with S3 filesystems registered will need an access/secret key pair.
These can be done on a filesystem basis or FST wide.

To set keys only for a particular, use the ``fs config`` option:

.. code-block:: bash

   eos fs config <fsid> s3credentials=<accesskey>:<secretkey>

To set keys FST wide, export the following environment variables:

.. code-block:: bash

   export EOS_FST_S3_ACCESS_KEY=<accesskey>
   export EOS_FST_S3_SECRET_KEY=<secretkey>

WebDAV with x509
################

To access WebDAV storage endpoints, you will need a way to authenticate.
Support is provided for x509 certificates.

The setting applies FST wide and is retrieved
from the following environment variable:

.. code-block:: bash

   export EOS_FST_HTTPS_X509_CERTIFICATE_PAT=/path/to/x509/certificate

Import procedure
----------------

The import procedure is triggered via the `fs import start` admin command,
which is documented `here <clicommands/fs>`__.

It offers the option to start an import procedure
or to check the status of an ongoing import.

When starting a new import operation, the command is sent to the MGM
which does the following checks:

- external path begins with the filesystem local prefix
- destination path is a directory
- destination path is in same scheduling group as the filesystem

A new id, together with an import status object
are generated for this import operation.

A signal is sent to the responsible FST,
which will place the import request in a queue.

Import requests are processed on a dedicated thread.
This will do a traversal of the endpoint path.
For each file discovered, a stat is performed to retrieve
needed information. This info is encoded into a message
which is sent back to the MGM.

The MGM receives this message, updates the namespace
and the import status object.
