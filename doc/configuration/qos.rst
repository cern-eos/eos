.. highlight:: rst

.. index::
   single: QoS Interface

QoS Interface
=============

EOS provides a QoS interface to manipulate storage properties of files.
The interface is composed of the following components:
  - QoS Class
  - QoS API
  - QoS CLI

QoS Class
---------

A QoS class is an abstraction of the following storage parameters:
  - layout id
  - replication level
  - checksum type
  - placement policy

QoS classes also offer certain guarantees, such as latency or redundancy provided,
and list the legal transitions to other QoS classes.

QoS configuration
+++++++++++++++++

Within the system, they are defined via a QoS definition file,
which gets loaded at MGM boot-up.

The following config settings control the QoS behavior:

.. code-block:: bash

  # xrd.cf.mgm
  mgmofs.qosdir /var/eos/qos/          # QoS directory
  mgmofs.qoscfg /var/eos/qos/qos.conf  # location of QoS config file

  # Environment variable
  EOS_ENABLE_QOS=""                    # enable QoS if defined

QoS definition file
+++++++++++++++++++

QoS classes are defined using JSON and have the following structure:
  - name
  - allowed transitions to other QoS classes
  - metadata guarantees
  - storage parameters

Example of a definition file:

.. code-block:: bash

  {
    "disk_plain": {
      "name": "disk_plain",
      "transition": [
        "disk_replica"
      ],
      "metadata": {
        "cdmi_data_redundancy_provided": 0,
        "cdmi_geographic_placement_provided": [
          "CH"
        ],
        "cdmi_latency_provided": 75
      },
      "attributes": {
        "layout": "plain",
        "replica": 1,
        "checksum": "adler32",
        "placement": "scattered"
      }
    },

    "disk_replica": {
      "name": "disk_replica",
      "transition": [
        "disk_plain"
      ],
      "metadata": {
        "cdmi_data_redundancy_provided": 1,
        "cdmi_geographic_placement_provided": [
          "CH"
        ],
        "cdmi_latency_provided": 75
      },
      "attributes": {
        "layout": "replica",
        "replica": 2,
        "checksum": "adler32",
        "placement": "scattered"
      }
    }
  }

QoS API
-------

The QoS API allows the following operations to be performed:
  - List available QoS classes
  - Retrieve the QoS class of an entry
  - Setting the QoS class of an entry

Listing QoS classes
+++++++++++++++++++

The operation is performed via the CLI and will list all the QoS classes
loaded on the system. Listing a given QoS class will print the class definition.

Example:

.. code-block:: bash

  > eos -j qos list | jq .
  {
    "name": [
      "disk_plain",
      "disk_replica"
    ]
  }

  > eos -j qos list disk_replica | jq .
  {
    "attributes": {
      "checksum": "adler32",
      "layout": "replica",
      "placement": "scattered",
      "replica": "2"
    },
    "metadata": {
      "cdmi_data_redundancy_provided": 1,
      "cdmi_geographic_placement_provided": [
        "CH"
      ],
      "cdmi_latency_provided": 75
    },
    "name": "disk_replica",
    "transition": [
      "disk_plain"
    ]
  }

Retrieving QoS class of an entry
++++++++++++++++++++++++++++++++

The QoS class of an entry is identified at runtime.
For files, this means inspecting the relevant storage parameters.

For directories, this means extracting the relevant storage parameters
from the directory's extended attributes.

Example:

.. code-block:: bash

  > eos -j qos get /eos/xdc/qos/file
  {
    "attributes": {
      "checksum": "adler32",
      "layout": "plain",
      "placement": "scattered",
      "replica": "1"
    },
    "current_qos": "disk_plain",
    "disksize": "100000000",
    "id": "40092",
    "metadata": {
      "cdmi_data_redundancy_provided": "0",
      "cdmi_geographic_placement_provided": [
        "CH"
      ],
      "cdmi_latency_provided": "75"
    },
    "path": "/eos/xdc/qos/file",
    "size": "100000000"
  }

Setting QoS class of an entry
+++++++++++++++++++++++++++++

Setting the QoS class of a directory implies changing the storage-related
extended attributes.

Setting the QoS class of a file implies changing that file's storage parameters.
This is done by scheduling a conversion job and setting the following
extended attribute on the file: `user.eos.qos.target=<qos_class>`.

Example:

.. code-block:: bash

  > eos -j qos set /eos/xdc/qos/file disk_replica
  {
    "conversionid" : "0000000000009c9c:default#00650112~scattered",
    "retc" : 0
  }

  > eos -j qos get /eos/xdc/qos/file | jq .
  {
    "attributes": {
      "checksum": "adler32",
      "layout": "plain",
      "placement": "scattered",
      "replica": "1"
    },
    "current_qos": "disk_plain",
    "disksize": "100000000",
    "id": "40092",
    "metadata": {
      "cdmi_data_redundancy_provided": "0",
      "cdmi_geographic_placement_provided": [
        "CH"
      ],
      "cdmi_latency_provided": "75"
    },
    "path": "/eos/xdc/qos/file",
    "size": "100000000",
    "target_qos": "disk_replica"
  }

  > eos attr ls /eos/xdc/qos/file/
  user.eos.qos.target="disk_replica"

When the file is successfully converted, the target QoS extended attribute is removed.

Querying this extended attribute is one way to find out whether the QoS transition
took place. However, for more details, it is recommended
to use the provided conversion id.

QoS CLI
=======

An `eos qos` command is provided for CLI interaction.
More information may be found in the :ref:`clientcommands` section.

.. note::

  All QoS CLI commands also provide JSON output.
