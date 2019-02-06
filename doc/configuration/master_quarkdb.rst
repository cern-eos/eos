.. highlight:: rst

.. index::
   single: Master/Slave QuarkDB Configuration

Master/Slave QuarkDB Configuration
===================================

The Citrine release starting with version (4.4.0) adds support for single master,
multiple slave setup when used with a QuarkDB backend. The master election happens
automatically by using the "lease" functionality provided by QuarkDB.

How it works
------------

When an MGM node is started, it will connect to the QuarkDB backend and try to
acquire the "master_lease". If it successfully acquires it then the current
node becomes the master. The lease is acquired for a period of 10 seconds. The
current MGM will renew the lease every 5 seconds. The lease has two important
pieces of information attached to it: the current owner of the lease and the
validity of the lease.

When another MGM is started, it will try to acquire the same lease and fail. At
this point the new MGM is marked as a slave and will also know who the current
master is, therefore redirecting any "write" traffic to the correct master MGM.

Export instance configuration to QuarkDB
----------------------------------------

In order to have the master-slave setup properly working the configuration of the
EOS instance needs to also be stored in QuarkDB. This is done by setting the
following configuration directive in the ``/etc/xrd.cf.mgm`` file:

.. code-block:: bash

   mgmofs.cfgtype quarkdb

The configuration will be stored in the same QuarkDB cluster as the namespace
information. Therefore, the following directives are used by both the namespace
implementation and the configuration engine:

.. code-block:: bash

  mgmofs.qdbcluster eos-mgm-1.cern.ch:7777 eos-mgm-2.cern.ch:7777 eos-mgm-3.cern.ch:7777
  mgmofs.qdbpassword_file /etc/eos.keytab

Start the MGM and use the ``eos config export <path_to_config_file>`` to export the
configuration stored in the pointed file to the current instance of QuarkDB. This
should create a new key entry in QuarkDB with the following name ``eos-config:default``
for the **default** configuration.

Master-slave status
-------------------

The current status of the running MGM daemon can be monitored by using the ``eos ns``
command. The relevant part regarding the master-slave status is displayed in the
**Replication** section, e.g.:

.. code-block:: bash

   ALL      Replication                is_master=true master_id=eos-mgm-1.cern.ch:1094

This section details whether the current MGM is the master and also prints the
identity of the current master. For a slave MGM the output of the command looks
like the following:

.. code-block:: bash

   ALL      Replication                is_master=false master_id=eos-mgm-1.cern.ch:1094

For a slave MGM the namespace metadata cache is disabled. This is done to avoid
confusion for a user connected to the slave MGM and getting stalle information
- since it might have been updated by the master MGM. Therefore, all the metadata
on the slave is fetched directly from the QuarkDB backend.

Force master-slave transition
------------------------------

In order to force a master-slave transition it is sufficient to issue the following
command on the MGM master node: ``eos ns master other``. The "other" argument can
be replaced by anything else except the current master id. This will cause the current
MGM node not to update its lease therefore loosing its master status. The other
(slave) MGMs will now compete for the lease and only one of them will become the
new master.
