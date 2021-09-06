.. index::
   single: Admin Configuration

.. _eos_admin_configure:

EOS admin configuration
=======================

EOS distinguished two type of nodes:

* the MGM node (namespace node)
* the FST nodes (storage nodes)

.. warning:: This quickstart skips installing a QuarkDB instance for brevity.  In EOS5+, QuarkDB is required and it is highly suggested for most deployments in EOS4 due to the memory and time requirements for large namespaces.  See the conventional deployment for details at https://eos-docs.web.cern.ch/develop.html#deployment.

Copy an example config file to /etc/sysconfig/eos

.. code-block:: bash

   // SysV flavour
   cp /etc/sysconfig/eos.example /etc/sysconfig/eos
   // systemd flavour
   cp /etc/sysconfig/eos_env.example /etc/sysconfig/eos_env

Setup MGM
---------
Change the following variables in /etc/sysconfig/eos[_env]

Define three roles of daemons to run on this node:

.. code-block:: bash

   export XRD_ROLES="mq mgm fst"

In this example we will run the message broker (MQ), the namespace (MGM) and the storage (FST) service all on one machine.

Define your EOS instance name

.. note::

   EOS_INSTANCE_NAME has to start with "eos" and has the form "eos<name>". We will use <name> test in this instruction.

.. code-block:: bash

   export EOS_INSTANCE_NAME=eostest

Adjust the following variables according to your MGM hostanme

.. code-block:: bash

   export EOS_BROKER_URL=root://localhost:1097//eos/
   export EOS_MGM_MASTER1=<MGM hostname>
   export EOS_MGM_MASTER2=<MGM hostname>
   export EOS_MGM_ALIAS=<MGM hostname>
   export EOS_FUSE_MGM_ALIAS=<MGM hostname>

In /etc/xrd.cf.mgm change security setting as you need

.. code-block:: bash

   # Example disable krb5 and gsi
   #sec.protocol krb5
   #sec.protocol gsi
   sec.protbind * only sss unix

Let's start EOS

.. code-block:: bash

   // SysV flavour
   bash> service eos start

   // systemd flavour
   bash> systemctl start eos@*

For details for `systemd` support see :ref:`systemd`

Enable shared secret `sss` security

.. code-block:: bash

   eos -b vid enable sss

EOS uses a space and scheduling group concept

* spaces = made by groups
* groups = made by group of filesystems
* filesystems = mount point

The `default` space name is **default**. Groups in the **default** space are numbered **default.0** .. **default.<n>**. Each group can contain an arbitrary number of filesystems.

.. note::

   You should have atleast as many scheduling groups as the number of filesystems per storage node. The maximum number of
   filesystems in one EOS instance is limited to 64k.

If you have 20 disks on storage nodes you create 20 groups in space **default**

.. code-block:: bash

   for name in `seq 1 20`; do eos -b group set default.$name on; done

You can list your single storage node and your group configuration doing

.. code-block:: bash

   eos node ls
   eos group ls

.. note::

   We see also our MGM node because we added "fst" in XRD_ROLES. You can remove if you don't want to use your MGM machine as a storage srever.

To start a FUSE mount on MGM you can do

.. code-block:: bash

   bash> mkdir /eos/
   bash> service eosd start

Setup FST
---------

We will now setup a storage node on a different machine than the MGM node.

We create an empty eos config file in /etc/sysconfig/eos[_env] and change <MGM hostname> accordingly to our MGM hostname

.. code-block:: bash

   // for systemd based configurations omit the export statement

   DAEMON_COREFILE_LIMIT=unlimited
   export XRD_ROLES="fst"
   export LD_PRELOAD=/usr/lib64/libjemalloc.so.1
   export EOS_BROKER_URL=root://<MGM hostname>:1097//eos/

.. code-block:: bash

   // SysV flavour
   bash> service eos start

   // systemd flavour
   bash> systemctl start eos@fst

Now on the MGM node we should see a new FST running

.. code-block:: bash

   [root@eosfoo.ch ~]# eos node ls
   #-----------------------------------------------------------------------------------------------------------------------------
   #     type #                       hostport #   status #     status # txgw #gw-queued # gw-ntx #gw-rate # heartbeatdelta #nofs
   #-----------------------------------------------------------------------------------------------------------------------------
   nodesview                      eosfoo.ch:1095   online           on    off          0       10      120                1     1
   nodesview                      eosfst.ch:1095   online           on    off          0       10      120                1     1

Now we are going to add filesystems (partitions) to the storage node

.. note::

   Make sure that your data partition is having "user_xattr" option on, when you are mounting. Here is example in /etc/fstab

   /dev/sdb1 /data01  ext4    defaults,user_xattr        0 0

Let's assume that you have four partitions ``/data01 /data02 /data03 /data04``. You have to change the ownership of all storage directories to daemon:daemon because all EOS daemons run under the ``daemon`` account.

.. code-block:: bash

   chown -R daemon:daemon /data*

Register them towards the MGM via

.. code-block:: bash

   eosfstregister /data default:4

.. note::

   If you want to remove filesystems, they have to be in state ``empty`` (see :ref:`draining`) and then can be deleted via `eos fs rm <fsid>`

Configure MGM
-------------

To enable all hosts and filesystems in the **default** space do

.. code-block:: bash

   eos space set default on

You can check the status of your space with

.. code-block:: bash

   eos space ls
   eos space status default

If ``space ls`` shows non zero as **rw** space, you have successfully configured your EOS instance to store data.

Configure MGM space
-------------------

There are some space related parameters to modify the behaviour of EOS

.. code-block:: bash

   # disable quota for first tests
   eos space quota default off

   # disable balancer
   eos space config default space.balancer=off

   # set balancer threshold to 5%
   eos space config default space.balancer.threshold=5

   # set checksum scan interval to 1 week
   eos space config default space.scaninterval=604800

   # set drain delay for IO errors to 1 hours
   eos space config default space.graceperiod=3600

   # set max drain time to 1 day
   eos space config default space.drainperiod=86400

Basic Testing
-------------

You can create a test directory, upload and download a file as a first proof of concept test:

.. code-block:: bash

   # create a test directory
   eos mkdir /eos/testarea/
   # open the permissions
   eos chmode 777 /eos/testarea/
   # upload a test file
   eos cp /etc/group /eos/testarea/file.1
   # download a test file
   eos cp /eos/testarea/file.1 /tmp/group
   # compare the two files
   diff /etc/group /tmp/group
   # inspect the physical location and meta data of the uploaded file
   eos file info /eos/testarea/file.1


Enable kerberos security
------------------------

.. toctree::
   :maxdepth: 1

   krb5


Setup AUTH service
------------------

If an MGM has to handle more than 32k clients it is recommended to deploy a scalable front-end
service called AUTH.

Setup AUTH plugin
*****************

The authentication plugin is intended to be used as an OFS library with a
vanilla XRootD server. What it does is to connect using ZMQ sockets to the
real MGM nodes (in general it should connect to a master and a slave MGM).
It does this by reading out the endpoints it needs to connect to from the
configuration file (/etc/xrd.cf.auth). These need to follow the format:
"host:port" and the first one should be the endpoint corresponding to the
master MGM and the second one to the slave MGM. The EosAuthOfs plugin then
tries to replay all the requests it receives from the clients to the master
MGM node. It does this by marshalling the request and identity of the client
using ProtocolBuffers and sends this request using ZMQ to the master MGM
node. The authentication plugin can run on the same machine as an MGM node or
on a different machine. Once can use several such authentication services
at the same time.

There are several tunable parameters for this configuration (auth + MGMs):

AUTH - configuration
********************

- **eosauth.mastermgm** and **eosauth.slavemgm** - contain the hostnames and the
   ports to which ZMQ can connect to the MGM nodes so that it can forward
   requests and receive responses. Only the mastermgm parameter is mandatory
   the other one is optional and can be left out.
- **eosauth.numsockets** - once a clients wants to send a request the thread
    allocated to him in XRootD will require a socket to send the request
    to the MGM node. Therefore, we set up a pool of sockets from the
    begining which can be used to send/receiver requests/responses.
    The default size is 10 sockets.

MGM - configuration
*******************

- **mgmofs.auththreads** - since we now receive requests using ZMQ, we no longer
    use the default thread pool from XRootD and we need threads for dealing
    with the requests. This parameter sets the thread pool size when starting
    the MGM node.
- **mgmofs.authport** - this is the endpoint where the MGM listens for ZMQ
    requests from any EosAuthOfs plugins. This port needs to be opened also
    in the firewall.

In case of a master <=> slave switch the EosAuthOfs plugin adapts
automatically based on the information provided by the slave MGM which
should redirect all clients with write requests to the master node. Care
should be taken when specifying the two endpoints since the switch is done
ONLY IF the redirection HOST matches one of the two endpoints specified in
the configuration  of the authentication plugin (namely eosauth.instance).
Once the switch is done all requests be them read or write are sent to the
new master MGM node.
