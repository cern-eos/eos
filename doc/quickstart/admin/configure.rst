.. index::
   single: Admin Configuration

.. _eos_admin_configure:

EOS admin configuration
=======================

There are two types of nodes
* the MGM node (namespace node)
* the FST nodes (storage nodes)

Copy example config file to /etc/sysconfig/eos

.. code-block:: text
   
   cp /etc/sysconfig/eos.example /etc/sysconfig/eos

Setup MGM
---------
Change following variables in /etc/sysconfig/eos

Form MGM you should set following roles

.. code-block:: text

   export XRD_ROLES="mq mgm fst"

Set eos name

.. note::

   EOS_INSTANCE_NAME has to start with "eos" and has form "eos<name>". I will use <name> test for in this instruction, but you can
   pick any name you like.

.. code-block:: text

   export EOS_INSTANCE_NAME=eostest

change following variables 

.. code-block:: text

   export EOS_BROKER_URL=root://localhost:1097//eos/
   export EOS_MGM_MASTER1=<MGM hostname>
   export EOS_MGM_MASTER2=<MGM hostname>
   export EOS_MGM_ALIAS=<MGM hostname>
   export EOS_FUSE_MGM_ALIAS=<MGM hostname>

in /etc/xrd.cf.mgm change security setting as you need

.. code-block:: text

   # Example disable krb5 and gsi
   #sec.protocol krb5
   #sec.protocol gsi
   sec.protbind * only sss unix
   
Let's start eos

.. code-block:: text

   bash> service eos start

and let's enable sss security

.. code-block:: text

   eos -b vid enable sss

and let's define sub-spaces 

.. note::

   You should have atleast as many subspaces as the number of filesystems per storage node. 
   
if you have 20 disks on the storage nodes you do

.. code-block:: text

   for name in `seq 1 20`; do eos -b group set default.$name on; done

Now you should see at lease one node (FST) via eos console (eos -b)

   
.. note::

   We see our MGM node because we added "fst" in XRD_ROLES. You can remove if you don't want to user your MGM machine as FST.


To create fuse mount on MGM you can do

.. code-block:: text

   bash> mkdir /eos/
   bash> service eosd start

Setup FST
---------

Let's create empty eos config file in /etc/sysconfig/eos and change <MGM hostname>

.. code-block:: text

   DAEMON_COREFILE_LIMIT=unlimited
   export XRD_ROLES="fst"
   export LD_PRELOAD=/usr/lib64/libjemalloc.so.1
   export EOS_BROKER_URL=root://<MGM hostname>:1097//eos/

.. code-block:: text

   bash> service eos start

Now on MGM node we should see our new FST

.. code-block:: text

   eos -b node ls

you should see something similar

.. code-block:: text

   [root@eos-head-iep-grid ~]# eos -b node ls
   #-----------------------------------------------------------------------------------------------------------------------------
   #     type #                       hostport #   status #     status # txgw #gw-queued # gw-ntx #gw-rate # heartbeatdelta #nofs
   #-----------------------------------------------------------------------------------------------------------------------------
   nodesview   eos-data-iep-grid.saske.sk:1095     online           on    off          0       10      120                1     1
   nodesview   eos-head-iep-grid.saske.sk:1095     online           on    off          0       10      120                1     1
   
Now, let's add some file systems (some disk partitions)

.. note::

   Make sure that your data partition is having "user_xattr" option on, when you are mounting. Here is example in /etc/fstab
   
   /dev/sdb1 /data01  ext4    defaults,user_xattr        0 0
   
Let's assume that you have 4 partitions /data01 /data02 /data03 /data04. You have to change owner to daemon:daemon

.. code-block:: text

   chown -R daemon:daemon /data*
   
and register them on MGM via

.. code-block:: text

   eosfstregister /data default:4
   
.. note::

   !!! i have no idea how to unregister !!!!

Finish MGM
----------

To enable all hosts and filesystems in default space do following:

.. code-block:: text

   eos -b space set default on

To see our space you do

.. code-block:: text

   eos -b space ls
   
And you should see space in "RW" and then you are ready to go.

Testing MGM
-----------

Preapre for testing let's do following changes

.. code-block:: text

   # disable quota for first tests
   eos -b space quota default off
   
   # disable balancer
   eos -b space config default space.balancer=off
   
   # set balancer threshold to 5%
   eos -b space config default space.balancer.threshold=5
   
   # set checksum scan interval to 1 week
   eos -b space config default space.scaninterval=604800
   
   # set drain delay for IO errors to 1 hours
   eos -b space config default space.graceperiod=3600
   
   # set max drain time to 1 day
   eos -b space config default space.drainperiod=86400

Enable kerberos security
------------------------

.. toctree::
   :maxdepth: 1
   
   krb5

Setup AUTH plugin
-----------------
The authentication plugin is inteded to be used as an OFS library with a 
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
--------------------
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
-------------------
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
   
