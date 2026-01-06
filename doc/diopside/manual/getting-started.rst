.. index::
   single: Getting Started

.. highlight:: rst

.. _gettingstarted:

Getting Started
=================

Our recommended way to install a production ready EOS instance is using RPM configuration.
A Kubernetes-based demonstrator is available for testing purposes.

EOS installation with Kubernetes and Helm for test / demonstration
-----------------------------------------------------------------
The `EOS Charts repository <https://gitlab.cern.ch/eos/eos-charts>`_ provides Helm charts for the deployment of EOS in Kubernetes for test and demonstration purposes.
A working Kubernetes cluster (`v1.20.15` or newer) and the Helm package manager (`v3.8.0` or newer) are required.

The installation is fully automated via the `server` chart, which deploys 1 MGM, 3 QDB instances in cluster mode, and 4 FSTs.

.. code-block:: bash

  helm install eos oci://registry.cern.ch/eos/charts/server

The resulting cluster will consist of 8 pods:

.. code-block:: bash

  kubectl get pods
  NAME        READY   STATUS    RESTARTS   AGE
  eos-fst-0   1/1     Running   0          4m47s
  eos-fst-1   1/1     Running   0          79s
  eos-fst-2   1/1     Running   0          69s
  eos-fst-3   1/1     Running   0          59s
  eos-mgm-0   2/2     Running   0          4m47s
  eos-qdb-0   1/1     Running   0          4m47s
  eos-qdb-1   1/1     Running   0          2m21s
  eos-qdb-2   1/1     Running   0          2m6s

...and will be configured with relevant defaults to make it a fully-working instance:

.. code-block:: bash

  eos ns
  # ------------------------------------------------------------------------------------
  # Namespace Statistics
  # ------------------------------------------------------------------------------------
  ALL      Files                            5 [booted] (0s)
  ALL      Directories                      11
  ALL      Total boot time                  0 s
  # ------------------------------------------------------------------------------------
  ALL      Compactification                 status=off waitstart=0 interval=0 ratio-file=0.0:1 ratio-dir=0.0:1
  # ------------------------------------------------------------------------------------
  ALL      Replication                      mode=master-rw state=master-rw master=eos-mgm-0.eos-mgm.default.svc.cluster.local configdir=/var/eos/config/ config=default
  # ------------------------------------------------------------------------------------
  {...cut...}

  eos fs ls
  ┌───────────────────────────────────────────┬────┬──────┬────────────────────────────────┬────────────────┬────────────────┬────────────┬──────────────┬────────────┬────────┬────────────────┐
  │host                                       │port│    id│                            path│      schedgroup│          geotag│        boot│  configstatus│       drain│  active│          health│
  └───────────────────────────────────────────┴────┴──────┴────────────────────────────────┴────────────────┴────────────────┴────────────┴──────────────┴────────────┴────────┴────────────────┘
   eos-fst-0.eos-fst.default.svc.cluster.local 1095      1                     /fst_storage        default.0      docker::k8s       booted             rw      nodrain   online              N/A
   eos-fst-1.eos-fst.default.svc.cluster.local 1095      2                     /fst_storage        default.1      docker::k8s       booted             rw      nodrain   online              N/A
   eos-fst-2.eos-fst.default.svc.cluster.local 1095      3                     /fst_storage        default.2      docker::k8s       booted             rw      nodrain   online              N/A
   eos-fst-3.eos-fst.default.svc.cluster.local 1095      4                     /fst_storage        default.3      docker::k8s       booted             rw      nodrain   online              N/A

EOS up in few minutes
---------------------
We will start setting up a complete EOS installation on a single physical machine using the EOS5 configuration method. Later we will demonstrate the steps to add high-availability to MGMs/QDBs and how to scale-out FST nodes. All commands have to be issued using the `root` account!

Grab a machine preferably with Alma9 (or Alma8 or CentOS7). Only the repository setup differs for these platforms (shown is the Alma9 installation, just replace 9 with 7,8 in the URLs :

Installation
------------

.. code-block:: bash
  
  dnf config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"
  dnf config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-9/x86_64/"
  dnf install -y eos-server eos-quarkdb eos-fusex --nogpgcheck

Unique Instance Shared Secret
-----------------------------

Every instance should run with a unique instance-private shared secret.
This can be easily created using:

.. code-block:: bash

  eos daemon sss recreate

The command will create a local file `/etc/eos.keytab` storing the instance-specific shared secret needed for MGM,FST,MQ (and additionally a shared secret `/etc/eos/fuse.sss.keytab` useful for clients when doing FUSE mounts in combination with EOS token).

Start Services
--------------

We will startup three services in a manual way to get a better understanding about the procedure and the used configuration.

To shorten the setup we disable the firewall for the moment. The ports to open in the firewall are explained later.

.. code-block:: bash

  systemctl stop firewalld


.. note:: 

  After each `daemon run` the shell should hang with the daemon in foreground. If the startup fails, the process will exit. If the startup is successfull use `Control-Z `and type `bg` to put the process in the background and continue with the next service until all four have been started.
 
.. code-block:: bash

  # start QuarkDB on this host		  
  eos daemon run qdb
  # start MGM on this host
  eos daemon run mgm
  # all this host to connect as an FST
  eos node set `hostname -f`:1095 on
  # start FST on this host
  eos daemon run fst

  
.. note:: 

  Each command prints commands executed during the daemon initialization phase and the XRootD configuration file used. In reality each EOS service is an XRootD server process with dedicated plug-in and configuration. The init phases have been designed to be able to startup a service without doing ANY customized configuration bringing good defaults.

You should be able to see the running daemons doing:

The production way to do this is to run

.. code-block:: bash

  # start QuarkDB
  systemctl start eos5-qdb@qdb
  # start MGM
  systemctl start eos5-mgm@mgm
  # allow this host connect as an FST
  eos node set `hostname -f`:1095 on
  # start FST on this host
  systemctl start eos5-fst@fst

and to enable the services in the boot procedure

.. code-block:: bash

  systemctl enable eos5-qdb@qdb
  systemctl enable eos5-mgm@mgm
  systemctl enable eos5-fst@fst
  
.. code-block:: bash

  ps aux | grep eos


Using the CLI
-------------

Your EOS installation is now up and running. We are now starting the CLI to inspect and configure our EOS instance:

.. code-block:: bash

  [root@vm root]# eos version
  EOS_INSTANCE=eosdev
  EOS_SERVER_VERSION=5.2.5 EOS_SERVER_RELEASE=5.2.5
  EOS_CLIENT_VERSION=5.2.5 EOS_CLIENT_RELEASE=5.2.5

  [root@vm root]# eos whoami
  Virtual Identity: uid=0 (0,3,99) gid=0 (0,4,99) [authz:sss] sudo* host=localhost domain=localdomain

You can navigate the namespace using well known commands:

.. code-block:: bash

  [root@vm root]# eos ls -la /eos/
  drwxrwx--x   1 root     root            23249 Jan  1  1970 .
  drwxr-x--x   1 root     root                0 Jan  1  1970 ..
  drwxrwx--x   1 root     root            23249 Aug 18 17:28 dev


The default EOS instance name is *eosdev* and in every EOS instance you will the find the pre-created directory structure like shown:

.. code-block:: bash

  [root@vm root]# eos find -d /eos/ 
  path=/eos/
  path=/eos/dev/
  path=/eos/dev/proc/
  path=/eos/dev/proc/archive/
  path=/eos/dev/proc/clone/
  path=/eos/dev/proc/conversion/
  path=/eos/dev/proc/recycle/
  path=/eos/dev/proc/tape-rest-api/
  path=/eos/dev/proc/tape-rest-api/bulkrequests/
  path=/eos/dev/proc/tape-rest-api/bulkrequests/evict/
  path=/eos/dev/proc/tape-rest-api/bulkrequests/stage/
  path=/eos/dev/proc/token/
  path=/eos/dev/proc/tracker/
  path=/eos/dev/proc/workflow/

All EOS instance names have to start with *eos* prefix (eosxyz). If you configure your EOS instance to have name **eosfoo** you will see an automatic structure created during MGM startup which looks like this:

.. code-block:: bash

  /eos/
  /eos/foo/
  ...


Adding Storage Space
---------------------

The first thing we do is to create the `default` space, which will host all our filesystems:

.. code-block:: bash

  eos space define default


Now we want to attach local disk space to our EOS instance into the `default` space . In this example we will register six filesystems to our instance. The filesystems can be on a single or individual partitions. 

.. code-block:: bash

  # create four directories to be used as separate EOS filesystems and own them with the `daemon` account
  for name in 01 02 03 04 05 06; do
      mkdir -p /data/fst/$name; 
  chown daemon:daemon /data/fst/$name
  done


.. code-block:: bash

  # register all sub-directories under /data/fst as EOS filesystems
  eosfstregister -r localhost /data/fst/ default:6


The `eosfstregister` command lists all directories under `/data/fst/` and assumes that is has to register 6 filesystem to the *default* space indicated by the parameter `default:6` (See `eosfstregister -h` for the command syntax) to the MGM running on `localhost`. Before filesystems are usable, they have to be owned by the `daemon` account. 

We do now one additional step. By default EOS will place each filesystem from the same node to a separate placement group, so it will create 6 scheduling groups `default.0`, `default.1` ... `default.6` and place filesystem 1 in `default.0`, 2 into `default.1` aso ...
To write a file EOS selects a group and tries place the file into a single group. If you want now to write files with two replicas you have to have at least 2 filesystems per group, if you want to use erasure coding e.g. RAID6, you would need to have 6 filesystems per group. Therefore we now move all disks into the `default.0` group (disk 1 is already in group `default.0`):

.. code-block:: bash

  for name in 2 3 4 5 6; do eos fs mv --force $name default.0; done


Exploring EOS Views
---------------------

Now you are ready to check-out the four views EOS provides:

.. code-block:: bash
  
  eos space ls

.. code-block:: bash

  eos node ls 


.. code-block:: bash

  eos group ls


.. code-block:: bash

  eos fs ls 


All this commands take several additional output options to provide more information e.g. `eos space ls -l` or `eos space ls --io` ...
You will notice, that in all this views you either see `active=0` or `offline`.  This is because we have registered filesystems, but we didn't enable them yet.

Enabling EOS Space
---------------------

The last step before using our storage setup is to enable the default space:

.. code-block:: bash

  eos space set default on

Enabling the space means to enable all nodes, groups and filesystems in that space.

Now you can now see everything as `online` and `active` in the four views.

Read and Write using CLI
-------------------------

We can now upload and download our first file to our storage system. We will create a new directory and define a storage policy, to store files as single replica files (one copy):

.. code-block:: bash

   eos mkdir /eos/dev/test/                            #create directory
   eos attr set default=replica /eos/dev/test/         #define default replication policy
   eos attr set sys.forced.nstripes=1 /eos/dev/test/   #define to have one replica only
   eos chmod 777 /eos/dev/test/                        #allow everybody to write here
   eos cp /etc/passwd /eos/dev/test/                   #upload a test file
   eos cp /eos/dev/test/passwd /tmp/passwd             #download the test file
   diff /etc/passwd /tmp/passwd                        #compare with original file


You can list the directory where the file was stored:

.. code-block:: bash
  
   eos ls -l /eos/dev/test/


and you can find out a lot information about this file e.g. the *adler32* checksum which was configured automatically doing `eos attr set default=replica /eos/dev/test` and the location of our file (on which filesystem the files has been stored):

.. code-block:: bash

  eos file info /eos/dev/test/passwd


Read and Write using /eos/ mounts
---------------------------------

We can FUSE mount our EOS instance on the same node by just doing:

.. code-block:: bash

  mkdir -p /eos/
  # put your host.domain name in the command
  eosxd -ofsname=host.domain:/eos/ /eos/


An alternative to running the *eosxd* executable is to use the FUSE mount type:

.. code-block:: bash

  mount -t fuse eosxd -ofsname=host.domain:/eos/ /eos/


In either way, you should be able to see the mount and the configured space using `df`:

.. code-block:: bash
  
  df /eos/

All the usual shell commands will now also work on the FUSE mount.

.. note:: 
  
  Be aware that the default FUSE mount does not map the current uid/gid to the same uid/gid inside EOS. Moreover *root* access is always squashed to uid,gid=99 (nobody). 

In summary on this FUSE mount with default configuration on localhost you will be mapped to user *nobody* inside EOS. If you copy a file on this FUSE mount to `/eos/dev/test/` the file will be owned by `99/99`. 

Firewall Configuration for external Access
------------------------------------------

To make your instance accessible from outside you have to make sure that all the relevant ports are open for incoming traffic.

Here is a list of ports used by the various services:

+----------------+------+
| Service        | Port |
+================+======+
| MGM (XRootD)   | 1094 |
+----------------+------+
| MGM (FUSE ZMQ) | 1100 |
+----------------+------+
| FST (XRootD)   | 1095 |
+----------------+------+
| QDB (REDIS)    | 7777 |
+----------------+------+

If port 1100 is not open, FUSE access still works, but FUSE clients are not displayed as being online and they don't receive callbacks for meta-data changes e.g. changes made on another client are not immediately visible.

.. code-block:: bash

  systemctl start firewalld
  for port in 1094 1095 1097 1100 7777; do 
   firewall-cmd --zone=public --permanent --add-port=$port/tcp
  done


Single Node Quick Setup Code Snippet
------------------------------------

.. code-block:: bash
  
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9s/x86_64/"
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-9s/x86_64/"
  yum install -y eos-server eos-quarkdb eos-fusex --nogpgcheck

  systemctl start firewalld
  for port in 1094 1095 1100 7777; do 
    firewall-cmd --zone=public --permanent --add-port=$port/tcp
  done

  eos daemon sss recreate

  systemctl start eos5-qdb@qdb
  systemctl start eos5-mgm@mgm
  eos node set `hostname -f`:1095 on 
  systemctl start eos5-fst@fst
  
  sleep 30

  for name in 01 02 03 04 05 06; do
    mkdir -p /data/fst/$name; 
    chown daemon:daemon /data/fst/$name
  done

  eos space define default

  eosfstregister -r localhost /data/fst/ default:6

  for name in 2 3 4 5 6; do eos fs mv --force $name default.0; done

  eos space set default on 

  eos mkdir /eos/dev/rep-2/                         
  eos mkdir /eos/dev/ec-42/
  eos attr set default=replica /eos/dev/rep-2 /
  eos attr set default=raid6 /eos/dev/ec-42/
  eos chmod 777 /eos/dev/rep-2/             
  eos chmod 777 /eos/dev/ec-42/

  mkdir -p /eos/
  eosxd -ofsname=`hostname -f`:/eos/ /eos/


Adding FSTs to a single node setup
----------------------------------

.. code-block:: bash
  
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9s/x86_64/"
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-9s/x86_64/"
  yum install -y eos-server --nogpgcheck

  systemctl start firewalld
  for port in 1095; do 
    firewall-cmd --zone=public --permanent --add-port=$port/tcp
  done

  # On the FST node configure MGM node in /etc/config/eos/generic/all
  SERVER_HOST=mgmnode.domain

  # Copy /etc/eos.keytab from MGM node to the new FST node to /etc/eos.keytab
  scp root@mgmnode.domain:/etc/eos.keytab /etc/eos.keytab

  # Allow the new FST on the MGM to connect as an FST
  @mgm: eos node set fstnode.domain:1095 on

  # Start FST service
  systemctl start eos5-fst@fst
  systemctl enable eos5-fst@fst

  # Verify Node online
  @mgm: eos node ls

  
Expanding single node MGM/QDB setup to HA cluster
-------------------------------------------------
In a production environment we need to have QDB and MGM service high-available. We will show here, how to configure three co-located QDB+MGM nodes.The three nodes are called in the example `node1.domain` `node2.domain` `node3.domain`. We assume you running mgm is node1 and new nodes are node2 and node3.

.. code-block:: bash
  
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9s/x86_64/"
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-9s/x86_64/"
  yum install -y eos-server eos-quarkdb eos-fusex --nogpgcheck

  systemctl start firewalld
  for port in 1094 1100 7777; do 
   firewall-cmd --zone=public --permanent --add-port=$port/tcp
  done

  # Copy /etc/eos.keytab from MGM node to the new MGM nodes to /etc/eos.keytab
  scp root@node1:/etc/eos.keytab /etc/eos.keytab

  # Create observer QDB nodes on node2 and node3
  eos daemon config qdb qdb new observer

  # Start QDB on node2 and node3
  systemctl start eos5-qdb@qdb
  systemctl enable eos5-qdb@qdb

  # Allow node2 & node3 as follower on node 1
  @node1: redis-cli -p 7777
  @node1: 127.0.0.1:7777> raft-add-observer node2.domain:7777
  @node1: 127.0.0.1:7777> raft-add-observer node3.domain:7777

  # ( this is equivalent to 'eos daemon config qdb qdb add node2.domain:7777' but broken in the release version )

  # node2 & node3 get contacted by node1 and start syncing the raft log

  # Promote node2 and node3 as full members
  @node1: redis-cli -p 7777
  @node1: 127.0.0.1:7777> raft-promote-observer node2.domain:7777
  @node1: 127.0.0.1:7777> raft-promote-observer node3.domain:7777

  # ( this is equivalent to 'eos daemon config qdb qdb promote node2.domain:7777 )
  
  # Verify RAFT status on any QDB node
  redis-cli -p 777
  127.0.0.1:7777> raft-info
  
  # ( this is equivalent to 'eos daemon config qdb qdb info' )
  
  # Startup MGM services
  @node2: systemctl start eos5-mgm@mgm
  @node3: systemctl start eos5-mgm@mgm

  # You can connect on each node using the eos command to the local MGM
  @node1:  eos ns | grep master
  ALL      Replication                      is_master=true master_id=node1.domain:1094
  @node2:  eos ns | grep master
  ALL      Replication                      is_master=false master_id=node1.domain:1094
  @node3:  eos ns | grep master
  ALL      Replication                      is_master=false master_id=node1.domain:1094

  # You can force the QDB leader to a given node e.g.
  @node2: eos daemon config qdb qdb coup

  # you can force the active MGM to run on a given node by running on the current active MGM:
  @node1: eos ns master node2.domain:1094
  success: current master will step down
  
Three Node Quick Setup Code Snippet
-----------------------------------

You can also setup a three node cluster from scratch right from the beginning, which is shown here:

.. code-block:: bash

  # on all three nodes do 
  killall -9 xrootd     # make sure no daemons are running
  rm -rf /var/lib/qdb/  # wipe previous QDB database

  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"
  yum-config-manager --add-repo "https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-9/x86_64/"
  yum install -y eos-server eos-quarkdb eos-fusex --nogpgcheck

  systemctl start firewalld
  for port in 1094 1095 1097 1100 7777; do 
   firewall-cmd --zone=public --permanent --add-port=$port/tcp
  done


Now edit `/etc/eos/config/qdb/qdb` and change the variable definition with your QDB nodes:

.. code-block:: bash

  QDB_NODES=${QDB_HOST}:${QDB_PORT}

to

.. code-block:: bash

  QDB_NODES=node1.cern.ch:7777,node2.cern.ch:7777,node3.cern.ch:7777


Create new instance sss keys on one node and copy them to the other two nodes:

.. code-block:: bash

  # node 1
  eos daemon sss recreate
  # copy to node2,node3
  scp /etc/eos.keytab root@node2:/etc/eos.keytab
  scp /eos/eos/fuse.sss.keytab root@node2:/etc/eos.keytab
  scp /etc/eos.keytab root@node3:/etc/eos.keytab
  scp /eos/eos/fuse.sss.keytab root@node3:/etc/eos.keytab
  

Now start QDB on all three nodes:

.. code-block:: bash

  systemctl start eos5-qdb@qdb


Now you can inspect the RAFT state on all QDBs:

.. code-block:: bash

  eos daemon config qdb qdb info

  1) TERM 1
  2) LOG-START 0
  3) LOG-SIZE 2
  4) LEADER node2.cern.ch:7777
  5) CLUSTER-ID eosdev
  6) COMMIT-INDEX 1
  7) LAST-APPLIED 1
  8) BLOCKED-WRITES 0
  9) LAST-STATE-CHANGE 293 (4 minutes, 53 seconds)
  10) ----------
  11) MYSELF node2.domain:7777
  12) VERSION 5.1..5.1.3
  13) STATUS LEADER
  14) NODE-HEALTH GREEN
  15) JOURNAL-FSYNC-POLICY sync-important-updates
  16) ----------
  17) MEMBERSHIP-EPOCH 0
  18) NODES node1.domain:7777,node2.domaina:7777,node3.domain:7777
  19) OBSERVERS 
  20) QUORUM-SIZE 2



Here you see that the current LEADER is node2.domain.  If you want to force that node1.cern.ch becomes a leader you can type on node1:

.. code-block:: bash

  [root@node1 ] eos daemon config qdb qdb coup

and verify using 

.. code-block:: bash

  eos daemon config qdb qdb info

who the new LEADER is.

Now we start `mgm` on all three nodes:

.. code-block:: bash

  [root@node1] systemctl start eos5-mgm@mgm
  [root@node2] systemctl start eos5-mgm@mgm
  [root@node3] systemctl start eos5-mgm@mgm

You can connect to the MGM on each node. 

.. code-block:: bash

  [root@node1] eos ns | grep Replication
  ALL      Replication                      is_master=true master_id=node1.cern.ch:1094
  [root@node2] eos ns | grep Replication
  ALL      Replication                      is_master=false master_id=node1.cern.ch:1094
  [root@node3] eos ns | grep Replication
  ALL      Replication                      is_master=false master_id=node1.cern.ch:1094


The three MGMs use a lease mechanism to acquire the active role. If you want to push manually the active role from `node1` to `node2`, you do:

.. code-block:: bash

  [root@node1 ] eos ns master node2.cern.ch

When the default lease time expired, the master should change:

.. code-block:: bash

  [root@node1] eos ns | grep Replication
  ALL      Replication                      is_master=false master_id=node1.cern.ch:1094
  [root@node2] eos ns | grep Replication
  ALL      Replication                      is_master=true master_id=node1.cern.ch:1094
  [root@node2] eos ns | grep Replication
  ALL      Replication                      is_master=false master_id=node1.cern.ch:1094


.. note::

  Sometimes you might observe changes of the master under heavy load. If this happens too frequently you can increase the lease time modifying the `sysconfig` variable of the mgm daemon e.g. to set other 120s lease time (instead of default 60s) you define:

  .. code-block:: bash
    
    EOS_QDB_MASTER_LEASE_MS="120000"


Joining a node to a QDB raft cluster
------------------------------------

The procedure to add the node foo.bar:7777 to a QDB cluster is straight-forward. *QDB_PATH* has to be not existing and the *QDB* service will be down in any case when running this command on the new/unconfigured node:

.. code-block:: bash

  eos daemon config qdb qdb new observer

To get the node join as a member you run three commands
On the new node:

.. code-block:: bash

  1 [ @newnode ] : systemctl start eos5-qdb@qdb


=> `eos daemon config qdb qdb info` will not show the new node as an observer. The *QDB* logfile `/var/log/eos/qdb/xrdlog.qdb` will say, this new *QDB* node is still in limbo state and needs to be added!

On the elected leader node:

.. code-block:: bash

  2 [ @leader  ] : eos daemon config qdb qdb add foo.bar:7777

=> `eos daemon config qdb qdb info` will show the new node as replicating and LAGGING until the synchronization is complete and status will be UP-TO-DATE. The new node is not yet a member of the cluster quorum.

On the elected leader node:

.. code-block:: bash

  3 [ @leader  ] : eos daemon config qdb qdb promote foo.bar

=> `eos daemon config qdb qdb info` will show the new node as a member of the cluster under NODES.

# 5 Replacing/Removing a node in a 3 node QDB setup
To replace or remove node foo.bar:7777 one needs only two to three steps:
Shutdown *QDB* on that node:

.. code-block:: bash
 
  1 [ @drainnode ] : systemctl stop eos5-qdb@qdb

Remove that node on the leader from the membership:

.. code-block:: bash

  2 [ @leader    ] : eos daemon config qdb qdb remove foo.bar:7777

Optionally delete *QDB* database files on foo.bar:7777 (don't run this on the LEADER !!!!):

.. code-block:: bash

  3 [ @drainnode ] : rm -rf /var/lib/qdb/

Now run the _join_ procedure from the previous section on the node, which should replace the decommissioned member!

Backup your QDB database
------------------------


.. code-block:: bash

  eos daemon config qdb qdb backup

.. Add features using Configuration modules
.. -----------------------------------------

.. ## With 5.2 Release


.. ### Enable http access with a configuration module
.. This will configure your instance to provide http(s) access.

.. .. code-block:: bash
  
..   echo "http" >> /etc/eos/config/mgm/mgm.modules
..   eos daemon module-init mgm
..   systemctl restart eos5-mgm@mgm


.. ### Enable alice configuration/access with a configuration module
.. This will configure your instance to become an ALICE SE with all required settings and namespace setup.

.. .. code-block:: bash

..   echo "alice" >> /etc/eos/config/mgm/mgm.modules
..   eos daemon module-init mgm
..   systemctl restart eos5-mgm@mgm


.. ### Add Kerberos token authentication to your instance with a configuration module
.. This will add kerberos5 authentication to your instance.

.. .. code-block:: bash

..   echo "krb5" >> /etc/eos/config/mgm/mgm.modules
..   eos daemon module-init mgm
..   systemctl restart eos5-mgm@mgm

 
.. ### Add GSI proxy authentication to your instance with a configuration module
.. This will enable GSI authentication with certificates and proxies on your instance.

.. .. code-block:: bash

..   echo "gsi" >> /etc/eos/config/mgm/mgm.modules
..   eos daemon module-init mgm
..   systemctl restart eos5-mgm@mgm


