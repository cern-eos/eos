.. highlight:: rst

.. index::
   pair: Mounting EOS; FUSE


FUSE Client
===========

The FUSE client allows to access EOS as a mounted file system.

There a two FUSE client modes available:

.. epigraph::

   ========= ===== ===================================================================
   daemon    user  description
   ========= ===== ===================================================================
   eosd      !root An end-user private mount which is not shared between users 
   eosd      root  A system-wide mount shared between users
   ========= ===================================================================


**eosd** End-User mount
-------------------------
The end user mount supports the strong authentication methods in EOS:

* **KRB5**
* **X509**

The shared mount supports these authentication methods only in the CITRINE branch.

Mouting
+++++++

You can mount an EOS instance using the EOS client:

.. code-block:: bash

   # mount my EOS instance to /home/user/eos

   eos fuse mount /home/user/eos

Un-Mounting
+++++++++++

You can unmount an EOS instance using:

.. code-block:: bash
  
   # unmount my EOS instance from /home/user/eos
    
   eos fuse umount /home/user/eos

.. note::
   
   The mount point can be given as an relative or absolute path!

.. warning::

   The mount point has to be a non-existing or an **empty** directory!

Authentication
++++++++++++++

The authentication method is proposed by the EOS server and the client evaluates
the server list until it finds a matching one. You can test the used authentication 
method using (see the **authz:** field):

.. code-block:: bash

   [eosdevsrv1]# eos -b whoami
   Virtual Identity: uid=755 (99,3) gid=1338 (99,4) [authz:krb5] sudo* host=localhost.localdomain geo-location=513

If the filesystem is mounted you can validate the same information using:

.. code-block:: bash

   [eosdevsrv1]# cat /home/user/eos/<instance>/proc/whoami

Log File
++++++++

In case of troubles you can find a log file for private mounts under ``/tmp/eos-fuse-<uid>.log``. If you run the single user
mount as root, you find the log file in ``/var/log/eos/fuse/fuse.log``

**eosd** Shared mount
---------------------
If you have machines shared by many users like batch nodes it makes sense to use 
the shared FUSE mount. The shared FUSE mount includes several high-performance add-ons.

Configuration
+++++++++++++

You configure the FUSE mount via ``/etc/syconfig/eos`` (the first two variables **have to be defined**):

.. code-block:: bash

   # Directory where to mount FUSE
   export EOS_FUSE_MOUNTDIR=/eos/

   # MGM URL from where to mount FUSE
   export EOS_FUSE_MGM_ALIAS=eosnode.foo.bar

   # If the remote directory path does not match the local, you can define the remote path to be different -
   # if not defined EOS_FUSE_REMOTEDIR=EOS_FUSE_MOUNTDIR is assumed e.g. local and remote tree have the same prefix
   # export EOS_FUSE_REMOTEDIR=/eos/testinstance/subtree/

   # Enable FUSE debugging mode (default off)
   # export EOS_FUSE_DEBUG=1

   # Disable PIO mode (used for high-preformance RAIN file access)
   # export EOS_FUSE_NOPIO=1

   # Disable multithreading in FUSE (default on)
   # export EOS_FUSE_NO_MT=1
 
   # Disable using access for access permission check (default on)
   # export EOS_FUSE_NOACCESS=0

   # Disable to use the kernel cache (default on)
   # export EOS_FUSE_KERNELCACHE=0

   # Bypass the buffercache for write - avoids 4k chopping of IO (default off)
   # (this is not what people understand under O_DIRECT !!!!
   # export EOS_FUSE_DIRECTIO=1

   # Disable the write-back cache (default on)
   # export EOS_FUSE_CACHE=0
  
   # Set the write-back cache size (default 300M) 
   # export EOS_FUSE_CACHE_SIZE=0

   # Set the write-back cache pagesize (default 256k) 
   # export EOS_FUSE_CACHE_SIZE=262144

   # Use the FUSE big write feature ( FUSE >=2.8 ) (default on)
   # export EOS_FUSE_BIGWRITES=1

   # Mount all files with 'x' bit to be able to run as an executable (default off)  
   # export EOS_FUSE_EXEC=1
    
   # Enable protection against recursive deletion (rm -r command) 
   #    starting from the root of the mount (if 1)
   #    or from any of its sub directories at a maximum depth (if >1) (default 1)
   # EOS_FUSE_RMLVL_PROTECT=1

   # Enable FUSE read-ahead (default off)
   # export EOS_FUSE_RDAHEAD=0

   # Configure FUSE read-ahead window (default 128k)
   # export EOS_FUSE_RDAHEAD_WINDOW=131072

   # Show hidden files from atomic/versioning and backup entries (default off)
   # export EOS_FUSE_SHOW_SPECIAL_FILES=0

   # Show extended attributes related to EOS itself - this are sys.* and emulated user.eos.* attributes for files (default off)
   # export EOS_FUSE_SHOW_EOS_ATTRIBUTES=0

   # Add(OR) an additional mode mask to the mode shown (default off)
   # export EOS_FUSE_MODE_OVERLAY=000     (use 007 to show things are rwx for w)

   # Enable lazy open on read-only files (default off)
   # export EOS_FUSE_LAZYOPENRO=1

   # Enable lazy open on read-write files (default on
   #    this option hides a lot of latency and is recommend to be used
   #    it requires how-ever that it is supported by EOS MGM version
   # export EOS_FUSE_LAZYOPENRW=1   

   # Set the kernel attribute cache time - this is the timewindow before you can see changes done on other clients
   # export EOS_FUSE_ATTR_CACHE_TIME=10

   # Set the kernel entry timeout - this is the time a directory listing is cached
   # export EOS_FUSE_ENTRY_CACHE_TIME=10

   # Set the timeout for the kernel negative stat cache 
   # export EOS_FUSE_NEG_ENTRY_CACHE_TIME=30

   # Set the liftime for a file creation ownership - withint this time each file re-open for update will be considered as cached locally and will not see remote changes
   # export EOS_FUSE_CREATOR_CAP_LIFETIME=30
   
   # Set the individual max. cache size per write-opened file where we have a creator capability
   # export EOS_FUSE_FILE_WB_CACHE_SIZE=67108864

   # Configure a log-file prefix - useful for several FUSE instances
   # export EOS_FUSE_LOG_PREFIX=dev
   # => will create /var/log/eos/fuse.dev.log

   # Configure multiple FUSE mounts a,b configured in /etc/sysconfig/eos.a /etc/sysconfig/eos.b
   #export EOS_FUSE_MOUNTS="a b"


In most cases one should enable the read-ahead feature with a read-ahead window of 1M on LAN and larger for WAN RTTs and if available use the big writes feature!
If you want to mount several EOS instances, you can specify a list of mounts using **EOS_FUSE_MOUNTS** and then configure these mounts in individual sysconfig files 
with their name as suffix e.g. mount **dev** will be defined in ``/etc/sysconfig/eos.dev``. In case of a list of mounts the log file names have the name automatically inserted like ``fuse.dev.log``.

Starting the Service
++++++++++++++++++++
Once you configured the FUSE mountpoint(s) you can use standard service mechanism to start, stop and check your shared mounts:

.. code-block:: bash

   # start all eosd instances
   service eosd start

   # start a particular eosd instance 
   service eosd start myinstance

   # stop all eosd instances
   service eosd stop 

   # stop a particular eosd instance
   service eosd stop myinstance

   # check the status of all instances
   service eosd status
   
   # check the status of a particular instance
   service eosd status myinstance

   # if instances are up restart them conditional
   service eosd condrestart [myinstance]
   
   # shutdown/cleanup all eosd instances running as root
   service eosd killall

Example Configuration
+++++++++++++++++++++

He is an example to configure two FUSE mounts from instance **use** and **public**

Define two FUSE mounts in /etc/sysconfig/eos

.. code-block:: bash

   # define which instance mounts we have configured
   export EOS_FUSE_MOUNTS="user public"

   # #################################################################
   # shared EOS FUSE options
   # #################################################################
   # in-memory write-back shared cache 
   export EOS_FUSE_CACHE_SIZE=268435456
   # just normal logging
   export EOS_FUSE_DEBUG=0
   # not to verbose - just prints timing and errors
   export EOS_FUSE_LOGLEVEL=5
   # don't wast time to do parallel IO - only useful for RAIN layouts
   export EOS_FUSE_NOPIO=1
   # configure 256k readahead (additional to 128k kernel readahead)
   export EOS_FUSE_RDAHEAD=1
   export EOS_FUSE_RDAHEAD_WINDOW=262144
   # stop rm -r for directories with deepness <=2
   export EOS_FUSE_RMLVL_PROTECT=2
   # configure JEMALLOC
   test -e /usr/lib64/libjemalloc.so.1 && export LD_PRELOAD=/usr/lib64/libjemalloc.so.1

   # #################################################################
   # shared XrdCl options
   # #################################################################
   # tag xroot traffic
   export XRD_APPNAME=eos-fuse
   export XRD_CONNECTIONRETRY=4096
   export XRD_CONNECTIONWINDOW=0
   # keep connections to FSTs for 5 minutes
   export XRD_DATASERVERTTL=300
   # keep connections to MGM for 30 minutes
   export XRD_LOADBALANCERTTL=1800
   # standard verbosity for logging
   export XRD_LOGLEVEL=Info
   # don't follow more than 5 redirects
   export XRD_REDIRECTLIMIT=5
   # short request timeout of 60s - might be low for high throughput storage
   export XRD_REQUESTTIMEOUT=60
   export XRD_STREAMERRORWINDOW=15
   export XRD_STREAMTIMEOUT=15
   # interval how often timeouts are checked .. to get ~60s we have to set it to a second
   export XRD_TIMEOUTRESOLUTION=1
   # client worker thread pool 
   export XRD_WORKERTHREADS=16


Then the individual part of each FUSE mount is described in two sysconfig files:

**user**: ``/etc/sysconfig/eos.user``

.. code-block:: bash

   # from where do we mount ...
   export EOS_FUSE_MGM_ALIAS=eosuser.cern.ch
   # where to we mount
   export EOS_FUSE_MOUNTDIR=/eos/user/

**public**: ``/etc/sysconfig/eos.public``

.. code-block:: bash

   # from where do we mount ...
   export EOS_FUSE_MGM_ALIAS=eospublic.cern.ch
   # where to we mount
   export EOS_FUSE_MOUNTDIR=/eos/public/

Authentication
--------------
The shared FUSE mount supports strong authentication like **KRB5** or **X509**.

Each machine running a shared FUSE mount can be
configured as a gateway machine in the MGM if strong authentication is not desired on client side:

Add a FUSE host
+++++++++++++++

.. code-block:: bash

   vid add gateway fusehost.foo.bar unix

It is also possible now to add a set of hosts matching a hostname pattern:

.. code-block:: bash

   vid add gateway lxplus* sss

Remove a FUSE host
++++++++++++++++++

.. code-block:: bash

   vid remove gateway fusehost.foo.bar unix

To improve security you can require **sss** (shared secret authentication) instead 
of **unix** (authentication) in the above commands 
and distribute the **sss** keytab file to all FUSE hosts ``/etc/eos.keytab``.

**mount** and autofs support
++++++++++++++++++++++++++++
If you have a defined FUSE instances and can manage them with the eosd service scripts, you use a mount wrapper to define mounts in /etc/fstab or mount manually. 

.. note::

   You should make sure that you don't have **eosd** as a persistent service:
   /sbin/chkconfig --del eosd

To mount **myinstance** to the local directory ``/eos/myinstance`` you can write:

.. code-block:: bash

   # mount
   mount -t eos myinstance /eos/myinstance

   # umount
   umount /eos/myinstance

To define a FUSE mount in ``/etc/fstab`` you add for example:

.. code-block:: bash

   myinstance  /eos/myinstance defaults 0 0 

If you want to use **autofs**, you have to create a file ``/etc/auto.eos`` :

.. code-block:: bash

   myinstance -fstype=eos :myinstance

Add to the file ``/etc/auto.master`` at the bottom:

.. code-block:: bash

   /eos /etc/auto.eos

For convenience make sure that you enable browsing in ``/etc/autofst.conf``:

   browse_mode = yes  # this lets you see the mountdir myinstance in ``/eos/`` as ``/eos/myinstance/``. Once you acces this directory it will be automatically mounted.



.. note::

   Enable **autofs** with ``service autofs start``   

Exporting FUSE filesystems
--------------------------


FUSE export with NFS4
+++++++++++++++++++++

To export FUSE via NFS4 you have to disable(shorten) the attribute caching in the FUSE configuration file:

.. code-block:: bash
  
   export EOS_FUSE_ATTR_CACHE_TIME=0.0000000000000001

If you mount an instance as /eos you have to configure an NFS export like this in /etc/exports:

   /eos *.cern.ch(fsid=131,rw,insecure,subtree_check,async,root_squash)

You have to start/reload your nfs4 server and then you should be able to access the NFS volume using

.. code-block:: bash

   mount -t nfs4 <server> <localhost>

FUSE export with CIFS/Samba
+++++++++++++++++++++++++++

To export FUSE via Samba you have only to enable a mode overlay to avoid messages about permission problems during browsing in the FUSE configuration file:

.. code-block:: bash
 
   export EOS_FUSE_MODE_OVERLAY=077


The rest of the CIFS server configuration is idential to a local filesystem Samba export.





