.. highlight:: rst

.. index::
   pair: Mounting EOS; FUSE


FUSE Client
===========

The FUSE client allows to access EOS as a mounted file system.

There a two FUSE client modes available:

.. epigraph::

   ========= ===================================================================
   daemon    description
   ========= ===================================================================
   eosfsd    An end-user private mount which is not shared between users 
   eosd      A system-wide mount shared between users
   ========= ===================================================================


**eosfsd** End-User mount
-------------------------
The end user mount supports the strong authentication methods in EOS:

* **KRB5**
* **X509**

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

   The mount point has to be non-existing or an **empty** directory!

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

In case of troubles you can find a log file for private mounts under ``/tmp/eos-fuse-<uid>.log``.

**eosd** Shared mount
---------------------
If you have machines shared by many users like batch nodes it makes sense to use 
the shared FUSE mount.

Configuration
+++++++++++++

You configure the FUSE mount via ``/etc/syconfig/eos`` (the first two ** have to be defined**):

.. code-block:: bash

   # Directory where to mount FUSE
   export EOS_FUSE_MOUNTDIR=/eos/
   # MGM URL from where to mount FUSE
   export EOS_FUSE_MGM_ALIAS=eosnode.foo.bar

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

   # Use the FUSE big write feature ( FUSE >=2.8 ) (default off)
   # export EOS_FUSE_BIGWRITES=1

   # Mount all files with 'x' bit to be able to run as an executable (default off)  
   # export EOS_FUSE_EXEC=1

   # Enable FUSE read-ahead (default off)
   # export EOS_FUSE_RDAHEAD=0

   # Configure FUSE read-ahead window (default 128k)
   # export EOS_FUSE_RDAHEAD_WINDOW=131072
 
   # Configure a log-file prefix - useful for several FUSE instances
   # export EOS_FUSE_LOG_PREFIX=dev
   # => will create /var/log/eos/fuse.dev.log

   # Configure multiple FUSE mounts a,b configured in /etc/sysconfig/eos.a /etc/sysconfig/eos.b
   #export EOS_FUSE_MOUNTS="a b"


In most cases one should enable the read-ahead feature with a read-ahead window of 1M and if available enable the big writes feature!
These features are off by default! If you want to mount several EOS instances, you can specify a list of mounts using **EOS_FUSE_MOUNTS** and then configure these mounts in individual sysconfig files 
with their name as suffix e.g. mount **dev** will be defined in ``/etc/sysconfig/eos.dev``. In case of a list of mounts the log file names have the name automatically inserted like ``fuse.dev.log``.
    
Authentication
--------------
The shared FUSE mount currently does not support (anymore) strong authentication 
methods like **KRB5** or **X509**. This is available only in the Citrine branch.

Each machine running a shared FUSE mount has to be
configured as a gateway machine in the MGM:

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


