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

   # Use the FUSE big write feature ( FUSE >=2.8 ) (default on)
   # export EOS_FUSE_BIGWRITES=1

   # Mount all files with 'x' bit to be able to run as an executable (default off)  
   # export EOS_FUSE_EXEC=1
    
   # Enable protection against recursive deletion (rm -r command) 
   #    starting from the root of the mount (if 1)
   #    or from any of its sub directories at a maximum depth (if >1) (default 1)
   # EOS_FUSE_RMLVL_PROTECT=1
   
   # Enable Kerberos authentication. This avoid need to set gateways on the mgm. 
   #    file cache credential should be used. (default 0)
   # EOS_FUSE_USER_KRB5CC=0

   # Enable X509 GSI authentication. This avoid need to set gateways on the mgm. 
   #    file user proxy should be used. (default 0)
   # EOS_FUSE_USER_GSIPROXY=0

   # If a connection fails using strong authentication, this is the timeout before actully retrying
   #    in the meantime, all access by the concerned user will be rejected (indicating authentication failure)
   #    !! WARNING: If a low value is used on a batch machine, it could have an impact on the authentication burden on the server side
   #    On interactive servers, it will be the longest time taken between refreshing the credentials and this taking effect on the fuse mount 
   #    (default is XRD_STREAMERRORWINDOW default value)
   # EOS_FUSE_STREAMERRORWINDOW=1
   
   # If KRB5 or X509 are enabled, specify the mapping from pid to strong authentication 
   #    should be kept as symlinks under /var/run/eosd/credentials/pidXXXX 
   #    (default 0)
   # EOS_FUSE_PIDMAP=0
   
   # Enable FUSE read-ahead (default off)
   # export EOS_FUSE_RDAHEAD=0

   # Configure FUSE read-ahead window (default 128k)
   # export EOS_FUSE_RDAHEAD_WINDOW=131072

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
    
Authentication
--------------
The shared FUSE mount currently support two authentication modes

- gateway mode authentication
- strong authentication mode featuring both **KRB5** and **X509**  

Only one authentication mechanism can be used with a single shared mount 
and it is specified using the configuration entry EOS_FUSE_USER_KRB5CC mentioned above.
 
 
Authentication in gateway mode
++++++++++++++++++++++++++++++
Each machine running a shared FUSE mount has to be
configured as a gateway machine in the MGM:

Add a FUSE host
~~~~~~~~~~~~~~~

.. code-block:: bash

   vid add gateway fusehost.foo.bar unix

It is also possible now to add a set of hosts matching a hostname pattern:

.. code-block:: bash

   vid add gateway lxplus* sss

Remove a FUSE host
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   vid remove gateway fusehost.foo.bar unix

To improve security you can require **sss** (shared secret authentication) instead 
of **unix** (authentication) in the above commands 
and distribute the **sss** keytab file to all FUSE hosts ``/etc/eos.keytab``.

Strong authentication mode
++++++++++++++++++++++++++
Enabling and configuring strong authentication is done using config keys 
EOS_FUSE_USER_KRB5CC, EOS_FUSE_USER_USERPROXY and EOS_FUSE_USER_KRB5FIRST (see above).

Each linux session can be bound to one credential file.
A same user can access the fuse mount using multiple identities using multiple instance.
To bind the current linux session to a credential file, the user has to use the script **eosfusebind**

The following command line 

.. code-block:: bash

   eosfusebind krb5 [credfile]

tries to find a krb5 credential cache file in the following order, stopping at the first match
- optional credfile argument if specified  
- environment variable KRB5CCNAME
- default location /tmp/krb5cc_<uid>
 
The following command line 

.. code-block:: bash

   eosfusebind x509 [credfile]

tries to find a x509 user proxy file in the following order, stopping at the first match
- optional credfile argument if specified  
- environment variable X509_USER_PROXY
- default location /tmp/x509up_u<uid>
 
Warning, **eosfusebind** does not check that the credential file is valid. 
It only checks it exists and has 600 permissions.
The actual authentication is carried out by the fuse mount.
Every time a new binding is made, all bindings from any terminated sessions (for the current user) are cleaned-up.
Binding an already bound session replaces the previous binding.

It is possible to show the bindings for the current session or the current user with the following commands

.. code-block:: bash

   eosfusebind --show-session
   eosfusebind --show-user

It is possible to unbind a given session or all the session of the current user using the following command

.. code-block:: bash

   eosfusebind --unbind-session
   eosfusebind --unbind-user

If the process tries to access the fuse mount and if its session is not bound to a valid credential file, access will be refused.

Protection against recursive top level deletion
-----------------------------------------------

The configuration entry EOS_FUSE_RMLVL_PROTECT defined above allow to enable this protection.
This will deny any deletion to an 'rm -r' command starting from the top level directory of the fuse mount down to the specified depth.

For instance, if eos is mounted in ``/eos`` and if ``EOS_FUSE_RMLVL_PROTECT=3``, then:

- ``rm /eos/*`` WILL run
- ``rm -i -rf /eos`` will NOT run
- ``rm -rf /eos/level2`` will NOT run
- ``rm -r /eos/level2/level3`` will NOT run
- ``rm -r /eos/level2/level3/level4`` WILL run.

The rule currently implemented is the following one:

The fuse mount will deny any removal coming from a command named ``rm`` with one of the short option(s) being ``r`` or one of the long option(s) being ``recursive`` 
if one of the non optional arguments is a path located under the mountpoint at a depth lower than the value specifed by ``EOS_FUSE_RMLVL_PROTECT``.

**mount** and autofs support
++++++++++++++++++++++++++++
If you have a defined FUSE instances and can manage them with the eosd service scripts, you use a mount wrapper to define mounts in /etc/fstab or mount manually. 

.. note::

   You should make sure that you don't have **eosd** as a persistent service:
   /sbin/chkconfig --del eos

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



 
