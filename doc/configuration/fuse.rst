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

   # If both KRB5 and X509 are enabled, specify if KRB5 should be tried first. 
   #    (default 0)
   # EOS_FUSE_USER_KRB5FIRST=0
   
   # If KRB5 or X509 are enabled, specify the mapping from pid to strong authentication 
   #    should be kept as symlinks under /var/run/eosd/credentials/pidXXXX 
   #    (default 0)
   # EOS_FUSE_PIDMAP=0
   
    
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

