.. index::
   single: Configuration

.. highlight:: rst

.. _configuration:

Configuration
=============


EOS has two parts of configuration:

.. epigraph::

   ======================== ==================================================================================================
   Configuration File Types Description
   ======================== ==================================================================================================
   **External** config file shared library location implementing a component and **static** configuration
                              - these files are typically once edited and during the lifetime of an instance very infrequently changed - a change of these files **always requires a service restart**!

   **Internal**             configuration of filesystems, policies, mappings aso 
                              - these are stored using the EOS configuration engine inside EOS itself - any change is immediately active and **never requires a server restart**!
   ======================== ==================================================================================================

There are two methods to store external configuration: the classical EOS4 and the newer EOS5 approach. The EOS5 configuration aims to make the configuration more transparent and simpler using variable substitution for hostnames aso.

* EOS4/Classical configuration 
* EOS5/New configuration

**NOTE**
EOS5 configuration is not (yet) used in production instances at CERN.

.. index::
   pair: Configuration; Classical

Classical Configuration
-----------------------

The classical configuration includes the following configuration files:

.. epigraph::

   ============== ============================ ==========================
   Service        Configuration File           Type
   ============== ============================ ==========================
   MGM            /etc/xrd.cf.mgm              XRootD
   FST            /etc/xrd.cf.fst              XRootD
   MQ             /etc/xrd.cf.mq               XRootD
   QDB            /etxrootd/xrootd-quarkdb.cfg XRootD
   ALL            /etc/sysconfig/eos_env       Sysconfig
   ============== ============================ ==========================


You can find an example `sysconfig` file with explanation of configuration variables under `/etc/sysconfig/eos_env.example`
The configuration files coming from an RPM installation have useful default settings. The main changes required in the static xrootd configuration files are concerning plug-ins (e.g. add HTTP(s) plug-ins) or authentication mechanisms and their logical ordering. Individual configuration settings are picked up in topical chapters. 

.. index::
   pair: Configuration; Systems - Classical

Classical `Systemd` Services
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The systemctl services for the four types of daemon are:

.. code-block:: bash
    
  systemctl start|stop|status xrootd@quarkdb
  systemctl start|stop|status eos@mq
  systemctl start|stop|status eos@mgm
  systemctl start|stop|status eos@fst

The logical startup order justified by their dependencies should be: 

1. QDB
2. MQ
3. MGM
4. FSTs

The MGM service requires QDB and MQ to be up.
The FST Service requires MGM, QDB and MQ to be up.
QDB and MQ service have no dependencies.

Start `systemd` Services at Boot
"""""""""""""""""""""""""""""""""""
To start EOS services when a machine boots one needs only to enable the corresponding `systemd` service:

.. code-block:: bash 

  systemctl enable xrootd@quarkdb
  systemctl enable eos@mq
  systemctl enable eos@mgm
  systemctl enable eos@fst

.. index::
   pair: Configuration; Log Files - Classical

Classical Service Logfiles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. epigraph::

   ========= ================================= ======
   Service   Logfile Location                  Type 
   ========= ================================= ======
   MGM       /var/log/eos/mgm                  EOS  
   FST       /var/log/eos/fst                  EOS  
   MQ        /var/log/eos/mq                   EOS  
   QDB       /var/log/xrootd/quarkdb           QDB  
   ========= ================================= ======

The EOS logfiles are usually called xrdlog.(service). In the MGM directory there are sublogfiles, which filter out log lines from the main logfile:
 
 .. epigraph::

    ============================= ============================================================================
    Logfile                       Contents
    ============================= ============================================================================
    Balancer.log                  : log information for the balancer service
    Clients.log                   : log information for client calls to the MGM
    Converter.log                 : log information of the converter service
    DrainJob.log                  : log information for draining
    eosxd-logtraces.log           : traces requested using 'eos fusex evict ... sendlog' from clients
    eosxd-stacktraces.log         : traces requested using 'eos fusex evict ... stracktrace' from clients
    error.log                     : all message with ERROR level from FSTs
    FileInspector.log             : log information for the file inspector service
    GeoBalancer.log               : log information for the GEO balancer service
    GeoTreeEngine.log             : log information for the GEO tree engine
    GroupBalancer.log             : log information for the group balancer
    GroupDrainer.log              : log information for the group drainer
    Grpc.log                      : log information for the GRPC server
    Http.log                      : log information for the HTTP(S) server
    logbook.log                   : annotated commands stored in the logbook on user request
    LRU.log                       : log information for the LRU service
    Master.log                    : log information for HA master transitions
    MetadataFlusher.log           : log information for the metadata flusher
    Mounts.log                    : log information for FUSE mount/umount
    OAuth.log                     : log information for OAUTH authentication
    Recycle.log                   : log information for the Recycle (purging) service
    ReplicationTracker.log        : log information for the Replication tracker service
    WFE::Job.log                  : log information for Workflow Engine jobs
    WFE.log                       : log information for the Workflow Engine
    xrdlog.mgm                    : main log file with all log messages
    ZMQ.log                       : log information for the ZMQ server
    ============================= ============================================================================


.. index::
   pair: Configuration; eos5


EOS5 Configuration
-------------------

.. index::
   pair: Configuration; Configuration Files - eos5

Configuration Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The configuration files for the EOS5 service management are located under `/etc/eos/`

.. code-block:: 

    [root@mgm root]# find /etc/eos/config/
    /etc/eos/config/
    /etc/eos/config/mgm
    /etc/eos/config/mgm/mgm
    /etc/eos/config/mq
    /etc/eos/config/mq/mq
    /etc/eos/config/fst
    /etc/eos/config/fst/fst
    /etc/eos/config/qdb
    /etc/eos/config/qdb/qdb
    /etc/eos/config/generic
    /etc/eos/config/generic/all



Configuration Sections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
They are internally structured into generic sections `init` `sysonfig`:

.. code-block::

    # ------------------------------------------------------------ #
    [init]
    # ------------------------------------------------------------ #


.. code-block::

    # ------------------------------------------------------------ #
    [sysconfig]
    # ------------------------------------------------------------ #


.. code-block::

    # ------------------------------------------------------------ #
    [unshare]
    # ------------------------------------------------------------ #


and daemon specific sections:

.. code-block::

    # ------------------------------------------------------------ #
    [mgm:xrootd:mgm]
    # ------------------------------------------------------------ #


.. code-block::
    
    # ------------------------------------------------------------ #
    [fst:xrootd:fst]
    # ------------------------------------------------------------ #


.. code-block::

    # ------------------------------------------------------------ #
    [mq:xrootd:mq]
    # ------------------------------------------------------------ #

.. code-block::

    # ------------------------------------------------------------ #
    [qdb:xrootd:qdb]
    # ------------------------------------------------------------ #

The first tag inside `[daemon:xrootd:name]` `qdb` `mq` `fst` `mgm` references the daemon where this applies. The second tag `xrootd` reflects that this is actually part of the XRootD configuration file generated for the respective daemon type. The last tag is the `name` of the daemon instance. It is possible to run one or several of each daemon type per machine. The default `name` is just the daemon type itself e.g. `qdb` daemon default name is `qdb`. On cane have several instances of one type of daemon e.g. `mgm1` `mgm2` `mgm3` `fst1` `fst2` `fst3` aso.

### Daemon Startup
If you want to see the config for a specific daemon you can type:

.. code-block::

    [root@mgm root] eos daemon config fst fst  # show the configuration for the fst daemon and the fst instance name fst

This spits out the three active sections for init, sysconfig and xrootd configuration:

.. code-block::

    [root@mgm root] eos daemon config mq mq
    # ---------------------------------------
    # ------------- i n i t -----------------
    # ---------------------------------------
    mkdir -p /var/run/eos/
    chown daemon:root /var/run/eos/
    if [ -e /etc/eos.keytab ]; then chown daemon /etc/eos.keytab ; chmod 400 /etc/eos.keytab ; fi
    mkdir -p /var/eos/md /var/eos/report
    chmod 755 /var/eos /var/eos/report
    mkdir -p /var/spool/eos/core/mgm /var/spool/eos/core/mq /var/spool/eos/core/fst /var/spool/eos/core/qdb /var/spool/eos/admin
    mkdir -p /var/log/eos
    chown -R daemon /var/spool/eos
    find /var/log/eos -maxdepth 1 -type d -exec chown daemon {} \;
    find /var/eos/ -maxdepth 1 -mindepth 1 -not -path "/var/eos/fs" -not -path "/var/eos/fusex" -type d -exec chown -R daemon {} \;
    chmod -R 775 /var/spool/eos
    mkdir -p /var/eos/auth /var/eos/stage
    chown daemon /var/eos/auth /var/eos/stage
    setfacl -m default:u:daemon:r /var/eos/auth/

    # ---------------------------------------
    # ------------- s y s c o n f i g -------
    # ---------------------------------------
    SERVER_HOST=...
    INSTANCE_NAME=eosdev
    GEO_TAG=local
    EOS_XROOTD=/opt/eos/xrootd/
    LD_LIBRARY_PATH=/opt/eos/xrootd//lib64
    LD_PRELOAD=/usr/lib64/libjemalloc.so

    # ---------------------------------------
    # ------------- x r o o t d  ------------
    # ---------------------------------------
    # running config file: /var/run/eos/xrd.cf.mq
    xrootd.fslib libXrdMqOfs.so
    all.export /eos/ nolock
    all.role server
    xrootd.async off nosf
    xrootd.seclib libXrdSec.so
    sec.protocol sss -c /etc/eos.keytab -s /etc/eos.keytab
    sec.protbind * only sss
    xrd.sched mint 16 maxt 1024 idle 128
    xrd.port 1097
    xrd.network keepalive
    xrd.timeout idle 120
    mq.maxmessagebacklog 100000
    mq.maxqueuebacklog 50000
    mq.rejectqueuebacklog 100000
    mq.trace low
    mq.queue /eos/
    #########################################


Init Sections
"""""""""""""""""""""""""""""
The `init` section are shell commands which are executed on startup. The default `init` sections create some of the required directories and change ownership accordingly. The `init` section of QDB also initializes a new QDB database automatically.
Commands which should be executed for all daemons you put into `/etc/eos/config/generic/all`. Commands to be executed for a specific daemon you put into the daemon config file e.g. `/etc/eos/config/qdb/qdb`.

Sysconfig Sections
"""""""""""""""""""""""""""""
The `sysconfig` section contains variable definitions e.g. `/etc/eos/config/generic/all` contains:

.. code-block:: bash

    # ------------------------------------------------------------ #
    [sysconfig]
    # ------------------------------------------------------------ #

    # EOSHOST is replaced by the eos CLI with the current hostname
    SERVER_HOST=${EOSHOST}
    INSTANCE_NAME=eosdev
    GEO_TAG=local
    

The configuration file syntax allows, that they can work on several hosts without changing host names etc. In this example you see that when you want to reference the machine where you run this command, you just use the variable `${EOSHOST}`, so that you don't have to write myhost1.foo myhost2.foo depending on the machine name. This is also the place where you define the name of your instance.

Unshare Section
"""""""""""""""""""""""""""""
The `unshare` section can be used to create a private mount namespace *inside* the environment of any XRootD process. This is useful if you want to mount a remote filesystem for FSTs, which are only visible to the FST process mount namespace, but to nobody else on the machine itself. A `df` as root will not show this external mount. You just write the needed `mount` commmand into the `init` section and it will be executed on daemon startup. It is possible also to encrypt commands in the `init` section, in case you have to use a mount key. To get an encrypted command for init sections you use:

.. code-block:: bash
        
    eos daemon seal "mount -t nfs ... /nfs/"
    enc:fmAWznYjTWqRGfeiDSpfQy3MzQpJOhVI

and you would place `enc:fmAWznYjTWqRGfeiDSpfQy3MzQpJOhVI` into your `init` section.

Service Management
"""""""""""""""""""""""""""""

The `systemd` command set to start each single daemon manually on a node is:
.. code-block:: 

    systemctl start eos5-qdb@qdb
    systemctl start eos5-mq@mq
    systemctl start eos5-mgm@mgm
    systemctl start eos5-fst@fst

The syntax is `eos5-daemon@name` e.g. start fst daemon with name fst1: `systemctl start eos5-fst@fst1`

To enable all daemon on startup, you do:

.. code-block:: bash

    systemctl enable eos5-qdb@qdb
    systemctl enable eos5-mq@mq
    systemctl enable eos5-mgm@mgm
    systemctl enable eos5-fst@fst

.. index::
   pair: Configuration; Log Files - eos5

EOS5 Service Logfiles
^^^^^^^^^^^^^^^^^^^^^^

.. epigraph::

   ========= ================================= ======
   Service   Logfile Location                  Type 
   ========= ================================= ======
   MGM       /var/log/eos/mgm                  EOS  
   FST       /var/log/eos/fst                  EOS  
   MQ        /var/log/eos/mq                   EOS  
   QDB       /var/log/eos/qdb                  QDB  
   ========= ================================= ======


The EOS logfiles are usually called xrdlog.(service). In the MGM directory there are sublogfiles, which filter out log lines from the main logfile:

 .. epigraph::

    ============================= ============================================================================
    Logfile                       Contents
    ============================= ============================================================================
    Balancer.log                  : log information for the balancer service
    Clients.log                   : log information for client calls to the MGM
    Converter.log                 : log information of the converter service
    DrainJob.log                  : log information for draining
    eosxd-logtraces.log           : traces requested using 'eos fusex evict ... sendlog' from clients
    eosxd-stacktraces.log         : traces requested using 'eos fusex evict ... stracktrace' from clients
    error.log                     : all message with ERROR level from FSTs
    FileInspector.log             : log information for the file inspector service
    GeoBalancer.log               : log information for the GEO balancer service
    GeoTreeEngine.log             : log information for the GEO tree engine
    GroupBalancer.log             : log information for the group balancer
    GroupDrainer.log              : log information for the group drainer
    Grpc.log                      : log information for the GRPC server
    Http.log                      : log information for the HTTP(S) server
    logbook.log                   : annotated commands stored in the logbook on user request
    LRU.log                       : log information for the LRU service
    Master.log                    : log information for HA master transitions
    MetadataFlusher.log           : log information for the metadata flusher
    Mounts.log                    : log information for FUSE mount/umount
    OAuth.log                     : log information for OAUTH authentication
    Recycle.log                   : log information for the Recycle (purging) service
    ReplicationTracker.log        : log information for the Replication tracker service
    WFE::Job.log                  : log information for Workflow Engine jobs
    WFE.log                       : log information for the Workflow Engine
    xrdlog.mgm                    : main log file with all log messages
    ZMQ.log                       : log information for the ZMQ server
    ============================= ============================================================================









 

