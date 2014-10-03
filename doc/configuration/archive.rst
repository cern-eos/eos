.. highlight:: rst

.. index::
   single: Archive; Configuration


Archive
=======

The archive daemon is managing the transfer of files to/from EOS from/to a
remote location offering an XRootD interface. Before starting the new service
there are a few configuration parameters that need to be set.

Daemon configuration
+++++++++++++++++++++
First of all, one needs to set the user account under which the daemon will be
running by modifying the script located at ``/etc/sysconfig/eosarchived``. The
daemon needs to create a series of temporary files during the transfers which
will be saved at the location pointed by the environment variable **EOS_ARCHIVE_DIR**.
Also, the location where the daemon log file is saved can be changed by modifying
the variable **LOG_DIR**.

If none of the above variables is modified then the default configuration is as
follows:

.. code-block:: bash

  export USER=eosarchi
  export GROUP=c3
  export EOS_ARCHIVE_DIR=/var/eos/archive/
  export LOG_DIR=/var/log/eos/archive/

The variables set in ``/etc/sysconfig/eosarchived`` are general ones that apply
to both the daemon process and the individual tranfer proceses spawned later on.

Another configuration file which holds further configurable values is
``/etc/eosarchived.conf``. The advantage of this file is that it can be modified
while the daemon is running and newly submitted transfers will pick-up the new
configuration without the need of a full daemon restart.

The **LOG_LEVEL** variable is set in ``/etc/eosarchived.conf`` and  must be a
string corresponding to the syslog loglevel. The **eosarchived** daemon logs
are saved by default in ``/var/log/eos/archive/eosarchive.log``.

MGM Configuration
+++++++++++++++++

The configuration file for the **MGM** node contains a new directive called
**mgmofs.archivedir** which needs to point to the same location as the
**EOS_ARCHIVE_DIR** defined earlier for the **eosarchived** daemon. The two
locations must match because the **MGM** and the **eosarchived** daemons
communicate between each other using ZMQ and this is the path where any common
ZMQ files are stored.

.. code-block:: bash

  mgmofs.archivedir /var/eos/archive/  # has to be the same as EOS_ARCHIVE_DIR from eosarchived

Another variable that needs to be set for the **MGM** node is the location where
all the archived directories are saved. Care should be taken so that the user
name under which the **eosarchived** daemon runs, has the proper rights to read
and write files to this remote location. This envrionment variables can be set in
the ``/etc/sysconfig/eos`` file as follows:

.. code-block:: bash

  export EOS_ARCHIVE_URL=root://castorpps.cern.ch//castor/cern.ch/archives/

Keytab file generation
++++++++++++++++++++++

Assuming that the **eosarchived** daemon is running under the account *eosarchi*,
then one has to make sure the following files are present at the **MGM**. First of
all, the eos-server.keytab file must include a new entry for the **eosarchi**:

.. code-block:: console

     [root@dev doc]$ ls -lrt /etc/eos-server.keytab
     -r--------. 1 daemon daemon 137 Mar 22  2012 /etc/eos-server.keytab

     [root@dev ~]# xrdsssadmin list /etc/eos-server.keytab
     Number Len Date/Time Created Expires  Keyname User & Group
     ------ --- --------- ------- -------- -------
	  2  32 09/17/14 19:25:01 -------- archive eosarchi c3
	  1  32 09/17/14 19:24:47 -------- eosinst daemon daemon

The next file that needs to be present is the eos-archive.keytab file which is
going to be used by the **eosarchived** daemon.

.. code-block:: console

     [root@dev ~]# ls -lrt /etc/eos-archive.keytab
     -r--------. 1 eosarchi c3 133 Sep 18 09:48 /etc/eos-archive.keytab

     [root@dev ~]# xrdsssadmin list /etc/eos-archive.keytab
     Number Len Date/Time Created Expires  Keyname User & Group
     ------ --- --------- ------- -------- -------
	  2  32 09/17/14 19:25:01 -------- archive eosarchi c3

Some important notes about the **eos-archive.keytab** file:

- if renamed or saved in a different location then the variable **XrdSecSSSKT**
  from ``/etc/sysconfig/eosarchived`` needs to be updated to point to the new
  location/name
- the permissions on this keytab file must match the identity under which the
  eoarchived daemon is running

Futhermore, the **eosarchi** user needs to be added to the **sudoers** list in
EOS so that it can perform any type of operation while creating or transfering
archives.

.. code-block:: console

    EOS Console [root://localhost] |/eos/> vid set membership eosarchi +sudo

As far as the **xrd.cf.mgm** configuration file is concerned, one must ensure
that **sss** authentication has precedence over **unix** when it comes to local
connections:

.. code-block:: bash

   sec.protbind localhost.localdomain sss unix
   sec.protbind localhost             sss unix
