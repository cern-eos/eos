.. index::
   single: Client Configuration

.. _eos_client_configure:

EOS client configuration
========================

You need to setup two variables in your environment to direct the **eos** CLI to connect to a given EOS instance.

.. code-block:: text

   export EOS_MGM_URL="root://<mgm hostname>"
   export EOS_HOME="<home dir in eos space>"


For example for MGM at eosfoo.ch and home directory in EOS for user bar /eos/foo/users/b/bar/

.. code-block:: text

   export EOS_MGM_URL="root://eosfoo.ch"
   export EOS_HOME="/eos/foo/users/b/bar/"

and now we can invoce the CLI

.. code-block:: text

   me@myhost.ch ~ $ eos
   # ---------------------------------------------------------------------------
   # EOS  Copyright (C) 2018 CERN/Switzerland
   # This program comes with ABSOLUTELY NO WARRANTY; for details type `license'.
   # This is free software, and you are welcome to redistribute it 
   # under certain conditions; type `license' for details.
   # ---------------------------------------------------------------------------
   EOS_INSTANCE=eosfoo.ch
   EOS_SERVER_VERSION=4.2.18 EOS_SERVER_RELEASE=1
   EOS_CLIENT_VERSION=4.2.18 EOS_CLIENT_RELEASE=3
   EOS Console [root://eosfoo.ch] |/> cd
   EOS Console [root://eosfoo.ch] |/eos/foo/users/b/bar/>

And now you are ready to use any command in EOS. E.G.  find all files under the /eos/foo/users/b/bar/ directory

.. code-block:: text

   EOS Console [root://eosfoo.ch] |/> find -f /eos/foo/users/b/bar/
   /eos/foo/users/b/bar/test.txt

You can also mount EOS using FUSE mount from eosfoo.ch. First you need to install the eos-fuse RPM and edit /etc/sysconfig/eos on your client machine

.. note::

   To install eos-fuse follow instruction at :ref:`eos_base_install`

.. code-block:: text
   
   root@lx001.saske.sk ~ $ cat /etc/sysconfig/eos
   # ------------------------------------------------------------------
   # FUSE Configuration
   # ------------------------------------------------------------------
   # The mount directory for 'eosd'
   export EOS_FUSE_MOUNTDIR=/eos/
   # The MGM host from where to do the inital mount
   export EOS_FUSE_MGM_ALIAS=eosfoo.ch
   

You can start the mount as the **eosd** service

.. code-block:: text

   # in sl5 and sl6
   service eosd start
   # in CentOS 7 or in fedora 18 and above
   systemclt start eosd


