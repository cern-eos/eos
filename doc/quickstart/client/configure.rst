.. index::
   single: Client Configuration

.. _eos_client_configure:

EOS client configuration
========================

You need to setup two variables

.. code-block:: text

   export EOS_MGM_URL="root://<mgm hostname>"
   export EOS_HOME="<home dir in eos space>"


For example for MGM at eos-head-iep-grid.saske.sk and home directory in eos for mvala user /eos/saske.sk/users/m/mvala

.. code-block:: text

   export EOS_MGM_URL="root://eos-head-iep-grid.saske.sk"
   export EOS_HOME="/eos/saske.sk/users/m/mvala"

and now we can run eos

.. code-block:: text

   mvala@lx001.saske.sk ~ $ eos
   # ---------------------------------------------------------------------------
   # EOS  Copyright (C) 2011 CERN/Switzerland
   # This program comes with ABSOLUTELY NO WARRANTY; for details type `license'.
   # This is free software, and you are welcome to redistribute it 
   # under certain conditions; type `license' for details.
   # ---------------------------------------------------------------------------
   EOS_INSTANCE=eossaske.sk
   EOS_SERVER_VERSION=0.2.29 EOS_SERVER_RELEASE=1
   EOS_CLIENT_VERSION=0.2.29 EOS_CLIENT_RELEASE=3
   EOS Console [root://eos-head-iep-grid.saske.sk] |/> cd
   EOS Console [root://eos-head-iep-grid.saske.sk] |/eos/saske.sk/users/m/mvala/>

And now you are ready to use any command in eos for example find all files in /eos/saske.sk/users/m/mvala/ dir

.. code-block:: text

   EOS Console [root://eos-head-iep-grid.saske.sk] |/> find -f /eos/saske.sk/users/m/mvala/
   /eos/saske.sk/users/m/mvala/test.txt

You can also mount as FUSE mount for eos-head-iep-grid.saske.sk. First you need to install eos-fuse and edit /etc/sysconfig/eos on client machine

|more| 

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
   export EOS_FUSE_MGM_ALIAS=eos-head-iep-grid.saske.sk
   

And let's start eosd service

.. code-block:: text

   # in sl5 and sl6
   service eosd start
   # or in fedora 18 and above
   systemclt start eosd

