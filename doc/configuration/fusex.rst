.. highlight:: rst

.. index::
   pair: Mounting EOS; FUSEx


FUSEx Client/Server
===================

The FUSEx client is a more posix confirm and performant re-implementation of the FUSE client. It allows to access EOS as a mounted file system.

There a two FUSEx client modes available:

.. epigraph::

   ========= ===== ===================================================================
   daemon    user  description
   ========= ===== ===================================================================
   eosxd     !root An end-user private mount which is not shared between users 
   eosxd     root  A system-wide mount shared between users
   ========= ===== ===================================================================


The MGM requires an additional open port (default 1100) to distribute callbacks to clients using ZMQ as distribution network.

Limiting Server Side FUSEx access
-----------------------------------

**eosxd** client rates  can be limited using the rate limiter interface available via the **access** command in the CLI.

.. code-block:: bash

   # limit the access for listing to 100 Hz per user
   eos access set limit 100 rate:user:\*:Eosxd::prot::LS

   # limit the access for stats to 1000 Hz per user
   eos access set limit 1000 rate:user:\*:Eosxd::prot::STAT

   # limit the access for returning list entries to 10 kHz per user
   eos access set limit 10000 rate:user:\*:Eosxd::ext::LS-Entry

   # limit the access for meta-data updates to 1 kHz per user
   eos access set limit 1000 rate:user:\*:Eosxd::prot::SET
   

LS, STAT and SET limits are applied by the corresponding server side protocol methods. LS-Entry is applied when another LS call is requested. Please note the difference in the naming of the **prot** and **ext** counter types.
