.. highlight:: rst

.. _attributelocks:

Using extended attribute locks
========================================

An extended attribute lock is a simple mechanism to block file opens on locked files to foreigners. Foreigners are not owners. The owner is defined by the username and the application name. 
So if any of these differs a client is considered a foreigner. 

We define two types of locks:
- exclusive : no foreigner can open a file with an exclusive lock for reading or writing
- shared    : foreigner can open a file with an exclusive lock in case they are reading

Shared attribute locks are currently not exposed in the CLI.

To create an exclusive extended attribute lock you do:

.. code-block:: bash

   # create a lock
   eos -r 100 100 -a myapp file touch -l /eos/dev/lockedfile

   # the owner can read
   eos -r 100 100 -a myapp cp /eos/dev/lockedfile      - # will succeed
  
   # a foreigner can not read
   eos -r 101 101 -a myapp cp /eos/dev/lockedfile      - # will fail
   eos -r 100 100 -a anotherapp cp /eos/dev/lockedfile - # will fail 

   # create a lock with a given liftime e.g. 1000s 
   eos -r 100 100 -a myapp file touch -l /eos/dev/lockedfile 1000

By default locks are taken for 24h. The lifetime can be specified as seen before if needed.

You can remove a lock if you are the owner by doing:

.. code-block:: bash

   # remove a lock
   eos -r 100 100 -a myapp file touch -u /eos/dev/lockedfile


The internal representation of an attribute lock is given here:

.. code-block:: bash

   attr ls /eos/dev/lockedfile | grep sys.app.locks
   sys.app.lock="expires:1665042101,type:exclusive,owner:daemon:"

The high-level functionality for creating/deletion of attribute locks can be circumvented by creating/deleting the *sys.app.locks* attribute using extended attribute interfaces.
