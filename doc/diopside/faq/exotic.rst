.. index::
   pair: FAQ; Exotic Cases

.. highlight:: rst

.. _exotic:

Exotic Cases
============

I need to change the hostname or the port of an FST
---------------------------------------------------

If you need to rename an FST host or change the port number, you can use the `eos-config-inspect` tool.

1. *Shutdown* your MGM

2. *Modify* the configuration of each concerned filesystem by filesystem id

To relocate filesystem 1 to foo.bar:1095 having QDB on `localhost:7777` you do:


.. code-block:: bash 

   eos-config-inspect relocate-filesystem --fsid 1 --new-fst-host foo.bar --new-fst-port 1095 --members localhost:7777

3. *Verify/Rewrite* your *vid* rules, in case the hostname was used there 
3. *Restart* your MGM
4. **Ready**
