.. index::
   single: Enable kerberos security

.. _eos_admin_krb5:

Enable kerberos security
========================

First you need setup user maping (via /etc/passwd, ldap, ...) on MGM and your gol is to see for example user info

.. note::
   
   In this case i have mvala user from alice group.

.. code-block:: text

   [root@eos-head-iep-grid ~]# id mvala
   uid=10000(mvala) gid=10000(alice) groups=10000(alice)

Please install krb5 packages

.. code-block:: text
   
   yum install krb5-workstation

Then you need to ask kerberos admin to create "host/<mgm hostname>@EXAMPLE.COM", where EXAMPE.COM is your REALM (like CERN.CH, SASKE.SK, ...) and create 
keytab file, for example krb5.keytab. Then you need to save this file at /etc/krb5.keytab on MGM node. To test it you can use ktutil command. Following example 
is showing keytab needs to be used on MGM host eos-head-iep-grid.saske.sk

.. code-block:: text

   [root@eos-head-iep-grid ~]# ktutil 
   ktutil:
   ktutil:  read_kt /etc/krb5.keytab
   ktutil:  list
   slot KVNO Principal
   ---- ---- ---------------------------------------------------------------------
      1    2 host/eos-head-iep-grid.saske.sk@SASKE.SK
      2    2 host/eos-head-iep-grid.saske.sk@SASKE.SK
      3    2 host/eos-head-iep-grid.saske.sk@SASKE.SK
      4    2 host/eos-head-iep-grid.saske.sk@SASKE.SK

Then on MGM in /etc/xrd.cf.mgm you shoudld have following line 

.. code-block:: text

   sec.protocol krb5 -exptkn:/var/eos/auth/krb5#<uid> host/<host>@EXAMPLE.COM
   
   sec.protbind * only krb5 sss unix
   
To enable krb5 security do

.. code-block:: text
   
   eos -b vid enable krb5

