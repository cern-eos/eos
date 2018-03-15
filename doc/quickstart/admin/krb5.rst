.. index::
   single: Kerberos Security 

.. _eos_admin_krb5:

Enabling kerberos security
==========================

The initial requirement is that your local LINUX accounts correspond to kerberos principal names.

To start install krb5 packages

.. code-block:: text
   
   yum install krb5-workstation

Then you need to ask kerberos admin to create "host/<mgm hostname>@EXAMPLE.COM", where EXAMPE.COM is your REALM (like CERN.CH, SASKE.SK, ...) and create 
a keytab file, for example krb5.keytab. The keytab file is stored under /etc/krb5.keytab on the MGM node. To test it you can use ktutil command. The following example is showing keytab contents to be used on MGM host eosfoo.bar.ch@BAR.CH

.. code-block:: text

   [root@eosfoo.bar.ch ~]# ktutil 
   ktutil:
   ktutil:  read_kt /etc/krb5.keytab
   ktutil:  list
   slot KVNO Principal
   ---- ---- ---------------------------------------------------------------------
      1    2 host/eosfoo.bar.ch@BAR.CH
      2    2 host/eosfoo.bar.ch@BAR.CH
      3    2 host/eosfoo.bar.ch@BAR.CH
      4    2 host/eosfoo.bar.ch@BAR.CH

On the MGM in ``/etc/xrd.cf.mgm`` you have to enable kerberos 5 authentication

.. code-block:: text

   sec.protocol krb5 host/<host>@EXAMPLE.COM
   
   sec.protbind * only krb5 sss unix
   
To enable krb5 security mapping of user names you do

.. code-block:: text
   
   eos -b vid enable krb5

