.. highlight:: rst

.. index::
   single: Rest API Formatting



Formatting
==========

EOS distinguish user and admin queries. User relevant queries require not particular role in EOS. Admin queries require to have the admin role or some even require the root role.

Admin Queries
++++++++++++++

.. code-block:: text

   #root
   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.format=json
     &eos.ruid=0
     &eos.rgid=0
     ...

   #admin
   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.format=json
     &eos.ruid=3
     &eos.rgid=4
     ...


User Queries
++++++++++++

.. code-block:: text

   http://<host>:8000/proc/user/ | root://<host>//proc/user/
     ?mgm.format=json
     ...

JSON Output
++++++++++++

The JSON object contains a tag for errors messages 'errormsg' and the return code of the command. The response object is found under the key names of the command/subcommand executed e.g.

.. code-block:: text

   curl "http://localhost:8000/proc/admin/?mgm.cmd=foo&mgm.subcmd=bar&eos.ruid=0&eos.rgid=0&mgm.format=json"
   {
     "errormsg" : "error: no such admin command 'foo'",
     "foo" : 
     {
       "bar" : null
     },
     "retc" : 22
    }


   curl "http://localhost:8000/proc/admin/?mgm.cmd=foo&eos.ruid=0&eos.rgid=0&mgm.format=json"
   {
     "errormsg" : "error: no such admin command 'foo'",
     "foo" : null,
     "retc" : 22
    }
