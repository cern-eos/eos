chown
-----

.. code-block:: text

   chown [-r] <owner>[:<group>] <path>
      chown [-r] :<group> <path>
   '[eos] chown ..' provides the change owner interface of EOS.
   <path> is the file/directory to modify, <owner> has to be a user id or user name. <group> is optional and has to be a group id or group name.
   To modify only the group use :<group> as identifier!
   Remark: EOS does access control on directory level - the '-r' option only applies to directories! It is not possible to set uid!=0 and gid=0!
.. code-block:: text

   Options:
      -r : recursive
