.. highlight:: rst

Third Party Copy
================

EOS Beryl and XRootD >=3.3.x supports native third party copy.

You can run a third party copy using the **xrdcp** command:

.. code-block:: bash

   xrdcp --tpc only root://eosinstance.foo.bar//eos/instance/myfile.root root://xrootd.foo.bar//data01/myfile.root

asdf

.. warning::
   EOS can read a file via third party copy from any instance using **unix** or
   no authentication on the storage server. EOS can not write a file via third
   party copy from other instances if they enable any kind of authentication on 
   server node without **sss** and a shared keytab entry between both instances.
