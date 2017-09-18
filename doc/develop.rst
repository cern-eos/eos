.. highlight:: rst
.. index::
   single: Developing EOS

Develop
=======================


Source Code
-------------------
For development clone the GIT source tree ...

.. code-block:: bash

   git clone https://gitlab.cern.ch/dss/eos.git

Create a build directory ...

.. code-block:: bash

   cd eos
   mkdir build
   cd build


Dependencies
----------------
.. warning:: Before compilation of the master branch you have to make sure that you installed the packaged listed in the following table ...

.. epigraph::

   ===============================  =========
   Package                          Version                        
   ===============================  =========
   xrootd-server                    = 4.7                       
   xrootd-server-devel              = 4.7                       
   xrootd-private-devel             = 4.7                       
   xrootd-cl-devel                  = 4.7                       
   readline-devel                   default                        
   readline                         default
   ncurses-devel                    default                        
   ncurses-static		    default
   ncurses                          default
   libattr-devel                    default                        
   libattr                          default
   openldap-devel                   default                        
   openldap                         default
   e2fsprogs-devel                  default
   e2fsprogs                        default                        
   zlib-devel                       default                        
   openssl-devel                    default                        
   ncurses-devel                    default                        
   xfsprogs-devel                   default
   xfsprogs                         default                        
   fuse-devel                       >= 2.7  
   fuse-libs                        >= 2.7                       
   fuse                             >= 2.7                         
   leveldb-devel                    >= 1.7 
   leveldb                          >= 1.7                        
   git                              default                        
   cmake                            >= 2.8                           
   sparsehash-devel                 default
   libmicrohttpd                    EOS rpm 
   libmicrohttpd-devel              EOS rpm
   libuuid                          default
   libuuid-devel                    default
   zeromq                           default
   zeromq-devel                     default
   protobuf                         default
   protobuf-devel                   default
   perl-Time-HiRes                  default
   ===============================  =========

There are two convenience scripts to install all dependencies in the EOS source tree:

SLC6
++++
.. code-block:: bash
  
   utils/sl6-packages.sh

EL7
+++
.. code-block:: bash

   utils/el7-packages.sh

Compilation
-----------
Run *cmake* ...

.. code-block:: bash

   cmake ../


Compile the project ...

.. code-block:: bash

   make -j 4
   make install

