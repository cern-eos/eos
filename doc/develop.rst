.. highlight:: rst

Develop
=======================


Source Code
-------------------
For development clone the GIT source tree ...

.. code-block:: bash

   git clone http://eos.cern.ch/repos/eos.git

Create a build directory ...

.. code-block:: bash

   cd eos
   mkdir build
   cd build

<<<<<<< HEAD

Dependencies
----------------
.. warning:: Before compilation you have to make sure that you installed the packaged listed in the following table ...

.. epigraph::

   ===============================  =========
   Package                          Version                        
   ===============================  =========
   xrootd-server                    = 3.3.3                       
   xrootd-server-devel              = 3.3.3                       
   xrootd-private-devel             = 3.3.3                       
   xrootd-cl-devel                  = 3.3.3                       
   readline-devel                   default                        
   ncurses-devel                    default                        
   libattr-devel                    default                        
   openldap-devel                   default                        
   e2fsprogs-devel                  default                        
   zlib-devel                       default                        
   openssl-devel                    default                        
   ncurses-devel                    default                        
   xfsprogs-devel                   default                        
   fuse-devel                       >= 2.7                         
   fuse                             >= 2.7                         
   leveldb-devel                    >= 1.7                         
   git                              default                        
   cmake                            2.8                           
   ===============================  =========

Compilation
-----------
Run *cmake* ...

.. code-block:: bash

   cmake ../


Compile the project ...

.. code-block:: bash

   make -j 4
   make install

