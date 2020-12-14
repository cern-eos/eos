.. highlight:: rst
.. index::
   single: Developing EOS

Develop
=======================

.. image:: cpp.jpg
   :scale: 40%
   :align: left

Source Code
-------------------
For development clone the GIT source tree ...

.. code-block:: bash

   git clone https://gitlab.cern.ch/dss/eos.git

   cd eos
   git submodule update --recursive --init
   
Create a build directory ...

.. code-block:: bash

   mkdir build
   cd build

Dependencies
----------------
.. warning:: Before compilation of the master branch you have to make sure that you installed all required dependencies.

There is a convenience scripts to install all dependencies in the EOS source tree:

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

