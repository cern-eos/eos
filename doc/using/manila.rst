.. highlight:: rst

.. _openstack-manila:

Openstack Manila
================

To manage manila shares from OpenStack you need to enable the EOS GRPC service. See :ref:`grpc_reference`

Once the GRPC service is up and running you have to configure accordingly an authorization key mapping to a sudoer account.

To enable support for openstack you have to create a proc directory entry:

.. code-block:: bash

   eos mkdir /eos/<instance>/proc/openstack

and define the prefix directory where you want openstack to create shares e.g.

.. code-block:: bash

   eos mkdir -p /eos/<instance>/openstack/
   eos attr set manila.prefix /eos/<instance>/openstack/

You can restrict the maximum allowed quota per share in GB by defining:

.. code-block:: bash 

   eos attr set manila.max_quota=100 /eos/<instance>/openstack/

By default each share will assign user quota to the project creator on the share prefix directory. If you want to use project quota on the share directory itself you can define:

.. code-block:: bash

   eos attr set manila.project=1 /eos/<instance>/openstack/

If you want to have sticky ownership in the share directory by default you can define:

.. code-block:: bash

   eos attr set manila.owner.auth=1 /eos/<instance>/openstack/


If you don't want your share directories having a prefix directory of the first letter of each share creator you can define ( the default is a letter prefix ) :

.. code-block:: bash

   eos attr set manila.letter.prefix=0 /eos/<instance>/openstack/

		
If you want to allow deletion of created shares via the API you have to define:

.. code-block:: bash 

   eos attr set manila.deletion=1 /eos/<instance>/openstack/ 


