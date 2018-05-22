.. highlight:: rst

.. index::
   single: ExOS-io/CEPH

ExOS
=========
ExOS is an IO plug-in for CEPH backend using librados.
The ExOS plug-in is optimized to use at least one replicated pool to store meta-data and an erasure coded pool 
to store data. The plug-in supports also update operations. Therefore the backend is also suitable to be used from FUSE(x) clients. The plug-in can be configured to export many pairs of RADOS pools, which are linked to one EOS filesystem each.


Configuration
-------------

To attach a RADOS data and metadata pool to a EOS filesystem one has to add a filesystem manually:

.. code-block:: bash

   # add ExOS backend to 'ceph' space using rados client id 'ecuser' and replicated pool 'ec1_meta' and ec-encoded pool 'ec1_data'
   eos fs add 6ae2e3c7-02c4-4101-b0cc-c051d4d066dc myfst.foo.bar:1095 exos://myfst.foo.bar:1095/ec_1?:rados.user=ecuser:rados.md=ec1_meta:rados.data=ec1_data ceph rw

   # enable 'ceph' space
   eos space set ceph on

   # force the scheduler to select the 'ceph' space in a subtree
   eos attr -r set sys.forced.space=ceph /eos/ceph/

As usual the filesystems can also be registered in the **default** space and there is no need to change the forced space attribute.

If there is no need to have individual meta/data pool combinations per filesystem one can use these environment variables in ``/etc/sysconfig/eos_env`` :

.. code-block:: bash

   EXOSIO_USER=ecuser
   EXOSIO_DATA_POOL=ec1_data
   EXOSIO_MD_POOL=ec1_md

The plug-in can be run in full debug mode by defining EXOSIO_DEBUG in ``/etc/sysconfig/eos_env``:

.. code-block:: bash

   EXOSIO_DEBUG=1


RADOS Object Structure 
----------------------

The objects in the data pool are named similiar to a local EOS filesystem:

.. code-block:: bash

   rados --id ecuser --pool ec1_meta ls
   ec1/00000000/00000d54
   ec1/00000000/00000d59
   ec1/00000000/00000d58
   ec1/00000000/00000d52
   ec1/00000000/00000d5b
   ec1/00000000/00000d55
   ec1/00000000/00000d53
   ec1/00000000/00000d57
   ec1/00000000/00000d5a
   ec1/00000000/00000d56
   EXOS/ROOT
		
EXOS/ROOT is a special object containing the internal inode counter which defines the names in the data pool. 

.. code-block:: bash

   rados --id ecuser --pool ec1_meta listomapvals EXOS/ROOT
   exos.inode
   value (16 bytes) :
   00000000  30 30 30 30 30 30 30 30  30 30 30 30 30 30 30 61  |000000000000000a|
   00000010


The regular EOS objects contain meta data as omap entries:

.. code-block:: bash

   rados --id ecuser --pool ec1_meta listomapvals ec1/00000000/00000d54
   exos.inode
   value (16 bytes) :
   00000000  30 30 30 30 30 30 30 30  30 30 30 30 30 30 30 33  |0000000000000003|
   00000010

   exos.mtime
   value (20 bytes) :
   00000000  31 35 32 36 39 38 31 30  34 32 2e 33 32 39 31 32  |1526981042.32912|
   00000010  39 33 34 30                                       |9340|
   00000014

   exos.pool
   value (10 bytes) :
   00000000  ...                                               |ec1_meta|
   0000000a

   exos.size
   value (4 bytes) :
   00000000  31 35 38 32                                       |1582|
   00000004

   user.eos.blockcxerror
   value (1 bytes) :
   00000000  30                                                |0|
   00000001

   user.eos.checksum
   value (4 bytes) :
   00000000  af 24 d2 55                                       |.$.U|
   00000004

   user.eos.checksumtype
   value (5 bytes) :
   00000000  61 64 6c 65 72                                    |adler|
   00000005

   user.eos.filecxerror
   value (1 bytes) :
   00000000  30                                                |0|
   00000001

   user.eos.lfn
   value (21 bytes) :
   00000000  2f 65 6f 73 2f 6d 75 6c  74 69 6d 67 6d 2f 65 78  |/eos/multimgm/ex|
   00000010  6f 73 2f 70 33                                    |os/p3|
   00000015

   user.eos.timestamp
   value (16 bytes) :
   00000000  31 35 32 36 39 38 31 30  34 39 39 33 33 34 33 37  |1526981049933437|
   00000010


The data pool contains one to many objects for each EOS entry stored in the meta data pool. These objects are named like 'exos-inode#extent'. The inode names here are not the EOS inodes but internal inodes to the implementation. Extent '0000' contains bytes 0-32M, extent '0001' contains bytes 32M-64M aso.


.. code-block:: bash

   rados --id ecuser --pool ec1_data ls
   0000000000000003#0000
   0000000000000008#0000
   0000000000000002#0000
   0000000000000009#0000
   0000000000000006#0000
   0000000000000004#0000
   0000000000000005#0000
   0000000000000001#0000
   000000000000000a#0000
   0000000000000007#0000   

The data pool does not store attributes/omap on objects:

.. code-block:: bash 

   rados --id ecuser --pool ec1_data listomapvals 0000000000000003#0000
   # shows nothing


Manual scanning
---------------

It is possible to run a manual FST scan on an ExOS filesystem, which will checksum files and flag them in case of errors.

.. code-block:: bash

   eos-scan-fs "exos://myfst.foo.bar:1095/?rados.md=ec1_meta:rados.data=ec1_data:rados.user=ecuser"
   # shows the usual eos-fst-scan output 
   



