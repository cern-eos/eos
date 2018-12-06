.. highlight:: rst

.. index::
      pair: Logicalpath; Import

Logicalpath
===========

The way files are stored on EOS filesystems is transparent to the users.
They identify a file in the namespace either via a path
or a file id (fid / fxid). How this translates into the placement
of the file is the responsibility of the system.
So far, the physical location of files was computed from the file id.

With the introduction of the logical path setting,
filesystems may store files under a 'path-like' location.

Example:

.. code-block:: bash

  /fst/00000000/0000a12d vs /fst/eos/instance/path/filename

To activate the logical path setting,
the following command must be executed:

.. code-block:: bash

   eos fs config <fsid> logicalpath=1


The setting can be switched on or off at any time, with immediate effects.

Translation mechanism
---------------------

File created on a filesystem with the logicalpath setting on will have
an additional extended attribute ``sys.eos.lpath``
which keeps a mapping of the file's location.
The extended attribute stores string information of a file's
physical location on a given file system.

Operations regarding the file's physical path make use of a namespace
utility class which takes into account both the possibility of a logicalpath,
as well as constructing the path from the file's id.

For example, the file ``/eos/s3/testfile`` with 3 replicas,
with 2 of those using logicalpath, will look like this:

.. code-block:: bash

   > eos fileinfo /eos/s3/testfile --fullpath
   ...
   ┌───┬──────┬────────────────────────┬────────────────┬────────────────────────┬──────────┬──────────────┬────────────┬────────┬────────────────────────┬───────────────────────────────────────┐
   │no.│ fs-id│                    host│      schedgroup│                    path│      boot│  configstatus│ drainstatus│  active│                  geotag│                      physical location│
   └───┴──────┴────────────────────────┴────────────────┴────────────────────────┴──────────┴──────────────┴────────────┴────────┴────────────────────────┴───────────────────────────────────────┘
   0        1    xdc-fst1.cern.ch              default.0                    /fst1     booted             rw      nodrain   online                      xdc                 /fst1/00000013/0002f914
   1        2    xdc-fst1.cern.ch                lpath.0              /fst1_lpath     booted             rw      nodrain   online                      xdc             /fst1_lpath/eos/s3/testfile
   2        3    xdc-fst2.cern.ch                   s3.0  s3://s3.cern.ch/bucket/     booted             rw      nodrain   online                      xdc  s3://s3.cern.ch/bucket/eos/s3/testfile

   > eos attr ls /eos/s3/testfile
   sys.eos.lpath="2|/eos/s3/testfile&3|/eos/s3/testfile"

The two replicas, stored on filesystems using logicalpath,
can be seen in the file's extended attributes.
