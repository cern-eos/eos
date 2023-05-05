.. highlight:: rst

.. index::
   single: Converter Engine

Converter Engine
================

The Converter Engine is the EOS component responsible for scheduling
and performing file conversion jobs. A conversion job means rewriting a file
with a different storage parameter: layout, replica number, space
or placement policy.

The Converter Engine is split into two main components:
*Converter Driver* and *Converter Scheduler*.

.. note::

  The Converter Engine is compatible only with the QuarkDB
  namespace implementation. In case the in-memory namespace is used,
  the service will not start at MGM boot-up.

Converter Driver
----------------

The Converter Driver is the component responsible for performing the actual
conversion job. This is done using XRootD third party copy between the FSTs.

The Converter Driver keeps a threadpool available for conversion jobs.
Periodically, it queries QuarkDB for conversion jobs, in batches of 1000. 
The retrieved jobs are scheduled, one per thread, up to a configurable 
runtime threads limit. After each scheduling, a check is performed 
to identify completed or failed jobs.
  
Successful conversion jobs:
  - get removed from the QuarkDB pending jobs set
  - get removed from the MGM in-flight jobs tracker

Failed conversion jobs:
  - get removed from the QuarkDB pending jobs set
  - get removed from the MGM in-flight jobs tracker
  - get updated to the QuarkDB failed jobs set
  - get updated to the MGM failed jobs set

Within QuarkDB, the following hash sets are used:

.. code-block:: bash

  eos-conversion-jobs-pending
  eos-conversion-jobs-failed

Each hash entry has the following structure: *<fid>:<conversion_info>*.

Conversion Info
+++++++++++++++

A conversion info is defined as following:

.. code-block:: bash

  <fid(016hex)>:<space[.group]>#<layout(08hex)>[~<placement>]

    <fid>       - 16-digit with leading zeroes hexadecimal file id
    <space>     - space or space.group notation
    <layout>    - 8-digit with leading zeroes hexadecimal layout id
    <placement> - the placement policy to apply

The job info is parsed by the Converter Driver before creating 
the associated job. Entries with invalid info are simply discarded 
from the QuarkDB pending jobs set.

Conversion Job
++++++++++++++

A conversion job goes through the following steps:
  - The current file metadata is retrieved
  - The TPC job is prepared with appropriate opaque info
  - The TPC job is executed
  - Once TPC is completed, verify the new file has all fragments according to layout
  - Verify initial file hasn't changed (checksum is the same)
  - Merge the conversion entry with the initial file
  - Mark conversion job as completed

If at any step a failure is encountered, the conversion job
will be flagged as failed.

Converter Scheduler
-------------------

The Converter Scheduler is the component responsible for creating conversion jobs,
according to a given set of conversion rules. A conversion rule is placed
on a namespace entry (file or directory), contains optional filters
and the target storage parameter.

- When a conversion rule is placed on a file, an immediate conversion job is created
  and pushed to QuarkDB.
- When a conversion rule is placed on a directory, a tree traversal is initiated
  and all files which pass the filtering criteria will be scheduled for conversion.
  