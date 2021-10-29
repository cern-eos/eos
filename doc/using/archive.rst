.. highlight:: rst

.. index::
   single: Archive; Interface

Archive Interface
=============================

The archive interface currently has the following signature:

.. code-block:: bash

   archive <subcmd>
	   create <path>                      : create archive file
	   put [--retry] <path>               : copy files from EOS to archive location
	   get [--retry] <path>               : recall archive back to EOS
	   purge[--retry] <path>              : purge files on disk
	   transfers [all|put|get|purge|uuid] : show status of running jobs
	   list [<path>]                      : show status of archived directories in the subtree
	   kill <job_uuid>                    : kill transfer
	   help [--help|-h]                   : display help message

In order to safely archive an EOS subtree to tape (CTA) the following steps detailed in this document must
be performed. Assume we want to archive the EOS subtree rooted at /eos/dir/archive/test. First of all,
the user needs to make sure that he/she has the necessary permissions to submit archiving commands.
The permissions check is enforced at directory level by using the **sys.acl** extended attribute
and it allows setting permissions at user, group or egroup level. The **ACL flag** for achiving
is **'a'**.

.. code-block:: bash

    sys.acl="u:tguy:a"  # user tguy has the right to archive for the current directory

Once the proper permissions are in place, we need to take a snapshot of all the metadata of the
files and directories under this subtree. For this we use the **archive create** command inside
the *EOS Console*:

.. code-block:: bash

   archive create /eos/dir/archive/test

After issuing this command the EOS subtree is **immutable** and no updates are allowed either to the
data or the metadata. Transferring the data to tape (CTA) is done using the **archive put** command:

.. code-block:: bash

   archive put /eos/dir/archive/test

At any point during a transfer the user can retrieve the current status of the transfer by issuing an
**archive transfers** command. Once the transfer finishes there will be two additional files saved at
the root of the archived subtree: the **.archive.log** file with contains the logs of the last transfer
(note the 'dot' in the begining of the filename - so to list it use **ls -la** in the *EOS Console*)
and another file called **.archive.<operation>.<outcome>** where operation is one of the following:
get/put/purge and the outcome can either be **done** or **err**. If an error occurs the user has the
possibility to resubmit the transfer by using the **--retry** option.

When the put operation is successful one should find a file called **.archive.put.done** at the root
of the subtree and the user can now issue the purge command which will delete all the data from EOS
thus freeing the space.

.. code-block:: bash

    archive purge /eos/dir/archive/test

To get the data back into EOS one should use the archive get command:

.. code-block:: bash

    archive get /eos/dir/archive/test

The same conventions as before apply when it comes to the progress and the final status of the transfer.
If the user would like to retrieve the status of previously archived directories he/she can use the
**archive list** command which will return the status of all archived directories rooted at the given
directory or if no directory is given then "/" is assumed. This command displays also the running
jobs but no detailed information about them is provided - for this you should use the **archive transfers**
command.

In case the user wants to permanently delete the data saved on **tape (CTA)**, then unless he has root
privileges on the EOS instance he will need to contact one of the administrators to perform this operation.
Permanently deleting the achive will not delete any data from EOS, but only the data saved in CASTOR.
Therefore, it is the **user's responsibility** to make sure he/she first gets the data back to EOS before
requesting the deletion of the archive.
