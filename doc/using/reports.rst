.. highlight:: rst

.. index::
   single: reports

.. _systemd:

Report log files
================

The EOS MGM writes report log files under `/var/eos/report/<YEAR>/<MONTH>/<YEAR><MONTH><DAY>.eosreport`

On top of a few `xrd.cf.mgm` configuration variables, it must be enabled on the MGM:

.. code-block:: bash

  EOS Console [root://localhost] |/eos/ctaatlaspps/archivetest/> io enable -r
  success: enabled IO report store


File creation/update records
----------------------------

Each FST sends for each file replica or piece it writes a record which looks like this:

``log=cb9ae364-4f7c-11e8-8a9a-02163e009ce2&path=/eos/testfile&ruid=0&rgid=0&td=root.13142:52@slc7&host=test.cern.ch
&lid=1048578&fid=1056332&fsid=1&ots=1525425804&otms=531&cts=1525425804&ctms=533&nrc=0&nwc=1&rb=0&rb_min=0&rb_max=0
&rb_sigma=0.00&rv_op=0&rvb_min=0&rvb_max=0&rvb_sum=0&rvb_sigma=0.00&rs_op=0&rsb_min=0&rsb_max=0&rsb_sum=0&rsb_sigma=0.00
&rc_min=0&rc_max=0&rc_sum=0&rc_sigma=0.00&wb=2202&wb_min=2202&wb_max=2202&wb_sigma=0.00&sfwdb=0&sbwdb=0&sxlfwdb=0
&sxlbwdb=0&nfwds=0&nbwds=0&nxlfwds=0&nxlbwds=0&rt=0.00&rvt=0.00&wt=0.01&osize=0&csize=2202&delete_on_close=0&prio_c=2&prio_l=4&prio_d=1
&sec.prot=sss&sec.name=daemon&sec.host=localhost&sec.vorg=&sec.grps=daemon&sec.role=&sec.info=&sec.app=eoscp``

.. epigraph::

   ==================== ==================================================================================================
   TAG                  Description
   ==================== ==================================================================================================
   log                  uuid to correlate log entries
   path                 logical path
   ruid                 mapped user id
   rgid                 mapped group id
   td                   trace identifier: <unix-user>.<pid>.<fd>@<host>.<domain>
   lid                  layout id
   fid                  file id
   fsid                 file system id
   ots                  open timestamp
   otms                 open time milliseconds
   cts                  close timestamp
   ctms                 close time milliseconds 
   nrc                  number of read calls
   nwc                  number of write calls
   rb                   bytes read (non vector reads)
   rb_min               smallest read call in bytes (non vector reads)
   rb_max               largest read call in bytes (non vector reads)
   rb_sigma             standard deviation of read bytes (non vector reads)  
   rv_op                number of vector operations
   rvb_min              smallest vector read in bytes
   rvb_max              largest vector read in bytes
   rvb_sum              sum of all vector read bytes
   rvb_sigma            standard deviation of vector read bytes
   rs_op                number of single reads in vector operations
   rsb_min              smallest read call in vector operations
   rsb_max              largest read call in vector operations
   rsb_sum              sum of all individual read call bytes in vector operations
   rsb_sigma            standard deviation of single read calls in vector operations
   rc_min               smallest number of read calls in vector read operations
   rc_max               largest number of read calls in vector read operations
   rc_sum               sum of all read call sin vector read operations
   rc_sigma             standard deviation of number of read calls in vector read operations
   wb                   bytes written 
   wb_min               smallest write call in bytes
   wb_max               largest write call in bytes
   wb_sigma             standard deviation of write call in bytes
   sfwdb                forward seeked bytes 
   sbwdb                backward seeked bytes
   sxlfwdb              forward seeked bytes moving at least 128kb per seek
   sxlbwdb              backward seekd bytes moving at least 128kb per seek
   nfwds                number of forward seeks
   nbwds                number of backward seeks
   nxlfwds              number of large forward seeks (>=128kb)
   nxlbwds              number of large backward seeks (>=128kb)
   rt                   time spent in ms waiting for disk reads
   rvt                  time spent in ms waiting for disk reads for vector reads
   wt                   time spent in ms waiting for disk writes
   osize                size of the file when opening
   csize                size of the file when closing
   delete_on_close      flag indicating delete on close status
   prio_c               IO priority class (0:none 1:realtime 2:best effort 3:idle)
   prio_l               IO priority level 0..7
   prio_d               1: default values (best effort level 4) 0: explicitly set
   sec.prot             security protocol e.g. krb5,gsi,sss,unix
   sec.name             mapped user name e.g. root/daemon
   sec.host             client host
   sec.vorg             virtual organisation (only VOMS)
   sec.grps             virtual group (only VOMS)
   sec.role             virtual role (only VOMS)
   sec.info             security information e.g. DN
   sec.app              application responsible for record e.g. balancing,gridftp,eoscp,fuse
   tpc.src              TPC source hostname (only on TPC transfers)
   tpc.dst              TPC destination hostname (only on TPC transfers)
   tpc.src_lfn          TPC file path at source (only on TPC transfers)
   ==================== ==================================================================================================

Note: In case of TPC transfers, only one of `tpc.src` or `tpc.dst` is available,
depending on the type of TPC transfer

FST deletion records
----------------------------

Each FST sends for a deletion on disk a record which is tagged with application *deletion* :
`log=619d7b82-4f79-11e8-a96c-02163e009ce2&host=test.cern.ch&fid=1056316&fsid=1&dc_ts=1525425793&dc_tns=968438733&dm_ts=1525425793&dm_tns=968438733&da_ts=1525425793&da_tns=968438733&dsize=2202&sec.app=deletion`

.. epigraph::

   ==================== ==================================================================================================
   TAG                  Description
   ==================== ==================================================================================================
   log                  uuid to correlate log entries
   host                 FST host name
   fid                  file id of the file deleted
   fsid                 filesystem id where the file is deleted
   del_ts               timestamp when the deletion message was generated
   del_tns              timestamp in ns when the deletion message was generated
   dc_ts                change timestamp of the deleted file
   dc_tns               change timestamp in ns of the deleted file
   dm_ts                modification timestamp of the deleted file
   dm_tns               modification timestamp in ns of the deleted file
   da_ts                access timestamp on local disk of the deleted file
   da_tns               access timestamp on local disk in ns of the deleted file
   dsize                size of the file before deletion
   sec.app              always: deletion
   ==================== ==================================================================================================

MGM deletion records
----------------------------

The MGM sends for each final deletion a record which is tagged with application *rm* :
`log=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx&host=test.cern.ch:1094&fid=1056331&ruid=0&rgid=0dc_ts=1525425819&dc_tns=354463329&dm_ts=1525425804&dm_tns=478169000&dsize=2202&sec.app=rm`

The MGM sends for each deletion moving a file into the recycle bin a record tagged with application *recycle* :
`log=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx&host=test.cern.ch:1094&fid=1056325&ruid=0&rgid=0dc_ts=1525425819&dc_tns=351463254&dm_ts=1525425804&dm_tns=182997000&dsize=2202&sec.app=recycle`

.. epigraph::

   ==================== ==================================================================================================
   TAG                  Description
   ==================== ==================================================================================================
   log                  always: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   host                 MGM host name
   fid                  file id
   del_ts               timestamp when the deletion message was generated
   del_tns              timestamp in ns when the deletion message was generated
   dc_ts                change timestamp of the deleted file
   dc_tns               change timestamp in ns of the deleted file
   dm_ts                modification timestamp of the deleted file
   dm_tns               modification timestamp in ns of the deleted file
   dsize                size of the file before deletion
   sec.app              rm,recycle (see above)
   ==================== ==================================================================================================
