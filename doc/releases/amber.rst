.. highlight:: rst

.. index::
   single: Amber

Amber
========
.. note::

``Lifetime: 2010-2013``
The **Amber** release was the first production version of EOS. It has been used
since end of 2010. 

The focus of this release was to consolidate the robustness and consistency of 
the storage system. The centralized view allows to see the consistency state 
of the complete storage close to real time with one minute delay. 
The cleanup of inconsistent replicas has been carefully improved. 
By construction there shouldn't be any inconsistencies after an upload with a successful 
'close' command. In any case of failure replicas after automatically 
cleaned and the client notified. The former persistent meta-data database stored 
on the /var file system of each storage node has been removed and implemented 
as a SQLITE3 based cache, which can be rebuilt any time. 
This allows the operations team to perform a simple re-installation of the disk 
server root partition. 

A second focus was put on monitoring. IO monitoring allows to seperate file 
access and bandwidth by client domain and/or by applications. 
This allows e.g. to distinguish draining, balancing, gridftp and 
other IO. EOS can send UDP messages compatible to the AAA/FAX collectors 
including the full client identity for each file access. An IO namespace 
allows realtime tracking of the activity in sub branches of the namespace 
for seven days in the past by access and volume.

A third major add-on was the connection of EOS to external/other 
storage systems. This includes the implementation of synchronous and 
asynchronous transfers using XRootD, HTTP, gsiFTP and S3 protocol. 
Synchronous transfers are implemented by the 'eos cp' CLI; asynchronous 
transfers are managed via the 'eos transfer' CLI. The asynchronous transfer 
framework is based on a transfer engine implementing a transfer queue managed 
by a simple state machine with progress tracking and log file access. 
Transfers are scheduled on so-called gateway FSTs. 
Every storage node can be configured to act as a gateway for transfers with 
predefined slots and bandwidth. XRootD KRB5 and GSI authentication provide 
credential forwarding allowing a gateway to use delegated credentials. 
Overall this system allows to im- and export easily data from/to external 
storage systems like CASTOR or Cloud storage. In the future this can be a building 
block of backup and archiving functionality in EOS. The transfer system can be 
enabled on existing EOS instances or deployed as a stand-alone entity.