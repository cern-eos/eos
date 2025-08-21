.. highlight:: rst

.. _io-priorities:

IO Priorities
=============

IO priorities are currently ony supported by devices using the CFQ (CentOS7) or BFQ (Centos8s) scheduler for reads and direct writes.
You can figure out which scheduler is used by inspecting:

.. code-block:: bash

   cat /sys/block/*/queue/scheduler


Supported IO Priorities
-----------------------

* real-time (level 0-7) 
* best-effort (level 0-7)
* idle (level 0)

Real-time Class
+++++++++++++++

This is the real-time I/O class.  This scheduling class is given higher priority than any other class: processes from this class are given first access to the disk every time. Thus, this I/O class needs to be used with some care: one I/O real-time process can starve the entire system. Within the real-time class, there are 8 levels of class data (priority) that determine exactly how much time this process needs the disk for on each service. The highest real-time priority level is 0; the lowest is 7.
The priority is defined in EOS f.e. as *rt:0* or *rt:7*.

Best-Effort Class
+++++++++++++++++

This is the best-effort scheduling class, which is the default for any process that hasn't set a specific I/O priority.The class data (priority) determines how much I/O bandwidth the process will get.  Best-effort priority levels are analogous to CPU nice values. The priority level determines a priority relative to other processes in the best-effort scheduling class.  Priority levels range from 0 (highest) to 7 (lowest). The priority is defined in EOS f.e. as *be:0* or *be:4*.


Idle Class
++++++++++

This is the idle scheduling class.  Processes running at this level get I/O time only when no one else needs the disk.  The idle class has no class data, but the configuration requires to configure it in EOS as *idle:0* . Attention is required when assigning this priority class to a process, since it may become starved if higher priority processes are constantly accessing the disk.

Setting IO Priorities
---------------------

IO priorities can be set in various ways:

.. code-block:: bash

   # via CGI if the calling user is member of the operator role e.g. make 99 member of operator role
   eos vid set membership 99 -uids 11
   # use URLs like
   "root://localhost//eos/higgs.root?eos.iopriority=be:0"

   # as a default space policy for readers
   eos space config default space.policy.iopriority:r=rt:0

   # as a space policy
   eos space config erasure space.policy.iopriority:w=idle:0

   # as a default application policy e.g. for application foo writers 
   eos space config default space.iopriority:w.app:foo=be:4

   # as a space application policy e.g. for application bar writers
   eos space config erasure space.iopriority:w.app:bar=be:7

The CGI (if allowed via the operator role) is overruling any other priority configuration. Otherwise the order of evaluation is shown as in the block above. 

For handling of policies in general (how to show, configure and delete) refer to :ref: `space-policies`.


