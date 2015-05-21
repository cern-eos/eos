.. highlight:: rst

.. index::
   pair: Console message Broadcasts; TTY


FUSE Client
===========

You can enable console broadcasts on MGM nodes for all desired messages by defining the two following
environment variables in ``/etc/sysconfig/eos``

.. code-block:: bash

   # ------------------------------------------------------------------
   # MGM TTY Console Broadcast Configuration
   # ------------------------------------------------------------------

   # define the log file where you want to grep
   export EOS_TTY_BROADCAST_LISTEN_LOGFILE="/var/log/eos/mgm/xrdlog.mgm"

   # define the log file regex you want to broad cast to all consoles
   export EOS_TTY_BROACAST_EGREP="CRIT,ALERT,EMERG"

