.. highlight:: rst

Daemon Control
==============


Control individual daemons
--------------------------

.. code-block:: bash

    systemctl start eos@mq   # for starting MQ service
    systemctl start eos@sync # for starting SYNC service
    systemctl start eos@mgm  # for starting MGM service
    systemctl start eos@fst  # for starting FST service
    systemctl start eos@fed  # for starting FED service

It's the same for stop, status and restart. You can't start the daemon if it is
not configured in "/etc/sysconfig/eos_env" config file.


Control all daemons from the eos_env config file in the same time
-----------------------------------------------------------------

.. code-block:: bash

   systemctl start eos     # for starting
   systemctl stop eos@*    # for stopping all running daemons
   systemctl status eos@*  # for getting the status of all running daemons
   systemctl restart eos@* # for restarting all the running daemons

You can change the list of daemons (mgm|mq|sync|fst|fed)
in ``/etc/sysconfig/eos_env`` config file.


Configure EOS MGM/MQ as master or slave
---------------------------------------

.. code-block:: bash

   systemctl start eos@master # to configure MQ or/and MGM on localhost as master
   systemctl start eos@slave  # to configure MQ or/and MGM on localhost as slave
   systemctl start eosslave   # making EOS services running in slave mode

You can configure MQ or/and MGM only if they exist
in ``/etc/sysconfig/eos_env`` config file.


Control FST database
--------------------

.. code-block:: bash

   systemctl start eosfstdb@clean  # cleaning FST db for fast restart
   systemctl start eosfstdb@resync # forcing FST db resync for restart


Synchronize files between two MGM machines
------------------------------------------

.. code-block:: bash

   systemctl start eossync     # for starting
   systemctl stop eossync@*    # for stopping
   systemctl status eossync@*  # for status
   systemctl restart eossync@* # for restarting



EOS FUSE service
----------------

.. code-block:: bash

   systemctl start eosd     #   for starting
   systemctl stop eosd@*    #   for stopping
   systemctl status eosd@*  #   for status
   systemctl restart eosd@* #   for restarting

Config file ``/etc/sysconfig/eosd_env`` is necessary.
