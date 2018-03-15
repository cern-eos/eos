drain
-----

.. code-block:: text

  '[eos] drain ..' provides the drain interface of EOS.
  drain start|stop|status [OPTIONS]
  Options:
  drain start <fsid> [<targetFsId>]: 
    start the draining of the given fsid. If a targetFsId is specified, the drain process will move the replica to that fs
.. code-block:: text

  drain stop <fsid> : 
    stop the draining of the given fsid.
  drain clear <fsid> : 
    clear the draining info for the given fsid.
  drain status [fsid] :
    show the status of the drain activities on the system. If the fsid is specified shows detailed info about that fs drain
  Report bugs to eos-dev@cern.ch.
