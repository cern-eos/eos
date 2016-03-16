debug
-----

.. code-block:: text

   debug [node-queue] this|<level> [-filter <unitlist>]
   '[eos] debug ...' allows to modify the verbosity of the EOS log files in MGM and FST services.
.. code-block:: text

   Options:
   debug  <level> [-filter <unitlist>] :
      set the MGM where the console is connected to into debug level <level>
   debug  <node-queue> <level> [-filter <unitlist>] :
      set the <node-queue> into debug level <level>. <node-queue> are internal EOS names e.g. '/eos/<hostname>:<port>/fst'
      <unitlist> : a comma seperated list of strings of software units which should be filtered out in the message log ! The default filter list is 'Process,AddQuota,UpdateHint,UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,RegisterNode,SharedHash'.
   The allowed debug levels are: debug info warning notice err crit alert emerg
   Examples:
      debug info *                        set MGM & all FSTs into debug mode 'info'
      debug err /eos/*/fst                set all FSTs into debug mode 'info'
      debug crit /eos/*/mgm               set MGM into debug mode 'crit'
      debug debug -filter MgmOfsMessage   set MGM into debug mode 'debug' and filter only messages comming from unit 'MgmOfsMessage'.
