geosched
--------

.. code-block:: text

  '[eos] geosched ..' Interact with the file geoscheduling engine in EOS.
  geosched show|set|updater|forcerefresh|disabled ...
  Options:
    geosched show [-c] tree [<scheduling subgroup>]                    :  show scheduling trees
    :  if <scheduling group> is specified only the tree for this group is shown. If it's not all, the trees are shown.
    :  '-c' enables color display
    geosched show [-c] snapshot [{<scheduling subgroup>,*}] [<optype>] :  show snapshots of scheduling trees
    :  if <scheduling group> is specified only the snapshot(s) for this group is/are shown. If it's not all, the snapshots for all the groups are shown.
    :  if <optype> is specified only the snapshot for this operation is shown. If it's not, the snapshots for all the optypes are shown.
    :  <optype> can be one of the folowing plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc
    :  '-c' enables color display
    geosched show param                                                :  show internal parameters
    geosched show state                                                :  show internal state
    geosched set <param name> [param index] <param value>              :  set the value of an internal state parameter (all names can be listed with geosched show state)
    geosched updater {pause|resume}                                    :  pause / resume the tree updater
    geosched forcerefresh                                              :  force a refresh of the trees/snapshots
    geosched disabled add <geotag> {<optype>,*} {<scheduling subgroup>,*}      :  disable a branch of a subtree for the specified group and operation
    :  multiple branches can be disabled (by successive calls) as long as they have no intersection
    geosched disabled rm {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*}   :  re-enable a disabled branch for the specified group and operation
    :  when called with <geotag> *, the whole tree(s) are re-enabled, canceling all previous disabling
    geosched disabled show {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*} :  show list of disabled branches for for the specified groups and operation
