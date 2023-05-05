geosched
--------

.. code-block:: text

  '[eos] geosched ..' Interact with the file geoscheduling engine in EOS.
  geosched show|set|updater|forcerefresh|disabled|access ...
  Options:
    geosched show [-c|-m] tree [<scheduling group>]                    :  show scheduling trees
    :  if <scheduling group> is specified only the tree for this group is shown. If it's not all, the trees are shown.
    :  '-c' enables color display
    :  '-m' list in monitoring format
    geosched show [-c|-m] snapshot [{<scheduling group>,*} [<optype>]] :  show snapshots of scheduling trees
    :  if <scheduling group> is specified only the snapshot(s) for this group is/are shown. If it's not all, the snapshots for all the groups are shown.
    :  if <optype> is specified only the snapshot for this operation is shown. If it's not, the snapshots for all the optypes are shown.
    :  <optype> can be one of the folowing plct,accsro,accsrw,accsdrain,plctdrain
    :  '-c' enables color display
    :  '-m' list in monitoring format
    geosched show param                                                :  show internal parameters
    geosched show state [-m]                                           :  show internal state
    :  '-m' list in monitoring format
    geosched set <param name> [param index] <param value>              :  set the value of an internal state parameter (all names can be listed with geosched show state)
    geosched updater {pause|resume}                                    :  pause / resume the tree updater
    geosched forcerefresh                                              :  force a refresh of the trees/snapshots
    geosched disabled add <geotag> {<optype>,*} {<scheduling subgroup>,*}      :  disable a branch of a subtree for the specified group and operation
    :  multiple branches can be disabled (by successive calls) as long as they have no intersection
    geosched disabled rm {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*}   :  re-enable a disabled branch for the specified group and operation
    :  when called with <geotag> *, the whole tree(s) are re-enabled, canceling all previous disabling
    geosched disabled show {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*} :  show list of disabled branches for for the specified groups and operation
    geosched access setdirect <geotag> <geotag_list>                   :  set a mapping between an accesser geotag and a set of target geotags
    :  these mappings specify which geotag can be accessed from which geotag without going through a firewall entrypoint
    :  geotag_list is of the form token1::token2,token3::token4::token5,...
    geosched access showdirect [-m]                                    :  show mappings between accesser geotags and target geotags
    :  '-m' list in monitoring format
    geosched access cleardirect {<geotag>|all}                         :  clear a mapping between an accesser geotag and a set of target geotags
    geosched access setproxygroup <geotag> <proxygroup>                :  set the proxygroup acting as a firewall entrypoint for the given subtree
    :  if a client accesses a file from a geotag which does not have direct access to the subtree the replica is,
    :  it will be scheduled to access through a node from the given proxygroup
    geosched access showproxygroup [-m]                                :  show mappings between accesser geotags and target geotags
    :  '-m' list in monitoring format
    geosched access clearproxygroup {<geotag>|all}                     :  clear a mapping between an accesser geotag and a set of target geotags
