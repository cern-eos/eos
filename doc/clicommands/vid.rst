vid
---

.. code-block:: text

   usage: vid ls [-u] [-g] [-s] [-U] [-G] [-g] [-a] [-l] [-n]                                          : list configured policies
      -u : show only user role mappings
      -g : show only group role mappings
      -s : show list of sudoers
      -U : show user  alias mapping
      -G : show group alias mapping
      -y : show configured gateways
      -a : show authentication
      -l : show geo location mapping
      -n : show numerical ids instead of user/group names
      vid set membership <uid> -uids [<uid1>,<uid2>,...]
      vid set membership <uid> -gids [<gid1>,<gid2>,...]
      vid rm membership <uid>             : delete the membership entries for <uid>.
      vid set membership <uid> [+|-]sudo
      vid set map -krb5|-gsi|-https|-sss|-unix|-tident|-voms <pattern> [vuid:<uid>] [vgid:<gid>]
      -voms <pattern>  : <pattern> is <group>:<role> e.g. to map VOMS attribute /dteam/cern/Role=NULL/Capability=NULL one should define <pattern>=/dteam/cern:
      vid set geotag <IP-prefix> <geotag>  : add to all IP's matching the prefix <prefix> the geo location tag <geotag>
      N.B. specify the default assumption via 'vid set geotag default <default-tag>'
      vid rm <key>                         : remove configured vid with name key - hint: use config dump to see the key names of vid rules
      vid enable|disable krb5|gsi|sss|unix|https
      : enable/disables the default mapping via password database
      vid add|remove gateway <hostname> [krb5|gsi|sss|unix|https]
      : adds/removes a host as a (fuse) gateway with 'su' priviledges
      [<prot>] restricts the gateway role change to the specified authentication method
