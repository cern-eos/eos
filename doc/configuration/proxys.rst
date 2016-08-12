.. highlight:: rst

.. index::
   single: Proxys

Proxys
======

Overview
--------

In EOS, it is possible to configure filsystems so that they should be accessed by clients through some FSTs acting as proxys.
Such *node* are called *proxy*, short for data-proxy. 

Configuring a proxy
-------------------

A proxy *node* is just a normal FST daemon running on some machine. It can host standard FST filesystems or no filsystem at all as
they are not needed for the purpose of acting as a *proxy*.

*Proxys* are grouped into *proxygroups* whose the name designates the capacity of the *proxys* in the group. For instance, the *proxygroupgroup* 
``kinetic`` may group *proxies* that can talk XRootD with a client comming to get/put some data and that can talk with Kinetic drives for the storage.

Depending n the version of the software, FSTs can act as proxy for different types of FS.

.. note::
 
   A given *node* can belong to multiple *proxygroups*

To manage nodes' membership, the following eos commands can be used:

::

   node proxygroupadd 
   node proxygrouprm
   node proxygroupclear

To view nodes' membership, the following eos commands can be used:

::

   node status

Here follows an example.
   
.. code-block:: bash

Note that if proxygroups is not defined for a node , it means the same as proxygroups being defined and empty.     
   
It is also possible to review the scheduling snapshots associated to a proxygroup with the command

::

   geosched show snapshot 

Here follows an example (partial output).
   
.. code-block:: bash

   EOS Console [root://localhost] |/eos/multipath/> geosched show snapshot
   ### scheduling snapshot for proxy group c5group :
   --------c5group/( free:2|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:0)
          `----------nogeotag/( free:2|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:0)
                    |----------1/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:0)@p05153074617805.cern.ch:1095
                    `----------2/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:0)@p05153074625071.cern.ch:1095

Configuring a filesystem
------------------------

A filesystem can be:

- A native FST filesystem. In that case, there is no need for a proxy when a client accesses the filesystem.

- An imported filesystem. In that case, a *proxygroup* should be configured.

.. note::

  As of today, supported imported filsystems can be of the following types:
  
  - XRootd filesystem (another EOS instance for example)
  
  - Kinetic Drives Cluster
  
  - RadosFs storage

.. line::
| [ADD SOMETHING ABOUT CONFIGURING URL TO ACCESS FS FROM PROXY]
| To tag a filesystem as requiring an access through a proxy of a given proxygroup, the following eos command can be used:

::

   fs config <fsid> proxygroup=<proxygroup>

Note that the special value <none> is equivalent to proxygroup not being defined i.e. no proxygroup associated to the fs.

It is possible to review the proxygroup a filesystem relies on using the following eos command:

::

   fs status <fsid>
   
Here follows an example (partial output).
   
.. code-block:: bash

   EOS Console [root://localhost] |/eos/multipath/> fs status 2
   # ------------------------------------------------------------------------------------
   # FileSystem Variables
   # ....................................................................................
   bootcheck                        := 0
   bootsenttime                     := 1470773776
   configstatus                     := rw
   drainperiod                      := 86400
   graceperiod                      := 86400
   host                             := p05153074617805.cern.ch
   hostport                         := p05153074617805.cern.ch:1095
   id                               := 2
   path                             := kinetic://cluster5/
   port                             := 1095
   proxygroup                       := c5group
   queue                            := /eos/p05153074617805.cern.ch:1095/fst
   
Note that if proxygroup is not define, it is equivalent to proxygroup having the value <none>.
              
The special proxygroup *firewallentrypoint*
-------------------------------------------

.. line::

| The proxygroup named *firewallentrypoint* has a special role. Though, it is managed exactly as another proxygroup.
| It should be configured if the EOS instance is behind a firewall and if some client will access the instance from the outside world.
| In that case, the outside world should be defined by setting a geotag "proxy" to clients from the outside world. (see :doc:`geotags`) [TO BE ADAPTED].
| Then, when a client comes from the outside world, they will be shceduled to access the FST's of the instance through one of the proxys contained in the proxygroup *firewallentrypoint*.
| This proxy group should contain nodes configured in one of the following two ways.

Just another proxy
~~~~~~~~~~~~~~~~~~
The node is just a standard proxy that can access all the possible types of filesystems. It can then be used as a proxy for any fs in the instance.
Concretely, that means that such nodes would be part of all the proxygroups.

Forwarding gateway
~~~~~~~~~~~~~~~~~~

.. line::

| It is possible to use an XRootD forwarding daemon together with an FST daemon on the *firewallentrypoint* nodes.
| With this configuration, the proxy node might not be part of all the proxygroups (it could even belong only to *firewallentrypoint*).
| If a client is scheduled to a filesystem of which the proxygroup is not supported by the scheduled *firewallentrypoint* proxy, the scheduler will use the forwarding gateway running on that machine to forward the access to a proxy from the right proxygroup.    

.. code-block:: bash

   EOS Console [root://localhost] |/eos/multipath/> fs ls
   
   #...........................................................................................................................................................................
   #                   host (#...) #   id #                           path #     schedgroup #         geotag #       boot # configstatus #      drain # active #         health
   #...........................................................................................................................................................................
    p05153074617805.cern.ch (1095)      1          kinetic://single-drive/          default                        booted             ro      nodrain   online         1/1 (+0)
    p05153074617805.cern.ch (1095)      2              kinetic://cluster5/          default                        booted             rw      nodrain   online       25/25 (+4)

File scheduling through proxies
-------------------------------
.. line::
| First some tools are mentioned to help to make the config right. 
| Then, the scheduling procedure is detailed and some additional features are presented.

Observing the state of the scheduler and the properties of the files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. line::

| Proxy scheduling is part of the geoscheduling engine. (see :doc:`geoscheduling`)
| As such, there is an easy way to check if all the proxys are well configured and then taken into account in the geoscheduling system as members of the expected proxygroups.
| Proxys are organized in trees, one for each *proxygroup*. Those trees are automatically kept in sync with configurations of the node, including the config variable proxygroups. 
| To review the snapshots, the following EOS command can be used.

::

   geosched show snapshot 

It can also be very handy, at least for testing purpose, to be able to list the filesystems the replicas of a files are stored on along with their proxygroups.
This can be carried out using the EOS command.

::

   fileinfo <path> --proxy 

Proxy scheduling logic
~~~~~~~~~~~~~~~~~~~~~~

.. line::

Here follows a sketch of the file scheduling algorithm with an emphasize on the proxy part. When an file access or placement is requested, the execution go through the following steps:

- The filesystems are selected according to the layout of the file and some scheduling settings.

- For each filesystem in the selection find a data proxy if one is required (proxygroup defined for the fs) and a firewallentrypoint proxy if required (client coming geotagged as "proxy" [TO BE ADAPTED]) by doing :

  * if it's a filesticky scheduling get the proxy associated to the accessed file or compute one.

  * if we have a data proxy and if needed, find a firewallentrypoint proxy as close as possible to the data proxy and we are done. If we don't have a data proxy yet choose a firewallentrypoint proxy which is as close to the client as possible if the client is geotagged and this behavior is configured (``pProxyCloseToFs`` false [TO BE DETAILED]) or as close to the filesystem otherwise. 

  * if we have no data proxy yet, check if the firewall entrypoint is a member of the required proxygroup. If it is, set it also as the data proxy. If it's not, select a data proxy following the same requirements as in the previous step. 


File-sticky proxy scheduling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For some reason, it may be necessary that access to a file goes consistently through one node or a subset of the proxygroup.
It is called *file-sticky proxy scheduling*. It is used for instance to maximize performance of some file caching that would be done on the proxy nodes. It involves a filesystem parameter called ``filestickyproxydepth``.
It can be set using the eos command:

.. code-block:: bash

   eos fs <fsid> setconfig filestickyproxydepth=<some_integer>
   
Note that having this variable undefined is equivalent to have it defined with a negative value and it means that the file-sticky proxy scheduling is disabled.

Usually the outcome of a proxy scheduling for a given filesystem would be the best possible and slightly randomized trade-off between proximity of the filesystem (or the client) and of the proxy and availability of ressource of the proxy. The algorithm which is used does not depend on the file, only on the geotags of the client, the geotag of the filesystem and the geotag of proxies in the proxygroup to be scheduled from.

.. line::

| When using file-sticky proxy scheduling, the behavior is different. 
| First a starting point for the search is decided. If ``ProxyCloseToFs`` is false and that the client has a geotag, it is the client's geotag. Other wise, it is the filesystem's getag.
| The starting point is projected on the considered proxygroup's scheduling tree. Then the resulting point is moved ``filestickyproxydepth`` steps uproot.
| All the proxies in the subtree starting from there are then flated-out in an array. The proxies are sorted by id.  
| The proxy is then selected using the inode number of the file to be accessed.

[WRITE AN EXAMPLE]

| Choosing the value of ``filestickyproxydepth`` depends on where (in terms of geotag) are placed the proxys compared to the filesystems.

[WRITE AN EXAMPLE]   