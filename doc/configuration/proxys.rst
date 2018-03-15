.. highlight:: rst

.. index::
   single: Proxys

Proxys and firewall entrypoints
===============================

Overview
--------

In EOS, it is possible to configure filsystems so that they require to be accessed by clients through some FSTs acting as proxys.
Such *node* are called *proxy*, short for data-proxy.
Some nodes can also act as firewall entrypoints (fwep) to allow access to data nodes behind a firewall. 

Configuring a proxy
-------------------

A proxy *node* is just a normal FST daemon running on some machine. It can host standard FST filesystems or no filsystem at all as
they are not needed for the purpose of acting as a *proxy*.

*Proxys* are grouped into *proxygroups* whose the name may designate the capacity of the *proxys* in the group. For instance, the *proxygroup* 
``kinetic`` may group *proxies* that can talk XRootD with a client coming to get/put some data and that can talk with Kinetic drives for the storage.

Depending on the version of the software, FSTs can act as proxy for different types of FS.

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

   EOS Console [root://localhost] |/eos/multipath/> node status p05153074617805.cern.ch:1095
   # ------------------------------------------------------------------------------------
   # Node Variables
   # ....................................................................................
   debug.level                      := info
   debug.state                      := debug
   domain                           := MGM
   gw.ntx                           := 10
   gw.rate                          := 120
   kinetic.cluster.default          := base64:...
   kinetic.location.default         := base64:...
   kinetic.reload                   := default
   kinetic.security.default         := base64:...
   manager                          := p05153074617805.cern.ch:1094
   proxygroups                      := c5group
   ...

Note that if ``proxygroups`` is not defined for a node , it means the same as proxygroups being defined and empty.     
   
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
  
  - http(s) storage
  
  - S3(s) storage

.. line::

| The type is configured by setting the mount point a filesystem when calling ``eos fs add``. The path can be a local directory starting with ``/`` or it can be ``s3(s)://`` , ``http(s)://`` , ``kinetic://`` , ``root://`` . 
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
              
Firewall entrypoints and direct acess
-------------------------------------

.. line::

| EOS offers some functionalities to define hosts (gathered in proxygroups) acting as firewall entrypoints (fwep) and when they should be used.
| First, it is possible to restrain target geotags that are directly accessible from client geotags (i.e no need to go through a fwep).
| This can be done using the command

::

   geosched access setdirect
   
| The direct access rules can be reviewed using

::

   geosched access showdirect
 
| Please note that direct access rules act as a white list. If norule is defined, it means that all accesses are meant to be direct.
| Here follows an example of direct access rules

.. code-block:: bash

   EOS Console [root://localhost] |/eos/geotree/users/gadde/2rep/> geosched access showdirect
   --------AccessGeotagMapping
          |----------site1 [site1 => site1]
          |         `----------rack1 [site1::rack2 => site1,site2::rack2]
          |         
          `----------site2 [site2 => site2]

| This output means that a client geotagged ``site1`` can directly access a filesystem tagged ``site1``, a client geottaged ``site1::rack1`` can access a filesystem geotagged ``site1`` or ``site2::rack2``.
| Note that the rule to apply is the first rule met poping tokens from the right of the geotag.   
| In the current example, a client tagged ``site1::rack2`` has no rule for its geotag and it has a rule for ``site1``, it will use it.
| A client tagged ``site1::rack1`` has a rule attached to its geotag and will use it.
| There is only one matched rule. For instance, here, the client tagged ``site1::rack1`` can access ``site1::rack2`` and ``site2::rack2`` but cannot access ``site1`` (other than ``site1::rack2``).
| The client tagged ``site1::rack2`` can access site1 which means any geotag starting with ``site1::``.


| If access cannot be direct as by the rules defined earlier, a proxy MUST be found for the access to succeed.
| A selection rule maps a target geotag to a proxygroup from which an host used as fwep will be selected during the scheduling of the access.
| Fwep selection rules can be set with the command

 
::

   geosched access setproxygroup
   
| The rules can be reviewed with the command

::

   geosched access showproxygroup
   
| Here follows an example of fwep selection.

.. code-block:: bash

   --------AccessGeotagMapping
          `----------site2 [site2 => ep2]
                    `----------rack2 [site2::rack2 => ep22]

| Note, that the seleciton of the rule to apply works the same as for the direct access rules.
| It means that in our example, a non direct access to a filesystem tagged ``site2`` or ``site2::rack1`` will go through a fwep taken from proxygroup ``ep2``.
| A non direct access to a filesystem tagged ``site2::rack2`` or ``site2::rack2::whatever`` will go through a fwep taken from proxygroup ``ep22``.
| A non direct access to a filesystem tagged ``site1`` will FAIL because no proxygroup to find a fwep from can be deduced from the available rules.
|
| Machine acting as fweps should be configured in one of the two following ways.

Just another proxy
~~~~~~~~~~~~~~~~~~
The node is just a standard proxy that can access all the possible types of filesystems. It can then be used as a proxy for any fs in the instance.

Forwarding gateway
~~~~~~~~~~~~~~~~~~

.. line::

| It is possible to use an XRootD forwarding daemon together with an FST daemon on fwep nodes.
| With this configuration, the proxy node might not be able to serve the access to all types of filesystems.
| If a client is scheduled to a filesystem of which the proxygroup is not supported by the scheduled fwep proxy, the scheduler will use the forwarding gateway running on that machine to forward the access to a proxy from the right proxygroup.    

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
| Proxys are organized in trees, one for each *proxygroup*. Those trees are automatically kept in sync with configurations of the nodes, including the config variable proxygroups. 
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

- For each filesystem in the selection find a data proxy if one is required (proxygroup defined for the fs) and a fwep proxy (in the proxygroup according to the fwep selection rules) if required the direct access rules by doing :

  * if it is a filesticky scheduling get the proxy associated to the accessed file.

  * if we have a data proxy and if needed, find a fwep proxy as close as possible to the data proxy and we are done. If we don't have a data proxy yet choose a fwep proxy which is as close to the client as possible if the client is geotagged and this behavior is configured ( parameter ``pProxyCloseToFs`` is set 0 in the geoscheduling configuration) or as close to the filesystem otherwise. 

  * if we have no data proxy yet, check if the fwep is a member of the required proxygroup. If it is, set it also as the data proxy. If it is not, select a data proxy following the same requirements as in the previous step. 


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


| Choosing the value of ``filestickyproxydepth`` depends on where (in terms of geotag) are placed the proxys compared to the filesystems.
   
