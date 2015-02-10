.. highlight:: rst

.. index::
   single: File Geoscheduling

File Geoscheduling
==================

Overview
--------

The EOS file scheduler is a core component of EOS which decides on which filesystems to place or access files. 
This decision is based on:

* the geotag of each filesystem
* the state of each filesystem and of the machine hosting it
* the geotag of the requesting client
* the layout of the requested file
* several admin-defined internal parameters
* several admin-defined or user-defined directory attributes

This information is structured under the form of so-called *scheduling trees* 
-the shape of the trees being given by the geotags of the filesystems-.
There is one *scheduling tree* by scheduling group.
 
The file scheduler is a stateful component of which state is continuously updated to reflect the state of all
the filesystems involved in the instance.

The file scheduler is involved in ALL the file access/placement operations including file access/placement from clients, 
space balancing and filesystem draining.  

The interaction with the file geoscheduling is three-folded:

* the geosched command that allows to view/set internal state/parameters of the GeoTreeEngine
* geoscheduling related directory attributes that allow to alter the file scheduling in a directory-specific way.
* geotag aware eos commands that can display useful information summarized along the scheduling trees

Interacting with the GeoTreeEngine using the geosched command
-------------------------------------------------------------
The GeoTreeEngine is a software component inside EOS in charge of keeping a consistent 
up-to-date view of each scheduling group. For each scheduling group, this view is summarized into
a *scheduling tree* and multiple *snapshots* of this scheduling tree, one for each type of access/placement operation.
These snapshots are then copied and used to serve all the file access/placement requests.
To achieve its tasks, the GeoTreeEngine has several features including:

* a background *updater* which keeps snapshots and trees up-to-date. It updates snapshots and, only when needed, trees. Only when it is ultimately necessary, the updates on the snapshots are backported to the trees. It happens when a filesystem is added or removed from a scheduling group. So in general, snapshots have fresher information than trees. This is perfectly normal. 
* a *penalty system* which makes sure that some filesystems cannot be overscheduled in bursts of requests. Atomic penalties can be self-estimated or fixed. These penalties are substracted from the *dlscore* and the *ulscore* of the scheduled fs.
* a *latency estimation system* which estimates how fresh is the information the state of the GeoTreeEngine is based on.

Internal parameters
~~~~~~~~~~~~~~~~~~~
The commands

::

   geosched show param
   
and

::

   geosched set
   
allow to view and set internal parameters of the GeoTreeEngine.

.. code-block:: bash

   EOS Console [root://localhost] |/eos/demo/> geosched show param
   ### GeoTreeEngine parameters :
   skipSaturatedPlct = 0
   skipSaturatedAccess = 1
   skipSaturatedDrnAccess = 1
   skipSaturatedBlcAccess = 1
   skipSaturatedDrnPlct = 0
   skipSaturatedBlcPlct = 0
   penaltyUpdateRate = 1
   plctDlScorePenalty = 10(default) | 10(1Gbps) | 10(10Gbps) | 10(100Gbps) | 10(1000Gbps)
   plctUlScorePenalty = 10(defaUlt) | 10(1Gbps) | 10(10Gbps) | 10(100Gbps) | 10(1000Gbps)
   accessDlScorePenalty = 10(default) | 10(1Gbps) | 10(10Gbps) | 10(100Gbps) | 10(1000Gbps)
   accessUlScorePenalty = 10(defaUlt) | 10(1Gbps) | 10(10Gbps) | 10(100Gbps) | 10(1000Gbps)
   fillRatioLimit = 80
   fillRatioCompTol = 100
   saturationThres = 10
   timeFrameDurationMs = 1000
   ### GeoTreeEngine list of groups :
   default.0 , default.1 , default.10 , default.11 , default.12 , default.13
   default.14 , default.15 , default.16 , default.17 , default.18 , default.19
   default.2 , default.20 , default.21 , default.22 , default.3 , default.4
   default.5 , default.6 , default.7 , default.8 , default.9 , 

Here follows the list of these parameters.

.. epigraph::
  
   ========================= ======================================================================
   parameter                 definition
   ========================= ======================================================================
   *skipSaturatedPlct*       if 0, select the optimal fs for placement regardless of the fact it is IO-saturated. if 1, try to find an IO-unsaturated fs first and then fallback onto saturated fs.
   *skipSaturatedAccess*     as *skipSaturatedPlct* but for access
   *skipSaturatedDrnPlct*    as *skipSaturatedPlct* but for draining placement
   *skipSaturatedDrnAccess*  as *skipSaturatedPlct* but for draining access
   *skipSaturatedBlcPlct*    as *skipSaturatedPlct* but for balancing placement
   *skipSaturatedBlcAccess*  as *skipSaturatedPlct* but for balancing access
   *penaltyUpdateRate*       weight of the penalty update at each time Frame. **0 means penalties are fixed**, 100 means that new values are estimated for each time frame regardless of the past. This parameter is used to ensure some stability for the penalties when they are self-estimated. 
   *plctDlScorePenalty*      atomic penalty applied to a fs download score on any type of placement operation. It is a vector indexed by the networking speed class of the file system.
   *plctUlScorePenalty*      as *plctDlScorePenalty* but for the upload score 
   *accessDlScorePenalty*    as *plctDlScorePenalty* but for access operations.
   *accessUlScorePenalty*    as *accessDlScorePenalty* but for the upload score
   *fillRatioLimit*          fill ratio above which a filesystem should not be used for a placement or a RW access operation.
   *fillRatioCompTol*        quantity by which fill ratio of two fs should differ to be considered as different. 100 means that whatever the fill ratios of two compared fs are, they will not be considered as different. The file scheduler, among other criterions, tries to balance fs fill ratios using this tolerance. As a consequence, if it is set to 10 it will try to get all the fill ratios equal in a 10% tol. **If this value is set to 100, there is no such inline space balancing**.
   *saturationThres*         threshold under which a fs upload or download score makes a fs considered as saturated.
   *timeFrameDurationMs*     periodicity of the internal state update (especially *snapshots* and possibly *trees*).
   ========================= ======================================================================

Internal state
~~~~~~~~~~~~~~
The internal state of the GeoTreeEngine is essentially composed of *scheduling trees* and *snapshots*. It also includes the penalty accounting table and the fs age/latency report. 
The former can be obtained with commands

::

   geosched show tree
   geosched show snapshot
   
and the latter with the command

::

   geosched show state

Some examples follow.

.. code-block:: bash

   EOS Console [root://localhost] |/eos/demo/> geosched show tree default.0
   ### scheduling tree for scheduling group default.0 :
   --------default.0 [3,9]
          |----------site1 [1,3]
          |         `----------rack1 [1,2]
          |                   `----------1@lxfsrd47a04.cern.ch [1,1,UnvRW]
          |                   
          |         
          `----------site2 [2,5]
                    |----------rack1 [1,2]
                    |         `----------24@lxfsre13a01.cern.ch [1,1,UnvRW]
                    |         
                    `----------rack2 [1,2]
                              `----------46@lxfsrg15a01.cern.ch [1,1,UnvRW]

.. code-block:: bash

   EOS Console [root://localhost] |/eos/demo/> geosched show snapshot default.0
   ### scheduling snapshot for scheduling group default.0 and operation 'Placement' :
   --------default.0/( free:2|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:1|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:1|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:1|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:1|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:1|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch
   
   ### scheduling snapshot for scheduling group default.0 and operation 'Access RO' :
   --------default.0/( free:0|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:0|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch
   
   ### scheduling snapshot for scheduling group default.0 and operation 'Access RW' :
   --------default.0/( free:0|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:0|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch
   
   ### scheduling snapshot for scheduling group default.0 and operation 'Draining Access' :
   --------default.0/( free:0|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:0|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch
   
   ### scheduling snapshot for scheduling group default.0 and operation 'Draining Placement' :
   --------default.0/( free:0|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:1|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch
   
   ### scheduling snapshot for scheduling group default.0 and operation 'Balancing Access' :
   --------default.0/( free:0|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:0|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:0|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch
   
   ### scheduling snapshot for scheduling group default.0 and operation 'Draining Placement' :
   --------default.0/( free:0|repl:0|pidx:1|status:OK|ulSc:99|dlSc:99|filR:0|totS:3.85797e+12)
          |----------site1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |         `----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)
          |                   `----------1/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.86507e+12)@lxfsrd47a04.cern.ch
          |                   
          |         
          `----------site2/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |----------rack1/( free:0|repl:0|pidx:0|status:OK|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)
                    |         `----------24/( free:1|repl:0|pidx:0|status:RW|ulSc:99|dlSc:99|filR:0|totS:1.99291e+12)@lxfsre13a01.cern.ch
                    |         
                    `----------rack2/( free:0|repl:0|pidx:0|status:Dis|ulSc:0|dlSc:0|filR:0|totS:0)
                              `----------46/( free:1|repl:0|pidx:0|status:DISRW|ulSc:99|dlSc:99|filR:0|totS:1.99091e+12)@lxfsrg15a01.cern.ch

The internal state of the GeoTreeEngine is kept up-to-date by the background updater. It can be paused and resumed with the commands.

::

   geosched updater pause
   geosched updater resume
   
**A refresh of all the** *scheduling trees* **and** *snapshots* **can be obtained with the command**

::

   geosched forcerefresh

Branch disabling
~~~~~~~~~~~~~~~~
The GeoTreeEngine implements a mechanism to inhibit branches of the snapshots for selected types of operation.
It can be done for all the scheduling groups or only for specific ones.
The list of inhibited branches for each operation can be managed with the commands

::

   geosched disabled add
   geosched disabled rm
   geosched disabled show

One can foresee multiple applications for this. An example can be found in **the default value that forbids any placement operation to a non-geotagged filesystem**.

Geoscheduling-related directory extended attributes
---------------------------------------------------
In EOS, directories have several extended attributes to control the *placement policy* in multiple situations. 
There are three types **placement policy**. Here follows a table with their definition depending on the file layout.

.. epigraph::

   ======= ====================================== ==================================================================== ============================
   Layout     gathered:tag1::tag2                 hybrid:tag1::tag2                                                    scattered
   ======= ====================================== ==================================================================== ============================
   Replica all as close as possible to tag1::tag2 all-1 around tag1::tag2 and 1 as scattered as possible               all as scattered as possible
   RAID    all as close as possible to tag1::tag2 all-n_parity around tag1::tag2 and n_parity as scattered as possible all as scattered as possible
   ======= ====================================== ==================================================================== ============================

The following variables deal with the default *placement policy* in a directory.

.. epigraph::
  
   =============================== ======================================================================
   parameter                                     definition
   =============================== ======================================================================
   sys.forced.placementpolicy      enforces to use a given placement policy for all file placements in the directory
   sys.forced.noplacementpolicy    disables user defined placement policy for the directory
   user.forced.placementpolicy     s.a.
   user.forced.noplacementpolicy   s.a.
   =============================== ======================================================================
   
For more detailed information about these attributes, please refer to the help of the command

::

   attr

The file conversion command

::

   file convert

supports mentioning *placement policy*. 

The file conversion feature of the :doc:`lru` is also *placement policy*-aware.
The extended directory attribute

.. epigraph::
  
   ================================== =
   parameter                      
   ================================== =
   sys.conversion.\<match_rule_name\>
   ================================== =

supports mentioning *placement policy*. For more detailed information about the syntax, please refer to the help of the command

::

   attr


Geotag aware commands
---------------------
The commands

::

   group ls
   space ls
   
both feature a switch *-g <depth>* that allows to summarize the displayed information along the scheduling trees down to depth *<depth>*.

.. code-block:: bash

   EOS Console [root://localhost] |/eos/demo/> space ls -g 2
   #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   #     type #           name  #  groupsize #   groupmod #N(fs) #N(fs-rw) #sum(usedbytes) #sum(capacity) #capacity(rw) #nom.capacity #quota #balancing # threshold # converter #  ntx # active #intergroup
   #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   spaceview           default             0            0     67        66        272.69 G       133.62 T      131.62 T             0    off        off          20          on      2        0         off
   #-------------------------------------------------------------------------------------------------------
   #                         geotag   #N(fs) #N(fs-rw) #sum(usedbytes) #sum(capacity) #capacity(rw)        
   #-------------------------------------------------------------------------------------------------------
                             <ROOT>       67        66        272.69 G       133.62 T      131.62 T        
                      <ROOT>::site1       23        23        105.72 G        45.79 T       45.79 T        
                      <ROOT>::site2       44        43        166.97 G        87.83 T       85.84 T        
               <ROOT>::site1::rack1       23        23        105.72 G        45.79 T       45.79 T        
               <ROOT>::site2::rack1       22        22         74.36 G        43.92 T       43.92 T        
               <ROOT>::site2::rack2       22        21         92.61 G        43.92 T       41.92 T        
