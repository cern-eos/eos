// ----------------------------------------------------------------------
// File: GeoTreeEngine.hh
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSMGM_GEOTREEENGINE__HH__
#define __EOSMGM_GEOTREEENGINE__HH__

// THIS IS EXPERIMENTAL AND DOES NOT REALLY WORK
// FOR FUTURE WORK
#define HAVE_ATOMICS 1

/*----------------------------------------------------------------------------*/
#include "mgm/FsView.hh"
#include "mgm/geotree/SchedulingSlowTree.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/Timing.hh"
#include "common/FileSystem.hh"
#include "mq/FileSystemChangeListener.hh"
#include "mq/MessagingRealm.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysAtomics.hh"
/*----------------------------------------------------------------------------*/
#include <list>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

/*----------------------------------------------------------------------------*/
/**
 * @file GeoTreeEngine.hh
 *
 * @brief Class responsible to handle %GeoTree Operations
 * (file placement for new replica, source finding for balancing and draining)
 *
 * # Overview
 *
 * The GeoTreeEngine is the EOS software component in charge of doing the file scheduling
 * operations for file/replica access and placement based on the so called GeoTrees.
 * For an overview of the configuration of the geoscheduling please read doc/configuration/geoscheduling.rst
 * For an overview of the configuration of the proxy/proxygroup please read doc/configuration/proxys.rst
 * They are certainly good preliminary readings to this cover document.
 *
 * ## Geotags and GeoTrees
 *
 * Geotags are strings of the form <TAG1>::<TAG2>::...::<TAGN> where <..> are alphanumerical strings.
 * A collection of geotags can easily be represented in a tree structure where the first tokens are closer
 * to the root of the tree and where the last tokens are farther from the root.
 * Such trees are implemented into two types of structure.
 * - Trees (also refered as SlowTree s) : they are common trees with nodes pointing to each other.
 * They are flexible and allow adding, removing, moving subtrees easily but they are slow to browse because
 * of their inefficient memory layout.
 * - Snapshots (also refered as FastTree s) : they are fast structures designed for optimal memory access.
 * They are much faster that SlowTrees (10X) but their shape and structure is fixed at creation time.
 * They come along with auxiliary structures to speed up various lookup types (they are grouped using GeoTreeEngine::FastStructSched).
 * The snapshots host the state of the FileSystem s and use them to carry out scheduling operations efficiently.
 *
 * ## General architecture and sub-components
 * The GeoTreeEngine hosts several subcomponents in charge of several sub-tasks in the global scheduling operations.
 *
 * ### File scheduling
 * The file scheduling is the step where is decided on which FileSystem the file/replicas are placed/accessed.
 * To do so, each FsGroup is mapped to two GeoTreeEngine::FastStructSched structures hosting the SlowTree and the FastTree structures (plus the auxiliary structures).
 * For each FsGroup, there is a foreground GeoTreeEngine::FastStructSched used by current scheduling operations and a background one allowing the updater to
 * run without interering with the scheduling operations (GeoTreeEngine#pGroup2SchedTME).
 * The GeoTreeEngine::FastStructSched structures is associated to one SlowTree and features multiple FastTree structures. There is one FastTree structure per type of operation.
 * A type of operation is the combination of a GeoTreeEngine::SchedType and of 'access' or 'placement'.
 * Note that only the FastTree structures are used by the thread performing the scheduling operations.
 * Note also that those threads create there own working copies and work on them. It avoids any exclusive lock to be needed.
 * File scheduling uses the penalty subsystem and the updater to keep its structures up-to-date.
 *
 * ### Proxy scheduling
 * The proxy scheduling is the step where is decided which FsNode will be used to proxy the reading of the data from the file system
 * or to go through the firewall. This step is optional and is performed only when necessary.
 * Its architecture is derived straight from the architecture of File scheduling.
 * There are a few differences though. Trees are populated with FsNode s rather than FileSystem s.
 * Each proxygroup is mapped (GeoTreeEngine#pGroup2ProxyTME) to two GeotreeEngine::FastStructProxy (foreground and background).
 * There are no type of operation. Hence there is only one FastTree per GeotreeEngine::FastStructProxy.
 * Proxy scheduling does not use the penalty subsystem but it does use the updater to keep its structures up-to-date.
 *
 * ### Firewall entrypoint scheduling
 * The Firewall entrypoint scheduling is a specific type of proxyscheduling. It has a preliminary step that regular proxy scheduling does not have.
 * This step uses GeoTreeEngine::AccessStruct structure GeoTreeEngine::pAccessGeotagMapping to check whether going through a firewall entrypoint is required.
 * It is basically a GeoTree that stores on each node the list of GeoTag it is allowed to access directly.
 * It then uses GeoTreeEngine::AccessStruct structure GeoTreeEngine::pAccessProxygroup to check which proxygroup should be use to select such a firewall entrypoint.
 * Then the Proxy scheduling machinery is used.
 * This one is basically a GeoTree that stores on each node the name of the proxygroup to use to find a firewall entrypoint.
 * Note that GeoTreeEngine::AccessStruct does not have this foreground/background split as the changes are rare and the thread carrying out scheduling operations
 * access those structure in RO. Hence, there are mutexes in those structures. These structures are not updated by the updater because they are just a mapping information without any state to be updated.
 *
 * ### Trees/Snapshots updater
 * The updater is run as a background thread.
 * This component listens to relevant changes from the XrdMqSharedObjectChangeNotifier (GeoTreeEngine::listenFsChange).
 * It stores the notifications in the maps GeoTreeEngine#gNotificationsBufferProxy and GeoTreeEngine#gNotificationsBufferFs.
 * Every GeoTreeEngine#pTimeFrameDurationMs, the changes are commited to the background tree structures (GeoTreeEngine::updateTreeInfo) in the following way.
 * - if a change is about a fs/node that was present before the last refresh, the change is committed to the right fast structures
 * - if a change is about a fs/node that has been added since the last refresh, it is commited to the SlowTree.
 *
 * If any change was made to the SlowTree (add/remove fs/proxy, geotag change), GeoTreeEngine::FastStructSched/GeotreeEngine::FastStructProxy are then regenerated fom the SlowTree.
 * Once the whole refresh is done pointers to foreground and background structures are swapped.
 *
 *
 * ### Penalty subsystem
 * The penalty subsystem was introduced to fight a potentially harmful corner-case. Bursts of access/placement requests.
 * Without such a mecanism, the GeoTreeEngine would not update its view until the next refresh of the trees and that could lead to heavily saturating some FileSystem/FsNode.
 * If a burst was issued by one client, for many files/replicas on a few FileSystem s, the GeoTreeEngine would consider the state of those FileSystem s at the last refresh.
 * It would then schedule all these accesses to the closest FileSystem to the client without refreshing its view of their state.
 * Then, it would not schedule the next access to another FileSystem to distribute the burden.
 *
 * The penalty subsystem GeoTreeEngine::PenaltySubSys avoids such a behavior by amending the state of the filesystems in the foreground GeoTreeEngine::FastStructSched.
 * Penalties are atomic quantities that are substracted from the download/upload score of the fiesystems/proxy.
 * To get an understanding of this subsystem, several parts are worth considering:
 * - Latency Estimation (in GeoTreeEngine::updateTreeInfo): Latency estimation is crucial in such a subsystem. The latency is the average time between a change in the state of a remote FileSystem/FsNode and the time, it is actually reflected in the scheduling system.
 * To keep the view of the system in sync, this time should match the lifetime that penalties should have so that by the time the GeoTreeEngine sees the effect of a scheduling decision on the remote state, the penalty is removed.
 * The GeoTreeEngine::LatencySubSys is in charge of the estimation of the latency.
 * - Penalty Estimation (in GeoTreeEngine::updateTreeInfo): GeoTreeEngine::PenaltySubSys is in charge of estimating penalties. GeoTreeEngine#pPenaltyUpdateRate is an important parameter that tells how reactive is the estimation.
 * Value 0 means that the estimated values remain stuck at the initial value. It's the way to not using penalty estimation. Value 1 means that a completely new value is calculated only from the last time window.
 * - Atomic Amending: This is the crucial part where atomic penalties are substratced to reflect the additional burden that scheduling decision just being taken puts on a FileSystem.
 * This is carried ou in \ref GeoTreeEngine::FastStructSched::applyUlScorePenalty
 * and GeoTreeEngine::FastStructSched::applyDlScorePenalty.
 * Please note that it is done without using any mutex just by using atomic substractions.
 * It was designed like this to leave all the scheduling threads free of interactions/contentions between each other.
 * It is made so to the expense of possibly losing a few updates when the background and foreground are swapped (this should not lead to any segv though).
 * This is not a big issue because the penalty subsystem is not meant to be extremely precise. Only the order of magnitude matters.
 *
 * Note that the penalty subsystem is used only for file scheduling as hard drives don't like many concurrent accesses.
 * It is not used for proxy scheduling, as the limiting ressource there is network.
 *
 * ## Outline of a scheduling operation
 * The scheduling operations are carried out by the threads serving the clients. We give here a schematic overview of bith placement and access operations
 * Note that the new scheduling system does NOT place replicas accross groups. An FsGroup is considered as scheduling unit on its own.
 *
 * ### %Access
 * The main function is GeoTreeEngine::accessHeadReplicaMultipleGroup.
 * It is called like that and its complex mainly because it has to be able to deal with data placement accross several FsGroup.
 *
 *
 *
 * ### Placement
 *
 * # Integration
 * The GeoTreeEngine is strongly bound to several other components in EOS, mainly to keep its internal state consistent and updated.
 * Intrgration with the other components of EOS is made through the use of the \link GeoTreeEngine::GeoTreeEngine public member functions \endlink.
 *
 * ## Fs/Hosts Listenning
 * The heartbeat now has a timestamp that allows estimation of the latency.
 * A new class XrdMqSharedObjectChangeNotifier now dispatches shared object change notifications to threads that subcribed. The updater thread is subscribed for only the updates it needs to receveive.
 * The function GeoTreeEngine::listenFsChange() processes the notifications for the updater.
 *
 * ## Consistency with the FsView
 * Adding and removing FileSystem s and FsGroup s to/from the GeoTreeEngine is hooked in Adding/Removing FileSystem s from the FsView.
 * It then enforces a strict consitency between the FsGroup s and FileSystem s as viewed and by the FsView and as viewed by the GeoTreeEngine.
 *
 * ## Consistency with the Proxygroups definition
 * "Proxygroups" (the set of proxygroups a node belongs to) are defined as being config attributes of the Nodes. The view of the GeoTreeEngine over proxys is kept up-to-date by the XrdMgmOfs::FsConfigListener() function
 * that calls GeoTreeEngine::matchHostPxyGr everytime that a proxygroups value change is notified.
 * Note that "proxygroup" (the one proxygroup in charge of proxying io to a given FileSystem) is defined as a FileSystem config attribute and is as such tracked by the updater.
 *
 * ## Configuration
 * All the configuration of the GeoTreeEngine is stored via the ConfigEngine in the config files /var/eos/config/...
 * The configuration settings of the GeoTreeEngine are stored in the ConfigEngine only when they differ from the default values.
 * No state information is kept there.
 * The structure of the Scheduling trees are not stored neither. Structures are reconstructed at boot time as things get added to the FsView and as FileSystem/Node config changes are intercepted.
 * The geotag mapping for direct access and the entrypoint proxygroups are part of the configuration.
 *
 * ## Boot sequence
 * The GeoTreeEngine is created in the beginning of the configure stage if the XrdMgmOfs plugin.
 * At the end of the configure stage, a GeoTreeEngine::forceRefresh() is issued as some changes in the config entries of Nodes might have been missed before the notification listener is properly started.
 * This is especially true when using proxygroups.
 *
 * # Locking Scheme
 * The locking schema is not very straightforward. It has been designed to minimize locking contention for the threads serving the clients by issuing the scheduling operations.
 *
 * # Memory Management
 * Low level FastTree structures and their auxiliary structures are designed to use as few dynamic allocation as possible.
 * In upper layers inside the GeoTreeEngine, a more common use of dynamic objects is done.
 *
 * ## Thread-local buffers
 * Threads serving clients have a thread-local buffer GeoTreeEngine#tlGeoBuffer to store their working copies of FastTree s.
 * This is a rather large buffer and it's allocated only once when it's first used for each thread. It's freed when the thread is destroyed.
 *
 * # Configuration parameters
 * The GeoTreeEngine can be configured in many ways. As explained earlier, the configuration of the GeoTreeEngine is saved by the ConfigEngine.
 * \link GeoTreeEngine::configMutex Those data members \endlink  are the configuration parameters and govern how the GeoTreeEngine behaves.
 *
 */

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class responsible to handle GeoTree Operations
 *
 */
/*----------------------------------------------------------------------------*/
class GeoTreeEngine : public eos::common::LogId
{
//**********************************************************
// BEGIN INTERNAL DATA STRUCTURES
//**********************************************************
  struct Penalties {
    char
    dlScorePenalty, ulScorePenalty;

    Penalties() :
      dlScorePenalty(0), ulScorePenalty(0)
    {}
  };
  typedef std::vector<Penalties> tPenaltiesVec;
  typedef std::map<std::string, Penalties> tPenaltiesMap;

  struct tLatencyStats {
    double minlatency, maxlatency, averagelatency, lastupdate, age;
    tLatencyStats() :
      minlatency(std::numeric_limits<double>::max()),
      maxlatency(-std::numeric_limits<double>::max()),
      averagelatency(0.0), lastupdate(0.0), age(0.0) {};
    double getage(double nowms = 0.0)
    {
      if (nowms == 0.0) {
        struct timeval curtime;
        gettimeofday(&curtime, 0);
        nowms = curtime.tv_sec * 1000 + curtime.tv_usec / 1000;
      }

      return  nowms - lastupdate;
    }
    void update(const double& nowms = 0.0)
    {
      double latency = getage(nowms);
      averagelatency = (averagelatency != 0.0) ? (averagelatency * 0.99 + latency *
                       0.01) : latency;
      minlatency = std::min(minlatency , latency);
      maxlatency = std::max(maxlatency , latency);
    }
  };

  struct nodeAgreg {
    bool saturated;
    size_t fsCount;
    size_t rOpen;
    size_t wOpen;
    double netOutWeight;
    double netInWeight;
    double diskUtilSum;
    size_t netSpeedClass;
    nodeAgreg() : saturated(false), fsCount(0), rOpen(0), wOpen(0),
      netOutWeight(0.0), netInWeight(0.0), diskUtilSum(0.0), netSpeedClass(0) {};
  };
//**********************************************************
// END INTERNAL DATA STRUCTURES
//**********************************************************

//**********************************************************
// BEGIN INTERNAL CLASSES
//**********************************************************
  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the fast structures needed to carry out
   *        file data storage/access scheduling operations
   *
   */
  /*----------------------------------------------------------------------------*/
  struct FastStructSched {
    FastROAccessTree* rOAccessTree;
    FastRWAccessTree* rWAccessTree;
    FastDrainingAccessTree* drnAccessTree;
    FastPlacementTree* placementTree;
    FastDrainingPlacementTree* drnPlacementTree;
    SchedTreeBase::FastTreeInfo* treeInfo;
    Fs2TreeIdxMap* fs2TreeIdx;
    GeoTag2NodeIdxMap* tag2NodeIdx;
    tPenaltiesVec* penalties;

    FastStructSched()
    {
      rOAccessTree = new FastROAccessTree;
      rOAccessTree->selfAllocate(FastROAccessTree::sGetMaxNodeCount());
      rWAccessTree = new FastRWAccessTree;
      rWAccessTree->selfAllocate(FastRWAccessTree::sGetMaxNodeCount());
      drnAccessTree = new FastDrainingAccessTree;
      drnAccessTree->selfAllocate(FastDrainingAccessTree::sGetMaxNodeCount());
      placementTree = new FastPlacementTree;
      placementTree->selfAllocate(FastPlacementTree::sGetMaxNodeCount());
      drnPlacementTree = new FastDrainingPlacementTree;
      drnPlacementTree->selfAllocate(FastDrainingPlacementTree::sGetMaxNodeCount());
      treeInfo = new SchedTreeBase::FastTreeInfo;
      penalties = new tPenaltiesVec;
      penalties->reserve(SchedTreeBase::sGetMaxNodeCount());
      fs2TreeIdx = new Fs2TreeIdxMap;
      fs2TreeIdx->selfAllocate(SchedTreeBase::sGetMaxNodeCount());
      rOAccessTree->pFs2Idx
        = rWAccessTree->pFs2Idx
          = drnAccessTree->pFs2Idx
            = placementTree->pFs2Idx
              = drnPlacementTree->pFs2Idx
                = fs2TreeIdx;
      rOAccessTree->pTreeInfo
        = rWAccessTree->pTreeInfo
          = drnAccessTree->pTreeInfo
            = placementTree->pTreeInfo
              = drnPlacementTree->pTreeInfo
                = treeInfo;
      tag2NodeIdx = new GeoTag2NodeIdxMap;
      tag2NodeIdx->selfAllocate(SchedTreeBase::sGetMaxNodeCount());
    }

    ~FastStructSched()
    {
      if (rOAccessTree) {
        delete rOAccessTree;
      }

      if (rWAccessTree) {
        delete rWAccessTree;
      }

      if (drnAccessTree) {
        delete drnAccessTree;
      }

      if (placementTree) {
        delete placementTree;
      }

      if (drnPlacementTree) {
        delete drnPlacementTree;
      }

      if (treeInfo) {
        delete treeInfo;
      }

      if (penalties) {
        delete penalties;
      }

      if (fs2TreeIdx) {
        delete fs2TreeIdx;
      }

      if (tag2NodeIdx) {
        delete tag2NodeIdx;
      }
    }

    bool DeepCopyTo(FastStructSched* target) const
    {
      if (
        rOAccessTree->copyToFastTree(target->rOAccessTree) ||
        rWAccessTree->copyToFastTree(target->rWAccessTree) ||
        drnAccessTree->copyToFastTree(target->drnAccessTree) ||
        placementTree->copyToFastTree(target->placementTree) ||
        drnPlacementTree->copyToFastTree(target->drnPlacementTree)
      ) {
        return false;
      }

      // copy the information
      *(target->treeInfo) = *treeInfo;

      if (
        fs2TreeIdx->copyToFsId2NodeIdxMap(target->fs2TreeIdx) ||
        tag2NodeIdx->copyToGeoTag2NodeIdxMap(target->tag2NodeIdx)) {
        return false;
      }

      // copy the penalties
      std::copy(penalties->begin(), penalties->end(),
                target->penalties->begin());
      // update the information in the FastTrees to point to the copy
      target->rOAccessTree->pFs2Idx
        = target->rWAccessTree->pFs2Idx
          = target->drnAccessTree->pFs2Idx
            = target->placementTree->pFs2Idx
              = target->drnPlacementTree->pFs2Idx
                = target->fs2TreeIdx;
      target->rOAccessTree->pTreeInfo
        = target->rWAccessTree->pTreeInfo
          = target->drnAccessTree->pTreeInfo
            = target->placementTree->pTreeInfo
              = target->drnPlacementTree->pTreeInfo
                = target->treeInfo;
      return true;
    }

    void UpdateTrees()
    {
      rOAccessTree->updateTree();
      rWAccessTree->updateTree();
      drnAccessTree->updateTree();
      placementTree->updateTree();
      drnPlacementTree->updateTree();
    }

    inline void applyDlScorePenalty(SchedTreeBase::tFastTreeIdx idx,
                                    const char& penalty, bool background)
    /**< Apply download score penalty */
    {
      AtomicSub(placementTree->pNodes[idx].fsData.dlScore, penalty);
      AtomicSub(drnPlacementTree->pNodes[idx].fsData.dlScore, penalty);
      AtomicSub(rOAccessTree->pNodes[idx].fsData.dlScore, penalty);
      AtomicSub(rWAccessTree->pNodes[idx].fsData.dlScore, penalty);
      AtomicSub(drnAccessTree->pNodes[idx].fsData.dlScore, penalty);

      if (!background) {
        AtomicAdd((*penalties)[idx].dlScorePenalty, penalty);
      }
    }

    inline void applyUlScorePenalty(SchedTreeBase::tFastTreeIdx idx,
                                    const char& penalty, bool background)
    /**< Apply upload score penalty */
    {
      AtomicSub(placementTree->pNodes[idx].fsData.ulScore, penalty);
      AtomicSub(drnPlacementTree->pNodes[idx].fsData.ulScore, penalty);
      AtomicSub(rOAccessTree->pNodes[idx].fsData.ulScore, penalty);
      AtomicSub(rWAccessTree->pNodes[idx].fsData.ulScore, penalty);
      AtomicSub(drnAccessTree->pNodes[idx].fsData.ulScore, penalty);

      if (!background) {
        AtomicAdd((*penalties)[idx].ulScorePenalty, penalty);
      }
    }

    inline bool buildFastStructures(SlowTree* slowTree)
    {
      return slowTree->buildFastStrcturesSched(
               placementTree , rOAccessTree, rWAccessTree,
               drnPlacementTree , drnAccessTree,
               treeInfo , fs2TreeIdx, tag2NodeIdx
             );
    }

    inline void resizePenalties(const size_t& newsize)
    {
      penalties->resize(newsize);
    }

    inline void setConfigParam(
      const char& fillRatioLimit,
      const char& fillRatioCompTol,
      const char& saturationThres)
    {
      rOAccessTree->setSaturationThreshold(saturationThres);
      rWAccessTree->setSaturationThreshold(saturationThres);
      drnAccessTree->setSaturationThreshold(saturationThres);
      placementTree->setSaturationThreshold(saturationThres);
      placementTree->setSpreadingFillRatioCap(fillRatioLimit);
      placementTree->setFillRatioCompTol(fillRatioCompTol);
      drnPlacementTree->setSaturationThreshold(saturationThres);
      drnPlacementTree->setSpreadingFillRatioCap(fillRatioLimit);
      drnPlacementTree->setFillRatioCompTol(fillRatioCompTol);
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the fast structures needed to carry out
   *        file gateway style scheduling operations
   *
   */
  /*----------------------------------------------------------------------------*/
  struct FastStructProxy {
    FastGatewayAccessTree* proxyAccessTree;
    SchedTreeBase::FastTreeInfo* treeInfo;
    Host2TreeIdxMap* host2TreeIdx;
    GeoTag2NodeIdxMap* tag2NodeIdx;
    tPenaltiesVec* penalties;

    FastStructProxy()
    {
      proxyAccessTree = new FastGatewayAccessTree;
      proxyAccessTree->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());
      treeInfo = new SchedTreeBase::FastTreeInfo;
      penalties = new tPenaltiesVec;
      penalties->reserve(FastGatewayAccessTree::sGetMaxNodeCount());
      host2TreeIdx = new Host2TreeIdxMap;
      host2TreeIdx->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());
      proxyAccessTree->pFs2Idx = host2TreeIdx;
      proxyAccessTree->pTreeInfo
        = treeInfo;
      tag2NodeIdx = new GeoTag2NodeIdxMap;
      tag2NodeIdx->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());
    }

    ~FastStructProxy()
    {
      if (proxyAccessTree) {
        delete proxyAccessTree;
      }

      if (treeInfo) {
        delete treeInfo;
      }

      if (penalties) {
        delete penalties;
      }

      if (tag2NodeIdx) {
        delete tag2NodeIdx;
      }
    }

    bool DeepCopyTo(FastStructProxy* target) const
    {
      if (
        proxyAccessTree->copyToFastTree(target->proxyAccessTree)
      ) {
        return false;
      }

      // copy the information
      *(target->treeInfo) = *treeInfo;

      if (
        tag2NodeIdx->copyToGeoTag2NodeIdxMap(target->tag2NodeIdx)) {
        return false;
      }

      if (
        host2TreeIdx->copyToFsId2NodeIdxMap(target->host2TreeIdx)) {
        return false;
      }

      // copy the penalties
      std::copy(penalties->begin(), penalties->end(),
                target->penalties->begin());
      // update the information in the FastTrees to point to the copy
      target->proxyAccessTree->pFs2Idx
        = NULL;
      target->proxyAccessTree->pTreeInfo
        = target->treeInfo;
      return true;
    }

    void UpdateTrees()
    {
      proxyAccessTree->updateTree();
    }

    inline void applyDlScorePenalty(SchedTreeBase::tFastTreeIdx idx,
                                    const char& penalty, bool background)
    {
      AtomicSub(proxyAccessTree->pNodes[idx].fsData.dlScore, penalty);

      if (!background) {
        AtomicAdd((*penalties)[idx].dlScorePenalty, penalty);
      }
    }

    inline void applyUlScorePenalty(SchedTreeBase::tFastTreeIdx idx,
                                    const char& penalty, bool background)
    {
      AtomicSub(proxyAccessTree->pNodes[idx].fsData.ulScore, penalty);

      if (!background) {
        AtomicAdd((*penalties)[idx].ulScorePenalty, penalty);
      }
    }

    inline bool buildFastStructures(SlowTree* slowTree)
    {
      return slowTree->buildFastStructuresGW(
               proxyAccessTree, host2TreeIdx,
               treeInfo , tag2NodeIdx
             );
    }

    inline void resizePenalties(const size_t& newsize)
    {
      penalties->resize(newsize);
    }

    inline void setConfigParam(
      const char& fillRatioLimit,
      const char& fillRatioCompTol,
      const char& saturationThres)
    {
      proxyAccessTree->setSaturationThreshold(saturationThres);
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage tree based operation of given type
   *        it is just a base to derived structs
   */
  /*----------------------------------------------------------------------------*/
  template<typename FastStruct> struct TreeMapEntry {

    // ==== SlowTree : this is used to add or remove nodes ==== //
    // every access to mSlowTree or mFs2SlowTreeNode should be protected by a lock to mSlowTreeMutex
    SlowTree* slowTree;
    //std::map<eos::common::FileSystem::fsid_t,SlowTreeNode*> fs2SlowTreeNode;
    eos::common::RWMutex slowTreeMutex;
    bool slowTreeModified;

    // ===== Fast Structures Management and Double Buffering ====== //
    FastStruct fastStructures[2];
    // the pointed object is read only accessed by several thread
    FastStruct* foregroundFastStruct;
    // the pointed object is accessed in read /write only by the thread update
    FastStruct* backgroundFastStruct;
    // the two previous pointers are swapped once an update is done. To do so, we need a mutex and a counter (for deletion)
    // every access to *mForegroundFastStruct for reading should be protected by a LockRead to mDoubleBufferMutex
    // when swapping mForegroundFastStruct and mBackgroundFastStruct is needed a LockWrite is taken to mDoubleBufferMutex
    eos::common::RWMutex doubleBufferMutex;
    size_t fastStructLockWaitersCount;
    bool fastStructModified;

    TreeMapEntry(const std::string& groupName = "") :
      slowTreeModified(false),
      foregroundFastStruct(fastStructures),
      backgroundFastStruct(fastStructures + 1),
      fastStructLockWaitersCount(0),
      fastStructModified(false)
    {
      slowTree = new SlowTree(groupName);
      slowTreeMutex.SetBlocking(true);
      doubleBufferMutex.SetBlocking(true);
    }

    ~TreeMapEntry()
    {
      if (slowTree) {
        delete slowTree;
      }
    }

    void swapFastStructBuffers()
    {
      eos::common::RWMutexWriteLock lock(doubleBufferMutex);
      std::swap(foregroundFastStruct, backgroundFastStruct);
    }

    void updateBGFastStructuresConfigParam(
      const char& fillRatioLimit,
      const char& fillRatioCompTol,
      const char& saturationThres)
    {
      backgroundFastStruct->setConfigParam(fillRatioLimit, fillRatioCompTol,
                                           saturationThres);
      refreshBackGroundFastStructures();
    }

    void refreshBackGroundFastStructures()
    {
      backgroundFastStruct->UpdateTrees();
    }

    bool updateFastStructures()
    {
      FastStruct* ft = backgroundFastStruct;

      if (!ft->buildFastStructures(slowTree)) {
        eos_static_crit("Error updating the fast structures");
        return false;
      }

      ft->resizePenalties(slowTree->getNodeCount());
      return true;
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage a given scheduling group
   *
   */
  /*----------------------------------------------------------------------------*/
  struct SchedTME : public TreeMapEntry<FastStructSched> {
    FsGroup* group;

    std::map<eos::common::FileSystem::fsid_t, SlowTreeNode*> fs2SlowTreeNode;

    SchedTME(const std::string& groupName) :
      TreeMapEntry<FastStructSched>(groupName),
      group(NULL)
    {}

    void updateSlowTreeInfoFromBgFastStruct()
    {
      for (auto it = fs2SlowTreeNode.begin(); it != fs2SlowTreeNode.end(); ++it) {
        const SchedTreeBase::tFastTreeIdx* idx;

        if (!backgroundFastStruct->fs2TreeIdx->get(it->first, idx)) {
          // this node was added in the SlowTree, the fast structures doesn't include it yet
          continue;
        }

        FastPlacementTree::FsData& fastState =
          backgroundFastStruct->placementTree->pNodes[*idx].fsData;
        SlowTreeNode::TreeNodeStateFloat& slowState = it->second->pNodeState;
        slowState.dlScore = fastState.dlScore;
        slowState.ulScore = fastState.ulScore;
        slowState.mStatus = fastState.mStatus &
                            ~eos::mgm::SchedTreeBase::Disabled; // we don't want to back proagate the disabled bit
        slowState.fillRatio = fastState.fillRatio;
        slowState.totalSpace = float(fastState.totalSpace);
        SchedTreeBase::TreeNodeInfo& fastInfo = (*backgroundFastStruct->treeInfo)[*idx];
        SlowTreeNode::TreeNodeInfo& slowInfo = it->second->pNodeInfo;
        slowInfo.netSpeedClass = fastInfo.netSpeedClass;
        slowInfo.proxygroup = fastInfo.proxygroup;
        slowInfo.fileStickyProxyDepth = fastInfo.fileStickyProxyDepth;
      }
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage a scheduling of XRootd gateways and data proxys
   *
   */
  /*----------------------------------------------------------------------------*/
  struct ProxyTMEBase : public TreeMapEntry<FastStructProxy> {
    FsGroup* group;

    std::map<std::string, SlowTreeNode*> host2SlowTreeNode;

    ProxyTMEBase(const std::string& groupName) :
      TreeMapEntry<FastStructProxy>(groupName),
      group(NULL)
    {}

    void updateSlowTreeInfoFromBgFastStruct()
    {
      for (auto it = host2SlowTreeNode.begin(); it != host2SlowTreeNode.end(); ++it) {
        const SchedTreeBase::tFastTreeIdx* idx;

        if (!backgroundFastStruct->host2TreeIdx->get(it->first.c_str(), idx)) {
          // this node was added in the SlowTree, the fast structures doesn't include it yet
          continue;
        }

        FastPlacementTree::FsData& fastState =
          backgroundFastStruct->proxyAccessTree->pNodes[*idx].fsData;
        SlowTreeNode::TreeNodeStateFloat& slowState = it->second->pNodeState;
        slowState.dlScore = fastState.dlScore;
        slowState.ulScore = fastState.ulScore;
        slowState.mStatus = fastState.mStatus &
                            ~eos::mgm::SchedTreeBase::Disabled; // we don't want to back proagate the disabled bit
        SchedTreeBase::TreeNodeInfo& fastInfo = (*backgroundFastStruct->treeInfo)[*idx];
        SlowTreeNode::TreeNodeInfo& slowInfo = it->second->pNodeInfo;
        slowInfo.netSpeedClass = fastInfo.netSpeedClass;
      }
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage a scheduling of Data proxy
   *
   */
  /*----------------------------------------------------------------------------*/
  struct DataProxyTME : public ProxyTMEBase {
    DataProxyTME(const std::string& groupName) : ProxyTMEBase(groupName)
    {}
  };

  bool updateFastStructures(SchedTME* entry)
  {
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    // if nothing is modified here move to the next group
    if (!(entry->slowTreeModified || entry->fastStructModified)) {
      return true;
    }

    if (entry->slowTreeModified) {
      entry->updateSlowTreeInfoFromBgFastStruct();

      if (!entry->updateFastStructures()) {
        eos_crit("error updating the fast structures from the slowtree");
        return false;
      }

      applyBranchDisablings(*entry);

      if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
        stringstream ss;
        ss << (*entry->backgroundFastStruct->placementTree);
        eos_debug("fast structures updated successfully from slowtree : new FASTtree is \n %s",
                  ss.str().c_str());
        ss.str() = "";
        ss << (*entry->slowTree);
        eos_debug("fast structures updated successfully from slowtree : old SLOW tree was \n %s",
                  ss.str().c_str());
      }
    } else {
      // the rebuild of the fast structures is not necessary
      entry->refreshBackGroundFastStructures();

      if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
        stringstream ss;
        ss << (*entry->backgroundFastStruct->placementTree);
        eos_debug("fast structures updated successfully from fastree : new FASTtree is \n %s",
                  ss.str().c_str());
      }
    }

    // mark the entry as updated
    entry->slowTreeModified = false;
    entry->fastStructModified = false;
    // update the BackGroundFastStructures configuration parameters accordingly to the one present in the GeoTree (and update the fast trees)
    entry->updateBGFastStructuresConfigParam(pFillRatioLimit, pFillRatioCompTol,
        pSaturationThres);
    // clear the penalties
    std::fill(entry->backgroundFastStruct->penalties->begin(),
              entry->backgroundFastStruct->penalties->end(), Penalties());
    // swap the buffers (this is the only bit where the fast structures is not accessible for a placement/access operation)
    entry->swapFastStructBuffers();
    return true;
  }

  bool updateFastStructures(ProxyTMEBase* entry)
  {
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    // if nothing is modified here move to the next group
    if (!(entry->slowTreeModified || entry->fastStructModified)) {
      return true;
    }

    if (entry->slowTreeModified) {
      entry->updateSlowTreeInfoFromBgFastStruct();

      if (!entry->updateFastStructures()) {
        eos_crit("error updating the fast structures from the slowtree");
        return false;
      }

      if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
        stringstream ss;
        ss << (*entry->backgroundFastStruct->proxyAccessTree);
        eos_debug("fast structures updated successfully from slowtree : new FASTtree is \n %s",
                  ss.str().c_str());
        ss.str() = "";
        ss << (*entry->slowTree);
        eos_debug("fast structures updated successfully from slowtree : old SLOW tree was \n %s",
                  ss.str().c_str());
      }
    } else {
      // the rebuild of the fast structures is not necessary
      entry->refreshBackGroundFastStructures();

      if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
        stringstream ss;
        ss << (*entry->backgroundFastStruct->proxyAccessTree);
        eos_debug("fast structures updated successfully from fastree : new FASTtree is \n %s",
                  ss.str().c_str());
      }
    }

    // mark the entry as updated
    entry->slowTreeModified = false;
    entry->fastStructModified = false;
    // update the BackGroundFastStructures configuration parameters accordingly to the one present in the GeoTree (and update the fast trees)
    entry->updateBGFastStructuresConfigParam(pFillRatioLimit, pFillRatioCompTol,
        pSaturationThres);
    // clear the penalties
    std::fill(entry->backgroundFastStruct->penalties->begin(),
              entry->backgroundFastStruct->penalties->end(), Penalties());
    // swap the buffers (this is the only bit where the fast structures is not accessible for a placement/access operation)
    entry->swapFastStructBuffers();
    return true;
  }

  //**********************************************************
// END INTERNAL CLASSES
//**********************************************************


  /// enum holding the possible operations
public:
  enum SchedType
  { regularRO, regularRW, draining};

protected:
//**********************************************************
// BEGIN DATA MEMBERS
//**********************************************************

  //! this is the size of the thread local buffer to hold the fast structure being used
  static const size_t gGeoBufferSize;

  //--------------------------------------------------------------------------------------------------------
  // Background Notifications and Updates
  //--------------------------------------------------------------------------------------------------------

  //! these are implicitly convertible enums
  //! they map to specific changes that happen on the fs
  static const int
  sfgGeotag, sfgId, sfgBoot, sfgDrain, sfgDrainer, sfgBlcingrun, sfgBlcerrun,
             sfgBalthres, sfgActive, sfgBlkavailb, sfgDiskload,
             sfgEthmib, sfgInratemib, sfgOutratemib, sfgWriteratemb,
             sfgReadratemb, sfgFsfilled, sfgNomfilled, sfgConfigstatus, sfgHost, sfgErrc,
             sfgPubTmStmp, sfgPxyGrp, sfgFileStickPxy, sfgWopen, sfgRopen ;

  //! This mutex protects the consistency between the GeoTreeEngine state and the filesystems it contains
  //! To make any change that temporarily set an unconsistent state (mainly adding a fs, removing a fs,
  //! listening to the changes in the set if contained fs), one needs to writelock this mutex.
  //! When the mutex is realesed, the GeoTreeEngine internal ressources should be in a consitent state.
  eos::common::RWMutex pAddRmFsMutex;

  //! this is the set of all the watched keys to be notified about for FileSystems
  static set<std::string> gWatchedKeys;

  //! this map allow to convert a notification key to an enum for efficient processing
  static const std::map<string, int> gNotifKey2EnumSched;

  //--------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------


  //--------------------------------------------------------------------------------------------------------
  // Configuration
  //--------------------------------------------------------------------------------------------------------
  //! [Configuration]
  /// this mutex protects all the configuration settings
  eos::common::RWMutex configMutex;// protects all the following settings

  /// these settings indicate if saturated FS should try to be avoided
  /// this might lead to unoptimal access/placement location-wise
  bool pSkipSaturatedAccess, pSkipSaturatedDrnAccess, pSkipSaturatedBlcAccess;
  /// these setting indicates if sthe proxy should be selected closest to the fs or closest to the client
  bool pProxyCloseToFs;

  /// this set the speed on how fast the penalties are allowed to
  /// change as they are estimated
  /// 0 means no self-estimate 1 mean gets a completely new value every time
  float pPenaltyUpdateRate; /**< Penalty update rate */

  /// the following settings control the SchedulingFastTrees
  /// it has an impact on how the priority of branches in the trees
  char
  /// between 0 and 100 : maximum fillRatio allowed on a fs to select it
  pFillRatioLimit,
  /// between 0 and 100 : quantity by which fillRatio must differ to be considered as different
  /// 100 disable any consideration about available space on the fs
  /// 0 enables a strict online balancing : if two fs are being considered with equal geolocation proximity
  ///                                       the emptier will be selected
  pFillRatioCompTol,
  /// score below which a FS is to be considered as (IO)saturated
  pSaturationThres;

  /// the following settings control the frequency and the latency of the background updating process
  int
  /// this is the minimum duration of a time frame
  pTimeFrameDurationMs, /**< time between two refresh of the trees */
  /// this is how older than a refresh a penalty must be do be dropped
  pPublishToPenaltyDelayMs;

  /// the following settings control the Disabled branches in the trees
  // group -> (optype -> geotag)
  std::map<std::string, std::map<std::string, std::set<std::string> > >
  pDisabledBranches;
  //! [Configuration]

  //--------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------

  //--------------------------------------------------------------------------------------------------------
  // State
  //--------------------------------------------------------------------------------------------------------
  //
  // => fs scheduling groups management / operations
  //    they are used to schedule fs accesses
  //
  std::map<const FsGroup*, SchedTME*> pGroup2SchedTME;
  std::map<FileSystem::fsid_t, SchedTME*> pFs2SchedTME;
  std::map<FileSystem::fsid_t, FileSystem*> pFsId2FsPtr;
  /// protects all the above maps
  eos::common::RWMutex pTreeMapMutex;

  // => proxy scheduling groups management / operations
  //    they are used to schedule data proxy to translate dedicated proxygroup to xrootd to serve the client (if any defined)
  //    they are also used to manage the entry points to the instance (if any defined)
  //
  std::map<std::string , DataProxyTME*>
  pPxyGrp2DpTME;          // one proxygroup => one TreeMapEntry
  std::map<std::string , std::set<DataProxyTME*>>
      pPxyHost2DpTMEs; // one proxyhost  => several proxygroups
  std::map<std::string, SchedTreeBase::tFastTreeIdx> pPxyQueue2PxyId;
  std::set<SchedTreeBase::tFastTreeIdx> pPxyId2Recycle;
  /// protects all the above maps
  eos::common::RWMutex pPxyTreeMapMutex;

  //
  struct AccessStruct {
    SlowTree* accessST;
    std::map<std::string, std::string> accessGeotagMap;
    FastGatewayAccessTree* accessFT;
    SchedTreeBase::FastTreeInfo* accessFTI;
    Host2TreeIdxMap* accessHost2Idx;
    GeoTag2NodeIdxMap* accessTag2Idx;
    /// protects all the above maps
    eos::common::RWMutex accessMutex;
    bool inuse;
    std::string configkey;

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    AccessStruct(const std::string& cfgkey):
      accessST(0), accessFT(0), accessFTI(0), accessHost2Idx(0), accessTag2Idx(0),
      inuse(false), configkey(cfgkey) {}

    std::string getMappingStr() const;

    bool setMapping(const std::string& geotag, const std::string& geotaglist,
                    bool updateFastStruct = true, bool setconfig = true);

    bool setMapping(const std::string& mapping, bool setconfig = false);

    bool clearMapping(const std::string& geotag = "", bool updateFastStruct = true,
                      bool setconfig = true);

    bool showMapping(XrdOucString* output, std::string operation, bool monitoring);
  };
  /// => access geotag mappings management / operations
  ///    they are used to check if going through a firewall entrypoint is required
  AccessStruct pAccessGeotagMapping;
  /// => access proxygroups management / operations
  ///    they are used to know which proxygroup to use when firewall entrypoint is required
  AccessStruct pAccessProxygroup;

  //
  // => thread local data
  //
  /// Thread local buffer to hold a working copy of a fast structure
  static thread_local void* tlGeoBuffer;
  static pthread_key_t gPthreadKey;
  /// Current scheduling group for the current thread
  static thread_local const FsGroup* tlCurrentGroup;
  //
  // => penalties system
  //
  const size_t pCircSize;
  size_t pFrameCount;
  struct PenaltySubSys {
    std::vector<tPenaltiesVec> pCircFrCnt2FsPenalties;
    std::vector<tPenaltiesMap> pCircFrCnt2HostPenalties;
    /// self estimated penalties
    std::map<std::string, nodeAgreg> pUpdatingNodes;
    size_t pMaxNetSpeedClass;
    /// Atomic penalties to be applied to the scheduled FSs
    /// those are in the state section because they can be self estimated
    /// the following vectors map an netzorkSpeedClass to a penalty
    std::vector<float> pPlctDlScorePenaltyF, pPlctUlScorePenaltyF;
    std::vector<float> pAccessDlScorePenaltyF, pAccessUlScorePenaltyF;
    std::vector<float> pProxyScorePenaltyF;
    // casted version to avoid conversion on every plct / access operation
    std::vector<char> pPlctDlScorePenalty, pPlctUlScorePenalty;
    std::vector<char> pAccessDlScorePenalty, pAccessUlScorePenalty;
    std::vector<char> pProxyScorePenalty;
    // Constructor
    PenaltySubSys(const size_t& circSize) :
      pCircFrCnt2FsPenalties(circSize),
      pCircFrCnt2HostPenalties(circSize),
      pMaxNetSpeedClass(0),
      pPlctDlScorePenaltyF(8, 10), pPlctUlScorePenaltyF(8,
          10),  // 8 is just a simple way to deal with the initialiaztion of the vector (it's an overshoot but the overhead is tiny)
      pAccessDlScorePenaltyF(8, 10), pAccessUlScorePenaltyF(8, 10),
      pProxyScorePenaltyF(8, 10) ,
      pPlctDlScorePenalty(8, 10), pPlctUlScorePenalty(8,
          10),  // 8 is just a simple way to deal with the initialiaztion of the vector (it's an overshoot but the overhead is tiny)
      pAccessDlScorePenalty(8, 10), pAccessUlScorePenalty(8, 10),
      pProxyScorePenalty(8, 10) {};
  };
  PenaltySubSys pPenaltySched;
  //
  // => latency estimation
  //
  struct LatencySubSys {
    std::vector<tLatencyStats> pFsId2LatencyStats;
    std::vector<size_t> pCircFrCnt2Timestamp;
    // Constructor
    LatencySubSys(const size_t& circSize) :
      pCircFrCnt2Timestamp(circSize) {}
  };
  LatencySubSys pLatencySched;
  mq::FileSystemChangeListener mFsListener;
  //
  // => background updating
  //
  /// thread ID of the dumper thread
  AssistedThread updaterThread;
  /// maps a notification subject to changes that happened in the current time frame
  static std::map<std::string, int>
  gNotificationsBufferFs;   /**< Shared object change notification for filesystems */
  static std::map<std::string, int>
  gNotificationsBufferProxy; /**< Shared object change notification for proxy nodes */
  static const unsigned char sntFilesystem, sntGateway, sntDataproxy;
  static std::map<std::string, unsigned char> gQueue2NotifType;
  /// deletions to be carried out ASAP
  /// they are delayed so that any function that is using the treemapentry can safely finish
  std::list<SchedTME*> pPendingDeletionsFs;
  std::list<DataProxyTME*> pPendingDeletionsDp;
  /// indicate if the updater is paused
  static sem_t gUpdaterPauseSem;
  static bool gUpdaterPaused;
  static bool gUpdaterStarted;
//**********************************************************
// END DATA MEMBERS
//**********************************************************


  void updateAtomicPenalties();

  /// Trees update management
  void listenFsChange(ThreadAssistant& assistant);

  /// Clean
  void checkPendingDeletionsFs()
  {
    int count = 0;
    auto lastEntry = pPendingDeletionsFs.begin();
    bool eraseLastEntry = false;

    for (auto it = pPendingDeletionsFs.begin(); it != pPendingDeletionsFs.end();
         it++) {
      if (eraseLastEntry) {
        pPendingDeletionsFs.erase(lastEntry);
      }

      eraseLastEntry = false;

      if (!(*it)->fastStructLockWaitersCount) {
        delete(*it);
        eraseLastEntry = true;
        count++;
      }

      lastEntry = it;
    }

    if (eraseLastEntry) {
      pPendingDeletionsFs.erase(lastEntry);
    }

    eos_debug("%d pending deletions executed for filesystems", count);
  }

  void checkPendingDeletionsDp()
  {
    int count = 0;
    auto lastEntry = pPendingDeletionsDp.begin();
    bool eraseLastEntry = false;

    for (auto it = pPendingDeletionsDp.begin(); it != pPendingDeletionsDp.end();
         it++) {
      if (eraseLastEntry) {
        pPendingDeletionsDp.erase(lastEntry);
      }

      eraseLastEntry = false;

      if (!(*it)->fastStructLockWaitersCount) {
        delete(*it);
        eraseLastEntry = true;
        count++;
      }

      lastEntry = it;
    }

    if (eraseLastEntry) {
      pPendingDeletionsDp.erase(lastEntry);
    }

    eos_debug("%d pending deletions executed for dataproxys", count);
  }

  /// thread-local buffer management
  static void tlFree(void* arg);
  static char* tlAlloc(size_t size);

  inline void applyDlScorePenalty(SchedTME* entry,
                                  const SchedTreeBase::tFastTreeIdx& idx, const char& penalty,
                                  bool background = false)
  {
    FastStructSched* ft = background ? entry->backgroundFastStruct :
                          entry->foregroundFastStruct;
    ft->applyDlScorePenalty(idx, penalty, background);
  }

  inline void applyUlScorePenalty(SchedTME* entry,
                                  const SchedTreeBase::tFastTreeIdx& idx, const char& penalty,
                                  bool background = false)
  {
    FastStructSched* ft = background ? entry->backgroundFastStruct :
                          entry->foregroundFastStruct;
    ft->applyUlScorePenalty(idx, penalty, background);
  }

  inline void recallScorePenalty(SchedTME* entry,
                                 const SchedTreeBase::tFastTreeIdx& idx)
  {
    auto fsid = (*entry->backgroundFastStruct->treeInfo)[idx].fsId;
    tLatencyStats& lstat = pLatencySched.pFsId2LatencyStats[fsid];
    //auto mydata = entry->backgroundFastStruct->placementTree->pNodes[idx].fsData;
    int count = 0;

    for (size_t circIdx = pFrameCount % pCircSize;
         (lstat.lastupdate != 0) &&
         (pLatencySched.pCircFrCnt2Timestamp[circIdx] > lstat.lastupdate -
          pPublishToPenaltyDelayMs);
         circIdx = ((pCircSize + circIdx - 1) % pCircSize)) {
      if (entry->foregroundFastStruct->placementTree->pNodes[idx].fsData.dlScore > 0)
        applyDlScorePenalty(entry, idx,
                            pPenaltySched.pCircFrCnt2FsPenalties[circIdx][fsid].dlScorePenalty,
                            true
                           );

      if (entry->foregroundFastStruct->placementTree->pNodes[idx].fsData.ulScore > 0)
        applyUlScorePenalty(entry, idx,
                            pPenaltySched.pCircFrCnt2FsPenalties[circIdx][fsid].ulScorePenalty,
                            true
                           );

      if (++count == (int)pCircSize) {
        eos_warning("Last fs update for fs %d is older than older penalty : it could happen as a transition but should not happen permanently.",
                    (int)fsid);
        break;
      }
    }

//    if(mydata.dlScore!=entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.dlScore || mydata.ulScore!=entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.ulScore)
//    {
//      eos_static_info("score before recalling penalties dl=%d  ul=%d",
//                      (int)mydata.dlScore,
//                      (int)mydata.ulScore);
//
//      eos_static_info("score after recalling penalties dl=%d  ul=%d",
//                    (int)entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.dlScore,
//                    (int)entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.ulScore);
//    }
  }

  template<class T> bool placeNewReplicas(SchedTME* entry,
                                          const size_t& nNewReplicas,

                                          std::vector<SchedTreeBase::tFastTreeIdx>* newReplicas,
                                          T* placementTree,
                                          std::vector<SchedTreeBase::tFastTreeIdx>* existingReplicas = NULL,
                                          unsigned long long bookingSize = 0,
                                          const SchedTreeBase::tFastTreeIdx& startFromNode = 0,
                                          const size_t& nFinalCollocatedReplicas = 0,
                                          std::vector<SchedTreeBase::tFastTreeIdx>* excludedNodes = NULL)
  {
    // a read lock is supposed to be acquired on the fast structures
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    bool updateNeeded = false;

    if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
      stringstream ss;
      ss << (*placementTree);
      eos_debug("fast tree used to copy from is: \n %s", ss.str().c_str());
    }

    // make a working copy of the required fast tree
    // allocate the buffer only once for the lifetime of the thread
    if (!tlGeoBuffer) {
      tlGeoBuffer = tlAlloc(gGeoBufferSize);
    }

    if (placementTree->copyToBuffer((char*)tlGeoBuffer, gGeoBufferSize)) {
      eos_crit("could not make a working copy of the fast tree");
      return false;
    }

    T* tree = (T*)tlGeoBuffer;
    // place the existing replicas
    size_t nAdjustCollocatedReplicas = nFinalCollocatedReplicas;

    if (existingReplicas) {
      size_t ncomp = (*tree->pTreeInfo)[startFromNode].fullGeotag.find("::");

      if (ncomp == std::string::npos) {
        ncomp = (*tree->pTreeInfo)[startFromNode].fullGeotag.size();
      }

      for (auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it) {
        tree->pNodes[*it].fileData.freeSlotsCount = 0;
        tree->pNodes[*it].fileData.takenSlotsCount = 1;

        // check if this replica is to be considered as a collocated one
        if (startFromNode) {
          // we have an accesser geotag
          if ((*tree->pTreeInfo)[startFromNode].fullGeotag.compare(0, ncomp,
              (*tree->pTreeInfo)[*it].fullGeotag) == 0
              && ((*tree->pTreeInfo)[*it].fullGeotag.size() == ncomp ||
                  (*tree->pTreeInfo)[*it].fullGeotag[ncomp] == ':')) {
            // this existing replica is under the same first level of the tree
            // we consider it as a collocated replica
            if (nAdjustCollocatedReplicas) {
              nAdjustCollocatedReplicas--;
            }
          }
        }
      }

      if (nAdjustCollocatedReplicas > nNewReplicas) {
        nAdjustCollocatedReplicas = nNewReplicas;
      }

      // if(!startFromNode), the value of nCollocatedReplicas does not make any difference and furthermore, it should be zero

      // update the tree
      // (could be made faster for a small number of existing replicas by using update branches)
      if (!existingReplicas->empty()) {
        updateNeeded = true;
      }
    }

    if (excludedNodes) {
      // mark the excluded branches as unavailable and sort the branches (no deep, or we would lose the unavailable marks)
      for (auto it = excludedNodes->begin(); it != excludedNodes->end(); ++it) {
        tree->pNodes[*it].fsData.mStatus = tree->pNodes[*it].fsData.mStatus &
                                           ~SchedTreeBase::Available;
      }

      if (!excludedNodes->empty()) {
        updateNeeded = true;
      }
    }

    if (bookingSize) {
      for (auto it = tree->pFs2Idx->begin(); it != tree->pFs2Idx->end(); it++) {
        // we prebook the space on all the possible nodes before the selection
        // reminder : this is just a working copy of the tree and will affect only the current placement
        const SchedTreeBase::tFastTreeIdx& idx = (*it).second;
        float& freeSpace = tree->pNodes[idx].fsData.totalSpace;

        if (freeSpace > bookingSize) { // if there is enough space , prebook it
          freeSpace -= bookingSize;
        } else { // if there is not enough space, make the node unavailable
          tree->pNodes[idx].fsData.mStatus = tree->pNodes[idx].fsData.mStatus &
                                             ~SchedTreeBase::Available;
        }
      }

      updateNeeded = true;
    } else {
      // Test at lest that we have some free space
      for (auto it = tree->pFs2Idx->begin(); it != tree->pFs2Idx->end(); ++it) {
        const SchedTreeBase::tFastTreeIdx& idx = (*it).second;
        float& freeSpace = tree->pNodes[idx].fsData.totalSpace;

        if (!freeSpace) {
          tree->pNodes[idx].fsData.mStatus = tree->pNodes[idx].fsData.mStatus &
                                             ~SchedTreeBase::Available;
          updateNeeded = true;
        }
      }
    }

    // do the placement
    if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
      stringstream ss;
      ss << (*tree);
      eos_debug("fast tree used for placement is: \n %s", ss.str().c_str());
    }

    if (updateNeeded) {
      tree->updateTree();
    }

    for (size_t k = 0; k < nNewReplicas; k++) {
      SchedTreeBase::tFastTreeIdx idx;
      SchedTreeBase::tFastTreeIdx startidx = (k < nNewReplicas -
                                              nAdjustCollocatedReplicas) ? 0 : startFromNode;

      if (!tree->findFreeSlot(idx, startidx, true /*allow uproot if necessary*/, true,
                              false)) {
        eos_debug("could not find a new slot for a replica in the fast tree");
        stringstream ss;
        ss << (*tree);
        eos_debug("iteration number %lu fast tree used for placement is: \n %s", k,
                  ss.str().c_str());
        return false;
      }

      newReplicas->push_back(idx);
    }

    return true;
  }

  template<class T> unsigned char accessReplicas(SchedTME* entry,
      const size_t& nNewReplicas,
      std::vector<SchedTreeBase::tFastTreeIdx>* accessedReplicas,
      SchedTreeBase::tFastTreeIdx accesserNode,
      std::vector<SchedTreeBase::tFastTreeIdx>* existingReplicas,
      T* accessTree,
      bool skipSaturated = false)
  {
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
      stringstream ss;
      ss << (*accessTree);
      eos_debug("fast tree used to copy from is: \n %s", ss.str().c_str());
    }

    // make a working copy of the required fast tree
    // allocate the buffer only once for the lifetime of the thread
    if (!tlGeoBuffer) {
      tlGeoBuffer = tlAlloc(gGeoBufferSize);
    }

    if (accessTree->copyToBuffer((char*)tlGeoBuffer, gGeoBufferSize)) {
      eos_crit("could not make a working copy of the fast tree");
      return 0;
    }

    T* tree = (T*)tlGeoBuffer;
    eos_static_debug("saturationTresh original=%d / copy=%d",
                     (int)accessTree->pBranchComp.saturationThresh,
                     (int)tree->pBranchComp.saturationThresh);

    // place the existing replicas
    if (existingReplicas) {
      for (auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it) {
        tree->pNodes[*it].fileData.freeSlotsCount = 1;
        tree->pNodes[*it].fileData.takenSlotsCount = 0;
      }

      // update the tree
      // (could be made faster for a small number of existing replicas by using update branches)
      tree->updateTree();
    }

    // do the access
    if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
      stringstream ss;
      ss << (*tree);
      eos_debug("fast tree used for access is: \n %s", ss.str().c_str());
    }

    // do the access
    unsigned char retCode = 0;

    for (size_t k = 0; k < nNewReplicas; k++) {
      SchedTreeBase::tFastTreeIdx idx;

      if (!tree->findFreeSlot(idx, accesserNode, true, true, skipSaturated)) {
        if (skipSaturated) {
          eos_debug("Could not find any replica to access while skipping saturated fs. Trying with saturated nodes included");
        }

        if ((!skipSaturated) || !tree->findFreeSlot(idx, 0, false, true, false)) {
          eos_debug("could not find a new slot for a replica in the fast tree");
          return 0;
        } else {
          retCode = 1;
        }
      } else {
        retCode = 2;
      }

      accessedReplicas->push_back(idx);
    }

    return retCode;
  }

  bool updateTreeInfo(SchedTME* entry, eos::common::FileSystem::fs_snapshot_t* fs,
                      int keys, SchedTreeBase::tFastTreeIdx ftidx = 0 , SlowTreeNode* stn = NULL);
  bool updateTreeInfo(const map<string, int>& updatesFs,
                      const map<string, int>& updatesDp);
  //bool updateTreeInfoFs(const map<string,int> &updatesFs);

  template<typename T> bool _setInternalParam(T& param, const T& value,
      bool updateStructs)
  {
    eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
    eos::common::RWMutexWriteLock lock2(pTreeMapMutex);
    eos::common::RWMutexWriteLock lock3(configMutex);
    bool result = true;
    param = value;

    for (auto it = pFs2SchedTME.begin(); it != pFs2SchedTME.end(); it++) {
      if (updateStructs) {
        it->second->fastStructModified = true;
        it->second->slowTreeModified = true;
        result = result && updateFastStructures(it->second);
      }
    }

    return result;
  }

  static void setConfigValue(const char* prefix,
                             const char* key,
                             const char* val);

  template<typename T> bool setInternalParam(T& param, const int& value,
      bool updateStructs, const std::string& configentry)
  {
    bool ret = _setInternalParam(param, static_cast<T>(value), updateStructs);

    if (ret && configentry.length()) {
      XrdOucString s;
      s.append((int)value);
      setConfigValue("geosched", configentry.c_str() , s.c_str());
    }

    return ret;
  }

  template<typename T> bool setInternalParam(T& param, const float& value,
      bool updateStructs, const std::string& configentry)
  {
    bool ret = _setInternalParam(param, static_cast<T>(value), updateStructs);

    if (ret && configentry.length()) {
      XrdOucString s;
      char buf[32];
      sprintf(buf, "%f", value);
      s += buf;
      setConfigValue("geosched", configentry.c_str() , s.c_str());
    }

    return ret;
  }

  bool setInternalParam(std::vector<char>& param, const std::vector<char >& value,
                        bool updateStructs, const std::string& configentry)
  {
    bool ret = _setInternalParam(param, value, updateStructs);

    if (ret && configentry.length()) {
      XrdOucString s;
      s += "[";

      for (size_t i = 0; i < param.size(); i++) {
        s += (int)value[i];
        s += ",";
      }

      s[s.length() - 1] = ']';
      setConfigValue("geosched", configentry.c_str() , s.c_str());
    }

    return ret;
  }

  bool setInternalParam(std::vector<float>& param,
                        const std::vector<float>& value, bool updateStructs,
                        const std::string& configentry)
  {
    bool ret = _setInternalParam(param, value, updateStructs);

    if (ret && configentry.length()) {
      XrdOucString s;
      s += "[";

      for (size_t i = 0; i < param.size(); i++) {
        char buf[32];
        sprintf(buf, "%f", value[i]);
        s += buf;
        s += ",";
      }

      s[s.length() - 1] = ']';
      setConfigValue("geosched", configentry.c_str() , s.c_str());
    }

    return ret;
  }

  // enum to specify the expected type of proxy scheduling
  typedef enum {
    filesticky, // try to map a given file as much as possible to a same proxy. This is to optimize caching in the Proxy.
    regular,    // give priority to the closer and more idle proxy in a proxygroup
    any         // do the regular scheduling for all the filesystems
  } tProxySchedType;
  bool findProxy(const std::vector<SchedTreeBase::tFastTreeIdx>& fsidxs,
                 std::vector<SchedTME*> entries,
                 ino64_t inode,
                 std::vector<std::string>* proxies,
                 std::vector<std::string>* proxyGroups = NULL,
                 const std::string& clientgeotag = "",
                 tProxySchedType proxyschedtype = regular);
  bool markPendingBranchDisablings(const std::string& group,
                                   const std::string& optype, const std::string& geotag);
  bool applyBranchDisablings(const SchedTME& entry);
  bool setSkipSaturatedAccess(bool value, bool setconfig = false);
  bool setSkipSaturatedDrnAccess(bool value, bool setconfig = false);
  bool setSkipSaturatedBlcAccess(bool value, bool setconfig = false);
  bool setProxyCloseToFs(bool value, bool setconfig = false);
  bool setScorePenalty(std::vector<float>& fvector, std::vector<char>& cvector,
                       const std::vector<char>& value, const std::string& configentry);
  bool setScorePenalty(std::vector<float>& fvector, std::vector<char>& cvector,
                       const char* vvalue, const std::string& configentry);
  bool setScorePenalty(std::vector<float>& fvector, std::vector<char>& cvector,
                       char value, int netSpeedClass, const std::string& configentry);
  bool setPlctDlScorePenalty(char value, int netSpeedClass,
                             bool setconfig = false);
  bool setPlctUlScorePenalty(char value, int netSpeedClass,
                             bool setconfig = false);
  bool setAccessDlScorePenalty(char value, int netSpeedClass,
                               bool setconfig = false);
  bool setAccessUlScorePenalty(char value, int netSpeedClass,
                               bool setconfig = false);
  bool setProxyScorePenalty(char value, int netSpeedClass,
                            bool setconfig = false);
  bool setPlctDlScorePenalty(const char* value, bool setconfig = false);
  bool setPlctUlScorePenalty(const char* value, bool setconfig = false);
  bool setAccessDlScorePenalty(const char* value, bool setconfig = false);
  bool setAccessUlScorePenalty(const char* value, bool setconfig = false);
  bool setProxyScorePenalty(const char* value, bool setconfig = false);
  bool setFillRatioLimit(char value, bool setconfig = false);
  bool setFillRatioCompTol(char value, bool setconfig = false);
  bool setSaturationThres(char value, bool setconfig = false);
  bool setTimeFrameDurationMs(int value, bool setconfig = false);
  bool setPenaltyUpdateRate(float value, bool setconfig = false);
  bool accessReqFwEP(const std::string& targetGeotag,
                     const std::string& accesserGeotag) const ;
  std::string accessGetProxygroup(const std::string& geotag) const ;
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  GeoTreeEngine(mq::MessagingRealm* realm);

  // ---------------------------------------------------------------------------
  //! Force a refresh of the information in the scheduling trees
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool forceRefreshSched();

  // ---------------------------------------------------------------------------
  //! Force a refresh of the information in the scheduling trees and in theproxy trees
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool forceRefresh();

  // ---------------------------------------------------------------------------
  //! Insert a file system into the GeoTreeEngine
  // @param fs
  //   the file system to be inserted
  // @param group
  //   the group the file system belongs to
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool insertFsIntoGroup(FileSystem* fs , FsGroup* group,
                         const common::FileSystemCoreParams& coreParams);

  // ---------------------------------------------------------------------------
  //! Remove a file system into the GeoTreeEngine
  // @param fs
  //   the file system to be removed
  // @param group
  //   the group the file system belongs to
  // @param updateFastStructures
  //   should the fast structures be updated immediately without waiting for the next time frame
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool removeFsFromGroup(FileSystem* fs , FsGroup* group,
                         bool updateFastStructures = true);

  // ---------------------------------------------------------------------------
  //! Remove a file system into the GeoTreeEngine
  // @param group
  //   the group the file system belongs to
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool removeGroup(FsGroup* group);

  // ---------------------------------------------------------------------------
  //! Print formated information about the GeoTreeEngine
  // @param info
  //   the string to which info is to be written
  // @param dispTree
  //   do trees should be printed
  // @param dispSnaps
  //   do snapshots should be printed
  // @param dispLs
  //   do internal state should be printed
  // @param schedgroup
  //   narrow down information to this schedgroup
  // @param schedgroup
  //   narrow down information to this type of operation
  // ---------------------------------------------------------------------------
  void printInfo(std::string& info,
                 bool dispTree, bool dispSnaps, bool dispParam, bool dispState,
                 const std::string& schedgroup, const std::string& optype,
                 bool useColors = false, bool monitoring = false);

  // ---------------------------------------------------------------------------
  //! Place several replicas in one scheduling group.
  // @param group
  //   the group to place the replicas in
  // @param nNewReplicas
  //   the number of replicas to be placed
  // @param newReplicas
  //   vector to which fsids of new replicas are appended if the placement
  //   succeeds. They are appended in decreasing priority order
  // @param inode
  //   inode of the file to place, used for filesticky proxy scheduling
  // @param dataProxys
  //   if this pointer is non NULL, one proxy is returned for each filesystem returned
  //   if they have a proxygroup defined
  //   if a fs has proxygroup and no proxy could be found, the placement operation fails
  // @param firewallEntryPoints
  //   if this pointer is non NULL, one firewall entry point is returned for each filesystem returned
  //   if no entry point could be found for an fs, the placement operation fails
  // @param type
  //   type of placement to be performed. It can be:
  //     regularRO, regularRW, balancing or draining
  // @param existingReplicas
  //   fsids of preexisting replicas for the current file
  //   this is important to make a a good placement (e.g. skip the same fs)
  // @param bookingSize
  //   the space to be booked on the fs
  //   currently, it's not booking. It's only checking that there is enough space.
  // @param startFromGeoTag
  //   try to place the files under this geotag
  //   useful to group up replicas or to replace a replica by a new one nearby
  // @param clientGeoTag
  //   try to place the proxys (data and firewall) close to the client
  // @param nCollocatedReplicas
  //   among the nNewReplicas, nCollocatedReplicas are placed as close as possible to startFromGeoTag
  //   the other ones are scattered out as much as possible in the tree
  //   this count includes the existingReplicas
  // @param excludeFs
  //   fsids of files to exclude from the placement operation
  // @param excludeGeoTags
  //   geotags of branches to exclude from the placement operation
  //     (e.g. exclude a site)
  // @param forceGeoTags
  //   geotags of branches new replicas should be taken from
  //     (e.g. force a site)
  // @return
  //   true if the success false else
  // ---------------------------------------------------------------------------
  bool placeNewReplicasOneGroup(FsGroup* group, const size_t& nNewReplicas,
                                std::vector<eos::common::FileSystem::fsid_t>* newReplicas,
                                ino64_t inode,
                                std::vector<std::string>* dataProxys,
                                std::vector<std::string>* firewallEntryPoints,
                                SchedType type,
                                std::vector<eos::common::FileSystem::fsid_t>* existingReplicas,
                                std::vector<std::string>* fsidsgeotags = 0,
                                unsigned long long bookingSize = 0,
                                const std::string& startFromGeoTag = "",
                                const std::string& clientGeoTag = "",
                                const size_t& nCollocatedReplicas = 0,
                                std::vector<eos::common::FileSystem::fsid_t>* excludeFs = NULL,
                                std::vector<std::string>* excludeGeoTags = NULL);

  // this function to access replica spread across multiple scheduling group is a BACKCOMPATIBILITY artifact
  // the new scheduler doesn't try to place files across multiple scheduling groups.
  //  bool accessReplicasMultipleGroup(const size_t &nAccessReplicas,
  //      std::vector<eos::common::FileSystem::fsid_t> *accessedReplicas,
  //      std::vector<eos::common::FileSystem::fsid_t> *existingReplicas,
  //      SchedType type=regularRO,
  //      const std::string &accesserGeotag="",
  //      std::vector<eos::common::FileSystem::fsid_t> *excludeFs=NULL,
  //      std::vector<std::string> *excludeGeoTags=NULL);

  // ---------------------------------------------------------------------------
  //! Access replicas across one or several scheduling group.
  //! Check that the right number of replicas is online.
  //! return the best possible head replica
  // @param nReplicas
  //   the number of replicas to access
  // @param fsindex
  //   return the index of the head replica in the existingReplicas vector
  // @param existingReplicas
  //   fsids of preexisting replicas for the current file
  // @param inode
  //   inode of the file to place, used for filesticky proxy scheduling
  // @param dataProxys
  //   if this pointer is non NULL, one proxy is returned for each filesystem returned
  //   if they have a proxygroup defined
  //   if a fs has proxygroup and no proxy could be found, the access operation fails
  // @param firewallEntryPoints
  //   if this pointer is non NULL, one firewall entry point is returned for each filesystem returned
  //   if no entry point could be found for an fs, the access operation fails
  // @param type
  //   type of access to be performed. It can be:
  //     regularRO, regularRW, balancing or draining
  // @param accesserGeoTag
  //   try to get the replicas as close to this geotag as possible
  // @param forcedFsId
  //   if non zeros, force the head replica fsid
  // @param unavailableFs
  //   return the unavailable file systems for the current access operation
  // @return
  //   EROFS   if not enough replicas are provided to the function to
  //           make sure that enough replicas are available for this access
  //   ENODATA if the forced head replica is not in the provided replicas
  //   EIO     if some internal inconsistency arises
  //   ENONET  if there is not enough available fs among the provided ones
  //           for this access operation
  //   0       if success
  // ---------------------------------------------------------------------------
  int accessHeadReplicaMultipleGroup(const size_t& nReplicas,
                                     unsigned long& fsIndex,
                                     std::vector<eos::common::FileSystem::fsid_t>* existingReplicas,
                                     ino64_t inode,
                                     std::vector<std::string>* dataProxys,
                                     std::vector<std::string>* firewallEntryPoints,
                                     SchedType type = regularRO,
                                     const std::string& accesserGeotag = "",
                                     const eos::common::FileSystem::fsid_t& forcedFsId = 0,
                                     std::vector<eos::common::FileSystem::fsid_t>* unavailableFs = NULL
                                    );

  // ---------------------------------------------------------------------------
  //! Start the background updater thread
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  void StartUpdater();

  // ---------------------------------------------------------------------------
  //! Pause the updating of the GeoTreeEngine but keep accumulating
  //! modification notifications
  // ---------------------------------------------------------------------------
  inline static bool PauseUpdater()
  {
    if (gUpdaterStarted && !gUpdaterPaused) {
      timespec ts;
      eos::common::Timing::GetTimeSpec(ts, false);
      ts.tv_sec +=
        2; // we wait for two seconds and then we fail. It avoids deadlocking when no update is received (No FST)
      int rc = 0;

      while ((rc = sem_timedwait(&gUpdaterPauseSem, &ts)) && errno == EINTR) {
        continue;
      }

      if (rc && (errno == ETIMEDOUT)) {
        return false;
      }

      if (rc && errno) {
        throw "sem_timedwait() failed";
      }

      gUpdaterPaused = true;
      return true;
    }

    return true; // already paused
  }

  // ---------------------------------------------------------------------------
  //! Resume the updating of the GeoTreeEngine
  //! Process all the notifications accumulated since it was paused
  // ---------------------------------------------------------------------------
  inline static void ResumeUpdater()
  {
    if (gUpdaterStarted && gUpdaterPaused) {
      if (sem_post(&gUpdaterPauseSem)) {
        throw "sem_post() failed";
      }

      gUpdaterPaused = false;
    }
  }

  // ---------------------------------------------------------------------------
  //! Stop the background updater thread
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  void StopUpdater();

  // ---------------------------------------------------------------------------
  //! Get the fs informations in the GeotreeEngine
  //! It's faster than accessing the MqHash
  // @param fsids
  //   a vector containing the FsIds
  // @param fsgeotags
  //   return if non NULL, geotags of the fsids are reported in this vector
  // @param hosts
  //   return if non NULL, hosts of the fsids are reported in this vector
  // @param sortedgroups
  //   return if non NULL, get the list of groups in decreasing order of number of fs in the list they contain
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool getInfosFromFsIds(const std::vector<FileSystem::fsid_t>& fsids,
                         std::vector<std::string>* fsgeotags,
                         std::vector<std::string>* hosts, std::vector<FsGroup*>* sortedgroups);

  // ---------------------------------------------------------------------------
  //! Set an internal parameter to a value
  // @param param
  //   the name of the parameter to set
  // @param value
  //   the value of the parameter to set
  // @param iparamidx
  //   in case this parameter is a vector, it's the index of the value to set
  //   if iparamidx == -1, sets all the values of the elevemets of the vector to the same passed value
  //   if iparamidx == -2, the value string contains all the values in the vector e.g.: "[2,3,4]"
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool setParameter(std::string param, const std::string& value, int iparamidx,
                    bool setconfig = false);

  // ---------------------------------------------------------------------------
  //! Add a branch disabling rule
  // @param group
  //   group name or "*"
  // @param optype
  //   "*" or one of the following plct,accsro,accsrw,accsdrain,plctdrain
  // @param geotag
  //   geotag of the branch to disable
  // @param output
  //   if non NULL, issue error messages there
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool addDisabledBranch(const std::string& group, const std::string& optype,
                         const std::string& geotag, XrdOucString* output = NULL, bool toConfig = false);

  // ---------------------------------------------------------------------------
  //! Rm a branch disabling rule
  // @param group
  //   group name or "*"
  // @param optype
  //   "*" or one of the following plct,accsro,accsrw,accsdrain,plctdrain
  // @param geotag
  //   geotag of the branch to disable
  // @param output
  //   if non NULL, issue error messages there
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool rmDisabledBranch(const std::string& group, const std::string& optype,
                        const std::string& geotag, XrdOucString* output = NULL, bool toConfig = false);

  // ---------------------------------------------------------------------------
  //! Show branch disabling rules
  // @param group
  //   group name or "*"
  // @param optype
  //   "*" or one of the following plct,accsro,accsrw,accsdrain,plctdrain
  // @param geotag
  //   geotag of the branch to disable
  // @param output
  //   the display is appended to that string
  // @param lock
  //   lock the config param mutex
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool showDisabledBranches(const std::string& group, const std::string& optype,
                            const std::string& geotag, XrdOucString* output, bool lock = true);

  // ---------------------------------------------------------------------------
  //! Set an access geotag mapping.
  // @param geotag
  //   geotag of the accesser
  // @param geotag list
  //   a list of geotags (separted by commas) defining subtrees of the geotree
  //   that can be accessed by the accesser
  // @param updateFastStruct
  //        update the fast structures too (needs to be done for the cchange to be effective)
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  inline bool setAccessGeotagMapping(XrdOucString* output,
                                     const std::string& geotag, const std::string& geotaglist,
                                     bool updateFastStruct = true, bool setconfig = true)
  {
    bool ret = pAccessGeotagMapping.setMapping(geotag, geotaglist, updateFastStruct,
               setconfig);

    if (!ret && output) {
      *output += "Error: failed to add direct access geotag mapping";
    }

    return ret;
  }
  inline bool setAccessGeotagMapping(const std::string& geotag,
                                     bool setconfig = false)
  {
    return pAccessGeotagMapping.setMapping(geotag, setconfig);
  }


  // ---------------------------------------------------------------------------
  //! Set an access geotag mapping.
  // @param geotag
  //   geotag of the accesser for which to clear the mapping
  //   if empty, all the mappings are deleted
  // @param updateFastStruct
  //        update the fast structures too (needs to be done for the cchange to be effective)
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  inline bool clearAccessGeotagMapping(XrdOucString* output,
                                       const std::string& geotag = "", bool updateFastStruct = true,
                                       bool setconfig = true)
  {
    bool ret = pAccessGeotagMapping.clearMapping(geotag, updateFastStruct,
               setconfig);

    if (!ret && output) {
      *output += "Error: failed to clear direct access geotag mapping";
    }

    if (ret && geotag.empty() && output) {
      *output += "Cleared all direct access geotag mappings";
    }

    return ret;
  }

  // ---------------------------------------------------------------------------
  //! Set an access geotag mapping.
  // @param output
  //   the display is appended to that string
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  inline bool showAccessGeotagMapping(XrdOucString* output, bool monitoring)
  {
    if (!pAccessGeotagMapping.inuse) {
      *output +=
        "There is no direct access geotag mapping defined. All file accesses will be scheduled as direct accesses.";
      return true;
    }

    return pAccessGeotagMapping.showMapping(output, "AccessGeotagMapping",
                                            monitoring);
  }

  // ---------------------------------------------------------------------------
  //! Set an access proxygroup mapping.
  // @param geotag
  //   geotag of the accesser
  // @param proxygroup
  //   name of the proxygroup acting as firewall entrypoint for the subtree starting at the geotag
  // @param updateFastStruct
  //        update the fast structures too (needs to be done for the cchange to be effective)
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  inline bool setAccessProxygroup(XrdOucString* output, const std::string& geotag,
                                  const std::string& proxygroup, bool updateFastStruct = true,
                                  bool setconfig = true)
  {
    bool ret = pAccessProxygroup.setMapping(geotag, proxygroup, updateFastStruct,
                                            setconfig);

    if (!ret && output) {
      *output += "Error: failed to add access proxygroup mapping";
    }

    return ret;
  }
  inline bool setAccessProxygroup(const std::string& geotag,
                                  bool setconfig = false)
  {
    return pAccessProxygroup.setMapping(geotag, setconfig);
  }


  // ---------------------------------------------------------------------------
  //! Remove an (or all) access geotag mapping.
  // @param geotag
  //   geotag of the accesser for which to clear the mapping
  //   if empty, all the mappings are deleted
  // @param updateFastStruct
  //        update the fast structures too (needs to be done for the cchange to be effective)
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  inline bool clearAccessProxygroup(XrdOucString* output,
                                    const std::string& geotag = "", bool updateFastStruct = true,
                                    bool setconfig = true)
  {
    bool ret = pAccessProxygroup.clearMapping(geotag, updateFastStruct, setconfig);

    if (!ret && output) {
      *output += "Error: failed to clear access proxygroup mapping";
    }

    if (ret && geotag.empty() && output) {
      *output += "Cleared all access proxygroup mappings";
    }

    return ret;
  }

  // ---------------------------------------------------------------------------
  //! Set an access proxygroup mapping.
  // @param output
  //   the display is appended to that string
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  inline bool showAccessProxygroup(XrdOucString* output, bool monitoring)
  {
    if (!pAccessProxygroup.inuse) {
      *output +=
        "There is no access proxygroup mapping defined. No firewall entry point access can be scheduled.";
      return true;
    }

    return pAccessProxygroup.showMapping(output, "AccessProxygroupMapping",
                                         monitoring);
  }
  //! [public member functions]

};

EOSMGMNAMESPACE_END

#endif
