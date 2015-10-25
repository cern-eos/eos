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
//#define EOS_GEOTREEENGINE_USE_INSTRUMENTED_MUTEX

/*----------------------------------------------------------------------------*/
#include "mgm/FsView.hh"
#include "mgm/geotree/SchedulingSlowTree.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

/*----------------------------------------------------------------------------*/
/**
 * @file GeoTreeEngine.hh
 * 
 * @brief Class responsible to handle GeoTree Operations
 * (file placement for new replica, source finding for balancing and draining)
 * 
 * The Messaging class continuously keeps the tree info in this class up-to-date.
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
  struct Penalties
  {
    char
    dlScorePenalty,ulScorePenalty;

    Penalties() :
      dlScorePenalty(0),ulScorePenalty(0)
    {}
  };
  typedef std::vector<Penalties> tPenaltiesVec;
  typedef std::map<std::string,Penalties> tPenaltiesMap;

  struct tLatencyStats
  {
    double minlatency,maxlatency,averagelatency,lastupdate,age;
    tLatencyStats() :
      minlatency(std::numeric_limits<double>::max()), maxlatency(-std::numeric_limits<double>::max()),
      averagelatency(0.0),lastupdate(0.0), age(0.0) {};
    double getage(double nowms=0.0)
    {
      if(nowms == 0.0)
      {
        struct timeval curtime;
        gettimeofday(&curtime, 0);
        nowms = curtime.tv_sec*1000 + curtime.tv_usec/1000;
      }
      return  nowms - lastupdate;
    }
    void update(const double &nowms=0.0)
    {
      double latency = getage(nowms);
      averagelatency = (averagelatency!=0.0)?(averagelatency*0.99+latency*0.01):latency;
      minlatency = std::min( minlatency , latency);
      maxlatency = std::max( maxlatency , latency);
    }
  };

  struct nodeAgreg{
    bool saturated;
    size_t fsCount;
    size_t rOpen;
    size_t wOpen;
    size_t gOpen; // number of files open as dataproxy (no data hosted on any local fs)
    double netOutWeight;
    double netInWeight;
    double diskUtilSum;
    size_t netSpeedClass;
    nodeAgreg() : saturated(false),fsCount(0),rOpen(0),wOpen(0),gOpen(0),netOutWeight(0.0),netInWeight(0.0),diskUtilSum(0.0),netSpeedClass(0) {};
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
  struct FastStructSched
  {
    FastROAccessTree* rOAccessTree;
    FastRWAccessTree* rWAccessTree;
    FastBalancingAccessTree* blcAccessTree;
    FastDrainingAccessTree* drnAccessTree;
    FastPlacementTree* placementTree;
    FastBalancingPlacementTree* blcPlacementTree;
    FastDrainingPlacementTree* drnPlacementTree;
    SchedTreeBase::FastTreeInfo* treeInfo;
    Fs2TreeIdxMap* fs2TreeIdx;
    GeoTag2NodeIdxMap* tag2NodeIdx;
    tPenaltiesVec *penalties;

    FastStructSched()
    {
      rOAccessTree = new FastROAccessTree;
      rOAccessTree->selfAllocate(FastROAccessTree::sGetMaxNodeCount());
      rWAccessTree = new FastRWAccessTree;
      rWAccessTree->selfAllocate(FastRWAccessTree::sGetMaxNodeCount());
      blcAccessTree = new FastBalancingAccessTree;
      blcAccessTree->selfAllocate(FastBalancingAccessTree::sGetMaxNodeCount());
      drnAccessTree = new FastDrainingAccessTree;
      drnAccessTree->selfAllocate(FastDrainingAccessTree::sGetMaxNodeCount());
      placementTree = new FastPlacementTree;
      placementTree->selfAllocate(FastPlacementTree::sGetMaxNodeCount());
      blcPlacementTree = new FastBalancingPlacementTree;
      blcPlacementTree->selfAllocate(FastBalancingPlacementTree::sGetMaxNodeCount());
      drnPlacementTree = new FastDrainingPlacementTree;
      drnPlacementTree->selfAllocate(FastDrainingPlacementTree::sGetMaxNodeCount());

      treeInfo = new SchedTreeBase::FastTreeInfo;
      penalties = new tPenaltiesVec;
      penalties->reserve(SchedTreeBase::sGetMaxNodeCount());

      fs2TreeIdx = new Fs2TreeIdxMap;
      fs2TreeIdx->selfAllocate(SchedTreeBase::sGetMaxNodeCount());

      rOAccessTree->pFs2Idx
      = rWAccessTree->pFs2Idx
      = blcAccessTree->pFs2Idx
      = drnAccessTree->pFs2Idx
      = placementTree->pFs2Idx
      = blcPlacementTree->pFs2Idx
      = drnPlacementTree->pFs2Idx
      = fs2TreeIdx;

      rOAccessTree->pTreeInfo
      = rWAccessTree->pTreeInfo
      = blcAccessTree->pTreeInfo
      = drnAccessTree->pTreeInfo
      = placementTree->pTreeInfo
      = blcPlacementTree->pTreeInfo
      = drnPlacementTree->pTreeInfo
      = treeInfo;

      tag2NodeIdx = new GeoTag2NodeIdxMap;
      tag2NodeIdx->selfAllocate(SchedTreeBase::sGetMaxNodeCount());
    }

    ~FastStructSched()
    {
      if(rOAccessTree) delete rOAccessTree;
      if(rWAccessTree) delete rWAccessTree;
      if(blcAccessTree) delete blcAccessTree;
      if(drnAccessTree) delete drnAccessTree;
      if(placementTree) delete placementTree;
      if(blcPlacementTree) delete blcPlacementTree;
      if(drnPlacementTree) delete drnPlacementTree;
      if(treeInfo) delete treeInfo;
      if(penalties) delete penalties;
      if(fs2TreeIdx) delete fs2TreeIdx;
      if(tag2NodeIdx) delete tag2NodeIdx;
    }

    bool DeepCopyTo (FastStructSched *target) const
    {
      if(
	  rOAccessTree->copyToFastTree(target->rOAccessTree) ||
	  rWAccessTree->copyToFastTree(target->rWAccessTree) ||
	  blcAccessTree->copyToFastTree(target->blcAccessTree) ||
	  drnAccessTree->copyToFastTree(target->drnAccessTree) ||
	  placementTree->copyToFastTree(target->placementTree) ||
	  blcPlacementTree->copyToFastTree(target->blcPlacementTree) ||
	  drnPlacementTree->copyToFastTree(target->drnPlacementTree)
      )
      {
	return false;
      }
      // copy the information
      *(target->treeInfo) = *treeInfo;
      if(
	  fs2TreeIdx->copyToFsId2NodeIdxMap(target->fs2TreeIdx) ||
	  tag2NodeIdx->copyToGeoTag2NodeIdxMap(target->tag2NodeIdx) )
      {
	return false;
      }

      // copy the penalties
      std::copy(penalties->begin(), penalties->end(),
                    target->penalties->begin());

      // update the information in the FastTrees to point to the copy
      target->rOAccessTree->pFs2Idx
      = target->rWAccessTree->pFs2Idx
      = target->blcAccessTree->pFs2Idx
      = target->drnAccessTree->pFs2Idx
      = target->placementTree->pFs2Idx
      = target->blcPlacementTree->pFs2Idx
      = target->drnPlacementTree->pFs2Idx
      = target->fs2TreeIdx;
      target->rOAccessTree->pTreeInfo
      = target->rWAccessTree->pTreeInfo
      = target->blcAccessTree->pTreeInfo
      = target->drnAccessTree->pTreeInfo
      = target->placementTree->pTreeInfo
      = target->blcPlacementTree->pTreeInfo
      = target->drnPlacementTree->pTreeInfo
      = target->treeInfo;

      return true;
    }

    void UpdateTrees()
    {
      rOAccessTree->updateTree();
      rWAccessTree->updateTree();
      blcAccessTree->updateTree();
      drnAccessTree->updateTree();
      placementTree->updateTree();
      blcPlacementTree->updateTree();
      drnPlacementTree->updateTree();
    }

    void WriteSlowState( SlowTreeNode::TreeNodeStateFloat &slowState, SchedTreeBase::tFastTreeIdx idx) const
    {
      FastPlacementTree::FsData &fastState = placementTree->pNodes[idx].fsData;
      slowState.dlScore = float(fastState.dlScore)/255;
      slowState.ulScore = float(fastState.ulScore)/255;
      slowState.mStatus = fastState.mStatus & ~eos::mgm::SchedTreeBase::Disabled; // we don't want to back proagate the disabled bit
      slowState.fillRatio = float(fastState.fillRatio)/255;
      slowState.totalSpace = float(fastState.totalSpace);
    }

    inline void applyDlScorePenalty(SchedTreeBase::tFastTreeIdx idx, const char &penalty, bool background)
    {
      AtomicSub(placementTree->pNodes[idx].fsData.dlScore,penalty);
      AtomicSub(drnPlacementTree->pNodes[idx].fsData.dlScore,penalty);
      AtomicSub(blcPlacementTree->pNodes[idx].fsData.dlScore,penalty);
      AtomicSub(rOAccessTree->pNodes[idx].fsData.dlScore,penalty);
      AtomicSub(rWAccessTree->pNodes[idx].fsData.dlScore,penalty);
      AtomicSub(drnAccessTree->pNodes[idx].fsData.dlScore,penalty);
      AtomicSub(blcAccessTree->pNodes[idx].fsData.dlScore,penalty);
      if(!background)
      {
        AtomicAdd((*penalties)[idx].dlScorePenalty,penalty);
      }
    }

    inline void applyUlScorePenalty(SchedTreeBase::tFastTreeIdx idx, const char &penalty, bool background)
    {
      AtomicSub(placementTree->pNodes[idx].fsData.ulScore,penalty);
      AtomicSub(drnPlacementTree->pNodes[idx].fsData.ulScore,penalty);
      AtomicSub(blcPlacementTree->pNodes[idx].fsData.ulScore,penalty);
      AtomicSub(rOAccessTree->pNodes[idx].fsData.ulScore,penalty);
      AtomicSub(rWAccessTree->pNodes[idx].fsData.ulScore,penalty);
      AtomicSub(drnAccessTree->pNodes[idx].fsData.ulScore,penalty);
      AtomicSub(blcAccessTree->pNodes[idx].fsData.ulScore,penalty);
      if(!background)
      {
        AtomicAdd((*penalties)[idx].ulScorePenalty,penalty);
      }
    }

    inline bool isUlScorePos ( SchedTreeBase::tFastTreeIdx idx)
    {
      return placementTree->pNodes[idx].fsData.ulScore>0;
    }

    inline bool isDlScorePos ( SchedTreeBase::tFastTreeIdx idx)
    {
      return placementTree->pNodes[idx].fsData.dlScore>0;
    }

    inline SchedTreeBase::FastTreeInfo* getTreeInfo() { return treeInfo; }

    inline bool buildFastStructures(SlowTree *slowTree)
    {
      return slowTree->buildFastStrcturesSched(
                placementTree , rOAccessTree, rWAccessTree,
                blcPlacementTree , blcAccessTree,
                drnPlacementTree , drnAccessTree,
                treeInfo , fs2TreeIdx, tag2NodeIdx
            );
    }

    inline void resizePenalties ( const size_t &newsize)
    {
      penalties->resize(newsize);
    }

    inline void setConfigParam(
        const char &fillRatioLimit,
        const char &fillRatioCompTol,
        const char &saturationThres)
    {
      rOAccessTree->setSaturationThreshold(saturationThres);
      rWAccessTree->setSaturationThreshold(saturationThres);
      drnAccessTree->setSaturationThreshold(saturationThres);
      blcAccessTree->setSaturationThreshold(saturationThres);

      placementTree->setSaturationThreshold(saturationThres);
      placementTree->setSpreadingFillRatioCap(fillRatioLimit);
      placementTree->setFillRatioCompTol(fillRatioCompTol);
      blcPlacementTree->setSaturationThreshold(saturationThres);
      blcPlacementTree->setSpreadingFillRatioCap(fillRatioLimit);
      blcPlacementTree->setFillRatioCompTol(fillRatioCompTol);
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
  struct FastStructGW
  {
    FastGatewayAccessTree* gWAccessTree;
    SchedTreeBase::FastTreeInfo* treeInfo;
    Host2TreeIdxMap* host2TreeIdx;
    GeoTag2NodeIdxMap* tag2NodeIdx;
    tPenaltiesVec *penalties;

    FastStructGW()
    {
      gWAccessTree = new FastGatewayAccessTree;
      gWAccessTree->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());

      treeInfo = new SchedTreeBase::FastTreeInfo;
      penalties = new tPenaltiesVec;
      penalties->reserve(FastGatewayAccessTree::sGetMaxNodeCount());

      host2TreeIdx = new Host2TreeIdxMap;
      host2TreeIdx->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());

      gWAccessTree->pFs2Idx = host2TreeIdx;

      gWAccessTree->pTreeInfo
      = treeInfo;

      tag2NodeIdx = new GeoTag2NodeIdxMap;
      tag2NodeIdx->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());
    }

    ~FastStructGW()
    {
      if(gWAccessTree) delete gWAccessTree;
      if(treeInfo) delete treeInfo;
      if(penalties) delete penalties;
      if(tag2NodeIdx) delete tag2NodeIdx;
    }

    bool DeepCopyTo (FastStructGW *target) const
    {
      if(
          gWAccessTree->copyToFastTree(target->gWAccessTree)
      )
      {
        return false;
      }
      // copy the information
      *(target->treeInfo) = *treeInfo;
      if(
          tag2NodeIdx->copyToGeoTag2NodeIdxMap(target->tag2NodeIdx) )
      {
        return false;
      }

      // copy the penalties
      std::copy(penalties->begin(), penalties->end(),
                    target->penalties->begin());

      // update the information in the FastTrees to point to the copy
      target->gWAccessTree->pFs2Idx
      = NULL;
      target->gWAccessTree->pTreeInfo
      = target->treeInfo;

      return true;
    }

    void UpdateTrees()
    {
      gWAccessTree->updateTree();
    }

    void WriteSlowState( SlowTreeNode::TreeNodeStateFloat &slowState, SchedTreeBase::tFastTreeIdx idx) const
    {
      FastPlacementTree::FsData &fastState = gWAccessTree->pNodes[idx].fsData;
      slowState.dlScore = float(fastState.dlScore)/255;
      slowState.ulScore = float(fastState.ulScore)/255;
      slowState.mStatus = fastState.mStatus & ~eos::mgm::SchedTreeBase::Disabled; // we don't want to back proagate the disabled bit
      slowState.fillRatio = float(fastState.fillRatio)/255;
      slowState.totalSpace = float(fastState.totalSpace);
    }

    inline void applyDlScorePenalty(SchedTreeBase::tFastTreeIdx idx, const char &penalty, bool background)
    {
      AtomicSub(gWAccessTree->pNodes[idx].fsData.dlScore,penalty);
      if(!background)
      {
        AtomicAdd((*penalties)[idx].dlScorePenalty,penalty);
      }
    }

    inline void applyUlScorePenalty(SchedTreeBase::tFastTreeIdx idx, const char &penalty, bool background)
    {
      AtomicSub(gWAccessTree->pNodes[idx].fsData.ulScore,penalty);
      if(!background)
      {
        AtomicAdd((*penalties)[idx].ulScorePenalty,penalty);
      }
    }

    inline bool isUlScorePos ( SchedTreeBase::tFastTreeIdx idx)
    {
      return gWAccessTree->pNodes[idx].fsData.ulScore>0;
    }

    inline bool isDlScorePos ( SchedTreeBase::tFastTreeIdx idx)
    {
      return gWAccessTree->pNodes[idx].fsData.dlScore>0;
    }

    inline SchedTreeBase::FastTreeInfo* getTreeInfo() { return treeInfo; }

    inline bool buildFastStructures(SlowTree *slowTree)
    {
      return slowTree->buildFastStrcturesGW(
                gWAccessTree,host2TreeIdx,
                treeInfo , tag2NodeIdx
            );
    }

    inline void resizePenalties ( const size_t &newsize)
    {
      penalties->resize(newsize);
    }

    inline void setConfigParam(
        const char &fillRatioLimit,
        const char &fillRatioCompTol,
        const char &saturationThres)
    {
      gWAccessTree->setSaturationThreshold(saturationThres);
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage tree based operation of given type
   *        it is just a base to derived structs
   */
  /*----------------------------------------------------------------------------*/
  template<typename FastStruct> struct TreeMapEntry
  {

    // ==== SlowTree : this is used to add or remove nodes ==== //
    // every access to mSlowTree or mFs2SlowTreeNode should be protected by a lock to mSlowTreeMutex
    SlowTree *slowTree;
    //std::map<eos::common::FileSystem::fsid_t,SlowTreeNode*> fs2SlowTreeNode;
    eos::common::RWMutex slowTreeMutex;
    bool slowTreeModified;

    // ===== Fast Structures Management and Double Buffering ====== //
    FastStruct fastStructures[2];
    // the pointed object is read only accessed by several thread
    FastStruct *foregroundFastStruct;
    // the pointed object is accessed in read /write only by the thread update
    FastStruct *backgroundFastStruct;
    // the two previous pointers are swapped once an update is done. To do so, we need a mutex and a counter (for deletion)
    // every access to *mForegroundFastStruct for reading should be protected by a LockRead to mDoubleBufferMutex
    // when swapping mForegroundFastStruct and mBackgroundFastStruct is needed a LockWrite is taken to mDoubleBufferMutex
    eos::common::RWMutex doubleBufferMutex;
    size_t fastStructLockWaitersCount;
    bool fastStructModified;

    TreeMapEntry(const std::string &groupName="") :
    slowTreeModified(false),
    foregroundFastStruct(fastStructures),
    backgroundFastStruct(fastStructures+1),
    fastStructLockWaitersCount(0),
    fastStructModified(false)
    {
      slowTree = new SlowTree(groupName);
      slowTreeMutex.SetBlocking(true);
      doubleBufferMutex.SetBlocking(true);
    }

    ~TreeMapEntry()
    {
      if(slowTree) delete slowTree;
    }

    void swapFastStructBuffers()
    {
      eos::common::RWMutexWriteLock lock(doubleBufferMutex);
      std::swap(foregroundFastStruct,backgroundFastStruct);
    }

    void updateBGFastStructuresConfigParam(
	const char &fillRatioLimit,
	const char &fillRatioCompTol,
	const char &saturationThres)
    {

      backgroundFastStruct->setConfigParam(fillRatioLimit,fillRatioCompTol,saturationThres);

      refreshBackGroundFastStructures();
    }

    void refreshBackGroundFastStructures()
    {
      backgroundFastStruct->UpdateTrees();
    }

    bool updateFastStructures()
    {
      FastStruct *ft = backgroundFastStruct;

      if(!ft->buildFastStructures(slowTree))
      {
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
  struct SchedTME : public TreeMapEntry<FastStructSched>
  {
    FsGroup *group;

    std::map<eos::common::FileSystem::fsid_t,SlowTreeNode*> fs2SlowTreeNode;

    SchedTME( const std::string &groupName) :
      TreeMapEntry<FastStructSched>(groupName),
      group(NULL)
      {}

    void updateSlowTreeInfoFromBgFastStruct()
    {
      for(auto it = fs2SlowTreeNode.begin(); it!= fs2SlowTreeNode.end(); ++it)
      {
        const SchedTreeBase::tFastTreeIdx *idx;
        if(!backgroundFastStruct->fs2TreeIdx->get(it->first,idx))
        {
          // this node was added in the SlowTree, the fast structures doesn't include it yet
          continue;
        }
        FastPlacementTree::FsData &fastState = backgroundFastStruct->placementTree->pNodes[*idx].fsData;
        SlowTreeNode::TreeNodeStateFloat &slowState = it->second->pNodeState;
        slowState.dlScore = float(fastState.dlScore)/255;
        slowState.ulScore = float(fastState.ulScore)/255;
        slowState.mStatus = fastState.mStatus & ~eos::mgm::SchedTreeBase::Disabled; // we don't want to back proagate the disabled bit
        slowState.fillRatio = float(fastState.fillRatio)/255;
        slowState.totalSpace = float(fastState.totalSpace);
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
  struct GwTMEBase : public TreeMapEntry<FastStructGW>
  {
    FsGroup *group;

    std::map<std::string,SlowTreeNode*> host2SlowTreeNode;

    GwTMEBase( const std::string &groupName) :
      TreeMapEntry<FastStructGW>(groupName),
      group(NULL)
      {}

    void updateSlowTreeInfoFromBgFastStruct()
    {
      for(auto it = host2SlowTreeNode.begin(); it!= host2SlowTreeNode.end(); ++it)
      {
        const SchedTreeBase::tFastTreeIdx *idx;
        if(!backgroundFastStruct->host2TreeIdx->get(it->first.c_str(),idx))
        {
          // this node was added in the SlowTree, the fast structures doesn't include it yet
          continue;
        }
        FastPlacementTree::FsData &fastState = backgroundFastStruct->gWAccessTree->pNodes[*idx].fsData;
        SlowTreeNode::TreeNodeStateFloat &slowState = it->second->pNodeState;
        slowState.dlScore = float(fastState.dlScore)/255;
        slowState.ulScore = float(fastState.ulScore)/255;
        slowState.mStatus = fastState.mStatus & ~eos::mgm::SchedTreeBase::Disabled; // we don't want to back proagate the disabled bit
      }
    }

  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage a scheduling of XRootd gateways
   *
   */
  /*----------------------------------------------------------------------------*/
  struct GatewayTME : public GwTMEBase
  {
    GatewayTME( const std::string &groupName) : GwTMEBase( groupName)
    {}
  };

  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage a scheduling of Data proxy
   *
   */
  /*----------------------------------------------------------------------------*/
  struct DataProxyTME : public GwTMEBase
  {
    DataProxyTME( const std::string &groupName) : GwTMEBase( groupName)
    {}
  };

  bool updateFastStructures( SchedTME *entry )
  {
    // if nothing is modified here move to the next group
    if(!(entry->slowTreeModified || entry->fastStructModified))
    return true;

    if(entry->slowTreeModified)
    {
      entry->updateSlowTreeInfoFromBgFastStruct();
      if(!entry->updateFastStructures())
      {
	eos_crit("error updating the fast structures from the slowtree");
	return false;
      }
      applyBranchDisablings(*entry);
      if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
      {
	stringstream ss;
	ss << (*entry->backgroundFastStruct->placementTree);
	eos_debug("fast structures updated successfully from slowtree : new FASTtree is \n %s",ss.str().c_str());
	ss.str()="";
	ss << (*entry->slowTree);
	eos_debug("fast structures updated successfully from slowtree : old SLOW tree was \n %s",ss.str().c_str());
      }

    }
    else
    {
      // the rebuild of the fast structures is not necessary
      entry->refreshBackGroundFastStructures();
      if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
      {
	stringstream ss;
	ss << (*entry->backgroundFastStruct->placementTree);
	eos_debug("fast structures updated successfully from fastree : new FASTtree is \n %s",ss.str().c_str());
      }
    }

    // mark the entry as updated
    entry->slowTreeModified = false;
    entry->fastStructModified = false;

    // update the BackGroundFastStructures configuration parameters accordingly to the one present in the GeoTree (and update the fast trees)
    entry->updateBGFastStructuresConfigParam(pFillRatioLimit,pFillRatioCompTol,pSaturationThres);

    // clear the penalties
    std::fill(entry->backgroundFastStruct->penalties->begin(), entry->backgroundFastStruct->penalties->end(), Penalties());

    // swap the buffers (this is the only bit where the fast structures is not accessible for a placement/access operation)
    entry->swapFastStructBuffers();

    return true;
  }

  bool updateFastStructures( GwTMEBase *entry )
  {
    // if nothing is modified here move to the next group
    if(!(entry->slowTreeModified || entry->fastStructModified))
    return true;

    if(entry->slowTreeModified)
    {
      entry->updateSlowTreeInfoFromBgFastStruct();
      if(!entry->updateFastStructures())
      {
        eos_crit("error updating the fast structures from the slowtree");
        return false;
      }
      applyBranchDisablings(*entry);
      if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
      {
        stringstream ss;
        ss << (*entry->backgroundFastStruct->gWAccessTree);
        eos_debug("fast structures updated successfully from slowtree : new FASTtree is \n %s",ss.str().c_str());
        ss.str()="";
        ss << (*entry->slowTree);
        eos_debug("fast structures updated successfully from slowtree : old SLOW tree was \n %s",ss.str().c_str());
      }

    }
    else
    {
      // the rebuild of the fast structures is not necessary
      entry->refreshBackGroundFastStructures();
      if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
      {
        stringstream ss;
        ss << (*entry->backgroundFastStruct->gWAccessTree);
        eos_debug("fast structures updated successfully from fastree : new FASTtree is \n %s",ss.str().c_str());
      }
    }

    // mark the entry as updated
    entry->slowTreeModified = false;
    entry->fastStructModified = false;

    // update the BackGroundFastStructures configuration parameters accordingly to the one present in the GeoTree (and update the fast trees)
    entry->updateBGFastStructuresConfigParam(pFillRatioLimit,pFillRatioCompTol,pSaturationThres);

    // clear the penalties
    std::fill(entry->backgroundFastStruct->penalties->begin(), entry->backgroundFastStruct->penalties->end(), Penalties());

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
  { regularRO,regularRW,balancing,draining};

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
  sfgGeotag,sfgId,sfgBoot,sfgDrain,sfgDrainer,sfgBlcingrun,sfgBlcerrun,
  sfgBalthres,sfgActive,sfgBlkavailb,sfgDiskload,
  sfgEthmib,sfgInratemib,sfgOutratemib,sfgWriteratemb,
  sfgReadratemb,sfgFsfilled,sfgNomfilled,sfgConfigstatus,sfgHost,sfgErrc,sfgPubTmStmp;

  //! This mutex protects the consistency between the GeoTreeEngine state and the filesystems it contains
  //! To make any change that temporarily set an unconsistent state (mainly adding a fs, removing a fs,
  //! listening to the changes in the set if contained fs), one needs to writelock this mutex.
  //! When the mutex is realesed, the GeoTreeEngine internal ressources should be in a consitent state.
  eos::common::RWMutex pAddRmFsMutex;

  //! this is the set of all the watched keys to be notified about for FileSystems
  static set<std::string> gWatchedKeys;

  //! this is the set of all the watched keys to be notified about for Gateways/DataProxy
  static set<std::string> gWatchedKeysGw;

  //! this map allow to convert a notification key to an enum for efficient processing
  static const std::map<string,int> gNotifKey2Enum;

  //! this map allow to convert a notification key to an enum for efficient processing
  static const std::map<string,int> gNotifKey2EnumGw;

  //! this is the list of the watched queues to be notified about
  std::set<std::string> pWatchedQueues;
  //--------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------


  //--------------------------------------------------------------------------------------------------------
  // Configuration
  //--------------------------------------------------------------------------------------------------------
  /// this mutex protects all the configuration settings
  eos::common::RWMutex configMutex;// protects all the following settings

  /// these settings indicate if saturated FS should try to be avoided
  /// this might lead to unoptimal access/placement location-wise
  bool pSkipSaturatedPlct,pSkipSaturatedAccess,
  pSkipSaturatedDrnAccess,pSkipSaturatedBlcAccess,
  pSkipSaturatedDrnPlct,pSkipSaturatedBlcPlct;

  /// this set the speed on how fast the penalties are allowed to
  /// change as they are estimated
  /// 0 means no self-estimate 1 mean gets a completely new value every time
  float pPenaltyUpdateRate;

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
  pTimeFrameDurationMs,
  /// this is how older than a refresh a penalty must be do be dropped
  pPublishToPenaltyDelayMs;

  /// the following settings control the Disabled branches in the trees
  // group -> (optype -> geotag)
  std::map<std::string, std::map<std::string,std::set<std::string> > > pDisabledBranches;

  //--------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------

  //--------------------------------------------------------------------------------------------------------
  // State
  //--------------------------------------------------------------------------------------------------------
  //
  // => fs scheduling groups management / operations
  //    they are used to schedule fs accesses
  //
  std::map<const FsGroup*,SchedTME*> pGroup2SchedTME;
  std::map<FileSystem::fsid_t,SchedTME*> pFs2SchedTME;
  std::map<FileSystem::fsid_t,FileSystem*> pFsId2FsPtr;
  /// protects all the above maps
  eos::common::RWMutex pTreeMapMutex;

  // => proxy scheduling groups management / operations
  //    they are used to schedule data proxy to translate dedicated proxygroup to xrootd to serve the client (if any defined)
  //    they are also used to manage the entry points to the instance (if any defined)
  //
  std::map<std::string ,DataProxyTME*>  pPxyGrp2DpTME;           // one proxygroup => one TreeMapEntry
  std::map<std::string ,std::set<DataProxyTME*>> pPxyHost2DpTMEs; // one proxyhost  => several proxygroups
  std::map<std::string,SchedTreeBase::tFastTreeIdx> pPxyQueue2PxyId;
  std::set<SchedTreeBase::tFastTreeIdx> pPxyId2Recycle;
  /// protects all the above maps
  eos::common::RWMutex pPxyTreeMapMutex;

  //
  // => thread local data
  //
  /// Thread local buffer to hold a working copy of a fast structure
  static __thread void* tlGeoBuffer;
  static pthread_key_t gPthreadKey;
  /// Current scheduling group for the current thread
  static __thread const FsGroup* tlCurrentGroup;
  //
  // => penalties system
  //
  const size_t pCircSize;
  size_t pFrameCount;
  struct PenaltySubSys
  {
    std::vector<tPenaltiesVec> pCircFrCnt2FsPenalties;
    std::vector<tPenaltiesMap> pCircFrCnt2HostPenalties;
    /// self estimated penalties
    std::map<std::string, nodeAgreg> pUpdatingNodes;
    size_t pMaxNetSpeedClass;
    /// Atomic penalties to be applied to the scheduled FSs
    /// those are in the state section because they can be self estimated
    /// the following vectors map an netzorkSpeedClass to a penalty
    std::vector<float> pPlctDlScorePenaltyF,pPlctUlScorePenaltyF;
    std::vector<float> pAccessDlScorePenaltyF,pAccessUlScorePenaltyF;
    std::vector<float> pGwScorePenaltyF;
    // casted version to avoid conversion on every plct / access operation
    std::vector<char> pPlctDlScorePenalty,pPlctUlScorePenalty;
    std::vector<char> pAccessDlScorePenalty,pAccessUlScorePenalty;
    std::vector<char> pGwScorePenalty;
    // Constructor
    PenaltySubSys(const size_t &circSize) :
      pCircFrCnt2FsPenalties(circSize),
      pCircFrCnt2HostPenalties(circSize),
        pMaxNetSpeedClass(0),
        pPlctDlScorePenaltyF(8,10),pPlctUlScorePenaltyF(8,10),     // 8 is just a simple way to deal with the initialiaztion of the vector (it's an overshoot but the overhead is tiny)
        pAccessDlScorePenaltyF(8,10),pAccessUlScorePenaltyF(8,10),pGwScorePenaltyF(8,10) ,
        pPlctDlScorePenalty(8,10),pPlctUlScorePenalty(8,10),     // 8 is just a simple way to deal with the initialiaztion of the vector (it's an overshoot but the overhead is tiny)
        pAccessDlScorePenalty(8,10),pAccessUlScorePenalty(8,10),pGwScorePenalty(8,10) {};
  };
  PenaltySubSys pPenaltySched;
  //
  // => latency estimation
  //
  struct LatencySubSys
  {
    tLatencyStats pGlobalLatencyStats,globalAgeStats;
    std::vector<tLatencyStats> pFsId2LatencyStats;
    std::map<std::string,tLatencyStats> pHost2LatencyStats;
    std::vector<size_t> pCircFrCnt2Timestamp;
    // Constructor
    LatencySubSys(const size_t &circSize) :
      pCircFrCnt2Timestamp(circSize) {}
  };
  LatencySubSys pLatencySched;
  //
  // => background updating
  //
  /// thread ID of the dumper thread
  pthread_t pUpdaterTid;
  /// maps a notification subject to changes that happened in the current time frame
  static std::map<std::string,int> gNotificationsBufferFs;
  static std::map<std::string,int> gNotificationsBufferDp;
  static const unsigned char sntFilesystem,sntGateway,sntDataproxy;
  static std::map<std::string,unsigned char> gQueue2NotifType;
  /// deletions to be carried out ASAP
  /// they are delayed so that any function that is using the treemapentry can safely finish
  std::list<SchedTME*> pPendingDeletions;
  /// indicate if the updater is paused
  static XrdSysSemaphore gUpdaterPauseSem;
  static bool gUpdaterPaused;
  static bool gUpdaterStarted;
//**********************************************************
// END DATA MEMBERS
//**********************************************************


  void updateAtomicPenalties();

  /// Trees update management
  void listenFsChange();
  static void* startFsChangeListener( void *pp);


  /// Clean
  void checkPendingDeletions()
  {
    int count = 0;
    auto lastEntry = pPendingDeletions.begin();
    bool eraseLastEntry = false;
    for(auto it=pPendingDeletions.begin(); it!=pPendingDeletions.end(); it++)
    {
      if(eraseLastEntry) pPendingDeletions.erase(lastEntry);
      eraseLastEntry = false;
      if(!(*it)->fastStructLockWaitersCount)
      {
	delete (*it);
	eraseLastEntry = true;
	count++;
      }
      lastEntry = it;
    }
    if(eraseLastEntry) pPendingDeletions.erase(lastEntry);

    eos_debug("%d pending deletions executed",count);
  }

  /// thread-local buffer management
  static void tlFree( void *arg);
  static char* tlAlloc( size_t size);

  inline void applyDlScorePenalty(SchedTME *entry, const SchedTreeBase::tFastTreeIdx &idx, const char &penalty, bool background=false)
  {
    FastStructSched *ft = background?entry->backgroundFastStruct:entry->foregroundFastStruct;
    ft->applyDlScorePenalty(idx,penalty,background);
  }

  inline void applyDlScorePenalty(GwTMEBase *entry, const SchedTreeBase::tFastTreeIdx &idx, const char &penalty, bool background=false)
  {
    FastStructGW *ft = background?entry->backgroundFastStruct:entry->foregroundFastStruct;
    ft->applyDlScorePenalty(idx,penalty,background);
  }

  inline void applyUlScorePenalty(SchedTME *entry, const SchedTreeBase::tFastTreeIdx &idx, const char &penalty, bool background=false)
  {
    FastStructSched *ft = background?entry->backgroundFastStruct:entry->foregroundFastStruct;
    ft->applyUlScorePenalty(idx,penalty,background);
  }

  inline void applyUlScorePenalty(GwTMEBase *entry, const SchedTreeBase::tFastTreeIdx &idx, const char &penalty, bool background=false)
  {
    FastStructGW *ft = background?entry->backgroundFastStruct:entry->foregroundFastStruct;
    ft->applyUlScorePenalty(idx,penalty,background);
  }

  inline void recallScorePenalty(SchedTME *entry, const SchedTreeBase::tFastTreeIdx &idx)
  {
    auto fsid = (*entry->backgroundFastStruct->treeInfo)[idx].fsId;
    tLatencyStats &lstat = pLatencySched.pFsId2LatencyStats[fsid];
    //auto mydata = entry->backgroundFastStruct->placementTree->pNodes[idx].fsData;
    int count = 0;
    for( size_t circIdx = pFrameCount%pCircSize;
        (lstat.lastupdate!=0) && (pLatencySched.pCircFrCnt2Timestamp[circIdx] > lstat.lastupdate - pPublishToPenaltyDelayMs);
        circIdx=((pCircSize+circIdx-1)%pCircSize) )
    {
      if(entry->foregroundFastStruct->placementTree->pNodes[idx].fsData.dlScore>0)
      applyDlScorePenalty(entry,idx,
                          pPenaltySched.pCircFrCnt2FsPenalties[circIdx][fsid].dlScorePenalty,
                          true
                          );
      if(entry->foregroundFastStruct->placementTree->pNodes[idx].fsData.ulScore>0)
      applyUlScorePenalty(entry,idx,
                          pPenaltySched.pCircFrCnt2FsPenalties[circIdx][fsid].ulScorePenalty,
                          true
                          );
      if(++count == (int)pCircSize)
      {
        eos_warning("Last fs update for fs %d is older than older penalty : it could happen as a transition but should not happen permanently.",(int)fsid);
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

  inline void recallScorePenalty(GwTMEBase *entry, const SchedTreeBase::tFastTreeIdx &idx)
  {
    auto host = (*entry->backgroundFastStruct->treeInfo)[idx].host;
    tLatencyStats &lstat = pLatencySched.pHost2LatencyStats[host];
    int count = 0;
    for( size_t circIdx = pFrameCount%pCircSize;
        (lstat.lastupdate!=0) && (pLatencySched.pCircFrCnt2Timestamp[circIdx] > lstat.lastupdate - pPublishToPenaltyDelayMs);
        circIdx=((pCircSize+circIdx-1)%pCircSize) )
    {
      if(entry->foregroundFastStruct->gWAccessTree->pNodes[idx].fsData.dlScore>0)
      applyDlScorePenalty(entry,idx,
                          pPenaltySched.pCircFrCnt2HostPenalties[circIdx][host].dlScorePenalty,
                          true
                          );
      if(entry->foregroundFastStruct->gWAccessTree->pNodes[idx].fsData.ulScore>0)
      applyUlScorePenalty(entry,idx,
                          pPenaltySched.pCircFrCnt2HostPenalties[circIdx][host].ulScorePenalty,
                          true
                          );
      if(++count == (int)pCircSize)
      {
        eos_warning("Last host update for host %s is older than older penalty : it could happen as a transition but should not happen permanently.",host.c_str());
        break;
      }
    }
  }

  template<class T> bool placeNewReplicas(SchedTME* entry, const size_t &nNewReplicas,

      std::vector<SchedTreeBase::tFastTreeIdx> *newReplicas,
      T *placementTree,
      std::vector<SchedTreeBase::tFastTreeIdx> *existingReplicas=NULL,
      unsigned long long bookingSize=0,
      const SchedTreeBase::tFastTreeIdx &startFromNode=0,
      const size_t &nFinalCollocatedReplicas=0,
      std::vector<SchedTreeBase::tFastTreeIdx> *excludedNodes=NULL,
      std::vector<SchedTreeBase::tFastTreeIdx> *forceNodes=NULL,
      bool skipSaturated=false)
  {
    // a read lock is supposed to be acquired on the fast structures

    bool updateNeeded = false;

    if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
    {
      stringstream ss;
      ss << (*placementTree);
      eos_debug("fast tree used to copy from is: \n %s",ss.str().c_str());
    }

    // make a working copy of the required fast tree
    // allocate the buffer only once for the lifetime of the thread
    if(!tlGeoBuffer) tlGeoBuffer = tlAlloc(gGeoBufferSize);

    if(placementTree->copyToBuffer((char*)tlGeoBuffer,gGeoBufferSize))
    {
      eos_crit("could not make a working copy of the fast tree");
      return false;
    }
    T *tree = (T*)tlGeoBuffer;

    if(forceNodes)
    {
      ///// =====  NOT IMPLEMENTED
      assert(false);
      // make all the nodes
      for(SchedTreeBase::tFastTreeIdx k=0; k<tree->getMaxNodeCount(); k++)
      tree->pNodes[k].fsData.mStatus &= ~SchedTreeBase::Available;
    }

    // place the existing replicas
    size_t nAdjustCollocatedReplicas = nFinalCollocatedReplicas;
    if(existingReplicas)
    {
      size_t ncomp = (*tree->pTreeInfo)[startFromNode].fullGeotag.find("::");
      if(ncomp == std::string::npos) ncomp = (*tree->pTreeInfo)[startFromNode].fullGeotag.size();
      for(auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it)
      {
	tree->pNodes[*it].fileData.freeSlotsCount = 0;
	tree->pNodes[*it].fileData.takenSlotsCount = 1;
	// check if this replica is to be considered as a collocated one
	if(startFromNode)
	{ // we have an accesser geotag
	  if((*tree->pTreeInfo)[startFromNode].fullGeotag.compare(0,ncomp,(*tree->pTreeInfo)[*it].fullGeotag)==0
	      && ((*tree->pTreeInfo)[*it].fullGeotag.size()==ncomp || (*tree->pTreeInfo)[*it].fullGeotag[ncomp]==':') )
	  {
	    // this existing replica is under the same first level of the tree
	    // we consider it as a collocated replica
	    if(nAdjustCollocatedReplicas) nAdjustCollocatedReplicas--;
	  }
	}
      }
      if(nAdjustCollocatedReplicas>nNewReplicas) nAdjustCollocatedReplicas = nNewReplicas;
      // if(!startFromNode), the value of nCollocatedReplicas does not make any difference and furthermore, it should be zero

      // update the tree
      // (could be made faster for a small number of existing replicas by using update branches)
      if(!existingReplicas->empty())
      updateNeeded = true;
    }

    if(excludedNodes)
    {
      // mark the excluded branches as unavailable and sort the branches (no deep, or we would lose the unavailable marks)
      for(auto it = excludedNodes->begin(); it != excludedNodes->end(); ++it)
      {
	tree->pNodes[*it].fsData.mStatus = tree->pNodes[*it].fsData.mStatus & ~SchedTreeBase::Available;
      }
      if(!excludedNodes->empty())
      updateNeeded = true;
    }

    if(bookingSize)
    {
      for(auto it = tree->pFs2Idx->begin(); it != tree->pFs2Idx->end(); it++ )
      {
	// we prebook the space on all the possible nodes before the selection
	// reminder : this is just a working copy of the tree and will affect only the current placement
	const SchedTreeBase::tFastTreeIdx &idx = (*it).second;
	float &freeSpace = tree->pNodes[idx].fsData.totalSpace;
	if(freeSpace>bookingSize)// if there is enough space , prebook it
	freeSpace -= bookingSize;
	else// if there is not enough space, make the node unavailable
	tree->pNodes[idx].fsData.mStatus = tree->pNodes[idx].fsData.mStatus & ~SchedTreeBase::Available;
      }
      updateNeeded = true;
    }

    // do the placement
    if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
    {
      stringstream ss;
      ss << (*tree);
      eos_debug("fast tree used for placement is: \n %s",ss.str().c_str());
    }

    if(updateNeeded)
    {
      tree->updateTree();
    }

    for(size_t k = 0; k < nNewReplicas; k++)
    {
      SchedTreeBase::tFastTreeIdx idx;
      SchedTreeBase::tFastTreeIdx startidx = (k<nNewReplicas-nAdjustCollocatedReplicas)?0:startFromNode;
      if(!tree->findFreeSlot(idx, startidx, true /*allow uproot if necessary*/, true, skipSaturated))
      {
	if(skipSaturated) eos_debug("Could not find any replica for placement while skipping saturated fs. Trying with saturated nodes included");
	if( (!skipSaturated) || !tree->findFreeSlot(idx, startidx, true /*allow uproot if necessary*/, true, false) )
	{
	  eos_debug("could not find a new slot for a replica in the fast tree");
	  stringstream ss;
	  ss << (*tree);
	  eos_debug("iteration number %lu fast tree used for placement is: \n %s",k,ss.str().c_str());
	  return false;
	}
      }
      newReplicas->push_back(idx);
    }

    return true;
  }

  template<class T> unsigned char accessReplicas(SchedTME* entry, const size_t &nNewReplicas,
      std::vector<SchedTreeBase::tFastTreeIdx> *accessedReplicas,
      SchedTreeBase::tFastTreeIdx accesserNode,
      std::vector<SchedTreeBase::tFastTreeIdx> *existingReplicas,
      T *accessTree,
      std::vector<SchedTreeBase::tFastTreeIdx> *excludedNodes=NULL,
      std::vector<SchedTreeBase::tFastTreeIdx> *forceNodes=NULL,
      bool skipSaturated=false)
  {

    if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
    {
      stringstream ss;
      ss << (*accessTree);
      eos_debug("fast tree used to copy from is: \n %s",ss.str().c_str());
    }

    // make a working copy of the required fast tree
    // allocate the buffer only once for the lifetime of the thread
    if(!tlGeoBuffer) tlGeoBuffer = tlAlloc(gGeoBufferSize);

    if(accessTree->copyToBuffer((char*)tlGeoBuffer,gGeoBufferSize))
    {
      eos_crit("could not make a working copy of the fast tree");
      return 0;
    }
    T *tree = (T*)tlGeoBuffer;
    eos_static_debug("saturationTresh original=%d / copy=%d",(int)accessTree->pBranchComp.saturationThresh,(int)tree->pBranchComp.saturationThresh);

    if(forceNodes)
    {
      ///// =====  NOT IMPLEMENTED
      assert(false);
      // make all the nodes
      for(SchedTreeBase::tFastTreeIdx k=0; k<tree->getMaxNodeCount(); k++)
      tree->pNodes[k].fsData.mStatus &= ~SchedTreeBase::Available;
    }

    // place the existing replicas
    if(existingReplicas)
    {
      for(auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it)
      {
	tree->pNodes[*it].fileData.freeSlotsCount = 1;
	tree->pNodes[*it].fileData.takenSlotsCount = 0;
      }
      // update the tree
      // (could be made faster for a small number of existing replicas by using update branches)
      tree->updateTree();
    }

    if(excludedNodes)
    {
      // mark the excluded branches as unavailable and sort the branches (no deep, or we would lose the unavailable marks)
      for(auto it = excludedNodes->begin(); it != excludedNodes->end(); ++it)
      {
	tree->pNodes[*it].fsData.mStatus = tree->pNodes[*it].fsData.mStatus & ~SchedTreeBase::Available;
	tree->updateBranch(*it);
      }
    }

    // do the access
    if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
    {
      stringstream ss;
      ss << (*tree);
      eos_debug("fast tree used for access is: \n %s",ss.str().c_str());
    }

    // do the access
    unsigned char retCode = 0;
    for(size_t k = 0; k < nNewReplicas; k++)
    {
      SchedTreeBase::tFastTreeIdx idx;
      if(!tree->findFreeSlot(idx,accesserNode,true,true,skipSaturated))
      {
	if(skipSaturated) eos_debug("Could not find any replica to access while skipping saturated fs. Trying with saturated nodes included");
	if( (!skipSaturated) || !tree->findFreeSlot(idx, 0, false, true, false) )
	{
	  eos_debug("could not find a new slot for a replica in the fast tree");
	  return 0;
	}
	else retCode = 1;
      }
      else
      retCode = 2;
      accessedReplicas->push_back(idx);
    }

    return retCode;
  }

  bool updateTreeInfo(SchedTME* entry, eos::common::FileSystem::fs_snapshot_t *fs, int keys, SchedTreeBase::tFastTreeIdx ftidx=0 , SlowTreeNode *stn=NULL);
  bool updateTreeInfo(GwTMEBase* entry, eos::common::FileSystem::host_snapshot_t *fs, int keys, SchedTreeBase::tFastTreeIdx ftidx=0 , SlowTreeNode *stn=NULL);
  bool updateTreeInfo(const map<string,int> &updatesFs, const map<string,int> &updatesDp);
  //bool updateTreeInfoFs(const map<string,int> &updatesFs);

  template<typename T> bool _setInternalParam(T& param, const T& value, bool updateStructs)
    {
      eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
      eos::common::RWMutexWriteLock lock2(pTreeMapMutex);
      eos::common::RWMutexWriteLock lock3(configMutex);

      bool result = true;

      param = value;

      for(auto it = pFs2SchedTME.begin(); it != pFs2SchedTME.end(); it++)
      {
        if(updateStructs)
        {
          it->second->fastStructModified = true;
          it->second->slowTreeModified = true;
          result = result && updateFastStructures(it->second);
        }
      }

      return result;
    }

  static void setConfigValue (const char* prefix,
                                            const char* key,
                                            const char* val,
                                            bool tochangelog=true);

  template<typename T> bool setInternalParam(T& param, const int& value, bool updateStructs, const std::string &configentry)
  {
    bool ret = _setInternalParam(param,static_cast<T>(value),updateStructs);
    if(ret && configentry.length())
    {
    XrdOucString s;
    s.append((int)value);
    setConfigValue("geosched",configentry.c_str() , s.c_str());
    }
    return ret;
  }

  template<typename T> bool setInternalParam(T& param, const float& value, bool updateStructs, const std::string &configentry)
  {
    bool ret = _setInternalParam(param,static_cast<T>(value),updateStructs);
    if(ret && configentry.length())
    {
    XrdOucString s;
    char buf[32];
    sprintf(buf,"%f",value);
    s+=buf;
    setConfigValue("geosched",configentry.c_str() , s.c_str());
    }
    return ret;
  }

  bool setInternalParam(std::vector<char>& param, const std::vector<char >& value, bool updateStructs, const std::string &configentry)
  {
    bool ret = _setInternalParam(param,value,updateStructs);
    if(ret && configentry.length())
    {
      XrdOucString s;
      s+="[";
      for(size_t i=0;i<param.size();i++)
      {
        s+=(int)value[i];
        s+=",";
      }
      s[s.length()-1]=']';
      setConfigValue("geosched",configentry.c_str() , s.c_str());
    }
    return ret;
  }

  bool setInternalParam(std::vector<float>& param, const std::vector<float>& value, bool updateStructs, const std::string &configentry)
  {
    bool ret = _setInternalParam(param,value,updateStructs);
    if(ret && configentry.length())
    {
      XrdOucString s;
      s+="[";
      for(size_t i=0;i<param.size();i++)
      {
        char buf[32];
        sprintf(buf,"%f",value[i]);
        s+=buf;
        s+=",";
      }
      s[s.length()-1]=']';
      setConfigValue("geosched",configentry.c_str() , s.c_str());
    }
    return ret;
  }

  bool markPendingBranchDisablings(const std::string &group, const std::string&optype, const std::string&geotag);
  bool applyBranchDisablings(const SchedTME& entry);
  bool applyBranchDisablings(const GwTMEBase& entry);
  bool setSkipSaturatedPlct(bool value);
  bool setSkipSaturatedAccess(bool value);
  bool setSkipSaturatedDrnAccess(bool value);
  bool setSkipSaturatedBlcAccess(bool value);
  bool setSkipSaturatedDrnPlct(bool value);
  bool setSkipSaturatedBlcPlct(bool value);
  bool setScorePenalty(std::vector<float> &fvector, std::vector<char> &cvector, const std::vector<char> &value, const std::string &configentry);
  bool setScorePenalty(std::vector<float> &fvector, std::vector<char> &cvector, const char* vvalue, const std::string &configentry);
  bool setScorePenalty(std::vector<float> &fvector, std::vector<char> &cvector, char value, int netSpeedClass, const std::string &configentry);
  bool setPlctDlScorePenalty(char value, int netSpeedClass);
  bool setPlctUlScorePenalty(char value, int netSpeedClass);
  bool setAccessDlScorePenalty(char value, int netSpeedClass);
  bool setAccessUlScorePenalty(char value, int netSpeedClass);
  bool setGwScorePenalty(char value, int netSpeedClass);
  bool setPlctDlScorePenalty(const char *value);
  bool setPlctUlScorePenalty(const char *value);
  bool setAccessDlScorePenalty(const char *value);
  bool setAccessUlScorePenalty(const char *value);
  bool setGwScorePenalty(const char *value);
  bool setFillRatioLimit(char value);
  bool setFillRatioCompTol(char value);
  bool setSaturationThres(char value);
  bool setTimeFrameDurationMs(int value);
  bool setPenaltyUpdateRate(float value);
public:
  GeoTreeEngine () :
  pSkipSaturatedPlct(false),pSkipSaturatedAccess(true),
  pSkipSaturatedDrnAccess(true),pSkipSaturatedBlcAccess(true),
  pSkipSaturatedDrnPlct(false),pSkipSaturatedBlcPlct(false),
  pPenaltyUpdateRate(1),
  pFillRatioLimit(80),pFillRatioCompTol(100),pSaturationThres(10),
  pTimeFrameDurationMs(1000),pPublishToPenaltyDelayMs(1000),
  pCircSize(30),pFrameCount(0),
  pPenaltySched(pCircSize),
  pLatencySched(pCircSize),
  pUpdaterTid(0)
  {
    // by default, disable all the placement operations for non geotagged fs
    addDisabledBranch("*","plct","nogeotag",NULL,false);
    addDisabledBranch("*","accsblc","nogeotag",NULL,false);
    addDisabledBranch("*","accsdrain","nogeotag",NULL,false);
    // set blocking mutexes for lower latencies
    pAddRmFsMutex.SetBlocking(true);
    configMutex.SetBlocking(true);
    pTreeMapMutex.SetBlocking(true);
    for(auto it=pPenaltySched.pCircFrCnt2FsPenalties.begin(); it!=pPenaltySched.pCircFrCnt2FsPenalties.end(); it++)
      it->reserve(100);
    // create the thread local key to handle allocation/destruction of thread local geobuffers
    pthread_key_create(&gPthreadKey, GeoTreeEngine::tlFree);
#ifdef EOS_GEOTREEENGINE_USE_INSTRUMENTED_MUTEX
#ifdef EOS_INSTRUMENTED_RWMUTEX
    eos::common::RWMutex::SetOrderCheckingGlobal(true);
    pAddRmFsMutex.SetDebugName("pAddRmFsMutex");
    pTreeMapMutex.SetDebugName("pTreeMapMutex");
    eos::common::RWMutex::AddOrderRule("GTE base rule",std::vector<eos::common::RWMutex*>(
	    { &pAddRmFsMutex,&pTreeMapMutex}));
#endif
#endif
  }
  // ---------------------------------------------------------------------------
  //! constants to describe the fs type, including in client access capability
  // ---------------------------------------------------------------------------
  static const unsigned char fsTypeEosReplica, fsTypeEosPIO, fsTypeKinetic, fstTypeAll;
  // ---------------------------------------------------------------------------
  //! Force a refresh of the information in the tree
  //! It's needed only a startup time as some change notification might be missed
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
  // @param updateFastStructures
  //   should the fast structures be updated immediately without waiting for the next time frame
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool insertFsIntoGroup(FileSystem *fs , FsGroup *group, bool updateFastStructures = false);

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
  bool removeFsFromGroup(FileSystem *fs , FsGroup *group, bool updateFastStructures = true);

  // ---------------------------------------------------------------------------
  //! Remove a file system into the GeoTreeEngine
  // @param group
  //   the group the file system belongs to
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool removeGroup(FsGroup *group);

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
  void printInfo(std::string &info,
      bool dispTree, bool dispSnaps, bool dispParam, bool dispState,
      const std::string &schedgroup, const std::string &optype, bool useColors=false);

  // ---------------------------------------------------------------------------
  //! Place several replicas in one scheduling group.
  // @param group
  //   the group to place the replicas in
  // @param nNewReplicas
  //   the number of replicas to be placed
  // @param newReplicas
  //   vector to which fsids of new replicas are appended if the placement
  //   succeeds. They are appended in decreasing priority order
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
  bool placeNewReplicasOneGroup( FsGroup* group, const size_t &nNewReplicas,
      std::vector<eos::common::FileSystem::fsid_t> *newReplicas,
      SchedType type,
      std::vector<eos::common::FileSystem::fsid_t> *existingReplicas,
      std::vector<std::string>*fsidsgeotags=0,
      unsigned long long bookingSize=0,
      const std::string &startFromGeoTag="",
      const size_t &nCollocatedReplicas=0,
      std::vector<eos::common::FileSystem::fsid_t> *excludeFs=NULL,
      std::vector<std::string> *excludeGeoTags=NULL,
      std::vector<std::string> *forceGeoTags=NULL);

  // ---------------------------------------------------------------------------
  //! Access several replicas in one scheduling group.
  // @param group
  //   the group to place the replicas in
  // @param nReplicas
  //   the number of replicas to access
  // @param accessedReplicas
  //   vector to which fsids of replicas to access are appended if the scheduling
  //   succeeds. They are appended in decreasing priority order
  // @param existingReplicas
  //   fsids of preexisting replicas for the current file
  // @param type
  //   type of access to be performed. It can be:
  //     regularRO, regularRW, balancing or draining
  // @param accesserGeoTag
  //   try to get the replicas as close to this geotag as possible
  // @param exludeFs
  //   fsids of files to exclude from the access operation
  // @param excludeGeoTags
  //   geotags of branches to exclude from the access operation
  //     (e.g. exclude a site)
  // @param forceGeoTags
  //   geotags of branches accessed replicas should be taken from
  //     (e.g. force a site)
  // @return
  //   true if the success false else
  // ---------------------------------------------------------------------------
  bool accessReplicasOneGroup(FsGroup* group, const size_t &nReplicas,
      std::vector<eos::common::FileSystem::fsid_t> *accessedReplicas,
      std::vector<eos::common::FileSystem::fsid_t> *existingReplicas,
      SchedType type=regularRO,
      const std::string &accesserGeotag="",
      std::vector<eos::common::FileSystem::fsid_t> *excludeFs=NULL,
      std::vector<std::string> *excludeGeoTags=NULL,
      std::vector<std::string> *forceGeoTags=NULL);

  // this function to access replica spread across multiple scheduling group is a BACKCOMPATIBILITY artifact
  // the new scheduler doesn't try to place files across multiple scheduling groups.
  //	bool accessReplicasMultipleGroup(const size_t &nAccessReplicas,
  //			std::vector<eos::common::FileSystem::fsid_t> *accessedReplicas,
  //			std::vector<eos::common::FileSystem::fsid_t> *existingReplicas,
  //			SchedType type=regularRO,
  //			const std::string &accesserGeotag="",
  //			std::vector<eos::common::FileSystem::fsid_t> *excludeFs=NULL,
  //			std::vector<std::string> *excludeGeoTags=NULL,
  //			std::vector<std::string> *forceGeoTags=NULL);

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
  int accessHeadReplicaMultipleGroup(const size_t &nReplicas,
      unsigned long &fsIndex,
      std::vector<eos::common::FileSystem::fsid_t> *existingReplicas,
      SchedType type=regularRO,
      const std::string &accesserGeotag="",
      const eos::common::FileSystem::fsid_t &forcedFsId=0,
      std::vector<eos::common::FileSystem::fsid_t> *unavailableFs=NULL,
      bool noIO=false
  );

  // ---------------------------------------------------------------------------
  //! Start the background updater thread
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool StartUpdater();

  // ---------------------------------------------------------------------------
  //! Pause the updating of the GeoTreeEngine but keep accumulating
  //! modification notifications
  // ---------------------------------------------------------------------------
  inline static void PauseUpdater()
  { if(gUpdaterStarted && !gUpdaterPaused) { gUpdaterPauseSem.Wait(); gUpdaterPaused = true; }}

  // ---------------------------------------------------------------------------
  //! Resume the updating of the GeoTreeEngine
  //! Process all the notifications accumulated since it was paused
  // ---------------------------------------------------------------------------
  inline static void ResumeUpdater()
  { if(gUpdaterStarted && gUpdaterPaused) { gUpdaterPauseSem.Post(); gUpdaterPaused = false; }}

  // ---------------------------------------------------------------------------
  //! Helper class to use Pausing the updater
  // ---------------------------------------------------------------------------
  class UpdaterPauser
  {
  public:
    UpdaterPauser() { GeoTreeEngine::PauseUpdater(); }
    ~UpdaterPauser() { GeoTreeEngine::ResumeUpdater(); }
  };

  // ---------------------------------------------------------------------------
  //! Stop the background updater thread
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool StopUpdater();

  // ---------------------------------------------------------------------------
  //! Get the groups to which belong some fs
  //! It's faster than accessing the MqHash
  // @param fsids
  //   a vector containing the FsIds
  // @param fsgeotags
  //   return if non NULL, geotags of the fsids are reported in this vector
  // @param sortedgroups
  //   return if non NULL, get the list of groups in decreasing order of number of fs in the list they contain
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool getGroupsFromFsIds(const std::vector<FileSystem::fsid_t> fsids, std::vector<std::string> *fsgeotags, std::vector<FsGroup*> *sortedgroups);

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
  bool setParameter( std::string param, const std::string &value,int iparamidx);

  // ---------------------------------------------------------------------------
  //! Add a branch disabling rule
  // @param group
  //   group name or "*"
  // @param optype
  //   "*" or one of the following plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc
  // @param geotag
  //   geotag of the branch to disable
  // @param output
  //   if non NULL, issue error messages there
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool addDisabledBranch (const std::string& group, const std::string &optype, const std::string&geotag, XrdOucString *output=NULL, bool toConfig=true);

  // ---------------------------------------------------------------------------
  //! Rm a branch disabling rule
  // @param group
  //   group name or "*"
  // @param optype
  //   "*" or one of the following plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc
  // @param geotag
  //   geotag of the branch to disable
  // @param output
  //   if non NULL, issue error messages there
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool rmDisabledBranch (const std::string& group, const std::string &optype, const std::string&geotag, XrdOucString *output=NULL);

  // ---------------------------------------------------------------------------
  //! Rm a branch disabling rule
  // @param group
  //   group name or "*"
  // @param optype
  //   "*" or one of the following plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc
  // @param geotag
  //   geotag of the branch to disable
  // @param output
  //   if non NULL, issue error messages there
  // @param lock
  //   lock the config param mutex
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool showDisabledBranches (const std::string& group, const std::string &optype, const std::string&geotag, XrdOucString *output, bool lock=true);

  // ---------------------------------------------------------------------------
  //! Insert a file system into the GeoTreeEngine
  // @param host
  //   the host to be inserted
  // @param proxygroup
  //   the proxygroup the host is a dataproxy for
  // @param updateFastStructures
  //   should the fast structures be updated immediately without waiting for the next time frame
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool insertHostIntoPxyGr(FsNode *host , const std::string &proxygroup, bool lockAddRm, bool lockFsView, bool updateFastStructures = false);

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
  bool removeHostFromPxyGr(FsNode *host , const std::string &proxygroup, bool lockAddRm, bool lockFsView, bool updateFastStructures = true);

  //! Remove a file system into the GeoTreeEngine
  // @param fs
  //   the file system to be removed
  // @param status
  //   the names of the proxygroups separated by colons
  // @param updateFastStructures
  //   should the fast structures be updated immediately without waiting for the next time frame
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool matchHostPxyGr(FsNode *host , const std::string &status, bool lockFsView, bool updateFastStructures = true);
};

extern GeoTreeEngine gGeoTreeEngine;

EOSMGMNAMESPACE_END

#endif

