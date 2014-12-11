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
    double netOutWeight;
    double netInWeight;
    double diskUtilSum;
    size_t netSpeedClass;
    nodeAgreg() : saturated(false),fsCount(0),rOpen(0),wOpen(0),netOutWeight(0.0),netInWeight(0.0),diskUtilSum(0.0),netSpeedClass(0) {};
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
   *        file scheduling operations
   *
   */
  /*----------------------------------------------------------------------------*/
  struct FastStructures
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

    FastStructures()
    {
      rOAccessTree = new FastROAccessTree;
      rOAccessTree->selfAllocate(255);
      rWAccessTree = new FastRWAccessTree;
      rWAccessTree->selfAllocate(255);
      blcAccessTree = new FastBalancingAccessTree;
      blcAccessTree->selfAllocate(255);
      drnAccessTree = new FastDrainingAccessTree;
      drnAccessTree->selfAllocate(255);
      placementTree = new FastPlacementTree;
      placementTree->selfAllocate(255);
      blcPlacementTree = new FastBalancingPlacementTree;
      blcPlacementTree->selfAllocate(255);
      drnPlacementTree = new FastDrainingPlacementTree;
      drnPlacementTree->selfAllocate(255);

      treeInfo = new SchedTreeBase::FastTreeInfo;
      penalties = new tPenaltiesVec;
      penalties->reserve(255);

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

      fs2TreeIdx = new Fs2TreeIdxMap;
      fs2TreeIdx->selfAllocate(255);

      tag2NodeIdx = new GeoTag2NodeIdxMap;
      tag2NodeIdx->selfAllocate(255);
    }

    ~FastStructures()
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

    bool DeepCopyTo (FastStructures *target)
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
  };


  /*----------------------------------------------------------------------------*/
  /**
   * @brief this structure holds all the structures needed by the GeoTreeEngine
   *        to manage a given scheduling group
   *
   */
  /*----------------------------------------------------------------------------*/
  struct TreeMapEntry
  {
    FsGroup *group;

    // ==== SlowTree : this is used to add or remove nodes ==== //
    // every access to mSlowTree or mFs2SlowTreeNode should be protected by a lock to mSlowTreeMutex
    SlowTree *slowTree;
    std::map<eos::common::FileSystem::fsid_t,SlowTreeNode*> fs2SlowTreeNode;
    eos::common::RWMutex slowTreeMutex;
    bool slowTreeModified;

    // ===== Fast Structures Management and Double Buffering ====== //
    FastStructures fastStructures[2];
    // the pointed object is read only accessed by several thread
    FastStructures *foregroundFastStruct;
    // the pointed object is accessed in read /write only by the thread update
    FastStructures *backgroundFastStruct;
    // the two previous pointers are swapped once an update is done. To do so, we need a mutex and a counter (for deletion)
    // every access to *mForegroundFastStruct for reading should be protected by a LockRead to mDoubleBufferMutex
    // when swapping mForegroundFastStruct and mBackgroundFastStruct is needed a LockWrite is taken to mDoubleBufferMutex
    eos::common::RWMutex doubleBufferMutex;
    size_t fastStructLockWaitersCount;
    bool fastStructModified;

    TreeMapEntry(const std::string &groupName="") :
    group(NULL),
    slowTreeModified(false),
    foregroundFastStruct(fastStructures),
    backgroundFastStruct(fastStructures+1),
    fastStructLockWaitersCount(0),
    fastStructModified(false)
    {
      slowTree = new SlowTree(groupName);
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

      backgroundFastStruct->rOAccessTree->setSaturationThreshold(saturationThres);
      backgroundFastStruct->rWAccessTree->setSaturationThreshold(saturationThres);
      backgroundFastStruct->blcAccessTree->setSaturationThreshold(saturationThres);
      backgroundFastStruct->drnAccessTree->setSaturationThreshold(saturationThres);

      backgroundFastStruct->placementTree->setSaturationThreshold(saturationThres);
      backgroundFastStruct->placementTree->setSpreadingFillRatioCap(fillRatioLimit);
      backgroundFastStruct->placementTree->setFillRatioCompTol(fillRatioCompTol);
      backgroundFastStruct->blcPlacementTree->setSaturationThreshold(saturationThres);
      backgroundFastStruct->blcPlacementTree->setSpreadingFillRatioCap(fillRatioLimit);
      backgroundFastStruct->blcPlacementTree->setFillRatioCompTol(fillRatioCompTol);
      backgroundFastStruct->drnPlacementTree->setSaturationThreshold(saturationThres);
      backgroundFastStruct->drnPlacementTree->setSpreadingFillRatioCap(fillRatioLimit);
      backgroundFastStruct->drnPlacementTree->setFillRatioCompTol(fillRatioCompTol);

      refreshBackGroundFastStructures();
    }

    bool updateFastStructures();

    void refreshBackGroundFastStructures()
    {
      backgroundFastStruct->rOAccessTree->updateTree();
      backgroundFastStruct->rWAccessTree->updateTree();
      backgroundFastStruct->placementTree->updateTree(0,true,true);
      backgroundFastStruct->blcAccessTree->updateTree();
      backgroundFastStruct->blcPlacementTree->updateTree(0,true,true);
      backgroundFastStruct->drnAccessTree->updateTree();
      backgroundFastStruct->drnPlacementTree->updateTree(0,true,true);
    }

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
	slowState.mStatus = fastState.mStatus;
	slowState.fillRatio = float(fastState.fillRatio)/255;
	slowState.totalSpace = float(fastState.totalSpace);
      }
    }

  };

  bool updateFastStructures( TreeMapEntry *entry )
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
      if(eos::common::Logging::gLogMask & LOG_DEBUG)
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
      if(eos::common::Logging::gLogMask & LOG_DEBUG)
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
  sfgReadratemb,sfgFsfilled,sfgNomfilled,sfgConfigstatus,sfgHost,sfgErrc;

  //! This mutex protects the consistency between the GeoTreeEngine state and the filesystems it contains
  //! To make any change that temporarily set an unconsistent state (mainly adding a fs, removing a fs,
  //! listening to the changes in the set if contained fs), one needs to writelock this mutex.
  //! When the mutex is realesed, the GeoTreeEngine internal ressources should be in a consitent state.
  eos::common::RWMutex pAddRmFsMutex;

  //! this is the set of all the watched keys to be notified about
  static set<std::string> gWatchedKeys;

  //! this map allow to convert a notification key to an enum for efficient processing
  static const std::map<string,int> gNotifKey2Enum;

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
  //--------------------------------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------

  //--------------------------------------------------------------------------------------------------------
  // State
  //--------------------------------------------------------------------------------------------------------
  //
  // => scheduling groups management / operations
  //
  std::map<const FsGroup*,TreeMapEntry*> pGroup2TreeMapEntry;
  std::map<FileSystem::fsid_t,TreeMapEntry*> pFs2TreeMapEntry;
  std::map<FileSystem::fsid_t,FileSystem*> pFsId2FsPtr;
  /// protects all the above maps
  eos::common::RWMutex pTreeMapMutex;
  //
  // => thread local data
  //
  /// Thread local buffer to hold a working copy of a fast structure
  static __thread void* tlGeoBuffer;
  /// Current scheduling group for the current thread
  static __thread const FsGroup* tlCurrentGroup;
  //
  // => penalties system
  //
  const size_t pCircSize;
  size_t pFrameCount;
  std::vector<tPenaltiesVec> pCircFrCnt2FsPenalties;
  /// self estimated penalties
  std::map<std::string, nodeAgreg> pUpdatingNodes;
  size_t pMaxNetSpeedClass;
  /// Atomic penalties to be applied to the scheduled FSs
  /// those are in the state section because they can be self estimated
  /// the following vectors map an netzorkSpeedClass to a penalty
  std::vector<float> pPlctDlScorePenaltyF,pPlctUlScorePenaltyF;
  std::vector<float> pAccessDlScorePenaltyF,pAccessUlScorePenaltyF;
  // casted version to avoid conversion on every plct / access operation
  std::vector<char> pPlctDlScorePenalty,pPlctUlScorePenalty;
  std::vector<char>  pAccessDlScorePenalty,pAccessUlScorePenalty;
  //
  // => latency estimation
  //
  tLatencyStats pGlobalLatencyStats,globalAgeStats;
  std::vector<tLatencyStats> pFsId2LatencyStats;
  std::vector<size_t> pCircFrCnt2Timestamp;
  //
  // => background updating
  //
  /// thread ID of the dumper thread
  pthread_t pUpdaterTid;
  /// maps a notification subject to changes that happened in the current time frame
  static std::map<std::string,int> gNotificationsBuffer;
  /// deletions to be carried out ASAP
  /// they are delayed so that any function that is using the treemapentry can safely finish
  std::list<TreeMapEntry*> pPendingDeletions;
  /// indicate if the updater is paused
  static bool gUpdaterPaused;
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

  inline void applyDlScorePenalty(TreeMapEntry *entry, const SchedTreeBase::tFastTreeIdx &idx, const char &penalty, bool background=false)
  {
    FastStructures *ft = background?entry->backgroundFastStruct:entry->foregroundFastStruct;
    AtomicSub(ft->placementTree->pNodes[idx].fsData.dlScore,penalty);
    AtomicSub(ft->drnPlacementTree->pNodes[idx].fsData.dlScore,penalty);
    AtomicSub(ft->blcPlacementTree->pNodes[idx].fsData.dlScore,penalty);
    AtomicSub(ft->rOAccessTree->pNodes[idx].fsData.dlScore,penalty);
    AtomicSub(ft->rWAccessTree->pNodes[idx].fsData.dlScore,penalty);
    AtomicSub(ft->drnAccessTree->pNodes[idx].fsData.dlScore,penalty);
    AtomicSub(ft->blcAccessTree->pNodes[idx].fsData.dlScore,penalty);
    if(!background)
    {
    AtomicAdd((*ft->penalties)[idx].dlScorePenalty,penalty);
    }
  }


  inline void applyUlScorePenalty(TreeMapEntry *entry, const SchedTreeBase::tFastTreeIdx &idx, const char &penalty, bool background=false)
  {
    FastStructures *ft = background?entry->backgroundFastStruct:entry->foregroundFastStruct;
    AtomicSub(ft->placementTree->pNodes[idx].fsData.ulScore,penalty);
    AtomicSub(ft->drnPlacementTree->pNodes[idx].fsData.ulScore,penalty);
    AtomicSub(ft->blcPlacementTree->pNodes[idx].fsData.ulScore,penalty);
    AtomicSub(ft->rOAccessTree->pNodes[idx].fsData.ulScore,penalty);
    AtomicSub(ft->rWAccessTree->pNodes[idx].fsData.ulScore,penalty);
    AtomicSub(ft->drnAccessTree->pNodes[idx].fsData.ulScore,penalty);
    AtomicSub(ft->blcAccessTree->pNodes[idx].fsData.ulScore,penalty);
    if(!background)
    {
    AtomicAdd((*ft->penalties)[idx].ulScorePenalty,penalty);
    }
  }

  inline void recallScorePenalty(TreeMapEntry *entry, const SchedTreeBase::tFastTreeIdx &idx)
  {
    auto fsid = (*entry->backgroundFastStruct->treeInfo)[idx].fsId;
    tLatencyStats &lstat = pFsId2LatencyStats[fsid];
    size_t count = 0;
    //eos_static_info("size=%d",(int)pFsId2LatencyStats.size());
    auto mydata = entry->backgroundFastStruct->placementTree->pNodes[idx].fsData;
    for( size_t circIdx = pFrameCount%pCircSize;
        (lstat.lastupdate!=0) && (pCircFrCnt2Timestamp[circIdx] > lstat.lastupdate - pPublishToPenaltyDelayMs);
        circIdx=((pCircSize+circIdx-1)%pCircSize) )
    {
      //eos_static_info("circIdx=%d  count=%d  pCircFrCnt2Timestamp[circIdx]=%lu  &&  lstat.lastupdate=%lf",(int)circIdx, (int)count,pCircFrCnt2Timestamp[circIdx],lstat.lastupdate);
      count++;
      {
        eos_static_warning("breaking because I will stay in an infinite loop");
        break;
      }
      if(entry->foregroundFastStruct->placementTree->pNodes[idx].fsData.dlScore>0)
      applyDlScorePenalty(entry,idx,
                          pCircFrCnt2FsPenalties[circIdx][fsid].dlScorePenalty,
                          true
                          );
      if(entry->foregroundFastStruct->placementTree->pNodes[idx].fsData.ulScore>0)
      applyUlScorePenalty(entry,idx,
                          pCircFrCnt2FsPenalties[circIdx][fsid].ulScorePenalty,
                          true
                          );
    }
    if(mydata.dlScore!=entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.dlScore || mydata.ulScore!=entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.ulScore)
    {
      eos_static_info("score before recalling penalties dl=%d  ul=%d",
                      (int)mydata.dlScore,
                      (int)mydata.ulScore);

      eos_static_info("score after recalling penalties dl=%d  ul=%d",
                    (int)entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.dlScore,
                    (int)entry->backgroundFastStruct->placementTree->pNodes[idx].fsData.ulScore);
    }
  }

  template<class T> bool placeNewReplicas(TreeMapEntry* entry, const size_t &nNewReplicas,

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

    if(eos::common::Logging::gLogMask & LOG_DEBUG)
    {
      stringstream ss;
      ss << (*placementTree);
      eos_debug("fast tree used to copy from is: \n %s",ss.str().c_str());
    }

    // make a working copy of the required fast tree
    if(!tlGeoBuffer) tlGeoBuffer = new char[gGeoBufferSize];// should store this and delete it in the destructor

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
    if(eos::common::Logging::gLogMask & LOG_DEBUG)
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
	if(skipSaturated) eos_notice("Could not find any replica for placement while skipping saturated fs. Trying with saturated nodes included");
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

  template<class T> unsigned char accessReplicas(TreeMapEntry* entry, const size_t &nNewReplicas,
      std::vector<SchedTreeBase::tFastTreeIdx> *accessedReplicas,
      SchedTreeBase::tFastTreeIdx accesserNode,
      std::vector<SchedTreeBase::tFastTreeIdx> *existingReplicas,
      T *accessTree,
      std::vector<SchedTreeBase::tFastTreeIdx> *excludedNodes=NULL,
      std::vector<SchedTreeBase::tFastTreeIdx> *forceNodes=NULL,
      bool skipSaturated=false)
  {

    if(eos::common::Logging::gLogMask & LOG_DEBUG)
    {
      stringstream ss;
      ss << (*accessTree);
      eos_debug("fast tree used to copy from is: \n %s",ss.str().c_str());
    }

    // make a working copy of the required fast tree
    if(!tlGeoBuffer) tlGeoBuffer = new char[gGeoBufferSize];// should store this and delete it in the destructor

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
    if(eos::common::Logging::gLogMask & LOG_DEBUG)
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
	if(skipSaturated) eos_notice("Could not find any replica to access while skipping saturated fs. Trying with saturated nodes included");
	if( (!skipSaturated) || !tree->findFreeSlot(idx, 0, false, true, false) )
	{
	  eos_err("could not find a new slot for a replica in the fast tree");
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

  bool updateTreeInfo(TreeMapEntry* entry, eos::common::FileSystem::fs_snapshot_t *fs, int keys, SchedTreeBase::tFastTreeIdx ftidx=0 , SlowTreeNode *stn=NULL);
  bool updateTreeInfo(const std::map<std::string,int> &updates);

  template<typename T> bool setInternalParam(T& param, const T& value, bool updateStructs)
  {
    eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
    eos::common::RWMutexWriteLock lock2(pTreeMapMutex);
    eos::common::RWMutexWriteLock lock3(configMutex);

    bool result = true;

    param = value;

    for(auto it = pFs2TreeMapEntry.begin(); it != pFs2TreeMapEntry.end(); it++)
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

public:
  GeoTreeEngine () :
  pSkipSaturatedPlct(false),pSkipSaturatedAccess(true),
  pSkipSaturatedDrnAccess(true),pSkipSaturatedBlcAccess(true),
  pSkipSaturatedDrnPlct(false),pSkipSaturatedBlcPlct(false),
  pPenaltyUpdateRate(1),
  pFillRatioLimit(80),pFillRatioCompTol(100),pSaturationThres(10),
  pTimeFrameDurationMs(1000),pPublishToPenaltyDelayMs(1000),
  pCircSize(30),pFrameCount(0), pCircFrCnt2FsPenalties(pCircSize),
  pMaxNetSpeedClass(0),
  pPlctDlScorePenaltyF(256,10),pPlctUlScorePenaltyF(256,10),     // 256 is just a simple way to deal with the initialiaztion of the vector (it's an overshoot but the overhead is tiny)
  pAccessDlScorePenaltyF(256,10),pAccessUlScorePenaltyF(256,10),
  pPlctDlScorePenalty(256,10),pPlctUlScorePenalty(256,10),     // 256 is just a simple way to deal with the initialiaztion of the vector (it's an overshoot but the overhead is tiny)
  pAccessDlScorePenalty(256,10),pAccessUlScorePenalty(256,10),
  pCircFrCnt2Timestamp(pCircSize),
  pUpdaterTid(0)
  {
    for(auto it=pCircFrCnt2FsPenalties.begin(); it!=pCircFrCnt2FsPenalties.end(); it++)
      it->reserve(100);
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
      const std::string &schedgroup, const std::string &optype);

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
  inline void PauseUpdater()
  { gUpdaterPaused = true;}

  // ---------------------------------------------------------------------------
  //! Resume the updating of the GeoTreeEngine
  //! Process all the notifications accumulated since it was paused
  // ---------------------------------------------------------------------------
  inline void ResumeUpdater()
  { gUpdaterPaused = false;}

  // ---------------------------------------------------------------------------
  //! Stop the background updater thread
  // @return
  //   true if success false else
  // ---------------------------------------------------------------------------
  bool StopUpdater();

  bool getGroupsFromFsIds(const std::vector<FileSystem::fsid_t> fsids, std::vector<std::string> *fsgeotags, std::vector<FsGroup*> *sortedgroups);

  inline bool setSkipSaturatedPlct(bool value)
  {
    return setInternalParam(pSkipSaturatedPlct,value,false);
  }
  inline bool setSkipSaturatedAccess(bool value)
  {
    return setInternalParam(pSkipSaturatedAccess,value,false);
  }
  inline bool setSkipSaturatedDrnAccess(bool value)
  {
    return setInternalParam(pSkipSaturatedDrnAccess,value,false);
  }
  inline bool setSkipSaturatedBlcAccess(bool value)
  {
    return setInternalParam(pSkipSaturatedBlcAccess,value,false);
  }
  inline bool setSkipSaturatedDrnPlct(bool value)
  {
    return setInternalParam(pSkipSaturatedDrnPlct,value,false);
  }
  inline bool setSkipSaturatedBlcPlct(bool value)
  {
    return setInternalParam(pSkipSaturatedBlcPlct,value,false);
  }
  inline bool setScorePenalty(std::vector<float> &fvector, std::vector<char> &cvector, char value, int netSpeedClass)
  {
    if(netSpeedClass>=0)
    {
      if(netSpeedClass>=(int)fvector.size())
      return false;
      return setInternalParam(fvector[netSpeedClass],(float)value,false)
      && setInternalParam(cvector[netSpeedClass],value,false);
    }
    else
    {
      std::vector<float> vvaluef(256,value);
      std::vector<char> vvalue(256,value);
      return setInternalParam(fvector,vvaluef,false)
      && setInternalParam(cvector,vvalue,false);
    }
  }

  inline bool setPlctDlScorePenalty(char value, int netSpeedClass)
  {
    return setScorePenalty(pPlctDlScorePenaltyF,pPlctDlScorePenalty,value,netSpeedClass);
  }
  inline bool setPlctUlScorePenalty(char value, int netSpeedClass)
  {
    return setScorePenalty(pPlctUlScorePenaltyF,pPlctUlScorePenalty,value,netSpeedClass);
  }
  inline bool setAccessDlScorePenalty(char value, int netSpeedClass)
  {
    return setScorePenalty(pAccessDlScorePenaltyF,pAccessDlScorePenalty,value,netSpeedClass);
  }
  inline bool setAccessUlScorePenalty(char value, int netSpeedClass)
  {
    return setScorePenalty(pAccessUlScorePenaltyF,pAccessUlScorePenalty,value,netSpeedClass);
  }
  inline bool setFillRatioLimit(char value)
  {
    return setInternalParam(pFillRatioLimit,value,true);
  }
  inline bool setFillRatioCompTol(char value)
  {
    return setInternalParam(pFillRatioCompTol,value,true);
  }
  inline bool setSaturationThres(char value)
  {
    return setInternalParam(pSaturationThres,value,true);
  }
  inline bool setTimeFrameDurationMs(int value)
  {
    return setInternalParam(pTimeFrameDurationMs,value,false);
  }
  inline bool setPenaltyUpdateRate(float value)
  {
    return setInternalParam(pPenaltyUpdateRate,value,false);
  }
};

extern GeoTreeEngine gGeoTreeEngine;

EOSMGMNAMESPACE_END

#endif

