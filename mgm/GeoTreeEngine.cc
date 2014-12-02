// ----------------------------------------------------------------------
// File: GeoTreeEngine.cc
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

/*----------------------------------------------------------------------------*/
#include "mgm/GeoTreeEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/FileSystem.hh"
/*----------------------------------------------------------------------------*/
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <ctime>
#include <sys/stat.h>
#include <tuple>
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

using namespace std;
using namespace eos::common;
using namespace eos::mgm;

EOSMGMNAMESPACE_BEGIN

GeoTreeEngine gGeoTreeEngine;

const size_t GeoTreeEngine::gGeoBufferSize = 64 * 1024;
__thread void* GeoTreeEngine::tlGeoBuffer = NULL;
__thread const FsGroup* GeoTreeEngine::tlCurrentGroup = NULL;

const int
GeoTreeEngine::sfgId = 1,
GeoTreeEngine::sfgHost = 1 << 1,
GeoTreeEngine::sfgGeotag = 1 << 2,
GeoTreeEngine::sfgBoot = 1 << 3,
GeoTreeEngine::sfgActive = 1 << 4,
GeoTreeEngine::sfgConfigstatus = 1 << 5,
GeoTreeEngine::sfgDrain = 1 << 6,
GeoTreeEngine::sfgDrainer = 1 << 6,
GeoTreeEngine::sfgBlcingrun = 1 << 6,
GeoTreeEngine::sfgBlcerrun = 1 << 6,
GeoTreeEngine::sfgBalthres = 1 << 7,
GeoTreeEngine::sfgBlkavailb = 1 << 8,
GeoTreeEngine::sfgFsfilled = 1 << 9,
GeoTreeEngine::sfgNomfilled = 1 << 10,
GeoTreeEngine::sfgWriteratemb = 1 << 11,
GeoTreeEngine::sfgReadratemb = 1 << 12,
GeoTreeEngine::sfgDiskload = 1 << 13,
GeoTreeEngine::sfgEthmib = 1 << 14,
GeoTreeEngine::sfgInratemib = 1 << 15,
GeoTreeEngine::sfgOutratemib = 1 << 16,
GeoTreeEngine::sfgErrc = 1 << 17;

set<string> GeoTreeEngine::gWatchedKeys;

const map<string,int> GeoTreeEngine::gNotifKey2Enum =
{
  make_pair("id",sfgId),
  make_pair("host",sfgHost),
  make_pair("stat.geotag",sfgGeotag),
  make_pair("stat.boot",sfgBoot),
  make_pair("stat.active",sfgActive),
  make_pair("configstatus",sfgConfigstatus),
  make_pair("stat.drain",sfgDrain),
  make_pair("stat.drainer",sfgDrainer),
  make_pair("stat.balancing.running",sfgBlcingrun),
  make_pair("stat.balancer.running",sfgBlcerrun),
  make_pair("stat.balance.threshold",sfgBalthres),
  make_pair("stat.nominal.filled",sfgNomfilled),
  make_pair("stat.statfs.bavail",sfgBlkavailb),
  make_pair("stat.statfs.filled",sfgFsfilled),
  make_pair("stat.disk.writeratemb",sfgWriteratemb),
  make_pair("stat.disk.readratemb",sfgReadratemb),
  make_pair("stat.disk.load",sfgDiskload),
  make_pair("stat.net.ethratemib",sfgEthmib),
  make_pair("stat.net.inratemib",sfgInratemib),
  make_pair("stat.net.outratemib",sfgOutratemib),
  make_pair("stat.errc",sfgErrc)
};

map<string,int> GeoTreeEngine::gNotificationsBuffer;
bool GeoTreeEngine::gUpdaterPaused = false;

bool GeoTreeEngine::TreeMapEntry::updateFastStructures()
{
  FastStructures *ft = backgroundFastStruct;

  if(!slowTree->buildFastStrctures(
	  ft->placementTree , ft->rOAccessTree, ft->rWAccessTree,
	  ft->blcPlacementTree , ft->blcAccessTree,
	  ft->drnPlacementTree , ft->drnAccessTree,
	  ft->treeInfo , ft->fs2TreeIdx, ft->tag2NodeIdx
      ))
  {
    eos_static_crit("Error updating the fast structures");
    return false;
  }

  return true;
}

bool GeoTreeEngine::insertFsIntoGroup(FileSystem *fs ,
    FsGroup *group,
    bool updateFastStruct)
{
  eos::common::RWMutexWriteLock lock(pAddRmFsMutex);

  FileSystem::fsid_t fsid = fs->GetId();
  TreeMapEntry *mapEntry;

  {
    pTreeMapMutex.LockWrite();
    // ==== check that fs is not already registered
    if(pFs2TreeMapEntry.count(fsid))
    {
      eos_err("error inserting fs %lu into group %s : fs is already part of a group",
	  (unsigned long)fsid,
	  group->mName.c_str()
      );
      pTreeMapMutex.UnLockWrite();
      return false;
    }

    // ==== get the entry
    if(pGroup2TreeMapEntry.count(group))
    mapEntry = pGroup2TreeMapEntry[group];
    else
    {
      mapEntry = new TreeMapEntry(group->mName.c_str());
#ifdef EOS_GEOTREEENGINE_USE_INSTRUMENTED_MUTEX
#ifdef EOS_INSTRUMENTED_RWMUTEX
      char buffer[64],buffer2[64];
      sprintf(buffer,"GTE %s doublebuffer",group->mName.c_str());
      sprintf(buffer2,"%s doublebuffer",group->mName.c_str());
      mapEntry->doubleBufferMutex.SetDebugName(buffer2);
      int retcode = eos::common::RWMutex::AddOrderRule(buffer,std::vector<eos::common::RWMutex*>(
	      { &pAddRmFsMutex,&pTreeMapMutex,&mapEntry->doubleBufferMutex}));
      eos_static_info("creating RWMutex rule order %p, retcode is %d",&mapEntry->doubleBufferMutex, retcode);

      sprintf(buffer,"GTE %s slowtree",group->mName.c_str());
      sprintf(buffer2,"%s slowtree",group->mName.c_str());
      mapEntry->slowTreeMutex.SetDebugName(buffer2);
      retcode = eos::common::RWMutex::AddOrderRule(buffer,std::vector<eos::common::RWMutex*>(
	      { &pAddRmFsMutex,&pTreeMapMutex,&mapEntry->slowTreeMutex}));
      eos_static_info("creating RWMutex rule order %p, retcode is %d",&mapEntry->slowTreeMutex, retcode);
#endif
#endif
    }
    mapEntry->slowTreeMutex.LockWrite();
    pTreeMapMutex.UnLockWrite();
  }

  // ==== fill the entry
  // create new TreeNodeInfo/TreeNodeState pair and update its data
  eos::common::FileSystem::fs_snapshot_t fsn;
  fs->SnapShotFileSystem(fsn, true);

  SchedTreeBase::TreeNodeInfo info;
  info.geotag = fsn.mGeoTag;
  if(info.geotag.empty())
  {
    char buffer[64];
    snprintf(buffer,64,"nogeotag");
    info.geotag = buffer;
  }
  info.host= fsn.mHost;
  if(info.host.empty())
  {
    uuid_t uuid;
    char buffer[64];
    snprintf(buffer,64,"nohost-");
    uuid_generate_time(uuid);
    uuid_unparse(uuid, buffer+7);
    info.geotag = buffer;
  }
  info.netSpeedClass = (unsigned char)round(log10(fsn.mNetEthRateMiB*8 * 1024 * 1024 + 1));
  info.netSpeedClass = info.netSpeedClass>8 ? info.netSpeedClass-8 : (unsigned char)0; // netSpeedClass 1 means 1Gbps
  info.fsId = 0;
  info.fsId= fsn.mId;
  if(!info.fsId)
  {
    mapEntry->slowTreeMutex.UnLockWrite();

    eos_err("error inserting fs %lu into group %s : FsId is not set!",
	(unsigned long)fsid,
	group->mName.c_str()
    );

    return false;
  }

  SchedTreeBase::TreeNodeStateFloat state;

  // try to insert the new node in the Slowtree
  SlowTreeNode *node = mapEntry->slowTree->insert(&info,&state);
  if(node==NULL)
  {
    mapEntry->slowTreeMutex.UnLockWrite();

    eos_err("error inserting fs %lu into group %s : slow tree node insertion failed",
	(unsigned long)fsid,
	group->mName.c_str()
    );

    return false;
  }

  // update all the information about this new node
  if(!updateTreeInfo(mapEntry,&fsn,~sfgGeotag & ~sfgId & ~sfgHost ,0,node))
  {
    mapEntry->slowTreeMutex.UnLockWrite();
    pTreeMapMutex.LockRead();
    eos_err("error inserting fs %lu into group %s : slow tree node update failed",
	(unsigned long)fsid,
	group->mName.c_str()
    );
    pTreeMapMutex.UnLockRead();
    return false;
  }

  mapEntry->fs2SlowTreeNode[fsid] = node;
  mapEntry->slowTreeModified = true;

  // update the fast structures now if requested
  if(updateFastStruct)
  {
    if(!updateFastStructures(mapEntry))
    {
      mapEntry->slowTreeMutex.UnLockWrite();
      pTreeMapMutex.LockRead();
      eos_err("error inserting fs %lu into group %s : fast structures update failed",
	  fsid,
	  group->mName.c_str(),
	  pFs2TreeMapEntry[fsid]->group->mName.c_str()
      );
      pTreeMapMutex.UnLockRead();
      return false;
    }
    else
    {
      mapEntry->slowTreeModified = false;
    }
  }

  // ==== update the entry in the map
  {
    pTreeMapMutex.LockWrite();
    mapEntry->group = group;
    pGroup2TreeMapEntry[group] = mapEntry;
    pFs2TreeMapEntry[fsid] = mapEntry;
    pFsId2FsPtr[fsid] = fs;
    pTreeMapMutex.UnLockWrite();
    mapEntry->slowTreeMutex.UnLockWrite();
  }

  // ==== update the shared object notifications
  {
    if(gWatchedKeys.empty())
    {
      for(auto it = gNotifKey2Enum.begin(); it != gNotifKey2Enum.end(); it++ )
      {
	gWatchedKeys.insert(it->first);
      }
    }
    if(!gOFS->ObjectNotifier.SubscribesToSubjectAndKey("geotreeengine",fs->GetQueuePath(),gWatchedKeys,XrdMqSharedObjectChangeNotifier::kMqSubjectStrictModification))
    {
      eos_crit("error inserting fs %lu into group %s : error subscribing to shared object notifications",
	  (unsigned long)fsid,
	  group->mName.c_str()
      );
      return false;
    }
  }

  if(eos::common::Logging::gLogMask & LOG_INFO)
  {
    stringstream ss;
    ss << (*mapEntry->slowTree);

    eos_debug("inserted fs %lu into group %s geotag is %s and fullgeotag is %s\n%s",
	(unsigned long)fsid,
	group->mName.c_str(),
	node->pNodeInfo.geotag.c_str(),
	node->pNodeInfo.fullGeotag.c_str(),
	ss.str().c_str()
    );
  }

  return true;
}

bool GeoTreeEngine::removeFsFromGroup(FileSystem *fs ,
    FsGroup *group,
    bool updateFastStruct)
{
  eos::common::RWMutexWriteLock lock(pAddRmFsMutex);

  TreeMapEntry *mapEntry;
  FileSystem::fsid_t fsid = fs->GetId();

  {
    pTreeMapMutex.LockWrite();
    // ==== check that fs is registered
    if(!pFs2TreeMapEntry.count(fsid))
    {
      eos_err("error removing fs %lu from group %s : fs is not registered",
	  (unsigned long)fsid,
	  group->mName.c_str()
      );
      pTreeMapMutex.UnLockWrite();
      return false;
    }
    mapEntry = pFs2TreeMapEntry[fsid];

    // ==== get the entry
    if(!pGroup2TreeMapEntry.count(group))
    {
      eos_err("error removing fs %lu from group %s : fs is not registered ",
	  (unsigned long)fsid,
	  group->mName.c_str()
      );
      pTreeMapMutex.UnLockWrite();
      return false;
    }
    pTreeMapMutex.UnLockWrite();
    mapEntry = pGroup2TreeMapEntry[group];
    mapEntry->slowTreeMutex.LockWrite();
  }

  // ==== update the shared object notifications
  {
    if(!gOFS->ObjectNotifier.UnsubscribesToSubjectAndKey("geotreeengine",fs->GetQueuePath(),gWatchedKeys,XrdMqSharedObjectChangeNotifier::kMqSubjectStrictModification))
    {
      pTreeMapMutex.UnLockWrite();
      eos_crit("error removing fs %lu into group %s : error unsubscribing to shared object notifications",
	  (unsigned long)fsid,
	  group->mName.c_str()
      );
      return false;
    }
  }

  // ==== discard updates about this fs
  // ==== clean the notifications buffer
  gNotificationsBuffer.erase(fs->GetQueuePath());
  // ==== clean the thread-local notification queue
  {
    XrdMqSharedObjectChangeNotifier::Subscriber *subscriber = gOFS->ObjectNotifier.GetSubscriberFromCatalog("geotreeengine",false);
    subscriber->SubjectsMutex.Lock();
    for ( auto it = subscriber->NotificationSubjects.begin();
	it != subscriber->NotificationSubjects.end(); it++ )
    {
      // to mark the filesystem as removed, we change the notification type flag
      if(it->mSubject.compare(0,fs->GetQueuePath().length(),fs->GetQueuePath())==0)
      {
	eos_static_warning("found a notification to remove %s ",it->mSubject.c_str());
	it->mType = XrdMqSharedObjectManager::kMqSubjectDeletion;
      }
    }
    subscriber->SubjectsMutex.UnLock();
  }

  // ==== update the entry
  SchedTreeBase::TreeNodeInfo info;
  const SlowTreeNode *intree = mapEntry->fs2SlowTreeNode[fsid];
  info = intree->pNodeInfo;
  info.geotag = intree->pNodeInfo.fullGeotag;
  eos_debug("SlowNodeTree to be removed is %lu   %s   %s   %s",
      (unsigned long)intree->pNodeInfo.fsId,
      intree->pNodeInfo.host.c_str(),
      intree->pNodeInfo.geotag.c_str(),
      intree->pNodeInfo.fullGeotag.c_str());
  // try to update the SlowTree
  info.fsId = 0;
  if(!mapEntry->slowTree->remove(&info))
  {
    mapEntry->slowTreeMutex.UnLockWrite();
    eos_err("error removing fs %lu from group %s : removing the slow tree node failed. geotag is %s and geotag in tree is %s and %s",
	(unsigned long)fsid,
	group->mName.c_str(),
	info.geotag.c_str(),
	intree->pNodeInfo.fullGeotag.c_str(),
	intree->pNodeInfo.geotag.c_str()
    );
    return false;
  }
  mapEntry->fs2SlowTreeNode.erase(fsid);
  // if the tree is empty, remove the entry from the map
  if(!mapEntry->fs2SlowTreeNode.empty())// if the tree is getting empty, no need to update it
  mapEntry->slowTreeModified = true;

  if(updateFastStruct && mapEntry->slowTreeModified)
  if(!updateFastStructures(mapEntry))
  {
    mapEntry->slowTreeMutex.UnLockWrite();
    pTreeMapMutex.LockRead();
    eos_err("error removing fs %lu from group %s : fast structures update failed",
	fsid,
	group->mName.c_str(),
	pFs2TreeMapEntry[fsid]->group->mName.c_str()
    );
    pTreeMapMutex.UnLockRead();
    return false;
  }

  // ==== update the entry in the map if needed
  {
    pTreeMapMutex.LockWrite();
    pFs2TreeMapEntry.erase(fsid);
    pFsId2FsPtr.erase(fsid);
    if(mapEntry->fs2SlowTreeNode.empty())
    {
      pGroup2TreeMapEntry.erase(group); // prevent from access by other threads
      pPendingDeletions.push_back(mapEntry);
    }
    mapEntry->slowTreeMutex.UnLockWrite();
    pTreeMapMutex.UnLockWrite();
  }

  return true;
}

void GeoTreeEngine::printInfo(std::string &info,
    bool dispTree, bool dispSnaps, bool dispLs,
    const std::string &schedgroup, const std::string &optype)
{
  RWMutexReadLock lock(pTreeMapMutex);

  stringstream ostr;

  map<string,string> orderByGroupName;

  if(dispLs)
  {
    ostr << "*** GeoTreeEngine parameters :" << std::endl;
    ostr << "skipSaturatedPlct = " << skipSaturatedPlct << std::endl;
    ostr << "skipSaturatedAccess = "<< skipSaturatedAccess << std::endl;
    ostr << "skipSaturatedDrnAccess = "<< skipSaturatedDrnAccess << std::endl;
    ostr << "skipSaturatedBlcAccess = "<< skipSaturatedBlcAccess << std::endl;
    ostr << "skipSaturatedDrnPlct = "<< skipSaturatedDrnPlct << std::endl;
    ostr << "skipSaturatedBlcPlct = "<< skipSaturatedBlcPlct << std::endl;
    ostr << "penaltyUpdateRate = "<< penaltyUpdateRate << std::endl;
    ostr << "plctDlScorePenalty = "<< plctDlScorePenaltyF[0] << "(default)" << " | "
        << plctDlScorePenaltyF[1] << "(1Gbps)" << " | "
        << plctDlScorePenaltyF[2] << "(10Gbps)" << " | "
        << plctDlScorePenaltyF[3] << "(100Gbps)" << " | "
        << plctDlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "plctUlScorePenalty = "<< plctUlScorePenaltyF[0] << "(defaUlt)" << " | "
        << plctUlScorePenaltyF[1] << "(1Gbps)" << " | "
        << plctUlScorePenaltyF[2] << "(10Gbps)" << " | "
        << plctUlScorePenaltyF[3] << "(100Gbps)" << " | "
        << plctUlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "accessDlScorePenalty = "<< accessDlScorePenaltyF[0] << "(default)" << " | "
        << accessDlScorePenaltyF[1] << "(1Gbps)" << " | "
        << accessDlScorePenaltyF[2] << "(10Gbps)" << " | "
        << accessDlScorePenaltyF[3] << "(100Gbps)" << " | "
        << accessDlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "accessUlScorePenalty = "<< accessUlScorePenaltyF[0] << "(defaUlt)" << " | "
        << accessUlScorePenaltyF[1] << "(1Gbps)" << " | "
        << accessUlScorePenaltyF[2] << "(10Gbps)" << " | "
        << accessUlScorePenaltyF[3] << "(100Gbps)" << " | "
        << accessUlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "fillRatioLimit = "<< (int)fillRatioLimit << std::endl;
    ostr << "fillRatioCompTol = "<< (int)fillRatioCompTol << std::endl;
    ostr << "saturationThres = "<< (int)saturationThres << std::endl;
    ostr << "timeFrameDurationMs = "<< (int)timeFrameDurationMs << std::endl;
    ostr << "stateLatency  = "<< latencyStats.min <<"ms.(min)"<< " | "
        << latencyStats.average <<"ms.(avg)"<< " | "
        << latencyStats.max <<"ms.(max)"<< std::endl;
  }

  // ==== run through the map of file systems
  for(auto it = pGroup2TreeMapEntry.begin(); it != pGroup2TreeMapEntry.end(); it++)
  {
    stringstream ostr;

    if(dispTree && (schedgroup.empty() || (schedgroup == it->second->group->mName) ) )
    {
      ostr << "*** scheduling tree for scheduling group "<< it->second->group->mName <<" :" << std::endl;
      ostr << *it->second->slowTree << std::endl;
    }

    if(dispSnaps && (schedgroup.empty() || (schedgroup == it->second->group->mName) ) )
    {
      if(optype.empty() || (optype == "plct") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Placement\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->placementTree << std::endl;
      }
      if(optype.empty() || (optype == "accsro") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Access RO\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->rOAccessTree << std::endl;
      }
      if(optype.empty() || (optype == "accsrw") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Access RW\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->rWAccessTree << std::endl;
      }
      if(optype.empty() || (optype == "accsdrain") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Draining Access\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->drnAccessTree << std::endl;
      }
      if(optype.empty() || (optype == "plctdrain") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Draining Placement\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->drnPlacementTree << std::endl;
      }
      if(optype.empty() || (optype == "accsblc") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Balancing Access\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->blcAccessTree << std::endl;
      }
      if(optype.empty() || (optype == "plctblc") )
      {
	ostr << "*** scheduling snapshot for scheduling group "<< it->second->group->mName <<" and operation \'Draining Placement\' :" << std::endl;
	ostr << *it->second->foregroundFastStruct->blcPlacementTree << std::endl;
      }
    }

    orderByGroupName[it->second->group->mName] = ostr.str();
  }

  if(dispLs)
  {
    ostr << "*** GeoTreeEngine list of groups :" << std::endl;
    if(orderByGroupName.size())
    {
      const int lineWidth = 80;
      const int countNamePerLine = lineWidth / (orderByGroupName.begin()->first.size()+3);
      int count = 1;
      for(auto it = orderByGroupName.begin(); it != orderByGroupName.end(); it++, count++)
      {
	ostr << it->first;
	if(count%countNamePerLine)
	ostr << " , ";
	else
	ostr << "\n";
      }
    }
  }

  for(auto it = orderByGroupName.begin(); it != orderByGroupName.end(); it++)
  ostr << it->second;

  info = ostr.str();
}

bool GeoTreeEngine::placeNewReplicasOneGroup( FsGroup* group, const size_t &nNewReplicas,
    vector<FileSystem::fsid_t> *newReplicas,
    SchedType type,
    vector<FileSystem::fsid_t> *existingReplicas,
    std::vector<std::string> *fsidsgeotags,
    unsigned long long bookingSize,
    const std::string &startFromGeoTag,
    const size_t &nCollocatedReplicas,
    vector<FileSystem::fsid_t> *excludeFs,
    vector<string> *excludeGeoTags,
    vector<string> *forceGeoTags)
{
  assert(nNewReplicas);
  assert(newReplicas);

  // find the entry in the map
  tlCurrentGroup = group;
  TreeMapEntry *entry;
  {
    RWMutexReadLock lock(this->pTreeMapMutex);
    if(!pGroup2TreeMapEntry.count(group))
    {
      eos_err("could not find the requested placement group in the map");
      return false;
    }
    entry = pGroup2TreeMapEntry[group];
    AtomicInc(entry->fastStructLockWaitersCount);
  }

  // readlock the original fast structure
  entry->doubleBufferMutex.LockRead();

  // locate the existing replicas and the excluded fs in the tree
  vector<SchedTreeBase::tFastTreeIdx> newReplicasIdx(nNewReplicas),*existingReplicasIdx=NULL,*excludeFsIdx=NULL,*forceBrIdx=NULL;
  newReplicasIdx.resize(0);
  if(existingReplicas)
  {
    existingReplicasIdx = new vector<SchedTreeBase::tFastTreeIdx>(existingReplicas->size());
    existingReplicasIdx->resize(0);
    int count = 0;
    for(auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it , ++count)
    {
      const SchedTreeBase::tFastTreeIdx *idx = static_cast<const SchedTreeBase::tFastTreeIdx*>(0);
      if(!entry->foregroundFastStruct->fs2TreeIdx->get(*it,idx) && !(*fsidsgeotags)[count].empty())
      {
	// the fs is not in that group.
	// this could happen because the former file scheduler
	// could place replicas across multiple groups
	// with the new geoscheduler, it should not happen

	// in that case, we try to match a filesystem having the same geotag
	SchedTreeBase::tFastTreeIdx idx = entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode((*fsidsgeotags)[count].c_str());
	if(idx && (*entry->foregroundFastStruct->treeInfo)[idx].nodeType == SchedTreeBase::TreeNodeInfo::fs)
	{
	  if((std::find(existingReplicasIdx->begin(),existingReplicasIdx->end(),idx) == existingReplicasIdx->end()))
	  existingReplicasIdx->push_back(idx);
	}
	// if we can't find any such filesystem, the information is not taken into account
	// (and then can lead to unoptimal placement
	else
	{
	  eos_debug("could not place preexisting replica on the fast tree");
	}
	continue;
      }
      existingReplicasIdx->push_back(*idx);
    }
  }
  if(excludeFs)
  {
    excludeFsIdx = new vector<SchedTreeBase::tFastTreeIdx>(excludeFs->size());
    excludeFsIdx->resize(0);
    for(auto it = excludeFs->begin(); it != excludeFs->end(); ++it)
    {
      const SchedTreeBase::tFastTreeIdx *idx;
      if(!entry->foregroundFastStruct->fs2TreeIdx->get(*it,idx))
      {
	// the excluded fs might belong to another group
	// so it's not an error condition
	// eos_warning("could not place excluded fs on the fast tree");
	continue;
      }
      excludeFsIdx->push_back(*idx);
    }
  }
  if(excludeGeoTags)
  {
    if(!excludeFsIdx)
    {
      excludeFsIdx = new vector<SchedTreeBase::tFastTreeIdx>(excludeGeoTags->size());
      excludeFsIdx->resize(0);
    }
    for(auto it = excludeGeoTags->begin(); it != excludeGeoTags->end(); ++it)
    {
      SchedTreeBase::tFastTreeIdx idx;
      idx=entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(it->c_str());
      excludeFsIdx->push_back(idx);
    }
  }
  if(forceGeoTags)
  {
    forceBrIdx = new vector<SchedTreeBase::tFastTreeIdx>(forceGeoTags->size());
    excludeFsIdx->resize(0);
    for(auto it = forceGeoTags->begin(); it != forceGeoTags->end(); ++it)
    {
      SchedTreeBase::tFastTreeIdx idx;
      idx=entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(it->c_str());
      forceBrIdx->push_back(idx);
    }
  }

  SchedTreeBase::tFastTreeIdx startFromNode=0;
  if(!startFromGeoTag.empty())
  {
    startFromNode=entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(startFromGeoTag.c_str());
  }

  // actually do the job
  bool success = false;
  switch(type)
  {
    case regularRO:
    case regularRW:
    success = placeNewReplicas(entry,nNewReplicas,&newReplicasIdx,entry->foregroundFastStruct->placementTree,
	existingReplicasIdx,bookingSize,startFromNode,nCollocatedReplicas,excludeFsIdx,forceBrIdx,skipSaturatedPlct);
    break;
    case draining:
    success = placeNewReplicas(entry,nNewReplicas,&newReplicasIdx,entry->foregroundFastStruct->drnPlacementTree,
	existingReplicasIdx,bookingSize,startFromNode,nCollocatedReplicas,excludeFsIdx,forceBrIdx,skipSaturatedDrnPlct);
    break;
    case balancing:
    success = placeNewReplicas(entry,nNewReplicas,&newReplicasIdx,entry->foregroundFastStruct->blcPlacementTree,
	existingReplicasIdx,bookingSize,startFromNode,nCollocatedReplicas,excludeFsIdx,forceBrIdx,skipSaturatedBlcPlct);
    break;
    default:
    ;
  }
  if(!success) goto cleanup;

  // fill the resulting vector and
  // update the fastTree UlScore and DlScore by applying the penalties
  newReplicas->resize(0);
  for(auto it = newReplicasIdx.begin(); it != newReplicasIdx.end(); ++it)
  {
    const SchedTreeBase::tFastTreeIdx *idx=NULL;
    const unsigned int fsid = (*entry->foregroundFastStruct->treeInfo)[*it].fsId;
    const char netSpeedClass = (*entry->foregroundFastStruct->treeInfo)[*it].netSpeedClass;
    newReplicas->push_back(fsid);
    // apply the penalties
    entry->foregroundFastStruct->fs2TreeIdx->get(fsid,idx);
    if(entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.dlScore>=plctDlScorePenalty[netSpeedClass])
    applyDlScorePenalty(entry,*idx,plctDlScorePenalty[netSpeedClass]);
    if(entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.ulScore>=plctUlScorePenalty[netSpeedClass])
    applyUlScorePenalty(entry,*idx,plctUlScorePenalty[netSpeedClass]);
  }

  // unlock, cleanup
  cleanup:
  entry->doubleBufferMutex.UnLockRead();
  AtomicDec(entry->fastStructLockWaitersCount);
  if(existingReplicasIdx) delete existingReplicasIdx;
  if(excludeFsIdx) delete excludeFsIdx;
  if(forceBrIdx) delete forceBrIdx;

  return success;
}

bool GeoTreeEngine::accessReplicasOneGroup(FsGroup* group, const size_t &nAccessReplicas,
    vector<FileSystem::fsid_t> *accessedReplicas,
    vector<FileSystem::fsid_t> *existingReplicas,
    SchedType type,
    const string &accesserGeotag,
    vector<FileSystem::fsid_t> *excludeFs,
    vector<string> *excludeGeoTags,
    vector<string> *forceGeoTags)
{

  // some basic checks
  assert(nAccessReplicas);
  assert(accessedReplicas);
  assert(existingReplicas);
  // check that enough replicas exist already
  if(nAccessReplicas > existingReplicas->size())
  return false;
  // if there is no choice, return all replicas
  if(nAccessReplicas == existingReplicas->size())
  {
    accessedReplicas->resize(0);
    accessedReplicas->insert(accessedReplicas->begin(),existingReplicas->begin(),existingReplicas->end());
    return true;
  }

  // find the entry in the map
  tlCurrentGroup = group;
  TreeMapEntry *entry;
  {
    RWMutexReadLock lock(this->pTreeMapMutex);
    if(!pGroup2TreeMapEntry.count(group))
    {
      eos_err("could not find the requested placement group in the map");
      return false;
    }
    entry = pGroup2TreeMapEntry[group];
    AtomicInc(entry->fastStructLockWaitersCount);
  }

  // readlock the original fast structure
  entry->doubleBufferMutex.LockRead();

  // locate the existing replicas and the excluded fs in the tree
  vector<SchedTreeBase::tFastTreeIdx> accessedReplicasIdx(nAccessReplicas),*existingReplicasIdx=NULL,*excludeFsIdx=NULL,*forceBrIdx=NULL;
  accessedReplicasIdx.resize(0);
  if(existingReplicas)
  {
    existingReplicasIdx = new vector<SchedTreeBase::tFastTreeIdx>(existingReplicas->size());
    existingReplicasIdx->resize(0);
    for(auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it)
    {
      const SchedTreeBase::tFastTreeIdx *idx;
      if(!entry->foregroundFastStruct->fs2TreeIdx->get(*it,idx))
      {
	eos_warning("could not place preexisting replica on the fast tree");
	continue;
      }
      existingReplicasIdx->push_back(*idx);
    }
  }
  if(excludeFs)
  {
    excludeFsIdx = new vector<SchedTreeBase::tFastTreeIdx>(excludeFs->size());
    excludeFsIdx->resize(0);
    for(auto it = excludeFs->begin(); it != excludeFs->end(); ++it)
    {
      const SchedTreeBase::tFastTreeIdx *idx;
      if(!entry->foregroundFastStruct->fs2TreeIdx->get(*it,idx))
      {
	eos_warning("could not place excluded fs on the fast tree");
	continue;
      }
      excludeFsIdx->push_back(*idx);
    }
  }
  if(excludeGeoTags)
  {
    if(!excludeFsIdx)
    {
      excludeFsIdx = new vector<SchedTreeBase::tFastTreeIdx>(excludeGeoTags->size());
      excludeFsIdx->resize(0);
    }
    for(auto it = excludeGeoTags->begin(); it != excludeGeoTags->end(); ++it)
    {
      SchedTreeBase::tFastTreeIdx idx;
      idx=entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(it->c_str());
      excludeFsIdx->push_back(idx);
    }
  }
  if(forceGeoTags)
  {
    forceBrIdx = new vector<SchedTreeBase::tFastTreeIdx>(forceGeoTags->size());
    excludeFsIdx->resize(0);
    for(auto it = forceGeoTags->begin(); it != forceGeoTags->end(); ++it)
    {
      SchedTreeBase::tFastTreeIdx idx;
      idx=entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(it->c_str());
      forceBrIdx->push_back(idx);
    }
  }

  // find the closest tree node to the accesser
  SchedTreeBase::tFastTreeIdx accesserNode = entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(accesserGeotag.c_str());;

  // actually do the job
  unsigned char success = 0;
  switch(type)
  {
    case regularRO:
    success = accessReplicas(entry,nAccessReplicas,&accessedReplicasIdx,accesserNode,existingReplicasIdx,
	entry->foregroundFastStruct->rOAccessTree,excludeFsIdx,forceBrIdx,skipSaturatedAccess);
    break;
    case regularRW:
    success = accessReplicas(entry,nAccessReplicas,&accessedReplicasIdx,accesserNode,existingReplicasIdx,
	entry->foregroundFastStruct->rWAccessTree,excludeFsIdx,forceBrIdx,skipSaturatedAccess);
    break;
    case draining:
    success = accessReplicas(entry,nAccessReplicas,&accessedReplicasIdx,accesserNode,existingReplicasIdx,
	entry->foregroundFastStruct->drnAccessTree,excludeFsIdx,forceBrIdx,skipSaturatedDrnAccess);
    break;
    case balancing:
    success = accessReplicas(entry,nAccessReplicas,&accessedReplicasIdx,accesserNode,existingReplicasIdx,
	entry->foregroundFastStruct->blcAccessTree,excludeFsIdx,forceBrIdx,skipSaturatedBlcAccess);
    break;
    default:
    ;
  }
  if(!success) goto cleanup;

  // fill the resulting vector
  // update the fastTree UlScore and DlScore by applying the penalties
  accessedReplicas->resize(0);
  for(auto it = accessedReplicasIdx.begin(); it != accessedReplicasIdx.end(); ++it)
  {
    const SchedTreeBase::tFastTreeIdx *idx=NULL;
    const unsigned int fsid = (*entry->foregroundFastStruct->treeInfo)[*it].fsId;
    const char netSpeedClass = (*entry->foregroundFastStruct->treeInfo)[*it].netSpeedClass;
    accessedReplicas->push_back(fsid);
    // apply the penalties
    if(!entry->foregroundFastStruct->fs2TreeIdx->get(fsid,idx))
    {
      eos_static_crit("inconsistency : cannot retrieve index of selected fs though it should be in the tree");
      success = false;
      goto cleanup;
    }
    if(entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.dlScore>=accessDlScorePenalty[netSpeedClass])
    applyDlScorePenalty(entry,*idx,accessDlScorePenalty[netSpeedClass]);
    if(entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.ulScore>=accessUlScorePenalty[netSpeedClass])
    applyUlScorePenalty(entry,*idx,accessUlScorePenalty[netSpeedClass]);
  }

  // unlock, cleanup
  cleanup:
  entry->doubleBufferMutex.UnLockRead();
  AtomicDec(entry->fastStructLockWaitersCount);
  if(existingReplicasIdx) delete existingReplicasIdx;
  if(excludeFsIdx) delete excludeFsIdx;
  if(forceBrIdx) delete forceBrIdx;

  return success;
}

///// helper class to order filesystems for a given geoscore
//struct FsComparator
//{
//	eos::mgm::SchedTreeBase::TreeNodeSlots freeSlot;
//	GeoTreeEngine::SchedType type;
//	FsComparator(GeoTreeEngine::SchedType _type) : type(_type)
//	{	freeSlot.freeSlotsCount=1;};
//	int operator() (const tuple< FileSystem::fsid_t , GeoTreeEngine::TreeMapEntry* , SchedTreeBase::tFastTreeIdx > & left, const tuple< FileSystem::fsid_t , GeoTreeEngine::TreeMapEntry* , SchedTreeBase::tFastTreeIdx > & right) const
//	{
//		switch(type)
//		{
//			case GeoTreeEngine::regularRO:
//			return eos::mgm::FastROAccessTree::compareAccess(
//					& get<1>(left)->foregroundFastStruct->rOAccessTree->pNodes[std::get<2>(left)].fsData,
//					&freeSlot,
//					& get<1>(right)->foregroundFastStruct->rOAccessTree->pNodes[std::get<2>(right)].fsData,
//					&freeSlot
//			);
//			break;
//			case GeoTreeEngine::regularRW:
//			return eos::mgm::FastROAccessTree::compareAccess(
//					& get<1>(left)->foregroundFastStruct->rWAccessTree->pNodes[std::get<2>(left)].fsData,
//					&freeSlot,
//					& get<1>(right)->foregroundFastStruct->rWAccessTree->pNodes[std::get<2>(right)].fsData,
//					&freeSlot
//			);
//			break;
//			case GeoTreeEngine::draining:
//			return eos::mgm::FastROAccessTree::compareAccess(
//					& get<1>(left)->foregroundFastStruct->drnAccessTree->pNodes[std::get<2>(left)].fsData,
//					&freeSlot,
//					& get<1>(right)->foregroundFastStruct->drnAccessTree->pNodes[std::get<2>(right)].fsData,
//					&freeSlot
//			);
//			break;
//			case GeoTreeEngine::balancing:
//			return eos::mgm::FastROAccessTree::compareAccess(
//					& get<1>(left)->foregroundFastStruct->blcAccessTree->pNodes[std::get<2>(left)].fsData,
//					&freeSlot,
//					& get<1>(right)->foregroundFastStruct->blcAccessTree->pNodes[std::get<2>(right)].fsData,
//					&freeSlot
//			);
//			break;
//			default:
//			break;
//		}
//		return 0;
//	}
//};
//
// This function try to get multiple fs to access a file from
// Fs are spread across multiple fs
// The resulting fs are returned by decreasing order of priority
//bool GeoTreeEngine::accessReplicasMultipleGroup(const size_t &nAccessReplicas,
//		vector<FileSystem::fsid_t> *accessedReplicas,
//		vector<FileSystem::fsid_t> *existingReplicas,
//		SchedType type,
//		const string &accesserGeotag,
//		vector<FileSystem::fsid_t> *excludeFs,
//		vector<string> *excludeGeoTags,
//		vector<string> *forceGeoTags)
//{
//
//	// some basic checks
//	assert(nAccessReplicas);
//	assert(accessedReplicas);
//	assert(existingReplicas);
//	// check that enough replicas exist already
//	if(nAccessReplicas > existingReplicas->size())
//	{
//		eos_static_debug("not enough replica");
//		return false;
//	}
//
//	// if there is no choice, return all replicas
//	if(nAccessReplicas == existingReplicas->size())
//	{
//		accessedReplicas->resize(0);
//		accessedReplicas->insert(accessedReplicas->begin(),existingReplicas->begin(),existingReplicas->end());
//		return true;
//	}
//
//	// find the group holdings the fs of the existing replicas
//	map<TreeMapEntry*,vector<FileSystem::fsid_t>> entry2FsId;
//	TreeMapEntry *entry=NULL;
//	{
//		RWMutexReadLock lock(this->pTreeMapMutex);
//		for(auto exrepIt = existingReplicas->begin(); exrepIt != existingReplicas->end(); exrepIt++)
//		{
//			auto mentry = pFs2TreeMapEntry.find(*exrepIt);
//			// if we cannot find the fs in any group, there is an inconsistency somewhere
//			if(mentry == pFs2TreeMapEntry.end())
//			{
//				eos_static_err("cannot find the existing replica in any scheduling group");
//				return false;
//			}
//			entry = mentry->second;
//			entry2FsId[entry].push_back(*exrepIt);
//		}
//		// to prevent any change of the trees
//		entry->doubleBufferMutex.LockRead();
//		// to prevent the destruction of the entry
//		AtomicInc(entry->fastStructLockWaitersCount);
//	}
//
//	// if we have only one group , we use the one group procedure
//	if(entry2FsId.size()==1)
//	{
//		entry = entry2FsId.begin()->first;
//		eos_static_debug("existing replicas are only in one group, using accessReplicasOneGroup");
//		// to prevent the destruction of the entry
//		entry->doubleBufferMutex.UnLockRead();
//		AtomicDec(entry->fastStructLockWaitersCount);
//		return accessReplicasOneGroup( entry->group, nAccessReplicas,
//				accessedReplicas,existingReplicas,
//				type,accesserGeotag,excludeFs,
//				excludeGeoTags,forceGeoTags);
//	}
//
//	// we have multiple groups
//	eos::mgm::ROAccessPriorityComparator comp;
//	eos::mgm::SchedTreeBase::TreeNodeSlots freeSlot;
//	freeSlot.freeSlotsCount=1;
//
//	// compute their geolocation score
//	size_t availFsCount = 0;
//	map< unsigned , std::vector< tuple< FileSystem::fsid_t , TreeMapEntry* , SchedTreeBase::tFastTreeIdx > > > geoScore2Fs;
//	for(auto entryIt = entry2FsId.begin(); entryIt != entry2FsId.end(); entryIt ++)
//	for(auto fsIt = entryIt->second.begin(); fsIt != entryIt->second.end(); fsIt++)
//	{
//		const SchedTreeBase::tFastTreeIdx *idx;
//		if(!entryIt->first->foregroundFastStruct->fs2TreeIdx->get(*fsIt,idx) )
//		{
//			eos_static_warning("cannot find fs in the group in the 2nd pass");
//			continue;
//		}
//		// check if the fs is available
//		bool isValid = false;
//		switch(type)
//		{
//			case regularRO:
//			comp.isValidSlot(&entryIt->first->foregroundFastStruct->rOAccessTree->pNodes[*idx].fsData,&freeSlot);
//			break;
//			case regularRW:
//			comp.isValidSlot(&entryIt->first->foregroundFastStruct->rWAccessTree->pNodes[*idx].fsData,&freeSlot);
//			break;
//			case draining:
//			comp.isValidSlot(&entryIt->first->foregroundFastStruct->drnAccessTree->pNodes[*idx].fsData,&freeSlot);
//			break;
//			case balancing:
//			comp.isValidSlot(&entryIt->first->foregroundFastStruct->blcAccessTree->pNodes[*idx].fsData,&freeSlot);
//			break;
//			default:
//			break;
//		}
//		if(!isValid)
//		{
//			eos_static_debug("fs skipped because unavailable");
//			continue;
//		}
//
//		const string &fsGeotag = (*entryIt->first->foregroundFastStruct->treeInfo)[*idx].fullGeotag;
//		unsigned geoScore = 0;
//		size_t kmax = min(accesserGeotag.length(),fsGeotag.length());
//		for(size_t k=0; k<kmax; k++)
//		{
//			if(accesserGeotag[k]!=fsGeotag[k])
//			break;
//			if(accesserGeotag[k]==':' && k+1 < kmax && accesserGeotag[k+1]==':')
//			geoScore++;
//		}
//		geoScore2Fs[geoScore].push_back(make_tuple(*fsIt,entryIt->first,*idx));
//		availFsCount++;
//	}
//
//	// check we have enough available fs
//	if(availFsCount<nAccessReplicas)
//	{
//		for(auto it = entry2FsId.begin(); it != entry2FsId.end(); it++ )
//		{
//			it->first->doubleBufferMutex.UnLockRead();
//			AtomicDec(it->first->fastStructLockWaitersCount);
//		}
//		eos_static_debug("not enough replica available");
//		return false;
//	}
//
//	FsComparator fscomp(type);
//	size_t fsToGet = nAccessReplicas;
//	for(auto geoscoreIt = geoScore2Fs.begin(); geoscoreIt != geoScore2Fs.end(); geoscoreIt++)
//	{
//		// sort in descending order.
//		std::sort(geoscoreIt->second.begin() , geoscoreIt->second.end() , fscomp);
//		size_t n = min(fsToGet,geoscoreIt->second.size());
//		for(auto it=geoscoreIt->second.begin(); it!=geoscoreIt->second.begin()+n; it++)
//		accessedReplicas->push_back(get<0>(*it));
//		fsToGet -= n;
//		if(fsToGet==0) break;
//	}
//	if(fsToGet)
//	{
//		eos_err("inconsistency : could not retrieve enough fs");
//		for(auto it = entry2FsId.begin(); it != entry2FsId.end(); it++ )
//		{
//			it->first->doubleBufferMutex.UnLockRead();
//			AtomicDec(it->first->fastStructLockWaitersCount);
//		}
//		accessedReplicas->clear();
//		return false;
//	}
//
//	// cleanup and exit
//	for(auto it = entry2FsId.begin(); it != entry2FsId.end(); it++ )
//	{
//		it->first->doubleBufferMutex.UnLockRead();
//		AtomicDec(it->first->fastStructLockWaitersCount);
//	}
//	return true;
//}

int GeoTreeEngine::accessHeadReplicaMultipleGroup(const size_t &nAccessReplicas,
    unsigned long &fsIndex,
    std::vector<eos::common::FileSystem::fsid_t> *existingReplicas,
    SchedType type,
    const std::string &accesserGeotag,
    const eos::common::FileSystem::fsid_t &forcedFsId,
    std::vector<eos::common::FileSystem::fsid_t> *unavailableFs
)
{
  int returnCode = ENODATA;

  // some basic checks
  assert(nAccessReplicas);
  assert(existingReplicas);

  // check that enough replicas exist already
  if(nAccessReplicas > existingReplicas->size())
  {
    eos_static_debug("not enough replica : has %d and requires %d :",(int)existingReplicas->size(),(int)nAccessReplicas);
    return EROFS;
  }

  // check if the forced replicas (if any) is among the existing replicas
  if(forcedFsId>0 && (std::find(existingReplicas->begin(), existingReplicas->end(), forcedFsId) == existingReplicas->end()) )
  {
    return ENODATA;
  }

  // find the group holdings the fs of the existing replicas
  // check that the replicas are available
  size_t availFsCount = 0;
  eos::mgm::ROAccessPriorityComparator comp;
  eos::mgm::SchedTreeBase::TreeNodeSlots freeSlot;
  freeSlot.freeSlotsCount=1;
  std::vector<eos::common::FileSystem::fsid_t>::iterator it;

  // maps tree maps entries (i.e. scheduling groups) to fsids containing a replica being available and the corresponding fastTreeIndex
  map<TreeMapEntry*,vector< pair<FileSystem::fsid_t,SchedTreeBase::tFastTreeIdx> > > entry2FsId;
  TreeMapEntry *entry=NULL;
  {
    // lock the scheduling group -> trees map so that the a map entry cannot be delete while processing it
    RWMutexReadLock lock(this->pTreeMapMutex);
    for(auto exrepIt = existingReplicas->begin(); exrepIt != existingReplicas->end(); exrepIt++)
    {
      auto mentry = pFs2TreeMapEntry.find(*exrepIt);
      // if we cannot find the fs in any group, there is an inconsistency somewhere
      if(mentry == pFs2TreeMapEntry.end())
      {
	eos_static_warning("cannot find the existing replica in any scheduling group");
	continue;
      }
      entry = mentry->second;

      // lock the double buffering to make sure all the fast trees are not modified
      if(!entry2FsId.count(entry))
      {
	// if the entry is already there, it was locked already
	entry->doubleBufferMutex.LockRead();
	// to prevent the destruction of the entry
	AtomicInc(entry->fastStructLockWaitersCount);
      }

      const SchedTreeBase::tFastTreeIdx *idx;
      if(!entry->foregroundFastStruct->fs2TreeIdx->get(*exrepIt,idx) )
      {
	eos_static_warning("cannot find fs in the scheduling group in the 2nd pass");
	if(!entry2FsId.count(entry))
	{
	  entry->doubleBufferMutex.UnLockRead();
	  AtomicDec(entry->fastStructLockWaitersCount);
	}
	continue;
      }
      // check if the fs is available
      bool isValid = false;
      switch(type)
      {
	case regularRO:
	isValid = entry->foregroundFastStruct->rOAccessTree->pBranchComp.isValidSlot(&entry->foregroundFastStruct->rOAccessTree->pNodes[*idx].fsData,&freeSlot);
	break;
	case regularRW:
	isValid = entry->foregroundFastStruct->rWAccessTree->pBranchComp.isValidSlot(&entry->foregroundFastStruct->rWAccessTree->pNodes[*idx].fsData,&freeSlot);
	break;
	case draining:
	isValid = entry->foregroundFastStruct->drnAccessTree->pBranchComp.isValidSlot(&entry->foregroundFastStruct->drnAccessTree->pNodes[*idx].fsData,&freeSlot);
	break;
	case balancing:
	isValid = entry->foregroundFastStruct->blcAccessTree->pBranchComp.isValidSlot(&entry->foregroundFastStruct->blcAccessTree->pNodes[*idx].fsData,&freeSlot);
	break;
	default:
	break;
      }
      if(isValid)
      {
	entry2FsId[entry].push_back(make_pair(*exrepIt,*idx));
	availFsCount++;
      }
      else
      {
	// create an empty entry in the map if needed
	if(!entry2FsId.count(entry))
	entry2FsId[entry]=vector< pair<FileSystem::fsid_t,SchedTreeBase::tFastTreeIdx> >();
	// update the unavailable fs
	unavailableFs->push_back(*exrepIt);
      }
    }
  }

  // check there is enough available replicas
  if(availFsCount<nAccessReplicas)
  {
    returnCode = ENONET;
    goto cleanup;
  }

  // check if the forced replicas (if any) is available
  if(forcedFsId>0 && (std::find(unavailableFs->begin(), unavailableFs->end(), forcedFsId) != unavailableFs->end()) )
  {
    returnCode = ENONET;
    goto cleanup;
  }

  // we have multiple groups
  // compute their geolocation scores to the the available fsids (+things) having a replica
  {
    SchedTreeBase::tFastTreeIdx accesserNode = 0;
    FileSystem::fsid_t selectedFsId = 0;
    {
      // maps a geolocation scores (int) to all the file system having this geolocation scores
      map< unsigned , std::vector< FileSystem::fsid_t > > geoScore2Fs;
      vector<SchedTreeBase::tFastTreeIdx> accessedReplicasIdx(1);
      // find the closest tree node to the accesser
      accesserNode = entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(accesserGeotag.c_str());;
      for(auto entryIt = entry2FsId.begin(); entryIt != entry2FsId.end(); entryIt ++)
      {
	if(eos::common::Logging::gLogMask & LOG_DEBUG)
	{
	  char buffer[1024];
	  buffer[0]=0;
	  char *buf = buffer;
	  for(auto it = entryIt->second.begin(); it!= entryIt->second.end(); ++it)
	  buf += sprintf(buf,"%lu  ",(unsigned long)(it->second));
	  eos_static_debug("existing replicas indices in geotree -> %s", buffer);

	  buffer[0]=0;
	  buf = buffer;
	  for(auto it = entryIt->second.begin(); it!= entryIt->second.end(); ++it)
	  buf += sprintf(buf,"%s  ",(*entryIt->first->foregroundFastStruct->treeInfo)[it->second].fullGeotag.c_str());
	  eos_static_debug("existing replicas geotags in geotree -> %s", buffer);
	}

	// if there is no replica here (might happen if it's spotted as unavailable after the first pass)
	if(entryIt->second.empty())
	continue;

	// fill a vector with the indices of the replicas
	vector<SchedTreeBase::tFastTreeIdx> existingReplicasIdx(entryIt->second.size());
	for(size_t i = 0; i < entryIt->second.size(); i++)
	existingReplicasIdx[i] = entryIt->second[i].second;

	// pickup an access slot is this scheduling group
	accessedReplicasIdx.clear();
	unsigned char retCode = 0;
	switch(type)
	{
	  case regularRO:
	  retCode = accessReplicas(entryIt->first,1,&accessedReplicasIdx,accesserNode,&existingReplicasIdx,
	      entry->foregroundFastStruct->rOAccessTree,NULL,NULL,skipSaturatedAccess);
	  break;
	  case regularRW:
	  retCode = accessReplicas(entryIt->first,1,&accessedReplicasIdx,accesserNode,&existingReplicasIdx,
	      entry->foregroundFastStruct->rWAccessTree,NULL,NULL,skipSaturatedAccess);
	  break;
	  case draining:
	  retCode = accessReplicas(entryIt->first,1,&accessedReplicasIdx,accesserNode,&existingReplicasIdx,
	      entry->foregroundFastStruct->drnAccessTree,NULL,NULL,skipSaturatedDrnAccess);
	  break;
	  case balancing:
	  retCode = accessReplicas(entryIt->first,1,&accessedReplicasIdx,accesserNode,&existingReplicasIdx,
	      entry->foregroundFastStruct->blcAccessTree,NULL,NULL,skipSaturatedBlcAccess);
	  break;
	  default:
	  break;
	}
	if(!retCode) goto cleanup;

	const string &fsGeotag = (*entryIt->first->foregroundFastStruct->treeInfo)[*accessedReplicasIdx.begin()].fullGeotag;
	unsigned geoScore = 0;
	size_t kmax = min(accesserGeotag.length(),fsGeotag.length());
	for(size_t k=0; k<kmax; k++)
	{
	  if(accesserGeotag[k]!=fsGeotag[k])
	  break;
	  if(accesserGeotag[k]==':' && k+1 < kmax && accesserGeotag[k+1]==':')
	  geoScore++;
	}
	// if the box is unsaturated, give an advantage to this FS
	if(retCode == 2)
	{
	  geoScore+=100;
	  eos_static_debug("found unsaturated fs");
	}

	geoScore2Fs[geoScore].push_back(
	    (*entryIt->first->foregroundFastStruct->treeInfo)[*accessedReplicasIdx.begin()].fsId);
      }

      // randomly chose a fs among the highest scored ones
      selectedFsId = geoScore2Fs.rbegin()->second[rand()%geoScore2Fs.rbegin()->second.size()];

      // return the corresponding index
      for (it = existingReplicas->begin(); it != existingReplicas->end(); it++)
      {
	if(*it == selectedFsId)
	{
	  fsIndex = (eos::common::FileSystem::fsid_t) (it-existingReplicas->begin());
	  break;
	}
      }
    }

    if(eos::common::Logging::gLogMask & LOG_DEBUG)
    {
      char buffer[1024];
      buffer[0]=0;
      char *buf = buffer;
      for(auto it = existingReplicas->begin(); it!= existingReplicas->end(); ++it)
      buf += sprintf(buf,"%lu  ",(unsigned long)(*it));

      eos_static_debug("existing replicas fs id's -> %s", buffer);
      eos_static_debug("accesser closest node to %s index -> %d  /  %s",accesserGeotag.c_str(), (int)accesserNode,(*entry->foregroundFastStruct->treeInfo)[accesserNode].fullGeotag.c_str());
      eos_static_debug("selected FsId -> %d / idx %d", (int)selectedFsId,(int)fsIndex);
    }
  }

  // check we found it
  if(it == existingReplicas->end())
  {
    eos_err("inconsistency : unable to find the selected fs but it should be there");
    returnCode = EIO;
    goto cleanup;
  }

  {
    // fill the resulting vector
    // update the fastTree UlScore and DlScore by applying the penalties
    // ONLY FOR THE HEAD NODE (SHOULD TAKE FINAL NODES TOO?)
    const SchedTreeBase::tFastTreeIdx *idx;
    // apply the penalties
    entry->foregroundFastStruct->fs2TreeIdx->get((*existingReplicas)[fsIndex],idx);
    const char netSpeedClass = (*entry->foregroundFastStruct->treeInfo)[fsIndex].netSpeedClass;
    if(entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.dlScore>=accessDlScorePenalty[netSpeedClass])
    applyDlScorePenalty(entry,*idx,accessDlScorePenalty[netSpeedClass]);
    if(entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.ulScore>=accessUlScorePenalty[netSpeedClass])
    applyUlScorePenalty(entry,*idx,accessUlScorePenalty[netSpeedClass]);
  }

  // if we arrive here, it all ran fine
  returnCode = 0;

  // cleanup and exit
  cleanup:
  for(auto cit = entry2FsId.begin(); cit != entry2FsId.end(); cit++ )
  {
    cit->first->doubleBufferMutex.UnLockRead();
    AtomicDec(cit->first->fastStructLockWaitersCount);
  }
  return returnCode;
}

bool GeoTreeEngine::StartUpdater()
{
  if (XrdSysThread::Run(&pUpdaterTid, GeoTreeEngine::startFsChangeListener, static_cast<void *>(this),
	  XRDSYSTHREAD_HOLD, "GeoTreeEngine Updater"))
  {
    return false;
  }
  return true;
}

bool GeoTreeEngine::StopUpdater()
{
  XrdSysThread::Cancel(pUpdaterTid);
  XrdSysThread::Join(pUpdaterTid, 0);
  return true;
}

void* GeoTreeEngine::startFsChangeListener(void *pp)
{
  ((GeoTreeEngine*)pp)->listenFsChange();
  return 0;
}

void GeoTreeEngine::listenFsChange()
{
  gOFS->ObjectNotifier.BindCurrentThread("geotreeengine");

  if(!gOFS->ObjectNotifier.StartNotifyCurrentThread())
  eos_crit("error starting shared objects change notifications");
  else
  eos_info("GeoTreeEngine updater is starting...");

  struct timespec curtime,prevtime;
#ifdef CLOCK_MONOTONIC_COARSE
  // this version is faster, we use it if it's available
  clock_gettime(CLOCK_MONOTONIC_COARSE,&prevtime);
#else
  clock_gettime(CLOCK_MONOTONIC,&prevtime);
#endif
  curtime = prevtime;

  do
  {
    gOFS->ObjectNotifier.tlSubscriber->SubjectsSem.Wait();

    XrdSysThread::SetCancelOff();

    // to be sure that we won't try to access a removed fs
    pAddRmFsMutex.LockWrite();

    // we always take a lock to take something from the queue and then release it
    gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();

    // listens on modifications on filesystem objects
    while (gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.size())
    {
      XrdMqSharedObjectManager::Notification event;
      event = gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.front();
      gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.pop_front();

      string newsubject = event.mSubject.c_str();

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation)
      {
	// ---------------------------------------------------------------------
	// handle subject creation
	// ---------------------------------------------------------------------
	eos_warning("received creation on subject %s : don't know what to do with this!", newsubject.c_str());

	continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion)
      {
	// ---------------------------------------------------------------------
	// handle subject deletion
	// ---------------------------------------------------------------------
	eos_debug("received deletion on subject %s : the fs was removed from the GeoTreeEngine, skipping this update", newsubject.c_str());

	continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification)
      {
	// ---------------------------------------------------------------------
	// handle subject modification
	// ---------------------------------------------------------------------

	eos_debug("received modification on subject %s", newsubject.c_str());

	string key = newsubject;
	string queue = newsubject;
	size_t dpos = 0;
	if ((dpos = queue.find(";")) != string::npos)
	{
	  key.erase(0, dpos + 1);
	  queue.erase(dpos);
	}

	// store the notification for the next update
	if(gNotificationsBuffer.count(queue))
	{
	  gNotificationsBuffer[queue] |= gNotifKey2Enum.at(key);
	}
	else
	{
	  gNotificationsBuffer[queue] = gNotifKey2Enum.at(key);
	}

	continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectKeyDeletion)
      {
	// ---------------------------------------------------------------------
	// handle subject key deletion
	// ---------------------------------------------------------------------
	eos_warning("received subject deletion on subject %s : don't know what to do with this!", newsubject.c_str());

	continue;
      }
      eos_warning("msg=\"don't know what to do with subject\" subject=%s", newsubject.c_str());
      continue;
    }
    gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.UnLock();
    pAddRmFsMutex.UnLockWrite();
    // do the processing
#ifdef CLOCK_MONOTONIC_COARSE
    // this version is faster, we use it if it's available
    clock_gettime(CLOCK_MONOTONIC_COARSE,&curtime);
#else
    clock_gettime(CLOCK_MONOTONIC,&curtime);
#endif

    eos_static_debug("Updating Fast Structures at %ds. %dns. Previous update was at prev: %ds. %dns. Time elapsed since the last update is: %dms.",(int)curtime.tv_sec,(int)curtime.tv_nsec,(int)prevtime.tv_sec,(int)prevtime.tv_nsec,(int)curtime.tv_sec*1000+((int)curtime.tv_nsec)/1000000-(int)prevtime.tv_sec*1000-((int)prevtime.tv_nsec)/1000000);
    {
      checkPendingDeletions(); // do it before tree info to leave some time to the other threads
      {
	eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
	updateTreeInfo(gNotificationsBuffer);
      }
      prevtime = curtime;
      gNotificationsBuffer.clear();
    }
    XrdSysThread::SetCancelOff();
    XrdSysTimer::Wait(std::max(timeFrameDurationMs,100));
  }
  while (1);
}

bool GeoTreeEngine::updateTreeInfo(TreeMapEntry* entry, eos::common::FileSystem::fs_snapshot_t *fs, int keys, SchedTreeBase::tFastTreeIdx ftIdx , SlowTreeNode *stn)
{
  eos::common::RWMutexReadLock lock(configMutex); // we git a consistent set of configuration parameters per refresh of the state
  // nothing to update
  if((!ftIdx && !stn) || !keys)
  return true;

  struct timeval curtime;
  gettimeofday(&curtime, 0);
  if(fs->mHeartBeatTime)
  {
  latencyStats.last = (curtime.tv_sec-fs->mHeartBeatTime)*1000.0 + (curtime.tv_usec-fs->mHeartBeatTimeNs*0.001)*0.001;
  latencyStats.update();
  }

#define setOneStateVarInAllFastTrees(variable,value) \
		{ \
	entry->backgroundFastStruct->rOAccessTree->pNodes[ftIdx].fsData.variable = value; \
	entry->backgroundFastStruct->rWAccessTree->pNodes[ftIdx].fsData.variable = value; \
	entry->backgroundFastStruct->placementTree->pNodes[ftIdx].fsData.variable = value; \
	entry->backgroundFastStruct->drnAccessTree->pNodes[ftIdx].fsData.variable = value; \
	entry->backgroundFastStruct->drnPlacementTree->pNodes[ftIdx].fsData.variable = value; \
	entry->backgroundFastStruct->blcAccessTree->pNodes[ftIdx].fsData.variable = value; \
	entry->backgroundFastStruct->blcPlacementTree->pNodes[ftIdx].fsData.variable = value; \
		}

#define setOneStateVarStatusInAllFastTrees(flag) \
		{ \
	entry->backgroundFastStruct->rOAccessTree->pNodes[ftIdx].fsData.mStatus |= flag; \
	entry->backgroundFastStruct->rWAccessTree->pNodes[ftIdx].fsData.mStatus |= flag; \
	entry->backgroundFastStruct->placementTree->pNodes[ftIdx].fsData.mStatus |= flag; \
	entry->backgroundFastStruct->drnAccessTree->pNodes[ftIdx].fsData.mStatus |= flag; \
	entry->backgroundFastStruct->drnPlacementTree->pNodes[ftIdx].fsData.mStatus |= flag; \
	entry->backgroundFastStruct->blcAccessTree->pNodes[ftIdx].fsData.mStatus |= flag; \
	entry->backgroundFastStruct->blcPlacementTree->pNodes[ftIdx].fsData.mStatus |= flag; \
		}

#define unsetOneStateVarStatusInAllFastTrees(flag) \
		{ \
	entry->backgroundFastStruct->rOAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
	entry->backgroundFastStruct->rWAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
	entry->backgroundFastStruct->placementTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
	entry->backgroundFastStruct->drnAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
	entry->backgroundFastStruct->drnPlacementTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
	entry->backgroundFastStruct->blcAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
	entry->backgroundFastStruct->blcPlacementTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
		}

  if(keys&sfgGeotag)
  {
    // update the treenodeinfo
    string newGeoTag = fs->mGeoTag;
    FileSystem::fsid_t fsid = fs->mId;
    if(!fsid)
    {
      eos_err("could not get the FsId");
      return false;
    }
    entry->slowTreeMutex.LockWrite();
    if(!entry->fs2SlowTreeNode.count(fsid))
    {
      eos_err("could not get the slowtree node");
      entry->slowTreeMutex.UnLockWrite();
      return false;
    }
    SlowTreeNode *oldNode = entry->fs2SlowTreeNode[fsid];

    const string &oldGeoTag = oldNode->pNodeInfo.fullGeotag;
    eos_debug("geotag change detected : old geotag is %s   new geotag is %s",oldGeoTag.substr(0,oldGeoTag.rfind("::")).c_str(),newGeoTag.c_str());
    //CHECK IF CHANGE ACTUALLY HAPPENED BEFORE ACTUALLY CHANGING SOMETHING
    if(oldGeoTag.substr(0,oldGeoTag.rfind("::"))!=newGeoTag)
    { // do the change only if there is one
      SlowTreeNode *newNode = NULL;
      newNode = entry->slowTree->moveToNewGeoTag(oldNode,newGeoTag);
      if(!newNode)
      {
	eos_err("error changing geotag in slowtree");
	entry->slowTreeMutex.UnLockWrite();
	return false;
      }
      entry->slowTreeModified = true;
      entry->fs2SlowTreeNode[fsid] = newNode;
      // !!! change the argument too
      stn = newNode;
    }
    entry->slowTreeMutex.UnLockWrite();
  }
  if(keys&sfgId)
  {
    // should not happen
    //eos_crit("the FsId should not change once it's created:  new value is %lu",(unsigned long)fs->mId);
    // .... unless it is the first change to give to the id it's initial value. It happens after it's been created so it's seen as a change.
  }
  if(keys&(sfgBoot|sfgActive|sfgErrc))
  {
    FileSystem::fsstatus_t statboot = fs->mStatus;
    unsigned int errc = fs->mErrCode;

    FileSystem::fsactive_t statactive = fs->mActiveStatus;

    if( (statboot==FileSystem::kBooted) &&
	(errc == 0) &&		// this we probably don't need
	(statactive==FileSystem::kOnline)// this checks the heartbeat and the group & node are enabled
    )
    { // the fs is available
      eos_debug("fs %lu is getting available  ftidx=%d  stn=%p",(unsigned long) fs->mId,(int)ftIdx,stn);
      if(ftIdx) setOneStateVarStatusInAllFastTrees(SchedTreeBase::Available);
      if(stn) stn->pNodeState.mStatus |= SchedTreeBase::Available;
    }
    else
    { // the fs is unavailable
      eos_debug("fs %lu is getting unavailable ftidx=%d  stn=%p",(unsigned long) fs->mId,(int)ftIdx,stn);
      if(ftIdx) unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Available);
      if(stn) stn->pNodeState.mStatus &= ~SchedTreeBase::Available;
    }
  }
  if(keys&sfgConfigstatus)
  {
    FileSystem::fsstatus_t status = fs->mConfigStatus;
    if(status==FileSystem::kRW)
    {
      if(ftIdx) setOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable|SchedTreeBase::Writable);
      if(stn) stn->pNodeState.mStatus |= (SchedTreeBase::Readable|SchedTreeBase::Writable);
    }
    else if(status==FileSystem::kRO)
    {
      if(ftIdx)
      {
	setOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable);
	unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Writable);
      }
      if(stn)
      {
	stn->pNodeState.mStatus |= SchedTreeBase::Readable;
	stn->pNodeState.mStatus &= ~SchedTreeBase::Writable;
      }
    }
    else if(status==FileSystem::kWO)
    {
      if(ftIdx)
      {
	unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable);
	setOneStateVarStatusInAllFastTrees(SchedTreeBase::Writable);
      }
      if(stn)
      {
	stn->pNodeState.mStatus &= ~SchedTreeBase::Readable;
	stn->pNodeState.mStatus |= SchedTreeBase::Writable;
      }
    }
    else
    {
      if(ftIdx)
      {
	unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable);
	unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Writable);
      }
      if(stn)
      {
	stn->pNodeState.mStatus &= ~SchedTreeBase::Readable;
	stn->pNodeState.mStatus &= ~SchedTreeBase::Writable;
      }
    }
  }
  if(keys&sfgDrain)
  {
    FileSystem::fsstatus_t drainStatus = fs->mDrainStatus;
    switch(drainStatus)
    {
      case FileSystem::kNoDrain:
      case FileSystem::kDrainPrepare:
      case FileSystem::kDrainWait:
      case FileSystem::kDrainStalling:
      case FileSystem::kDrained:
      case FileSystem::kDrainExpired:
      case FileSystem::kDrainLostFiles:
      if(ftIdx) unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Draining);
      if(stn) stn->pNodeState.mStatus &= ~SchedTreeBase::Draining;
      // mark as unavailable for read/write
      break;
      case FileSystem::kDraining:
      // mark as draining
      if(ftIdx) setOneStateVarStatusInAllFastTrees(SchedTreeBase::Draining);
      if(stn) stn->pNodeState.mStatus |= SchedTreeBase::Draining;
      break;
    }
  }
  if(keys&sfgBalthres)
  {
    if(fs->mBalRunning)
    {
      if(ftIdx) setOneStateVarStatusInAllFastTrees(SchedTreeBase::Balancing);
      if(stn) stn->pNodeState.mStatus |= SchedTreeBase::Balancing;
    }
    else
    {
      if(ftIdx) unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Balancing);
      if(stn) stn->pNodeState.mStatus &= ~SchedTreeBase::Balancing;
    }
  }
  //	if(keys&sfgFsfilled)
  //	{
  //		//half fr = half(fs->mNominalFilled);
  //		float fr = float(fs->mNominalFilled);
  //		if(ftIdx) setOneStateVarInAllFastTrees(fillRatio,fr);
  //		if(stn) stn->pNodeState.fillRatio = fr;
  //	}

  if(keys&sfgBlkavailb)
  {
    float ts = float(fs->mDiskBfree * (double)fs->mDiskBsize );

    if(ftIdx) setOneStateVarInAllFastTrees(totalSpace,ts);
    if(stn) stn->pNodeState.totalSpace = ts;
  }
  size_t netSpeedClass = 0; // <1Gb/s -> 0 ; 1Gb/s -> 1; 10Gb/s->2 ; 100Gb/s->...etc
  if(keys&(sfgDiskload|sfgInratemib|sfgOutratemib|sfgEthmib))
  {
    netSpeedClass = round(log10(fs->mNetEthRateMiB*8 * 1024 * 1024 + 1));
    netSpeedClass = netSpeedClass>8 ? netSpeedClass-8 : 0; // netSpeedClass 1 means 1Gbps
    // check if netspeed calss need an update
    if(entry->backgroundFastStruct->treeInfo->size()>=netSpeedClass+1 &&
        (*entry->backgroundFastStruct->treeInfo)[ftIdx].netSpeedClass!=(unsigned char)netSpeedClass)
    {
      if(ftIdx) (*entry->backgroundFastStruct->treeInfo)[ftIdx].netSpeedClass = netSpeedClass;
      if(stn) stn->pNodeInfo.netSpeedClass = netSpeedClass;
    }

    nodeAgreg& na = updatingNodes[fs->mQueue];// this one will create the entry if it doesnt exists already
    na.fsCount++;
    if(!na.saturated)
    {
      if(na.fsCount ==1 )
      {
        na.netSpeedClass = netSpeedClass;
        maxNetSpeedClass = std::max( maxNetSpeedClass , netSpeedClass);
        na.netOutWeight += (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetOutRateMiB / fs->mNetEthRateMiB) : 0.0));
        na.netInWeight += (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetInRateMiB / fs->mNetEthRateMiB) : 0.0));
        if(na.netOutWeight<0.1 || na.netInWeight<0.1)
        na.saturated = true; // network of the box is saturated
      }
      na.rOpen += fs->mDiskRopen;
      na.wOpen += fs->mDiskWopen;
      na.diskUtilSum += fs->mDiskUtilization;
      if(fs->mDiskUtilization > 0.9 )
      na.saturated = true; // one of the disks of the box is saturated
    }
  }
  if(keys&(sfgDiskload|sfgInratemib))
  {
    // update the upload score
    double ulScore = (1-fs->mDiskUtilization);
    double netoutweight = (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetOutRateMiB / fs->mNetEthRateMiB) : 0.0));
    ulScore *= ((netoutweight > 0) ? sqrt(netoutweight) : 0);

    if(ftIdx) setOneStateVarInAllFastTrees(ulScore,(char)(ulScore*100));
    if(stn) stn->pNodeState.ulScore = ulScore*100;
  }
  if(keys&(sfgOutratemib|sfgDiskload|sfgReadratemb))
  {
    double dlScore = (1-fs->mDiskUtilization);
    double netinweight = (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetInRateMiB / fs->mNetEthRateMiB) : 0.0));
    dlScore *= ((netinweight > 0) ? sqrt(netinweight) : 0);

    if(ftIdx) setOneStateVarInAllFastTrees(dlScore,(char)(dlScore*100));
    if(stn) stn->pNodeState.dlScore = dlScore*100;
  }
  if(keys&sfgFsfilled)
  {
    if(ftIdx) setOneStateVarInAllFastTrees(fillRatio,(char)fs->mDiskFilled);
    if(stn) stn->pNodeState.fillRatio = (char)fs->mDiskFilled;
  }

  // SHOULD WE TAKE THE NOMINAL FILLING AS SET BY THE BALANCING?
  //	if(keys&(sfgNomfilled))
  //	{
  //		fs->
  //	}

  return true;
}

bool GeoTreeEngine::updateTreeInfo(const map<string,int> &updates)
{
  // copy the foreground FastStructures to the BackGround FastStructures
  // so that the penalties applied after the placement/access are kept by defaut
  // (and overwritten if a new state is received from the fs)
  pTreeMapMutex.LockRead();
  for(auto it = pGroup2TreeMapEntry.begin(); it != pGroup2TreeMapEntry.end(); it++ )
  {
    TreeMapEntry *entry = it->second;
    RWMutexReadLock lock(entry->slowTreeMutex);
    if(!entry->foregroundFastStruct->DeepCopyTo(entry->backgroundFastStruct))
    {
      eos_crit("error deep copying in double buffering");
      pTreeMapMutex.UnLockRead();
      return false;
    }
  }
  pTreeMapMutex.UnLockRead();

  updatingNodes.clear();
  maxNetSpeedClass = 0;
  for(auto it = updates.begin(); it != updates.end(); ++it)
  {

    gOFS->ObjectManager.HashMutex.LockRead();
    XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(it->first.c_str(), "hash");
    if(!hash)
    {
      eos_static_warning("Inconsistency : Trying to access a deleted fs. Should not happen because any reference to a fs is cleaned from the updates buffer ehen the fs is being removed.");
      gOFS->ObjectManager.HashMutex.UnLockRead();
      continue;
    }
    FileSystem::fsid_t fsid = (FileSystem::fsid_t) hash->GetLongLong("id");
    if(!fsid)
    {
      eos_static_warning("Inconsistency : Trying to update an unregistered fs. Should not happen.");
      gOFS->ObjectManager.HashMutex.UnLockRead();
      continue;
    }
    gOFS->ObjectManager.HashMutex.UnLockRead();

    if(!pFsId2FsPtr.count(fsid))
    {
      eos_static_warning("Inconsistency: Trying to access an existing fs which is not referenced in the GeoTreeEngine anymore");
      continue;
    }
    eos::common::FileSystem *filesystem = pFsId2FsPtr[fsid];

    eos::common::FileSystem::fs_snapshot_t fs;
    filesystem->SnapShotFileSystem(fs, true);

    pTreeMapMutex.LockRead();
    if(!pFs2TreeMapEntry.count(fsid))
    {
      eos_err("update : TreeEntryMap has been removed, skipping this update");
      pTreeMapMutex.UnLockRead();
      continue;
    }
    TreeMapEntry *entry = pFs2TreeMapEntry[fsid];
    AtomicInc(entry->fastStructLockWaitersCount);
    pTreeMapMutex.UnLockRead();

    eos_debug("CHANGE BITFIELD %x",it->second);

    // update only the fast structures because even if a fast structure rebuild is needed from the slow tree
    // its information and state is updated from the fast structures
    entry->doubleBufferMutex.LockRead();
    const SchedTreeBase::tFastTreeIdx *idx=NULL;
    SlowTreeNode *node=NULL;
    if( !entry->backgroundFastStruct->fs2TreeIdx->get(fsid,idx) )
    {
      auto nodeit = entry->fs2SlowTreeNode.find(fsid);
      if(nodeit == entry->fs2SlowTreeNode.end())
      {
	eos_crit("Inconsistency : cannot locate an fs %lu supposed to be in the fast structures",(unsigned long)fsid);
	entry->doubleBufferMutex.UnLockRead();
	AtomicDec(entry->fastStructLockWaitersCount);
	return false;
      }
      node = nodeit->second;
      eos_debug("no fast tree for fs %lu : updating slowtree",(unsigned long)fsid);
    }
    else
    {
      eos_debug("fast tree available for fs %lu : not updating slowtree",(unsigned long)fsid);
    }
    updateTreeInfo(entry, &fs, it->second, idx?*idx:0 , node);
    if(idx) entry->fastStructModified = true;
    if(node) entry->slowTreeModified = true;
    // if we update the slowtree, then a fast tree generation is already pending
    entry->doubleBufferMutex.UnLockRead();
    AtomicDec(entry->fastStructLockWaitersCount);
  }
  updatePenalties();

  // update the trees that need to be updated ( could maybe optimized by updating only the branch needing, might be worth it if only 1 or 2 branches are updated )
  // self update for the fast structure if update from slow tree is not needed
  // if convert from slowtree is needed, update the slowtree from the fast for the info and for the state
  pTreeMapMutex.LockRead();
  for(auto it = pGroup2TreeMapEntry.begin(); it != pGroup2TreeMapEntry.end(); it++ )
  {
    TreeMapEntry *entry = it->second;
    RWMutexReadLock lock(entry->slowTreeMutex);
    if(!updateFastStructures(entry))
    {
      pTreeMapMutex.UnLockRead();
      eos_err("updating the tree");
      return false;
    }
  }
  pTreeMapMutex.UnLockRead();

  return true;
}

bool GeoTreeEngine::getGroupsFromFsIds(const std::vector<FileSystem::fsid_t> fsids, std::vector<std::string> *fsgeotags, std::vector<FsGroup*> *sortedgroups)
{
  bool result = true;
  if(fsgeotags) fsgeotags->reserve(fsids.size());
  if(sortedgroups) sortedgroups->reserve(fsids.size());
  std::map<FsGroup*,size_t> group2idx;
  std::vector<std::pair<size_t,size_t> > groupcount;
  groupcount.reserve(fsids.size());
  {
    RWMutexReadLock lock(this->pTreeMapMutex);
    for(auto it = fsids.begin(); it != fsids.end(); ++ it)
    {
      if(pFs2TreeMapEntry.count(*it))
      {
	FsGroup *group = pFs2TreeMapEntry[*it]->group;
	if(fsgeotags)
	{
	  const SchedTreeBase::tFastTreeIdx *idx=NULL;
	  if(pFs2TreeMapEntry[*it]->foregroundFastStruct->fs2TreeIdx->get(*it,idx))
	  fsgeotags->push_back(
	      (*pFs2TreeMapEntry[*it]->foregroundFastStruct->treeInfo)[*idx].fullGeotag
	  );
	  else
	  fsgeotags->push_back("");
	}
	if(sortedgroups)
	{
	  if(!group2idx.count(group))
	  {
	    group2idx[group] = group2idx.size();
	    sortedgroups->push_back(group);
	    groupcount.push_back(make_pair(1,groupcount.size()));
	  }
	  else
	  {
	    size_t idx = group2idx[group];
	    groupcount[idx].first++;
	  }
	}
      }
      else
      {
	// put an empty entry in the result vector to preserve the indexing
	fsgeotags->push_back("");
	// to signal that one of the fsids was not mapped to a group
	result = false;
      }
    }
  }

  if(sortedgroups)
  {
    // sort the count vector in ascending order to get the permutation
    std::sort(groupcount.begin(),groupcount.end(),std::greater<std::pair<size_t,size_t>>());
    // apply the permutation
    std::vector<FsGroup*> final(groupcount.size());
    size_t count = 0;
    for(auto it = groupcount.begin(); it != groupcount.end(); it++)
    final[count++] = (*sortedgroups)[it->second];

    *sortedgroups = final;
  }
  return result;
}

void GeoTreeEngine::updatePenalties()
{
  // In this function, we compute a rought a simplified version
  // of the penalties applied to selected fs for placement and access.
  // there is only one penalty and it's copied to ulplct, dlplct, ulaccess and dlaccess
  // variants.

  // if the update is enabled
  if(penaltyUpdateRate)
  {
    if(updatingNodes.empty())
    {
      //eos_static_debug("updatingNodes is empty!");
    }
    else
    {
      // each networking speed class has its own variables
      std::vector<double>
      ropen(maxNetSpeedClass+1,0.0),
      wopen(maxNetSpeedClass+1,0.0),
      ulload(maxNetSpeedClass+1,0.0),
      dlload(maxNetSpeedClass+1,0.0),
      fscount(maxNetSpeedClass+1,0.0),
      hostcount(maxNetSpeedClass+1,0.0),
      diskutil(maxNetSpeedClass+1,0.0);

      // we use the view to check that we have all the fs in a node
      // could be removed if we were sure to run a single on fst daemon / box
      FsView::gFsView.ViewMutex.LockRead();
      for( auto it = updatingNodes.begin(); it!= updatingNodes.end(); it++)
      {
        const std::string &nodestr = it->first;
        FsNode *node = NULL;
        if(FsView::gFsView.mNodeView.count(nodestr))
        node = FsView::gFsView.mNodeView[nodestr];
        else
        {
          std::stringstream ss;
          ss.str("");
          for (auto it2 = FsView::gFsView.mNodeView.begin(); it2 != FsView::gFsView.mNodeView.end(); it2++)
          ss << it2->first << "  ";
          eos_static_err("Inconsistency : cannot find updating node %s in %s",nodestr.c_str(),ss.str().c_str());
          continue;
        }
        if((!it->second.saturated) && it->second.fsCount == node->size())
        {
//          eos_static_debug("aggregated opened files for %s : wopen %d   ropen %d   outweight %lf   inweight %lf",
//              it->first.c_str(),it->second.wOpen,it->second.rOpen,it->second.netOutWeight,it->second.netInWeight);
        }
        else
        {
          // the fs/host is saturated, we don't use the whole host in the estimate
          if(it->second.saturated)
          eos_static_debug("fs update in node %s : box is saturated");
          // there is a mismatch between
          // this should not happen if only one single fst daemon is running on each fst node
          if(it->second.fsCount != node->size())
          eos_static_notice("fs update in node %s : %d fs in FsView vs %d fs in update. This probably means that several fst daemons are running on the same host",it->first.c_str(),(int)node->size(),(int)it->second.fsCount);
// could force to get everything
//          long long wopen = node->SumLongLong("stat.wopen",false);
//          long long ropen = node->SumLongLong("stat.ropen",false);
        }
        // update aggregated informations for the right networking class
        ropen[it->second.netSpeedClass]+=(it->second.rOpen);
        wopen[it->second.netSpeedClass]+=(it->second.wOpen);
        ulload[it->second.netSpeedClass]+=(1.0-it->second.netOutWeight);
        dlload[it->second.netSpeedClass]+=(1.0-it->second.netInWeight);
        diskutil[it->second.netSpeedClass]+=it->second.diskUtilSum;
        fscount[it->second.netSpeedClass]+=it->second.fsCount;
        hostcount[it->second.netSpeedClass]++;
      }
      FsView::gFsView.ViewMutex.UnLockRead();

      for(size_t netSpeedClass=0; netSpeedClass<=maxNetSpeedClass; netSpeedClass++)
      {
        if(ropen[netSpeedClass]+ropen[netSpeedClass]>4)
        {
          eos_static_debug("UPDATE netSpeedClass=%d  ulload=%lf  dlload=%lf  diskutil=%lf  ropen=%lf  wopen=%lf  fscount=%lf  hostcount=%lf",
              (int)netSpeedClass, ulload[netSpeedClass], dlload[netSpeedClass],diskutil[netSpeedClass], ropen[netSpeedClass],
              wopen[netSpeedClass], fscount[netSpeedClass], hostcount[netSpeedClass]);

          // the penalty aims at knowing roughly how many file concurrent file operations
          // can be led on a single fs before sturating a ressource (disk or network)

          // network penalty per file
          // the multiplication by the number of fs is to take into account
          // that the bw is shared between multiple fs
          double networkpen =
          0.5*(ulload[netSpeedClass]+dlload[netSpeedClass])/(ropen[netSpeedClass]+wopen[netSpeedClass])
          *(fscount[netSpeedClass]/hostcount[netSpeedClass]);

          // there is factor to take into account the read cache
          // TODO use a realistic value for this factor
          double diskpen =
          diskutil[netSpeedClass]/(0.4*ropen[netSpeedClass]+wopen[netSpeedClass]);

          eos_static_debug("penalties updates are network %lf   disk %lf",networkpen,diskpen);

          double update = 100*std::max(diskpen,networkpen);

          if(update<1 || update>99) // could be more restrictive
          {
            eos_static_debug("weird value for accessDlScorePenalty update : %lf. Not using this one.",update);
          }
          else
          {
            eos_static_debug("netSpeedClass %d : using update values %lf for penalties with weight %f%%",
                netSpeedClass, penaltyUpdateRate);
            eos_static_debug("netSpeedClass %d : values before update are accessDlScorePenalty=%f  plctDlScorePenalty=%f  accessUlScorePenalty=%f  plctUlScorePenalty=%f",
                netSpeedClass, accessDlScorePenaltyF[netSpeedClass],plctDlScorePenaltyF[netSpeedClass],accessUlScorePenaltyF[netSpeedClass],plctUlScorePenaltyF[netSpeedClass]);
            union
            {
              float f;
              uint32_t u;
            }uf;

            // atomic change, no need to lock anything
            uf.f = 0.01*( ( 100 - penaltyUpdateRate)*accessDlScorePenaltyF[netSpeedClass] + penaltyUpdateRate*update);
            AtomicCAS( reinterpret_cast<uint32_t&>(accessDlScorePenaltyF[netSpeedClass]) , reinterpret_cast<uint32_t&>(accessDlScorePenaltyF[netSpeedClass]) , uf.u );
            uf.f = 0.01*( ( 100 - penaltyUpdateRate)*plctDlScorePenaltyF[netSpeedClass] + penaltyUpdateRate*update);
            AtomicCAS( reinterpret_cast<uint32_t&>(plctDlScorePenaltyF[netSpeedClass]) , reinterpret_cast<uint32_t&>(plctDlScorePenaltyF[netSpeedClass]) , uf.u);
            uf.f = 0.01*( ( 100 - penaltyUpdateRate)*accessUlScorePenaltyF[netSpeedClass] + penaltyUpdateRate*update);
            AtomicCAS( reinterpret_cast<uint32_t&>(accessUlScorePenaltyF[netSpeedClass]) , reinterpret_cast<uint32_t&>(accessUlScorePenaltyF[netSpeedClass]) , uf.u);
            uf.f = 0.01*( ( 100 - penaltyUpdateRate)*plctUlScorePenaltyF[netSpeedClass] + penaltyUpdateRate*update);
            AtomicCAS( reinterpret_cast<uint32_t&>(plctUlScorePenaltyF[netSpeedClass]) , reinterpret_cast<uint32_t&>(plctUlScorePenaltyF[netSpeedClass]) , uf.u);
            eos_static_debug("netSpeedClass %d : values after update are accessDlScorePenalty=%f  plctDlScorePenalty=%f  accessUlScorePenalty=%f  plctUlScorePenalty=%f",
                netSpeedClass, accessDlScorePenaltyF[netSpeedClass],plctDlScorePenaltyF[netSpeedClass],accessUlScorePenaltyF[netSpeedClass],plctUlScorePenaltyF[netSpeedClass]);
            // update the casted versions too
            AtomicCAS( plctUlScorePenalty[netSpeedClass], plctUlScorePenalty[netSpeedClass], (SchedTreeBase::tFastTreeIdx) plctUlScorePenaltyF[netSpeedClass]);
            AtomicCAS( plctDlScorePenalty[netSpeedClass], plctDlScorePenalty[netSpeedClass], (SchedTreeBase::tFastTreeIdx) plctDlScorePenaltyF[netSpeedClass]);
            AtomicCAS( accessDlScorePenalty[netSpeedClass], accessDlScorePenalty[netSpeedClass], (SchedTreeBase::tFastTreeIdx) accessDlScorePenaltyF[netSpeedClass]);
            AtomicCAS( accessUlScorePenalty[netSpeedClass], accessUlScorePenalty[netSpeedClass], (SchedTreeBase::tFastTreeIdx) accessUlScorePenaltyF[netSpeedClass]);
          }
        }
        else
        {
          eos_static_debug("not enough file opened to get reliable statistics %d",(int)(ropen[netSpeedClass]+ropen[netSpeedClass]));
        }
      }
    }

  }


// *************************************************************************************
// The following code was intended to test some estimation techniques for the penatlies.
// They are aiming at something more precise than the implementation above
// More precision does not seem necessary for the moment
// The code is left there in case of need for later use
// *************************************************************************************
//void GeoTreeEngine::updatePenalties()
//{
//  class LS2dSolver
//  {
//    typedef std::vector<double> dvec;
//
//    int c__1;
//    int c__0;
//    int c__2;
//
//    static double innerProd( const dvec &v1, const dvec &v2 )
//    {
//      assert(v1.size() == v2.size() );
//      double ret = 0;
//      for(size_t i=0; i<v1.size(); i++)
//      ret+=v1[i]*v2[i];
//      return ret;
//    }
//  public:
//    LS2dSolver() : c__1(1),c__0(0),c__2(2) {}
//    static bool solve( const dvec &a, const dvec &b, const dvec&c, dvec&result)
//    {
//      double ab = innerProd(a,b);
//      double a2 = innerProd(a,a);
//      double b2 = innerProd(b,b);
//      double sa2b2 = sqrt(a2*b2);
//      double ac = innerProd(a,c);
//      double bc = innerProd(b,c);
//
//      char buffa[4096],buffb[4096],buffc[4096];
//      char *pta = (char*)buffa; pta[0]=0;
//      char *ptb = (char*)buffb; ptb[0]=0;
//      char *ptc = (char*)buffc; ptb[0]=0;
//
//      for(auto it = a.begin();it != a.end(); it++)
//      pta += sprintf(pta,"%lf , ",*it);
//
//      for(auto it = b.begin();it != b.end(); it++)
//      ptb += sprintf(ptb,"%lf , ",*it);
//
//      for(auto it = c.begin();it != c.end(); it++)
//      ptc += sprintf(ptc,"%lf , ",*it);
//
//      eos_static_info(" a = %s",buffa);
//      eos_static_info(" b = %s",buffb);
//      eos_static_info(" c = %s",buffc);
//      eos_static_info(" ab = %lf , a2 = %lf , b2 = %lf , sa2b2 = %lf , ac = %lf , bc = %lf",ab,a2,b2,sa2b2,ac,bc);
//
//      double K = ( ab - sa2b2 ) * ( ab + sa2b2);
//
//      eos_static_info(" K = %lf",K);
//
//      if( abs(K) < sqrt(std::numeric_limits<double>::epsilon()) )
//      return false;
//
//      K = 1.0/K;
//
//      result=
//      {
//        (-b2*ac + ab*bc) * K,
//        ( ab*ac - a2*bc) * K
//      };
//
//      return true;
//    }
//
//    /*     SUBROUTINE NNLS  (A,MDA,M,N,B,X,RNORM,W,ZZ,INDEX,MODE) */
//
//#define nnls_max(a,b) ((a) >= (b) ? (a) : (b))
//#define nnls_abs(x) ((x) >= 0 ? (x) : -(x))
//
//    /* The following subroutine was added after the f2c translation */
//    double d_sign(double *a, double *b)
//    {
//      double x;
//      x = (*a >= 0 ? *a : - *a);
//      return( *b >= 0 ? x : -x);
//    }
//
//    /* Subroutine */ int g1_(
//    double *a, double *b, double *cterm, double *sterm, double *sig)
//    {
//        /* System generated locals */
//        double d__1;
//
//        /* Builtin functions */
//        /* The following line was commented out after the f2c translation */
//        /* double sqrt(), d_sign(); */
//
//        /* Local variables */
//        static double xr, yr;
//
//
//    /*     COMPUTE ORTHOGONAL ROTATION MATRIX.. */
//
//    /*  The original version of this code was developed by */
//    /*  Charles L. Lawson and Richard J. Hanson at Jet Propulsion Laboratory
//    */
//    /*  1973 JUN 12, and published in the book */
//    /*  "SOLVING LEAST SQUARES PROBLEMS", Prentice-HalL, 1974. */
//    /*  Revised FEB 1995 to accompany reprinting of the book by SIAM. */
//
//    /*     COMPUTE.. MATRIX   (C, S) SO THAT (C, S)(A) = (SQRT(A**2+B**2)) */
//    /*                        (-S,C)         (-S,C)(B)   (   0          ) */
//    /*     COMPUTE SIG = SQRT(A**2+B**2) */
//    /*        SIG IS COMPUTED LAST TO ALLOW FOR THE POSSIBILITY THAT */
//    /*        SIG MAY BE IN THE SAME LOCATION AS A OR B . */
//    /*     ------------------------------------------------------------------
//    */
//    /*     ------------------------------------------------------------------
//    */
//        if (nnls_abs(*a) > nnls_abs(*b)) {
//            xr = *b / *a;
//    /* Computing 2nd power */
//            d__1 = xr;
//            yr = sqrt(d__1 * d__1 + 1.);
//            d__1 = 1. / yr;
//            *cterm = d_sign(&d__1, a);
//            *sterm = *cterm * xr;
//            *sig = nnls_abs(*a) * yr;
//            return 0;
//        }
//        if (*b != 0.) {
//            xr = *a / *b;
//    /* Computing 2nd power */
//            d__1 = xr;
//            yr = sqrt(d__1 * d__1 + 1.);
//            d__1 = 1. / yr;
//            *sterm = d_sign(&d__1, b);
//            *cterm = *sterm * xr;
//            *sig = nnls_abs(*b) * yr;
//            return 0;
//        }
//        *sig = 0.;
//        *cterm = 0.;
//        *sterm = 1.;
//        return 0;
//    } /* g1_ */
//
//
//    double diff_(double *x, double *y)
//    {
//        /* System generated locals */
//        double ret_val;
//
//
//    /*  Function used in tests that depend on machine precision. */
//
//    /*  The original version of this code was developed by */
//    /*  Charles L. Lawson and Richard J. Hanson at Jet Propulsion Laboratory
//    */
//    /*  1973 JUN 7, and published in the book */
//    /*  "SOLVING LEAST SQUARES PROBLEMS", Prentice-HalL, 1974. */
//    /*  Revised FEB 1995 to accompany reprinting of the book by SIAM. */
//
//        ret_val = *x - *y;
//        return ret_val;
//    } /* diff_ */
//
//
//    /*     SUBROUTINE H12 (MODE,LPIVOT,L1,M,U,IUE,UP,C,ICE,ICV,NCV) */
//
//     /*  CONSTRUCTION AND/OR APPLICATION OF A SINGLE */
//     /*  HOUSEHOLDER TRANSFORMATION..     Q = I + U*(U**T)/B */
//
//     /*  The original version of this code was developed by */
//     /*  Charles L. Lawson and Richard J. Hanson at Jet Propulsion Laboratory */
//     /*  1973 JUN 12, and published in the book */
//     /*  "SOLVING LEAST SQUARES PROBLEMS", Prentice-HalL, 1974. */
//     /*  Revised FEB 1995 to accompany reprinting of the book by SIAM. */
//     /*     ------------------------------------------------------------------ */
//     /*                     Subroutine Arguments */
//
//     /*     MODE   = 1 OR 2   Selects Algorithm H1 to construct and apply a */
//     /*            Householder transformation, or Algorithm H2 to apply a */
//     /*            previously constructed transformation. */
//     /*     LPIVOT IS THE INDEX OF THE PIVOT ELEMENT. */
//     /*     L1,M   IF L1 .LE. M   THE TRANSFORMATION WILL BE CONSTRUCTED TO */
//     /*            ZERO ELEMENTS INDEXED FROM L1 THROUGH M.   IF L1 GT. M */
//     /*            THE SUBROUTINE DOES AN IDENTITY TRANSFORMATION. */
//     /*     U(),IUE,UP    On entry with MODE = 1, U() contains the pivot */
//     /*            vector.  IUE is the storage increment between elements. */
//     /*            On exit when MODE = 1, U() and UP contain quantities */
//     /*            defining the vector U of the Householder transformation. */
//     /*            on entry with MODE = 2, U() and UP should contain */
//     /*            quantities previously computed with MODE = 1.  These will */
//     /*            not be modified during the entry with MODE = 2. */
//     /*     C()    ON ENTRY with MODE = 1 or 2, C() CONTAINS A MATRIX WHICH */
//     /*            WILL BE REGARDED AS A SET OF VECTORS TO WHICH THE */
//     /*            HOUSEHOLDER TRANSFORMATION IS TO BE APPLIED. */
//     /*            ON EXIT C() CONTAINS THE SET OF TRANSFORMED VECTORS. */
//     /*     ICE    STORAGE INCREMENT BETWEEN ELEMENTS OF VECTORS IN C(). */
//     /*     ICV    STORAGE INCREMENT BETWEEN VECTORS IN C(). */
//     /*     NCV    NUMBER OF VECTORS IN C() TO BE TRANSFORMED. IF NCV .LE. 0 */
//     /*            NO OPERATIONS WILL BE DONE ON C(). */
//     /*     ------------------------------------------------------------------ */
//     /* Subroutine */ int h12_(
//     int *mode, int*lpivot, int*l1, int*m,
//     double *u,
//     int *iue,
//     double *up, double *c__,
//     int *ice, int *icv, int *ncv)
//     {
//         /* System generated locals */
//         int u_dim1, u_offset, i__1, i__2;
//         double d__1, d__2;
//
//         /* Builtin functions */
//         /* The following line was commented out after the f2c translation */
//         /* double sqrt(); */
//
//         /* Local variables */
//         static int incr;
//         static double b;
//         static int i__, j;
//         static double clinv;
//         static int i2, i3, i4;
//         static double cl, sm;
//
//     /*     ------------------------------------------------------------------
//     */
//     /*     double precision U(IUE,M) */
//     /*     ------------------------------------------------------------------
//     */
//         /* Parameter adjustments */
//         u_dim1 = *iue;
//         u_offset = u_dim1 + 1;
//         u -= u_offset;
//         --c__;
//
//         /* Function Body */
//         if (0 >= *lpivot || *lpivot >= *l1 || *l1 > *m) {
//             return 0;
//         }
//         cl = (d__1 = u[*lpivot * u_dim1 + 1], nnls_abs(d__1));
//         if (*mode == 2) {
//             goto L60;
//         }
//     /*                            ****** CONSTRUCT THE TRANSFORMATION. ******
//     */
//         i__1 = *m;
//         for (j = *l1; j <= i__1; ++j) {
//     /* L10: */
//     /* Computing MAX */
//             d__2 = (d__1 = u[j * u_dim1 + 1], nnls_abs(d__1));
//             cl = nnls_max(d__2,cl);
//         }
//         if (cl <= 0.) {
//             goto L130;
//         } else {
//             goto L20;
//         }
//     L20:
//         clinv = 1. / cl;
//     /* Computing 2nd power */
//         d__1 = u[*lpivot * u_dim1 + 1] * clinv;
//         sm = d__1 * d__1;
//         i__1 = *m;
//         for (j = *l1; j <= i__1; ++j) {
//     /* L30: */
//     /* Computing 2nd power */
//             d__1 = u[j * u_dim1 + 1] * clinv;
//             sm += d__1 * d__1;
//         }
//         cl *= sqrt(sm);
//         if (u[*lpivot * u_dim1 + 1] <= 0.) {
//             goto L50;
//         } else {
//             goto L40;
//         }
//     L40:
//         cl = -cl;
//     L50:
//         *up = u[*lpivot * u_dim1 + 1] - cl;
//         u[*lpivot * u_dim1 + 1] = cl;
//         goto L70;
//     /*            ****** APPLY THE TRANSFORMATION  I+U*(U**T)/B  TO C. ******
//     */
//
//     L60:
//         if (cl <= 0.) {
//             goto L130;
//         } else {
//             goto L70;
//         }
//     L70:
//         if (*ncv <= 0) {
//             return 0;
//         }
//         b = *up * u[*lpivot * u_dim1 + 1];
//     /*                       B  MUST BE NONPOSITIVE HERE.  IF B = 0., RETURN.
//     */
//
//         if (b >= 0.) {
//             goto L130;
//         } else {
//             goto L80;
//         }
//     L80:
//         b = 1. / b;
//         i2 = 1 - *icv + *ice * (*lpivot - 1);
//         incr = *ice * (*l1 - *lpivot);
//         i__1 = *ncv;
//         for (j = 1; j <= i__1; ++j) {
//             i2 += *icv;
//             i3 = i2 + incr;
//             i4 = i3;
//             sm = c__[i2] * *up;
//             i__2 = *m;
//             for (i__ = *l1; i__ <= i__2; ++i__) {
//                 sm += c__[i3] * u[i__ * u_dim1 + 1];
//     /* L90: */
//                 i3 += *ice;
//             }
//             if (sm != 0.) {
//                 goto L100;
//             } else {
//                 goto L120;
//             }
//     L100:
//             sm *= b;
//             c__[i2] += sm * *up;
//             i__2 = *m;
//             for (i__ = *l1; i__ <= i__2; ++i__) {
//                 c__[i4] += sm * u[i__ * u_dim1 + 1];
//     /* L110: */
//                 i4 += *ice;
//             }
//     L120:
//             ;
//         }
//     L130:
//         return 0;
//     } /* h12_ */
//
//
//    /*  Algorithm NNLS: NONNEGATIVE LEAST SQUARES */
//
//    /*  The original version of this code was developed by */
//    /*  Charles L. Lawson and Richard J. Hanson at Jet Propulsion Laboratory */
//    /*  1973 JUN 15, and published in the book */
//    /*  "SOLVING LEAST SQUARES PROBLEMS", Prentice-HalL, 1974. */
//    /*  Revised FEB 1995 to accompany reprinting of the book by SIAM. */
//
//    /*     GIVEN AN M BY N MATRIX, A, AND AN M-VECTOR, B,  COMPUTE AN */
//    /*     N-VECTOR, X, THAT SOLVES THE LEAST SQUARES PROBLEM */
//
//    /*                      A * X = B  SUBJECT TO X .GE. 0 */
//    /*     ------------------------------------------------------------------ */
//    /*                     Subroutine Arguments */
//
//    /*     A(),MDA,M,N     MDA IS THE FIRST DIMENSIONING PARAMETER FOR THE */
//    /*                     ARRAY, A().   ON ENTRY A() CONTAINS THE M BY N */
//    /*                     MATRIX, A.           ON EXIT A() CONTAINS */
//    /*                     THE PRODUCT MATRIX, Q*A , WHERE Q IS AN */
//    /*                     M BY M ORTHOGONAL MATRIX GENERATED IMPLICITLY BY */
//    /*                     THIS SUBROUTINE. */
//    /*     B()     ON ENTRY B() CONTAINS THE M-VECTOR, B.   ON EXIT B() CON- */
//    /*             TAINS Q*B. */
//    /*     X()     ON ENTRY X() NEED NOT BE INITIALIZED.  ON EXIT X() WILL */
//    /*             CONTAIN THE SOLUTION VECTOR. */
//    /*     RNORM   ON EXIT RNORM CONTAINS THE EUCLIDEAN NORM OF THE */
//    /*             RESIDUAL VECTOR. */
//    /*     W()     AN N-ARRAY OF WORKING SPACE.  ON EXIT W() WILL CONTAIN */
//    /*             THE DUAL SOLUTION VECTOR.   W WILL SATISFY W(I) = 0. */
//    /*             FOR ALL I IN SET P  AND W(I) .LE. 0. FOR ALL I IN SET Z */
//    /*     ZZ()     AN M-ARRAY OF WORKING SPACE. */
//    /*     INDEX()     AN int WORKING ARRAY OF LENGTH AT LEAST N. */
//    /*                 ON EXIT THE CONTENTS OF THIS ARRAY DEFINE THE SETS */
//    /*                 P AND Z AS FOLLOWS.. */
//
//    /*                 INDEX(1)   THRU INDEX(NSETP) = SET P. */
//    /*                 INDEX(IZ1) THRU INDEX(IZ2)   = SET Z. */
//    /*                 IZ1 = NSETP + 1 = NPP1 */
//    /*                 IZ2 = N */
//    /*     MODE    THIS IS A SUCCESS-FAILURE FLAG WITH THE FOLLOWING */
//    /*             MEANINGS. */
//    /*             1     THE SOLUTION HAS BEEN COMPUTED SUCCESSFULLY. */
//    /*             2     THE DIMENSIONS OF THE PROBLEM ARE BAD. */
//    /*                   EITHER M .LE. 0 OR N .LE. 0. */
//    /*             3    ITERATION COUNT EXCEEDED.  MORE THAN 3*N ITERATIONS. */
//
//    /*     ------------------------------------------------------------------ */
//    /* Subroutine */ int nnls_(
//    double *a,
//    int *mda, int *m, int*n,
//    double *b, double*x, double*rnorm, double*w, double *zz,
//    int *index, int *mode)
//    {
//        /* System generated locals */
//        int a_dim1, a_offset, i__1, i__2;
//        double d__1, d__2;
//
//        /* Builtin functions */
//        /* The following lines were commented out after the f2c translation */
//        /* double sqrt(); */
//        /* int s_wsfe(), do_fio(), e_wsfe(); */
//
//        /* Local variables */
//        //extern double diff_();
//        static int iter;
//        static double temp, wmax;
//        static int i__, j, l;
//        static double t, alpha, asave;
//        static int itmax, izmax, nsetp;
//        //extern /* Subroutine */ int g1_();
//        static double dummy, unorm, ztest, cc;
//        //extern /* Subroutine */ int h12_();
//        static int ii, jj, ip;
//        static double sm;
//        static int iz, jz;
//        static double up, ss;
//        static int rtnkey, iz1, iz2, npp1;
//
//        /* Fortran I/O blocks */
//        /* The following line was commented out after the f2c translation */
//        /* static cilist io___22 = { 0, 6, 0, "(/a)", 0 }; */
//
//
//    /*     ------------------------------------------------------------------
//    */
//    /*     int INDEX(N) */
//    /*     double precision A(MDA,N), B(M), W(N), X(N), ZZ(M) */
//    /*     ------------------------------------------------------------------
//    */
//        /* Parameter adjustments */
//        a_dim1 = *mda;
//        a_offset = a_dim1 + 1;
//        a -= a_offset;
//        --b;
//        --x;
//        --w;
//        --zz;
//        --index;
//
//        /* Function Body */
//        *mode = 1;
//        if (*m <= 0 || *n <= 0) {
//            *mode = 2;
//            return 0;
//        }
//        iter = 0;
//        itmax = *n * 3;
//
//    /*                    INITIALIZE THE ARRAYS INDEX() AND X(). */
//
//        i__1 = *n;
//        for (i__ = 1; i__ <= i__1; ++i__) {
//            x[i__] = 0.;
//    /* L20: */
//            index[i__] = i__;
//        }
//
//        iz2 = *n;
//        iz1 = 1;
//        nsetp = 0;
//        npp1 = 1;
//    /*                             ******  MAIN LOOP BEGINS HERE  ****** */
//    L30:
//    /*                  QUIT IF ALL COEFFICIENTS ARE ALREADY IN THE SOLUTION.
//    */
//    /*                        OR IF M COLS OF A HAVE BEEN TRIANGULARIZED. */
//
//        if (iz1 > iz2 || nsetp >= *m) {
//            goto L350;
//        }
//
//    /*         COMPUTE COMPONENTS OF THE DUAL (NEGATIVE GRADIENT) VECTOR W().
//    */
//
//        i__1 = iz2;
//        for (iz = iz1; iz <= i__1; ++iz) {
//            j = index[iz];
//            sm = 0.;
//            i__2 = *m;
//            for (l = npp1; l <= i__2; ++l) {
//    /* L40: */
//                sm += a[l + j * a_dim1] * b[l];
//            }
//            w[j] = sm;
//    /* L50: */
//        }
//    /*                                   FIND LARGEST POSITIVE W(J). */
//    L60:
//        wmax = 0.;
//        i__1 = iz2;
//        for (iz = iz1; iz <= i__1; ++iz) {
//            j = index[iz];
//            if (w[j] > wmax) {
//                wmax = w[j];
//                izmax = iz;
//            }
//    /* L70: */
//        }
//
//    /*             IF WMAX .LE. 0. GO TO TERMINATION. */
//    /*             THIS INDICATES SATISFACTION OF THE KUHN-TUCKER CONDITIONS.
//    */
//
//        if (wmax <= 0.) {
//            goto L350;
//        }
//        iz = izmax;
//        j = index[iz];
//
//    /*     THE SIGN OF W(J) IS OK FOR J TO BE MOVED TO SET P. */
//    /*     BEGIN THE TRANSFORMATION AND CHECK NEW DIAGONAL ELEMENT TO AVOID */
//    /*     NEAR LINEAR DEPENDENCE. */
//
//        asave = a[npp1 + j * a_dim1];
//        i__1 = npp1 + 1;
//        h12_(&c__1, &npp1, &i__1, m, &a[j * a_dim1 + 1], &c__1, &up, &dummy, &
//                c__1, &c__1, &c__0);
//        unorm = 0.;
//        if (nsetp != 0) {
//            i__1 = nsetp;
//            for (l = 1; l <= i__1; ++l) {
//    /* L90: */
//    /* Computing 2nd power */
//                d__1 = a[l + j * a_dim1];
//                unorm += d__1 * d__1;
//            }
//        }
//        unorm = sqrt(unorm);
//        d__2 = unorm + (d__1 = a[npp1 + j * a_dim1], nnls_abs(d__1)) * .01;
//        if (diff_(&d__2, &unorm) > 0.) {
//
//    /*        COL J IS SUFFICIENTLY INDEPENDENT.  COPY B INTO ZZ, UPDATE Z
//    Z */
//    /*        AND SOLVE FOR ZTEST ( = PROPOSED NEW VALUE FOR X(J) ). */
//
//            i__1 = *m;
//            for (l = 1; l <= i__1; ++l) {
//    /* L120: */
//                zz[l] = b[l];
//            }
//            i__1 = npp1 + 1;
//            h12_(&c__2, &npp1, &i__1, m, &a[j * a_dim1 + 1], &c__1, &up, &zz[1], &
//                    c__1, &c__1, &c__1);
//            ztest = zz[npp1] / a[npp1 + j * a_dim1];
//
//    /*                                     SEE IF ZTEST IS POSITIVE */
//
//            if (ztest > 0.) {
//                goto L140;
//            }
//        }
//
//    /*     REJECT J AS A CANDIDATE TO BE MOVED FROM SET Z TO SET P. */
//    /*     RESTORE A(NPP1,J), SET W(J)=0., AND LOOP BACK TO TEST DUAL */
//    /*     COEFFS AGAIN. */
//
//        a[npp1 + j * a_dim1] = asave;
//        w[j] = 0.;
//        goto L60;
//
//    /*     THE INDEX  J=INDEX(IZ)  HAS BEEN SELECTED TO BE MOVED FROM */
//    /*     SET Z TO SET P.    UPDATE B,  UPDATE INDICES,  APPLY HOUSEHOLDER */
//    /*     TRANSFORMATIONS TO COLS IN NEW SET Z,  ZERO SUBDIAGONAL ELTS IN */
//    /*     COL J,  SET W(J)=0. */
//
//    L140:
//        i__1 = *m;
//        for (l = 1; l <= i__1; ++l) {
//    /* L150: */
//            b[l] = zz[l];
//        }
//
//        index[iz] = index[iz1];
//        index[iz1] = j;
//        ++iz1;
//        nsetp = npp1;
//        ++npp1;
//
//        if (iz1 <= iz2) {
//            i__1 = iz2;
//            for (jz = iz1; jz <= i__1; ++jz) {
//                jj = index[jz];
//                h12_(&c__2, &nsetp, &npp1, m, &a[j * a_dim1 + 1], &c__1, &up, &a[
//                        jj * a_dim1 + 1], &c__1, mda, &c__1);
//    /* L160: */
//            }
//        }
//
//        if (nsetp != *m) {
//            i__1 = *m;
//            for (l = npp1; l <= i__1; ++l) {
//    /* L180: */
//                a[l + j * a_dim1] = 0.;
//            }
//        }
//
//        w[j] = 0.;
//    /*                                SOLVE THE TRIANGULAR SYSTEM. */
//    /*                                STORE THE SOLUTION TEMPORARILY IN ZZ().
//    */
//        rtnkey = 1;
//        goto L400;
//    L200:
//
//    /*                       ******  SECONDARY LOOP BEGINS HERE ****** */
//
//    /*                          ITERATION COUNTER. */
//
//    L210:
//        ++iter;
//        if (iter > itmax) {
//            *mode = 3;
//            /* The following lines were replaced after the f2c translation */
//            /* s_wsfe(&io___22); */
//            /* do_fio(&c__1, " NNLS quitting on iteration count.", 34L); */
//            /* e_wsfe(); */
//            fprintf(stdout, "\n NNLS quitting on iteration count.\n");
//            fflush(stdout);
//            goto L350;
//        }
//
//    /*                    SEE IF ALL NEW CONSTRAINED COEFFS ARE FEASIBLE. */
//    /*                                  IF NOT COMPUTE ALPHA. */
//
//        alpha = 2.;
//        i__1 = nsetp;
//        for (ip = 1; ip <= i__1; ++ip) {
//            l = index[ip];
//            if (zz[ip] <= 0.) {
//                t = -x[l] / (zz[ip] - x[l]);
//                if (alpha > t) {
//                    alpha = t;
//                    jj = ip;
//                }
//            }
//    /* L240: */
//        }
//
//    /*          IF ALL NEW CONSTRAINED COEFFS ARE FEASIBLE THEN ALPHA WILL */
//    /*          STILL = 2.    IF SO EXIT FROM SECONDARY LOOP TO MAIN LOOP. */
//
//        if (alpha == 2.) {
//            goto L330;
//        }
//
//    /*          OTHERWISE USE ALPHA WHICH WILL BE BETWEEN 0. AND 1. TO */
//    /*          INTERPOLATE BETWEEN THE OLD X AND THE NEW ZZ. */
//
//        i__1 = nsetp;
//        for (ip = 1; ip <= i__1; ++ip) {
//            l = index[ip];
//            x[l] += alpha * (zz[ip] - x[l]);
//    /* L250: */
//        }
//
//    /*        MODIFY A AND B AND THE INDEX ARRAYS TO MOVE COEFFICIENT I */
//    /*        FROM SET P TO SET Z. */
//
//        i__ = index[jj];
//    L260:
//        x[i__] = 0.;
//
//        if (jj != nsetp) {
//            ++jj;
//            i__1 = nsetp;
//            for (j = jj; j <= i__1; ++j) {
//                ii = index[j];
//                index[j - 1] = ii;
//                g1_(&a[j - 1 + ii * a_dim1], &a[j + ii * a_dim1], &cc, &ss, &a[j
//                        - 1 + ii * a_dim1]);
//                a[j + ii * a_dim1] = 0.;
//                i__2 = *n;
//                for (l = 1; l <= i__2; ++l) {
//                    if (l != ii) {
//
//    /*                 Apply procedure G2 (CC,SS,A(J-1,L),A(J,
//    L)) */
//
//                        temp = a[j - 1 + l * a_dim1];
//                        a[j - 1 + l * a_dim1] = cc * temp + ss * a[j + l * a_dim1]
//                                ;
//                        a[j + l * a_dim1] = -ss * temp + cc * a[j + l * a_dim1];
//                    }
//    /* L270: */
//                }
//
//    /*                 Apply procedure G2 (CC,SS,B(J-1),B(J)) */
//
//                temp = b[j - 1];
//                b[j - 1] = cc * temp + ss * b[j];
//                b[j] = -ss * temp + cc * b[j];
//    /* L280: */
//            }
//        }
//
//        npp1 = nsetp;
//        --nsetp;
//        --iz1;
//        index[iz1] = i__;
//
//    /*        SEE IF THE REMAINING COEFFS IN SET P ARE FEASIBLE.  THEY SHOULD
//    */
//    /*        BE BECAUSE OF THE WAY ALPHA WAS DETERMINED. */
//    /*        IF ANY ARE INFEASIBLE IT IS DUE TO ROUND-OFF ERROR.  ANY */
//    /*        THAT ARE NONPOSITIVE WILL BE SET TO ZERO */
//    /*        AND MOVED FROM SET P TO SET Z. */
//
//        i__1 = nsetp;
//        for (jj = 1; jj <= i__1; ++jj) {
//            i__ = index[jj];
//            if (x[i__] <= 0.) {
//                goto L260;
//            }
//    /* L300: */
//        }
//
//    /*         COPY B( ) INTO ZZ( ).  THEN SOLVE AGAIN AND LOOP BACK. */
//
//        i__1 = *m;
//        for (i__ = 1; i__ <= i__1; ++i__) {
//    /* L310: */
//            zz[i__] = b[i__];
//        }
//        rtnkey = 2;
//        goto L400;
//    L320:
//        goto L210;
//    /*                      ******  END OF SECONDARY LOOP  ****** */
//
//    L330:
//        i__1 = nsetp;
//        for (ip = 1; ip <= i__1; ++ip) {
//            i__ = index[ip];
//    /* L340: */
//            x[i__] = zz[ip];
//        }
//    /*        ALL NEW COEFFS ARE POSITIVE.  LOOP BACK TO BEGINNING. */
//        goto L30;
//
//    /*                        ******  END OF MAIN LOOP  ****** */
//
//    /*                        COME TO HERE FOR TERMINATION. */
//    /*                     COMPUTE THE NORM OF THE FINAL RESIDUAL VECTOR. */
//
//    L350:
//        sm = 0.;
//        if (npp1 <= *m) {
//            i__1 = *m;
//            for (i__ = npp1; i__ <= i__1; ++i__) {
//    /* L360: */
//    /* Computing 2nd power */
//                d__1 = b[i__];
//                sm += d__1 * d__1;
//            }
//        } else {
//            i__1 = *n;
//            for (j = 1; j <= i__1; ++j) {
//    /* L380: */
//                w[j] = 0.;
//            }
//        }
//        *rnorm = sqrt(sm);
//        return 0;
//
//    /*     THE FOLLOWING BLOCK OF CODE IS USED AS AN INTERNAL SUBROUTINE */
//    /*     TO SOLVE THE TRIANGULAR SYSTEM, PUTTING THE SOLUTION IN ZZ(). */
//
//    L400:
//        i__1 = nsetp;
//        for (l = 1; l <= i__1; ++l) {
//            ip = nsetp + 1 - l;
//            if (l != 1) {
//                i__2 = ip;
//                for (ii = 1; ii <= i__2; ++ii) {
//                    zz[ii] -= a[ii + jj * a_dim1] * zz[ip + 1];
//    /* L410: */
//                }
//            }
//            jj = index[ip];
//            zz[ip] /= a[ip + jj * a_dim1];
//    /* L430: */
//        }
//        switch ((int)rtnkey) {
//            case 1:  goto L200;
//            case 2:  goto L320;
//        }
//
//        /* The next line was added after the f2c translation to keep
//           compilers from complaining about a void return from a non-void
//           function. */
//        return 0;
//
//    } /* nnls_ */
//
//
//
//
//
//    /* The following subroutine was added after the f2c translation */
//    int nnls_c(double* a, const int* mda, const int* m, const int* n, double* b,
//             double* x, double* rnorm, double* w, double* zz, int* index,
//             int* mode)
//    {
//      return (nnls_(a, (int*)mda, (int*)m, (int*)n, b, x, rnorm, w, zz, index, mode));
//    }
//
//    bool solveNN( const dvec &a, const dvec &b, const dvec&c, dvec&result)
//    {
//
//      char buffa[4096],buffb[4096],buffc[4096];
//      char *pta = (char*)buffa; pta[0]=0;
//      char *ptb = (char*)buffb; ptb[0]=0;
//      char *ptc = (char*)buffc; ptb[0]=0;
//
//      for(auto it = a.begin();it != a.end(); it++)
//      pta += sprintf(pta,"%lf , ",*it);
//
//      for(auto it = b.begin();it != b.end(); it++)
//      ptb += sprintf(ptb,"%lf , ",*it);
//
//      for(auto it = c.begin();it != c.end(); it++)
//      ptc += sprintf(ptc,"%lf , ",*it);
//
//      eos_static_info(" a = %s",buffa);
//      eos_static_info(" b = %s",buffb);
//      eos_static_info(" c = %s",buffc);
//
//      int m = a.size();
//      int n = 2;
//
//      double *matrix = new double[m*n];
//      double *vect = new double[m];
//
//      double *x = new double[n];
//
//      double *w = new double[n];
//      double *zz = new double[m];
//
//      int *index = new int[n];
//
//      double rnorm;
//      int mode;
//
//      int k=0;
//      for(int p = 0; p<m; p++)
//        matrix[k++] = a[p];
//      for(int p = 0; p<m; p++)
//        matrix[k++] = b[p];
//
//      k = 0;
//      for(int p = 0; p<m; p++)
//        vect[k++] = c[p];
//
//
//      nnls_c(matrix,&m,&m,&n,vect,
//             x,&rnorm,w,zz,index,&mode);
//
//      eos_static_info("mode is %d",mode);
//
//      result = { x[0] , x[1]};
//
//
//        delete[] matrix;
//        delete[] vect;
//        delete[] x;
//        delete[] w;
//        delete[] zz;
//        delete[] index;
//
//        return true;
//    }
//  };
//
//  LS2dSolver solver;
//
//  if(penaltyUpdateRate)
//  {
//
//    std::vector<double> update;
//
//    if(updatingNodes.empty())
//    {
//      eos_static_info("updatingNodes is empty!");
//    }
//    else
//    {
//      std::vector<std::vector<double> >
//      ropen(pRoOpenFilesCount.size()),
//      wopen(pRoOpenFilesCount.size()),
//      ulload(pRoOpenFilesCount.size()),
//      dlload(pRoOpenFilesCount.size());
//
//      std::stringstream ss;
//      for( auto it = updatingNodes.begin(); it!= updatingNodes.end(); it++)
//        ss << it->first << "  ";
//
//      eos_static_info("updatingNodes are : %s",ss.str().c_str());
//
//      FsView::gFsView.ViewMutex.LockRead();
//      for( auto it = updatingNodes.begin(); it!= updatingNodes.end(); it++)
//      {
//        //std::string nodestr = "/eos/"+(*it)+"/fst";
//        std::string nodestr = it->first;
//        FsNode *node = NULL;
//        if(FsView::gFsView.mNodeView.count(nodestr))
//          node = FsView::gFsView.mNodeView[nodestr];
//        else
//        {
//          ss.str("");
//          for (auto it2 = FsView::gFsView.mNodeView.begin(); it2 != FsView::gFsView.mNodeView.end(); it2++)
//            ss << it2->first << "  ";
//          eos_static_info("cannot find updating node %s in %s",nodestr.c_str(),ss.str().c_str());
//          continue;
//        }
//        if(it->second.count == node->size())
//        {
//          //eos_static_info("fs update in node %s : found the expected number of fs %d",it->first.c_str(),(int)it->second);
//          eos_static_info("aggregated opened files for %s : wopen %d   ropen %d   outweight %lf   inweight %lf",
//                          it->first.c_str(),it->second.wopen,it->second.ropen,it->second.netOutWeight,it->second.netInWeight);
//        }
//        else {
//          // this should not happen if only one single fst daemon is running each fst node
//          eos_static_info("fs update in node %s : %d fs in FsView vs %d fs in update",it->first.c_str(),(int)node->size(),(int)it->second.count);
////          long long wopen = node->SumLongLong("stat.wopen",false);
////          long long ropen = node->SumLongLong("stat.ropen",false);
////          eos_static_info("aggreagted opened files for %s : wopen %d   ropen %d",it->first.c_str(),(int)wopen,(int)ropen);
//        }
//        if(ropen.size()<it->second.netSpeedClass+1)
//        {
//          ropen.resize(it->second.netSpeedClass+1);
//          wopen.resize(it->second.netSpeedClass+1);
//          ulload.resize(it->second.netSpeedClass+1);
//          dlload.resize(it->second.netSpeedClass+1);
//        }
//        ropen[it->second.netSpeedClass].push_back(it->second.ropen);
//        wopen[it->second.netSpeedClass].push_back(it->second.wopen);
//        ulload[it->second.netSpeedClass].push_back(1.0-it->second.netOutWeight);
//        dlload[it->second.netSpeedClass].push_back(1.0-it->second.netInWeight);
//      }
//      FsView::gFsView.ViewMutex.UnLockRead();
//
//      for(size_t netSpeedClass=0; netSpeedClass<pRoOpenFilesCount.size(); netSpeedClass++)
//      {
//        if(ropen[netSpeedClass].size()>1)
//        {
//          if(solver.solveNN(ropen[netSpeedClass],wopen[netSpeedClass],ulload[netSpeedClass],update))
//          {
//            eos_static_info("netSpeedClass %d : using update values %lf and %lf for ul network penalty",
//                netSpeedClass, update[0], update[1]);
//          }
//          else
//          {
//            eos_static_info("could not compute the ul network update");
//          }
//
//          if(solver.solveNN(ropen[netSpeedClass],wopen[netSpeedClass],dlload[netSpeedClass],update))
//          {
//            eos_static_info("netSpeedClass %d : using update values %lf and %lf for dl network penalty",
//                netSpeedClass, update[0], update[1]);
//          }
//          else
//          {
//            eos_static_info("could not compute the dl network update");
//          }
//
//        }
//      }
//    }
//
//    eos_static_info("trying to update penalties");
//    for(size_t netSpeedClass=0; netSpeedClass<pRoOpenFilesCount.size(); netSpeedClass++)
//    {
//      eos_static_info("netSpeedClass = %d",(int)netSpeedClass);
//      if(pRoOpenFilesCount[netSpeedClass].size()<2) // not enough data for this speed class
//      continue;
//      if(solver.solve(pRoOpenFilesCount[netSpeedClass],pRwOpenFilesCount[netSpeedClass],pTotalUlPenalty[netSpeedClass],update))
//      {
//        if(update[0]<1 || update[0]>20)
//        {
//          eos_static_info("weird value for accessUlScorePenalty update : %lf. Not using this one.",update[0]);
//        }
//        else if(update[1]<1 || update[1]>20)
//        {
//          eos_static_info("weird value for plctUlScorePenalty update : %lf. Not using this one.",update[1]);
//        }
//        else
//        {
//          eos_static_info("netSpeedClass %d : using update values %lf and %lf accessUlScorePenalty and plctUlScorePenalty update with weight %d%%",
//              netSpeedClass, update[0], update[1], (int)penaltyUpdateRate);
//
//          AtomicCAS( accessUlScorePenalty , accessUlScorePenalty, static_cast<char>(0.01*( 100 - penaltyUpdateRate)*accessUlScorePenalty + 0.01*penaltyUpdateRate*update[0]) );
//          AtomicCAS( plctUlScorePenalty , plctUlScorePenalty , static_cast<char>(0.01*( 100 - penaltyUpdateRate)*plctUlScorePenalty + 0.01*penaltyUpdateRate*update[1]) );
//        }
//      }
//      if(solver.solve(pRoOpenFilesCount[netSpeedClass],pRwOpenFilesCount[netSpeedClass],pTotalDlPenalty[netSpeedClass],update))
//      {
//        if(update[0]<1 || update[0]>20)
//        {
//          eos_static_info("weird value for accessDlScorePenalty update : %lf. Not using this one.",update[0]);
//        }
//        else if(update[1]<1 || update[1]>20)
//        {
//          eos_static_info("weird value for plctDlScorePenalty update : %lf. Not using this one.",update[1]);
//        }
//        else
//        {
//          eos_static_info("netSpeedClass %d : using update values %lf and %lf accessUlScorePenalty and plctUlScorePenalty update with weight %d%%",
//              netSpeedClass, update[0], update[1], (int)penaltyUpdateRate);
//          AtomicCAS( accessDlScorePenalty , accessDlScorePenalty , static_cast<char>(0.01*( 100 - penaltyUpdateRate)*accessDlScorePenalty + 0.01*penaltyUpdateRate*update[0]) );
//          AtomicCAS( plctDlScorePenalty , plctDlScorePenalty , static_cast<char>(0.01*( 100 - penaltyUpdateRate)*plctDlScorePenalty + 0.01*penaltyUpdateRate*update[1]) );
//        }
//      }
//    }
//  }
}

EOSMGMNAMESPACE_END

