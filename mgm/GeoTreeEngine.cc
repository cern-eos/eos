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

#define HAVE_ATOMICS 1

#include "mgm/GeoTreeEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/FileSystem.hh"
#include "common/IntervalStopwatch.hh"
#include "common/Assert.hh"
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <ctime>
#include <sys/stat.h>
#include <tuple>
#include <algorithm>

using namespace std;
using namespace eos::common;
using namespace eos::mgm;

EOSMGMNAMESPACE_BEGIN

// We assume that all the trees have the same max size, we should take the max
// of all the sizes otherwise
const size_t GeoTreeEngine::gGeoBufferSize = sizeof(FastPlacementTree) +
    FastPlacementTree::sGetMaxDataMemSize();
thread_local void* GeoTreeEngine::tlGeoBuffer = NULL;
pthread_key_t GeoTreeEngine::gPthreadKey;

const int GeoTreeEngine::sfgId = 1;
const int GeoTreeEngine::sfgHost = 1 << 1;
const int GeoTreeEngine::sfgGeotag = 1 << 2;
const int GeoTreeEngine::sfgBoot = 1 << 3;
const int GeoTreeEngine::sfgActive = 1 << 4;
const int GeoTreeEngine::sfgConfigstatus = 1 << 5;
const int GeoTreeEngine::sfgDrain = 1 << 6;
const int GeoTreeEngine::sfgDrainer = 1 << 6;
const int GeoTreeEngine::sfgBalthres = 1 << 7;
const int GeoTreeEngine::sfgBlkavailb = 1 << 8;
const int GeoTreeEngine::sfgFsfilled = 1 << 9;
const int GeoTreeEngine::sfgNomfilled = 1 << 10;
const int GeoTreeEngine::sfgReadratemb = 1 << 12;
const int GeoTreeEngine::sfgDiskload = 1 << 13;
const int GeoTreeEngine::sfgEthmib = 1 << 14;
const int GeoTreeEngine::sfgInratemib = 1 << 15;
const int GeoTreeEngine::sfgOutratemib = 1 << 16;
const int GeoTreeEngine::sfgErrc = 1 << 17;
const int GeoTreeEngine::sfgPubTmStmp = 1 << 18;

set<string> GeoTreeEngine::gWatchedKeys;

const map<string, int> GeoTreeEngine::gNotifKey2EnumSched = {
  make_pair("id", sfgId),
  make_pair("host", sfgHost),
  make_pair("forcegeotag", sfgGeotag),
  make_pair("stat.geotag", sfgGeotag),
  make_pair("stat.boot", sfgBoot),
  make_pair("stat.active", sfgActive),
  make_pair("configstatus", sfgConfigstatus),
  make_pair("stat.drain", sfgDrain),
  make_pair("stat.drainer", sfgDrainer),
  make_pair("stat.balance.threshold", sfgBalthres),
  make_pair("stat.nominal.filled", sfgNomfilled),
  make_pair("stat.statfs.bavail", sfgBlkavailb),
  make_pair("stat.statfs.filled", sfgFsfilled),
  make_pair("stat.disk.readratemb", sfgReadratemb),
  make_pair("stat.disk.load", sfgDiskload),
  make_pair("stat.net.ethratemib", sfgEthmib),
  make_pair("stat.net.inratemib", sfgInratemib),
  make_pair("stat.net.outratemib", sfgOutratemib),
  make_pair("stat.errc", sfgErrc),
  make_pair("stat.publishtimestamp", sfgPubTmStmp),
};

map<string, int> GeoTreeEngine::gNotificationsBufferFs;
map<string, int> GeoTreeEngine::gNotificationsBufferProxy;
sem_t GeoTreeEngine::gUpdaterPauseSem;
bool GeoTreeEngine::gUpdaterPaused = false;
bool GeoTreeEngine::gUpdaterStarted = false;
const unsigned char GeoTreeEngine::sntFilesystem = 1,
                                   GeoTreeEngine::sntDataproxy = 4;
std::map<std::string, unsigned char> GeoTreeEngine::gQueue2NotifType;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
GeoTreeEngine::GeoTreeEngine(XrdMqSharedObjectChangeNotifier &notifier) :
  pSkipSaturatedAccess(true), pSkipSaturatedDrnAccess(true),
  pSkipSaturatedBlcAccess(true), pProxyCloseToFs(true),
  pPenaltyUpdateRate(1),
  pFillRatioLimit(80), pFillRatioCompTol(100), pSaturationThres(10),
  pTimeFrameDurationMs(1000), pPublishToPenaltyDelayMs(1000),
  pAccessGeotagMapping("accessgeotagmapping"),
  pAccessProxygroup("accessproxygroup"),
  pCircSize(30), pFrameCount(0),
  pPenaltySched(pCircSize),
  pLatencySched(pCircSize),
  mFsListener("geotree-fs-listener", notifier) {

  // by default, disable all the placement operations for non geotagged fs
  addDisabledBranch("*", "plct", "nogeotag", NULL, false);
  addDisabledBranch("*", "accsdrain", "nogeotag", NULL, false);
  // set blocking mutexes for lower latencies
  pAddRmFsMutex.SetBlocking(true);
  configMutex.SetBlocking(true);
  pTreeMapMutex.SetBlocking(true);

  for (auto it = pPenaltySched.pCircFrCnt2FsPenalties.begin();
    it != pPenaltySched.pCircFrCnt2FsPenalties.end(); it++) {
    it->reserve(100);
  }

  // create the thread local key to handle allocation/destruction of thread local geobuffers
  pthread_key_create(&gPthreadKey, GeoTreeEngine::tlFree);

  // initialize pauser semaphore
  if (sem_init(&gUpdaterPauseSem, 0, 1)) {
    throw "sem_init() failed";
 }
}

bool GeoTreeEngine::forceRefreshSched()
{
  // prevent any other use of the fast structures
  pAddRmFsMutex.LockWrite();
  pTreeMapMutex.LockWrite();

  // mark all fs needing a refresh for all the watched attributes
  // => SCHED
  for (auto it = pFsId2FsPtr.begin(); it != pFsId2FsPtr.end(); it++) {
    if (it->second) {
      gNotificationsBufferFs[it->second->GetQueuePath()] = (~0);
    }
  }

  for (auto it = pGroup2SchedTME.begin(); it != pGroup2SchedTME.end(); it++) {
    it->second->fastStructModified = true;
    it->second->slowTreeModified = true;
  }

  // mark all proxy needing a refresh for all the watched attributes
  // => PROXYGROUPS
  for (auto it = pPxyQueue2PxyId.begin(); it != pPxyQueue2PxyId.end(); it++) {
    gNotificationsBufferProxy[it->first] = (~0);
  }

  for (auto it = pPxyGrp2DpTME.begin(); it != pPxyGrp2DpTME.end(); it++) {
    it->second->fastStructModified = true;
    it->second->slowTreeModified = true;
  }

  // do the update
  pTreeMapMutex.UnLockWrite();
  updateTreeInfo(gNotificationsBufferFs, gNotificationsBufferProxy);
  pAddRmFsMutex.UnLockWrite();
  return true;
}

bool GeoTreeEngine::forceRefresh()
{
  // signal a pause to the background updating
  PauseUpdater();
  // do the refreshes
  bool result = forceRefreshSched();
  // signal a resume to the background updating
  ResumeUpdater();
  return result;
}

bool GeoTreeEngine::insertFsIntoGroup(FileSystem* fs,
                                      FsGroup* group,
                                      const common::FileSystemCoreParams& coreParams)
{
  bool updateFastStruct = false;
  eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
  FileSystem::fsid_t fsid = coreParams.getId();
  SchedTME* mapEntry = 0;
  bool is_new_entry = false;
  {
    pTreeMapMutex.LockWrite();

    // ==== check that fs is not already registered
    if (pFs2SchedTME.count(fsid)) {
      eos_err("error inserting fs %lu into group %s : fs is already part of a group",
              (unsigned long)fsid, group->mName.c_str());
      pTreeMapMutex.UnLockWrite();
      return false;
    }

    // ==== get the entry
    if (pGroup2SchedTME.count(group)) {
      mapEntry = pGroup2SchedTME[group];
    } else {
      mapEntry = new SchedTME(group->mName.c_str());
      is_new_entry = true;
      // Force update to be sure that the fast structures are properly created
      updateFastStruct = true;
    }

    mapEntry->slowTreeMutex.LockWrite();
    pTreeMapMutex.UnLockWrite();
  }
  // ==== fill the entry
  // create new TreeNodeInfo/TreeNodeState pair and update its data
  eos::common::FileSystem::fs_snapshot_t fsn;
  fs->SnapShotFileSystem(fsn, true);
  fsn.fillFromCoreParams(coreParams);
  // check if there is still some space for a new fs
  {
    size_t depth = 1;
    std::string sub("::");

    for (size_t offset = fsn.mGeoTag.find(sub); offset != std::string::npos;
         offset = fsn.mGeoTag.find(sub, offset + sub.length())) {
      depth++;
    }

    if (depth + mapEntry->slowTree->getNodeCount() >
        SchedTreeBase::sGetMaxNodeCount() - 2) {
      mapEntry->slowTreeMutex.UnLockWrite();
      eos_err("error inserting fs %lu into group %s : the group-tree is full",
              (unsigned long)fsid, group->mName.c_str());

      if (is_new_entry) {
        delete mapEntry;
      }

      return false;
    }
  }
  SchedTreeBase::TreeNodeInfo info;
  info.geotag = fsn.mGeoTag;

  if (info.geotag.empty()) {
    char buffer[64];
    snprintf(buffer, 64, "nogeotag");
    info.geotag = buffer;
  }

  info.host = coreParams.getHost();
  info.hostport = coreParams.getHostPort();

  if (info.host.empty()) {
    uuid_t uuid;
    char buffer[64];
    snprintf(buffer, 64, "nohost-");
    uuid_generate_time(uuid);
    uuid_unparse(uuid, buffer + 7);
    info.host = buffer;
  }

  info.netSpeedClass = 1; // EthRateMiB not yet initialized at this point,
  // use placeholder value
  info.fsId = coreParams.getId();

  if (!info.fsId) {
    mapEntry->slowTreeMutex.UnLockWrite();
    eos_err("error inserting fs %lu into group %s : FsId is not set!",
            (unsigned long)fsid, group->mName.c_str());

    if (is_new_entry) {
      delete mapEntry;
    }

    return false;
  }

  SchedTreeBase::TreeNodeStateFloat state;
  // try to insert the new node in the Slowtree
  SlowTreeNode* node = mapEntry->slowTree->insert(&info, &state);

  if (node == NULL) {
    mapEntry->slowTreeMutex.UnLockWrite();
    eos_err("error inserting fs %lu into group %s : slow tree node insertion failed",
            (unsigned long)fsid, group->mName.c_str());

    if (is_new_entry) {
      delete mapEntry;
    }

    return false;
  }

  // ==== update the penalties vectors if necessary
  if ((coreParams.getId() + 1) > pLatencySched.pFsId2LatencyStats.size()) {
    for (auto it = pPenaltySched.pCircFrCnt2FsPenalties.begin();
         it != pPenaltySched.pCircFrCnt2FsPenalties.end(); it++) {
      it->resize(coreParams.getId() + 1);
    }

    pLatencySched.pFsId2LatencyStats.resize(coreParams.getId() + 1);
  }

  // ==== update the shared object notifications
  {
    if (gWatchedKeys.empty()) {
      for (auto it = gNotifKey2EnumSched.begin(); it != gNotifKey2EnumSched.end();
           it++) {
        gWatchedKeys.insert(it->first);
      }
    }

    gQueue2NotifType[fs->GetQueuePath()] |= sntFilesystem;

    if(!mFsListener.subscribe(fs->GetQueuePath(), gWatchedKeys)) {
      eos_crit("error inserting fs %lu into group %s : error subscribing to "
               "shared object notifications", (unsigned long)fsid,
               group->mName.c_str());
      gQueue2NotifType[fs->GetQueuePath()] &= ~sntFilesystem;

      if (gQueue2NotifType[fs->GetQueuePath()] == 0) {
        gQueue2NotifType.erase(fs->GetQueuePath());
      }

      mapEntry->slowTreeMutex.UnLockWrite();

      if (is_new_entry) {
        delete mapEntry;
      }

      return false;
    }
  }

  // update all the information about this new node
  if (!updateTreeInfo(mapEntry, &fsn, ~sfgGeotag & ~sfgId & ~sfgHost , 0, node)) {
    mapEntry->slowTreeMutex.UnLockWrite();
    pTreeMapMutex.LockRead();
    eos_err("error inserting fs %lu into group %s : slow tree node update failed",
            (unsigned long)fsid, group->mName.c_str());
    pTreeMapMutex.UnLockRead();

    if (is_new_entry) {
      delete mapEntry;
    }

    return false;
  }

  mapEntry->fs2SlowTreeNode[fsid] = node;
  mapEntry->slowTreeModified = true;
  mapEntry->group = group;

  // update the fast structures now if requested
  if (updateFastStruct) {
    if (!updateFastStructures(mapEntry)) {
      mapEntry->slowTreeMutex.UnLockWrite();
      pTreeMapMutex.LockRead();
      eos_err("error inserting fs %lu into group %s : fast structures update failed",
              fsid, group->mName.c_str(), pFs2SchedTME[fsid]->group->mName.c_str());
      pTreeMapMutex.UnLockRead();

      if (is_new_entry) {
        delete mapEntry;
      }

      return false;
    } else {
      mapEntry->slowTreeModified = false;
    }
  }

  // ==== update the entry in the map
  {
    pTreeMapMutex.LockWrite();
    pGroup2SchedTME[group] = mapEntry;
    pFs2SchedTME[fsid] = mapEntry;
    pFsId2FsPtr[fsid] = fs;
    pTreeMapMutex.UnLockWrite();
    mapEntry->slowTreeMutex.UnLockWrite();
  }
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    stringstream ss;
    ss << (*mapEntry->slowTree);
    eos_debug("inserted fs %lu into group %s geotag is %s and fullgeotag is %s\n%s",
              (unsigned long)fsid, group->mName.c_str(),
              node->pNodeInfo.geotag.c_str(), node->pNodeInfo.fullGeotag.c_str(),
              ss.str().c_str());
  }

  return true;
}


bool GeoTreeEngine::removeFsFromGroup(FileSystem* fs, FsGroup* group,
                                      bool updateFastStruct)
{
  eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
  SchedTME* mapEntry;
  FileSystem::fsid_t fsid = fs->GetId();
  {
    pTreeMapMutex.LockWrite();

    // ==== check that fs is registered
    if (!pFs2SchedTME.count(fsid)) {
      eos_err("error removing fs %lu from group %s : fs is not registered",
              (unsigned long)fsid, group->mName.c_str());
      pTreeMapMutex.UnLockWrite();
      return false;
    }

    mapEntry = pFs2SchedTME[fsid];

    // ==== get the entry
    if (!pGroup2SchedTME.count(group)) {
      eos_err("error removing fs %lu from group %s : fs is not registered ",
              (unsigned long)fsid, group->mName.c_str());
      pTreeMapMutex.UnLockWrite();
      return false;
    }

    pTreeMapMutex.UnLockWrite();
    mapEntry = pGroup2SchedTME[group];
    mapEntry->slowTreeMutex.LockWrite();
  }
  // ==== update the shared object notifications
  {
    if (!mFsListener.unsubscribe(fs->GetQueuePath(), gWatchedKeys)) {
      mapEntry->slowTreeMutex.UnLockWrite();
      eos_crit("error removing fs %lu into group %s : error unsubscribing to "
               "shared object notifications", (unsigned long)fsid,
               group->mName.c_str());
      return false;
    }

    gQueue2NotifType[fs->GetQueuePath()] &= ~sntFilesystem;

    if (gQueue2NotifType[fs->GetQueuePath()] == 0) {
      gQueue2NotifType.erase(fs->GetQueuePath());
    }
  }
  // ==== discard updates about this fs
  // ==== clean the notifications buffer
  gNotificationsBufferFs.erase(fs->GetQueuePath());
  // ==== update the entry
  SchedTreeBase::TreeNodeInfo info;
  const SlowTreeNode* intree = mapEntry->fs2SlowTreeNode[fsid];
  info = intree->pNodeInfo;
  info.geotag = intree->pNodeInfo.fullGeotag;
  eos_debug("msg=\"remove from SlowNodeTree\" fsid=%lu host=\"%s\" "
            "geotag=\"%s\" fullgeotag=\"%s\"",
            (unsigned long)intree->pNodeInfo.fsId,
            intree->pNodeInfo.host.c_str(),
            intree->pNodeInfo.geotag.c_str(),
            intree->pNodeInfo.fullGeotag.c_str());
  // try to update the SlowTree
  info.fsId = 0;

  if (!mapEntry->slowTree->remove(&info)) {
    mapEntry->slowTreeMutex.UnLockWrite();
    eos_err("error removing fs %lu from group %s : removing the slow tree node "
            "failed. geotag is %s and geotag in tree is %s and %s",
            (unsigned long)fsid, group->mName.c_str(), info.geotag.c_str(),
            intree->pNodeInfo.fullGeotag.c_str(), intree->pNodeInfo.geotag.c_str());
    return false;
  }

  mapEntry->fs2SlowTreeNode.erase(fsid);

  // if the tree is empty, remove the entry from the map
  if (!mapEntry->fs2SlowTreeNode.empty()) { // if the tree is getting empty, no need to update it
    mapEntry->slowTreeModified = true;
  }

  if (updateFastStruct && mapEntry->slowTreeModified)
    if (!updateFastStructures(mapEntry)) {
      mapEntry->slowTreeMutex.UnLockWrite();
      pTreeMapMutex.LockRead();
      eos_err("error removing fs %lu from group %s : fast structures update failed",
              fsid, group->mName.c_str(), pFs2SchedTME[fsid]->group->mName.c_str());
      pTreeMapMutex.UnLockRead();
      return false;
    }

  // ==== update the entry in the map if needed
  {
    pTreeMapMutex.LockWrite();
    pFs2SchedTME.erase(fsid);
    pFsId2FsPtr.erase(fsid);

    if (mapEntry->fs2SlowTreeNode.empty()) {
      pGroup2SchedTME.erase(group); // prevent from access by other threads
      pPendingDeletionsFs.push_back(mapEntry);
    }

    mapEntry->slowTreeMutex.UnLockWrite();
    pTreeMapMutex.UnLockWrite();
  }
  return true;
}

void GeoTreeEngine::printInfo(std::string& info, bool dispTree, bool dispSnaps,
                              bool dispParam, bool dispState, const std::string&
                              schedgroup, const std::string& optype,
                              bool useColors, bool monitoring)
{
  RWMutexReadLock lock(pTreeMapMutex);
  stringstream ostr;
  map<string, string> orderByGroupName;
  std::string format_s = !monitoring ? "s" : "os";
  std::string format_ss = !monitoring ? "-s" : "os";
  std::string format_l = !monitoring ? "l" : "ol";
  std::string format_ll = !monitoring ? "-l" : "ol";
  std::string format_lll = !monitoring ? "+l" : "ol";
  std::string format_f = !monitoring ? "+f" : "of";
  std::string unit = !monitoring ? "s" : "";
  std::string na = !monitoring ? "-NA-" : "NA";
  unsigned scale = !monitoring ? 1000 :
                   1; // miliseconds to seconds for human view

  if (dispParam) {
    ostr << "### GeoTreeEngine parameters :" << std::endl;
    ostr << "skipSaturatedAccess = " << pSkipSaturatedAccess << std::endl;
    ostr << "skipSaturatedDrnAccess = " << pSkipSaturatedDrnAccess << std::endl;
    ostr << "skipSaturatedBlcAccess = " << pSkipSaturatedBlcAccess << std::endl;
    ostr << "proxyCloseToFs = " << pProxyCloseToFs << std::endl;
    ostr << "penaltyUpdateRate = " << pPenaltyUpdateRate << std::endl;
    ostr << "plctDlScorePenalty = " << pPenaltySched.pPlctDlScorePenaltyF[0] <<
         "(default)" << " | "
         << pPenaltySched.pPlctDlScorePenaltyF[1] << "(1Gbps)" << " | "
         << pPenaltySched.pPlctDlScorePenaltyF[2] << "(10Gbps)" << " | "
         << pPenaltySched.pPlctDlScorePenaltyF[3] << "(100Gbps)" << " | "
         << pPenaltySched.pPlctDlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "plctUlScorePenalty = " << pPenaltySched.pPlctUlScorePenaltyF[0] <<
         "(defaUlt)" << " | "
         << pPenaltySched.pPlctUlScorePenaltyF[1] << "(1Gbps)" << " | "
         << pPenaltySched.pPlctUlScorePenaltyF[2] << "(10Gbps)" << " | "
         << pPenaltySched.pPlctUlScorePenaltyF[3] << "(100Gbps)" << " | "
         << pPenaltySched.pPlctUlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "accessDlScorePenalty = " << pPenaltySched.pAccessDlScorePenaltyF[0] <<
         "(default)" << " | "
         << pPenaltySched.pAccessDlScorePenaltyF[1] << "(1Gbps)" << " | "
         << pPenaltySched.pAccessDlScorePenaltyF[2] << "(10Gbps)" << " | "
         << pPenaltySched.pAccessDlScorePenaltyF[3] << "(100Gbps)" << " | "
         << pPenaltySched.pAccessDlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "accessUlScorePenalty = " << pPenaltySched.pAccessUlScorePenaltyF[0] <<
         "(defaUlt)" << " | "
         << pPenaltySched.pAccessUlScorePenaltyF[1] << "(1Gbps)" << " | "
         << pPenaltySched.pAccessUlScorePenaltyF[2] << "(10Gbps)" << " | "
         << pPenaltySched.pAccessUlScorePenaltyF[3] << "(100Gbps)" << " | "
         << pPenaltySched.pAccessUlScorePenaltyF[4] << "(1000Gbps)" << std::endl;
    ostr << "fillRatioLimit = " << (int)pFillRatioLimit << std::endl;
    ostr << "fillRatioCompTol = " << (int)pFillRatioCompTol << std::endl;
    ostr << "saturationThres = " << (int)pSaturationThres << std::endl;
    ostr << "timeFrameDurationMs = " << (int)pTimeFrameDurationMs << std::endl;
  }

  if (dispState) {
    ostr << "frameCount = " << pFrameCount << std::endl;

    //! Added penalties for each fs over successive frames
    if (!monitoring) {
      ostr << "\n┏━> Added penalties for each fs over successive frames\n";
    }

    {
      // to be sure that no fs in inserted removed in the meantime
      eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
      struct timeval curtime;
      gettimeofday(&curtime, 0);
      size_t ts = curtime.tv_sec * 1000 + curtime.tv_usec / 1000;
      TableFormatterBase table;
      TableHeader table_header;

      if (monitoring) {
        table_header.push_back(std::make_tuple("type", 4, format_ss));
      }

      table_header.push_back(std::make_tuple("fsid", 4, format_ll));
      table_header.push_back(std::make_tuple("drct", 4, format_ss));

      for (size_t itcol = 0; itcol < pCircSize; itcol++) {
        float frame = pLatencySched.pCircFrCnt2Timestamp[
                        (pFrameCount + pCircSize - 1 - itcol) % pCircSize] ?
                      (ts - pLatencySched.pCircFrCnt2Timestamp[
                         (pFrameCount + pCircSize - 1 - itcol) % pCircSize]) * 0.001 : 0;
        char header_name[24];
        std::sprintf(header_name, "%.1f", frame);
        table_header.push_back(std::make_tuple(header_name, 4, format_l));
      }

      table.SetHeader(table_header);
      FsView::gFsView.ViewMutex.LockRead();
      size_t fsid_count = pPenaltySched.pCircFrCnt2FsPenalties.begin()->size();

      for (size_t fsid = 1; fsid < fsid_count; fsid++) {
        if (!FsView::gFsView.mIdView.exists(fsid)) {
          continue;
        }

        table.AddSeparator();
        // for Upload
        TableData table_data;
        table_data.emplace_back();

        if (monitoring) {
          table_data.back().push_back(TableCell("AddedPenalties", format_ss));
        }

        table_data.back().push_back(TableCell((unsigned long long)fsid, format_l));
        table_data.back().push_back(TableCell("UL", format_ss));

        for (size_t itcol = 0; itcol < pCircSize; itcol++) {
          int value = pPenaltySched.pCircFrCnt2FsPenalties[
                        (pFrameCount + pCircSize - 1 - itcol) % pCircSize][fsid].ulScorePenalty;
          table_data.back().push_back(TableCell(value, format_l));
        }

        // for Download
        table_data.emplace_back();

        if (monitoring) {
          table_data.back().push_back(TableCell("AddedPenalties", format_ss));
          table_data.back().push_back(TableCell((unsigned long long)fsid, format_l));
        } else {
          table_data.back().push_back(TableCell("", format_ss));
        }

        table_data.back().push_back(TableCell("DL", format_ss));

        for (size_t itcol = 0; itcol < pCircSize; itcol++) {
          int value = pPenaltySched.pCircFrCnt2FsPenalties[
                        (pFrameCount + pCircSize - 1 - itcol) % pCircSize][fsid].dlScorePenalty;
          table_data.back().push_back(TableCell(value, format_l));
        }

        table.AddRows(table_data);
      }

      FsView::gFsView.ViewMutex.UnLockRead();
      ostr << table.GenerateTable(HEADER2).c_str();
    }

    //! fst2GeotreeEngine latency
    if (!monitoring) {
      ostr << "\n┏━> fst2GeotreeEngine latency\n";
    }

    struct timeval nowtv;

    gettimeofday(&nowtv, NULL);

    size_t nowms = nowtv.tv_sec * 1000 + nowtv.tv_usec / 1000;

    double avAge = 0.0;

    size_t count = 0;

    std::vector<std::tuple<unsigned long long,
        double, double, double, double, bool>> data_fst;

    for (auto it : pLatencySched.pFsId2LatencyStats) {
      if (it.getage(nowms) < 600000) { // consider only if less than a minute
        avAge += it.getage(nowms);
        count++;
      }
    }

    avAge /= (count ? count : 1);
    TableFormatterBase table_fst;

    if (!monitoring)
      table_fst.SetHeader({
      std::make_tuple("fsid", 6, format_ll),
      std::make_tuple("minimum", 10, format_f),
      std::make_tuple("averge", 10, format_f),
      std::make_tuple("maximum", 10, format_f),
      std::make_tuple("age(last)", 10, format_f)
    });
    else
      table_fst.SetHeader({
      std::make_tuple("type", 0, format_ss),
      std::make_tuple("fsid", 0, format_ll),
      std::make_tuple("min", 0, format_f),
      std::make_tuple("avg", 0, format_f),
      std::make_tuple("max", 0, format_f),
      std::make_tuple("age(last)", 0, format_f)
    });
    FsView::gFsView.ViewMutex.LockRead();

    for (size_t fsid = 1; fsid < pLatencySched.pFsId2LatencyStats.size(); fsid++) {
      if (!FsView::gFsView.mIdView.exists(fsid)) {
        continue;
      }

      // more than 1 minute, something is wrong
      if (pLatencySched.pFsId2LatencyStats[fsid].getage(nowms) > 600000) {
        data_fst.push_back(std::make_tuple(fsid, 0, 0, 0, 0, false));
      } else
        data_fst.push_back(std::make_tuple(fsid,
                                           pLatencySched.pFsId2LatencyStats[fsid].minlatency,
                                           pLatencySched.pFsId2LatencyStats[fsid].averagelatency,
                                           pLatencySched.pFsId2LatencyStats[fsid].maxlatency,
                                           pLatencySched.pFsId2LatencyStats[fsid].getage(nowms), true));
    }

    FsView::gFsView.ViewMutex.UnLockRead();

    for (auto it : data_fst) {
      TableData table_data;
      table_data.emplace_back();

      if (monitoring) {
        table_data.back().push_back(TableCell("fst2GeotreeEngine", format_ss));
      }

      if (std::get<0>(it) == 0) {
        table_data.back().push_back(TableCell("global", format_ss));
      } else {
        table_data.back().push_back(TableCell(std::get<0>(it), format_l));
      }

      if (std::get<5>(it)) {
        table_data.back().push_back(TableCell(std::get<1>(it) / scale, format_f, unit));
        table_data.back().push_back(TableCell(std::get<2>(it) / scale, format_f, unit));
        table_data.back().push_back(TableCell(std::get<3>(it) / scale, format_f, unit));
        table_data.back().push_back(TableCell(std::get<4>(it) / scale, format_f, unit));
      } else for (int i = 0; i < 4; i++) {
          table_data.back().push_back(TableCell(na, format_ss));
        }

      table_fst.AddRows(table_data);

      if (std::get<0>(it) == 0 && data_fst.size() > 1) {
        table_fst.AddSeparator();
      }
    }

    ostr << table_fst.GenerateTable(HEADER2).c_str();
  }

  // ==== run through the map of file systems
  unsigned geo_depth_max = 0;
  // Set for tree: group, num of line, depth, color, prefix_1, prefix_2,
  //               geotag[::fsid], host, leavs count, nodes count, status
  std::set<std::tuple<std::string, unsigned, unsigned, TableFormatterColor,
      unsigned, unsigned, std::string, std::string,
      int, int, std::string>> data_tree;
  // Set for snapshot: group, num of line, depth, color, prefix_1, prefix_2,
  //                   operation, operation_short, fsid, geotag/host,
  //                   free, repl, pidx, status, ulSc, dlSc, filR, totS
  std::set<std::tuple<std::string, unsigned, unsigned, TableFormatterColor,
      unsigned, unsigned, std::string, std::string, unsigned, std::string,
      int, int, int, std::string, int, int, int, double>> data_snapshot;

  for (auto it = pGroup2SchedTME.begin(); it != pGroup2SchedTME.end(); it++) {
    if (dispTree && (schedgroup.empty() || schedgroup == "*" ||
                     (schedgroup == it->second->group->mName))) {
      it->second->slowTree->display(data_tree, geo_depth_max, useColors);
    }

    if (dispSnaps && (schedgroup.empty() || schedgroup == "*" ||
                      (schedgroup == it->second->group->mName))) {
      if (optype.empty() || (optype == "plct")) {
        unsigned geo_depth_max_temp = 0;
        it->second->foregroundFastStruct->placementTree->recursiveDisplay(
          data_snapshot, geo_depth_max_temp, "Placement", "plct", useColors);
        geo_depth_max = (geo_depth_max_temp > geo_depth_max) ?
                        geo_depth_max_temp : geo_depth_max;
      }

      if (optype.empty() || (optype == "accsro")) {
        unsigned geo_depth_max_temp = 0;
        it->second->foregroundFastStruct->rOAccessTree->recursiveDisplay(
          data_snapshot, geo_depth_max, "Access RO", "accsro", useColors);
        geo_depth_max = (geo_depth_max_temp > geo_depth_max) ?
                        geo_depth_max_temp : geo_depth_max;
      }

      if (optype.empty() || (optype == "accsrw")) {
        unsigned geo_depth_max_temp = 0;
        it->second->foregroundFastStruct->rWAccessTree->recursiveDisplay(
          data_snapshot, geo_depth_max, "Access RW", "accsrw", useColors);
        geo_depth_max = (geo_depth_max_temp > geo_depth_max) ?
                        geo_depth_max_temp : geo_depth_max;
      }

      if (optype.empty() || (optype == "accsdrain")) {
        unsigned geo_depth_max_temp = 0;
        it->second->foregroundFastStruct->drnAccessTree->recursiveDisplay(
          data_snapshot, geo_depth_max, "Draining Access", "accsdrain", useColors);
        geo_depth_max = (geo_depth_max_temp > geo_depth_max) ?
                        geo_depth_max_temp : geo_depth_max;
      }

      if (optype.empty() || (optype == "plctdrain")) {
        unsigned geo_depth_max_temp = 0;
        it->second->foregroundFastStruct->drnPlacementTree->recursiveDisplay(
          data_snapshot, geo_depth_max, "Draining Placement", "plctdrain", useColors);
        geo_depth_max = (geo_depth_max_temp > geo_depth_max) ?
                        geo_depth_max_temp : geo_depth_max;
      }
    }
  }

  // ==== run through the map of file systems
  for (auto it = pPxyGrp2DpTME.begin(); it != pPxyGrp2DpTME.end(); it++) {
    if (dispTree &&
        (schedgroup.empty() || schedgroup == "*" || (schedgroup == it->first))) {
      std::string group_name = it->first + "(proxy)";
      it->second->slowTree->display(data_tree, geo_depth_max, useColors);
    }

    if (dispSnaps &&
        (schedgroup.empty() || schedgroup == "*" || (schedgroup == it->first))) {
      unsigned geo_depth_max_temp = 0;
      it->second->foregroundFastStruct->proxyAccessTree->recursiveDisplay(
        data_snapshot, geo_depth_max, "Proxy group", "proxy", useColors);
      geo_depth_max = (geo_depth_max_temp > geo_depth_max) ?
                      geo_depth_max_temp : geo_depth_max;
    }
  }

  // Output for "geosched show tree"
  TableFormatterBase table_tree;
  TableHeader table_header;
  table_header.push_back(std::make_tuple("group", 6, format_ss));
  table_header.push_back(std::make_tuple("geotag", 6, format_ss));

  if (!monitoring && geo_depth_max > 1) {
    for (unsigned i = 1; i < geo_depth_max; i++) {
      std::string name = "lev" + std::to_string(i);
      table_header.push_back(std::make_tuple(name, 4, format_ss));
    }
  }

  table_header.push_back(std::make_tuple("fsid", 4, format_l));
  table_header.push_back(std::make_tuple("node", 12, format_s));
  table_header.push_back(std::make_tuple("branches", 5, format_l));
  table_header.push_back(std::make_tuple("leavs", 5, format_l));
  table_header.push_back(std::make_tuple("sum", 3, format_l));
  table_header.push_back(std::make_tuple("status", 6, format_s));
  table_tree.SetHeader(table_header);
  unsigned prefix[geo_depth_max + 1];

  for (auto it : data_tree) {
    unsigned geo_depth = 0;
    std::string geotag_temp = std::get<6>(it);

    while (geotag_temp.find("::") != std::string::npos) {
      geotag_temp.erase(0, geotag_temp.find("::") + 2);
      geo_depth++;
    }

    TableData table_data;
    table_data.emplace_back();

    // Print group (depth=1)
    if (std::get<2>(it) == 1) {
      for (unsigned i = 0; i < geo_depth_max + 1; i++) {
        prefix[i] = 0;
      }

      table_tree.AddSeparator();
      table_data.back().push_back(TableCell(std::get<0>(it), format_s, "", false,
                                            std::get<3>(it)));

      for (unsigned i = 0; i < geo_depth_max + 2; i++) {
        table_data.back().push_back(TableCell("", format_s, "",
                                              true)); // blank cell after group

        if (monitoring && i == 2) {
          break;
        }
      }
    }
    // Print geotag (depth=2)
    else if (std::get<2>(it) == 2) {
      if (!monitoring) {
        if (geo_depth == 0) {
          prefix[0] = std::get<5>(it);
          table_data.back().push_back(TableCell(prefix[0], "t"));
          table_data.back().push_back(TableCell(std::get<6>(it), format_s, "", false,
                                                std::get<3>(it)));

          for (unsigned i = 0; i < geo_depth_max - 1; i++) { // after arrows
            table_data.back().push_back(TableCell("", format_s, "", true));
          }
        } else {
          prefix[geo_depth - 1] = std::get<4>(it);
          prefix[geo_depth] = std::get<5>(it);

          for (unsigned i = 0; i <= geo_depth; i++) { // arrows
            table_data.back().push_back(TableCell(prefix[i], "t"));
          }

          std::string name = std::get<6>(it).substr(std::get<6>(it).rfind("::") + 2);
          table_data.back().push_back(TableCell(name, format_s, "", false,
                                                std::get<3>(it)));

          for (unsigned i = 1; i < geo_depth_max - geo_depth; i++) {
            table_data.back().push_back(TableCell("", format_s));
          }
        }
      } else {
        table_data.back().push_back(TableCell(std::get<0>(it), format_s));
        table_data.back().push_back(TableCell(std::get<6>(it), format_s));
      }

      table_data.back().push_back(TableCell("", format_s, "", true));
      table_data.back().push_back(TableCell("", format_s, "", true));
    }
    // Print fsid and node (depth=3)
    else if (std::get<2>(it) == 3) {
      if (!monitoring) {
        if (geo_depth > 0) {
          prefix[geo_depth - 1] = std::get<4>(it);
          prefix[geo_depth] = std::get<5>(it);

          for (unsigned i = 0; i <= geo_depth; i++) { // arrows
            unsigned arrow = (i == geo_depth &&
                              geo_depth_max - geo_depth > 0) ? prefix[i] + 2 : prefix[i];
            table_data.back().push_back(TableCell(arrow, "t"));
          }

          for (unsigned i = 0; i < geo_depth_max - geo_depth; i++) { // extended arrows
            unsigned arrow = (i == geo_depth_max - geo_depth - 1) ? 7 : 6;
            table_data.back().push_back(TableCell(arrow, "t"));
          }
        }
      } else {
        std::string geotag = std::get<6>(it).substr(0, std::get<6>(it).rfind("::"));
        table_data.back().push_back(TableCell(std::get<0>(it), format_s));
        table_data.back().push_back(TableCell(geotag, format_s));
      }

      unsigned fsid = std::atoi(std::get<6>(it).substr(std::get<6>
                                (it).rfind("::") + 2).c_str());
      table_data.back().push_back(TableCell(fsid, format_l, "", false,
                                            std::get<3>(it)));
      table_data.back().push_back(TableCell(std::get<7>(it), format_s, "", false,
                                            std::get<3>(it)));
    }

    // Print other columns
    table_data.back().push_back(TableCell(std::get<9>(it) - std::get<8>(it),
                                          format_l));
    table_data.back().push_back(TableCell(std::get<8>(it), format_l));
    table_data.back().push_back(TableCell(std::get<9>(it), format_l));
    table_data.back().push_back(TableCell(std::get<10>(it), format_s, "",
                                          (std::get<2>(it) != 3)));
    table_tree.AddRows(table_data);
  }

  ostr << table_tree.GenerateTable(HEADER).c_str();
  // Output for "geosched show snapshot"
  std::string geotag = "";
  size_t operation_count = 0;
  TableFormatterBase table_snapshot;
  TableHeader snapshot_header;
  snapshot_header.push_back(std::make_tuple("group", 6, format_ss));
  snapshot_header.push_back(std::make_tuple("operation", 6, format_ss));
  snapshot_header.push_back(std::make_tuple("geotag", 6, format_ss));

  if (!monitoring && geo_depth_max > 1) {
    for (unsigned i = 1; i < geo_depth_max; i++) {
      std::string name = "lev" + std::to_string(i);
      snapshot_header.push_back(std::make_tuple(name, 2, format_ss));
    }
  }

  snapshot_header.push_back(std::make_tuple("fsid", 4, format_l));
  snapshot_header.push_back(std::make_tuple("node", 12, format_s));
  snapshot_header.push_back(std::make_tuple("free", 4, format_l));
  snapshot_header.push_back(std::make_tuple("repl", 4, format_l));
  snapshot_header.push_back(std::make_tuple("pidx", 4, format_l));
  snapshot_header.push_back(std::make_tuple("status", 6, format_s));
  snapshot_header.push_back(std::make_tuple("ulSc", 4, format_l));
  snapshot_header.push_back(std::make_tuple("dlSc", 4, format_l));
  snapshot_header.push_back(std::make_tuple("filR", 4, format_l));
  snapshot_header.push_back(std::make_tuple("totS", 4, format_lll));
  table_snapshot.SetHeader(snapshot_header);
  set<std::string> operations;

  for (auto it : data_snapshot) { // we need count of used operations
    operations.insert(std::get<6>(it));
  }

  unsigned geo_depth = 0;

  for (auto it : data_snapshot) {
    if (std::get<2>(it) == 2) {
      geo_depth = 0;
      std::string geotag_temp = std::get<9>(it);

      while (geotag_temp.find("::") != std::string::npos) {
        geotag_temp.erase(0, geotag_temp.find("::") + 2);
        geo_depth++;
      }
    }

    TableData table_data;
    table_data.emplace_back();

    // Print group (depth=1)
    if (std::get<2>(it) == 1) {
      for (unsigned i = 0; i < geo_depth_max + 1; i++) {
        prefix[i] = 0;
      }

      if (!monitoring) {
        if (schedgroup == "*" || std::get<6>(it) == "Placement" ||
            std::get<1>(it) == 0) {
          table_snapshot.AddSeparator();
          table_data.back().push_back(TableCell(std::get<0>(it), format_s, "", false,
                                                std::get<3>(it)));
          table_data.emplace_back();
          operation_count = 0;
        }

        operation_count++;
        unsigned tree_arrow = (schedgroup == "*" ||
                               operation_count == operations.size()) ? 2 : 3;
        table_data.back().push_back(TableCell(tree_arrow, "t"));
        table_data.back().push_back(TableCell(std::get<6>(it), format_s, "", false,
                                              std::get<3>(it)));
      } else {
        table_data.back().push_back(TableCell(std::get<0>(it), format_s));
        table_data.back().push_back(TableCell(std::get<7>(it), format_s));
      }

      for (unsigned i = 0; i < geo_depth_max + 2; i++) {
        table_data.back().push_back(TableCell("", format_s, "",
                                              true)); // blank cell after group

        if (monitoring && i == 2) {
          break;
        }
      }
    }
    // Print geotag (depth=2)
    else if (std::get<2>(it) == 2) {
      geotag = std::get<9>(it);

      if (!monitoring) {
        unsigned tree_arrow = (schedgroup == "*" ||
                               operation_count == operations.size()) ? 0 : 1;
        table_data.back().push_back(TableCell(tree_arrow, "t"));

        if (geo_depth == 0) {
          prefix[0] = std::get<5>(it);
          table_data.back().push_back(TableCell(prefix[0], "t"));
          table_data.back().push_back(TableCell(geotag, format_s, "", false,
                                                std::get<3>(it)));

          for (unsigned i = 0; i < geo_depth_max - 1; i++) { // after arrows
            table_data.back().push_back(TableCell("", format_s, "", true));
          }
        } else {
          prefix[geo_depth - 1] = std::get<4>(it);
          prefix[geo_depth] = std::get<5>(it);

          for (unsigned i = 0; i <= geo_depth; i++) { // arrows
            table_data.back().push_back(TableCell(prefix[i], "t"));
          }

          std::string name = geotag.substr(geotag.rfind("::") + 2);
          table_data.back().push_back(TableCell(name, format_s, "", false,
                                                std::get<3>(it)));

          for (unsigned i = 1; i < geo_depth_max - geo_depth; i++) {
            table_data.back().push_back(TableCell("", format_s));
          }
        }
      } else {
        table_data.back().push_back(TableCell(std::get<0>(it), format_s));
        table_data.back().push_back(TableCell(std::get<7>(it), format_s));
        table_data.back().push_back(TableCell(geotag, format_s));
      }

      table_data.back().push_back(TableCell("", format_s, "", true));
      table_data.back().push_back(TableCell("", format_s, "", true));
    }
    // Print fsid and node (depth=3)
    else if (std::get<2>(it) == 3) {
      if (!monitoring) {
        unsigned tree_arrow = (schedgroup == "*" ||
                               operation_count == operations.size()) ? 0 : 1;
        table_data.back().push_back(TableCell(tree_arrow, "t"));
        prefix[geo_depth] = std::get<4>(it);
        prefix[geo_depth + 1] = std::get<5>(it);

        for (unsigned i = 0; i <= geo_depth + 1; i++) { // arrows
          unsigned arrow = (i == geo_depth + 1 &&
                            geo_depth_max - geo_depth - 1 > 0) ? prefix[i] + 2 : prefix[i];
          table_data.back().push_back(TableCell(arrow, "t"));
        }

        for (unsigned i = 0; i < geo_depth_max - geo_depth - 1; i++) { // extended arrow
          unsigned arrow = (i == geo_depth_max - geo_depth - 2) ? 7 : 6;
          table_data.back().push_back(TableCell(arrow, "t"));
        }
      } else {
        table_data.back().push_back(TableCell(std::get<0>(it), format_s));
        table_data.back().push_back(TableCell(std::get<7>(it), format_s));
        table_data.back().push_back(TableCell(geotag, format_s));
      }

      table_data.back().push_back(TableCell(std::get<8>(it), format_l, "", false,
                                            std::get<3>(it)));
      table_data.back().push_back(TableCell(std::get<9>(it), format_s, "", false,
                                            std::get<3>(it)));
    }

    // Print other columns
    table_data.back().push_back(TableCell(std::get<10>(it), format_l));
    table_data.back().push_back(TableCell(std::get<11>(it), format_l));
    table_data.back().push_back(TableCell(std::get<12>(it), format_l));
    table_data.back().push_back(TableCell(std::get<13>(it), format_s));
    table_data.back().push_back(TableCell(std::get<14>(it), format_l));
    table_data.back().push_back(TableCell(std::get<15>(it), format_l));
    table_data.back().push_back(TableCell(std::get<16>(it), format_l));
    table_data.back().push_back(TableCell(std::get<17>(it), format_lll));
    table_snapshot.AddRows(table_data);
  }

  ostr << table_snapshot.GenerateTable(HEADER).c_str();
  info = ostr.str();
}


bool
GeoTreeEngine::placeNewReplicasOneGroup(FsGroup* group,
                                        const size_t& nNewReplicas, vector<FileSystem::fsid_t>* newReplicas,
                                        ino64_t inode, std::vector<std::string>* dataProxys,
                                        std::vector<std::string>* firewallEntryPoint,
                                        SchedType type,
                                        vector<FileSystem::fsid_t>* existingReplicas,
                                        std::vector<std::string>* fsidsgeotags,
                                        unsigned long long bookingSize,
                                        const std::string& startFromGeoTag,
                                        const std::string& clientGeoTag,
                                        const size_t& nCollocatedReplicas,
                                        vector<FileSystem::fsid_t>* excludeFs,
                                        vector<string>* excludeGeoTags)
{
  assert(nNewReplicas);
  assert(newReplicas);
  std::vector<SchedTME*> entries;
  // find the entry in the map
  SchedTME* entry;
  {
    RWMutexReadLock lock(this->pTreeMapMutex);

    if (!pGroup2SchedTME.count(group)) {
      eos_err("could not find the requested placement group in the map");
      return false;
    }

    entry = pGroup2SchedTME[group];
    AtomicInc(entry->fastStructLockWaitersCount);
  }
  // readlock the original fast structure
  entry->doubleBufferMutex.LockRead();
  // locate the existing replicas and the excluded fs in the tree
  vector<SchedTreeBase::tFastTreeIdx> newReplicasIdx(nNewReplicas),
         *existingReplicasIdx = NULL, *excludeFsIdx = NULL;
  newReplicasIdx.resize(0);

  if (existingReplicas) {
    existingReplicasIdx = new vector<SchedTreeBase::tFastTreeIdx>
    (existingReplicas->size());
    existingReplicasIdx->resize(0);
    int count = 0;

    for (auto it = existingReplicas->begin(); it != existingReplicas->end();
         ++it , ++count) {
      const SchedTreeBase::tFastTreeIdx* idx =
        static_cast<const SchedTreeBase::tFastTreeIdx*>(0);

      if (!entry->foregroundFastStruct->fs2TreeIdx->get(*it, idx) &&
          !(*fsidsgeotags)[count].empty()) {
        // the fs is not in that group.
        // this could happen because the former file scheduler
        // could place replicas across multiple groups
        // with the new geoscheduler, it should not happen
        // in that case, we try to match a filesystem having the same geotag
        SchedTreeBase::tFastTreeIdx idx =
          entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode((
                *fsidsgeotags)[count].c_str());

        if (idx &&
            (*entry->foregroundFastStruct->treeInfo)[idx].nodeType ==
            SchedTreeBase::TreeNodeInfo::fs) {
          if ((std::find(existingReplicasIdx->begin(), existingReplicasIdx->end(),
                         idx) == existingReplicasIdx->end())) {
            existingReplicasIdx->push_back(idx);
          }
        }
        // if we can't find any such filesystem, the information is not taken into account
        // (and then can lead to unoptimal placement
        else {
          eos_debug("could not place preexisting replica on the fast tree");
        }

        continue;
      }

      if (idx) {
        existingReplicasIdx->push_back(*idx);
      }
    }
  }

  if (excludeFs) {
    excludeFsIdx = new vector<SchedTreeBase::tFastTreeIdx>(excludeFs->size());
    excludeFsIdx->resize(0);

    for (auto it = excludeFs->begin(); it != excludeFs->end(); ++it) {
      const SchedTreeBase::tFastTreeIdx* idx;

      if (!entry->foregroundFastStruct->fs2TreeIdx->get(*it, idx)) {
        // the excluded fs might belong to another group
        // so it's not an error condition
        // eos_warning("could not place excluded fs on the fast tree");
        continue;
      }

      excludeFsIdx->push_back(*idx);
    }
  }

  if (excludeGeoTags) {
    if (!excludeFsIdx) {
      excludeFsIdx = new vector<SchedTreeBase::tFastTreeIdx>(excludeGeoTags->size());
      excludeFsIdx->resize(0);
    }

    for (auto it = excludeGeoTags->begin(); it != excludeGeoTags->end(); ++it) {
      SchedTreeBase::tFastTreeIdx idx;
      idx = entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(
              it->c_str());
      excludeFsIdx->push_back(idx);
    }
  }

  SchedTreeBase::tFastTreeIdx startFromNode = 0;

  if (!startFromGeoTag.empty()) {
    startFromNode =
      entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(
        startFromGeoTag.c_str());
  } else if (!clientGeoTag.empty()) {
    startFromNode =
      entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(
        clientGeoTag.c_str());
  }

  // actually do the job
  bool success = false;

  switch (type) {
  case regularRO:
  case regularRW:
    success = placeNewReplicas(entry, nNewReplicas, &newReplicasIdx,
                               entry->foregroundFastStruct->placementTree,
                               existingReplicasIdx, bookingSize, startFromNode,
                               nCollocatedReplicas, excludeFsIdx);
    break;

  case draining:
    success = placeNewReplicas(entry, nNewReplicas, &newReplicasIdx,
                               entry->foregroundFastStruct->drnPlacementTree,
                               existingReplicasIdx, bookingSize, startFromNode,
                               nCollocatedReplicas, excludeFsIdx);
    break;

  default:
    break;
  }

  if (!success) {
    goto cleanup;
  }

  // fill the resulting vector and
  // update the fastTree UlScore and DlScore by applying the penalties
  newReplicas->resize(0);

  for (auto it = newReplicasIdx.begin(); it != newReplicasIdx.end(); ++it) {
    const SchedTreeBase::tFastTreeIdx* idx = NULL;
    const unsigned int fsid = (*entry->foregroundFastStruct->treeInfo)[*it].fsId;

    if (!entry->foregroundFastStruct->fs2TreeIdx->get(fsid, idx)) {
      eos_crit("inconsistency : cannot retrieve index of selected fs though "
               "it should be in the tree");
      success = false;
      goto cleanup;
    }

    const char netSpeedClass =
      (*entry->foregroundFastStruct->treeInfo)[*idx].netSpeedClass;
    newReplicas->push_back(fsid);

    // Apply the penalties
    if (entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.dlScore >
        0) {
      applyDlScorePenalty(entry, *idx,
                          pPenaltySched.pPlctDlScorePenalty[netSpeedClass]);
    }

    if (entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.ulScore >
        0) {
      applyUlScorePenalty(entry, *idx,
                          pPenaltySched.pPlctUlScorePenalty[netSpeedClass]);
    }
  }

  if (dataProxys || firewallEntryPoint) {
    entries.assign(newReplicasIdx.size(), entry);
  }

  // find proxy for filesticky scheduling
  if (dataProxys) {
    if (!findProxy(newReplicasIdx, entries, inode, dataProxys, NULL,
                   pProxyCloseToFs ? "" : clientGeoTag, filesticky)) {
      success = false;
      goto cleanup;
    }
  }

  // find the firewall entry point if needed
  if (firewallEntryPoint) {
    std::vector<std::string> firewallProxyGroups(newReplicasIdx.size());

    // if there are some access geotag mapping rules, use them
    if (pAccessGeotagMapping.inuse && pAccessProxygroup.inuse)
      for (size_t i = 0; i < newReplicasIdx.size(); i++) {
        if (clientGeoTag.empty() ||
            accessReqFwEP((
                            *entries[i]->foregroundFastStruct->treeInfo)[newReplicasIdx[i]].fullGeotag ,
                          clientGeoTag)) {
          firewallProxyGroups[i] = accessGetProxygroup((
                                     *entries[i]->foregroundFastStruct->treeInfo)[newReplicasIdx[i]].fullGeotag);
        }
      }

    // Use the dataproxys as entrypoints if possible
    if (dataProxys) {
      *firewallEntryPoint = *dataProxys;
    }

    if (!findProxy(newReplicasIdx, entries, inode, firewallEntryPoint,
                   &firewallProxyGroups, pProxyCloseToFs ? "" : clientGeoTag, any)) {
      success = false;
      goto cleanup;
    }
  }

  // find proxy in the right proxygroup if any
  if (dataProxys) {
    // If we already have some firewall entry points, pass them to the findProxy
    // procedure to check if it's needed to find a distinct data proxy
    // use the entrypoints as dataproxy if possible
    if (firewallEntryPoint) {
      *dataProxys = *firewallEntryPoint;
    }

    if (!findProxy(newReplicasIdx, entries, inode, dataProxys, NULL,
                   pProxyCloseToFs ? "" : clientGeoTag, regular)) {
      success = false;
      goto cleanup;
    }
  }

  // Unlock, cleanup
cleanup:

  if (!success) {
    newReplicas->clear();
  }

  entry->doubleBufferMutex.UnLockRead();
  AtomicDec(entry->fastStructLockWaitersCount);

  if (existingReplicasIdx) {
    delete existingReplicasIdx;
  }

  if (excludeFsIdx) {
    delete excludeFsIdx;
  }

  return success;
}

// Would be better as defined locally in find Proxy
// but it is not supported by gcc 4.4
struct TreeInfoFsIdComparator {
  SchedTreeBase::FastTreeInfo* nodesinfo;
  TreeInfoFsIdComparator(SchedTreeBase::FastTreeInfo* infos)
  {
    nodesinfo = infos;
  }
  bool operator()(const SchedTreeBase::tFastTreeIdx& a,
                  const SchedTreeBase::tFastTreeIdx& b) const
  {
    return (*nodesinfo)[a].fsId < (*nodesinfo)[b].fsId;
  }
};

bool GeoTreeEngine::findProxy(const std::vector<SchedTreeBase::tFastTreeIdx>&
                              fsIdxs,
                              std::vector<SchedTME*> entries,
                              ino64_t inode,
                              std::vector<std::string>* dataProxys,
                              std::vector<std::string>* proxyGroups,
                              const std::string& clientgeotag,
                              tProxySchedType proxyschedtype)
{
  // re initialize result vector
  dataProxys->resize(fsIdxs.size());
  const std::string* fsproxygroup = 0;
  DataProxyTME* pxyentry = NULL;
  FastGatewayAccessTree* tree = NULL;
  std::string sgeotag;

  for (size_t i = 0; i < fsIdxs.size(); i++) {
    const std::string* geotag = NULL;
    // get the proxygroup
    // WARNING: entries[i]->doubleBufferMutex should be locked by the caller of findProxy

    if (!(*dataProxys)[i].empty() && (*dataProxys)[i] != "<none>") {
      if (pPxyHost2DpTMEs.count((*dataProxys)[i])) {
        const auto& TMEs = pPxyHost2DpTMEs[(*dataProxys)[i]];
        // If dataProxys already contains proxy hostnames, check first if they
        // already do the job for the given proxygroup.
        bool isInRightPxyGrp = false;

        if (proxyGroups) {
          for (auto it = TMEs.begin(); it != TMEs.end(); it++) {
            if ((*it)->slowTree->getName() == (*proxyGroups)[i]) {
              isInRightPxyGrp = true;
              break;
            }
          }
        }

        if (isInRightPxyGrp) {
          continue;
        }

        {
          auto entry = (*TMEs.begin());

          // we don't want to lock the pxyentry which is already locked
          if (entry != pxyentry) {
            AtomicInc(entry->fastStructLockWaitersCount);
            entry->doubleBufferMutex.LockRead();
          }

          // if they don't, take their geotag as a staring point
          sgeotag =
            (*TMEs.begin())->host2SlowTreeNode[(*dataProxys)[i]]->pNodeInfo.fullGeotag;
          geotag = &sgeotag;

          if (entry != pxyentry) {
            entry->doubleBufferMutex.UnLockRead();
            AtomicDec(entry->fastStructLockWaitersCount);
          }
        }
      }
    }

    if (proxyGroups) {
      fsproxygroup = &((*proxyGroups)[i]);
    } else {
      fsproxygroup = &
                     (*entries[i]->foregroundFastStruct->treeInfo)[fsIdxs[i]].proxygroup;
    }

    if (fsproxygroup->empty() ||
        (*fsproxygroup) ==  "<none>") {
      // No proxygroup, nothing to do, there will be an entry with an empty string
      (*dataProxys)[i].clear();
      continue;
    }

    // If we don't have a proxy to match, if a client geotag is given then use
    // it else use the file system client
    bool trimlastlevel = geotag || clientgeotag.empty();

    if (!geotag) {
      geotag = (clientgeotag.empty() ? &
                ((*(entries[i]->foregroundFastStruct->treeInfo))[fsIdxs[i]].fullGeotag) :
                &clientgeotag);
    }

    // The deepest intermediate node is a numeric id for both scheduling and GW
    // trees and they are unrelated. We don't want to keep this to project the
    // fst location on the gw tree as it would not make sense lock it for each
    //  new fs.
    RWMutexReadLock lock(this->pPxyTreeMapMutex);

    if (!pPxyGrp2DpTME.count(*fsproxygroup)) {
      eos_err("could not find the requested proxy group %s in the map",
              fsproxygroup->c_str());
      return false;
    }

    pxyentry = pPxyGrp2DpTME[*fsproxygroup];
    AtomicInc(pxyentry->fastStructLockWaitersCount);
    // readlock the original fast structure
    pxyentry->doubleBufferMutex.LockRead();

    // copy the fasttree
    if (pxyentry->foregroundFastStruct->proxyAccessTree->copyToBuffer((
          char*)tlGeoBuffer, gGeoBufferSize)) {
      eos_crit("could not make a working copy of the fast tree for proxygroup %s",
               fsproxygroup->c_str());
      pxyentry->doubleBufferMutex.UnLockRead();
      AtomicDec(pxyentry->fastStructLockWaitersCount);
      return false;
    }

    tree = (FastGatewayAccessTree*)tlGeoBuffer;
    // get the closest node from the filesystem
    SchedTreeBase::tFastTreeIdx idx;
    idx = pxyentry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(
            trimlastlevel ? std::string(*geotag, 0,
                                        geotag->rfind("::")).c_str() : geotag->c_str());
    bool schedsuccess = false;
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    if (proxyschedtype == filesticky) {
      // scheduling should consistently go through the same (firewallentrypoint,proxy)
      // this is to do the caching of the file only on one proxy
      // serving a same file from two proxies is not optimal but it is not mendatory neither
      if ((*entries[i]->foregroundFastStruct->treeInfo)[fsIdxs[i]].fileStickyProxyDepth
          < 0) {
        schedsuccess = true;
      }
      // first find the best proxy
      else {
        // then consider all the possible proxy in the same proxygroup
        // within the subtree starting at the best proxy and going uproot by
        // (*pxyentry->foregroundFastStruct->treeInfo)[idx].fileStickyProxyDepth
        // allocate a vectors to get the proxies
        auto s = pxyentry->foregroundFastStruct->treeInfo->size();
        std::vector<SchedTreeBase::tFastTreeIdx> proxiesIdxs(s), upRootLevels(s),
            upRootLevelsIdxs(s);
        SchedTreeBase::tFastTreeIdx upRootLevelsCount = 0;
        SchedTreeBase::tFastTreeIdx np = 0;

        // get all the proxies
        if ((np = tree->findFreeSlotsAll(&proxiesIdxs[0], proxiesIdxs.size(), idx, true,
                                         SchedTreeBase::None, &upRootLevelsCount,
                                         &upRootLevelsIdxs[0], &upRootLevels[0]))) {
          schedsuccess = (np != 0);

          if (schedsuccess) {
            if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
              stringstream ss;
              ss << " all proxys are:";

              for (auto it = proxiesIdxs.begin(); it != proxiesIdxs.end(); it++) {
                ss << (*pxyentry->foregroundFastStruct->treeInfo)[*it].hostport;
                ss << "(" << (*pxyentry->foregroundFastStruct->treeInfo)[*it].fullGeotag << ")";

                if (it != proxiesIdxs.end() - 1) {
                  ss << ",";
                }
              }

              ss << " upRootLevels are:";

              for (auto it = upRootLevels.begin(); it != upRootLevels.end(); it++) {
                ss << (int)*it;

                if (it != upRootLevels.end() - 1) {
                  ss << ",";
                }
              }

              ss << " upRootLevelsIdxs are:";

              for (auto it = upRootLevelsIdxs.begin(); it != upRootLevelsIdxs.end(); it++) {
                ss << (int)*it;

                if (it != upRootLevelsIdxs.end() - 1) {
                  ss << ",";
                }
              }

              ss << " taken from idx:" << idx << "(" << *geotag << ")";
              eos_debug("%s", ss.str().c_str());
            }

            // keep only the proxies within the allowed uproot level, if any
            int uprlev = 0;

            while (
              uprlev < upRootLevelsCount &&
              upRootLevels[uprlev] <=
              (*entries[i]->foregroundFastStruct->treeInfo)[fsIdxs[i]].fileStickyProxyDepth
            ) {
              uprlev++;
            }

            if (uprlev == 0) {
              // no proxy with a right uproot level
              schedsuccess = false;
            } else {
              int resize = (uprlev == upRootLevelsCount) ? -1 : upRootLevelsIdxs[uprlev];

              if (resize > 0) {
                proxiesIdxs.resize(resize);
              } else {
                proxiesIdxs.resize(np);
              }

              // sort the proxies by fsid
              TreeInfoFsIdComparator cmp(pxyentry->foregroundFastStruct->treeInfo);
              std::sort(proxiesIdxs.begin(), proxiesIdxs.end(), cmp);
              // take the proxy
              idx = proxiesIdxs[inode % proxiesIdxs.size()];
              // if it succeeds, feel the corresponding element of the return vector
              (*dataProxys)[i] = (*pxyentry->foregroundFastStruct->treeInfo)[idx].hostport;

              if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
                stringstream ss;
                ss << "file sticky proxy scheduling fs:" <<
                   (*entries[i]->foregroundFastStruct->treeInfo)[fsIdxs[i]].fsId;
                ss << " | fileStickyProxyDepth:" << (int)(
                     *entries[i]->foregroundFastStruct->treeInfo)[fsIdxs[i]].fileStickyProxyDepth;
                ss << " | possible proxys are:";

                for (auto it = proxiesIdxs.begin(); it != proxiesIdxs.end(); it++) {
                  ss << (*pxyentry->foregroundFastStruct->treeInfo)[*it].hostport;
                  ss << "(" << (*pxyentry->foregroundFastStruct->treeInfo)[*it].fullGeotag << ")";

                  if (it != proxiesIdxs.end() - 1) {
                    ss << ",";
                  }
                }

                ss << " | inode:" << inode;
                ss << " | selected host is:" <<
                   (*pxyentry->foregroundFastStruct->treeInfo)[idx].hostport;
                eos_debug("%s", ss.str().c_str());
              }
            }
          }
        }
      }
    } else {
      if (proxyschedtype == any
          || ((*entries[i]->foregroundFastStruct->treeInfo)[fsIdxs[i]].fileStickyProxyDepth
              < 0 && proxyschedtype == regular)) {
        // get the proxy
        if (!(schedsuccess = tree->findFreeSlot(idx, idx,
                                                true /*allow uproot if necessary*/, false, true /*skipSaturated*/))) {
          (*dataProxys)[i] = (*pxyentry->foregroundFastStruct->treeInfo)[idx].hostport;
        } else {
          if ((schedsuccess = tree->findFreeSlot(idx, idx,
                                                 true /*allow uproot if necessary*/, false, false /*skipSaturated*/)))
            // if it succeeds, feel the corresponding element of the return vector
          {
            (*dataProxys)[i] = (*pxyentry->foregroundFastStruct->treeInfo)[idx].hostport;
          }
        }
      } else {
        schedsuccess = true;  // nothing to do
      }
    }

    // if the scheduling failed, throw an error
    if (!schedsuccess) {
      eos_err("could not find a proxy for proxygroup %s", fsproxygroup->c_str());
      std::stringstream ss;
      ss << "tree is as follow\n" << (*tree);
      eos_err(ss.str().c_str());
      pxyentry->doubleBufferMutex.UnLockRead();
      AtomicDec(pxyentry->fastStructLockWaitersCount);
      return false;
    }

    // unlock it for each new fs
    pxyentry->doubleBufferMutex.UnLockRead();
    AtomicDec(pxyentry->fastStructLockWaitersCount);
  }

  return true;
}

int GeoTreeEngine::accessHeadReplicaMultipleGroup(const size_t& nAccessReplicas,
    unsigned long& fsIndex,
    std::vector<eos::common::FileSystem::fsid_t>* existingReplicas,
    ino64_t inode,
    std::vector<std::string>* dataProxys,
    std::vector<std::string>* firewallEntryPoint,
    SchedType type,
    const std::string& accesserGeotag,
    const eos::common::FileSystem::fsid_t& forcedFsId,
    std::vector<eos::common::FileSystem::fsid_t>* unavailableFs)
{
  int returnCode = ENODATA;
  assert(nAccessReplicas);
  assert(existingReplicas);

  // Check that enough replicas exist already
  if (nAccessReplicas > existingReplicas->size()) {
    eos_debug("not enough replica : has %d and requires %d :",
              (int)existingReplicas->size(), (int)nAccessReplicas);
    return EROFS;
  }

  // Check if the forced replicas (if any) are among the existing replicas
  if (forcedFsId > 0 &&
      (std::find(existingReplicas->begin(), existingReplicas->end(),
                 forcedFsId) == existingReplicas->end())) {
    return ENODATA;
  }

  // Find the group holdings the fs of the existing replicas and check that the
  // replicas are available
  size_t availFsCount = 0;
  eos::mgm::SchedTreeBase::TreeNodeSlots freeSlot;
  freeSlot.freeSlotsCount = 1;
  std::vector<eos::common::FileSystem::fsid_t>::iterator it;
  std::vector<SchedTreeBase::tFastTreeIdx> ERIdx;
  ERIdx.reserve(existingReplicas->size());
  std::vector<SchedTME*> entries;
  entries.reserve(existingReplicas->size());
  // Maps tree maps entries (i.e. scheduling groups) to fs ids containing an
  // available replica and the corresponding fastTreeIndex
  map<SchedTME*, vector< pair<FileSystem::fsid_t, SchedTreeBase::tFastTreeIdx> > >
  entry2FsId;
  SchedTME* entry = NULL;
  {
    // Lock the scheduling group -> trees map so that the a map entry cannot
    // be delete while processing it.
    RWMutexReadLock lock(this->pTreeMapMutex);

    for (auto exrepIt = existingReplicas->begin();
         exrepIt != existingReplicas->end(); exrepIt++) {
      auto mentry = pFs2SchedTME.find(*exrepIt);

      // If we cannot find the fs in any group, there is an inconsistency somewhere
      if (mentry == pFs2SchedTME.end()) {
        eos_warning("cannot find the existing replica in any scheduling group");
        continue;
      }

      entry = mentry->second;

      // lock the double buffering to make sure all the fast trees are not modified
      if (!entry2FsId.count(entry)) {
        // if the entry is already there, it was locked already
        entry->doubleBufferMutex.LockRead();
        // to prevent the destruction of the entry
        AtomicInc(entry->fastStructLockWaitersCount);
      }

      const SchedTreeBase::tFastTreeIdx* idx;

      if (!entry->foregroundFastStruct->fs2TreeIdx->get(*exrepIt, idx)) {
        eos_warning("cannot find fs in the scheduling group in the 2nd pass");

        if (!entry2FsId.count(entry)) {
          entry->doubleBufferMutex.UnLockRead();
          AtomicDec(entry->fastStructLockWaitersCount);
        }

        continue;
      }

      // take the fastindex of each existing replica
      ERIdx.push_back(*idx);
      entries.push_back(entry);
      // check if the fs is available
      bool isValid = false;

      if (std::find(unavailableFs->begin(), unavailableFs->end(),
                    *exrepIt) == unavailableFs->end()) {
        switch (type) {
        case regularRO:
          isValid = entry->foregroundFastStruct->rOAccessTree->pBranchComp.isValidSlot(
                      &entry->foregroundFastStruct->rOAccessTree->pNodes[*idx].fsData, &freeSlot);
          break;

        case regularRW:
          isValid = entry->foregroundFastStruct->rWAccessTree->pBranchComp.isValidSlot(
                      &entry->foregroundFastStruct->rWAccessTree->pNodes[*idx].fsData, &freeSlot);
          break;

        case draining:
          isValid = entry->foregroundFastStruct->drnAccessTree->pBranchComp.isValidSlot(
                      &entry->foregroundFastStruct->drnAccessTree->pNodes[*idx].fsData, &freeSlot);
          break;

        default:
          break;
        }
      }

      if (isValid) {
        entry2FsId[entry].push_back(make_pair(*exrepIt, *idx));
        availFsCount++;
      } else {
        // create an empty entry in the map if needed
        if (!entry2FsId.count(entry)) {
          entry2FsId[entry] =
            vector< pair<FileSystem::fsid_t, SchedTreeBase::tFastTreeIdx> >();
        }

        // update the unavailable fs
        unavailableFs->push_back(*exrepIt);
      }
    }
  }

  // Check if there are enough available replicas
  if (availFsCount < nAccessReplicas) {
    returnCode = ENETUNREACH;
    goto cleanup;
  }

  // Check if the forced replica (if any) is available
  if (forcedFsId > 0 &&
      (std::find(unavailableFs->begin(), unavailableFs->end(),
                 forcedFsId) != unavailableFs->end())) {
    returnCode = ENETUNREACH;
    goto cleanup;
  }

  // We have multiple groups - compute their geolocation scores to the the
  // available fsids (+things) having a replica
  {
    SchedTreeBase::tFastTreeIdx accesserNode = 0;
    FileSystem::fsid_t selectedFsId = 0;
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    {
      // maps a geolocation scores (int) to all the file system having this geolocation scores
      map< unsigned , std::vector< FileSystem::fsid_t > > geoScore2Fs;
      vector<SchedTreeBase::tFastTreeIdx> accessedReplicasIdx(1);

      for (auto entryIt = entry2FsId.begin(); entryIt != entry2FsId.end();
           entryIt ++) {
        if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
          char buffer[1024];
          buffer[0] = 0;
          char* buf = buffer;

          for (auto it = entryIt->second.begin(); it != entryIt->second.end(); ++it) {
            buf += sprintf(buf, "%lu  ", (unsigned long)(it->second));
          }

          eos_debug("existing replicas indices in geotree -> %s", buffer);
          buffer[0] = 0;
          buf = buffer;

          for (auto it = entryIt->second.begin(); it != entryIt->second.end(); ++it) {
            buf += sprintf(buf, "%s  ",
                           (*entryIt->first->foregroundFastStruct->treeInfo)[it->second].fullGeotag.c_str());
          }

          eos_debug("existing replicas geotags in geotree -> %s", buffer);
        }

        // If there is no replica here (might happen if it's spotted as unavailable
        // after the first pass)
        if (entryIt->second.empty()) {
          continue;
        }

        entry = entryIt->first;
        // find the closest tree node to the accesser
        accesserNode = entry->foregroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(
                         accesserGeotag.c_str());;
        // fill a vector with the indices of the replicas
        vector<SchedTreeBase::tFastTreeIdx> existingReplicasIdx(entryIt->second.size());

        for (size_t i = 0; i < entryIt->second.size(); i++) {
          existingReplicasIdx[i] = entryIt->second[i].second;
        }

        // pickup an access slot is this scheduling group
        accessedReplicasIdx.clear();
        unsigned char retCode = 0;

        switch (type) {
        case regularRO:
          retCode = accessReplicas(entryIt->first, 1, &accessedReplicasIdx,
                                   accesserNode, &existingReplicasIdx,
                                   entry->foregroundFastStruct->rOAccessTree,
                                   pSkipSaturatedAccess);
          break;

        case regularRW:
          retCode = accessReplicas(entryIt->first, 1, &accessedReplicasIdx,
                                   accesserNode, &existingReplicasIdx,
                                   entry->foregroundFastStruct->rWAccessTree,
                                   pSkipSaturatedAccess);
          break;

        case draining:
          retCode = accessReplicas(entryIt->first, 1, &accessedReplicasIdx,
                                   accesserNode, &existingReplicasIdx,
                                   entry->foregroundFastStruct->drnAccessTree,
                                   pSkipSaturatedDrnAccess);
          break;

        default:
          break;
        }

        if (!retCode) {
          goto cleanup;
        }

        const string& fsGeotag =
          (*entryIt->first->foregroundFastStruct->treeInfo)[*accessedReplicasIdx.begin()].fullGeotag;
        unsigned geoScore = 0;
        size_t kmax = min(accesserGeotag.length(), fsGeotag.length());

        for (size_t k = 0; k < kmax; k++) {
          if (accesserGeotag[k] != fsGeotag[k]) {
            break;
          }

          if (accesserGeotag[k] == ':' && k + 1 < kmax && accesserGeotag[k + 1] == ':') {
            geoScore++;
          }
        }

        // if the box is unsaturated, give an advantage to this FS
        if (retCode == 2) {
          geoScore += 100;
          eos_debug("found unsaturated fs");
        }

        geoScore2Fs[geoScore].push_back(
          (*entryIt->first->foregroundFastStruct->treeInfo)[*accessedReplicasIdx.begin()].fsId);
      }

      // randomly choose a fs among the highest scored ones
      selectedFsId = geoScore2Fs.rbegin()->second[rand() %
                     geoScore2Fs.rbegin()->second.size()];

      // return the corresponding index
      for (it = existingReplicas->begin(); it != existingReplicas->end(); it++) {
        if (*it == selectedFsId) {
          fsIndex = (eos::common::FileSystem::fsid_t)(it - existingReplicas->begin());
          break;
        }
      }

      // check we found it
      if (it == existingReplicas->end()) {
        eos_err("inconsistency : unable to find the selected fs but it should be there");
        returnCode = EIO;
        goto cleanup;
      }
    }

    if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
      char buffer[1024];
      buffer[0] = 0;
      char* buf = buffer;

      for (auto it = existingReplicas->begin(); it != existingReplicas->end(); ++it) {
        buf += sprintf(buf, "%lu  ", (unsigned long)(*it));
      }

      eos_debug("existing replicas fs id's -> %s", buffer);

      if (entry) {
        eos_debug("accesser closest node to %s index -> %d / %s",
                  accesserGeotag.c_str(), (int)accesserNode,
                  (*entry->foregroundFastStruct->treeInfo)[accesserNode].fullGeotag.c_str());
      }

      eos_debug("selected FsId -> %d / idx %d", (int)selectedFsId, (int)fsIndex);
    }
  }

  // Apply penalties if needed
  if (true) {
    std::set<eos::common::FileSystem::fsid_t>
    setunav(unavailableFs->begin(), unavailableFs->end());

    for (size_t i = 0; i < existingReplicas->size(); i++) {
      size_t j = (fsIndex + i) % existingReplicas->size();
      auto& fs = (*existingReplicas)[j];

      // If this one is unavailable, skip it
      if (setunav.count(fs)) {
        continue;
      }

      if (!pFs2SchedTME.count(fs)) {
        continue;
      }

      entry = pFs2SchedTME[fs];
      const SchedTreeBase::tFastTreeIdx* idx;

      if (entry->foregroundFastStruct->fs2TreeIdx->get(fs, idx)) {
        const char netSpeedClass =
          (*entry->foregroundFastStruct->treeInfo)[*idx].netSpeedClass;

        // every available box will push data
        if (entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.ulScore >=
            pPenaltySched.pAccessUlScorePenalty[netSpeedClass]) {
          applyUlScorePenalty(entry, *idx,
                              pPenaltySched.pAccessUlScorePenalty[netSpeedClass]);
        }

        // every available box will have to pull data if it's a RW access (or if it's a gateway)
        if ((type == regularRW) || (j == fsIndex && nAccessReplicas > 1)) {
          if (entry->foregroundFastStruct->placementTree->pNodes[*idx].fsData.dlScore >=
              pPenaltySched.pAccessDlScorePenalty[netSpeedClass]) {
            applyDlScorePenalty(entry, *idx,
                                pPenaltySched.pAccessDlScorePenalty[netSpeedClass]);
          }
        }
      } else {
        eos_err("could not find fs on the fast tree to apply penalties");
      }

      // The gateway will also have to pull data from the
      if (j == fsIndex && nAccessReplicas == 1) { // mainly replica layout RO case
        break;
      }
    }
  }

  if (dataProxys) {
    if (!findProxy(ERIdx, entries, inode, dataProxys, NULL,
                   pProxyCloseToFs ? "" : accesserGeotag, filesticky)) {
      returnCode = ENETUNREACH;
      goto cleanup;
    }
  }

  if (firewallEntryPoint) {
    std::vector<std::string> firewallProxyGroups(ERIdx.size());

    // if there are some access geotag mapping rules, use them
    if (pAccessGeotagMapping.inuse && pAccessProxygroup.inuse)
      for (size_t i = 0; i < ERIdx.size(); i++) {
        if (accesserGeotag.empty() ||
            accessReqFwEP((*entries[i]->foregroundFastStruct->treeInfo)[ERIdx[i]].fullGeotag
                          , accesserGeotag)) {
          firewallProxyGroups[i] = accessGetProxygroup((
                                     *entries[i]->foregroundFastStruct->treeInfo)[ERIdx[i]].fullGeotag);
        }
      }

    if (dataProxys) {
      *firewallEntryPoint = *dataProxys;
    }

    if (!findProxy(ERIdx, entries, inode, firewallEntryPoint, &firewallProxyGroups,
                   pProxyCloseToFs ? "" : accesserGeotag, any)) {
      returnCode = ENETUNREACH;
      goto cleanup;
    }
  }

  if (dataProxys) {
    if (firewallEntryPoint) {
      *dataProxys = *firewallEntryPoint;
    }

    if (!findProxy(ERIdx, entries, inode, dataProxys, NULL,
                   pProxyCloseToFs ? "" : accesserGeotag, regular)) {
      returnCode = ENETUNREACH;
      goto cleanup;
    }
  }

  // If we get here, everything is fine
  returnCode = 0;
  // cleanup and exit
cleanup:

  for (auto cit = entry2FsId.begin(); cit != entry2FsId.end(); cit++) {
    cit->first->doubleBufferMutex.UnLockRead();
    AtomicDec(cit->first->fastStructLockWaitersCount);
  }

  return returnCode;
}

void GeoTreeEngine::StartUpdater()
{
  updaterThread.reset(&GeoTreeEngine::listenFsChange, this);
}

void GeoTreeEngine::StopUpdater()
{
  updaterThread.join();
  gUpdaterStarted = false;
}

void GeoTreeEngine::listenFsChange(ThreadAssistant& assistant)
{
  gUpdaterStarted = true;

  if(!mFsListener.startListening()) {
    eos_crit("error starting shared objects change notifications");
  } else {
    eos_info("GeoTreeEngine updater is starting...");
  }

  while (!assistant.terminationRequested()) {
    while (sem_wait(&gUpdaterPauseSem)) {
      if (EINTR != errno) {
        throw "sem_wait() failed";
      }
    }

    mq::FileSystemChangeListener::Event event;
    while(mFsListener.fetch(event, assistant)) {
      if(event.isDeletion()) {
          eos_debug("received deletion on subject %s : the fs was removed from "
                    "the GeoTreeEngine, skipping this update", event.fileSystemQueue.c_str());
          continue;
      }

      pAddRmFsMutex.LockWrite();

      auto notifTypeIt = gQueue2NotifType.find(event.fileSystemQueue);

      if (notifTypeIt == gQueue2NotifType.end()) {
        eos_err("could not determine the type of notification associated to queue ", event.fileSystemQueue.c_str());
      } else {
        // A machine might have several roles at the same time (DataProxy and
        // Gateway), so an update might end in multiple update maps
        if (notifTypeIt->second & sntFilesystem) {
          if (gNotificationsBufferFs.count(event.fileSystemQueue)) {
            (gNotificationsBufferFs)[event.fileSystemQueue] |= gNotifKey2EnumSched.at(event.key);
          } else {
            (gNotificationsBufferFs)[event.fileSystemQueue] = gNotifKey2EnumSched.at(event.key);
          }
        }
      }

      pAddRmFsMutex.UnLockWrite();
    }

    // Do the processing
    common::IntervalStopwatch stopwatch((std::chrono::milliseconds(
                                           pTimeFrameDurationMs)));
    {
      // Do it before tree info to leave some time to the other threads
      checkPendingDeletionsFs();
      checkPendingDeletionsDp();
      {
        eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
        updateTreeInfo(gNotificationsBufferFs, gNotificationsBufferProxy);
      }
      gNotificationsBufferFs.clear();
      gNotificationsBufferProxy.clear();
    }
    pFrameCount++;

    if (sem_post(&gUpdaterPauseSem)) {
      throw "sem_post() failed";
    }

    assistant.wait_for(stopwatch.timeRemainingInCycle());
  }
}

bool GeoTreeEngine::updateTreeInfo(SchedTME* entry,
                                   eos::common::FileSystem::fs_snapshot_t* fs, int keys,
                                   SchedTreeBase::tFastTreeIdx ftIdx , SlowTreeNode* stn)
{
  // We get a consistent set of configuration parameters per refresh of the state
  eos::common::RWMutexReadLock lock(configMutex);

  // Nothing to update
  if ((!ftIdx && !stn) || !keys) {
    return true;
  }

#define setOneStateVarInAllFastTrees(variable,value)                                      \
  {                                                                                       \
    entry->backgroundFastStruct->rOAccessTree->pNodes[ftIdx].fsData.variable = value;     \
    entry->backgroundFastStruct->rWAccessTree->pNodes[ftIdx].fsData.variable = value;     \
    entry->backgroundFastStruct->placementTree->pNodes[ftIdx].fsData.variable = value;    \
    entry->backgroundFastStruct->drnAccessTree->pNodes[ftIdx].fsData.variable = value;    \
    entry->backgroundFastStruct->drnPlacementTree->pNodes[ftIdx].fsData.variable = value; \
  }
#define setOneStateVarStatusInAllFastTrees(flag)                                          \
  {                                                                                       \
    entry->backgroundFastStruct->rOAccessTree->pNodes[ftIdx].fsData.mStatus |= flag;      \
    entry->backgroundFastStruct->rWAccessTree->pNodes[ftIdx].fsData.mStatus |= flag;      \
    entry->backgroundFastStruct->placementTree->pNodes[ftIdx].fsData.mStatus |= flag;     \
    entry->backgroundFastStruct->drnAccessTree->pNodes[ftIdx].fsData.mStatus |= flag;     \
    entry->backgroundFastStruct->drnPlacementTree->pNodes[ftIdx].fsData.mStatus |= flag;  \
  }
#define unsetOneStateVarStatusInAllFastTrees(flag)                                        \
  {                                                                                       \
    entry->backgroundFastStruct->rOAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag;     \
    entry->backgroundFastStruct->rWAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag;     \
    entry->backgroundFastStruct->placementTree->pNodes[ftIdx].fsData.mStatus &= ~flag;    \
    entry->backgroundFastStruct->drnAccessTree->pNodes[ftIdx].fsData.mStatus &= ~flag;    \
    entry->backgroundFastStruct->drnPlacementTree->pNodes[ftIdx].fsData.mStatus &= ~flag; \
  }

  if (keys & sfgGeotag) {
    // update the treenodeinfo
    string newGeoTag = fs->mGeoTag;

    if (newGeoTag.empty()) {
      newGeoTag = "nogeotag";
    }

    FileSystem::fsid_t fsid = fs->mId;

    if (!fsid) {
      eos_err("%s", "msg=\"skip update for fsid=0\"");
      return false;
    }

    entry->slowTreeMutex.LockWrite();

    if (!entry->fs2SlowTreeNode.count(fsid)) {
      eos_err("msg=\"no such slowtree node fsid=%lu\"", fsid);
      entry->slowTreeMutex.UnLockWrite();
      return false;
    }

    SlowTreeNode* oldNode = entry->fs2SlowTreeNode[fsid];
    //const string &oldGeoTag = oldNode->pNodeInfo.fullGeotag;
    string oldGeoTag = oldNode->pNodeInfo.fullGeotag;
    oldGeoTag = (oldGeoTag.rfind("::") != std::string::npos) ? oldGeoTag.substr(0,
                oldGeoTag.rfind("::")) : std::string("");

    //CHECK IF CHANGE ACTUALLY HAPPENED BEFORE ACTUALLY CHANGING SOMETHING
    if (oldGeoTag != newGeoTag) {
      // do the change only if there is one
      SlowTreeNode* newNode = NULL;
      newNode = entry->slowTree->moveToNewGeoTag(oldNode, newGeoTag);

      if (!newNode) {
        stringstream ss;
        ss << (*entry->slowTree);
        eos_err("error changing geotag in slowtree : move is \"%s\" => \"%s\" "
                "and slowtree is \n%s\n", oldGeoTag.c_str(), newGeoTag.c_str(),
                ss.str().c_str());
        entry->slowTreeMutex.UnLockWrite();
        return false;
      }

      eos_debug("geotag change detected : old geotag is \"%s\" new geotag is \"%s\"",
                oldGeoTag.c_str(), newGeoTag.c_str());
      entry->slowTreeModified = true;
      entry->fs2SlowTreeNode[fsid] = newNode;
      // !!! change the argument too
      stn = newNode;
    }

    entry->slowTreeMutex.UnLockWrite();
  }

  if (keys & sfgId) {
    // should not happen
    // eos_crit("the FsId should not change once it's created:  new value
    // is %lu",(unsigned long)fs->mId);
    // .... unless it is the first change to give to the id it's initial
    // value. It happens after it's been created so it's seen as a change.
  }

  if (keys & (sfgBoot | sfgActive | sfgErrc)) {
    BootStatus statboot = fs->mStatus;
    unsigned int errc = fs->mErrCode;
    ActiveStatus statactive = fs->mActiveStatus;
    eos_debug("fs %lu available recompute  boot=%s  errcode=%d  active=%s",
              (unsigned long) fs->mId,
              eos::common::FileSystem::GetStatusAsString(statboot),
              errc,
              (statactive == eos::common::ActiveStatus::kOnline) ? "online" : "offline");

    if ((statboot == BootStatus::kBooted) &&
        (errc == 0) &&    // this we probably don't need
        // This checks the heartbeat and the group & node are enabled
        (statactive == ActiveStatus::kOnline)) {
      // the fs is available
      eos_debug("fs %lu is getting available  ftidx=%d  stn=%p",
                (unsigned long) fs->mId, (int)ftIdx, stn);

      if (ftIdx) {
        setOneStateVarStatusInAllFastTrees(SchedTreeBase::Available);
      }

      if (stn) {
        stn->pNodeState.mStatus |= SchedTreeBase::Available;
      }
    } else {
      // the fs is unavailable
      eos_debug("fs %lu is getting unavailable ftidx=%d  stn=%p",
                (unsigned long) fs->mId, (int)ftIdx, stn);

      if (ftIdx) {
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Available);
      }

      if (stn) {
        stn->pNodeState.mStatus &= ~SchedTreeBase::Available;
      }
    }
  }

  if (keys & sfgConfigstatus) {
    common::ConfigStatus status = fs->mConfigStatus;

    if (status == common::ConfigStatus::kRW) {
      if (ftIdx) {
        setOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable |
                                           SchedTreeBase::Writable);
      }

      if (stn) {
        stn->pNodeState.mStatus |= (SchedTreeBase::Readable | SchedTreeBase::Writable);
      }
    } else if (status == common::ConfigStatus::kRO ||
               status == common::ConfigStatus::kDrain) {
      if (ftIdx) {
        setOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable);
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Writable);
      }

      if (stn) {
        stn->pNodeState.mStatus |= SchedTreeBase::Readable;
        stn->pNodeState.mStatus &= ~SchedTreeBase::Writable;
      }
    } else if (status == common::ConfigStatus::kWO) {
      if (ftIdx) {
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable);
        setOneStateVarStatusInAllFastTrees(SchedTreeBase::Writable);
      }

      if (stn) {
        stn->pNodeState.mStatus &= ~SchedTreeBase::Readable;
        stn->pNodeState.mStatus |= SchedTreeBase::Writable;
      }
    } else {
      if (ftIdx) {
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Readable);
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Writable);
      }

      if (stn) {
        stn->pNodeState.mStatus &= ~SchedTreeBase::Readable;
        stn->pNodeState.mStatus &= ~SchedTreeBase::Writable;
      }
    }
  }

  if (keys & sfgDrain) {
    DrainStatus drainStatus = fs->mDrainStatus;

    if (fs->mConfigStatus == common::ConfigStatus::kDrain &&
        drainStatus == DrainStatus::kDraining) {
      // mark as draining
      if (ftIdx) {
        setOneStateVarStatusInAllFastTrees(SchedTreeBase::Draining);
      }

      if (stn) {
        stn->pNodeState.mStatus |= SchedTreeBase::Draining;
      }
    } else {
      // This covers the following cases
      // case common::ConfigStatus::kNoDrain:
      // case common::ConfigStatus::kDrainPrepare:
      // case common::ConfigStatus::kDrainWait:
      // case common::ConfigStatus::kDrainStalling:
      // case common::ConfigStatus::kDrained:
      // case common::ConfigStatus::kDrainExpired:
      if (ftIdx) {
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Draining);
      }

      if (stn) {
        stn->pNodeState.mStatus &= ~SchedTreeBase::Draining;
      }
    }
  }

  if (keys & sfgDrainer) {
    if (ftIdx) {
      setOneStateVarStatusInAllFastTrees(SchedTreeBase::Drainer);
    }

    if (stn) {
      stn->pNodeState.mStatus |= SchedTreeBase::Drainer;
    }
  }

  if (keys & (sfgBalthres | sfgFsfilled | sfgNomfilled)) {
    auto nominal = fs->mNominalFilled;
    auto filled = fs->mDiskFilled;
    auto threshold = fs->mBalThresh;
    bool balancing = false;

    if (nominal && ((filled - threshold) >= nominal)) {
      balancing = true;
    }

    if (balancing) {
      if (ftIdx) {
        setOneStateVarStatusInAllFastTrees(SchedTreeBase::Balancing);
      }

      if (stn) {
        stn->pNodeState.mStatus |= SchedTreeBase::Balancing;
      }
    } else {
      if (ftIdx) {
        unsetOneStateVarStatusInAllFastTrees(SchedTreeBase::Balancing);
      }

      if (stn) {
        stn->pNodeState.mStatus &= ~SchedTreeBase::Balancing;
      }
    }
  }

  if (keys & sfgBlkavailb) {
    float ts = float(fs->mDiskBfree * (double)fs->mDiskBsize);
    // Account also for the headroom on the fst
    ts = ts - fs->mHeadRoom;

    if (ts < 0) {
      ts = 0;
    }

    if (ftIdx) {
      setOneStateVarInAllFastTrees(totalSpace, ts);
    }

    if (stn) {
      stn->pNodeState.totalSpace = ts;
    }
  }

  // <1Gb/s -> 0 ; 1Gb/s -> 1; 10Gb/s->2 ; 100Gb/s->...etc
  size_t netSpeedClass = 0;

  if ((keys & sfgPubTmStmp) && fs->mPublishTimestamp) {
    // update the latency of this fs
    tLatencyStats* lstat = NULL;

    if (ftIdx) {
      if (((int)((*entry->backgroundFastStruct->treeInfo)[ftIdx].fsId)) < ((
            int)pLatencySched.pFsId2LatencyStats.size())) {
        lstat = &pLatencySched.pFsId2LatencyStats[(*entry->backgroundFastStruct->treeInfo)[ftIdx].fsId];
      } else {
        eos_crit("trying to update latency for fs %d but latency stats vector "
                 "size is %d : something is wrong",
                 (int)(*entry->backgroundFastStruct->treeInfo)[ftIdx].fsId,
                 (int)pLatencySched.pFsId2LatencyStats.size());
      }
    } else if (stn) {
      if ((int)(stn->pNodeInfo.fsId) < ((int)
                                        pLatencySched.pFsId2LatencyStats.size())) {
        lstat = &pLatencySched.pFsId2LatencyStats[stn->pNodeInfo.fsId];
      } else {
        eos_err("trying to update latency for fs %d but latency stats vector "
                "size is %d : something is wrong", (int)(stn->pNodeInfo.fsId),
                (int)pLatencySched.pFsId2LatencyStats.size());
      }
    }

    if (lstat) {
      lstat->lastupdate = fs->mPublishTimestamp;
      lstat->update();
    }
  }

  if (keys & (sfgDiskload | sfgInratemib)) {
    // update the upload score
    double ulScore = (1 - fs->mDiskUtilization);
    double netoutweight = (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetOutRateMiB /
                                  fs->mNetEthRateMiB) : 0.0));
    ulScore *= ((netoutweight > 0) ? sqrt(netoutweight) : 0);

    if (ftIdx) {
      setOneStateVarInAllFastTrees(ulScore, (char)(ulScore * 100));
    }

    if (stn) {
      stn->pNodeState.ulScore = ulScore * 100;
    }
  }

  if (keys & (sfgOutratemib | sfgDiskload | sfgReadratemb)) {
    double dlScore = (1 - fs->mDiskUtilization);
    double netinweight = (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetInRateMiB /
                                 fs->mNetEthRateMiB) : 0.0));
    dlScore *= ((netinweight > 0) ? sqrt(netinweight) : 0);

    if (ftIdx) {
      setOneStateVarInAllFastTrees(dlScore, (char)(dlScore * 100));
    }

    if (stn) {
      stn->pNodeState.dlScore = dlScore * 100;
    }
  }

  if (keys & (sfgDiskload | sfgInratemib | sfgOutratemib | sfgEthmib)) {
    netSpeedClass = round(log10(fs->mNetEthRateMiB * 8 * 1024 * 1024 + 1));
    // netSpeedClass 1 means 1Gbps
    netSpeedClass = netSpeedClass > 8 ? netSpeedClass - 8 : 0;

    // check if netspeed class needs an update
    if (entry->backgroundFastStruct->treeInfo->size() >= netSpeedClass + 1 &&
        (*entry->backgroundFastStruct->treeInfo)[ftIdx].netSpeedClass !=
        (unsigned char)netSpeedClass) {
      if (ftIdx) {
        (*entry->backgroundFastStruct->treeInfo)[ftIdx].netSpeedClass = netSpeedClass;
      }

      if (stn) {
        stn->pNodeInfo.netSpeedClass = netSpeedClass;
      }
    }

    // This one will create the entry if it doesnt exists already
    nodeAgreg& na = pPenaltySched.pUpdatingNodes[fs->mHostPort];
    na.fsCount++;

    if (!na.saturated) {
      if (na.fsCount == 1) {
        na.netSpeedClass = netSpeedClass;
        pPenaltySched.pMaxNetSpeedClass = std::max(pPenaltySched.pMaxNetSpeedClass ,
                                          netSpeedClass);
        na.netOutWeight += (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetOutRateMiB /
                                   fs->mNetEthRateMiB) : 0.0));
        na.netInWeight += (1.0 - ((fs->mNetEthRateMiB) ? (fs->mNetInRateMiB /
                                  fs->mNetEthRateMiB) : 0.0));

        if (na.netOutWeight < 0.1 || na.netInWeight < 0.1) {
          na.saturated = true;  // network of the box is saturated
        }
      }

      na.rOpen += fs->mDiskRopen;
      na.wOpen += fs->mDiskWopen;
      na.diskUtilSum += fs->mDiskUtilization;

      if (fs->mDiskUtilization > 0.9) {
        na.saturated = true;  // one of the disks of the box is saturated
      }
    }

    // apply penalties that are still valid on fast trees
    if (ftIdx) {
      recallScorePenalty(entry, ftIdx);
    }

    // in case the fs in not in the fast trees , it has not been
    // used recently to schedule , so there is no penalty to recall!
    // so there is nothing like if(stn) recallScorePenalty(entry, stn);
  }

  if (keys & sfgFsfilled) {
    if (ftIdx) {
      setOneStateVarInAllFastTrees(fillRatio, (char)fs->mDiskFilled);
    }

    if (stn) {
      stn->pNodeState.fillRatio = (char)fs->mDiskFilled;
    }
  }

  // SHOULD WE TAKE THE NOMINAL FILLING AS SET BY THE BALANCING?
  //  if(keys&(sfgNomfilled)) {
  //    fs->
  //  }
  return true;
#undef setOneStateVarInAllFastTrees
#undef setOneStateVarStatusInAllFastTrees
#undef unsetOneStateVarStatusInAllFastTrees
}

bool GeoTreeEngine::updateTreeInfo(const map<string, int>& updatesFs,
                                   const map<string, int>& updatesDp)
{
  // copy the foreground FastStructures to the BackGround FastStructures
  // so that the penalties applied after the placement/access are kept by defaut
  // (and overwritten if a new state is received from the fs)
  // => SCHEDULING
  pTreeMapMutex.LockRead();

  for (auto it = pGroup2SchedTME.begin(); it != pGroup2SchedTME.end(); it++) {
    SchedTME* entry = it->second;
    RWMutexReadLock lock(entry->slowTreeMutex);

    if (!entry->foregroundFastStruct->DeepCopyTo(entry->backgroundFastStruct)) {
      eos_crit("error deep copying in double buffering");
      pTreeMapMutex.UnLockRead();
      return false;
    }

    // Copy the penalties of the last frame from each group and reset the
    // penalties counter in the fast trees.
    auto& pVec = pPenaltySched.pCircFrCnt2FsPenalties[pFrameCount % pCircSize];

    for (auto it2 = entry->foregroundFastStruct->fs2TreeIdx->begin();
         it2 != entry->foregroundFastStruct->fs2TreeIdx->end(); it2++) {
      auto cur = *it2;
      pVec[cur.first] = (*entry->foregroundFastStruct->penalties)[cur.second];
      AtomicCAS((*entry->foregroundFastStruct->penalties)[cur.second].dlScorePenalty,
                (*entry->foregroundFastStruct->penalties)[cur.second].dlScorePenalty, (char)0);
      AtomicCAS((*entry->foregroundFastStruct->penalties)[cur.second].ulScorePenalty,
                (*entry->foregroundFastStruct->penalties)[cur.second].ulScorePenalty, (char)0);
    }
  }

  pTreeMapMutex.UnLockRead();
  // => PROXYGROUPS
  pPxyTreeMapMutex.LockRead();

  for (auto it = pPxyGrp2DpTME.begin(); it != pPxyGrp2DpTME.end(); it++) {
    DataProxyTME* entry = it->second;
    RWMutexReadLock lock(entry->slowTreeMutex);

    if (!entry->foregroundFastStruct->DeepCopyTo(entry->backgroundFastStruct)) {
      eos_crit("error deep copying in double buffering");
      pPxyTreeMapMutex.UnLockRead();
      return false;
    }

    // Copy the penalties of the last frame from each group and reset the
    // penalties counter in the fast trees.
    auto& pMap = pPenaltySched.pCircFrCnt2HostPenalties[pFrameCount % pCircSize];

    for (auto it2 = entry->foregroundFastStruct->host2TreeIdx->begin();
         it2 != entry->foregroundFastStruct->host2TreeIdx->end(); it2++) {
      auto cur = *it2;
      pMap[cur.first] = (*entry->foregroundFastStruct->penalties)[cur.second];
      AtomicCAS((*entry->foregroundFastStruct->penalties)[cur.second].dlScorePenalty,
                (*entry->foregroundFastStruct->penalties)[cur.second].dlScorePenalty, (char)0);
      AtomicCAS((*entry->foregroundFastStruct->penalties)[cur.second].ulScorePenalty,
                (*entry->foregroundFastStruct->penalties)[cur.second].ulScorePenalty, (char)0);
    }
  }

  pPxyTreeMapMutex.UnLockRead();
  // timestamp the current frame
  {
    struct timeval curtime;
    gettimeofday(&curtime, 0);
    pLatencySched.pCircFrCnt2Timestamp[pFrameCount % pCircSize] = ((
          size_t)curtime.tv_sec) * 1000 + ((size_t)curtime.tv_usec) / 1000;
  }
  pPenaltySched.pUpdatingNodes.clear();
  pPenaltySched.pMaxNetSpeedClass = 0;

  // => SCHED
  for (auto it = updatesFs.begin(); it != updatesFs.end(); ++it) {
    pTreeMapMutex.LockRead();
    eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByQueuePath(
                                            it->first);

    if (!filesystem) {
      eos_err("update : Invalid FileSystem Entry, skipping this update");
      pTreeMapMutex.UnLockRead();
      continue;
    }

    eos::common::FileSystem::fs_snapshot_t fs;
    filesystem->SnapShotFileSystem(fs, true);
    FileSystem::fsid_t fsid = fs.mId;

    if (!pFs2SchedTME.count(fsid)) {
      eos_err("update : TreeEntryMap has been removed, skipping this update");
      pTreeMapMutex.UnLockRead();
      continue;
    }

    SchedTME* entry = pFs2SchedTME[fsid];
    AtomicInc(entry->fastStructLockWaitersCount);
    pTreeMapMutex.UnLockRead();
    eos_debug("CHANGE BITFIELD %s => %x", it->first.c_str(), it->second);
    // Update only the fast structures because even if a fast structure rebuild
    // is needed from the slow tree. Its information and state is updated from
    // the fast structures.
    entry->doubleBufferMutex.LockRead();
    const SchedTreeBase::tFastTreeIdx* idx = NULL;
    SlowTreeNode* node = NULL;

    if (!entry->backgroundFastStruct->fs2TreeIdx->get(fsid, idx)) {
      auto nodeit = entry->fs2SlowTreeNode.find(fsid);

      if (nodeit == entry->fs2SlowTreeNode.end()) {
        eos_crit("Inconsistency : cannot locate an fs %lu supposed to be in "
                 "the fast structures", (unsigned long)fsid);
        entry->doubleBufferMutex.UnLockRead();
        AtomicDec(entry->fastStructLockWaitersCount);
        return false;
      }

      node = nodeit->second;
      eos_debug("no fast tree for fs %lu : updating slowtree", (unsigned long)fsid);
    } else {
      eos_debug("fast tree available for fs %lu : not updating slowtree",
                (unsigned long)fsid);
    }

    updateTreeInfo(entry, &fs, it->second, idx ? *idx : 0 , node);

    if (idx) {
      entry->fastStructModified = true;
    }

    if (node) {
      entry->slowTreeModified = true;
    }

    // if we update the slowtree, then a fast tree generation is already pending
    entry->doubleBufferMutex.UnLockRead();
    AtomicDec(entry->fastStructLockWaitersCount);
  }

  // Update the atomic penalties
  updateAtomicPenalties();
  // Update the trees that need to be updated (could maybe optimized by
  // updating only the branch needing, might be worth it if only 1 or 2
  // branches are updated). Self update for the fast structure if update
  // from slow tree is not needed. If convert from slowtree is needed,
  // update the slowtree from the fast for the info and for the state
  // => SCHED
  pTreeMapMutex.LockRead();

  for (auto it = pGroup2SchedTME.begin(); it != pGroup2SchedTME.end(); it++) {
    SchedTME* entry = it->second;
    RWMutexReadLock lock(entry->slowTreeMutex);

    if (!updateFastStructures(entry)) {
      pTreeMapMutex.UnLockRead();
      eos_err("error updating the tree");
      return false;
    }
  }

  pTreeMapMutex.UnLockRead();
  return true;
}

bool GeoTreeEngine::getInfosFromFsIds(const std::vector<FileSystem::fsid_t>&
                                      fsids, std::vector<std::string>* fsgeotags,
                                      std::vector<std::string>* hosts,
                                      std::vector<FsGroup*>* sortedgroups)
{
  bool result = true;

  if (fsgeotags) {
    fsgeotags->reserve(fsids.size());
  }

  if (sortedgroups) {
    sortedgroups->reserve(fsids.size());
  }

  std::map<FsGroup*, size_t> group2idx;
  std::vector<std::pair<size_t, size_t> > groupcount;
  groupcount.reserve(fsids.size());
  {
    RWMutexReadLock lock(this->pTreeMapMutex);

    for (auto it = fsids.begin(); it != fsids.end(); ++ it) {
      if (pFs2SchedTME.count(*it)) {
        FsGroup* group = pFs2SchedTME[*it]->group;

        if (fsgeotags || hosts) {
          const SchedTreeBase::tFastTreeIdx* idx = NULL;

          if (pFs2SchedTME[*it]->foregroundFastStruct->fs2TreeIdx->get(*it, idx)) {
            if (fsgeotags) fsgeotags->push_back(
                (*pFs2SchedTME[*it]->foregroundFastStruct->treeInfo)[*idx].fullGeotag
              );

            if (hosts) hosts->push_back(
                (*pFs2SchedTME[*it]->foregroundFastStruct->treeInfo)[*idx].host
              );
          } else {
            if (fsgeotags) {
              fsgeotags->push_back("");
            }

            if (hosts) {
              hosts->push_back("");
            }
          }
        }

        if (sortedgroups) {
          if (!group2idx.count(group)) {
            group2idx[group] = group2idx.size();
            sortedgroups->push_back(group);
            groupcount.push_back(make_pair(1, groupcount.size()));
          } else {
            size_t idx = group2idx[group];
            groupcount[idx].first++;
          }
        }
      } else {
        // put an empty entry in the result vector to preserve the indexing
        if (fsgeotags) {
          fsgeotags->push_back("");
        }

        if (hosts) {
          hosts->push_back("");
        }

        // to signal that one of the fsids was not mapped to a group
        result = false;
      }
    }
  }

  if (sortedgroups) {
    // sort the count vector in ascending order to get the permutation
    std::sort(groupcount.begin(), groupcount.end(),
              std::greater<std::pair<size_t, size_t>>());
    // apply the permutation
    std::vector<FsGroup*> final(groupcount.size());
    size_t count = 0;

    for (auto it = groupcount.begin(); it != groupcount.end(); it++) {
      final[count++] = (*sortedgroups)[it->second];
    }

    *sortedgroups = final;
  }

  return result;
}

void GeoTreeEngine::updateAtomicPenalties()
{
  // In this function, we compute a rough a simplified version
  // of the penalties applied to selected fs for placement and access.
  // there is only one penalty and it's copied to ulplct, dlplct, ulaccess and dlaccess
  // variants.

  // if the update is enabled
  if (pPenaltyUpdateRate) {
    if (pPenaltySched.pUpdatingNodes.empty()) {
      //eos_debug("updatingNodes is empty!");
    } else {
      // each networking speed class has its own variables
      std::vector<double>
      ropen(pPenaltySched.pMaxNetSpeedClass + 1, 0.0),
            wopen(pPenaltySched.pMaxNetSpeedClass + 1, 0.0),
            ulload(pPenaltySched.pMaxNetSpeedClass + 1, 0.0),
            dlload(pPenaltySched.pMaxNetSpeedClass + 1, 0.0),
            fscount(pPenaltySched.pMaxNetSpeedClass + 1, 0.0),
            hostcount(pPenaltySched.pMaxNetSpeedClass + 1, 0.0),
            diskutil(pPenaltySched.pMaxNetSpeedClass + 1, 0.0);

      // we use the view to check that we have all the fs in a node
      // could be removed if we were sure to run a single on fst daemon / box

      // WARNING: see below / FsView::gFsView.ViewMutex.LockRead();
      for (auto it = pPenaltySched.pUpdatingNodes.begin();
           it != pPenaltySched.pUpdatingNodes.end(); it++) {
        const std::string& nodestr = it->first;

        // ===============
        // WARNING: the following part is commented out because it can create a
        // deadlock with FsViewMutex/pAddRmFsMutex in the above FsViewMutex lock
        // when inserting/removing a filesystem. It can be fixed but it's not
        // trivial. Because it's not needed in operation, we don't fix it for now.
        // When using several fst daemons on the same host, it could give
        // overestimated atomic penalties when they are selfestimated
        // ===============
        /*
        FsNode *node = NULL;
        if(FsView::gFsView.mNodeView.count(nodestr))
        node = FsView::gFsView.mNodeView[nodestr];
        else
        {
          std::stringstream ss;
          ss.str("");
          for (auto it2 = FsView::gFsView.mNodeView.begin();
               it2 != FsView::gFsView.mNodeView.end(); it2++) {
            ss << it2->first << "  ";
          }
          eos_err("Inconsistency : cannot find updating node %s in %s",
                   nodestr.c_str(),ss.str().c_str());
          continue;
        }
        if((!it->second.saturated) && it->second.fsCount == node->size())
        */
        // ===============
        if ((!it->second.saturated)) {
          // eos_debug("aggregated opened files for %s: wopen %d, ropen %d,
          //            outweight %lf, inweight %lf", it->first.c_str(),
          //            it->second.wOpen, it->second.rOpen,
          //            it->second.netOutWeight, it->second.netInWeight);
          // Update aggregated informations for the right networking class
          // (take into account only unsaturated boxes)
          ropen[it->second.netSpeedClass] += (it->second.rOpen);
          wopen[it->second.netSpeedClass] += (it->second.wOpen);
          ulload[it->second.netSpeedClass] += (1.0 - it->second.netOutWeight);
          dlload[it->second.netSpeedClass] += (1.0 - it->second.netInWeight);
          diskutil[it->second.netSpeedClass] += it->second.diskUtilSum;
          fscount[it->second.netSpeedClass] += it->second.fsCount;
          hostcount[it->second.netSpeedClass]++;
        } else {
          // The fs/host is saturated, we don't use the whole host in the estimate
          eos_debug("fs update in node %s : box is saturated", nodestr.c_str());
          continue;
          // Could force to get everything
          // long long wopen = node->SumLongLong("stat.wopen",false);
          // long long ropen = node->SumLongLong("stat.ropen",false);
        }
      }

      // WARNING: see above / FsView::gFsView.ViewMutex.UnLockRead();
      for (size_t netSpeedClass = 0; netSpeedClass <= pPenaltySched.pMaxNetSpeedClass;
           netSpeedClass++) {
        if (ropen[netSpeedClass] + wopen[netSpeedClass] > 4) {
          eos_debug("UPDATE netSpeedClass=%d, ulload=%lf, dlload=%lf, "
                    "diskutil=%lf, ropen=%lf, wopen=%lf  fscount=%lf, "
                    "hostcount=%lf", (int)netSpeedClass, ulload[netSpeedClass],
                    dlload[netSpeedClass], diskutil[netSpeedClass],
                    ropen[netSpeedClass], wopen[netSpeedClass],
                    fscount[netSpeedClass], hostcount[netSpeedClass]);
          // The penalty aims at knowing roughly how many concurrent file
          // operations can be done on a single fs before sturating a ressource
          // (disk or network)
          // network penalty per file = the multiplication by the number of fs
          // is to take into account that the bw is shared between multiple fs
          double avgnetload = 0.5 * (ulload[netSpeedClass] + dlload[netSpeedClass]) /
                              (ropen[netSpeedClass] + wopen[netSpeedClass]);
          double networkpenSched = avgnetload * (fscount[netSpeedClass] /
                                                 hostcount[netSpeedClass]);
          double networkpenGw    = avgnetload;
//          double networkpen =
//          0.5*(ulload[netSpeedClass]+dlload[netSpeedClass])/(ropen[netSpeedClass]+wopen[netSpeedClass])
//          *(fscount[netSpeedClass]/hostcount[netSpeedClass]);
          // there is factor to take into account the read cache
          // TODO use a realistic value for this factor
          double diskpen =
            diskutil[netSpeedClass] / (0.4 * ropen[netSpeedClass] + wopen[netSpeedClass]);
          eos_debug("penalties updates for scheduling are network %lf   disk %lf",
                    networkpenSched, diskpen);
          eos_debug("penalties updates for gateway/dataproxy are network %lf",
                    networkpenGw, diskpen);
          double updateSched = 100 * std::max(diskpen, networkpenSched);
          double updateGw = 100 * networkpenGw;

          if (updateSched < 1 || updateSched > 99) { // could be more restrictive
            eos_debug("weird value for accessDlScorePenalty update : %lf. Not "
                      "using this one.", updateSched);
          } else {
            eos_debug("netSpeedClass %d : using update values %lf for penalties "
                      "with weight %f%%", netSpeedClass, pPenaltyUpdateRate);
            eos_debug("netSpeedClass %d : values before update are "
                      "accessDlScorePenalty=%f, plctDlScorePenalty=%f, "
                      "accessUlScorePenalty=%f, plctUlScorePenalty=%f",
                      netSpeedClass, pPenaltySched.pAccessDlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pPlctDlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pAccessUlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pPlctUlScorePenaltyF[netSpeedClass]);
            union {
              float f;
              uint32_t u;
            } uf;
            // Atomic change, no need to lock anything
            uf.f = 0.01 * ((100 - pPenaltyUpdateRate) *
                           pPenaltySched.pAccessDlScorePenaltyF[netSpeedClass] +
                           pPenaltyUpdateRate * updateSched);
            AtomicCAS(reinterpret_cast<uint32_t&>
                      (pPenaltySched.pAccessDlScorePenaltyF[netSpeedClass]) ,
                      reinterpret_cast<uint32_t&>(pPenaltySched.pAccessDlScorePenaltyF[netSpeedClass])
                      , uf.u);
            uf.f = 0.01 * ((100 - pPenaltyUpdateRate) *
                           pPenaltySched.pPlctDlScorePenaltyF[netSpeedClass] +
                           pPenaltyUpdateRate * updateSched);
            AtomicCAS(reinterpret_cast<uint32_t&>
                      (pPenaltySched.pPlctDlScorePenaltyF[netSpeedClass]) ,
                      reinterpret_cast<uint32_t&>(pPenaltySched.pPlctDlScorePenaltyF[netSpeedClass]) ,
                      uf.u);
            uf.f = 0.01 * ((100 - pPenaltyUpdateRate) *
                           pPenaltySched.pAccessUlScorePenaltyF[netSpeedClass] +
                           pPenaltyUpdateRate * updateSched);
            AtomicCAS(reinterpret_cast<uint32_t&>
                      (pPenaltySched.pAccessUlScorePenaltyF[netSpeedClass]) ,
                      reinterpret_cast<uint32_t&>(pPenaltySched.pAccessUlScorePenaltyF[netSpeedClass])
                      , uf.u);
            uf.f = 0.01 * ((100 - pPenaltyUpdateRate) *
                           pPenaltySched.pPlctUlScorePenaltyF[netSpeedClass] +
                           pPenaltyUpdateRate * updateSched);
            AtomicCAS(reinterpret_cast<uint32_t&>
                      (pPenaltySched.pPlctUlScorePenaltyF[netSpeedClass]) ,
                      reinterpret_cast<uint32_t&>(pPenaltySched.pPlctUlScorePenaltyF[netSpeedClass]) ,
                      uf.u);
            uf.f = 0.01 * ((100 - pPenaltyUpdateRate) *
                           pPenaltySched.pProxyScorePenaltyF[netSpeedClass] +
                           pPenaltyUpdateRate * updateGw);
            AtomicCAS(reinterpret_cast<uint32_t&>
                      (pPenaltySched.pProxyScorePenaltyF[netSpeedClass]) ,
                      reinterpret_cast<uint32_t&>(pPenaltySched.pProxyScorePenaltyF[netSpeedClass]) ,
                      uf.u);
            eos_debug("netSpeedClass %d : values after update are "
                      "accessDlScorePenalty=%f, plctDlScorePenalty=%f, "
                      "accessUlScorePenalty=%f, plctUlScorePenalty=%f, "
                      "gwScorePenalty=%f", netSpeedClass,
                      pPenaltySched.pAccessDlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pPlctDlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pAccessUlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pPlctUlScorePenaltyF[netSpeedClass],
                      pPenaltySched.pProxyScorePenaltyF[netSpeedClass]);
            // Update the casted versions too
            AtomicCAS(pPenaltySched.pPlctUlScorePenalty[netSpeedClass],
                      pPenaltySched.pPlctUlScorePenalty[netSpeedClass],
                      (SchedTreeBase::tFastTreeIdx)
                      pPenaltySched.pPlctUlScorePenaltyF[netSpeedClass]);
            AtomicCAS(pPenaltySched.pPlctDlScorePenalty[netSpeedClass],
                      pPenaltySched.pPlctDlScorePenalty[netSpeedClass],
                      (SchedTreeBase::tFastTreeIdx)
                      pPenaltySched.pPlctDlScorePenaltyF[netSpeedClass]);
            AtomicCAS(pPenaltySched.pAccessDlScorePenalty[netSpeedClass],
                      pPenaltySched.pAccessDlScorePenalty[netSpeedClass],
                      (SchedTreeBase::tFastTreeIdx)
                      pPenaltySched.pAccessDlScorePenaltyF[netSpeedClass]);
            AtomicCAS(pPenaltySched.pAccessUlScorePenalty[netSpeedClass],
                      pPenaltySched.pAccessUlScorePenalty[netSpeedClass],
                      (SchedTreeBase::tFastTreeIdx)
                      pPenaltySched.pAccessUlScorePenaltyF[netSpeedClass]);
            AtomicCAS(pPenaltySched.pProxyScorePenalty[netSpeedClass],
                      pPenaltySched.pProxyScorePenalty[netSpeedClass],
                      (SchedTreeBase::tFastTreeIdx) pPenaltySched.pProxyScorePenaltyF[netSpeedClass]);
          }
        } else {
          eos_debug("not enough file opened to get reliable statistics %d",
                    (int)(ropen[netSpeedClass] + ropen[netSpeedClass]));
        }
      }
    }
  }
}

bool GeoTreeEngine::setSkipSaturatedAccess(bool value, bool setconfig)
{
  return setInternalParam(pSkipSaturatedAccess, (int)value, false,
                          setconfig ? "skipsaturatedaccess" : "");
}
bool GeoTreeEngine::setSkipSaturatedDrnAccess(bool value, bool setconfig)
{
  return setInternalParam(pSkipSaturatedDrnAccess, (int)value, false,
                          setconfig ? "skipsaturateddrnaccess" : "");
}
bool GeoTreeEngine::setSkipSaturatedBlcAccess(bool value, bool setconfig)
{
  return setInternalParam(pSkipSaturatedBlcAccess, (int)value, false,
                          setconfig ? "skipsaturatedblcaccess" : "");
}

bool GeoTreeEngine::setProxyCloseToFs(bool value, bool setconfig)
{
  return setInternalParam(pProxyCloseToFs, (int)value, false,
                          setconfig ? "proxyclosetofs" : "");
}

bool GeoTreeEngine::setScorePenalty(std::vector<float>& fvector,
                                    std::vector<char>& cvector,
                                    const std::vector<char>& vvalue,
                                    const std::string& configentry)
{
  if (vvalue.size() != 8) {
    return false;
  }

  std::vector<float> valuef(8);

  for (int i = 0; i < 8; i++) {
    valuef[i] = vvalue[i];
  }

  return setInternalParam(fvector, valuef, false, "")
         && setInternalParam(cvector, vvalue, false, configentry);
}

bool GeoTreeEngine::setScorePenalty(std::vector<float>& fvector,
                                    std::vector<char>& cvector,
                                    const char* svalue,
                                    const std::string& configentry)
{
  std::vector<double> dvvalue(8);
  std::vector<char> vvalue(8);

  if (sscanf(svalue, "[%lf,%lf,%lf,%lf,%lf,%lf,%lf,%lf]", &dvvalue[0],
             &dvvalue[1], &dvvalue[2], &dvvalue[3], &dvvalue[4], &dvvalue[5], &dvvalue[6],
             &dvvalue[7]) != 8) {
    return false;
  }

  for (int i = 0; i < 8; i++) {
    vvalue[i] = (char)dvvalue[i];
  }

  return setScorePenalty(fvector, cvector, vvalue, configentry);
}

bool GeoTreeEngine::setScorePenalty(std::vector<float>& fvector,
                                    std::vector<char>& cvector,
                                    char value, int netSpeedClass,
                                    const std::string& configentry)
{
  if (netSpeedClass >= 0) {
    if (netSpeedClass >= (int)fvector.size()) {
      return false;
    }

//    return setInternalParam(fvector[netSpeedClass],(float)value,false,"")
//    && setInternalParam(cvector[netSpeedClass],value,false,configentry);
    std::vector<char> vvalue(cvector);
    vvalue[netSpeedClass] = value;
    return setScorePenalty(fvector, cvector, vvalue, configentry);
  } else if (netSpeedClass == -1) {
    std::vector<char> vvalue(8, value);
    return setScorePenalty(fvector, cvector, vvalue, configentry);
  }

  return false;
}

bool GeoTreeEngine::setPlctDlScorePenalty(char value, int netSpeedClass,
    bool setconfig)
{
  return setScorePenalty(pPenaltySched.pPlctDlScorePenaltyF,
                         pPenaltySched.pPlctDlScorePenalty, value, netSpeedClass,
                         setconfig ? "plctdlscorepenalty" : "");
}
bool GeoTreeEngine::setPlctUlScorePenalty(char value, int netSpeedClass,
    bool setconfig)
{
  return setScorePenalty(pPenaltySched.pPlctUlScorePenaltyF,
                         pPenaltySched.pPlctUlScorePenalty, value, netSpeedClass,
                         setconfig ? "plctulscorepenalty" : "");
}
bool GeoTreeEngine::setAccessDlScorePenalty(char value, int netSpeedClass,
    bool setconfig)
{
  return setScorePenalty(pPenaltySched.pAccessDlScorePenaltyF,
                         pPenaltySched.pAccessDlScorePenalty, value, netSpeedClass,
                         setconfig ? "accessdlscorepenalty" : "");
}
bool GeoTreeEngine::setAccessUlScorePenalty(char value, int netSpeedClass,
    bool setconfig)
{
  return setScorePenalty(pPenaltySched.pAccessUlScorePenaltyF,
                         pPenaltySched.pAccessUlScorePenalty, value, netSpeedClass,
                         setconfig ? "accessulscorepenalty" : "");
}
bool GeoTreeEngine::setProxyScorePenalty(char value, int netSpeedClass,
    bool setconfig)
{
  return setScorePenalty(pPenaltySched.pProxyScorePenaltyF,
                         pPenaltySched.pProxyScorePenalty, value, netSpeedClass,
                         setconfig ? "gwscorepenalty" : "");
}

bool GeoTreeEngine::setPlctDlScorePenalty(const char* value, bool setconfig)
{
  return setScorePenalty(pPenaltySched.pPlctDlScorePenaltyF,
                         pPenaltySched.pPlctDlScorePenalty, value,
                         setconfig ? "plctdlscorepenalty" : "");
}
bool GeoTreeEngine::setPlctUlScorePenalty(const char* value, bool setconfig)
{
  return setScorePenalty(pPenaltySched.pPlctUlScorePenaltyF,
                         pPenaltySched.pPlctUlScorePenalty, value,
                         setconfig ? "plctulscorepenalty" : "");
}
bool GeoTreeEngine::setAccessDlScorePenalty(const char* value, bool setconfig)
{
  return setScorePenalty(pPenaltySched.pAccessDlScorePenaltyF,
                         pPenaltySched.pAccessDlScorePenalty, value,
                         setconfig ? "accessdlscorepenalty" : "");
}
bool GeoTreeEngine::setAccessUlScorePenalty(const char* value, bool setconfig)
{
  return setScorePenalty(pPenaltySched.pAccessUlScorePenaltyF,
                         pPenaltySched.pAccessUlScorePenalty, value,
                         setconfig ? "accessulscorepenalty" : "");
}
bool GeoTreeEngine::setProxyScorePenalty(const char* value, bool setconfig)
{
  return setScorePenalty(pPenaltySched.pProxyScorePenaltyF,
                         pPenaltySched.pProxyScorePenalty, value,
                         setconfig ? "gwscorepenalty" : "");
}

bool GeoTreeEngine::setFillRatioLimit(char value, bool setconfig)
{
  return setInternalParam(pFillRatioLimit, value, true,
                          setconfig ? "fillratiolimit" : "");
}
bool GeoTreeEngine::setFillRatioCompTol(char value, bool setconfig)
{
  return setInternalParam(pFillRatioCompTol, value, true,
                          setconfig ? "fillratiocomptol" : "");
}
bool GeoTreeEngine::setSaturationThres(char value, bool setconfig)
{
  return setInternalParam(pSaturationThres, value, true,
                          setconfig ? "saturationthres" : "");
}
bool GeoTreeEngine::setTimeFrameDurationMs(int value, bool setconfig)
{
  return setInternalParam(pTimeFrameDurationMs, value, false,
                          setconfig ? "timeframedurationms" : "");
}
bool GeoTreeEngine::setPenaltyUpdateRate(float value, bool setconfig)
{
  return setInternalParam(pPenaltyUpdateRate, value, false,
                          setconfig ? "penaltyupdaterate" : "");
}

bool GeoTreeEngine::setParameter(std::string param, const std::string& value,
                                 int iparamidx, bool setconfig)
{
  std::transform(param.begin(), param.end(), param.begin(), ::tolower);
  double dval = 0.0;
  (void) sscanf(value.c_str(), "%lf", &dval);
  int ival = (int)dval;
  bool ok = false;
#define readParamVFromString(PARAM,VALUE) {                                    \
    std::string q;                                                             \
    if(sscanf(VALUE.c_str(),"[%f,%f,%f,%f,%f,%f,%f,%f]",                       \
              &PARAM##F[0],&PARAM##F[1],&PARAM##F[2],&PARAM##F[3],&PARAM##F[4],\
              &PARAM##F[5],&PARAM##F[6],&PARAM##F[7])!=8) return false;        \
    for(int i=0;i<8;i++)                                                       \
      PARAM[i]=(char)PARAM##F[i];                                              \
    ok = true;}

  if (param == "timeframedurationms") {
    ok = this->setTimeFrameDurationMs(ival, setconfig);
  } else if (param == "saturationthres") {
    ok = this->setSaturationThres((char)ival, setconfig);
  } else if (param == "fillratiocomptol") {
    ok = this->setFillRatioCompTol((char)ival, setconfig);
  } else if (param == "fillratiolimit") {
    ok = this->setFillRatioLimit((char)ival, setconfig);
  } else if (param == "accessulscorepenalty") {
    if (iparamidx > -2) {
      ok = this->setAccessUlScorePenalty((char)ival, iparamidx, setconfig);
    } else {
      readParamVFromString(pPenaltySched.pAccessUlScorePenalty, value);
    }
  } else if (param == "accessdlscorepenalty") {
    if (iparamidx > -2) {
      ok = this->setAccessDlScorePenalty((char)ival, iparamidx, setconfig);
    } else {
      readParamVFromString(pPenaltySched.pAccessDlScorePenalty, value);
    }
  } else if (param == "plctulscorepenalty") {
    if (iparamidx > -2) {
      ok = this->setPlctUlScorePenalty((char)ival, iparamidx, setconfig);
    } else {
      readParamVFromString(pPenaltySched.pPlctUlScorePenalty, value);
    }
  } else if (param == "plctdlscorepenalty") {
    if (iparamidx > -2) {
      ok = this->setPlctDlScorePenalty((char)ival, iparamidx, setconfig);
    } else {
      readParamVFromString(pPenaltySched.pPlctDlScorePenalty, value);
    }
  } else if (param == "gwscorepenalty") {
    if (iparamidx > -2) {
      ok = this->setProxyScorePenalty((char)ival, iparamidx, setconfig);
    } else {
      readParamVFromString(pPenaltySched.pProxyScorePenalty, value);
    }
  } else if (param == "skipsaturatedblcaccess") {
    ok = this->setSkipSaturatedBlcAccess((bool)ival, setconfig);
  } else if (param == "skipsaturateddrnaccess") {
    ok = this->setSkipSaturatedDrnAccess((bool)ival, setconfig);
  } else if (param == "skipsaturatedaccess") {
    ok = this->setSkipSaturatedAccess((bool)ival, setconfig);
  } else if (param == "penaltyupdaterate") {
    ok = this->setPenaltyUpdateRate((float)dval, setconfig);
  } else if (param == "disabledbranches") {
    ok = true;

    if (value.size() > 4) {
      // first, clear the list of disabled branches
      this->rmDisabledBranch("*", "*", "*", NULL);
      // remove leading and trailing square brackets
      string list(value.substr(2, value.size() - 4));
      // from the end to avoid reallocation of the string
      size_t idxl, idxr;

      while ((idxr = list.rfind(')')) != std::string::npos && ok) {
        idxl = list.rfind('(');
        auto comidx = list.find(',', idxl);
        string geotag(list.substr(idxl + 1, comidx - idxl - 1));
        auto comidx2 = list.find(',', comidx + 1);
        string optype(list.substr(comidx + 1, comidx2 - comidx - 1));
        string group(list.substr(comidx2 + 1, idxr - comidx2 - 1));
        ok = ok && this->addDisabledBranch(group, optype, geotag, NULL, setconfig);
        list.erase(idxl, std::string::npos);
      }
    }
  } else if (param == "proxyclosetofs") {
    ok = this->setProxyCloseToFs((bool)ival, setconfig);
  } else if (param == "accessgeotagmapping") {
    ok = this->setAccessGeotagMapping(value, setconfig);
  } else if (param == "accessproxygroup") {
    ok = this->setAccessProxygroup(value, setconfig);
  }

  return ok;
}

void GeoTreeEngine::setConfigValue(const char* prefix,
                                   const char* key,
                                   const char* val,
                                   bool tochangelog)
{
  gOFS->ConfEngine->SetConfigValue(prefix, key, val, tochangelog);
}

bool GeoTreeEngine::markPendingBranchDisablings(const std::string& group,
    const std::string& optype, const std::string& geotag)
{
  for (auto git = pGroup2SchedTME.begin(); git != pGroup2SchedTME.end(); git++) {
    RWMutexReadLock lock(git->second->doubleBufferMutex);

    if (group == "*" || git->first->mName == group) {
      git->second->slowTreeModified = true;
    }
  }

  return true;
}

bool GeoTreeEngine::applyBranchDisablings(const SchedTME& entry)
{
  for (auto mit = pDisabledBranches.begin(); mit != pDisabledBranches.end();
       mit++) {
    // should I lock configMutex or is it already locked?
    const std::string& group(mit->first);

    if (group != "*" && entry.group->mName != group) {
      continue;
    }

    for (auto oit = mit->second.begin(); oit != mit->second.end(); oit++) {
      const std::string& optype(oit->first);

      for (auto geoit = oit->second.begin(); geoit != oit->second.end(); geoit++) {
        const std::string& geotag(*geoit);
        auto idx = entry.backgroundFastStruct->tag2NodeIdx->getClosestFastTreeNode(
                     geotag.c_str());

        // check there is an exact geotag match
        if ((*entry.backgroundFastStruct->treeInfo)[idx].fullGeotag != geotag) {
          continue;
        }

        if (optype == "*" || optype == "plct") {
          entry.backgroundFastStruct->placementTree->disableSubTree(idx);
        }

        if (optype == "*" || optype == "accsro") {
          entry.backgroundFastStruct->rOAccessTree->disableSubTree(idx);
        }

        if (optype == "*" || optype == "accsrw") {
          entry.backgroundFastStruct->rWAccessTree->disableSubTree(idx);
        }

        if (optype == "*" || optype == "plctdrain") {
          entry.backgroundFastStruct->drnPlacementTree->disableSubTree(idx);
        }

        if (optype == "*" || optype == "accsdrain") {
          entry.backgroundFastStruct->drnAccessTree->disableSubTree(idx);
        }
      }
    }
  }

  return true;
}

bool GeoTreeEngine::addDisabledBranch(const std::string& group,
                                      const std::string& optype,
                                      const std::string& geotag,
                                      XrdOucString* output, bool toConfig)
{
  eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
  eos::common::RWMutexWriteLock lock2(pTreeMapMutex);
  eos::common::RWMutexWriteLock lock3(configMutex);
  std::vector<std::string> intersection;
  // Do checks - go through the potentially intersecting groups
  auto git_begin = group == "*" ? pDisabledBranches.begin() :
                   pDisabledBranches.find(group);
  auto git_end = (group == "*" ? pDisabledBranches.end() :
                  pDisabledBranches.find(group));

  if (git_end != pDisabledBranches.end()) {
    git_end++;
  }

  for (auto git = git_begin; git != git_end; git++) {
    // go through the potentially intersecting optypes
    auto oit_begin = (optype == "*" ? git->second.begin() : git->second.find(
                        group));
    auto oit_end = (optype == "*" ? git->second.end() : git->second.find(group));

    if (oit_end != git->second.end()) {
      oit_end++;
    }

    for (auto oit = oit_begin; oit != oit_end; oit++) {
      XrdOucString toinsert(geotag.c_str());

      // Check that none of the disabled geotag is a prefix of the current one
      // and the other way around.
      for (auto geoit = oit->second.begin(); geoit != oit->second.end(); geoit++) {
        XrdOucString alreadyThere(geoit->c_str());

        if (alreadyThere.beginswith(toinsert) || toinsert.beginswith(alreadyThere)) {
          intersection.push_back(std::string("(") + geotag.c_str() + std::string(",") +
                                 oit->first + std::string(",") + git->first +
                                 std::string(")") + std::string(alreadyThere.c_str()));
        }
      }
    }
  }

  if (intersection.size()) {
    if (output) {
      output->append((std::string("unable to add disabled branch : ") +
                      std::string("(") + geotag + std::string(",") + optype +
                      std::string(",") + geotag +
                      std::string(") clashes with : ")).c_str());

      for (auto iit = intersection.begin(); iit != intersection.end(); iit++) {
        output->append((*iit + " , ").c_str());
      }
    }

    return false;
  }

  // Update the internal value
  pDisabledBranches[group][optype].insert(geotag);
  // To apply the new set of rules, mark the involved slow trees as modified to force a refresh
  markPendingBranchDisablings(group, optype, geotag);

  // update the config
  if (toConfig) {
    XrdOucString outStr("[ ");
    showDisabledBranches("*", "*", "*", &outStr, false);
    outStr.replace(")\n(", ") , (");
    outStr.replace(")\n", ")");
    outStr += " ]";
    setConfigValue("geosched", "disabledbranches" , outStr.c_str());
  }

  return true;
}

bool GeoTreeEngine::rmDisabledBranch(const std::string& group,
                                     const std::string& optype,
                                     const std::string& geotag,
                                     XrdOucString* output, bool toConfig)
{
  eos::common::RWMutexWriteLock lock(pAddRmFsMutex);
  eos::common::RWMutexWriteLock lock2(pTreeMapMutex);
  eos::common::RWMutexWriteLock lock3(configMutex);
  bool found = false;

  if (group == "*" && optype == "*" && geotag == "*") {
    found = true;
    eos_notice("clearing disabled branch list in GeoTreeEngine");
    pDisabledBranches.clear();
  } else if (pDisabledBranches.count(group)) {
    if (pDisabledBranches[group].count(optype)) {
      found = (bool)pDisabledBranches[group][optype].erase(geotag);
    }
  }

  if (!found) {
    if (output) output->append((std::string("could not find disabled branch : ") +
                                  std::string("(") + group + std::string(" , ") +
                                  optype + std::string(") -> ") + geotag).c_str());
  } else {
    // To apply the new set of rules, mark the involved slow trees as modified
    // to force a refresh.
    markPendingBranchDisablings(group, optype, geotag);

    if (toConfig) {
      // Update the config
      XrdOucString outStr("[ ");
      showDisabledBranches("*", "*", "*", &outStr, false);
      outStr.replace(")\n(", ") , (");
      outStr.replace(")\n", ")");
      outStr += " ]";
      setConfigValue("geosched", "disabledbranches" , outStr.c_str());
    }
  }

  return found;
}

bool
GeoTreeEngine::showDisabledBranches(const std::string& group,
                                    const std::string& optype,
                                    const std::string& geotag,
                                    XrdOucString* output, bool lock)
{
  if (lock) {
    configMutex.LockRead();
  }

  for (auto git = pDisabledBranches.begin(); git != pDisabledBranches.end();
       git++) {
    if (group == "*" || group == git->first)
      for (auto oit = git->second.begin(); oit != git->second.end(); oit++) {
        if (optype == "*" || optype == oit->first)
          for (auto geoit = oit->second.begin(); geoit != oit->second.end(); geoit++) {
            if (geotag == "*" || geotag == *geoit)
              if (output) {
                output->append((std::string("(") + *geoit + std::string(",") + oit->first +
                                std::string(",") + git->first + std::string(")\n")).c_str());
              }
          }
      }
  }

  if (lock) {
    configMutex.UnLockRead();
  }

  return true;
}

std::string GeoTreeEngine::AccessStruct::getMappingStr() const
{
  std::string ret;

  for (auto it = accessGeotagMap.begin(); it != accessGeotagMap.end() ; it++) {
    if (it != accessGeotagMap.begin()) {
      ret.append(";");
    }

    ret.append(it->first);
    ret.append("=>");
    ret.append(it->second);
  }

  return ret;
}

bool GeoTreeEngine::AccessStruct::setMapping(const std::string& mapping,
    bool setconfig)
{
  std::string mappingelement, geotag, geotaglist;
  std::stringstream ss(mapping);

  while (std::getline(ss, mappingelement, ';')) {
    auto idx = mappingelement.find("=>");

    if (idx == std::string::npos) {
      eos_static_err("error parsing config entry while restoring config : %s",
                     mappingelement.c_str());
      return false;
    }

    geotag = mappingelement.substr(0, idx);
    geotaglist = mappingelement.substr(idx + 2, std::string::npos);
    setMapping(geotag, geotaglist, false, false);
  }

  if (!geotag.empty()) {
    return setMapping(geotag, geotaglist, true,
                      setconfig);  // to rebuild the tree and set the config
  } else {
    return true;
  }
}

bool GeoTreeEngine::AccessStruct::setMapping(const std::string& geotag,
    const std::string& geotaglist, bool updateFastStruct, bool setconfig)
{
  RWMutexWriteLock lock(accessMutex);

  if (!inuse) {
    accessST = new SlowTree("AccessGeotagMapping");
    accessFT = new FastGatewayAccessTree();
    accessFT->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());
    accessFTI = new SchedTreeBase::FastTreeInfo();
    accessFTI->reserve(FastGatewayAccessTree::sGetMaxNodeCount());
    accessHost2Idx = new Host2TreeIdxMap();
    accessHost2Idx->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());
    accessTag2Idx = new GeoTag2NodeIdxMap();
    accessTag2Idx->selfAllocate(FastGatewayAccessTree::sGetMaxNodeCount());;
    inuse = true;
  }

  SlowTree::TreeNodeInfo tni;
  SlowTree::TreeNodeStateFloat tns;
  tni.geotag = geotag;
  tni.proxygroup = geotaglist;
  accessST->insert(&tni, &tns, false, true);
  accessGeotagMap[geotag] = geotaglist;

  if (updateFastStruct) {
    accessST->buildFastStrcturesAccess(accessFT, accessHost2Idx, accessFTI,
                                       accessTag2Idx);
  }

  if (setconfig) {
    setConfigValue("geosched", configkey.c_str(), getMappingStr().c_str());
  }

  return true;
}

bool GeoTreeEngine::AccessStruct::clearMapping(const std::string& geotag,
    bool updateFastStruct, bool setconfig)
{
  RWMutexWriteLock lock(accessMutex);

  if (inuse) {
    SlowTree::TreeNodeInfo tni;
    tni.geotag = geotag;

    // if we have a geotag, we remove that geotag
    if (!geotag.empty() && !accessST->remove(&tni, false)) {
      return false;
    }

    if (!geotag.empty()) {
      accessGeotagMap.erase(geotag);
    }

    // if we don't have a geotag or if the tree is now empty, remove everything
    if (geotag.empty() || accessST->getNodeCount() == 1) {
      delete accessST;
      delete accessFT;
      delete accessFTI;
      delete accessHost2Idx;
      delete accessTag2Idx;
      accessGeotagMap.clear();
      inuse = false;
    } else if (updateFastStruct) {
      accessST->buildFastStrcturesAccess(accessFT, accessHost2Idx, accessFTI,
                                         accessTag2Idx);
    }
  }

  if (setconfig) {
    setConfigValue("geosched", configkey.c_str(), getMappingStr().c_str());
  }

  return true;
}

bool GeoTreeEngine::AccessStruct::showMapping(XrdOucString* output,
    std::string operation,
    bool monitoring)
{
  RWMutexReadLock lock(accessMutex);

  if (inuse) {
    unsigned geo_depth_max = 0;
    std::string format_s = !monitoring ? "s" : "os";
    std::string format_ss = !monitoring ? "-s" : "os";
    // Set for tree: num of line, depth, prefix_1, prefix_2, fullGeotag, proxygroup/direct
    std::set<std::tuple<unsigned, unsigned, unsigned, unsigned, std::string, std::string>>
        data_access;
    accessST->displayAccess(data_access, geo_depth_max);
    TableFormatterBase table_access;
    TableHeader table_header;
    table_header.push_back(std::make_tuple("operation", 6, format_ss));
    table_header.push_back(std::make_tuple("geotag", 6, format_ss));

    if (!monitoring) {
      if (geo_depth_max > 1) {
        for (unsigned i = 1; i < geo_depth_max; i++) {
          std::string name = "lev" + std::to_string(i);
          table_header.push_back(std::make_tuple(name, 4, format_ss));
        }
      }

      table_header.push_back(std::make_tuple("fullGeotag", 6, format_s));
    }

    table_header.push_back(std::make_tuple("mapping", 6, format_s));
    table_access.SetHeader(table_header);
    unsigned prefix[geo_depth_max + 1];

    for (auto it : data_access) {
      if (!monitoring) {
        unsigned geo_depth = 0;
        std::string geotag_temp = std::get<4>(it);

        while (geotag_temp.find("::") != std::string::npos) {
          geotag_temp.erase(0, geotag_temp.find("::") + 2);
          geo_depth++;
        }

        TableData table_data;
        table_data.emplace_back();

        // Print operation (depth=1)
        if (std::get<1>(it) == 1) {
          table_data.back().push_back(TableCell(operation, "s"));
        }
        // Print geotag (depth=2 or 3)
        else if (std::get<1>(it) == 2 || std::get<1>(it) == 3) {
          if (geo_depth > 0) {
            prefix[geo_depth - 1] = std::get<2>(it);
          }

          prefix[geo_depth] = std::get<3>(it);

          for (unsigned i = 0; i <= geo_depth; i++) { // arrows
            table_data.back().push_back(TableCell(prefix[i], "t"));
          }

          std::string geotag = std::get<4>(it);
          geotag = (geo_depth > 0) ? geotag.substr(geotag.rfind("::") + 2) : geotag;
          table_data.back().push_back(TableCell(geotag, "s"));

          for (unsigned i = 0; i < geo_depth_max - geo_depth - 1;
               i++) { // blank cell after geotag
            table_data.back().push_back(TableCell("", "s"));
          }
        }

        // Print other columns
        if (!std::get<5>(it).empty()) {
          table_data.back().push_back(TableCell(std::get<4>(it), "s"));
          table_data.back().push_back(TableCell(std::get<5>(it), "s"));
        }

        table_access.AddRows(table_data);
      }
      // Monitoring
      else if (!std::get<5>(it).empty()) {
        TableData table_data;
        table_data.emplace_back();
        table_data.back().push_back(TableCell(operation, "s"));
        table_data.back().push_back(TableCell(std::get<4>(it), "s"));
        table_data.back().push_back(TableCell(std::get<5>(it), "s"));
        table_access.AddRows(table_data);
      }
    }

    output->append(table_access.GenerateTable(HEADER).c_str());
    return true;
  }

  return false;
}

bool GeoTreeEngine::accessReqFwEP(const std::string& targetGeotag,
                                  const std::string& accesserGeotag) const
{
  // if no direct access geotag mapping is defined, all accesses are direct
  if (!pAccessGeotagMapping.inuse) {
    return false;
  }

  // first get the parent node giving the access rule
  auto idx  = pAccessGeotagMapping.accessTag2Idx->getClosestFastTreeNode(
                accesserGeotag.c_str());
  SchedTreeBase::tFastTreeIdx idx2 = 0;
  pAccessGeotagMapping.accessFT->findFreeSlotFirstHitBack(idx2, idx);
  // parse the geotag list and check the access
  auto accessible = (*pAccessGeotagMapping.accessFTI)[idx2].proxygroup;
  size_t beg = std::numeric_limits<size_t>::max(),
         end = std::numeric_limits<size_t>::max();

  for (size_t i = 0; i < accessible.size(); i++) {
    if (accessible[i] == ',') {
      if (beg == std::numeric_limits<size_t>::max()) {
        continue;
      }

      end = i;

      // if we have a new token
      if (end > beg) {
        if (((end - beg) <= targetGeotag.size()
             && ((end - beg) == targetGeotag.size() || targetGeotag[end - beg] == ':'))
            && !strncmp(targetGeotag.c_str(), accessible.c_str() + beg, end - beg)) {
          return false;
        }

        beg = end + 1;
      }
    } else if (beg == std::numeric_limits<size_t>::max()) {
      beg = i;
    }
  }

  // the end of the string is also the end of the last token
  if (beg < accessible.size()) {
    end = accessible.size();
  }

  if (end > beg) {
    if (((end - beg) <= targetGeotag.size()
         && ((end - beg) == targetGeotag.size() || targetGeotag[end - beg] == ':'))
        && !strncmp(targetGeotag.c_str(), accessible.c_str() + beg, end - beg)) {
      return false;
    }
  }

  return true;
}
std::string GeoTreeEngine::accessGetProxygroup(const std::string& toAccess)
const
{
  // if no access proxygroup mapping is defined, there is no proxygroup to return
  if (!pAccessProxygroup.inuse) {
    return "";
  }

  // first get the parent node giving the proxygroup
  auto idx  = pAccessProxygroup.accessTag2Idx->getClosestFastTreeNode(
                toAccess.c_str());
  SchedTreeBase::tFastTreeIdx idx2 = 0;
  pAccessProxygroup.accessFT->findFreeSlotFirstHitBack(idx2, idx);
  return (*pAccessProxygroup.accessFTI)[idx2].proxygroup;
}


void GeoTreeEngine::tlFree(void* arg)
{
  eos_static_debug("destroying thread specific geobuffer");
  // delete the buffer
  delete[](char*)arg;
}

char* GeoTreeEngine::tlAlloc(size_t size)
{
  eos_static_debug("allocating thread specific geobuffer");
  char* buf = new char[size];

  if (pthread_setspecific(gPthreadKey, buf)) {
    eos_static_crit("error registering thread-local buffer located at %p for "
                    "cleaning up : memory will be leaked when thread is "
                    "terminated", buf);
  }

  return buf;
}

EOSMGMNAMESPACE_END
