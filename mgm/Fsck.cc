//------------------------------------------------------------------------------
// File: Fsck.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include <iostream>
#include <fstream>
#include <vector>
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Mapping.hh"
#include "mgm/Fsck.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/Messaging.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.cc"

EOSMGMNAMESPACE_BEGIN

const char* Fsck::gFsckEnabled = "fsck";
const char* Fsck::gFsckInterval = "fsckinterval";


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Fsck::Fsck():
  mEnabled(false), mInterval(30), mRunning(false), eTimeStamp(0)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Fsck::~Fsck()
{
  (void) Stop(false);
  std::cerr << __FUNCTION__ << ":: end of destructor" << std::endl;
}

//------------------------------------------------------------------------------
// Start FSCK trhread
//------------------------------------------------------------------------------
bool
Fsck::Start(int interval)
{
  if (interval) {
    mInterval = interval;
  }

  if (!mRunning) {
    mThread.reset(&Fsck::Check, this);
    mRunning = true;
    mEnabled = "true";
    return StoreFsckConfig();
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Stop FSCK thread
//------------------------------------------------------------------------------
bool
Fsck::Stop(bool store)
{
  if (mRunning) {
    eos_static_info("%s", "msg=\"join FSCK thread\"");
    mThread.join();
    mRunning = false;
    mEnabled = false;
    Log("disabled check");

    if (store) {
      return StoreFsckConfig();
    } else {
      return true;
    }
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
//! Apply the FSCK configuration stored in the configuration engine
//------------------------------------------------------------------------------
void
Fsck::ApplyFsckConfig()
{
  std::string enabled = FsView::gFsView.GetGlobalConfig(gFsckEnabled);

  if (enabled.length()) {
    mEnabled = enabled.c_str();
  }

  std::string interval = FsView::gFsView.GetGlobalConfig(gFsckInterval);

  if (interval.length()) {
    mInterval = atoi(interval.c_str());

    if (mInterval < 0) {
      mInterval = 30;
    }
  }

  Log("enabled=%s", mEnabled.c_str());
  Log("check interval=%d minutes", mInterval);

  if (mEnabled == "true") {
    Start();
  } else {
    Stop();
  }
}

//------------------------------------------------------------------------------
// Store the current running FSCK configuration in the config engine
//------------------------------------------------------------------------------
bool
Fsck::StoreFsckConfig()
{
  bool ok = 1;
  XrdOucString sInterval = "";
  sInterval += (int) mInterval;
  ok &= FsView::gFsView.SetGlobalConfig(gFsckEnabled, mEnabled.c_str());
  ok &= FsView::gFsView.SetGlobalConfig(gFsckInterval, sInterval.c_str());
  return ok;
}

//------------------------------------------------------------------------------
// Looping thread function collecting FSCK results
//------------------------------------------------------------------------------
void
Fsck::Check(ThreadAssistant& assistant) noexcept
{
  int bccount = 0;
  ClearLog();
  gOFS->WaitUntilNamespaceIsBooted();

  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(1));
    eos_static_debug("msg=\"start consistency checker thread\"");
    ClearLog();
    Log("started check");

    // Don't run fsck if we are not a master
    while (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(60));

      if (assistant.terminationRequested()) {
        return;
      }
    }

    Log("Filesystems to check: %lu", FsView::gFsView.GetNumFileSystems());
    // Broadcast fsck request and collect responses
    XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
    broadcastresponsequeue += "-fsck-";
    broadcastresponsequeue += bccount;
    XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;
    XrdOucString msgbody;
    // mgm.fsck.tags no longer necessary for newer versions, but keeping for
    // compatibility
    msgbody = "mgm.cmd=fsck&mgm.fsck.tags=*";
    XrdOucString stdOut = "";
    XrdOucString stdErr = "";

    if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue,
        broadcasttargetqueue, msgbody,
        stdOut, 10, &assistant)) {
      eos_static_err("msg=\"failed to broadcast and collect fsck from [%s]:[%s]\"",
                     broadcastresponsequeue.c_str(), broadcasttargetqueue.c_str());
      stdErr = "error: broadcast failed\n";
    }

    ResetErrorMaps();
    std::vector<std::string> lines;
    // Convert into a lines-wise seperated array
    eos::common::StringConversion::StringToLineVector((char*) stdOut.c_str(),
        lines);

    for (size_t nlines = 0; nlines < lines.size(); ++nlines) {
      std::set<unsigned long long> fids;
      unsigned long fsid = 0;
      std::string err_tag;

      if (eos::common::StringConversion::ParseStringIdSet((char*)
          lines[nlines].c_str(), err_tag, fsid, fids)) {
        if (fsid) {
          XrdSysMutexHelper lock(eMutex);

          // Add the fids into the error maps
          for (auto it = fids.cbegin(); it != fids.cend(); ++it) {
            eFsMap[err_tag][fsid].insert(*it);
            eMap[err_tag].insert(*it);
            eCount[err_tag]++;
          }
        }
      } else {
        eos_static_err("msg=\"cannot parse fsck response\" msg=\"%s\"",
                       lines[nlines].c_str());
      }
    }

    // @todo (esindril): note this is heavy for the qdb ns
    AccountOfflineReplicas();
    PrintOfflineReplicas();
    AccountNoReplicaFiles();
    AccountOfflineFiles();
    PrintErrorsSummary();
    // @todo (esindril): note this is heavy for the qdb ns
    AccountDarkFiles();
    Log("stopping check");
    Log("=> next run in %d minutes", mInterval);
    // Wait for next FSCK round ...
    assistant.wait_for(std::chrono::minutes(mInterval));
  }
}

//------------------------------------------------------------------------------
// Print the current log output
//------------------------------------------------------------------------------
void
Fsck::PrintOut(XrdOucString& out, XrdOucString option)
{
  XrdSysMutexHelper lock(mLogMutex);
  out = mLog;
}

//------------------------------------------------------------------------------
// Get usage information
//------------------------------------------------------------------------------
bool
Fsck::Usage(XrdOucString& out, XrdOucString& err)
{
  err += "error: invalid option specified\n";
  return false;
}

//------------------------------------------------------------------------------
// Return the current FSCK report
//------------------------------------------------------------------------------
bool
Fsck::Report(XrdOucString& out, XrdOucString& err, XrdOucString option,
             XrdOucString selection)
{
  bool printfid = (option.find("i") != STR_NPOS);
  bool printlfn = (option.find("l") != STR_NPOS);
  XrdSysMutexHelper lock(eMutex);
  XrdOucString checkoption = option;
  checkoption.replace("h", "");
  checkoption.replace("json", "");
  checkoption.replace("i", "");
  checkoption.replace("l", "");
  checkoption.replace("a", "");

  if (checkoption.length()) {
    return Fsck::Usage(out, err);
  }

  char stimestamp[1024];
  snprintf(stimestamp,
           sizeof(stimestamp) - 1,
           "%lu",
           (unsigned long) eTimeStamp);

  if ((option.find("json") != STR_NPOS) || (option.find("j") != STR_NPOS)) {
    // json output format
    out += "{\n";
    // put the check timestamp
    out += "  \"timestamp\": ";
    out += stimestamp;
    out += ",\n";

    if (!(option.find("a") != STR_NPOS)) {
      // Dump global table
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (selection.length() && (selection.find(emapit->first.c_str()) == STR_NPOS)) {
          continue;  // skip unselected
        }

        char sn[1024];
        snprintf(sn, sizeof(sn) - 1, "%llu",
                 (unsigned long long) emapit->second.size());
        out += "  \"";
        out += emapit->first.c_str();
        out += "\": {\n";
        out += "    \"n\":\"";
        out += sn;
        out += "\",\n";

        if (printfid) {
          out += "    \"fxid\": [";

          for (auto fidit = emapit->second.cbegin();
               fidit != emapit->second.cend(); ++fidit) {
            out += eos::common::FileId::Fid2Hex(*fidit).c_str();
            out += ",";
          }

          if (out.endswith(",")) {
            out.erase(out.length() - 1);
          }

          out += "]\n";
        }

        if (printlfn) {
          out += "    \"lfn\": [";
          std::set <eos::common::FileId::fileid_t>::const_iterator fidit;

          for (fidit = emapit->second.begin();
               fidit != emapit->second.end();
               fidit++) {
            std::shared_ptr<eos::IFileMD> fmd;
            eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

            try {
              fmd = gOFS->eosFileService->getFileMD(*fidit);
              std::string fullpath = gOFS->eosView->getUri(fmd.get());
              out += "\"";
              out += fullpath.c_str();
              out += "\"";
            } catch (eos::MDException& e) {
              out += "\"undefined\"";
            }

            out += ",";
          }

          if (out.endswith(",")) {
            out.erase(out.length() - 1);
          }

          out += "]\n";
        }

        if (out.endswith(",\n")) {
          out.erase(out.length() - 2);
          out += "\n";
        }

        out += "  },\n";
      }
    } else {
      // Do output per filesystem
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (selection.length() &&
            (selection.find(emapit->first.c_str()) == STR_NPOS)) {
          continue;  // skip unselected
        }

        // Loop over errors
        char sn[1024];
        snprintf(sn, sizeof(sn) - 1, "%llu",
                 (unsigned long long) emapit->second.size());
        out += "  \"";
        out += emapit->first.c_str();
        out += "\": {\n";
        out += "    \"n\":\"";
        out += sn;
        out += "\",\n";
        out += "    \"fsid\":";
        out += " {\n";
        std::map < eos::common::FileSystem::fsid_t,
            std::set < eos::common::FileId::fileid_t >> ::const_iterator efsmapit;

        for (efsmapit = eFsMap[emapit->first].begin();
             efsmapit != eFsMap[emapit->first].end();
             efsmapit++) {
          if (emapit->first == "zero_replica") {
            // This we cannot break down by filesystem id
            continue;
          }

          // Loop over filesystems
          out += "      \"";
          out += (int) efsmapit->first;
          out += "\": {\n";
          snprintf(sn, sizeof(sn) - 1,
                   "%llu",
                   (unsigned long long) efsmapit->second.size());
          out += "        \"n\": ";
          out += sn;
          out += ",\n";

          if (printfid) {
            out += "        \"fxid\": [";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;

            for (fidit = efsmapit->second.begin();
                 fidit != efsmapit->second.end(); fidit++) {
              out += eos::common::FileId::Fid2Hex(*fidit).c_str();
              out += ",";
            }

            if (out.endswith(",")) {
              out.erase(out.length() - 1);
            }

            out += "]\n";
          }

          if (printlfn) {
            out += "        \"lfn\": [";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;

            for (fidit = efsmapit->second.begin();
                 fidit != efsmapit->second.end();
                 fidit++) {
              std::shared_ptr<eos::IFileMD> fmd = std::shared_ptr<eos::IFileMD>((
                                                    eos::IFileMD*)0);
              eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
              eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

              try {
                fmd = gOFS->eosFileService->getFileMD(*fidit);
                std::string fullpath = gOFS->eosView->getUri(fmd.get());
                out += "\"";
                out += fullpath.c_str();
                out += "\"";
              } catch (eos::MDException& e) {
                out += "\"undefined\"";
              }

              out += ",";
            }

            if (out.endswith(",")) {
              out.erase(out.length() - 1);
            }

            out += "]\n";
          }

          if (out.endswith(",\n")) {
            out.erase(out.length() - 2);
            out += "\n";
          }

          out += "      },\n";
        }

        out += "    },\n";
      }
    }

    // list shadow filesystems
    std::map<eos::common::FileSystem::fsid_t,
        unsigned long long >::const_iterator fsit;
    out += "  \"shadow_fsid\": [";

    for (fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++) {
      char sfsid[1024];
      snprintf(sfsid, sizeof(sfsid) - 1,
               "%lu",
               (unsigned long) fsit->first);
      out += sfsid;
      out += ",";
    }

    if (out.endswith(",")) {
      out.erase(out.length() - 1);
    }

    out += "  ]\n";
    out += "}\n";
  } else {
    // greppable format
    if (!(option.find("a") != STR_NPOS)) {
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (selection.length() &&
            (selection.find(emapit->first.c_str()) == STR_NPOS)) {
          continue;  // skip unselected
        }

        char sn[1024];
        snprintf(sn, sizeof(sn) - 1,
                 "%llu",
                 (unsigned long long) emapit->second.size());
        out += "timestamp=";
        out += stimestamp;
        out += " ";
        out += "tag=\"";
        out += emapit->first.c_str();
        out += "\"";
        out += " n=";
        out += sn;

        if (printfid) {
          out += " fid=";

          for (auto fidit = emapit->second.cbegin();
               fidit != emapit->second.cend(); ++fidit) {
            out += eos::common::FileId::Fid2Hex(*fidit).c_str();
            out += ",";
          }

          if (out.endswith(",")) {
            out.erase(out.length() - 1);
          }

          out += "\n";
        }

        if (printlfn) {
          out += " lfn=";

          for (auto fidit = emapit->second.cbegin();
               fidit != emapit->second.cend(); fidit++) {
            std::shared_ptr<eos::IFileMD> fmd =
              std::shared_ptr<eos::IFileMD>((eos::IFileMD*)0);
            eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

            try {
              fmd = gOFS->eosFileService->getFileMD(*fidit);
              std::string fullpath = gOFS->eosView->getUri(fmd.get());
              out += "\"";
              out += fullpath.c_str();
              out += "\"";
            } catch (eos::MDException& e) {
              out += "\"undefined\"";
            }

            out += ",";
          }

          if (out.endswith(",")) {
            out.erase(out.length() - 1);
          }

          out += "\n";
        }

        // List shadow filesystems
        out += " shadow_fsid=";

        for (auto fsit = eFsDark.cbegin(); fsit != eFsDark.cend(); ++fsit) {
          char sfsid[1024];
          snprintf(sfsid, sizeof(sfsid) - 1,
                   "%lu",
                   (unsigned long) fsit->first);
          out += sfsid;
          out += ",";
        }

        if (out.endswith(",")) {
          out.erase(out.length() - 1);
        }

        out += "\n";
      }
    } else {
      // Do output per filesystem
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (selection.length() &&
            (selection.find(emapit->first.c_str()) == STR_NPOS)) {
          continue;  // skip unselected
        }

        // Loop over filesystems
        for (auto efsmapit = eFsMap[emapit->first].cbegin();
             efsmapit != eFsMap[emapit->first].cend(); ++efsmapit) {
          if (emapit->first == "zero_replica") {
            // This we cannot break down by filesystem id
            continue;
          }

          char sn[1024];
          out += "timestamp=";
          out += stimestamp;
          out += " ";
          out += "tag=\"";
          out += emapit->first.c_str();
          out += "\"";
          out += " ";
          out += "fsid=";
          out += (int) efsmapit->first;
          snprintf(sn, sizeof(sn) - 1,
                   "%llu",
                   (unsigned long long) efsmapit->second.size());
          out += " n=";
          out += sn;

          if (printfid) {
            out += " fid=";

            for (auto fidit = efsmapit->second.cbegin();
                 fidit != efsmapit->second.cend(); ++fidit) {
              out += eos::common::FileId::Fid2Hex(*fidit).c_str();
              out += ",";
            }

            if (out.endswith(",")) {
              out.erase(out.length() - 1);
            }

            out += "\n";
          } else {
            if (printlfn) {
              out += " lfn=";

              for (auto fidit = efsmapit->second.cbegin();
                   fidit != efsmapit->second.cend(); ++fidit) {
                std::shared_ptr<eos::IFileMD> fmd;
                eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
                eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

                try {
                  fmd = gOFS->eosFileService->getFileMD(*fidit);
                  std::string fullpath = gOFS->eosView->getUri(fmd.get());
                  out += "\"";
                  out += fullpath.c_str();
                  out += "\"";
                } catch (eos::MDException& e) {
                  out += "\"undefined\"";
                }

                out += ",";
              }

              if (out.endswith(",")) {
                out.erase(out.length() - 1);
              }

              out += "\n";
            } else {
              out += "\n";
            }
          }
        }
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Method to issue a repair action
//------------------------------------------------------------------------------
bool
Fsck::Repair(XrdOucString& out, XrdOucString& err, XrdOucString option)
{
  XrdSysMutexHelper lock(eMutex);

  // Check for a valid action in option
  if ((option != "checksum") &&
      (option != "checksum-commit") &&
      (option != "resync") &&
      (option != "unlink-unregistered") &&
      (option != "unlink-orphans") &&
      (option != "adjust-replicas") &&
      (option != "adjust-replicas-nodrop") &&
      (option != "drop-missing-replicas") &&
      (option != "unlink-zero-replicas") &&
      (option != "replace-damaged-replicas")) {
    err += "error: illegal option <";
    err += option;
    err += ">\n";
    return false;
  }

  if (option.beginswith("checksum")) {
    out += "# repair checksum ------------------------------------------------"
           "-------------------------\n";
    std::map < eos::common::FileSystem::fsid_t,
        std::set < eos::common::FileId::fileid_t >> fid2check;

    // Loop over all filesystems
    for (auto efsmapit = eFsMap["m_cx_diff"].cbegin();
         efsmapit != eFsMap["m_cx_diff"].cend(); ++efsmapit) {
      // Loop over all fids
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        fid2check[efsmapit->first].insert(*it);
      }
    }

    // Loop over all filesystems
    for (auto efsmapit = eFsMap["d_cx_diff"].cbegin();
         efsmapit != eFsMap["d_cx_diff"].cend(); ++efsmapit) {
      std::set <eos::common::FileId::fileid_t>::const_iterator it;

      // Loop over all fids
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        fid2check[efsmapit->first].insert(*it);
      }
    }

    // Loop over all filesystems
    for (auto efsmapit = fid2check.cbegin();
         efsmapit != fid2check.cend(); ++efsmapit) {
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        std::string path = "";
        std::shared_ptr<eos::IFileMD> fmd;
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

        try {
          fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd.get());
        } catch (eos::MDException& e) {}

        // Issue verify operations on that particular filesystem
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        int lretc = 1;

        if (path.length()) {
          if (option == "checksum-commit") {
            // Verify & commit
            lretc = gOFS->_verifystripe(path.c_str(), error, vid, efsmapit->first,
                                        "&mgm.verify.compute.checksum=1&"
                                        "mgm.verify.commit.checksum=1&"
                                        "mgm.verify.commit.size=1");
          } else {
            // Verify only
            lretc = gOFS->_verifystripe(path.c_str(), error, vid, efsmapit->first,
                                        "&mgm.verify.compute.checksum=1");
          }

          if (!lretc) {
            out += "success: sending verify to fsid=";
            out += (int) efsmapit->first;
            out += " for path=";
            out += path.c_str();
            out += "\n";
          } else {
            out += "error: sending verify to fsid=";
            out += (int) efsmapit->first;
            out += " failed for path=";
            out += path.c_str();
            out += "\n";
          }
        }
      }
    }

    return true;
  }

  if (option.beginswith("resync")) {
    out += "# resync         ------------------------------------------------"
           "-------------------------\n";
    std::map < eos::common::FileSystem::fsid_t,
        std::set < eos::common::FileId::fileid_t >> fid2check;

    for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
      // Don't sync offline replicas
      if (emapit->first == "rep_offline") {
        continue;
      }

      // Loop over all filesystems
      for (auto efsmapit = eFsMap[emapit->first].cbegin();
           efsmapit != eFsMap[emapit->first].cend(); ++efsmapit) {
        // Loop over all fids
        for (auto it = efsmapit->second.cbegin();
             it != efsmapit->second.cend(); ++it) {
          fid2check[efsmapit->first].insert(*it);
        }
      }
    }

    // Loop over all filesystems
    for (auto  efsmapit = fid2check.cbegin();
         efsmapit != fid2check.cend(); ++efsmapit) {
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        std::string path = "";
        std::shared_ptr<eos::IFileMD> fmd =
          std::shared_ptr<eos::IFileMD>((eos::IFileMD*)0);
        eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

        try {
          fmd = gOFS->eosFileService->getFileMD(*it);
        } catch (eos::MDException& e) {}

        if (fmd) {
          int lretc = 0;
          // Issue a resync command for a filesystem/fid pair
          lretc = gOFS->SendResync(*it, efsmapit->first);

          if (lretc) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: sending resync to fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "error: sending resync to fsid=%u failed for fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
        } else {
          char outline[1024];
          snprintf(outline, sizeof(outline) - 1,
                   "error: no file meta data for fsid=%u failed for fid=%08llx\n",
                   efsmapit->first, *it);
          out += outline;
        }
      }
    }

    return true;
  }

  if (option == "unlink-unregistered") {
    out += "# unlink unregistered --------------------------------------------"
           "-------------------------\n";
    // Unlink all unregistered files
    std::map < eos::common::FileSystem::fsid_t,
        std::set < eos::common::FileId::fileid_t >> fid2check;
    eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
    XrdOucErrInfo error;

    // Loop over all filesystems
    for (auto efsmapit = eFsMap["unreg_n"].cbegin();
         efsmapit != eFsMap["unreg_n"].cend(); ++efsmapit) {
      // Loop over all fids
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        std::shared_ptr<eos::IFileMD> fmd;
        bool haslocation = false;
        std::string spath = "";

        // Crosscheck if the location really is not attached
        try {
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
          spath = gOFS->eosView->getUri(fmd.get());

          if (fmd->hasLocation(efsmapit->first)) {
            haslocation = true;
          }
        } catch (eos::MDException& e) {}

        // Send external deletion
        if (gOFS->DeleteExternal(efsmapit->first, *it)) {
          char outline[1024];
          snprintf(outline, sizeof(outline) - 1,
                   "success: send unlink to fsid=%u fid=%08llx\n",
                   efsmapit->first, *it);
          out += outline;
        } else {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "err: unable to send unlink to fsid=%u fid=%08llx\n",
                   efsmapit->first, *it);
          out += errline;
        }

        if (haslocation) {
          // Drop from the namespace
          if (gOFS->_dropstripe(spath.c_str(), *it, error, vid, efsmapit->first, false)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "error: unable to drop stripe on fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send dropped stripe on fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          }
        }
      }
    }

    return true;
  }

  if (option == "unlink-orphans") {
    out += "# unlink orphans  ------------------------------------------------"
           "-------------------------\n";
    // Unlink all orphaned files
    std::map < eos::common::FileSystem::fsid_t,
        std::set < eos::common::FileId::fileid_t >> fid2check;

    // Loop over all filesystems
    for (auto efsmapit = eFsMap["orphans_n"].cbegin();
         efsmapit != eFsMap["orphans_n"].cend(); ++efsmapit) {
      // Loop over all fids
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        std::shared_ptr<eos::IFileMD> fmd;
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        bool haslocation = false;

        // Crosscheck if the location really is not attached
        try {
          fmd = gOFS->eosFileService->getFileMD(*it);

          if (fmd->hasLocation(efsmapit->first)) {
            haslocation = true;
          }
        } catch (eos::MDException& e) {}

        if (!haslocation) {
          if (gOFS->DeleteExternal(efsmapit->first, *it)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send unlink to fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to send unlink to fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += errline;
          }
        } else {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "err: not sending unlink to fsid=%u fid=%08llx - location exists!\n",
                   efsmapit->first, *it);
          out += errline;
        }
      }
    }

    return true;
  }

  if (option.beginswith("adjust-replicas")) {
    out += "# adjust replicas ------------------------------------------------"
           "-------------------------\n";
    // Adjust all layout errors e.g. missing replicas where possible
    std::map < eos::common::FileSystem::fsid_t,
        std::set < eos::common::FileId::fileid_t >> fid2check;

    // Loop over all filesystems
    for (auto efsmapit = eFsMap["rep_diff_n"].cbegin();
         efsmapit != eFsMap["rep_diff_n"].cend(); ++efsmapit) {
      // Loop over all fids
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        std::shared_ptr<eos::IFileMD> fmd;
        std::string path = "";

        try {
          {
            eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
            fmd = gOFS->eosFileService->getFileMD(*it);
            path = gOFS->eosView->getUri(fmd.get());
          }
          // Execute adjust replica
          eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
          XrdOucErrInfo error;
          ProcCommand Cmd;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";

          if (option == "adjust-replicas-nodrop") {
            info += "&mgm.file.option=nodrop";
          }

          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(out, err);

          if (!out.endswith("\n")) {
            out += "\n";
          }

          if (!err.endswith("\n")) {
            err += "\n";
          }

          Cmd.close();
        } catch (eos::MDException& e) {
        }
      }
    }

    return true;
  }

  if (option == "drop-missing-replicas") {
    out += "# drop missing replicas ------------------------------------------"
           "-------------------------\n";
    // Unlink all orphaned files - drop replicas which are in the namespace but
    // have no 'image' on disk
    std::map < eos::common::FileSystem::fsid_t,
        std::set < eos::common::FileId::fileid_t >> fid2check;
    eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
    XrdOucErrInfo error;

    // Loop over all filesystems
    for (auto efsmapit = eFsMap["rep_missing_n"].cbegin();
         efsmapit != eFsMap["rep_missing_n"].cend(); ++efsmapit) {
      // Loop over all fids
      for (auto it = efsmapit->second.cbegin();
           it != efsmapit->second.cend(); ++it) {
        std::shared_ptr<eos::IFileMD> fmd;
        bool haslocation = false;
        std::string path = "";

        // Crosscheck if the location really is not attached
        try {
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd.get());

          if (fmd->hasLocation(efsmapit->first)) {
            haslocation = true;
          }
        } catch (eos::MDException& e) {}

        if (!haslocation) {
          if (gOFS->DeleteExternal(efsmapit->first, *it)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send unlink to fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to send unlink to fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += errline;
          }
        } else {
          // Drop from the namespace
          if (gOFS->_dropstripe(path.c_str(), *it, error, vid,
                                efsmapit->first, false)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "error: unable to drop stripe on fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send dropped stripe on fsid=%u fid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          }

          // Execute a proc command
          ProcCommand Cmd;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";
          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(out, err);

          if (!out.endswith("\n")) {
            out += "\n";
          }

          if (!err.endswith("\n")) {
            err += "\n";
          }

          Cmd.close();
        }
      }
    }

    return true;
  }

  if (option == "unlink-zero-replicas") {
    out += "# unlink zero replicas -------------------------------------------"
           "-------------------------\n";
    // Drop all namespace entries which are older than 48 hours and have no
    // files attached. Loop over all fids ...
    auto const& set_fids = eMap["zero_replica"];

    for (auto it = set_fids.cbegin(); it != set_fids.cend(); ++it) {
      std::shared_ptr<eos::IFileMD> fmd;
      std::string path = "";
      time_t now = time(NULL);
      out += "progress: checking fid=";
      out += (int) * it;
      out += "\n";
      eos::IFileMD::ctime_t ctime;
      ctime.tv_sec = 0;
      ctime.tv_nsec = 0;

      try {
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        fmd = gOFS->eosFileService->getFileMD(*it);
        path = gOFS->eosView->getUri(fmd.get());
        fmd->getCTime(ctime);
      } catch (eos::MDException& e) {}

      if (fmd) {
        if ((ctime.tv_sec + (24 * 3600)) < now) {
          // If the file is older than 48 hours, we do the cleanup
          eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
          XrdOucErrInfo error;

          if (!gOFS->_rem(path.c_str(), error, vid)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: removed path=%s fid=%08llx\n",
                     path.c_str(), *it);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to remove path=%s fid=%08llx\n",
                     path.c_str(), *it);
            out += errline;
          }
        } else {
          out += "skipping fid=";
          out += (int) * it;
          out += " - file is younger than 48 hours\n";
        }
      }
    }

    return true;
  }

  if (option == "replace-damaged-replicas") {
    out += "# repairing replace-damaged-replicas -------------------------------------------"
           "-------------------------\n";

    // Loop over all filesystems
    for (const auto& efsmapit : eFsMap["d_mem_sz_diff"]) {
      // Loop over all fids
      for (const auto& fid : efsmapit.second) {
        std::string path;
        std::shared_ptr<eos::IFileMD> fmd;
        {
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

          try {
            fmd = gOFS->eosFileService->getFileMD(fid);
            path = gOFS->eosView->getUri(fmd.get());
          } catch (eos::MDException& e) {}
        }

        if (fmd == nullptr) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fid=%08llx, could not get meta data\n",
                   efsmapit.first, fid);
          out += errline;
          break;
        }

        bool replicaAvailable = false;
        {
          eos::common::RWMutexReadLock fsViewLock(FsView::gFsView.ViewMutex);

          for (const auto& fsid : fmd->getLocations()) {
            if (efsmapit.first != fsid) {
              FileSystem* fileSystem = nullptr;

              if (FsView::gFsView.mIdView.count(fsid) != 0) {
                fileSystem = FsView::gFsView.mIdView[fsid];
                const auto& inconsistentsOnFs = eFsMap["d_mem_sz_diff"][fsid];
                auto found = inconsistentsOnFs.find(fid);

                if (fileSystem != nullptr &&
                    fileSystem->GetConfigStatus(false) > FileSystem::kRO &&
                    found == inconsistentsOnFs.end()) {
                  replicaAvailable = true;
                  break;
                }
              }
            }
          }
        }

        if (!replicaAvailable) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fid=%08llx, no available file systems and replicas to use\n",
                   efsmapit.first, fid);
          out += errline;
          break;
        }

        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;

        if (gOFS->_dropstripe(path.c_str(), fid, error, vid, efsmapit.first, true)) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fid=%08llx, could not drop it\n",
                   efsmapit.first, fid);
          out += errline;
        } else {
          ProcCommand Cmd;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";
          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(out, err);

          if (!out.endswith("\n")) {
            out += "\n";
          }

          if (!err.endswith("\n")) {
            err += "\n";
          }

          Cmd.close();
        }
      }
    }

    return true;
  }

  err = "error: unavailable option";
  return false;
}

//------------------------------------------------------------------------------
// Clear the current FSCK log
//------------------------------------------------------------------------------
void
Fsck::ClearLog()
{
  XrdSysMutexHelper lock(mLogMutex);
  mLog = "";
}

//------------------------------------------------------------------------------
// Write log message to the current in-memory log
//------------------------------------------------------------------------------
void
Fsck::Log(const char* msg, ...) const
{
  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm* tm;
  va_list args;
  va_start(args, msg);
  char buffer[16384];
  char* ptr;
  time(&current_time);
  gettimeofday(&tv, &tz);
  tm = localtime(&current_time);
  sprintf(buffer, "%02d%02d%02d %02d:%02d:%02d %lu.%06lu ", tm->tm_year - 100,
          tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time,
          (unsigned long) tv.tv_usec);
  ptr = buffer + strlen(buffer);
  vsprintf(ptr, msg, args);
  XrdSysMutexHelper lock(mLogMutex);
  mLog += buffer;
  mLog += "\n";
  va_end(args);
}

//------------------------------------------------------------------------------
// Reset all collected errors in the error map
//------------------------------------------------------------------------------
void
Fsck::ResetErrorMaps()
{
  XrdSysMutexHelper lock(eMutex);
  eFsMap.clear();
  eMap.clear();
  eCount.clear();
  eFsUnavail.clear();
  eFsDark.clear();
  eTimeStamp = time(NULL);
}

//------------------------------------------------------------------------------
// Account for offline replicas due to unavailable file systems
//------------------------------------------------------------------------------
void
Fsck::AccountOfflineReplicas()
{
  // Grab all files which are damaged because filesystems are down
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  for (auto it = FsView::gFsView.mIdView.cbegin();
       it != FsView::gFsView.mIdView.cend(); ++it) {
    // protect against illegal 0 filesystem pointer
    if (!it->second) {
      eos_static_crit("found illegal pointer in filesystem view");
      continue;
    }

    eos::common::FileSystem::fsid_t fsid = it->first;
    eos::common::ActiveStatus fsactive = it->second->GetActiveStatus();
    eos::common::FileSystem::fsstatus_t fsconfig = it->second->GetConfigStatus();
    eos::common::BootStatus fsstatus = it->second->GetStatus();

    if ((fsstatus == eos::common::BootStatus::kBooted) &&
        (fsconfig >= eos::common::FileSystem::kDrain) &&
        (fsactive == eos::common::ActiveStatus::kOnline)) {
      // Healthy, don't need to do anything
      continue;
    } else {
      // Not ok and contributes to replica offline errors
      try {
        eos::Prefetcher::prefetchFilesystemFileListAndWait(gOFS->eosView,
            gOFS->eosFsView, fsid);
        XrdSysMutexHelper lock(eMutex);
        // Only need the view lock if we're in-memory
        eos::common::RWMutexReadLock nslock;

        if (gOFS->eosView->inMemory()) {
          nslock.Grab(gOFS->eosViewRWMutex);
        }

        std::deque<std::pair<FileIdentifier, folly::Future<bool>>> futs;

        for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
             (it_fid && it_fid->valid()); it_fid->next()) {
          eos::FileIdentifier fid(it_fid->getElement());
          futs.emplace_back(fid, gOFS->eosFileService->hasFileMD(fid));
        }

        for (size_t i = 0; i < futs.size(); i++) {
          if (futs[i].second.get() == true) {
            eFsUnavail[fsid]++;
            eFsMap["rep_offline"][fsid].insert(futs[i].first.getUnderlyingUInt64());
            eMap["rep_offline"].insert(futs[i].first.getUnderlyingUInt64());
            eCount["rep_offline"]++;
          }
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n",
                         e.getErrno(),
                         e.getMessage().str().c_str());
      }
    }
  }
}

//------------------------------------------------------------------------------
// Account for file with no replicas
//------------------------------------------------------------------------------
void
Fsck::AccountNoReplicaFiles()
{
  // Grab all files which have no replicas at all
  try {
    XrdSysMutexHelper lock(eMutex);
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
    // it_fid not invalidated when items are added or removed for QDB
    // namespace, safe to release lock after each item.
    bool needLockThroughout = ! gOFS->NsInQDB;

    for (auto it_fid = gOFS->eosFsView->getStreamingNoReplicasFileList();
         (it_fid && it_fid->valid()); it_fid->next()) {
      if (!needLockThroughout) {
        ns_rd_lock.Release();
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView,
            it_fid->getElement());
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }

      auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
      std::string path = gOFS->eosView->getUri(fmd.get());
      XrdOucString fullpath = path.c_str();

      if (fullpath.beginswith(gOFS->MgmProcPath)) {
        // Don't report eos /proc files
        continue;
      }

      if (fmd && (!fmd->isLink())) {
        eMap["zero_replica"].insert(it_fid->getElement());
        eCount["zero_replica"]++;
      }

      if (!needLockThroughout) {
        ns_rd_lock.Release();
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_static_debug("msg=\"caught exception\" errno=d%d msg=\"%s\"",
                     e.getErrno(), e.getMessage().str().c_str());
  }
}

//------------------------------------------------------------------------------
// Print offline replicas summary
//------------------------------------------------------------------------------
void
Fsck::PrintOfflineReplicas() const
{
  XrdSysMutexHelper lock(eMutex);

  // Loop over unavailable filesystems
  for (auto ua_it = eFsUnavail.cbegin(); ua_it != eFsUnavail.cend();
       ++ua_it) {
    std::string host = "not configured";
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    auto it_fs = FsView::gFsView.mIdView.find(ua_it->first);

    if (it_fs != FsView::gFsView.mIdView.end()) {
      host = it_fs->second->GetString("hostport");
    }

    Log("host=%s fsid=%lu replica_offline=%llu", host.c_str(),
        ua_it->first, ua_it->second);
  }
}

//------------------------------------------------------------------------------
// Account for offline files or files that require replica adjustments
// i.e. file_offline and adjust_replica
//------------------------------------------------------------------------------
void
Fsck::AccountOfflineFiles()
{
  using eos::common::LayoutId;
  // Loop over all replica_offline and layout error files to assemble a
  // file offline list
  std::set <eos::common::FileId::fileid_t> fid2check;
  {
    XrdSysMutexHelper lock(eMutex);
    fid2check.insert(eMap["rep_offline"].begin(), eMap["rep_offline"].end());
    fid2check.insert(eMap["rep_diff_n"].begin(), eMap["rep_diff_n"].end());
  }

  for (auto it = fid2check.begin(); it != fid2check.end(); ++it) {
    std::shared_ptr<eos::IFileMD> fmd;
    eos::IFileMD::LocationVector loc_vect;
    eos::IFileMD::layoutId_t lid {0ul};
    size_t nlocations {0};

    try { // Check if locations are online
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, *it);
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      fmd = gOFS->eosFileService->getFileMD(*it);
      lid = fmd->getLayoutId();
      nlocations = fmd->getNumLocation();
      loc_vect = fmd->getLocations();
    } catch (eos::MDException& e) {
      continue;
    }

    size_t offlinelocations = 0;
    XrdSysMutexHelper lock(eMutex);
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    for (const auto& loc : loc_vect) {
      if (loc) {
        auto it_fs = FsView::gFsView.mIdView.find(loc);

        if (it_fs != FsView::gFsView.mIdView.end()) {
          auto fs = it_fs->second;
          eos::common::BootStatus bootstatus = fs->GetStatus(true);
          eos::common::FileSystem::fsstatus_t configstatus = fs->GetConfigStatus();
          bool conda = (fs->GetActiveStatus(true) == eos::common::ActiveStatus::kOffline);
          bool condb = (bootstatus != eos::common::BootStatus::kBooted);
          bool condc = (configstatus == eos::common::FileSystem::kDrainDead);

          if (conda || condb || condc) {
            ++offlinelocations;
          }
        }
      }
    }

    unsigned long layout_type = LayoutId::GetLayoutType(lid);

    if (layout_type == LayoutId::kReplica) {
      if (offlinelocations == nlocations) {
        eMap["file_offline"].insert(*it);
        eCount["file_offline"]++;
      }
    } else if (layout_type >= LayoutId::kArchive) {
      // Proper condition for RAIN layout
      if (offlinelocations > LayoutId::GetRedundancyStripeNumber(lid)) {
        eMap["file_offline"].insert(*it);
        eCount["file_offline"]++;
      }
    }

    if (offlinelocations && (offlinelocations != nlocations)) {
      eMap["adjust_replica"].insert(*it);
      eCount["adjust_replica"]++;
    }
  }
}

//------------------------------------------------------------------------------
// Print summary of the different type of errors collected so far and their
// corresponding counters
//------------------------------------------------------------------------------
void
Fsck::PrintErrorsSummary() const
{
  XrdSysMutexHelper lock(eMutex);

  for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
    Log("%-30s : %llu (%llu)", emapit->first.c_str(),
        emapit->second.size(), eCount.at(emapit->first));
  }
}

//------------------------------------------------------------------------------
// Account for "dark" file entries i.e. file system ids which have file
// entries in the namespace view but have no configured file system in the
// FsView.
//------------------------------------------------------------------------------
void
Fsck::AccountDarkFiles()
{
  XrdSysMutexHelper lock(eMutex);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

  for (auto it = gOFS->eosFsView->getFileSystemIterator();
       it->valid(); it->next()) {
    IFileMD::location_t nfsid = it->getElement();

    try {
      // @todo(gbitzes): Urgent fix for QDB namespace needed.. This loop
      // will need to load all filesystems in memory, just to get a couple
      // of silly counters.
      uint64_t num_files = gOFS->eosFsView->getNumFilesOnFs(nfsid);

      if (num_files) {
        // Check if this exists in the gFsView
        if (!FsView::gFsView.mIdView.count(nfsid)) {
          eFsDark[nfsid] += num_files;
          Log("shadow fsid=%lu shadow_entries=%llu ", nfsid, num_files);
        }
      }
    } catch (const eos::MDException& e) {
      // ignore
    }
  }
}

EOSMGMNAMESPACE_END
