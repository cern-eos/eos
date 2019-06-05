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

#include "mgm/fsck/Fsck.hh"
#include "mgm/fsck/FsckEntry.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Mapping.hh"
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
  mShowDarkFiles(false), mEnabled(false),
  mInterval(30), mRunning(false), eTimeStamp(0)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Fsck::~Fsck()
{
  (void) Stop(false);
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
// Apply the FSCK configuration stored in the configuration engine
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
  bool ok = true;
  std::string interval_min = std::to_string(mInterval);
  ok &= FsView::gFsView.SetGlobalConfig(gFsckEnabled, mEnabled.c_str());
  ok &= FsView::gFsView.SetGlobalConfig(gFsckInterval, interval_min.c_str());
  return ok;
}

//------------------------------------------------------------------------------
// Apply configuration options to the fsck mechanism
//------------------------------------------------------------------------------
bool
Fsck::Config(const std::string& key, const std::string& value)
{
  if (key == "show-dark-files") {
    mShowDarkFiles = (value == "yes");
  } else {
    return false;
  }

  return true;
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

    AccountOfflineReplicas();
    PrintOfflineReplicas();
    AccountNoReplicaFiles();
    AccountOfflineFiles();
    PrintErrorsSummary();

    // @note the following operation is heavy for the qdb ns
    if (mShowDarkFiles) {
      AccountDarkFiles();
    }

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
Fsck::PrintOut(std::string& out) const
{
  XrdSysMutexHelper lock(mLogMutex);
  out = mLog.c_str();
}

//------------------------------------------------------------------------------
// Return the current FSCK report
//------------------------------------------------------------------------------
bool
Fsck::Report(std::string& out, const std::set<std::string> tags,
             bool display_per_fs,  bool display_fid, bool display_lfn,
             bool display_json, bool display_help)
{
  // @todo(esindril) add display_help info
  XrdSysMutexHelper lock(eMutex);
  char stimestamp[1024];
  snprintf(stimestamp, sizeof(stimestamp) - 1, "%lu", (unsigned long) eTimeStamp);

  if (display_json) {
    // json output format
    out += "{\n";
    // put the check timestamp
    out += "  \"timestamp\": ";
    out += stimestamp;
    out += ",\n";

    if (!display_per_fs) {
      // Dump global table
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (!tags.empty() && (tags.find(emapit->first) == tags.end())) {
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

        if (display_fid) {
          out += "    \"fxid\": [";

          for (auto fidit = emapit->second.cbegin();
               fidit != emapit->second.cend(); ++fidit) {
            out += eos::common::FileId::Fid2Hex(*fidit).c_str();
            out += ",";
          }

          if (*out.rbegin() == ',') {
            out.erase(out.length() - 1);
          }

          out += "]\n";
        }

        if (display_lfn) {
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

          if (*out.rbegin() == ',') {
            out.erase(out.length() - 1);
          }

          out += "]\n";
        }

        if (out.find(",\n") == (out.length() - 2)) {
          out.erase(out.length() - 2);
          out += "\n";
        }

        out += "  },\n";
      }
    } else {
      // Do output per filesystem
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (!tags.empty() && (tags.find(emapit->first) == tags.end())) {
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

          if (display_fid) {
            out += "        \"fxid\": [";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;

            for (fidit = efsmapit->second.begin();
                 fidit != efsmapit->second.end(); fidit++) {
              out += eos::common::FileId::Fid2Hex(*fidit).c_str();
              out += ",";
            }

            if (*out.rbegin() == ',') {
              out.erase(out.length() - 1);
            }

            out += "]\n";
          }

          if (display_lfn) {
            out += "        \"lfn\": [";
            std::set <eos::common::FileId::fileid_t>::const_iterator fidit;

            for (fidit = efsmapit->second.begin();
                 fidit != efsmapit->second.end();
                 fidit++) {
              eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
              eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

              try {
                auto fmd = gOFS->eosFileService->getFileMD(*fidit);
                std::string fullpath = gOFS->eosView->getUri(fmd.get());
                out += "\"";
                out += fullpath.c_str();
                out += "\"";
              } catch (eos::MDException& e) {
                out += "\"undefined\"";
              }

              out += ",";
            }

            if (*out.rbegin() == ',') {
              out.erase(out.length() - 1);
            }

            out += "]\n";
          }

          if (out.find(",\n") == (out.length() - 2)) {
            out.erase(out.length() - 2);
            out += "\n";
          }

          out += "      },\n";
        }

        out += "    },\n";
      }
    }

    // List shadow filesystems
    if (!eFsDark.empty()) {
      out += "  \"shadow_fsid\": [";

      for (auto fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++) {
        char sfsid[1024];
        snprintf(sfsid, sizeof(sfsid) - 1, "%lu", (unsigned long) fsit->first);
        out += sfsid;
        out += ",";
      }

      if (*out.rbegin() == ',') {
        out.erase(out.length() - 1);
      }

      out += "  ]\n";
      out += "}\n";
    }
  } else {
    // greppable format
    if (!display_per_fs) {
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (!tags.empty() && (tags.find(emapit->first) == tags.end())) {
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
          out += " fxid=";

          for (auto fidit = emapit->second.cbegin();
               fidit != emapit->second.cend(); ++fidit) {
            out += eos::common::FileId::Fid2Hex(*fidit).c_str();
            out += ",";
          }

          if (*out.rbegin() == ',') {
            out.erase(out.length() - 1);
          }

          out += "\n";
        }

        if (display_lfn) {
          out += " lfn=";

          for (auto fidit = emapit->second.cbegin();
               fidit != emapit->second.cend(); fidit++) {
            eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

            try {
              auto fmd = gOFS->eosFileService->getFileMD(*fidit);
              std::string fullpath = gOFS->eosView->getUri(fmd.get());
              out += "\"";
              out += fullpath.c_str();
              out += "\"";
            } catch (eos::MDException& e) {
              out += "\"undefined\"";
            }

            out += ",";
          }

          if (*out.rbegin() == ',') {
            out.erase(out.length() - 1);
          }

          out += "\n";
        }

        // List shadow filesystems
        if (!eFsDark.empty()) {
          out += " shadow_fsid=";

          for (auto fsit = eFsDark.cbegin(); fsit != eFsDark.cend(); ++fsit) {
            char sfsid[1024];
            snprintf(sfsid, sizeof(sfsid) - 1,
                     "%lu",
                     (unsigned long) fsit->first);
            out += sfsid;
            out += ",";
          }

          if (*out.rbegin() == ',') {
            out.erase(out.length() - 1);
          }

          out += "\n";
        }
      }
    } else {
      // Do output per filesystem
      for (auto emapit = eMap.cbegin(); emapit != eMap.cend(); ++emapit) {
        if (!tags.empty() && (tags.find(emapit->first) == tags.end())) {
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
            out += " fxid=";

            for (auto fidit = efsmapit->second.cbegin();
                 fidit != efsmapit->second.cend(); ++fidit) {
              out += eos::common::FileId::Fid2Hex(*fidit).c_str();
              out += ",";
            }

            if (*out.rbegin() == ',') {
              out.erase(out.length() - 1);
            }

            out += "\n";
          } else {
            if (display_lfn) {
              out += " lfn=";

              for (auto fidit = efsmapit->second.cbegin();
                   fidit != efsmapit->second.cend(); ++fidit) {
                eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *fidit);
                eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

                try {
                  auto fmd = gOFS->eosFileService->getFileMD(*fidit);
                  std::string fullpath = gOFS->eosView->getUri(fmd.get());
                  out += "\"";
                  out += fullpath.c_str();
                  out += "\"";
                } catch (eos::MDException& e) {
                  out += "\"undefined\"";
                }

                out += ",";
              }

              if (*out.rbegin() == ',') {
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
// Repair checksum errors
//------------------------------------------------------------------------------
void
Fsck::RepairChecksumErrs(std::string& out)
{
  std::ostringstream oss;
  XrdSysMutexHelper lock(eMutex);
  oss << "# repair checksum ------------------------------------------------"
      << "-------------------------" << std::endl;
  std::map<eos::common::FileSystem::fsid_t,
      std::set<eos::common::FileId::fileid_t>> fid2check;

  // Loop over all filesystems with MGM checksum mismatch
  for (auto efsmapit = eFsMap["m_cx_diff"].cbegin();
       efsmapit != eFsMap["m_cx_diff"].cend(); ++efsmapit) {
    for (const auto& fid : efsmapit->second) {
      fid2check[efsmapit->first].insert(fid);
    }
  }

  // Loop over all filesystems with disk checksum mismatch
  for (auto efsmapit = eFsMap["d_cx_diff"].cbegin();
       efsmapit != eFsMap["d_cx_diff"].cend(); ++efsmapit) {
    for (const auto& fid : efsmapit->second) {
      fid2check[efsmapit->first].insert(fid);
    }
  }

  // Loop over all filesystems
  for (auto it = fid2check.cbegin();
       it != fid2check.cend(); ++it) {
    for (const auto& fid : it->second) {
      std::string path = "";
      eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
      eos::common::RWMutexReadLock ns_rdlock(gOFS->eosViewRWMutex);

      try {
        auto fmd = gOFS->eosFileService->getFileMD(fid);
        path = gOFS->eosView->getUri(fmd.get());
      } catch (const eos::MDException& e) {
        continue;
      }

      // Issue verify operations on that particular file
      eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
      XrdOucErrInfo error;
      int lretc = 1;

      if (path.length()) {
        //if (options.find("checksum-commit") != options.end()) {
        // Verify & commit
        lretc = gOFS->_verifystripe(path.c_str(), error, vid, it->first,
                                    "&mgm.verify.compute.checksum=1&"
                                    "mgm.verify.commit.checksum=1&"
                                    "mgm.verify.commit.size=1");
        // } else {
        //   // Verify only
        //   lretc = gOFS->_verifystripe(path.c_str(), error, vid, efsmapit->first,
        //                               "&mgm.verify.compute.checksum=1");
        // }

        if (!lretc) {
          out += "success: sending verify to fsid=";
          out += (int) it->first;
          out += " for path=";
          out += path.c_str();
          out += "\n";
        } else {
          out += "error: sending verify to fsid=";
          out += (int) it->first;
          out += " failed for path=";
          out += path.c_str();
          out += "\n";
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Method to issue a repair action
//------------------------------------------------------------------------------
bool
Fsck::Repair(std::string& out, const std::set<string>& options)
{
  std::set<std::string> allowed_options {
    "checksum", "checksum-commit", "resync", "unlink-unregistered",
    "unlink-orpahsn", "adjust-replicas", "adjust-replicas-nodrop",
    "drop-missing-replicas", "unlink-zero-replicas", "replace-damaged-replicas"};

  // Check for a valid action in options
  for (const auto& elem : options) {
    if (allowed_options.find(elem) == allowed_options.end()) {
      out = SSTR("error: illegal option <" << elem << ">").c_str();
      return false;
    }
  }

  XrdSysMutexHelper lock(eMutex);

  if (options.find("checksum") != options.end()) {
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
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock ns_rdlock(gOFS->eosViewRWMutex);

        try {
          auto fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd.get());
        } catch (const eos::MDException& e) {
          continue;
        }

        // Issue verify operations on that particular file
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        int lretc = 1;

        if (path.length()) {
          if (options.find("checksum-commit") != options.end()) {
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

  if (options.find("resync") != options.end()) {
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
        std::shared_ptr<eos::IFileMD> fmd;
        eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

        try {
          fmd = gOFS->eosFileService->getFileMD(*it);
        } catch (const eos::MDException& e) {
          out += SSTR("error: no metadata for fsid=" << efsmapit->first
                      << " fid=" << std::setw(8) << std::setfill('0')
                      << std::hex << *it << std::endl).c_str();
          continue;
        }

        if (fmd) {
          // Issue a resync command for a filesystem/fid pair
          if (gOFS->SendResync(*it, efsmapit->first) == 0) {
            out += SSTR("success: sending resync to fsid=" << efsmapit->first
                        << " fxid=" << std::setw(8) << std::setfill('0')
                        << std::hex << *it << std::endl).c_str();
          } else {
            out += SSTR("error: failed resync to fsid=" << efsmapit->first
                        << " fxid=" << std::setw(8) << std::setfill('0')
                        << std::hex << *it << std::endl).c_str();
          }
        }
      }
    }

    return true;
  }

  if (options.find("unlink-unregistered") != options.end()) {
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
        bool fmd_exists = false;
        bool haslocation = false;
        std::string spath = "";

        // Crosscheck if the location really is not attached
        try {
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(*it);
          spath = gOFS->eosView->getUri(fmd.get());
          fmd_exists = true;

          if (fmd->hasLocation(efsmapit->first)) {
            haslocation = true;
          }
        } catch (const eos::MDException& e) {
          // ignore
        }

        if (!haslocation) {
          // Send external deletion
          if (gOFS->DeleteExternal(efsmapit->first, *it)) {
            out += SSTR("success: sent unlink to fsid=" << efsmapit->first
                        << " fxid=" << std::setw(8) << std::setfill('0')
                        << std::hex << *it << std::endl).c_str();
          } else {
            out += SSTR("error: failed to send unlink to fsid=" << efsmapit->first
                        << " fxid=" << std::setw(8) << std::setfill('0')
                        << std::hex << *it << std::endl).c_str();
          }

          if (fmd_exists) {
            // Drop from the namespace
            if (gOFS->_dropstripe(spath.c_str(), *it, error, vid, efsmapit->first, false)) {
              out += SSTR("error: unable to drop stripe on fsid=" << efsmapit->first
                          << " fxid=" << std::setw(8) << std::setfill('0')
                          << std::hex << *it << std::endl).c_str();
            } else {
              out += SSTR("success: sent drop stripe on fsid=" << efsmapit->first
                          << " fxid=" << std::setw(8) << std::setfill('0')
                          << std::hex << *it << std::endl).c_str();
            }
          }
        }
      }
    }

    return true;
  }

  if (options.find("unlink-orphans") != options.end()) {
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
        bool has_location = false;
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

        // Crosscheck if the location really is not attached
        try {
          auto fmd = gOFS->eosFileService->getFileMD(*it);

          if (fmd->hasLocation(efsmapit->first)) {
            has_location = true;
          }
        } catch (const eos::MDException& e) {}

        if (!has_location) {
          if (gOFS->DeleteExternal(efsmapit->first, *it)) {
            out += SSTR("success: sent unlink to fsid=" << efsmapit->first
                        << " fxid=" << std::setw(8) << std::setfill('0')
                        << std::hex << *it << std::endl).c_str();
          } else {
            out += SSTR("error: failed to send unlink to fsid=" << efsmapit->first
                        << " fxid=" << std::setw(8) << std::setfill('0')
                        << std::hex << *it << std::endl).c_str();
          }
        } else {
          out += SSTR("error: location exists, not sending unlin to fsid="
                      << efsmapit->first << " fxid=" << std::setw(8)
                      << std::setfill('0') << std::hex << *it << std::endl).c_str();
        }
      }
    }

    return true;
  }

  if ((options.find("adjust-replicas") != options.end()) ||
      (options.find("adjust-replicas-nodrop") != options.end())) {
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
          XrdOucString cmd_out, cmd_err;
          XrdOucErrInfo error;
          ProcCommand Cmd;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";

          if (options.find("adjust-replicas-nodrop") != options.end()) {
            info += "&mgm.file.option=nodrop";
          }

          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(cmd_out, cmd_err);

          if (cmd_out.length()) {
            if (!cmd_out.endswith("\n")) {
              cmd_out += "\n";
            }

            out += cmd_out.c_str();
          }

          if (cmd_err.length()) {
            if (!cmd_err.endswith("\n")) {
              cmd_err += "\n";
            }

            out += cmd_err.c_str();
          }

          Cmd.close();
        } catch (const eos::MDException& e) {
          // ignore missing file entries
        }
      }
    }

    return true;
  }

  if (options.find("drop-missing-replicas") != options.end()) {
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
        bool has_location = false;
        std::string path = "";

        // Crosscheck if the location really is not attached
        try {
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, *it);
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          fmd = gOFS->eosFileService->getFileMD(*it);
          path = gOFS->eosView->getUri(fmd.get());

          if (fmd->hasLocation(efsmapit->first)) {
            has_location = true;
          }
        } catch (eos::MDException& e) {}

        if (!has_location) {
          if (gOFS->DeleteExternal(efsmapit->first, *it)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send unlink to fsid=%u fxid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to send unlink to fsid=%u fxid=%08llx\n",
                     efsmapit->first, *it);
            out += errline;
          }
        } else {
          // Drop from the namespace
          if (gOFS->_dropstripe(path.c_str(), *it, error, vid,
                                efsmapit->first, false)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "error: unable to drop stripe on fsid=%u fxid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          } else {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send dropped stripe on fsid=%u fxid=%08llx\n",
                     efsmapit->first, *it);
            out += outline;
          }

          // Execute a proc command
          ProcCommand Cmd;
          XrdOucString cmd_out, cmd_err;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";
          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(cmd_out, cmd_err);

          if (cmd_out.length()) {
            if (!cmd_out.endswith("\n")) {
              cmd_out += "\n";
            }

            out += cmd_out.c_str();
          }

          if (cmd_err.length()) {
            if (!cmd_err.endswith("\n")) {
              cmd_err += "\n";
            }

            out += cmd_err.c_str();
          }

          Cmd.close();
        }
      }
    }

    return true;
  }

  if (options.find("unlink-zero-replicas") != options.end()) {
    out += "# unlink zero replicas -------------------------------------------"
           "-------------------------\n";
    // Drop all namespace entries which are older than 48 hours and have no
    // files attached. Loop over all fids ...
    auto const& set_fids = eMap["zero_replica"];

    for (auto it = set_fids.cbegin(); it != set_fids.cend(); ++it) {
      std::shared_ptr<eos::IFileMD> fmd;
      std::string path = "";
      time_t now = time(NULL);
      out += "progress: checking fxid=";
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
                     "success: removed path=%s fxid=%08llx\n",
                     path.c_str(), *it);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to remove path=%s fxid=%08llx\n",
                     path.c_str(), *it);
            out += errline;
          }
        } else {
          out += "skipping fxid=";
          out += (int) * it;
          out += " - file is younger than 48 hours\n";
        }
      }
    }

    return true;
  }

  if (options.find("replace-damaged-replicas") != options.end()) {
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
                   "error: unable to repair file fsid=%u fxid=%08llx, could not get meta data\n",
                   efsmapit.first, fid);
          out += errline;
          break;
        }

        bool replicaAvailable = false;
        {
          eos::common::RWMutexReadLock fsViewLock(FsView::gFsView.ViewMutex);

          for (const auto& fsid : fmd->getLocations()) {
            if (efsmapit.first != fsid) {
              FileSystem* fileSystem = FsView::gFsView.mIdView.lookupByID(fsid);
              const auto& inconsistentsOnFs = eFsMap["d_mem_sz_diff"][fsid];
              auto found = inconsistentsOnFs.find(fid);

              if (fileSystem != nullptr &&
                  fileSystem->GetConfigStatus(false) > common::ConfigStatus::kRO &&
                  found == inconsistentsOnFs.end()) {
                replicaAvailable = true;
                break;
              }
            }
          }
        }

        if (!replicaAvailable) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fxid=%08llx, no available file systems and replicas to use\n",
                   efsmapit.first, fid);
          out += errline;
          break;
        }

        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;

        if (gOFS->_dropstripe(path.c_str(), fid, error, vid, efsmapit.first, true)) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fxid=%08llx, could not drop it\n",
                   efsmapit.first, fid);
          out += errline;
        } else {
          ProcCommand Cmd;
          XrdOucString cmd_out, cmd_err;
          XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
          info += path.c_str();
          info += "&mgm.format=fuse";
          Cmd.open("/proc/user", info.c_str(), vid, &error);
          Cmd.AddOutput(cmd_out, cmd_err);

          if (cmd_out.length()) {
            if (!cmd_out.endswith("\n")) {
              cmd_out += "\n";
            }

            out += cmd_out.c_str();
          }

          if (cmd_err.length()) {
            if (!cmd_err.endswith("\n")) {
              cmd_err += "\n";
            }

            out += cmd_err.c_str();
          }

          Cmd.close();
        }
      }
    }

    return true;
  }

  out = "error: unavailable option";
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
    eos::common::ConfigStatus fsconfig = it->second->GetConfigStatus();
    eos::common::BootStatus fsstatus = it->second->GetStatus();

    if ((fsstatus == eos::common::BootStatus::kBooted) &&
        (fsconfig >= eos::common::ConfigStatus::kDrain) &&
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

        for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
             (it_fid && it_fid->valid()); it_fid->next()) {
          eFsUnavail[fsid]++;
          eFsMap["rep_offline"][fsid].insert(it_fid->getElement());
          eMap["rep_offline"].insert(it_fid->getElement());
          eCount["rep_offline"]++;
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
    bool needLockThroughout = !gOFS->NsInQDB;

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
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(ua_it->first);

    if (fs) {
      host = fs->GetString("hostport");
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
        FileSystem* fs = FsView::gFsView.mIdView.lookupByID(loc);

        if (fs) {
          eos::common::BootStatus bootstatus = fs->GetStatus(true);
          eos::common::ConfigStatus configstatus = fs->GetConfigStatus();
          bool conda = (fs->GetActiveStatus(true) == eos::common::ActiveStatus::kOffline);
          bool condb = (bootstatus != eos::common::BootStatus::kBooted);
          bool condc = (configstatus == eos::common::ConfigStatus::kDrainDead);

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
    uint64_t count {0ull};
    auto it = eCount.find(emapit->first);

    if (it != eCount.end()) {
      count = it->second;
    }

    Log("%-30s : %llu (%llu)", emapit->first.c_str(), emapit->second.size(),
        count);
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
        if (!FsView::gFsView.mIdView.exists(nfsid)) {
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
