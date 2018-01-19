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
#include "common/Mapping.hh"
#include "mgm/Fsck.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/Messaging.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method to issue a repair action
//------------------------------------------------------------------------------
bool
Fsck::Repair(XrdOucString& out, XrdOucString& err, XrdOucString option)
{
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

  auto inconsistencies = cache.getCachedObject(false, RetrieveInconsistencies);

  if (option.beginswith("checksum")) {
    out += "# repair checksum ------------------------------------------------"
           "-------------------------\n";
    std::map < eos::common::FileSystem::fsid_t,
      std::map<eos::common::FileId::fileid_t, std::list<std::string>>> fid2check;

    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["m_cx_diff"]) {
      // Loop over all fids
      for (auto fid : efsmapit.second.first) {
        fid2check[efsmapit.first][fid].emplace_back("m_cx_diff");
      }
    }

    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["d_cx_diff"]) {

      // Loop over all fids
      for (auto fid : efsmapit.second.first) {
        fid2check[efsmapit.first][fid].emplace_back("d_cx_diff");
      }
    }

    // Loop over all filesystems
    for (const auto& efsmapit : fid2check) {
      for (const auto& fidPair : efsmapit.second) {
        std::string path = "";

        try {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(fidPair.first);
          path = gOFS->eosView->getUri(fmd.get());
        } catch (eos::MDException& e) {}

        // Issue verify operations on that particular filesystem
        eos::common::Mapping::VirtualIdentity vid;
        eos::common::Mapping::Root(vid);
        XrdOucErrInfo error;
        int lretc = 1;

        if (path.length()) {
          if (option == "checksum-commit") {
            // Verify & commit
            lretc = gOFS->_verifystripe(path.c_str(), error, vid, efsmapit.first,
                                        "&mgm.verify.compute.checksum=1&"
                                        "mgm.verify.commit.checksum=1&"
                                        "mgm.verify.commit.size=1");
          } else {
            // Verify only
            lretc = gOFS->_verifystripe(path.c_str(), error, vid, efsmapit.first,
                                        "&mgm.verify.compute.checksum=1");
          }

          if (!lretc) {
            // remove the fsck files too
            for(const auto& inconsistency : fidPair.second) {
              RemoveFsckFile(inconsistency, efsmapit.first, fidPair.first);
            }

            out += "success: sending verify to fsid=";
            out += (int) efsmapit.first;
            out += " for path=";
            out += path.c_str();
            out += "\n";
          } else {
            out += "error: sending verify to fsid=";
            out += (int) efsmapit.first;
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
        std::map<eos::common::FileId::fileid_t, std::list<std::string>>> fid2check;

    for (const auto& emapit : inconsistencies) {
      // Don't sync offline replicas
      if (emapit.first == "rep_offline") {
        continue;
      }

      // Loop over all filesystems
      for (const auto& efsmapit : inconsistencies[emapit.first]) {
        // Loop over all fids
        for (auto fid : efsmapit.second.first) {
          fid2check[efsmapit.first][fid].emplace_back(emapit.first);
        }
      }
    }

    // Loop over all filesystems
    for (const auto& efsmapit : fid2check) {
      for (const auto& fidPair : efsmapit.second) {
        auto& fid = fidPair.first;

        try {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(fid);
        } catch (eos::MDException& e) {
          char outline[1024];
          snprintf(outline, sizeof(outline) - 1,
                   "error: no file meta data for fsid=%u failed for fxid=%llx\n",
                   efsmapit.first, fid);
          out += outline;
          continue;
        }

        int lretc = 0;
        // Issue a resync command for a filesystem/fid pair
        lretc = gOFS->SendResync(fid, efsmapit.first);

        if (lretc) {
          // remove inconsistency fsck files
          for (const auto& inconsistency : fidPair.second) {
            RemoveFsckFile(inconsistency, efsmapit.first, fid);
          }

          char outline[1024];
          snprintf(outline, sizeof(outline) - 1,
                   "success: sending resync to fsid=%u fxid=%llx\n",
                   efsmapit.first, fid);
          out += outline;
        } else {
          char outline[1024];
          snprintf(outline, sizeof(outline) - 1,
                   "error: sending resync to fsid=%u failed for fxid=%llx\n",
                   efsmapit.first, fid);
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
    eos::common::Mapping::VirtualIdentity vid;
    eos::common::Mapping::Root(vid);
    XrdOucErrInfo error;

    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["unreg_n"]) {
      // Loop over all fids
      for (auto fid : efsmapit.second.first) {
        bool haslocation = false;
        std::string spath = "";

        // Crosscheck if the location really is not attached
        try {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(fid);
          spath = gOFS->eosView->getUri(fmd.get());

          if (fmd->hasLocation(efsmapit.first)) {
            haslocation = true;
          }
        } catch (eos::MDException& e) {}

        // Send external deletion
        if (gOFS->DeleteExternal(efsmapit.first, fid)) {
          // remove the fsck file too
          RemoveFsckFile("unreg_n", efsmapit.first, fid);

          char outline[1024];
          snprintf(outline, sizeof(outline) - 1,
                   "success: send unlink to fsid=%u fxid=%llx\n",
                   efsmapit.first, fid);
          out += outline;
        } else {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "err: unable to send unlink to fsid=%u fxid=%llx\n",
                   efsmapit.first, fid);
          out += errline;
        }

        if (haslocation) {
          // Drop from the namespace
          if (gOFS->_dropstripe(spath.c_str(), error, vid, efsmapit.first, false)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "error: unable to drop stripe on fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
            out += outline;
          } else {
            for (const auto& inconsistency : gOFS->MgmFsckDirs) {
              RemoveFsckFile(inconsistency, efsmapit.first, fid);
            }

            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send dropped stripe on fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
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
    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["orphans_n"]) {
      // Loop over all fids
      for (auto fid : efsmapit.second.first) {
        bool haslocation = false;

        // Crosscheck if the location really is not attached
        try {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(fid);

          if (fmd != nullptr && fmd->hasLocation(efsmapit.first)) {
            haslocation = true;
          }
        } catch (eos::MDException& e) {}

        if (!haslocation) {
          if (gOFS->DeleteExternal(efsmapit.first, fid)) {
            // remove the fsck files too
            RemoveFsckFile("orphans_n", efsmapit.first, fid);

            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send unlink to fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to send unlink to fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
            out += errline;
          }
        } else {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "err: not sending unlink to fsid=%u fxid=%llx - location exists!\n",
                   efsmapit.first, fid);
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
    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["rep_diff_n"]) {
      // Loop over all fids
      for (auto fid : efsmapit.second.first) {
        std::string path = "";

        try {
          {
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
            auto fmd = gOFS->eosFileService->getFileMD(fid);
            path = gOFS->eosView->getUri(fmd.get());
          }
          // Execute adjust replica
          eos::common::Mapping::VirtualIdentity vid;
          eos::common::Mapping::Root(vid);
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

          // remove the fsck file too
          RemoveFsckFile("rep_diff_n", efsmapit.first, fid);
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
    eos::common::Mapping::VirtualIdentity vid;
    eos::common::Mapping::Root(vid);
    XrdOucErrInfo error;

    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["rep_missing_n"]) {
      // Loop over all fids
      for (auto fid : efsmapit.second.first) {
        bool haslocation = false;
        std::string path = "";

        // Crosscheck if the location really is not attached
        try {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(fid);
          path = gOFS->eosView->getUri(fmd.get());

          if (fmd->hasLocation(efsmapit.first)) {
            haslocation = true;
          }
        } catch (eos::MDException& e) {}

        if (!haslocation) {
          if (gOFS->DeleteExternal(efsmapit.first, fid)) {
            // remove the fsck file too
            RemoveFsckFile("rep_missing_n", efsmapit.first, fid);

            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send unlink to fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to send unlink to fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
            out += errline;
          }
        } else {
          // Drop from the namespace
          if (gOFS->_dropstripe(path.c_str(), error, vid,
                                efsmapit.first, false)) {
            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "error: unable to drop stripe on fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
            out += outline;
          } else {
            // remove the fsck files too
            for (const auto& inconsistency : gOFS->MgmFsckDirs) {
              RemoveFsckFile(inconsistency, efsmapit.first, fid);
            }

            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: send dropped stripe on fsid=%u fxid=%llx\n",
                     efsmapit.first, fid);
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
    for (const auto& fsMap : inconsistencies["zero_replica"]) {
      for (auto fid : fsMap.second.first) {
        std::string path = "";
        time_t now = time(NULL);
        out += "progress: checking fid=";
        out += std::to_string(fid).c_str();
        out += "\n";
        eos::IFileMD::ctime_t ctime;
        ctime.tv_sec = 0;
        ctime.tv_nsec = 0;

        try {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(fid);
          path = gOFS->eosView->getUri(fmd.get());
          fmd->getCTime(ctime);
        } catch (eos::MDException& e) {
          out += "skipping fid=";
          out += std::to_string(fid).c_str();
          out += " - file is younger than 48 hours\n";
          continue;
        }

        if ((ctime.tv_sec + (24 * 3600)) < now) {
          // If the file is older than 48 hours, we do the cleanup
          eos::common::Mapping::VirtualIdentity vid;
          eos::common::Mapping::Root(vid);
          XrdOucErrInfo error;

          if (!gOFS->_rem(path.c_str(), error, vid)) {
            // remove the fsck file too
            RemoveFsckFile("zero_replica", fsMap.first, fid);

            char outline[1024];
            snprintf(outline, sizeof(outline) - 1,
                     "success: removed path=%s fxid=%llx\n",
                     path.c_str(), fid);
            out += outline;
          } else {
            char errline[1024];
            snprintf(errline, sizeof(errline) - 1,
                     "err: unable to remove path=%s fxid=%llx\n",
                     path.c_str(), fid);
            out += errline;
          }
        }
      }
    }

    return true;
  }

  if (option == "replace-damaged-replicas") {
    out += "# repairing replace-damaged-replicas -------------------------------------------"
           "-------------------------\n";

    // Loop over all filesystems
    for (const auto& efsmapit : inconsistencies["d_mem_sz_diff"]) {
      // Loop over all fids
      for (const auto& fid : efsmapit.second.first) {
        std::string path;
        std::shared_ptr<eos::IFileMD> fmd;
        {
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

          try {
            fmd = gOFS->eosFileService->getFileMD(fid);
            path = gOFS->eosView->getUri(fmd.get());
          } catch (eos::MDException& e) {}
        }

        if (fmd == nullptr) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fxid=%llx, could not get meta data\n",
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
                const auto& inconsistentsOnFs = inconsistencies["d_mem_sz_diff"][fsid];
                auto found = inconsistentsOnFs.first.find(fid);

                if (fileSystem != nullptr &&
                    fileSystem->GetConfigStatus(false) > FileSystem::kRO &&
                    found == inconsistentsOnFs.first.end()) {
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
                   "error: unable to repair file fsid=%u fxid=%llx, no available file systems and replicas to use\n",
                   efsmapit.first, fid);
          out += errline;
          break;
        }

        eos::common::Mapping::VirtualIdentity vid;
        eos::common::Mapping::Root(vid);
        XrdOucErrInfo error;

        if (gOFS->_dropstripe(path.c_str(), error, vid, efsmapit.first, true)) {
          char errline[1024];
          snprintf(errline, sizeof(errline) - 1,
                   "error: unable to repair file fsid=%u fxid=%llx, could not drop it\n",
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

Fsck::InconsistencyMap*
Fsck::RetrieveInconsistencies() {
  auto inconPtr =
    new std::map<std::string, std::map<eos::common::FileSystem::fsid_t, std::pair<std::set<eos::common::FileId::fileid_t>, std::time_t>>>{};

  eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);

  for(auto& inconsistency : XrdMgmOfs::MgmFsckDirs) {
    std::map<eos::common::FileSystem::fsid_t, std::pair<std::set<eos::common::FileId::fileid_t>, std::time_t>> inconsistentFilesMap;

    std::string incContPath = std::string(gOFS->MgmProcFsckPath.c_str()) + "/" + inconsistency;
    auto container = gOFS->eosView->getContainer(incContPath);
    for(auto fsidIt = container->subcontainersBegin(); fsidIt != container->subcontainersEnd(); ++fsidIt) {
      auto fsidContainer =  gOFS->eosView->getContainer(incContPath + "/" + fsidIt->first);
      auto fidSet = std::set<eos::common::FileId::fileid_t>{};
      for(auto fidIt = fsidContainer->filesBegin(); fidIt != fsidContainer->filesEnd(); ++fidIt) {
        fidSet.insert(stoull(fidIt->first));
      }
      inconsistentFilesMap[stoull(fsidIt->first)] = std::make_pair(std::move(fidSet), std::time(nullptr));
    }

    (*inconPtr)[inconsistency] = std::move(inconsistentFilesMap);
  }

  return inconPtr;
}

void
Fsck::Stat(XrdOucString& out) {
  auto inconsistencies = cache.getCachedObject(false, RetrieveInconsistencies);
  std::ostringstream outBuff;

  auto fsNumber = inconsistencies.empty() ? 0 : inconsistencies.begin()->second.size();
  outBuff << "Filesystems checked: " << fsNumber << endl;

  for(auto& inconsistency : XrdMgmOfs::MgmFsckDirs) {
    outBuff << inconsistency << ": ";
    auto count = 0ul;
    for(auto& pair : inconsistencies[inconsistency]) {
      count += pair.second.first.size();
    }
    outBuff << count << endl;
  }

  out = outBuff.str().c_str();
}

void
Fsck::Report(XrdOucString& out, XrdOucString option, const XrdOucString& selection) {
  bool printlfn = (option.find("l") != STR_NPOS);
  bool perfsid = (option.find("a") != STR_NPOS);
  bool json = (option.find("json") != STR_NPOS);

  std::set<std::string> selected = {selection.c_str()};
  const auto& inconsistencies = (XrdMgmOfs::MgmFsckDirs.find(selection.c_str()) !=
                                 XrdMgmOfs::MgmFsckDirs.end())
                                ? selected : XrdMgmOfs::MgmFsckDirs;

  auto&& generatedReport = json ? GenerateJsonReport(inconsistencies, perfsid, printlfn)
                                : GenerateTextReport(inconsistencies, perfsid, printlfn);
  out = generatedReport.c_str();
}

inline std::string
fileNameDisplayFunc(unsigned long long fid) {
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try {
    std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(fid);
    return gOFS->eosView->getUri(fmd.get());
  } catch (eos::MDException& e) {
    return std::string("undefined");
  }
}

inline std::string
fileFidDisplayFunc(unsigned long long fid) {
  return std::to_string(fid);
}

std::string
Fsck::GenerateJsonReport(const std::set<std::string>& inconsistencyTypes, bool perfsid, bool printlfn) {
  auto inconsistencies = cache.getCachedObject(false, RetrieveInconsistencies);
  Json::Value root;

  Json::Value file;
  for(auto& inconsistency : inconsistencyTypes) {
    auto numberOfFiles = Json::UInt64{0};
    for (auto& pair : inconsistencies[inconsistency]) {
      if (!pair.second.first.empty()) {
        auto fsidString = std::to_string(pair.first);
        auto& files = perfsid ? root[inconsistency][fsidString]["files"] : root[inconsistency]["files"];
        if (perfsid) {
          root[inconsistency][fsidString]["n"] = Json::UInt64{pair.second.first.size()};
          root[inconsistency][fsidString]["timestamp"] = Json::Int64{pair.second.second};
        } else {
          numberOfFiles += pair.second.first.size();
        }

        for (auto& fid : pair.second.first) {
          if (printlfn) {
            files.append(fileNameDisplayFunc(fid));
          } else if (perfsid) {
            files.append(Json::UInt64{fid});
          } else {
            file.clear();
            file["fsid"] = Json::UInt64{pair.first};
            file["fid"] = Json::UInt64{fid};
            files.append(file);
          }
        }
      }
    }

    if (!perfsid && numberOfFiles > 0) {
      root[inconsistency]["n"] = numberOfFiles;
      root[inconsistency]["timestamp"] = Json::Int64{inconsistencies[inconsistency].cbegin()->second.second};
    }
  }

  Json::StyledWriter writer;
  return writer.write(root);
}

std::string
Fsck::GenerateTextReport(const std::set<std::string>& inconsistencyTypes, bool perfsid, bool printlfn) {
  auto inconsistencies = cache.getCachedObject(false, RetrieveInconsistencies);
  std::ostringstream outBuff;

  for(auto& inconsistency : inconsistencyTypes) {
    const char* separator = "";

    if(perfsid) {
      for(auto& pair : inconsistencies[inconsistency]) {
        if (!pair.second.first.empty()) {
          separator = "";
          outBuff << "timestamp=" << pair.second.second << " tag=\"" << inconsistency;
          outBuff << " fsid=" << pair.first << " n=" << pair.second.first.size() << (printlfn ? " lfn=" : " fxid=");
          for(auto& fid : pair.second.first) {
            auto fileDisplay = printlfn ? fileNameDisplayFunc(fid) : fileFidDisplayFunc(fid);
            outBuff << separator << std::move(fileDisplay);
            separator = ",";
          }
          outBuff << endl;
        }
        outBuff << endl;
      }
    }
    else {
      auto count = 0ul;
      auto time = inconsistencies[inconsistency].cbegin()->second.second;
      for(auto& pair : inconsistencies[inconsistency]) {
        count += pair.second.first.size();
      }

      if (count > 0) {
        outBuff << "timestamp=" << time << " tag=\"" << inconsistency << "\" n=" << count << (printlfn ? " lfn=" : " fxid=");
        for(auto& pair : inconsistencies[inconsistency]) {
          for (auto& fid : pair.second.first) {
            auto fileDisplay = printlfn ? fileNameDisplayFunc(fid) : fileFidDisplayFunc(fid);
            outBuff << separator << std::move(fileDisplay);
            separator = ",";
          }
        }
        outBuff << endl;
      }
    }
  }

  return outBuff.str();
}

void
Fsck::RemoveFsckFile(const string& inconsistency, eos::common::FileSystem::fsid_t fsid,
                     eos::common::FileId::fileid_t fid) {
  std::ostringstream fsckFilePathStr;
  fsckFilePathStr << gOFS->MgmProcFsckPath << "/" << inconsistency << "/" << fsid << "/" << fid;
  try {
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex);
    gOFS->eosView->removeFile(gOFS->eosView->getFile(fsckFilePathStr.str()).get());
  } catch (eos::MDException& e) {}
}

EOSMGMNAMESPACE_END
