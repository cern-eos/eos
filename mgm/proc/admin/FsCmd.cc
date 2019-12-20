//------------------------------------------------------------------------------
// File: FsCmd.cc
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
* EOS - the CERN Disk Storage System                                   *
* Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "FsCmd.hh"
#include "mgm/FsView.hh"
#include "mgm/proc/proc_fs.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include <unordered_set>

EOSMGMNAMESPACE_BEGIN

XrdSysSemaphore eos::mgm::FsCmd::mSemaphore{5};

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
eos::mgm::FsCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::FsProto fs = mReqProto.fs();
  const auto& subCmdCase = fs.subcmd_case();

  if (subCmdCase == eos::console::FsProto::SubcmdCase::kAdd) {
    reply.set_retc(Add(fs.add()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kBoot) {
    reply.set_retc(Boot(fs.boot()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kClone) {
    reply.set_retc(Clone(fs.clone()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kCompare) {
    reply.set_retc(Compare(fs.compare()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kConfig) {
    reply.set_retc(Config(fs.config()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDropdel) {
    reply.set_retc(DropDeletion(fs.dropdel()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDropghosts) {
    reply.set_retc(DropGhosts(fs.dropghosts()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDropfiles) {
    reply.set_retc(DropFiles(fs.dropfiles()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDumpmd) {
    reply.set_retc(DumpMd(fs.dumpmd()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kLs) {
    mOut = List(fs.ls());
    reply.set_retc(0);
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kMv) {
    reply.set_retc(Mv(fs.mv()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kRm) {
    reply.set_retc(Rm(fs.rm()));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kStatus) {
    reply.set_retc(Status(fs.status()));
  } else {
    reply.set_retc(EINVAL);
    mErr = "error: not supported";
  }

  reply.set_std_out(mOut);
  reply.set_std_err(mErr);
  return reply;
}

//------------------------------------------------------------------------------
// Add subcommand
//------------------------------------------------------------------------------
int
FsCmd::Add(const eos::console::FsProto::AddProto& addProto)
{
  std::string sfsid = addProto.manual() ? std::to_string(addProto.fsid()) : "0";
  std::string uuid = addProto.uuid();
  std::string nodequeue = addProto.nodequeue();

  // If nodequeue is empty then we have the host or even the host and the port
  if (nodequeue.empty()) {
    if (addProto.hostport().empty()) {
      mErr = "error: no nodequeue or or hostport specified";
      return EINVAL;
    }

    nodequeue = "/eos/";
    nodequeue += addProto.hostport();

    // If only hostname present then append default FST port number 1095
    if (nodequeue.find(':') == std::string::npos) {
      nodequeue += ":1095";
    }

    nodequeue += "/fst";
  }

  std::string mountpoint = addProto.mountpoint();
  std::string space = addProto.schedgroup();
  std::string configstatus = addProto.status();
  XrdOucString out, err;
  retc = proc_fs_add(sfsid, uuid, nodequeue, mountpoint, space, configstatus,
                     out, err, mVid);
  mOut = out.c_str() != nullptr ? out.c_str() : "";
  mErr = err.c_str() != nullptr ? err.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Boot subcommand
//------------------------------------------------------------------------------
int
FsCmd::Boot(const eos::console::FsProto::BootProto& bootProto)
{
  std::ostringstream outStream, errStream;

  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    std::string node = (bootProto.id_case() ==
                        eos::console::FsProto::BootProto::kNodeQueue ?
                        bootProto.nodequeue() : "");
    std::string sfsid = (bootProto.id_case() ==
                         eos::console::FsProto::BootProto::kFsid ?
                         std::to_string(bootProto.fsid()) : "0");
    std::string fsuuid = (bootProto.id_case() ==
                          eos::console::FsProto::BootProto::kUuid ?
                          bootProto.uuid() : "");
    bool forcemgmsync = bootProto.syncmgm();

    // eos::common::FileSystem::fsid_t fsid = std::stoi(sfsid);
    // @note it would be nicer if the method get refactored
    eos::common::FileSystem::fsid_t fsid = 0;
    try {
      fsid = std::stoi(sfsid);
    } catch (const std::exception& e) {
      fsid = 0;
    }

    if (node == "*") {
      // boot all filesystems
      if (mVid.uid == 0) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        outStream << "success: boot message sent to";

        for (const auto id : FsView::gFsView.mIdView) {
          if ((id.second->GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
            eos::common::FileSystem::eBootConfig bootConfig = (forcemgmsync)
                ? eos::common::FileSystem::kBootResync  // MGM resync
                : eos::common::FileSystem::kBootForced; // local resync
            auto now = time(nullptr);
            id.second->SetLongLong("bootcheck", bootConfig);
            id.second->SetLongLong("bootsenttime", (unsigned long long) now);
            outStream << " ";
            outStream << id.second->GetString("host").c_str();
            outStream << ":";
            outStream << id.second->GetString("path").c_str();
          }
        }
      } else {
        mRetC = EPERM;
        errStream << "error: you have to take role 'root' to execute this command";
      }
    } else if (node.length()) {
      // boot all filesystems on node queue
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      if (!FsView::gFsView.mNodeView.count(node)) {
        errStream << "error: cannot boot node - no node with name=";
        errStream << node.c_str();
        mRetC = ENOENT;
      } else {
        outStream << "success: boot message sent to";

        for (auto it = FsView::gFsView.mNodeView[node]->begin();
             it != FsView::gFsView.mNodeView[node]->end(); ++it) {
          FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

          if (fs != nullptr) {
            eos::common::FileSystem::eBootConfig bootConfig = (forcemgmsync)
                ? eos::common::FileSystem::kBootResync  // MGM resync
                : eos::common::FileSystem::kBootForced; // local resync
            auto now = time(nullptr);
            fs->SetLongLong("bootcheck", bootConfig);
            fs->SetLongLong("bootsenttime", ((now > 0) ? now : 0));
            outStream << " ";
            outStream << fs->GetString("host").c_str();
            outStream << ":";
            outStream << fs->GetString("path").c_str();
          }
        }
      }
    } else {
      // boot filesystem by fsid or uuid
      FileSystem* fs = nullptr;

      if (fsid) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = FsView::gFsView.mIdView.lookupByID(fsid);

        if (!fs) {
          errStream << "error: cannot boot filesystem - no filesystem with fsid=";
          errStream << sfsid.c_str();
          mRetC = ENOENT;
        }
      } else if (fsuuid.length()) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        if (FsView::gFsView.GetMapping(fsuuid)) {
          fs = FsView::gFsView.mIdView.lookupByID(FsView::gFsView.GetMapping(fsuuid));
        } else {
          errStream << "error: cannot boot filesystem - no filesystem with uuid=";
          errStream << fsuuid.c_str();
          mRetC = ENOENT;
        }
      }

      if (fs != nullptr) {
        eos::common::FileSystem::eBootConfig bootConfig = (forcemgmsync)
            ? eos::common::FileSystem::kBootResync  // MGM resync
            : eos::common::FileSystem::kBootForced; // local resync
        fs->SetLongLong("bootcheck", bootConfig);
        fs->SetLongLong("bootsenttime", (unsigned long long) time(nullptr));
        outStream << "success: boot message sent to ";
        outStream << fs->GetString("host").c_str();
        outStream << ":";
        outStream << fs->GetString("path").c_str();
      } else if (!mRetC) {
        // Should not get here
        errStream << "error: could not retrieve filesystem";
        mRetC = ENOENT;
      }
    }
  } else {
    mRetC = EPERM;
    errStream << "error: you have to take role 'root' or connect via 'sss' "
              "to execute this command";
  }

  mOut = outStream.str();
  mErr = errStream.str();
  return mRetC;
}

//------------------------------------------------------------------------------
// Config subcommand
//------------------------------------------------------------------------------
int
FsCmd::Config(const eos::console::FsProto::ConfigProto& configProto)
{
  auto key = configProto.key();
  auto value = configProto.value();
  std::string identifier = std::to_string(configProto.fsid());
  XrdOucString out, err;
  retc = proc_fs_config(identifier, key, value, out, err,
                        mVid, mComment.c_str());
  mOut = out.c_str() != nullptr ? out.c_str() : "";
  mErr = err.c_str() != nullptr ? err.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Dropdeletion subcommand
//------------------------------------------------------------------------------
int
FsCmd::DropDeletion(const eos::console::FsProto::DropDeletionProto& drop_del)
{
  std::string out, err;
  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_dropdeletion(drop_del.fsid(), mVid, out, err);
  mOut = out;
  mErr = err;
  return retc;
}

//------------------------------------------------------------------------------
// DropGhosts subcommand
//------------------------------------------------------------------------------
int
FsCmd::DropGhosts(const eos::console::FsProto::DropGhostsProto& drop_ghosts)
{
  std::string out, err;
  std::set<eos::IFileMD::id_t> fids;
  fids.insert(drop_ghosts.fids().cbegin(), drop_ghosts.fids().cend());
  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_dropghosts(drop_ghosts.fsid(), fids, mVid, out, err);
  mOut = out;
  mErr = err;
  return retc;
}

//------------------------------------------------------------------------------
// Dumpmd subcommand
//------------------------------------------------------------------------------
int
FsCmd::DumpMd(const eos::console::FsProto::DumpMdProto& dumpmdProto)
{
  XrdOucString out, err;

  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    // Stall if the namespace is still booting
    while (!gOFS->IsNsBooted()) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    std::string sfsid = std::to_string(dumpmdProto.fsid());
    XrdOucString option = dumpmdProto.display() ==
                          eos::console::FsProto::DumpMdProto::MONITOR ? "m" : "";
    XrdOucString dp = dumpmdProto.showpath() ? "1" : "0";
    XrdOucString df = dumpmdProto.showfid() ? "1" : "0";
    XrdOucString ds = dumpmdProto.showsize() ? "1" : "0";
    size_t entries = 0;
    retc = SemaphoreProtectedProcDumpmd(sfsid, option, dp, df, ds, out,
                                        err, entries);

    if (!mRetC) {
      gOFS->MgmStats.Add("DumpMd", mVid.uid, mVid.gid, entries);
    }
  } else {
    retc = EPERM;
    err = "error: you have to take role 'root' or connect via 'sss' "
          "to execute this command";
  }

  mOut = out.c_str() != nullptr ? out.c_str() : "";
  mErr = err.c_str() != nullptr ? err.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// List subcommand
//------------------------------------------------------------------------------
std::string
eos::mgm::FsCmd::List(const eos::console::FsProto::LsProto& lsProto)
{
  using eos::console::FsProto;
  bool json_output = false;
  std::string output;

  // Handle listing of drain jobs
  if ((lsProto.display() == FsProto::LsProto::RUNNING_DRAIN_JOBS) ||
      (lsProto.display() == FsProto::LsProto::FAILED_DRAIN_JOBS)) {
    bool only_failed =
      (lsProto.display() == FsProto::LsProto::FAILED_DRAIN_JOBS);
    eos::mgm::Drainer::DrainHdrInfo hdr_info;

    if (only_failed) {
      hdr_info = {{"File id",    "fid"},
                  {"Drain fsid", "fs_src"},
                  {"Dst fsid",   "fs_dst"},
                  {"Error info", "err_msg"}
      };
    } else {
      hdr_info = {{"File id",     "fid"},
                  {"Drain fsid",  "fs_src"},
                  {"Src fsid",    "tx_fs_src"},
                  {"Dst fsid",    "fs_dst"},
                  {"Start times", "start_timestamp"},
                  {"Progress",    "progress"},
                  {"Avg.(MB/s)",  "speed"}
      };
    }

    unsigned int fsid {0};

    // If matchlist is present then it must be an fsid
    if (!lsProto.matchlist().empty()) {
      try {
        fsid = std::stoul(lsProto.matchlist());
      } catch (...) {
        // ignore
      }
    }

    if (!gOFS->mDrainEngine.GetJobsInfo(output, hdr_info, fsid, only_failed)) {
      output = "error: failed while collecting drain jobs info";
    }

    return output;
  }

  auto display = lsProto.display();

  if ((display == FsProto::LsProto::DEFAULT) && WantsJsonOutput()) {
    display = FsProto::LsProto::MONITOR;
  }

  if (display == FsProto::LsProto::MONITOR) {
    json_output = WantsJsonOutput();
  }

  auto display_string = DisplayModeToString(display);
  auto format = FsView::GetFileSystemFormat(display_string);

  if (!lsProto.brief()) {
    if (format.find('S') != std::string::npos) {
      format.replace(format.find('S'), 1, "s");
    }
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintSpaces(output, "", format, 0,
                              lsProto.matchlist().c_str(),
                              display_string, mReqProto.dontcolor());

  if (json_output) {
    output = ResponseToJsonString(output);
  }

  return output;
}

//------------------------------------------------------------------------------
// Mv subcommand
//------------------------------------------------------------------------------
int
FsCmd::Mv(const eos::console::FsProto::MvProto& mvProto)
{
  if (mVid.uid == 0) {
    std::string source = mvProto.src();
    std::string dest = mvProto.dst();
    bool force = mvProto.force();
    XrdOucString out, err;
    retc = proc_fs_mv(source, dest, out, err, mVid, force);
    mOut = out.c_str() != nullptr ? out.c_str() : "";
    mErr = err.c_str() != nullptr ? err.c_str() : "";
  } else {
    mRetC = EPERM;
    mErr = "error: you have to take role 'root' to execute this command";
  }

  return mRetC;
}

//------------------------------------------------------------------------------
// Rm subcommand
//------------------------------------------------------------------------------
int
FsCmd::Rm(const eos::console::FsProto::RmProto& rmProto)
{
  std::string nodequeue;
  std::string mountpoint;
  std::string id = (rmProto.id_case() == eos::console::FsProto::RmProto::kFsid ?
                    std::to_string(rmProto.fsid()) : "");

  if (rmProto.id_case() == eos::console::FsProto::RmProto::kNodeQueue) {
    const auto& hostmountpoint = rmProto.nodequeue();
    auto splitAt = hostmountpoint.find("/fst");
    try { // @note quick patch against std::out_of_range, could be nicer
      nodequeue = hostmountpoint.substr(0, splitAt + 4);
      mountpoint = hostmountpoint.substr(splitAt + 4);
    } catch (std::out_of_range& e) {
      mOut = "";
      mErr = "error: there is no such nodequeue (check format): '" + rmProto.nodequeue() + "' " + id + "\n";
      retc = EINVAL;
      return retc;
    }
  }

  XrdOucString out, err;
  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_rm(nodequeue, mountpoint, id, out, err, mVid);
  mOut = out.c_str() != nullptr ? out.c_str() : "";
  mErr = err.c_str() != nullptr ? err.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Status subcommand
//------------------------------------------------------------------------------
int
FsCmd::Status(const eos::console::FsProto::StatusProto& statusProto)
{
  std::ostringstream outStream, errStream;

  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    eos::common::FileSystem::fsid_t fsid = 0;
    XrdOucString filelisting = "";
    bool listfile = false;
    bool riskanalysis = false;

    if (statusProto.longformat()) {
      listfile = true;
      riskanalysis = true;
    }

    if (statusProto.riskassessment()) {
      riskanalysis = true;
    }

    if (statusProto.id_case() == eos::console::FsProto::StatusProto::kNodeQueue) {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      const std::string& queuepath = statusProto.nodequeue();
      auto pos = queuepath.find("/fst");
      const std::string queue = queuepath.substr(0, pos + 4);
      const std::string mount = queuepath.substr(pos + 4);

      if (FsView::gFsView.mNodeView.count(queue)) {
        for (auto it = FsView::gFsView.mNodeView[queue]->begin();
             it != FsView::gFsView.mNodeView[queue]->end(); ++it) {
          FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

          if (fs && fs->GetPath() == mount) {
            // this is the filesystem
            fsid = *it;
          }
        }
      }

      if (!fsid) {
        errStream << "error: no such filesystem " << queuepath;
        mErr = errStream.str();
        mRetC = ENOENT;
        return mRetC;
      }
    } else {
      fsid = statusProto.fsid();
    }

    const std::string dotted_line =
      "# ------------------------------------------------------------------------------------\n";
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (fs) {
      outStream << dotted_line.c_str();
      outStream << "# FileSystem Variables\n";
      outStream << dotted_line.c_str();
      std::vector<std::string> keylist;
      fs->GetKeys(keylist);
      std::sort(keylist.begin(), keylist.end());

      for (auto& key : keylist) {
        char line[1024];
        snprintf(line, sizeof(line) - 1, "%-32s := %s\n", key.c_str(),
                 fs->GetString(key.c_str()).c_str());
        outStream << line;
      }

      if (riskanalysis) {
        outStream << dotted_line.c_str();
        outStream << "# Risk Analysis\n";
        outStream << dotted_line.c_str();
        // get some statistics about the filesystem
        //-------------------------------------------
        unsigned long long nfids = 0;
        unsigned long long nfids_healthy = 0;
        unsigned long long nfids_risky = 0;
        unsigned long long nfids_inaccessible = 0;
        unsigned long long nfids_todelete = 0;
        eos::Prefetcher::prefetchFilesystemFileListWithFileMDsAndParentsAndWait(
          gOFS->eosView, gOFS->eosFsView, fsid);
        eos::common::RWMutexReadLock viewLock(gOFS->eosViewRWMutex);

        try {
          nfids_todelete = gOFS->eosFsView->getNumUnlinkedFilesOnFs(fsid);
          nfids = gOFS->eosFsView->getNumFilesOnFs(fsid);

          for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
               (it_fid && it_fid->valid()); it_fid->next()) {
            std::shared_ptr<eos::IFileMD> fmd =
              gOFS->eosFileService->getFileMD(it_fid->getElement());

            if (fmd) {
              size_t nloc_ok = 0;
              size_t nloc = fmd->getNumLocation();

              for (auto& loc : fmd->getLocations()) {
                if (loc) {
                  FileSystem* repfs = FsView::gFsView.mIdView.lookupByID(loc);

                  if (repfs) {
                    eos::common::FileSystem::fs_snapshot_t snapshot;
                    repfs->SnapShotFileSystem(snapshot, false);

                    if ((snapshot.mStatus == eos::common::BootStatus::kBooted) &&
                        (snapshot.mConfigStatus == eos::common::ConfigStatus::kRW) &&
                        (snapshot.mErrCode == 0) && // this we probably don't need
                        (snapshot.GetActiveStatus() == eos::common::ActiveStatus::kOnline)) {
                      nloc_ok++;
                    }
                  }
                }
              }

              if (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
                  eos::common::LayoutId::kReplica) {
                if (nloc_ok == nloc) {
                  nfids_healthy++;
                } else {
                  if (nloc_ok == 0) {
                    nfids_inaccessible++;

                    if (listfile) {
                      filelisting += "status=offline path=";
                      filelisting += gOFS->eosView->getUri(fmd.get()).c_str();
                      filelisting += "\n";
                    }
                  } else {
                    if (nloc_ok < nloc) {
                      nfids_risky++;

                      if (listfile) {
                        filelisting += "status=atrisk  path=";
                        filelisting += gOFS->eosView->getUri(fmd.get()).c_str();
                        filelisting += "\n";
                      }
                    }
                  }
                }
              }

              if (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
                  eos::common::LayoutId::kPlain) {
                if (nloc_ok != nloc) {
                  nfids_inaccessible++;

                  if (listfile) {
                    filelisting += "status=offline path=";
                    filelisting += gOFS->eosView->getUri(fmd.get()).c_str();
                    filelisting += "\n";
                  }
                }
              }
            }
          }

          XrdOucString sizestring;
          char line[1024];
          snprintf(line, sizeof(line) - 1, "%-32s := %10s (%.02f%%)\n", "number of files",
                   eos::common::StringConversion::GetSizeString(sizestring, nfids), 100.0);
          outStream << line;
          snprintf(line, sizeof(line) - 1, "%-32s := %10s (%.02f%%)\n", "files healthy",
                   eos::common::StringConversion::GetSizeString(sizestring, nfids_healthy),
                   nfids ? (100.0 * nfids_healthy) / nfids : 100.0);
          outStream << line;
          snprintf(line, sizeof(line) - 1, "%-32s := %10s (%.02f%%)\n", "files at risk",
                   eos::common::StringConversion::GetSizeString(sizestring, nfids_risky),
                   nfids ? (100.0 * nfids_risky) / nfids : 100.0);
          outStream << line;
          snprintf(line, sizeof(line) - 1, "%-32s := %10s (%.02f%%)\n",
                   "files inaccessible", eos::common::StringConversion::GetSizeString(sizestring,
                       nfids_inaccessible), nfids ? (100.0 * nfids_inaccessible) / nfids : 100.0);
          outStream << line;
          snprintf(line, sizeof(line) - 1, "%-32s := %10s\n", "files pending deletion",
                   eos::common::StringConversion::GetSizeString(sizestring, nfids_todelete));
          outStream << line;
          outStream << dotted_line.c_str();

          if (listfile) {
            outStream << filelisting;
          }
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          eos_static_err("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        }
      }

      mRetC = 0;
    } else {
      errStream << "error: cannot find filesystem - no filesystem with fsid=";
      errStream << fsid;
      mRetC = ENOENT;
    }
  } else {
    mRetC = EPERM;
    errStream << "error: you have to take role 'root' to execute this command "
              "or connect via sss";
  }

  mOut = outStream.str();
  mErr = errStream.str();
  return mRetC;
}

//------------------------------------------------------------------------------
// Drop files attached to a file system by file id
//------------------------------------------------------------------------------
int
FsCmd::DropFiles(const eos::console::FsProto::DropFilesProto& dropfilesProto)
{
  XrdOucErrInfo errInfo;
  auto filesDeleted = 0u;
  // Create a snapshot to avoid deadlock with dropstripe
  std::vector<eos::common::FileId::fileid_t> fileids;
  {
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);

    for (auto it_fid = gOFS->eosFsView->getFileList(dropfilesProto.fsid());
         (it_fid && it_fid->valid()); it_fid->next()) {
      try {
        fileids.push_back(it_fid->getElement());
      } catch (eos::MDException& e) {
        eos_err("msg=\"failed to get metadata, ignore it\" fxid=%08llx",
                it_fid->getElement());
      }
    }
  }

  for (const auto& fid : fileids) {
    errInfo.clear();

    if (gOFS->_dropstripe("", fid, errInfo, mVid, dropfilesProto.fsid(),
                          dropfilesProto.force()) != 0) {
      eos_err("msg=\"failed to  delete replica\" fxid=%08llx fsid=%lu",
              fid, dropfilesProto.fsid());
    } else {
      filesDeleted++;
    }
  }

  std::ostringstream oss;
  oss << "Deleted " << filesDeleted << " replicas on filesystem " <<
      dropfilesProto.fsid() << std::endl;
  mOut = oss.str();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Compare two file systemd in therm of the files they contain
//------------------------------------------------------------------------------
int
FsCmd::Compare(const eos::console::FsProto::CompareProto& compareProto)
{
  std::string filePath;
  std::unordered_set<std::string, Murmur3::MurmurHasher<std::string>>
      sourceHash, targetHash;
  {
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);

    for (auto it_fid = gOFS->eosFsView->getFileList(compareProto.sourceid());
         (it_fid && it_fid->valid()); it_fid->next()) {
      try {
        auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
        filePath = gOFS->eosView->getUri(fmd.get());
        sourceHash.insert(filePath);
      } catch (eos::MDException& e) {}
    }

    for (auto it_fid = gOFS->eosFsView->getFileList(compareProto.targetid());
         (it_fid && it_fid->valid()); it_fid->next()) {
      try {
        auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
        filePath = gOFS->eosView->getUri(fmd.get());
        targetHash.insert(filePath);
      } catch (eos::MDException& e) {}
    }
  }
  std::ostringstream resultStream;

  for (const auto& source : sourceHash) {
    if (targetHash.find(source) == targetHash.end()) {
      resultStream << "path=" << source << " => found in " << compareProto.sourceid()
                   << " - missing in " << compareProto.targetid() << std::endl;
    }
  }

  for (const auto& target : targetHash) {
    if (sourceHash.find(target) == sourceHash.end()) {
      resultStream << "path=" << target << " => found in " << compareProto.targetid()
                   << " - missing in " << compareProto.sourceid() << std::endl;
    }
  }

  mOut = resultStream.str();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Clone the contents of one file system to another
//------------------------------------------------------------------------------
int
FsCmd::Clone(const eos::console::FsProto::CloneProto& cloneProto)
{
  std::string filePath;
  XrdOucErrInfo errInfo;
  auto success = 0u;
  eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);

  for (auto it_fid = gOFS->eosFsView->getFileList(cloneProto.sourceid());
       (it_fid && it_fid->valid()); it_fid->next()) {
    try {
      auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
      filePath = gOFS->eosView->getUri(fmd.get());
      errInfo.clear();

      if (gOFS->_copystripe(filePath.c_str(), errInfo, mVid,
                            cloneProto.sourceid(), cloneProto.targetid()) == 0) {
        success++;
      }
    } catch (eos::MDException& e) {
      eos_err("Could not get metadata, could not clone file replica %ul on filesystem",
              it_fid->getElement());
    }
  }

  std::ostringstream oss;
  oss << "Successfully replicated " << success << " files." << endl;
  mOut = oss.str();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Convert display mode flags to strings
//------------------------------------------------------------------------------
std::string
eos::mgm::FsCmd::DisplayModeToString(eos::console::FsProto::LsProto::DisplayMode
                                     mode)
{
  switch (mode) {
  case eos::console::FsProto::LsProto::LONG:
    return "l";

  case eos::console::FsProto::LsProto::MONITOR:
    return "m";

  case eos::console::FsProto::LsProto::DRAIN:
    return "d";

  case eos::console::FsProto::LsProto::ERROR:
    return "e";

  case eos::console::FsProto::LsProto::FSCK:
    return "fsck";

  case eos::console::FsProto::LsProto::IO:
    return "io";

  default:
    return "";
  }
}

int
FsCmd::SemaphoreProtectedProcDumpmd(std::string& fsid, XrdOucString& option,
                                    XrdOucString& dp, XrdOucString& df,
                                    XrdOucString& ds, XrdOucString& out,
                                    XrdOucString& err, size_t& entries)
{
  try {
    mSemaphore.Wait();
  } catch (...) {
    err += "error: failed while waiting on semaphore, cannot dumpmd";
    return EAGAIN;
  }

  int retc = proc_fs_dumpmd(fsid, option, dp, df, ds, out, err,
                            mVid, entries);

  try {
    mSemaphore.Post();
  } catch (...) {}

  return retc;
}

EOSMGMNAMESPACE_END
