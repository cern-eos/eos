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
#include "XrdOuc/XrdOucTokenizer.hh"
#include <unordered_set>

EOSMGMNAMESPACE_BEGIN

XrdSysSemaphore eos::mgm::FsCmd::mSemaphore{5};

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
eos::mgm::FsCmd::ProcessRequest()
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
  XrdOucString outLocal, errLocal;
  retc = proc_fs_add(sfsid, uuid, nodequeue, mountpoint, space, configstatus,
                     outLocal, errLocal, mVid);
  mOut = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  mErr = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
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
    std::string fsids = (bootProto.id_case() ==
                         eos::console::FsProto::BootProto::kFsid ?
                         std::to_string(bootProto.fsid()) : "0");
    bool forcemgmsync = bootProto.syncmgm();
    eos::common::FileSystem::fsid_t fsid = std::stoi(fsids);

    if (node == "*") {
      // boot all filesystems
      if (mVid.uid == 0) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        outStream << "success: boot message send to";

        for (const auto id : FsView::gFsView.mIdView) {
          if ((id.second->GetConfigStatus() > eos::common::FileSystem::kOff)) {
            if (forcemgmsync) {
              // set the check flag
              id.second->SetLongLong("bootcheck", eos::common::FileSystem::kBootResync);
            } else {
              // set the force flag
              id.second->SetLongLong("bootcheck", eos::common::FileSystem::kBootForced);
            }

            auto now = time(nullptr);

            if (now < 0) {
              now = 0;
            }

            id.second->SetLongLong("bootsenttime", (unsigned long long) now);
            outStream << " ";
            outStream << id.second->GetString("host").c_str();
            outStream << ":";
            outStream << id.second->GetString("path").c_str();
          }
        }
      } else {
        retc = EPERM;
        errStream << "error: you have to take role 'root' to execute this command";
      }
    } else {
      if (node.length()) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        if (!FsView::gFsView.mNodeView.count(node)) {
          errStream << "error: cannot boot node - no node with name=";
          errStream << node.c_str();
          retc = ENOENT;
        } else {
          outStream << "success: boot message send to";
          eos::mgm::BaseView::const_iterator it;

          for (it = FsView::gFsView.mNodeView[node]->begin();
               it != FsView::gFsView.mNodeView[node]->end(); ++it) {
            FileSystem* fs = nullptr;

            if (FsView::gFsView.mIdView.count(*it)) {
              fs = FsView::gFsView.mIdView[*it];
            }

            if (fs != nullptr) {
              if (forcemgmsync) {
                // set the check flag
                fs->SetLongLong("bootcheck", eos::common::FileSystem::kBootResync);
              } else {
                // set the force flag
                fs->SetLongLong("bootcheck", eos::common::FileSystem::kBootForced);
              }

              auto now = time(nullptr);
              fs->SetLongLong("bootsenttime", ((now > 0) ? now : 0));
              outStream << " ";
              outStream << fs->GetString("host").c_str();
              outStream << ":";
              outStream << fs->GetString("path").c_str();
            }
          }
        }
      }

      if (fsid) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        if (FsView::gFsView.mIdView.count(fsid)) {
          outStream << "success: boot message send to";
          FileSystem* fs = FsView::gFsView.mIdView[fsid];

          if (fs) {
            if (forcemgmsync) {
              // set the check flag
              fs->SetLongLong("bootcheck", eos::common::FileSystem::kBootResync);
              ;
            } else {
              // set the force flag
              fs->SetLongLong("bootcheck", eos::common::FileSystem::kBootForced);
            }

            fs->SetLongLong("bootsenttime", (unsigned long long) time(nullptr));
            outStream << " ";
            outStream << fs->GetString("host").c_str();
            outStream << ":";
            outStream << fs->GetString("path").c_str();
          }
        } else {
          errStream << "error: cannot boot filesystem - no filesystem with fsid=";
          errStream << fsids.c_str();
          retc = ENOENT;
        }
      }
    }
  } else {
    retc = EPERM;
    errStream << "error: you have to take role 'root' to execute this command";
  }

  mOut = outStream.str();
  mErr = errStream.str();
  return retc;
}

//------------------------------------------------------------------------------
// Config subcommand
//------------------------------------------------------------------------------
int
FsCmd::Config(const eos::console::FsProto::ConfigProto& configProto)
{
  auto key = configProto.key();
  auto value = configProto.value();
  std::string identifier;

  switch (configProto.id_case()) {
  case eos::console::FsProto::ConfigProto::kHostPortPath:
    identifier = configProto.hostportpath();
    break;

  case eos::console::FsProto::ConfigProto::kUuid:
    identifier = configProto.uuid();
    break;

  case eos::console::FsProto::ConfigProto::kFsid:
    identifier = std::to_string(configProto.fsid());
    break;

  default:
    break;
  }

  XrdOucString outLocal, errLocal;
  retc = proc_fs_config(identifier, key, value, outLocal, errLocal, mVid);
  mOut = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  mErr = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Dropdeletion subcommand
//------------------------------------------------------------------------------
int
FsCmd::DropDeletion(const eos::console::FsProto::DropDeletionProto&
                    dropdelProto)
{
  XrdOucString outLocal, errLocal;
  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_dropdeletion(std::to_string(dropdelProto.fsid()), outLocal,
                              errLocal, mVid);
  mOut = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  mErr = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}


//------------------------------------------------------------------------------
// Dumpmd subcommand
//------------------------------------------------------------------------------
int
FsCmd::DumpMd(const eos::console::FsProto::DumpMdProto& dumpmdProto)
{
  XrdOucString outLocal, errLocal;

  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    {
      // Stall if the namespace is still booting
      XrdSysMutexHelper lock(gOFS->InitializationMutex);

      while (gOFS->Initialized != gOFS->kBooted) {
        XrdSysTimer timer;
        timer.Snooze(2);
      }
    }
    std::string sfsid = std::to_string(dumpmdProto.fsid());
    XrdOucString option = dumpmdProto.display() ==
                          eos::console::FsProto::DumpMdProto::MONITOR ? "m" : "";
    XrdOucString dp = dumpmdProto.showpath() ? "1" : "0";
    XrdOucString df = dumpmdProto.showfid() ? "1" : "0";
    XrdOucString ds = dumpmdProto.showsize() ? "1" : "0";
    size_t entries = 0;
    retc = SemaphoreProtectedProcDumpmd(sfsid, option, dp, df, ds, outLocal,
                                        errLocal, entries);

    if (!retc) {
      gOFS->MgmStats.Add("DumpMd", mVid.uid, mVid.gid, entries);
    }
  } else {
    retc = EPERM;
    errLocal = "error: you have to take role 'root' or connect via 'sss' "
               "to execute this command";
  }

  mOut = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  mErr = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// List subcommand
//------------------------------------------------------------------------------
std::string
eos::mgm::FsCmd::List(const eos::console::FsProto::LsProto& lsProto)
{
  std::string output;
  auto displayModeString = DisplayModeToString(lsProto.display());
  auto listFormat = FsView::GetFileSystemFormat(displayModeString);

  if (!lsProto.brief()) {
    if (listFormat.find('S') != std::string::npos) {
      listFormat.replace(listFormat.find('S'), 1, "s");
    }
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintSpaces(output, "", listFormat, 0,
                              lsProto.matchlist().c_str(), displayModeString);
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
    XrdOucString outLocal, errLocal;
    retc = proc_fs_mv(source, dest, outLocal, errLocal, mVid);
    mOut = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
    mErr = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  } else {
    retc = EPERM;
    mErr = "error: you have to take role 'root' to execute this command";
  }

  return retc;
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
    nodequeue = hostmountpoint.substr(0, splitAt + 4);
    mountpoint = hostmountpoint.substr(splitAt + 4);
  }

  XrdOucString outLocal, errLocal;
  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_rm(nodequeue, mountpoint, id, outLocal, errLocal, mVid);
  mOut = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  mErr = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
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

    if (statusProto.riskassesment()) {
      riskanalysis = true;
    }

    if (statusProto.id_case() == eos::console::FsProto::StatusProto::kNodeQueue) {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      const std::string queuepath = statusProto.nodequeue();
      auto pos = queuepath.find("/fst");
      const std::string queue = queuepath.substr(0, pos + 4);
      const std::string mount = queuepath.substr(pos + 4);

      if (FsView::gFsView.mNodeView.count(queue)) {
        eos::mgm::BaseView::const_iterator it;

        for (it = FsView::gFsView.mNodeView[queue]->begin();
             it != FsView::gFsView.mNodeView[queue]->end(); it++) {
          if (FsView::gFsView.mIdView.count(*it)) {
            if (FsView::gFsView.mIdView[*it]->GetPath() == mount) {
              // this is the filesystem
              fsid = *it;
            }
          }
        }
      }

      if (!fsid) {
        errStream << "error: no such filesystem " << queuepath;
        mErr = errStream.str();
        retc = ENOENT;
        return retc;
      }
    } else {
      fsid = statusProto.fsid();
    }

    const std::string dotted_line =
      "# ------------------------------------------------------------------------------------\n";
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mIdView.count(fsid)) {
      FileSystem* fs = FsView::gFsView.mIdView[fsid];

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
                    if (FsView::gFsView.mIdView.count(loc)) {
                      FileSystem* repfs = FsView::gFsView.mIdView[loc];
                      eos::common::FileSystem::fs_snapshot_t snapshot;
                      repfs->SnapShotFileSystem(snapshot, false);

                      if ((snapshot.mStatus == eos::common::FileSystem::kBooted) &&
                          (snapshot.mConfigStatus == eos::common::FileSystem::kRW) &&
                          (snapshot.mErrCode == 0) && // this we probably don't need
                          (fs->GetActiveStatus(snapshot))) {
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

        retc = 0;
      }
    } else {
      errStream << "error: cannot find filesystem - no filesystem with fsid=";
      errStream << fsid;
      retc = ENOENT;
    }
  } else {
    retc = EPERM;
    errStream << "error: you have to take role 'root' to execute this command "
              "or connect via sss";
  }

  mOut = outStream.str();
  mErr = errStream.str();
  return retc;
}

int
FsCmd::DropFiles(const eos::console::FsProto::DropFilesProto& dropfilesProto)
{
  XrdOucErrInfo errInfo;
  auto filesDeleted = 0u;
  // Create a snapshot to avoid deadlock with dropstripe
  std::list<std::string> files;
  {
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);

    for (auto it_fid = gOFS->eosFsView->getFileList(dropfilesProto.fsid());
         (it_fid && it_fid->valid()); it_fid->next()) {
      try {
        auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
        files.emplace_back(gOFS->eosView->getUri(fmd.get()));
      } catch (eos::MDException& e) {
        eos_err("Could not get metadata for file %ul, ignoring it",
                it_fid->getElement());
      }
    }
  }

  for (const auto& filePath : files) {
    errInfo.clear();

    if (gOFS->_dropstripe(filePath.c_str(), errInfo, mVid, dropfilesProto.fsid(),
                          dropfilesProto.force()) != 0) {
      eos_err("Could not delete file replica %s on filesystem %u", filePath.c_str(),
              dropfilesProto.fsid());
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

int
FsCmd::Compare(const eos::console::FsProto::CompareProto& compareProto)
{
  std::string filePath;
  std::unordered_set<std::string, Murmur3::MurmurHasher<const std::string&>>
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
                                    XrdOucString& dp,
                                    XrdOucString& df, XrdOucString& ds, XrdOucString& out,
                                    XrdOucString& err, size_t& entries)
{
  try {
    mSemaphore.Wait();
  } catch (...) {
    err += "error: failed while waiting on semaphore, cannot dumpmd";
    return EAGAIN;
  }

  retc = proc_fs_dumpmd(fsid, option, dp, df, ds, out, err,
                        mVid, entries);

  try {
    mSemaphore.Post();
  } catch (...) {}

  return retc;
}

EOSMGMNAMESPACE_END
