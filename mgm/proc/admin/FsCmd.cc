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

EOSMGMNAMESPACE_BEGIN

XrdSysSemaphore eos::mgm::FsCmd::mSemaphore{100};

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
    std::string out, err;
    reply.set_retc(Add(fs.add(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kBoot) {
    std::string out, err;
    reply.set_retc(Boot(fs.boot(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kConfig) {
    std::string out, err;
    reply.set_retc(Config(fs.config(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDropdel) {
    std::string out, err;
    reply.set_retc(DropDeletion(fs.dropdel(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDumpmd) {
    std::string out, err;
    reply.set_retc(DumpMd(fs.dumpmd(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kLs) {
    reply.set_std_out(List(fs.ls()));
    reply.set_retc(0);
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kMv) {
    std::string out, err;
    reply.set_retc(Mv(fs.mv(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kRm) {
    std::string out, err;
    reply.set_retc(Rm(fs.rm(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else if (subCmdCase == eos::console::FsProto::SubcmdCase::kStatus) {
    std::string out, err;
    reply.set_retc(Status(fs.status(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Add subcommand
//------------------------------------------------------------------------------
int
FsCmd::Add(const eos::console::FsProto::AddProto& addProto, std::string& out,
           std::string& err)
{
  std::string sfsid = addProto.manual() ? std::to_string(addProto.fsid()) : "0";
  std::string uuid = addProto.uuid();
  std::string nodename = (addProto.nodequeue().empty() ? addProto.hostport() :
                          addProto.nodequeue());
  std::string mountpoint = addProto.mountpoint();
  std::string space = addProto.schedgroup();
  std::string configstatus = addProto.status();

  XrdOucString outLocal, errLocal;
  retc = proc_fs_add(sfsid, uuid, nodename, mountpoint, space, configstatus,
                     outLocal, errLocal, mVid);

  out = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  err = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Boot subcommand
//------------------------------------------------------------------------------
int
FsCmd::Boot(const eos::console::FsProto::BootProto& bootProto, std::string& out,
            std::string& err)
{
  std::ostringstream outStream, errStream;
  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    std::string node = (bootProto.id_case() ==
                        eos::console::FsProto::BootProto::kNodeQueue ?
                        bootProto.nodequeue() : "");
    std::string fsids = (bootProto.id_case() ==
                         eos::console::FsProto::BootProto::kFsid ?
                         std::to_string(bootProto.fsid()) : "");
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

  out = outStream.str();
  err = errStream.str();
  return retc;
}

//------------------------------------------------------------------------------
// Config subcommand
//------------------------------------------------------------------------------
int
FsCmd::Config(const eos::console::FsProto::ConfigProto& configProto,
              std::string& out, std::string& err)
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

  out = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  err = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Dropdeletion subcommand
//------------------------------------------------------------------------------
int
FsCmd::DropDeletion(const eos::console::FsProto::DropDeletionProto&
                    dropdelProto,
                    std::string& out, std::string& err)
{
  XrdOucString outLocal, errLocal;

  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_dropdeletion(std::to_string(dropdelProto.fsid()), outLocal, errLocal,
                              mVid);

  out = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  err = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}


//------------------------------------------------------------------------------
// Dumpmd subcommand
//------------------------------------------------------------------------------
int
FsCmd::DumpMd(const eos::console::FsProto::DumpMdProto& dumpmdProto,
              std::string& out, std::string& err)
{
  XrdOucString outLocal, errLocal;

  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    {
      // Stall if the namespace is still booting
      XrdSysMutexHelper lock(gOFS->InitializationMutex);

      if (gOFS->Initialized != gOFS->kBooted) {
        XrdOucErrInfo errInfo;
        return gOFS->Stall(errInfo, 60, "Namespace is still booting");
      }
    }
    std::string fsidst = std::to_string(dumpmdProto.fsid());
    XrdOucString option = dumpmdProto.display() ==
                          eos::console::FsProto::DumpMdProto::MONITOR ? "m" : "";
    XrdOucString dp = dumpmdProto.showpath() ? "1" : "0";
    XrdOucString df = dumpmdProto.showfid() ? "1" : "0";
    XrdOucString ds = dumpmdProto.showsize() ? "1" : "0";
    size_t entries = 0;

    try {
      mSemaphore.Wait();
    } catch (...) {
      err = "Cannot take protecting semaphore, cannot dump md.";
      return EAGAIN;
    }

    try {
      retc = proc_fs_dumpmd(fsidst, option, dp, df, ds, outLocal, errLocal,
                            mVid, entries);
    } catch (...) {
      try {
        mSemaphore.Post();
      } catch (...) {}
    }

    if (!retc) {
      gOFS->MgmStats.Add("DumpMd", mVid.uid, mVid.gid, entries);
    }
  } else {
    retc = EPERM;
    errLocal = "error: you have to take role 'root' or connect via 'sss' "
             "to execute this command";
  }

  out = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  err = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
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
FsCmd::Mv(const eos::console::FsProto::MvProto& mvProto, std::string& out,
          std::string& err)
{
  if (mVid.uid == 0) {
    std::string source = mvProto.src();
    std::string dest = mvProto.dst();

    XrdOucString outLocal, errLocal;

    retc = proc_fs_mv(source, dest, outLocal, errLocal, mVid);

    out = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
    err = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  } else {
    retc = EPERM;
    err = "error: you have to take role 'root' to execute this command";
  }

  return retc;
}

//------------------------------------------------------------------------------
// Rm subcommand
//------------------------------------------------------------------------------
int
FsCmd::Rm(const eos::console::FsProto::RmProto& rmProto, std::string& out,
          std::string& err)
{
  std::string nodename = "";
  std::string mountpoint = "";
  std::string id = (rmProto.id_case() == eos::console::FsProto::RmProto::kFsid ?
                    std::to_string(rmProto.fsid()) : "");

  if (rmProto.id_case() == eos::console::FsProto::RmProto::kNodeQueue) {
    const auto& hostmountpoint = rmProto.nodequeue();
    auto splitAt = hostmountpoint.find_first_of("/fst");
    nodename = hostmountpoint.substr(0, splitAt + 4);
    mountpoint = hostmountpoint.substr(splitAt + 4);
  }

  XrdOucString outLocal, errLocal;
  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_rm(nodename, mountpoint, id, outLocal, errLocal, mVid);

  out = outLocal.c_str() != nullptr ? outLocal.c_str() : "";
  err = errLocal.c_str() != nullptr ? errLocal.c_str() : "";
  return retc;
}

//------------------------------------------------------------------------------
// Status subcommand
//------------------------------------------------------------------------------
int
FsCmd::Status(const eos::console::FsProto::StatusProto& statusProto,
              std::string& out, std::string& err)
{
  std::ostringstream outStream, errStream;

  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    std::string fsids = statusProto.id_case() ==
                        eos::console::FsProto::StatusProto::kFsid
                        ? std::to_string(statusProto.fsid()) : "0";
    std::string node;
    std::string mount;

    if (statusProto.id_case() ==
        eos::console::FsProto::StatusProto::kHostMountpoint) {
      const auto& hostmountpoint = statusProto.hostmountpoint();
      auto slashAt = hostmountpoint.find_first_of('/');
      node = hostmountpoint.substr(0, slashAt);
      mount = hostmountpoint.substr(slashAt);
    }

    eos::common::FileSystem::fsid_t fsid = std::stoi(fsids);
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

    if (!fsid) {
      // try to get from the node/mountpoint
      if ((node.find(':') == std::string::npos)) {
        node += ":1095"; // default eos fst port
      }

      if ((node.find("/eos/") == std::string::npos)) {
        node.insert(0, "/eos/");
        node.append("/fst");
      }

      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      if (FsView::gFsView.mNodeView.count(node)) {
        eos::mgm::BaseView::const_iterator it;

        for (it = FsView::gFsView.mNodeView[node]->begin();
             it != FsView::gFsView.mNodeView[node]->end(); it++) {
          if (FsView::gFsView.mIdView.count(*it)) {
            if (FsView::gFsView.mIdView[*it]->GetPath() == mount) {
              // this is the filesystem
              fsid = *it;
            }
          }
        }
      }
    }

    if (fsid) {
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
        errStream << fsids.c_str();
        retc = ENOENT;
      }
    } else {
      errStream << "error: cannot find a matching filesystem";
      retc = ENOENT;
    }
  } else {
    retc = EPERM;
    errStream << "error: you have to take role 'root' to execute this command or connect via sss";
  }

  out = outStream.str();
  err = errStream.str();
  return retc;
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

EOSMGMNAMESPACE_END
