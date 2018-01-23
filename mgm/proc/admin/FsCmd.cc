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

EOSMGMNAMESPACE_BEGIN

XrdSysSemaphore eos::mgm::FsCmd::mSemaphore{100};

eos::console::ReplyProto
eos::mgm::FsCmd::ProcessRequest() {
  eos::console::ReplyProto reply;
  eos::console::FsProto fs = mReqProto.fs();

  const auto& subCmdCase = fs.subcmd_case();
  if (subCmdCase == eos::console::FsProto::SubcmdCase::kAdd) {
    std::string out, err;
    reply.set_retc(Add(fs.add(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kBoot) {
    std::string out, err;
    reply.set_retc(Boot(fs.boot(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kConfig) {
    std::string out, err;
    reply.set_retc(Config(fs.config(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDropdel) {
    std::string out, err;
    reply.set_retc(DropDeletion(fs.dropdel(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDropfiles) {
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kDumpmd) {
    std::string out, err;
    reply.set_retc(DumpMd(fs.dumpmd(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kMv) {
    std::string out, err;
    reply.set_retc(Mv(fs.mv(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kLs) {
    auto out = List(fs.ls());
    reply.set_std_out(out);
    reply.set_retc(0);
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kRm) {
    std::string out, err;
    reply.set_retc(Rm(fs.rm(), out, err));
    reply.set_std_out(std::move(out));
    reply.set_std_err(std::move(err));
  }
  else if (subCmdCase == eos::console::FsProto::SubcmdCase::kStatus) {
  }
  else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

std::string
eos::mgm::FsCmd::List(const eos::console::FsProto::LsProto& lsProto)
{
  std::string output;
  std::string format;

  auto displayModeString = DisplayModeToString(lsProto.display());
  auto listFormat = FsView::GetFileSystemFormat(displayModeString);

  if (!lsProto.brief()) {
    if (lsProto.silent()) {
      format = "s";
    }

    if (lsProto.silent()) {
      listFormat = "s";
    }
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintSpaces(output, format, listFormat, 0, lsProto.matchlist().c_str(),
                              displayModeString);
  return output;
}

int
FsCmd::Config(const eos::console::FsProto::ConfigProto& configProto, std::string& out, std::string& err) {
  auto key = configProto.key();
  auto value = configProto.value();

  std::string identifier;
  switch (configProto.id_case()) {
    case eos::console::FsProto::ConfigProto::IdCase::kHostPortPath:
      identifier = configProto.hostportpath();
      break;
    case eos::console::FsProto::ConfigProto::IdCase::kUuid:
      identifier = configProto.uuid();
      break;
    case eos::console::FsProto::ConfigProto::IdCase::kFsid:
      identifier = std::to_string(configProto.fsid());
      break;
    default:
      break;
  }

  std::string tident = GetTident();

  retc = proc_fs_config(identifier, key, value, stdOut, stdErr, tident, mVid);
  out = stdOut.c_str();
  err = stdErr.c_str();

  return retc;
}

int
FsCmd::Mv(const eos::console::FsProto::MvProto& mvProto, std::string& out, std::string& err) {
  if (mVid.uid == 0) {
    std::string source = mvProto.src();
    std::string dest = mvProto.dst();

    std::string tident = GetTident();

    retc = proc_fs_mv(source, dest, stdOut, stdErr, tident, mVid);
    out = stdOut.c_str();
    err = stdErr.c_str();
  } else {
    retc = EPERM;
    err = "error: you have to take role 'root' to execute this command";
  }

  return retc;
}

int
FsCmd::Rm(const eos::console::FsProto::RmProto& rmProto, std::string& out, std::string& err) {
  std::string nodename = "";
  std::string mountpoint = "";
  std::string id = rmProto.id_case() == eos::console::FsProto::RmProto::IdCase::kFsid ? std::to_string(rmProto.fsid()) : "";

  std::string tident = GetTident();

  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_rm(nodename, mountpoint, id, stdOut, stdErr, tident, mVid);
  out = stdOut.c_str();
  err = stdErr.c_str();

  return retc;
}

int
FsCmd::DropDeletion(const eos::console::FsProto::DropDeletionProto& dropdelProto, std::string& out,
                    std::string& err) {
  auto tident = GetTident();

  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
  retc = proc_fs_dropdeletion(std::to_string(dropdelProto.fsid()), stdOut, stdErr, tident, mVid);
  out = stdOut.c_str();
  err = stdErr.c_str();

  return retc;
}

int
FsCmd::Add(const eos::console::FsProto::AddProto& addProto, std::string& out, std::string& err) {
  std::string sfsid = std::to_string(addProto.fsid());
  std::string uuid = addProto.uuid();
  std::string nodename = addProto.nodequeue().empty() ? addProto.hostport() : addProto.nodequeue();
  std::string mountpoint = addProto.mountpoint();
  std::string space = addProto.schedgroup();
  std::string configstatus = addProto.status();

  auto tident = GetTident();
  retc = proc_fs_add(sfsid, uuid, nodename, mountpoint, space, configstatus,
                     stdOut, stdErr, tident, mVid);
  out = stdOut.c_str();
  err = stdErr.c_str();

  return retc;
}

int
FsCmd::Boot(const eos::console::FsProto::BootProto& bootProto, std::string& out, std::string& err) {
  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    std::string node = bootProto.id_case() == eos::console::FsProto::BootProto::IdCase::kNodeQueue ? bootProto.nodequeue() :
                       "";
    std::string fsids = bootProto.id_case() == eos::console::FsProto::BootProto::IdCase::kFsid ? std::to_string(bootProto.fsid()) :
                        "";
    bool forcemgmsync = bootProto.syncmgm();
    eos::common::FileSystem::fsid_t fsid = atoi(fsids.c_str());

    if (node == "*") {
      // boot all filesystems
      if (mVid.uid == 0) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        stdOut += "success: boot message send to";

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
            stdOut += " ";
            stdOut += id.second->GetString("host").c_str();
            stdOut += ":";
            stdOut += id.second->GetString("path").c_str();
          }
        }
      } else {
        retc = EPERM;
        stdErr = "error: you have to take role 'root' to execute this command";
      }
    } else {
      if (node.length()) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        if (!FsView::gFsView.mNodeView.count(node)) {
          stdErr = "error: cannot boot node - no node with name=";
          stdErr += node.c_str();
          retc = ENOENT;
        } else {
          stdOut += "success: boot message send to";

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
              stdOut += " ";
              stdOut += fs->GetString("host").c_str();
              stdOut += ":";
              stdOut += fs->GetString("path").c_str();
            }
          }
        }
      }

      if (fsid) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        if (FsView::gFsView.mIdView.count(fsid)) {
          stdOut += "success: boot message send to";
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
            stdOut += " ";
            stdOut += fs->GetString("host").c_str();
            stdOut += ":";
            stdOut += fs->GetString("path").c_str();
          }
        } else {
          stdErr = "error: cannot boot filesystem - no filesystem with fsid=";
          stdErr += fsids.c_str();
          retc = ENOENT;
        }
      }
    }
  } else {
    retc = EPERM;
    stdErr = "error: you have to take role 'root' to execute this command";
  }

  out = stdOut.c_str();
  err = stdOut.c_str();

  return retc;
}

int
FsCmd::DumpMd(const eos::console::FsProto::DumpMdProto& dumpmdProto, std::string& out, std::string& err) {
  if ((mVid.uid == 0) || (mVid.prot == "sss")) {
    {
      // Stall if the namespace is still booting
      XrdSysMutexHelper lock(gOFS->InitializationMutex);

      if (gOFS->Initialized != gOFS->kBooted) {
        XrdOucErrInfo errInfo;
        return gOFS->Stall(errInfo, 60, "Namespace is still booting");
      }
    }

    try {
      mSemaphore.Wait();
    } catch (...) {
      err = "Cannot take protecting semaphore, cannot dump md.";
      return EAGAIN;
    }

    std::string fsidst = std::to_string(dumpmdProto.fsid());
    XrdOucString option = dumpmdProto.display() == eos::console::FsProto::DumpMdProto::MONITOR ? "m" : "";
    XrdOucString dp = dumpmdProto.showpath() ? "1" : "0";
    XrdOucString df = dumpmdProto.showfid() ? "1" : "0";
    XrdOucString ds = dumpmdProto.showsize() ? "1" : "0";
    size_t entries = 0;

    auto tident = GetTident();

    try {
      retc = proc_fs_dumpmd(fsidst, option, dp, df, ds, stdOut, stdErr,
                            tident, mVid, entries);
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
    stdErr = "error: you have to take role 'root' or connect via 'sss' "
             "to execute this command";
  }

  out = stdOut.c_str();
  err = stdErr.c_str();

  return retc;
}

std::string
eos::mgm::FsCmd::DisplayModeToString(eos::console::FsProto::LsProto::DisplayMode mode) {
  switch (mode) {
    case eos::console::FsProto::LsProto::DisplayMode::FsProto_LsProto_DisplayMode_LONG:
      return "l";
    case eos::console::FsProto::LsProto::DisplayMode::FsProto_LsProto_DisplayMode_MONITOR:
      return "m";
    case eos::console::FsProto::LsProto::DisplayMode::FsProto_LsProto_DisplayMode_DRAIN:
      return "d";
    case eos::console::FsProto::LsProto::DisplayMode::FsProto_LsProto_DisplayMode_ERROR:
      return "e";
    case eos::console::FsProto::LsProto::DisplayMode::FsProto_LsProto_DisplayMode_FSCK:
      return "fsck";
    case eos::console::FsProto::LsProto::DisplayMode::FsProto_LsProto_DisplayMode_IO:
      return "io";
    default:
      return "";
  }
}

std::string
FsCmd::GetTident() {
  std::string tident = mVid.tident.c_str();
  size_t addpos = 0;

  if ((addpos = tident.find('@')) != std::string::npos) {
    tident.erase(0, addpos + 1);
  }

  return tident;
}

EOSMGMNAMESPACE_END
