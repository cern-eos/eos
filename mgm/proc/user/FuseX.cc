// ----------------------------------------------------------------------
// File: proc/user/FuseX.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "XrdOuc/XrdOucEnv.hh"
#include "common/SymKeys.hh"
#include "mgm/ZMQ.hh"
#include "mgm/FuseServer/Server.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "mgm/Stat.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"

EOSMGMNAMESPACE_BEGIN


#define FUSEXMAXCHILDREN 64

int
ProcCommand::FuseX()
{
  ACCESSMODE_R;
  FUNCTIONMAYSTALL("Eosxd::prot::LS", *pVid, *mError);
  { FUNCTIONMAYSTALL("Eosxd::ext::LS", *pVid, *mError);}
  { FUNCTIONMAYSTALL("Eosxd::ext::LS-Entry", *pVid, *mError);}
  gOFS->MgmStats.Add("Eosxd::prot::LS", pVid->uid, pVid->gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::prot::LS");
  // -------------------------------------------------------------------------------------------------------
  // This function returns meta data by inode or if provided first translates a path into an inode.
  // The client can provide the meta-data clock. If it is equivalent to the stored clock, this function
  // return EEXIST and no result stream.
  // If a path cannot be translated the function returns ENOENT or a relevant errno for namespace failures.
  // If mgm.op is equal to 'GETCAP' it does not return meta data but a capability.
  // -------------------------------------------------------------------------------------------------------
  XrdOucString sinode = pOpaque->Get("mgm.inode") ? pOpaque->Get("mgm.inode") :
                        "0";
  XrdOucString sclock = pOpaque->Get("mgm.clock") ? pOpaque->Get("mgm.clock") :
                        "0";
  XrdOucString spath  = pOpaque->Get("mgm.path") ? pOpaque->Get("mgm.path") : "";
  XrdOucString schild = pOpaque->Get("mgm.child") ? pOpaque->Get("mgm.child") :
                        "";
  XrdOucString sop    = pOpaque->Get("mgm.op") ? pOpaque->Get("mgm.op") : "GET";
  XrdOucString suuid  = pOpaque->Get("mgm.uuid") ? pOpaque->Get("mgm.uuid") : "";
  XrdOucString cid    = pOpaque->Get("mgm.cid") ? pOpaque->Get("mgm.cid") : "";
  XrdOucString authid = pOpaque->Get("mgm.authid") ? pOpaque->Get("mgm.authid") :
                        "";
  bool inlined = pOpaque->Get("mgm.inline") ? true : false; // clients supports inlined responses in error messages

  if (spath.length()) {
    // decode escaped path name
    spath = eos::common::StringConversion::curl_unescaped(spath.c_str()).c_str();
  }

  const char* inpath = spath.length() ? spath.c_str() : sinode.c_str();
  uint64_t inode = strtoull(sinode.c_str(), 0, 16);
  uint64_t clock = strtoull(sclock.c_str(), 0, 10);
  uint64_t parentinode = 0;

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("vid(%d,%d,%s)", vid.uid, vid.gid, vid.host.c_str());
  }

  PROC_BOUNCE_NOT_ALLOWED;
  eos::fusex::md md;
  md.set_clientuuid(suuid.c_str());
  md.set_clientid(cid.c_str());
  md.set_authid(authid.c_str());
  errno = 0;

  if (spath.length()) {
    // translate spath into an inode number
    std::shared_ptr<eos::IFileMD> fmd;
    std::shared_ptr<eos::IContainerMD> cmd;

    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, spath.c_str());

    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    std::string emsg;

    try {
      fmd = gOFS->eosView->getFile(spath.c_str(), true);
      inode = eos::common::FileId::FidToInode(fmd->getId());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      emsg = e.getMessage().str().c_str();
    }

    if (!fmd) {
      errno = 0;

      lock.Release();

      if (schild.length()) {
	eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView, spath.c_str());
      } else {
	eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, spath.c_str());
      }

      lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

      try {
        cmd = gOFS->eosView->getContainer(spath.c_str(), true);
        inode = cmd->getId();
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        emsg = e.getMessage().str().c_str();
      }
    }

    if (errno) {
      if (errno != ENOENT) {
        eos_err("msg=\"exception\" ec=%d emsg=\"%s\"",
                errno, emsg.c_str());
      } else {
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                  errno, emsg.c_str());
      }

      return gOFS->Emsg("FuseX", *mError, errno, "get-if-clock",
                        emsg.c_str());
    }
  }

  if (schild.length()) {
    // decode escaped child name
    schild = eos::common::StringConversion::curl_unescaped(schild.c_str()).c_str();
    std::shared_ptr<eos::IContainerMD> cmd;
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    std::string emsg;

    // lookup by parent dir + name
    try {
      cmd = gOFS->eosDirectoryService->getContainerMD(inode);
      inode = 0;

      if ((cmd->getNumContainers() + cmd->getNumFiles()) < FUSEXMAXCHILDREN) {
        parentinode = inode;
      }

      std::shared_ptr<eos::IFileMD> fmd = cmd->findFile(schild.c_str());

      if (fmd) {
        inode = eos::common::FileId::FidToInode(fmd->getId());
      } else {
        std::shared_ptr<eos::IContainerMD> ccmd =
          cmd->findContainer(schild.c_str());

        if (ccmd) {
          inode = ccmd->getId();
        }
      }

      if (!inode) {
        errno = ENOENT;
        emsg = schild.c_str();
        emsg += " - no such file or directory";
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      emsg = e.getMessage().str().c_str();
    }

    if (errno) {
      if (errno != ENOENT) {
        eos_err("msg=\"exception\" ec=%d emsg=\"%s\"",
                errno, emsg.c_str());
      } else {
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                  errno, emsg.c_str());
      }

      return gOFS->Emsg("FuseX", *mError, errno, "get-if-clock",
                        emsg.c_str());
    }
  }

  uint64_t md_clock = 0;
  md.set_md_ino(inode);

  if (parentinode) {
    // if we have a small response, we return the listing of the parent instead
    // the 'name' MD only, that saves us future roundtrips
    if (sop == "GET") {
      md.set_operation(md.LS);
      md.set_md_ino(parentinode);
    }
  } else {
    if (sop == "GET") {
      md.set_operation(md.GET);
    }

    if (sop == "LS") {
      md.set_operation(md.LS);
    }

    if (sop == "GETCAP") {
      md.set_operation(md.GETCAP);
    }
  }

  if (clock) {
    // if a clock is given, we only retrieve the MD clock without calling the FillXXX functions
    if (!eos::common::FileId::IsFileInode(md.md_ino())) {
      try {
        uint64_t md_clock;
        gOFS->eosDirectoryService->getContainerMD(md.md_ino(), &md_clock);
      } catch (eos::MDException& e) {
        try {
          gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(inode),
                                          &md_clock);
        } catch (eos::MDException& e) {
          return gOFS->Emsg("FuseX", *mError, e.getErrno(),
                            e.getMessage().str().c_str());
        }
      }
    } else {
    }

    if (EOS_LOGS_DEBUG) {
      eos_debug("c1=%llu c2=%llu", md_clock, clock);
    }

    if ((sop == "GET") || (sop == "LS")) {
      if (md_clock == clock) {
        // if the given clock is ok, we return EEXIST
        return gOFS->Emsg("FuseX", *mError, EEXIST, "get-if-clock", inpath);
      }
    }
  }

  std::string result;
  std::string id = std::string("Fusex::sync:") + vid.tident.c_str();
  mResultStream = "";
  int rc = gOFS->zMQ->gFuseServer.HandleMD(id, md, *pVid, &result, &md_clock);

  if (rc) {
    return gOFS->Emsg("FuseX", *mError, rc, "handle request", "");
  }

  if (EOS_LOGS_DEBUG) {
    eos_debug("c1=%llu c2=%llu", md_clock, clock);
  }

  if (sop == "GETCAP") {
    // check clock synchronization
    // the client is supposed to send his current time when requesting a CAP
    time_t now = time(NULL);

    if ((uint64_t)now > clock + 2) {
      eos_err("client-clock %lu %s server-clock %lu", clock, sclock.c_str(), now);
      return gOFS->Emsg("FuseX", *mError, EL2NSYNC, "get-cap-clock-out-of-sync",
                        inpath);
    }
  }

  if (inlined) {
    if (result.size() < 2048) {
      if (EOS_LOGS_DEBUG) {
	eos_debug("returning in-line result - len=%u", result.size());
      }
      
      std::string b64response;
      eos::common::SymKey::Base64(result, b64response);
      
      XrdOucBuffer* buff = new XrdOucBuffer(strndup(b64response.c_str(), b64response.size()), b64response.size());
      mError->setErrInfo(ECANCELED, buff);
      return SFS_ERROR;
    }
  }
  
  mResultStream = result;

  if (EOS_LOGS_DEBUG)
    eos_debug("returning resultstream len=%u %s", mResultStream.size(),
              mResultStream.c_str());

  mLen = mResultStream.size();

  if (EOS_LOGS_DEBUG)
    eos_debug("result-dump=%s",
              eos::common::StringConversion::string_to_hex(result).c_str());

  EXEC_TIMING_END("Eosxd::prot::LS");


  return SFS_OK;
}

EOSMGMNAMESPACE_END
