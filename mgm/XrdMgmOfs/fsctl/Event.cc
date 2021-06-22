// ----------------------------------------------------------------------
// File: Event.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "common/SecEntity.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/Acl.hh"
#include "mgm/Workflow.hh"
#include "mgm/FsView.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Trigger an event
//----------------------------------------------------------------------------
int
XrdMgmOfs::Event(const char* path,
                 const char* ininfo,
                 XrdOucEnv& env,
                 XrdOucErrInfo& error,
                 eos::common::VirtualIdentity& vid,
                 const XrdSecEntity* client)
{
  static const char* epname = "Event";
  char* auid = env.Get("mgm.ruid");
  char* agid = env.Get("mgm.rgid");
  char* asec = env.Get("mgm.sec");
  char* alogid = env.Get("mgm.logid");
  char* spath = env.Get("mgm.path");
  char* afid = env.Get("mgm.fid");
  char* aevent = env.Get("mgm.event");
  char* aworkflow = env.Get("mgm.workflow");
  char* errmsg = env.Get("mgm.errmsg");
  eos::common::VirtualIdentity localVid = eos::common::VirtualIdentity::Nobody();
  int errc;

  if (auid) {
    localVid.uid = strtoul(auid, 0, 10);
    localVid.uid_string = eos::common::Mapping::UidToUserName(localVid.uid, errc);
  }

  if (agid) {
    localVid.gid = strtoul(agid, 0, 10);
    localVid.gid_string = eos::common::Mapping::GidToGroupName(localVid.gid, errc);
    localVid.allowed_gids = vid.allowed_gids;
  }

  if (asec) {
    std::map<std::string, std::string> secmap =
      eos::common::SecEntity::KeyToMap(std::string(asec));
    localVid.prot = secmap["prot"].c_str();
    localVid.name = secmap["name"].c_str();
    localVid.host = secmap["host"];
    localVid.grps = secmap["grps"];
    localVid.app  = secmap["app"];
  }

  if (alogid) {
    tlLogId.SetLogId(alogid, error.getErrUser());
  }

  eos_thread_debug("vid.prot=%s, vid.uid=%u, vid.gid=%u",
                   vid.prot.c_str(), vid.uid, vid.gid);
  eos_thread_debug("local.prot=%s, local.uid=%u, local.gid=%u",
                   localVid.prot.c_str(), localVid.uid, localVid.gid);
  // Assuming that all workflow actions accept for prepare can modify a file,
  // check that we have either write or prepare permission as necessary on path
  bool isPrepare = (aevent != nullptr
                    && std::string(aevent).find("prepare") != std::string::npos);
  const int mode = isPrepare  ?  P_OK  :  W_OK;

  if (vid.prot != "sss" && gOFS->_access(spath, mode, error, localVid, "")) {
    const char* emsg =
      isPrepare ? "event - you don't have prepare permissions [EPERM]"
      : "event - you don't have write permission [EPERM]";
    return Emsg(epname, error, EPERM, emsg, spath);
  }

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Event");
  gOFS->MgmStats.Add("Event", 0, 0, 1);

  if (spath && afid && aevent && aworkflow) {
    eos_thread_info("subcmd=event event=%s path=%s fid=%s",
                    aevent, spath, afid);
    unsigned long long fid = strtoull(afid, 0, 16);
    std::string event = aevent;
    std::shared_ptr<eos::IFileMD> fmd;
    std::shared_ptr<eos::IContainerMD> cmd;
    Workflow workflow;
    eos::IContainerMD::XAttrMap attrmap;
    XrdOucString lWorkflow = aworkflow;

    if (lWorkflow.beginswith("eos.")) {
      // Template workflow defined under the workflow proc directory
      spath = (char*) gOFS->MgmProcWorkflowPath.c_str();
      fid = 0;
    }

    {
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);

      try {
        if (fid) {
          fmd = gOFS->eosFileService->getFileMD(fid);
        } else {
          fmd = gOFS->eosView->getFile(spath);
          fid = fmd->getId();
        }

        cmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
        eos::IFileMD::XAttrMap xattrs = cmd->getAttributes();

        for (const auto& elem : xattrs) {
          attrmap[elem.first] = elem.second;
        }

        // Check for attribute references
        if (attrmap.count("sys.attr.link")) {
          try {
            cmd = gOFS->eosView->getContainer(attrmap["sys.attr.link"]);
            eos::IFileMD::XAttrMap xattrs = cmd->getAttributes();

            for (const auto& elem : xattrs) {
              if (!attrmap.count(elem.first)) {
                attrmap[elem.first] = elem.second;
              }
            }
          } catch (eos::MDException& e) {
            cmd.reset();
            errno = e.getErrno();
            eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                             e.getErrno(), e.getMessage().str().c_str());
          }

          attrmap.erase("sys.attr.link");
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                         e.getErrno(), e.getMessage().str().c_str());
      }
    }

    // Load the corresponding workflow
    std::string strpath = spath;
    workflow.Init(&attrmap, strpath, fid);
    std::string decodedErrMessage =
      "trigger workflow - synchronous workflow failed";

    if (errmsg != nullptr) {
      if (!eos::common::SymKey::Base64Decode(errmsg, decodedErrMessage)) {
        decodedErrMessage = "";
      }
    }

    // Trigger the specified event
    const int rc = workflow.Trigger(event, aworkflow, localVid, ininfo,
                                    decodedErrMessage);

    if (rc == -1) {
      int envlen = 0;

      if (errno == ENOKEY) {
        // No workflow defined
        return Emsg(epname, error, EINVAL,
                    "trigger workflow - no workflow defined for"
                    " <workflow>.<event> [EINVAL]",
                    env.Env(envlen));
      } else {
        if (!workflow.IsSync()) {
          return Emsg(epname, error, EIO, "trigger workflow - internal error [EIO]",
                      env.Env(envlen));
        } else {
          return Emsg(epname, error, errno, decodedErrMessage.c_str(),
                      env.Env(envlen));
        }
      }
    }

    if (rc != 0) {
      std::ostringstream errStr;
      errStr << "complete workflow - error while executing "
             << event << " workflow [" << MacroStringError(rc) << "]";

      if (decodedErrMessage.empty()) {
        return Emsg(epname, error, rc, errStr.str().c_str(), spath);
      } else {
        return Emsg(epname, error, rc, decodedErrMessage.c_str(), spath);
      }
    }
  } else {
    int envlen = 0;
    const char* env_string = env.Env(envlen);
    eos_thread_err("invalid parameters for event call: %s", env_string);
    return Emsg(epname, error, EINVAL,
                "notify - invalid parameters for event call: %s [EINVAL]",
                env_string);
  }

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Event");
  return SFS_DATA;
}
