// ----------------------------------------------------------------------
// File: Event.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/********************A***************************************************
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  char* spath = env.Get("mgm.path");
  char* afid = env.Get("mgm.fid");
  char* alogid = env.Get("mgm.logid");
  char* aevent = env.Get("mgm.event");
  char* aworkflow = env.Get("mgm.workflow");
  char* auid = env.Get("mgm.ruid");
  char* agid = env.Get("mgm.rgid");
  char* asec = env.Get("mgm.sec");

  eos::common::Mapping::VirtualIdentity localVid;
  eos::common::Mapping::Nobody(localVid);

  int retc = 0;

  if (auid)
  {
    localVid.uid = strtoul(auid, 0, 10);
    localVid.uid_string = eos::common::Mapping::UidToUserName(localVid.uid, retc);
  }

  if (agid)
  {
    localVid.gid = strtoul(agid, 0, 10);
    localVid.gid_string = eos::common::Mapping::GidToGroupName(localVid.gid, retc);
  }

  if (asec)
  {
    std::map<std::string, std::string> secmap = eos::common::SecEntity::KeyToMap(
      std::string(asec));
    localVid.prot = secmap["prot"].c_str();
    localVid.name = secmap["name"].c_str();
    localVid.host = secmap["host"];
    localVid.grps = secmap["grps"];
    localVid.app  = secmap["app"];
  }

  if (alogid)
  {
    ThreadLogId.SetLogId(alogid, tident);
  }

  bool isPrepare = std::string(aevent).find("prepare") != std::string::npos;

  // check that we have write permission on path
  eos_debug("vid.prot=%s, vid.uid=%ull, vid.gid=%ull", vid.prot, vid.uid, vid.gid);
  eos_debug("local.prot=%s, local.uid=%ull, local.gid=%ull", localVid.prot, localVid.uid, localVid.gid);
  if (vid.prot != "sss" &&
      gOFS->_access(spath, isPrepare ? W_OK | P_OK : W_OK, error, localVid, "")) {
    Emsg(epname, error, EPERM,
         isPrepare ? "event - you don't have write and prepare permissions" : "event - you don't have write permission",
         spath);
    return SFS_ERROR;
  }

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Event");

  gOFS->MgmStats.Add("Event", 0, 0, 1);

  int envlen = 0;

  if (!spath || !afid || !alogid || !aevent)
  {
    return Emsg(epname, error, EINVAL, "notify - invalid parameters for event call",
    env.Env(envlen));
  }

  eos_thread_info("subcmd=event event=%s path=%s fid=%s",
  aevent,
  spath,
  afid);

  unsigned long long fid = strtoull(afid, 0, 16);

  std::string event = aevent;

  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> dh;

  Workflow workflow;
  eos::IContainerMD::XAttrMap attr;

  XrdOucString lWorkflow = aworkflow;

  if (lWorkflow.beginswith("eos."))
  {
    // this is a templated workflow defined on the workflow proc directory
    fid = 0;
    spath = (char*)gOFS->MgmProcWorkflowPath.c_str();
  }

  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);

    try
    {
      if (fid)
      {
        fmd = gOFS->eosFileService->getFileMD(fid);
      } else
      {
        fmd = gOFS->eosView->getFile(spath);
        fid = fmd->getId();
      }

      dh = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      eos::IFileMD::XAttrMap xattrs = dh->getAttributes();

      for (const auto& elem : xattrs)
      {
        attr[elem.first] = elem.second;
      }

      // check for attribute references
      if (attr.count("sys.attr.link"))
      {
        try {
          dh = gOFS->eosView->getContainer(attr["sys.attr.link"]);
          eos::IFileMD::XAttrMap xattrs = dh->getAttributes();

          for (const auto& elem : xattrs) {
            XrdOucString key = elem.first.c_str();

            if (!attr.count(elem.first)) {
              attr[key.c_str()] = elem.second;
            }
          }
        } catch (eos::MDException& e) {
          dh.reset();
          errno = e.getErrno();
          eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                    e.getMessage().str().c_str());
        }

        attr.erase("sys.attr.link");
      }
    } catch (eos::MDException& e)
    {
      errno = e.getErrno();
      eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
    }
  }

  std::string path = spath;
  // load the corresponding workflow
  workflow.Init(&attr, path, fid);

  // trigger the specified event
  int rc = workflow.Trigger(event, aworkflow, localVid);

  if (rc == -1)
  {
    if (errno == ENOKEY) {
      // there is no workflow defined
      return Emsg(epname, error, EINVAL, "trigger workflow - there is no workflow defined for this <workflow>.<event>",
		  env.Env(envlen));
    }
    else
    {
      if (!workflow.IsSync())
	return Emsg(epname, error, EIO, "trigger workflow - internal error",
		    env.Env(envlen));
      else
	return Emsg(epname, error, errno, "trigger workflow - synchronous workflow failed",
		    env.Env(envlen));
    }
  }

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Event");
  return SFS_DATA;
}
