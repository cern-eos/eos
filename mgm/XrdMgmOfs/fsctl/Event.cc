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

  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Nobody(vid);

  int retc = 0;

  if (auid)
  {
    vid.uid = strtoul(auid, 0, 10);
    vid.uid_string = eos::common::Mapping::UidToUserName(vid.uid, retc);
  }

  if (agid)
  {
    vid.gid = strtoul(agid, 0, 10);
    vid.gid_string = eos::common::Mapping::GidToGroupName(vid.gid, retc);
  }

  if (asec)
  {
    std::map<std::string, std::string> secmap = eos::common::SecEntity::KeyToMap(
      std::string(asec));
    vid.prot = secmap["prot"].c_str();
    vid.name = secmap["name"].c_str();
    vid.host = secmap["host"];
    vid.grps = secmap["grps"];
    vid.app  = secmap["app"];
  }

  if (alogid)
  {
    ThreadLogId.SetLogId(alogid, tident);
  }

  // check that we have write permission on path
  if (gOFS->_access(spath, W_OK | P_OK, error, vid, "")) {
    Emsg(epname, error, EPERM, "prepare - you don't have write and workflow permission", spath);
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
  int rc = workflow.Trigger(event, aworkflow, vid);

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
