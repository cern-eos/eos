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
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Event");

  gOFS->MgmStats.Add("Event", 0, 0, 1);

  char* spath = env.Get("mgm.path");
  char* afid = env.Get("mgm.fid");
  char* alogid = env.Get("mgm.logid");
  char* aevent = env.Get("mgm.event");
  char* aworkflow = env.Get("mgm.workflow");

  if (alogid)
  {
    ThreadLogId.SetLogId(alogid, tident);
  }

  if (!spath || !afid || !alogid || !aevent)
  {
    int envlen = 0;
    return Emsg(epname, error, EINVAL, "notify - invalid parameters for event call",
                env.Env(envlen));
  }

  eos_thread_info("subcmd=event event=%s path=%s fid=%s",
                  aevent,
                  spath,
                  afid);

  unsigned long long fid = strtoull(afid, 0, 16);

  std::string event = aevent;

  eos::FileMD* fmd = 0;
  eos::ContainerMD* dh = 0;
  Workflow workflow;
  eos::ContainerMD::XAttrMap attr;

  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    try
    {
      fmd = gOFS->eosFileService->getFileMD(fid);
      dh = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
      {
        attr[it->first] = it->second;
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
    }
  }

  // load the corresponding workflow
  workflow.Init(&attr);

  // trigger the specified event
  workflow.Trigger(event, aworkflow);

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Event");
  return SFS_DATA;
}
