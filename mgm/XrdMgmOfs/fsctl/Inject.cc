// ----------------------------------------------------------------------
// File: Inject.cc
// Author: Mihai Patrascoiu - CERN
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Inject");

  XrdOucString response;

  char* afsid = env.Get("mgm.inject.fsid");
  char* asize = env.Get("mgm.inject.size");
  char* extpath = env.Get("mgm.inject.extpath");
  char* lpath = env.Get("mgm.inject.lclpath");
  char* alogid = env.Get("mgm.logid");

  if (alogid) {
    ThreadLogId.SetLogId(alogid, tident);
  }

  if (afsid && extpath && lpath && asize) {
    eos_thread_info("injection for extpath=%s lclpath=%s "
                    "[fsid=%s, size=%s]", extpath, lpath, afsid, asize);

    unsigned long size = strtoull(asize, 0, 10);
    unsigned long fsid = strtoull(afsid, 0, 10);
    eos::IContainerMD::id_t cid;
    std::shared_ptr<eos::IFileMD> fmd;
    std::shared_ptr<eos::IContainerMD> cmd;

    try {
      // create new file entry
      fmd = gOFS->eosView->createFile(lpath, vid.uid, vid.gid);

      // retrieve container entry
      cid = fmd->getContainerId();
      cmd = gOFS->eosDirectoryService->getContainerMD(cid);
    } catch(eos::MDException& e) {
      std::string errmsg = e.getMessage().str();
      gOFS->MgmStats.Add("InjectFailedFmdCreate", 0, 0, 1);
      eos_thread_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                     e.getErrno(), errmsg.c_str());
      return Emsg(epname, error, errno, "create fmd", errmsg.c_str());
    }

    // obtain filesystem handler
    eos::mgm::FileSystem* filesystem = 0;

    if (FsView::gFsView.mIdView.count(fsid)) {
      filesystem = FsView::gFsView.mIdView[fsid];
    } else {
      eos_thread_err("msg=\"could not find filesystem fsid=%d\"", fsid);
      gOFS->MgmStats.Add("InjectFailedFsRetrieve", 0, 0, 1);
      return Emsg(epname, error, EIO, "retrieve filesystem", "");
    }

    // retrieve additional information for the new entry
    XrdOucString space;
    eos::IContainerMD::XAttrMap attrmap;
    unsigned long layoutId = 0;
    unsigned long forcedFsId = 0;
    long forcedGroup = -1;

    // create policy environment
    std::string schedgroup = filesystem->GetString("schedgroup");
    XrdOucString policyOpaque = "eos.space=";
    policyOpaque += schedgroup.c_str();
    XrdOucEnv policyEnv(policyOpaque.c_str());

    gOFS->_attr_ls(gOFS->eosView->getUri(cmd.get()).c_str(), error,
                   vid, 0, attrmap, false);
    // select space and layout according to policies
    Policy::GetLayoutAndSpace(lpath, attrmap, vid, layoutId, space,
                              policyEnv, forcedFsId, forcedGroup);

    // create logical path suffix
    XrdOucString lpathSuffix = extpath;
    std::string fsPrefix = filesystem->GetPath();
    if (lpathSuffix.beginswith(fsPrefix.c_str())) {
      lpathSuffix.erase(0, fsPrefix.length());
      if (!lpathSuffix.beginswith('/')) {
        lpathSuffix.insert('/', 0);
      }
    } else {
      eos_thread_err("could not determine filesystem prefix "
                     "in extpath=%s", extpath);
      gOFS->MgmStats.Add("InjectionFailedFsPrefix", 0, 0, 1);
      return Emsg(epname, error, errno, "match fs prefix", "");
  	}

    try {
      // set file entry parameters
      fmd->setFlags(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      fmd->setSize(size);
      fmd->addLocation(fsid);
      fmd->setLayoutId(layoutId);
      fmd->setAttribute("logicalpath", lpathSuffix.c_str());
      gOFS->eosView->updateFileStore(fmd.get());

      cmd->setMTimeNow();
      cmd->notifyMTimeChange(gOFS->eosDirectoryService);
      gOFS->eosView->updateContainerStore(cmd.get());
    } catch(eos::MDException& e) {
      std::string errmsg = e.getMessage().str();
      gOFS->MgmStats.Add("InjectFailedFmdUpdate", 0, 0, 1);
      eos_thread_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                     e.getErrno(), errmsg.c_str());
      return Emsg(epname, error, errno, "update fmd", errmsg.c_str());
    }

    // add file entry to quota
    eos::IQuotaNode* ns_quota = gOFS->eosView->getQuotaNode(cmd.get());
    if (ns_quota) {
      ns_quota->addFile(fmd.get());
    }

    // Construct response with necessary fmd data
    try {
      char buff[1024];
      snprintf(buff, sizeof(buff),
               "&fid=%lu&lid=%u&uid=%u&gid=%u",
               fmd->getId(), fmd->getLayoutId(),
               fmd->getCUid(), fmd->getCGid());
      response = buff;
    } catch (eos::MDException& e) {
      std::string errmsg = e.getMessage().str();
      gOFS->MgmStats.Add("InjectFailedResponse", 0, 0, 1);
      eos_thread_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                     e.getErrno(), errmsg.c_str());
      return Emsg(epname, error, errno, "create response", errmsg.c_str());
    }
  } else {
    int envlen = 0;
    eos_thread_err("inject message does not contain all meta information: %s",
                   env.Env(envlen));
    gOFS->MgmStats.Add("InjectFailedParameters", 0, 0, 1);
    XrdOucString filename = (extpath) ? extpath : "unknown";
    return Emsg(epname, error, EINVAL,
                "inject file - fsid, path, size not complete", filename.c_str());
  }

  gOFS->MgmStats.Add("Inject", 0, 0, 1);
  error.setErrInfo(response.length() + 1, response.c_str());
  EXEC_TIMING_END("Inject");
  return SFS_DATA;
}
