// ----------------------------------------------------------------------
// File: Import.cc
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

#include "common/Logging.hh"
#include "common/Path.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/utils/FsFilePath.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/FsView.hh"
#include "mgm/Policy.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Handle file importation into the namespace operation
//----------------------------------------------------------------------------
int
XrdMgmOfs::Import(const char* path,
                  const char* ininfo,
                  XrdOucEnv& env,
                  XrdOucErrInfo& error,
                  eos::common::LogId& ThreadLogId,
                  eos::common::Mapping::VirtualIdentity& vid,
                  const XrdSecEntity* client)
{
  static const char* epname = "Import";

  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Import");

  char* id = env.Get("mgm.import.id");
  char* alogid = env.Get("mgm.logid");

  if (alogid) {
    ThreadLogId.SetLogId(alogid, error.getErrUser());
  }

  if (!id) {
    int envlen = 0;
    eos_thread_err("import message does not contain an id: %s", env.Env(envlen));
    return Emsg(epname, error, EINVAL, "retrieve import id [EINVAL]");
  }

  // ---------------------------------------------------------------------
  // Import messages can be of following type: status or file import
  //
  // Import message type: status
  // ---------------------------------------------------------------------

  XrdOucString status = env.Get("mgm.import.status");

  if (status.length()) {
    char* atimestamp = env.Get("mgm.import.status.timestamp");

    if (atimestamp) {
      time_t timestamp = (time_t) atoll(atimestamp);
      eos::mgm::ImportStatus* importStatus = 0;

      eos::common::RWMutexWriteLock wlock(FsView::gFsView.ViewMutex);

      if (status == "start") {
        // Register new import operation into FsView map
        importStatus = new ImportStatus(id, timestamp);
        FsView::gFsView.mImportView[id] = importStatus;
      } else if (status == "end") {
        // Retrieve ImportStatus object
        if (FsView::gFsView.mImportView.count(id)) {
          importStatus = FsView::gFsView.mImportView[id];

          // Calculate time elapsed
          char stime_elapsed[128];
          time_t elapsed = timestamp - importStatus->mTimestamp;
          strftime(stime_elapsed, 127, "%Hh:%Mm:%Ss", gmtime(&elapsed));

          eos_thread_info("import[id=%s] finished "
                          "imported=%ld failed=%ld time_elapsed=%s",
                          id, importStatus->GetImported(),
                          importStatus->GetFailed(),
                          stime_elapsed);

          // Unregister import operation from FsView map
          FsView::gFsView.mImportView.erase(id);
        } else {
          eos_thread_err("import[id=%s] msg=\"cannot find import status object\"",
                         id);
          gOFS->MgmStats.Add("ImportFailedStatusRetrieve", 0, 0, 1);
          return Emsg(epname, error, EBADR,
                      "retrieve import status object [EBADR]", id);
        }
      }

      error.setErrInfo(0, "");
      return SFS_DATA;
    } else {
      int envlen = 0;
      eos_thread_err("import[id=%s] message does not contain all metadata: %s",
                     id, env.Env(envlen));
      gOFS->MgmStats.Add("ImportFailedStatus", 0, 0, 1);
      return Emsg(epname, error, EINVAL,
                  "process import status - message incomplete [EINVAL]",
                  status.c_str());
    }
  }

  // ---------------------------------------------------------------------
  // Import message type: import file
  // ---------------------------------------------------------------------

  char* afsid = env.Get("mgm.import.fsid");
  char* asize = env.Get("mgm.import.size");
  char* extpath = env.Get("mgm.import.extpath");
  char* lpath = env.Get("mgm.import.lclpath");

  XrdOucString response;

  eos::common::RWMutexReadLock rlock(FsView::gFsView.ViewMutex);
  eos::mgm::ImportStatus* importStatus = 0;

  // Retrieve ImportStatus object
  if (FsView::gFsView.mImportView.count(id)) {
    importStatus = FsView::gFsView.mImportView[id];
  } else {
    eos_thread_err("import[id=%s] msg=\"cannot find import status object\"",
                   id);
    gOFS->MgmStats.Add("ImportFailedStatusRetrieve", 0, 0, 1);
    return Emsg(epname, error, EBADR,
                "retrieve import status object [EBADR]", id);
  }

  if (afsid && asize && extpath && lpath) {
    eos_thread_info("import[id=%s] fsid=%s size=%s extpath=%s lclpath=%s",
                    id, afsid, asize, extpath, lpath);

    unsigned long size = strtoull(asize, 0, 10);
    unsigned long fsid = strtoull(afsid, 0, 10);
    eos::IContainerMD::id_t cid;
    std::shared_ptr<eos::IFileMD> fmd;
    std::shared_ptr<eos::IContainerMD> cmd;
    eos::mgm::FileSystem* filesystem = 0;

    // Attempt to create full path if necessary
    XrdSfsFileExistence file_exists;
    eos::common::Path cPath(lpath);

    int rc = gOFS->_exists(cPath.GetParentPath(), file_exists, error, vid);

    if (!rc) {
      // parent path must either do not exist or be a directory
      if ((file_exists != XrdSfsFileExistNo) &&
          (file_exists != XrdSfsFileExistIsDirectory)) {
        importStatus->IncrementFailed();
        gOFS->MgmStats.Add("ImportFailedParentPathNotDir", 0, 0, 1);
        return Emsg(epname, error, ENOTDIR,
                    "import file - parent path is not a directory [ENOTDIR]",
                    cPath.GetParentPath());
        }

      // Create parent path if it does not exist
      if (file_exists == XrdSfsFileExistNo) {
        mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        rc = gOFS->_mkdir(cPath.GetParentPath(), mode, error, vid);

        if (rc) {
          importStatus->IncrementFailed();
          gOFS->MgmStats.Add("ImportFailedMkdir", 0, 0, 1);
          return Emsg(epname, error, errno, "create parent path",
                      cPath.GetParentPath());
        }
      }
    } else {
      importStatus->IncrementFailed();
      gOFS->MgmStats.Add("ImportFailedParentPathCheck", 0, 0, 1);
      return Emsg(epname, error, errno, "check if parent path exists",
                  cPath.GetParentPath());
    }

    // Obtain filesystem handler
    if (FsView::gFsView.mIdView.count(fsid)) {
      filesystem = FsView::gFsView.mIdView[fsid];
    } else {
      eos_thread_err("import[id=%s] msg=\"could not find filesystem fsid=%d\"",
                     id, fsid);
      importStatus->IncrementFailed();
      gOFS->MgmStats.Add("ImportFailedFsRetrieve", 0, 0, 1);
      return Emsg(epname, error, EBADR, "retrieve filesystem [EBADR]",
                  std::to_string(fsid).c_str());
    }

    // Create logical path suffix
    XrdOucString lpathSuffix = extpath;
    std::string fsPrefix = filesystem->GetPath();
    if (lpathSuffix.beginswith(fsPrefix.c_str())) {
      lpathSuffix.erase(0, fsPrefix.length());
      if (!lpathSuffix.beginswith('/')) {
        lpathSuffix.insert('/', 0);
      }
    } else {
      eos_thread_err("import[id=%s] could not determine filesystem prefix "
                     "in extpath=%s", extpath);
      importStatus->IncrementFailed();
      gOFS->MgmStats.Add("ImportFailedFsPrefix", 0, 0, 1);
      return Emsg(epname, error, EBADE, "match fs prefix [EBADE]",
                  fsPrefix.c_str());
    }

    {
      // Create new file entry
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

      try {
        fmd = gOFS->eosView->createFile(lpath, vid.uid, vid.gid);

        // Retrieve container entry
        cid = fmd->getContainerId();
        cmd = gOFS->eosDirectoryService->getContainerMD(cid);
      } catch(eos::MDException& e) {
        std::string errmsg = e.getMessage().str();
        importStatus->IncrementFailed();
        gOFS->MgmStats.Add("ImportFailedFmdCreate", 0, 0, 1);
        eos_thread_err("import[id=%s] msg=\"exception\" ec=%d emsg=\"%s\"",
                       id, e.getErrno(), errmsg.c_str());
        if (e.getErrno() == EEXIST) {
          return Emsg(epname, error, EEXIST, "create fmd [EEXIST]", lpath);
        } else {
          return Emsg(epname, error, e.getErrno(), "create fmd", lpath);
        }
      }
    }

    // Policy environment setup
    XrdOucString space;
    eos::IContainerMD::XAttrMap attrmap;
    unsigned long layoutId = 0;
    unsigned long forcedFsId = 0;
    long forcedGroup = -1;

    {
      // Create policy environment
      eos::common::RWMutexReadLock ns_read_lock(gOFS->eosViewRWMutex);
      std::string schedgroup = filesystem->GetString("schedgroup");
      XrdOucString policyOpaque = "eos.space=";
      policyOpaque += schedgroup.c_str();
      XrdOucEnv policyEnv(policyOpaque.c_str());

      gOFS->_attr_ls(gOFS->eosView->getUri(cmd.get()).c_str(), error,
                     vid, 0, attrmap, false);
      // Select space and layout according to policies
      Policy::GetLayoutAndSpace(lpath, attrmap, vid, layoutId, space,
                                policyEnv, forcedFsId, forcedGroup);
    }

    {
      // Update the new file entry
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

      try {
        // Set file entry parameters
        fmd->setFlags(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        fmd->setSize(size);
        fmd->addLocation(fsid);
        fmd->setLayoutId(layoutId);
        eos::FsFilePath::StorePhysicalPath(fsid, fmd, lpathSuffix.c_str());
        gOFS->eosView->updateFileStore(fmd.get());

        cmd->setMTimeNow();
        cmd->notifyMTimeChange(gOFS->eosDirectoryService);
        gOFS->eosView->updateContainerStore(cmd.get());

        // Add file entry to quota
        eos::IQuotaNode* ns_quota = gOFS->eosView->getQuotaNode(cmd.get());
        if (ns_quota) {
          ns_quota->addFile(fmd.get());
        }
      } catch(eos::MDException& e) {
        std::string errmsg = e.getMessage().str();
        importStatus->IncrementFailed();
        gOFS->MgmStats.Add("ImportFailedFmdUpdate", 0, 0, 1);
        eos_thread_err("import[id=%s] msg=\"exception\" ec=%d emsg=\"%s\"",
                       id, e.getErrno(), errmsg.c_str());
        return Emsg(epname, error, e.getErrno(), "update fmd",
                    errmsg.c_str());
      }
    }

    // Construct response with file metadata
    std::string fmdEnv = "";
    fmd->getEnv(fmdEnv, true);
    response = fmdEnv.c_str();

    // Empty values will be ignored in XrdOucEnv creation
    if ((response.find("checksum=&")) != STR_NPOS ||
        response.endswith("checksum=")) {
      response.replace("checksum=", "checksum=none");
    }
  } else {
    int envlen = 0;
    eos_thread_err("import[id=%s] message does not contain all metadata: %s",
                   id, env.Env(envlen));
    importStatus->IncrementFailed();
    gOFS->MgmStats.Add("ImportFailedParameters", 0, 0, 1);
    XrdOucString filename = (extpath) ? extpath : "unknown";
    return Emsg(epname, error, EINVAL,
                "import file - id, fsid, path, size not complete [EINVAL]",
                filename.c_str());
  }

  importStatus->IncrementImported();
  gOFS->MgmStats.Add("Import", 0, 0, 1);
  error.setErrInfo(response.length() + 1, response.c_str());
  EXEC_TIMING_END("Import");
  return SFS_DATA;
}
