// ----------------------------------------------------------------------
// File: Drop.cc
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
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Drop a replica
//----------------------------------------------------------------------------
int
XrdMgmOfs::Drop(const char* path,
                const char* ininfo,
                XrdOucEnv& env,
                XrdOucErrInfo& error,
                eos::common::VirtualIdentity& vid,
                const XrdSecEntity* client)
{
  static const char* epname = "Drop";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Drop");
  int envlen;
  eos_thread_info("drop request for %s", env.Env(envlen));
  char* afid = env.Get("mgm.fid");
  char* afsid = env.Get("mgm.fsid");

  if (afid && afsid) {
    eos::IFileMD::id_t fid = eos::common::FileId::Hex2Fid(afid);
    unsigned long fsid = strtoul(afsid, 0, 10);
    std::shared_ptr<eos::IContainerMD> container;
    std::shared_ptr<eos::IFileMD> fmd;
    eos::IQuotaNode* ns_quota = nullptr;
    eos::Prefetcher::prefetchFilesystemFileListAndWait(gOFS->eosView,
        gOFS->eosFsView, fsid);
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
    eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex, __FUNCTION__,
        __LINE__, __FILE__);

    try {
      fmd = eosFileService->getFileMD(fid);
    } catch (...) {
      eos_thread_warning("msg=\"no meta record exists anymore\" fxid=%s", afid);
      // Nevertheless drop the file identifier from the file system view
      gOFS->eosFsView->eraseEntry(fsid, fid);
    }

    if (fmd) {
      try {
        container =
          gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      } catch (eos::MDException& e) {}

      if (container) {
        try {
          ns_quota = gOFS->eosView->getQuotaNode(container.get());
        } catch (eos::MDException& e) {
          ns_quota = nullptr;
        }
      }

      try {
        std::vector<unsigned int> drop_fsid;
        bool updatestore = false;
        // If mgm.dropall flag is set then it means we got a deleteOnClose
        // at the gateway node and we need to delete all replicas
        char* drop_all = env.Get("mgm.dropall");

        if (drop_all) {
          for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
            drop_fsid.push_back(fmd->getLocation(i));
          }
        } else {
          drop_fsid.push_back(fsid);
        }

        // Drop the selected replicas
        for (const auto& id : drop_fsid) {
          eos_thread_debug("msg=\"remove location\" fxid=%s fsid=%lu", afid, id);
          updatestore = false;

          if (fmd->hasLocation(id)) {
            fmd->unlinkLocation(id);
            updatestore = true;
          }

          if (fmd->hasUnlinkedLocation(id)) {
            fmd->removeLocation(id);
            updatestore = true;
          }

          if (updatestore) {
            gOFS->eosView->updateFileStore(fmd.get());
            // After update we might have to get the new address
            fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
          } else {
            // The FileSystem view has a reference for this file but the file
            // has no replicas registered on the current file system - we need
            // to force delete the entry from the FileSystem view
            gOFS->eosFsView->eraseEntry(id, fid);
          }
        }

        // Delete the record only if all replicas are dropped
        if ((!fmd->getNumUnlinkedLocation()) && (!fmd->getNumLocation())
            && (drop_all || updatestore)) {
          // However we should only remove the file from the namespace, if
          // there was indeed a replica to be dropped, otherwise we get
          // unlinked files if the secondary replica fails to write but
          // the machine can call the MGM
          if (ns_quota) {
            // If we were still attached to a container, we can now detach
            // and count the file as removed
            ns_quota->removeFile(fmd.get());
          }

          gOFS->eosView->removeFile(fmd.get());

          if (container) {
            container->setMTimeNow();
            gOFS->eosView->updateContainerStore(container.get());
            container->notifyMTimeChange(gOFS->eosDirectoryService);
            eos::ContainerIdentifier container_id = container->getIdentifier();
            eos::ContainerIdentifier container_pid = container->getParentIdentifier();
            ns_wr_lock.Release();
            gOFS->FuseXCastContainer(container_id);
            gOFS->FuseXCastRefresh(container_id, container_pid);
          }
        }
      } catch (...) {
        eos_thread_warning("no meta record exists anymore for fxid=%s", afid);
      }
    }
  } else {
    eos_thread_err("drop message does not contain all meta information: %s",
                   env.Env(envlen));
    return Emsg(epname, error, EIO, "drop replica [EIO]",
                "missing meta information");
  }

  gOFS->MgmStats.Add("Drop", vid.uid, vid.gid, 1);
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Drop");
  return SFS_DATA;
}
