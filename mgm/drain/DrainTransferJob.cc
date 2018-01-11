//------------------------------------------------------------------------------
// @file DrainTransferJob.cc
// @author Andrea Manzi - CERN
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

#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/XrdMgmOfs.hh"
#include "authz/XrdCapability.hh"
#include "common/SecEntity.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DrainTransferJob::~DrainTransferJob()
{
  eos_notice("Destroying transfer job");

  if (mThread.joinable()) {
    mThread.join();
  }
}

//------------------------------------------------------------------------------
// Save error message and set the status accordingly
//------------------------------------------------------------------------------
void DrainTransferJob::ReportError(const std::string& error)
{
  eos_notice(error.c_str());
  mErrorString = error;
  mStatus = Status::Failed;
}

//------------------------------------------------------------------------------
// Start thread doing the draining
//------------------------------------------------------------------------------
void DrainTransferJob::Start()
{
  mThread = std::thread(&DrainTransferJob::DoIt, this);
}

//------------------------------------------------------------------------------
// Implement the thrid-party transfer
// @todo (amanzi): review this whole method, it could be simplified and better
//                 encapsulated e.g. lambdas
//------------------------------------------------------------------------------
void
DrainTransferJob::DoIt()
{
  using eos::common::LayoutId;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdSysTimer sleeper;
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> cmd;
  uid_t owner_uid = 0;
  gid_t owner_gid = 0;
  unsigned long long size = 0;
  std::string source_xs;
  XrdOucString source_sz;
  long unsigned int lid = 0;
  unsigned long long cid = 0;
  XrdOucEnv* source_capabilityenv = 0;
  XrdOucEnv* target_capabilityenv = 0;
  std::string source_path;
  mStatus = Status::Running; // update status
  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosFileService->getFileMD(mFileId);
      lid = fmd->getLayoutId();
      cid = fmd->getContainerId();
      owner_uid = fmd->getCUid();
      owner_gid = fmd->getCGid();
      size = fmd->getSize();
      source_path = gOFS->eosView->getUri(fmd.get());
      eos::common::Path cPath(source_path.c_str());
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      cmd = gOFS->eosView->getContainer(gOFS->eosView->getUri(cmd.get()));

      // get the checksum string if defined
      for (unsigned int i = 0; i < LayoutId::GetChecksumLen(fmd->getLayoutId());
           i++) {
        char hb[3];
        sprintf(hb, "%02x", (unsigned char)(fmd->getChecksum().getDataPadded(i)));
        source_xs += hb;
      }

      eos::common::StringConversion::GetSizeString(source_sz,
          (unsigned long long) fmd->getSize());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_notice("fid=%016x errno=%d msg=\"%s\"\n",
                 mFileId, e.getErrno(), e.getMessage().str().c_str());
      ReportError("Error reading the file metadata");
      return;
    }
  }
  eos::common::FileSystem::fs_snapshot target_snapshot;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem* target_fs = 0;
  eos::common::FileSystem* source_fs = 0;
  {
    eos::common::RWMutexReadLock rd_fs_lock(FsView::gFsView.ViewMutex);
    source_fs = FsView::gFsView.mIdView[mFsIdSource];
    target_fs = FsView::gFsView.mIdView[mFsIdTarget];

    if (!target_fs || !source_fs) {
      ReportError("Target/source fs not found");
      return;
    }

    source_fs->SnapShotFileSystem(source_snapshot);
    target_fs->SnapShotFileSystem(target_snapshot);
  }

  if ((LayoutId::GetLayoutType(lid) == LayoutId::kRaidDP) ||
       (LayoutId::GetLayoutType(lid) == LayoutId::kRaid6)) {
    // @todo (amanzi): to be implemented - run TPC with reconstruction
  } else {
    // Prepare the TPC copy job
    XrdCl::PropertyList properties;
    XrdCl::PropertyList result;
    XrdOucString hexfid = "";
    XrdOucString sizestring;

    // Non-empty files run with TPC only
    if (size) {
      properties.Set("thirdParty", "only");
    }

    properties.Set("force", true);
    properties.Set("posc", false);
    properties.Set("coerce", false);
    XrdOucString source, target = "";
    std::string cgi;
    cgi += "&eos.app=drainer";
    cgi += "&eos.targetsize=";
    cgi += source_sz.c_str();

    if (!source_xs.empty()) {
      cgi += "&eos.checksum=";
      cgi += source_xs;
    }

    XrdCl::URL url_src;
    url_src.SetProtocol("root");
    url_src.SetHostName(source_snapshot.mHost.c_str());
    url_src.SetPort(stoi(source_snapshot.mPort));
    url_src.SetUserName("daemon");
    unsigned long long target_lid = lid & 0xffffff0f;

    // Mask block checksums for the current replica layout
    if (LayoutId::GetBlockChecksum(lid) != LayoutId::kNone) {
      target_lid &= 0xff0fffff;
    }

    XrdOucString source_params = "";
    source_params += "mgm.access=read";
    source_params += "&mgm.lid=";
    source_params += eos::common::StringConversion::GetSizeString(sizestring,
                     (unsigned long long) target_lid);
    source_params += "&mgm.cid=";
    source_params +=  eos::common::StringConversion::GetSizeString(sizestring, cid);
    source_params += "&mgm.ruid=1";
    source_params += "&mgm.rgid=1";
    source_params += "&mgm.uid=1";
    source_params += "&mgm.gid=1";
    source_params += "&mgm.path=";
    source_params += source_path.c_str();
    source_params += "&mgm.manager=";
    source_params += gOFS->ManagerId.c_str();
    eos::common::FileId::Fid2Hex(mFileId, hexfid);
    source_params += "&mgm.fid=";
    source_params += +hexfid.c_str();
    source_params += "&mgm.sec=";
    source_params += eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
    source_params += "&mgm.drainfsid=";
    source_params += (int) mFsIdSource;
    // build the replica_source_capability contents
    source_params += "&mgm.localprefix=";
    source_params += source_snapshot.mPath.c_str();
    source_params += "&mgm.fsid=";
    source_params += (int) source_snapshot.mId;
    source_params += "&mgm.sourcehostport=";
    source_params += source_snapshot.mHostPort.c_str();
    source_params += "&eos.app=drainer";
    source_params += "&eos.ruid=0&eos.rgid=0";
    // build source
    source +=  source_path.c_str();
    target = source;
    XrdCl::URL url_trg;
    url_trg.SetProtocol("root");
    url_trg.SetHostName(target_snapshot.mHost.c_str());
    url_trg.SetPort(stoi(target_snapshot.mPort));
    url_trg.SetUserName("daemon");
    url_trg.SetParams(cgi);
    XrdOucString target_params = "";
    target_params += "mgm.access=write";
    target_params += "&mgm.lid=";
    target_params += eos::common::StringConversion::GetSizeString(sizestring,
                     (unsigned long long) target_lid);
    target_params += "&mgm.source.lid=";
    target_params += eos::common::StringConversion::GetSizeString(sizestring,
                     (unsigned long long) lid);
    target_params += "&mgm.source.ruid=";
    target_params += eos::common::StringConversion::GetSizeString(sizestring,
                     (unsigned long long) owner_uid);
    target_params += "&mgm.source.rgid=";
    target_params += eos::common::StringConversion::GetSizeString(sizestring,
                     (unsigned long long) owner_gid);
    target_params += "&mgm.cid=";
    target_params += eos::common::StringConversion::GetSizeString(sizestring,
                     cid);
    target_params += "&mgm.ruid=1";
    target_params += "&mgm.rgid=1";
    target_params += "&mgm.uid=1";
    target_params += "&mgm.gid=1";
    target_params += "&mgm.path=";
    target_params += source_path.c_str();
    target_params += "&mgm.manager=";
    target_params += gOFS->ManagerId.c_str();
    target_params += "&mgm.fid=";
    target_params += hexfid.c_str();
    target_params += "&mgm.sec=";
    target_params += eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
    target_params += "&mgm.drainfsid=";
    target_params += (int) mFsIdSource;
    // build the target_capability contents
    target_params += "&mgm.localprefix=";
    target_params += target_snapshot.mPath.c_str();
    target_params += "&mgm.fsid=";
    target_params += (int) target_snapshot.mId;
    target_params += "&mgm.sourcehostport=";
    target_params += target_snapshot.mHostPort.c_str();
    target_params += "&mgm.bookingsize=";
    target_params += eos::common::StringConversion::GetSizeString(sizestring,
                     size);
    // issue a replica_source_capability
    XrdOucEnv insource_capability(source_params.c_str());
    XrdOucEnv intarget_capability(target_params.c_str());
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
    int caprc = 0;

    if ((caprc = gCapabilityEngine.Create(&insource_capability,
                                          source_capabilityenv,
                                          symkey, gOFS->mCapabilityValidity)) ||
        (caprc = gCapabilityEngine.Create(&intarget_capability,
                                          target_capabilityenv,
                                          symkey, gOFS->mCapabilityValidity))) {
      std::string err  = "unable to create source/target capability - errno=";
      err += caprc;
      ReportError(err);
    } else {
      int caplen = 0;
      XrdOucString source_cap = source_capabilityenv->Env(caplen);
      XrdOucString target_cap = target_capabilityenv->Env(caplen);
      source_cap += "&source.url=root://";
      source_cap += source_snapshot.mHostPort.c_str();
      source_cap += "//replicate:";
      source_cap += hexfid;
      target_cap += "&target.url=root://";
      target_cap += target_snapshot.mHostPort.c_str();
      target_cap += "//replicate:";
      target_cap += hexfid;
      url_src.SetParams(source_cap.c_str());
      url_src.SetPath(source.c_str());
      url_trg.SetParams(target_cap.c_str());
      url_trg.SetPath(target.c_str());
      properties.Set("source", url_src);
      properties.Set("target", url_trg);
      properties.Set("sourceLimit", (uint16_t) 1);
      properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
      properties.Set("parallelChunks", (uint8_t) 1);
      properties.Set("tpcTimeout",  900);
      //create the process job
      XrdCl::CopyProcess lCopyProcess;
      lCopyProcess.AddJob(properties, &result);
      XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();
      eos_notice("[tpc]: %s=>%s %s",
                 url_src.GetURL().c_str(),
                 url_trg.GetURL().c_str(),
                 lTpcPrepareStatus.ToStr().c_str());

      if (lTpcPrepareStatus.IsOK()) {
        XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
        eos_notice("[tpc]: %s %d", lTpcStatus.ToStr().c_str(), lTpcStatus.IsOK());

        if (!lTpcStatus.IsOK()) {
          ReportError(lTpcStatus.ToStr().c_str());
        } else {
          eos_notice("Drain Job completed Succesfully");
          mStatus = Status::OK;
        }
      } else {
        ReportError("Failed to prepare the Drain job");
      }
    }

    if (source_capabilityenv) {
      delete source_capabilityenv;
    }

    if (target_capabilityenv) {
      delete target_capabilityenv;
    }
  }
}

EOSMGMNAMESPACE_END
