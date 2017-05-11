// ----------------------------------------------------------------------
// File: DrainTransferJob.cc
// Author: Andrea Manzi - CERN
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

/*----------------------------------------------------------------------------*/
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN


DrainTransferJob::~DrainTransferJob ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 * 
/----------------------------------------------------------------------------*/
{
}

void
DrainTransferJob::DoIt ()
/*----------------------------------------------------------------------------*/
/**
 *  * @brief Implement the ThirdParty transfer
 *   * 
 /----------------------------------------------------------------------------*/
{

  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo error;
  XrdSysTimer sleeper;
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> cmd;
  uid_t owner_uid = 0;
  gid_t owner_gid = 0;
  unsigned long long size = 0;
  eos::IContainerMD::XAttrMap attrmap;
  XrdOucString sourceChecksum;
  XrdOucString sourceAfterChecksum;
  XrdOucString sourceSize;
  long unsigned int lid = 0;
  unsigned long long cid = 0;
  
  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosFileService->getFileMD(mFileId);
      lid = fmd->getLayoutId();
      cid = fmd->getContainerId();
      owner_uid = fmd->getCUid();
      owner_gid = fmd->getCGid();
      size = fmd->getSize();
      mSourcePath = gOFS->eosView->getUri(fmd.get());
      eos::common::Path cPath(mSourcePath.c_str());
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      cmd = gOFS->eosView->getContainer(gOFS->eosView->getUri(cmd.get()));
      XrdOucErrInfo error;
      // load the attributes
      gOFS->_attr_ls(gOFS->eosView->getUri(cmd.get()).c_str(), error, rootvid, 0,
                     attrmap, false, true);

      // get the checksum string if defined
      for (unsigned int i = 0;
           i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++) {
        char hb[3];
        sprintf(hb, "%02x", (unsigned char)(fmd->getChecksum().getDataPadded(i)));
        sourceChecksum += hb;
      }

      // get the size string
      eos::common::StringConversion::GetSizeString(sourceSize,
               (unsigned long long) fmd->getSize());

    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_err("fid=%016x errno=%d msg=\"%s\"\n",
                     mFileId, e.getErrno(), e.getMessage().str().c_str());
      }
    }

    eos::common::FileSystem::fs_snapshot target_snapshot;
    eos::common::FileSystem::fs_snapshot source_snapshot;
    eos::common::FileSystem* target_fs = 0;
    eos::common::FileSystem* source_fs = 0;

    {
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
      
      source_fs = FsView::gFsView.mIdView[mfsIdSource];
      target_fs = FsView::gFsView.mIdView[mfsIdTarget];
   
      if (!target_fs) {
      //should report some error
      }
      
      source_fs->SnapShotFileSystem(source_snapshot);
      target_fs->SnapShotFileSystem(target_snapshot);
      
    }

    bool success = false;

    // Prepare the TPC copy job
    XrdCl::PropertyList properties;
    XrdCl::PropertyList result;
    XrdOucString hexfid = "";
    XrdOucString sizestring;

    if (size) {
      // non-empty files run with TPC
        properties.Set("thirdParty", "only");
    }

    properties.Set("force", true);
    properties.Set("posc", false);
    properties.Set("coerce", false);
    std::string source = mSourcePath.c_str();
    std::string target = source;
    //target to get from the 
    std::string cgi;
    cgi += "&eos.app=drainer";
    cgi += "&eos.targetsize=";
    cgi += sourceSize.c_str();

    if (sourceChecksum.length()) {
      cgi += "&eos.checksum=";
      cgi += sourceChecksum.c_str();
    }
    XrdCl::URL url_src;
    url_src.SetProtocol("root");
    url_src.SetHostName(source_snapshot.mHost.c_str());
    url_src.SetPort(stoi(source_snapshot.mPort));
    url_src.SetUserName("root");
    //layour id
    unsigned long long target_lid = lid & 0xffffff0f; 
    if (eos::common::LayoutId::GetBlockChecksum(lid) == eos::common::LayoutId::kNone)
    {
      // mask block checksums (e.g. for replica layouts)                                                               
      target_lid &= 0xf0ffffff;
    }
    XrdOucString source_params ="";
    source_params+= "mgm.access=read";
    source_params+= "&mgm.lid=";
    source_params+= eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) target_lid);
    source_params+= "&mgm.cid=";
    source_params+=  eos::common::StringConversion::GetSizeString( sizestring, cid);
    source_params+= "&mgm.ruid=1";
    source_params+= "&mgm.rgid=1";
    source_params+= "&mgm.uid=1";
    source_params+= "&mgm.gid=1";
    source_params+= "&mgm.path=";
    source_params+= source.c_str();
    source_params+= "&mgm.manager=";
    source_params+= gOFS->ManagerId.c_str(); 
    eos::common::FileId::Fid2Hex(mFileId, hexfid);
    source_params+= "&mgm.fid=";
    source_params+= +hexfid.c_str();
    source_params+= "&mgm.sec=";
    source_params+= eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
    source_params+= "&mgm.drainfsid=";
    source_params+= (int) mfsIdSource;          
              
    // build the replica_source_capability contents
    source_params+="&mgm.localprefix=";
    source_params+= source_snapshot.mPath.c_str();
    source_params+="&mgm.fsid=";
    source_params+= (int) source_snapshot.mId;
    source_params+="&mgm.sourcehostport=";
    source_params+=source_snapshot.mHostPort.c_str();
    source_params+="&eos.app=drainer";
    source_params+="&eos.ruid=0&eos.rgid=0";
    source_params+="&source.url=root://";
    source_params+= source_snapshot.mHostPort.c_str();
    source_params+= "//replicate:";
    source_params+= hexfid.c_str();

    url_src.SetParams(source_params.c_str());
    url_src.SetPath(source);
    XrdCl::URL url_trg;
    url_trg.SetProtocol("root");
    url_trg.SetHostName(target_snapshot.mHost.c_str());
    url_src.SetPort(stoi(target_snapshot.mPort));
    url_trg.SetUserName("root");
    url_trg.SetParams(cgi);

    XrdOucString target_params ="";
    target_params+="mgm.access=write";
    
    target_params+="&mgm.lid=";
    target_params+= eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) target_lid);
    target_params+="&mgm.source.lid=";
    target_params+= eos::common::StringConversion::GetSizeString(sizestring,
                                   (unsigned long long) lid);
    target_params+="&mgm.source.ruid=";
    target_params+= eos::common::StringConversion::GetSizeString(sizestring,
                                   (unsigned long long) owner_uid);  
    target_params+="&mgm.source.rgid=";
    target_params+= eos::common::StringConversion::GetSizeString(sizestring,
                                   (unsigned long long) owner_gid); 
    target_params+="&mgm.cid=";
    target_params+=eos::common::StringConversion::GetSizeString(sizestring,
                                   cid);
            
    target_params+= "&mgm.ruid=1";
    target_params+= "&mgm.rgid=1";
    target_params+= "&mgm.uid=1";
    target_params+= "&mgm.gid=1";
    target_params+= "&mgm.path=";
    target_params+= source.c_str();
    target_params+= "&mgm.manager=";
    target_params+= gOFS->ManagerId.c_str(); 
    target_params+= "&mgm.fid=";
    target_params+= hexfid.c_str();
    target_params+= "&mgm.sec=";
    target_params+= eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
    target_params+= "&mgm.drainfsid=";
    target_params+= (int) mfsIdSource;  
    // build the target_capability contents
    target_params+= "&mgm.localprefix=";
    target_params+= target_snapshot.mPath.c_str();
    target_params+= "&mgm.fsid=";
    target_params+= (int) target_snapshot.mId;
    target_params+= "&mgm.sourcehostport=";
    target_params+= target_snapshot.mHostPort.c_str();       
    target_params+= "&mgm.bookingsize=";
    target_params+= eos::common::StringConversion::GetSizeString(sizestring,
                                   size);
    target_params+= "&target.url=root://";
    target_params+= target_snapshot.mHostPort.c_str();
    target_params+= "//replicate:";
    target_params+= hexfid.c_str();
    
    url_trg.SetParams(target_params.c_str());

    url_trg.SetPath(target);
    properties.Set("source", url_src);
    properties.Set("target", url_trg);
    properties.Set("sourceLimit", (uint16_t) 1);
    properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
    properties.Set("parallelChunks", (uint8_t) 1);
    XrdCl::CopyProcess lCopyProcess;
    lCopyProcess.AddJob(properties, &result);
    XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();
    eos_static_info("[tpc]: %s=>%s %s",
                    url_src.GetURL().c_str(),
                    url_trg.GetURL().c_str(),
                    lTpcPrepareStatus.ToStr().c_str());

    if (lTpcPrepareStatus.IsOK()) {
        XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
        eos_static_info("[tpc]: %s %d", lTpcStatus.ToStr().c_str(), lTpcStatus.IsOK());
        success = lTpcStatus.IsOK();
    } else {
        //we should report the error
    }
    delete this;

}

EOSMGMNAMESPACE_END


