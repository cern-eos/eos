// ----------------------------------------------------------------------
// File: ReplicaParLayout.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
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

/*----------------------------------------------------------------------------*/
#include "fst/layout/ReplicaParLayout.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"
/*----------------------------------------------------------------------------*/
#include <xfs/xfs.h>
/*----------------------------------------------------------------------------*/
extern XrdOssSys  *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
ReplicaParLayout::ReplicaParLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *outerror) : Layout(thisFile, "replica", lid, outerror)
{
  nStripes = eos::common::LayoutId::GetStripeNumber(lid) + 1; // this 1=0x0 16=0xf :-)

  for (int i=0; i< eos::common::LayoutId::kSixteenStripe; i++) {
    replicaClient[i] = 0;
    replicaUrl[i] = "";
  }
  ioLocal=false;
}

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::open(const char                *path,
                       XrdSfsFileOpenMode   open_mode,
                       mode_t               create_mode,
                       const XrdSecEntity        *client,
                       const char                *opaque)
{

  // no replica index definition indicates that this is gateway access just forwarding to another remote server

  int replicaIndex = 0;
  int replicaHead  = 0;

  bool isGateWay=false;
  bool isHeadServer=false;

  const char* index = ofsFile->openOpaque->Get("mgm.replicaindex");
  
  if (index) {
    replicaIndex = atoi(index);
    if ( (replicaIndex<0) || (replicaIndex>eos::common::LayoutId::kSixteenStripe) ) {
      eos_err("illegal replica index %d", replicaIndex);
      return gOFS.Emsg("ReplicaParOpen",*error, EINVAL, "open replica - illegal replica index found", index);
    }
    ioLocal = true;
  } else {
    ioLocal = false;
    isGateWay = true;
  }


  const char* head = ofsFile->openOpaque->Get("mgm.replicahead");
  if (head) {
    replicaHead = atoi(head);
    if ( (replicaHead<0) || (replicaHead>eos::common::LayoutId::kSixteenStripe) ) {
      eos_err("illegal replica head %d", replicaHead);
      return gOFS.Emsg("ReplicaParOpen",*error, EINVAL, "open replica - illegal replica head found", head);
    }
  } else {
    eos_err("replica head missing");
    return gOFS.Emsg("ReplicaParOpen",*error, EINVAL, "open replica - no replica head defined");
  }

  // define the replication head
  if (replicaIndex == replicaHead) {
    isHeadServer = true;
  }
  
  // define if this is the first client contact point
  if (isGateWay || ( (!isGateWay) && (isHeadServer) )) {
    isEntryServer = true;
  }

  int envlen;
  XrdOucString remoteOpenOpaque = ofsFile->openOpaque->Env(envlen);

  XrdOucString remoteOpenPath = ofsFile->openOpaque->Get("mgm.path");

  // only a gateway or head server needs to contact others
  if ( (isGateWay)  || (isHeadServer) ) {
    // assign stripe urls'
    for (int i=0; i< nStripes; i++) {
      remoteOpenOpaque = ofsFile->openOpaque->Env(envlen);
      XrdOucString reptag = "mgm.url"; reptag += i;
      const char* rep = ofsFile->capOpaque->Get(reptag.c_str());
      if ( !rep ) {
        eos_err("Failed to open replica - missing url for replica %s", reptag.c_str());
        return gOFS.Emsg("ReplicaParOpen",*error, EINVAL, "open stripes - missing url for replica ", reptag.c_str());
      }
      
      // check if the first replica is remote
      replicaUrl[i] = rep;
      replicaUrl[i] += remoteOpenPath;
      replicaUrl[i] +="?";
      
      // prepare the index for the next target
      if (index) {
        XrdOucString oldindex = "mgm.replicaindex=";
        XrdOucString newindex = "mgm.replicaindex=";
        oldindex += index;
        newindex += i;
        remoteOpenOpaque.replace(oldindex.c_str(),newindex.c_str());
      } else {
        // this points now to the head
        remoteOpenOpaque += "&mgm.replicaindex=";
        remoteOpenOpaque += head;       
      }
      replicaUrl[i] += remoteOpenOpaque;
    }
  }

  for (int i=0; i< nStripes; i++) {
    // open all the replica's needed
    // local IO 
    if ( (ioLocal) && (i == replicaIndex) )  {
      // only the referenced entry URL does local IO
      LocalReplicaPath = path;
      if (ofsFile->isRW) {
        // write case
        // local handle
        if (ofsFile->openofs(path, open_mode, create_mode, client, opaque)) {
          eos_err("Failed to open replica - local open failed on ", path);
          return gOFS.Emsg("ReplicaOpen",*error, EIO, "open replica - local open failed ", path);
        }
      } else {
        // read case
        // local handle
        if (ofsFile->openofs(path, open_mode, create_mode, client, opaque)) {
          eos_err("Failed to open replica - local open failed on ", path);
          return gOFS.Emsg("ReplicaOpen",*error, EIO, "open replica - local open failed ", path);
        }
      }
    } else {
      // gateway contats the head, head contacts all
      if ( (isGateWay && i == replicaHead) ||
           (isHeadServer && (i != replicaIndex)) ) {
        if (ofsFile->isRW) {
	  EnvPutInt(NAME_READCACHESIZE,0);
          replicaClient[i] = new XrdClient(replicaUrl[i].c_str());
          
	  XrdOucString maskUrl = replicaUrl[i].c_str()?replicaUrl[i].c_str():"";
	  // mask some opaque parameters to shorten the logging                                                                                                                                                
	  eos::common::StringConversion::MaskTag(maskUrl,"cap.sym");
	  eos::common::StringConversion::MaskTag(maskUrl,"cap.msg");
	  eos::common::StringConversion::MaskTag(maskUrl,"authz");
          eos_info("Opening Layout Stripe %s\n", maskUrl.c_str());
          // write case
          if (!replicaClient[i]->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | kXR_open_updt | kXR_new, false)) {
            // open failed
            eos_err("Failed to open stripes - remote open failed on ", replicaUrl[i].c_str());
            return gOFS.Emsg("ReplicaParOpen",*error, EREMOTEIO, "open stripes - remote open failed ", replicaUrl[i].c_str());
          }
        } else {
          // read case just uses one replica
          continue;
        }
      }
    }
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
ReplicaParLayout::~ReplicaParLayout() 
{
  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      delete replicaClient[i];
    }
  }
}


/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  int rc1 = 0;
  int rc2 = true;

  if (ioLocal) {
    rc1= ofsFile->readofs(offset, buffer, length);
  } else {
    if (replicaClient[0]) {
      rc2 = replicaClient[0]->Read(buffer, offset, length);
      if (rc2 != length) {
        eos_err("Failed to read remote replica - read failed - %llu %llu %s", offset, length, replicaUrl[0].c_str());
        return gOFS.Emsg("ReplicaParRead",*error, EREMOTEIO, "read remote replica - read failed", replicaUrl[0].c_str());
      }
    }
  }

  if (rc1 <0) {
    eos_err("Failed to read local replica - read failed - %llu %llu %s", offset, length, replicaUrl[0].c_str());
    return gOFS.Emsg("ReplicaParRead",*error, errno, "read local replica - read failed", replicaUrl[0].c_str());
  }

  return rc1;
}

/*----------------------------------------------------------------------------*/
int 
ReplicaParLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  int rc1 = SFS_OK;
  int rc2 = true;

  if (ioLocal) {
    rc1= ofsFile->writeofs(offset, buffer, length);
  }

  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Write(buffer, offset, length)) {
        eos_err("Failed to write remote replica - write failed - %llu %llu %s", offset, length, replicaUrl[i].c_str());
        rc2 = 0;
      }
    }
  }

  if (rc1 <0) {
    eos_err("Failed to write local replica - write failed - %llu %llu %s", offset, length, replicaUrl[0].c_str());
    return gOFS.Emsg("ReplicaWrite",*error, errno, "write local replica - write failed", replicaUrl[0].c_str());
  }

  if (!rc2) {
    return gOFS.Emsg("ReplicaWrite",*error, EREMOTEIO, "write remote replica - write failed");
  }
  return length;

}

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::truncate(XrdSfsFileOffset offset)
{
  int rc1 = SFS_OK;
  int rc2 = true;

  if (ioLocal) {
    rc1= ofsFile->truncateofs(offset);
  }

  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Truncate(offset)) {
        eos_err("Failed to truncate remote replica - %llu %s", offset, replicaUrl[i].c_str());
        rc2=0;
      }
    }
  }

  if (rc1 <0) {
    eos_err("Failed to truncate local replica - %llu %s", offset, replicaUrl[0].c_str());
    return gOFS.Emsg("ReplicaParTruncate",*error, errno, "truncate local replica", replicaUrl[0].c_str());
  }
  if (!rc2) {
    return gOFS.Emsg("ReplicaParTruncate",*error, EREMOTEIO, "truncate remote replica");
  }
  return rc1;
}


/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::stat(struct stat *buf) 
{
  return XrdOfsOss->Stat(ofsFile->fstPath.c_str(),buf);
}

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::sync() 
{
  int rc1 = SFS_OK;
  int rc2 = true;
  if (ioLocal) {
    rc1 = ofsFile->syncofs();
  }

  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Sync()) {
        eos_err("Failed to sync remote replica - %s", replicaUrl[i].c_str());
        rc2=0;
      }
    }
  }  
  
  if (rc1 <0) {
    eos_err("Failed to sync local replica - %s", replicaUrl[0].c_str());
    return gOFS.Emsg("ReplicaParSync",*error, errno, "sync local replica", replicaUrl[0].c_str());
  }
  if (!rc2) {
    return gOFS.Emsg("ReplicaParSync",*error, EREMOTEIO, "sync remote replica");
  }  
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::remove()
{
  int rc1 = SFS_OK;
  int rc2 = true;
  if (ioLocal) {
    struct stat buf;
    if (!::stat(LocalReplicaPath.c_str(),&buf)) {
      // only try to delete if there is something to delete!
      rc1 = unlink(LocalReplicaPath.c_str());
    }
  }

  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Truncate(EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN)) {
        eos_err("Failed to truncate remote replica with deletion offset - %s", replicaUrl[i].c_str());
        rc2=0;
      } 
    } 
  }
  
  if (rc1 <0) {
    eos_err("Failed to remove local replica - %s", replicaUrl[0].c_str());
    return gOFS.Emsg("ReplicaClose",*error, errno, "remove local replica", replicaUrl[0].c_str());
  }

  if (!rc2) {
    return gOFS.Emsg("ReplicaRemove",*error, EREMOTEIO, "remove remote replica", "");
  }  
  return rc1;
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::close()
{
  int rc1 = SFS_OK;
  int rc2 = true;
  if (ioLocal) {
    rc1 = ofsFile->closeofs();
  }

  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Close()) {
        eos_err("Failed to close remote replica - %s", replicaUrl[i].c_str());
        rc2=0;
      } 
    } 
  }
  
  if (rc1 <0) {
    eos_err("Failed to close local replica - %s", replicaUrl[0].c_str());
    return gOFS.Emsg("ReplicaClose",*error, errno, "close local replica", replicaUrl[0].c_str());
  }

  if (!rc2) {
    return gOFS.Emsg("ReplicaClose",*error, EREMOTEIO, "close remote replica", "");
  }  
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::fallocate(XrdSfsFileOffset length)
{
  XrdOucErrInfo error;
  if(ofsFile->fctl(SFS_FCTL_GETFD,0,error))
    return -1;
  int fd = error.getErrInfo();
  if (fd>0) {
    if(platform_test_xfs_fd(fd)) {
      // select the fast XFS allocation function if available
      xfs_flock64_t fl;
      fl.l_whence= 0;
      fl.l_start= 0;
      fl.l_len= (off64_t)length;
      return xfsctl(NULL, fd, XFS_IOC_RESVSP64, &fl);
    } else {
      return posix_fallocate(fd,0,length);
    }
  }
  return -1;
}

/*----------------------------------------------------------------------------*/
int
ReplicaParLayout::fdeallocate(XrdSfsFileOffset fromoffset, XrdSfsFileOffset tooffset)
{
  XrdOucErrInfo error;
  if(ofsFile->fctl(SFS_FCTL_GETFD,0,error))
    return -1;
  int fd = error.getErrInfo();
  if (fd>0) {
    if(platform_test_xfs_fd(fd)) {
      // select the fast XFS deallocation function if available
      xfs_flock64_t fl;
      fl.l_whence= 0;
      fl.l_start= fromoffset;
      fl.l_len= (off64_t)tooffset-fromoffset;
      return xfsctl(NULL, fd, XFS_IOC_UNRESVSP64, &fl);
    } else {
      return 0;
    }
  }
  return -1;
}

EOSFSTNAMESPACE_END
