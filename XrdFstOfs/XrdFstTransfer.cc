/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdFstOfs/XrdFstTransfer.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

int
XrdFstTransfer::Do() 
{
  struct XrdCommonFmd::FMD fmd;

  XrdOucEnv capOpaque(opaque.c_str());
  
  XrdOucString replicaUrl = "root://"; 
  replicaUrl += capOpaque.Get("mgm.sourcehostport");
  replicaUrl += "//replicate:";
  replicaUrl += capOpaque.Get("mgm.fid");
  replicaUrl += "?";
  replicaUrl += capability;

  // ----------------------------------------------------------------------------------------------------------
  // retrieve the file meta data from the remote server

  XrdCommonClientAdmin* replicaAdmin = gOFS.CommonClientAdminManager.GetAdmin(capOpaque.Get("mgm.sourcehostport"));

  int rc=0;
  eos_static_debug("GetRemoteFmd %s %s %s",  capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"));
  rc = gFmdHandler.GetRemoteFmd(replicaAdmin, capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"),fmd);

  if (rc) {
    eos_static_err("Failed to get remote fmd from %s [%d] fid %s from %s %s=>%s", capOpaque.Get("mgm.sourcehostport"),rc, capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"));
    return rc;
  }

  if (!gOFS.LockManager.TryLock(fId)) {
    eos_static_err("File is currently locked for writing - giving up fid %s", capOpaque.Get("mgm.fid"));
    return EBUSY;
  }

  // get checksum plugin
  XrdFstOfsChecksum* checkSum = XrdFstOfsChecksumPlugins::GetChecksumObject(fmd.lid);

  if (checkSum) checkSum->Reset();

  // ----------------------------------------------------------------------------------------------------------
  // open replica to pull

  bool failed = false; // indicates if the copy failed
  
  off_t offset = 0;    // gives the filesize after a transfer

  XrdClient* replicaClient = new XrdClient(replicaUrl.c_str());

  if (!replicaClient->Open(0,0,false)) {
    eos_static_err("Failed to open replica to pull fid %llu from %s %d=>%d", capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"));
    delete replicaClient;
    return EIO;
  } else {
    // open local replica
    XrdOucString fstPath="";
    
    XrdCommonFileId::FidPrefix2FullPath(capOpaque.Get("mgm.fid"),capOpaque.Get("mgm.localprefixtarget"),fstPath);
    
    XrdFstOfsFile* ofsFile = new XrdFstOfsFile(0);

    if (!ofsFile) {
      eos_static_err("Failed to allocate ofs file %s", fstPath.c_str());
      delete replicaClient;
      return ENOMEM;
    }
    
    if (ofsFile->openofs(fstPath.c_str(), SFS_O_TRUNC | SFS_O_RDWR, SFS_O_MKPTH |S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH, 0,0)) {
      if (!errno) errno = EIO;
      eos_static_err("Failed to open local replica file %s errno=%u", fstPath.c_str(),errno);
      delete replicaClient;
      return errno;
    }
    
    // simple copy loop
    int buffersize = 1024*1024;
    char* cpbuffer = (char*)malloc(buffersize);
    if (!cpbuffer) {
      eos_static_err("Failed to allocate copy buffer");
      delete replicaClient;
      return ENOMEM;
    }
    
    do {
      int nread = replicaClient->Read(cpbuffer,offset,buffersize);
      if (nread>0) {
	if (!ofsFile->writeofs(offset, cpbuffer, nread)) {
	  failed = true;
	  break;
	}
      }

      if (checkSum && (nread>=0)) checkSum->Add(cpbuffer,nread,offset);

      if (nread != buffersize) {
	offset += nread;
	break;
      }

      offset += nread;
    } while (1);

    // free the copy buffer
    free(cpbuffer);
    ofsFile->closeofs();
    if (checkSum) checkSum->Finalize();

    if ( (replicaClient->LastServerError()->errnum) && (replicaClient->LastServerError()->errnum!=kXR_noErrorYet) ) {
      eos_static_err("transfer error during replica of %s fid=%sfrom %s=>%s xsum=%s ec=%d emsg=%s", capOpaque.Get("mgm.path"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"), checkSum?checkSum->GetHexChecksum():"none", replicaClient->LastServerError()->errnum, replicaClient->LastServerError()->errmsg);
      eos_static_err("ile %s errno=%u", fstPath.c_str(),errno);
      failed = true;
    }
    if (failed) {
      // in case of failure we drop this entry
      unlink(fstPath.c_str());
    }
  }
  replicaClient->Close();

  delete replicaClient;

  if (failed) {
    // if the copy failed
    if (!errno) errno = EIO;
    if (checkSum) delete checkSum;
    return errno;
  }

  XrdCommonFmd* newfmd = gFmdHandler.GetFmd(fId, fsIdTarget,fmd.uid, fmd.gid, fmd.lid,1);
  // inherit the file meta data
  newfmd->Replicate(fmd);

  // ----------------------------------------------------------------------------------------------------------
  // compare remote and computed checksum
  int checksumlen;
  bool checksumerror=false;
  if (checkSum) {
    checkSum->GetBinChecksum(checksumlen);
    for (int i=0; i<checksumlen ; i++) {
      if (newfmd->fMd.checksum[i] != checkSum->GetBinChecksum(checksumlen)[i]) {
	checksumerror=true;
	// set the new computed checksum anyway - this policy we might change ?!?!?
	newfmd->fMd.checksum[i] = checkSum->GetBinChecksum(checksumlen)[i];
      }
    }
  }
  
  // ----------------------------------------------------------------------------------------------------------
  // compare transfer and FMD size
  if ((long long)offset != newfmd->fMd.size) {
    eos_static_err("size error during replica of %s fid=%sfrom %s=>%s xsum=%s txsize=%llu fmdsize=%llu", capOpaque.Get("mgm.path"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"), checkSum?checkSum->GetHexChecksum():"none", offset, newfmd->fMd.size);
  }

  if (checkSum && checksumerror) {
    eos_static_err("checksum error during replica of %s fid=%sfrom %s=>%s xsum=%s", capOpaque.Get("mgm.path"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"), checkSum?checkSum->GetHexChecksum():"none");
  }

  // ----------------------------------------------------------------------------------------------------------
  // commit file meta data locally

  if (!gFmdHandler.Commit(newfmd)) {
    delete newfmd;
    if (checkSum) delete checkSum;
    return EIO;
  }

  // ----------------------------------------------------------------------------------------------------------
  // commit file meta data centrally
  XrdOucString capOpaqueFile="";
  XrdOucString mTimeString="";
  capOpaqueFile += "/?";
  capOpaqueFile += "&mgm.path="; capOpaqueFile += capOpaque.Get("mgm.path");
  capOpaqueFile += "&mgm.fid=";  capOpaqueFile += capOpaque.Get("mgm.fid");
  capOpaqueFile += "&mgm.pcmd=commit";
  capOpaqueFile += "&mgm.size=";
  //  eos_static_crit("filesize is %llu %llu", newfmd->fMd.size, newfmd->fMd.size);
  char filesize[1024]; sprintf(filesize,"%llu", newfmd->fMd.size);
  capOpaqueFile += filesize;
  capOpaqueFile += "&mgm.mtime=";
  capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, newfmd->fMd.mtime);
  capOpaqueFile += "&mgm.mtime_ns=";
  capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, newfmd->fMd.mtime_ns);
  
  capOpaqueFile += "&mgm.add.fsid=";
  capOpaqueFile += (int)newfmd->fMd.fsid;

  if (checkSum) {
    capOpaqueFile += "&mgm.checksum=";
    capOpaqueFile += checkSum->GetHexChecksum();
  }

  XrdOucErrInfo* error = 0;

  rc = gOFS.CallManager(error, capOpaque.Get("mgm.path"),capOpaque.Get("mgm.manager"), capOpaqueFile);


  if (rc) {
    delete newfmd;
    if (checkSum) delete checkSum;
    eos_static_err("Unable to commit meta data to central cache");
    return rc;
  }

  eos_static_info("successful replica of %s fid=%sfrom %s=>%s xsum=%s txsize=%llu fmdsize=%llu", capOpaque.Get("mgm.path"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"), checkSum?checkSum->GetHexChecksum():"none", offset, newfmd->fMd.size);

  if (checkSum) delete checkSum;

  delete newfmd;
  return 0;
}

