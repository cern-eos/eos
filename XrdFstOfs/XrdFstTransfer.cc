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

  char result[64*1024]; result[0]=0;
  int  result_size=64*1024;

  XrdOucEnv capOpaque(opaque.c_str());
  
  XrdOucString replicaUrl = "root://"; 
  replicaUrl += capOpaque.Get("mgm.sourcehostport");
  replicaUrl += "//replicate:";
  replicaUrl += capOpaque.Get("mgm.fid");
  replicaUrl += "?";
  replicaUrl += capability;

  // ----------------------------------------------------------------------------------------------------------
  // retrieve the file meta data from the remote server

  XrdFstOfsClientAdmin* replicaAdmin = gOFS.FstOfsClientAdminManager.GetAdmin(capOpaque.Get("mgm.sourcehostport"));

  XrdOucString fmdquery="/?fst.pcmd=getfmd&fst.getfmd.fid=";fmdquery += capOpaque.Get("mgm.fid");
  fmdquery += "&fst.getfmd.fsid="; fmdquery += capOpaque.Get("mgm.fsid");

  int rc=0;
  replicaAdmin->Lock();
  replicaAdmin->GetAdmin()->Connect();
  replicaAdmin->GetAdmin()->GetClientConn()->ClearLastServerError();
  replicaAdmin->GetAdmin()->Query(kXR_Qopaquf,
				  (kXR_char *) fmdquery.c_str(),
				  (kXR_char *) result, result_size);
  
  if (!replicaAdmin->GetAdmin()->LastServerResp()) {
    eos_static_err("Unable to retrieve meta data from server %s for fid=%s fsid=%s",capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"));
    
    rc = SFS_ERROR;
  }
  switch (replicaAdmin->GetAdmin()->LastServerResp()->status) {
  case kXR_ok:
    eos_static_debug("got replica file meta data from server %s for fid=%s fsid=%s",capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"));
    rc = SFS_OK;
    break;
    
  case kXR_error:
    eos_static_err("Unable to retrieve meta data from server %s for fid=%s fsid=%s",capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"));
    rc = SFS_ERROR;
    break;
    
  default:
    rc = SFS_OK;
    break;
  }
  replicaAdmin->UnLock();

  if (rc) 
    return EIO;

  if (!strncmp(result,"ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_err("Unable to retrieve meta data on remote server %s for fid=%s fsid=%s",capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.fsid"));
    return ENODATA;
  }
  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(result);

  struct XrdCommonFmd::FMD fmd;
  
  if (!XrdCommonFmd::EnvToFmd(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    return EIO;
  }
  // very simple check
  if (fmd.fid != fId) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid is %lu instead of %lu !", fmd.fid, fId);
    return EIO;
  }
  
  // ----------------------------------------------------------------------------------------------------------
  // open replica to pull

  XrdClient* replicaClient = new XrdClient(replicaUrl.c_str());
  if (!replicaClient->Open(0,0,false)) {
    eos_static_err("Failed to open replica to pull fid %llu from %s %d=>%d", capOpaque.Get("mgm.fid"), capOpaque.Get("mgm.sourcehostport"), capOpaque.Get("mgm.fsid"), capOpaque.Get("mgm.fsidtarget"));
    delete replicaClient;
    return EIO;
  } else {
    // open local replica
    XrdOucString fstPath="";

    XrdCommonFileId::FidPrefix2FullPath(capOpaque.Get("mgm.fid"),capOpaque.Get("mgm.localprefixtarget"),fstPath);
    
    int fout = open(fstPath.c_str(), O_CREAT| O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fout <0) {
      eos_static_err("Failed to open local replica file %s errno=%u", fstPath.c_str(),errno);
      delete replicaClient;
      return errno;
    }
    
    // simple copy loop
    off_t offset = 0;
    int buffersize = 1024*1024;
    char* cpbuffer = (char*)malloc(buffersize);
    if (!cpbuffer) {
      eos_static_err("Failed to allocate copy buffer");
      delete replicaClient;
      return ENOMEM;
    }

    bool failed = false;
    do {
      int nread = replicaClient->Read(cpbuffer,offset,buffersize);
      if (nread>=0) {
	int nwrite = write(fout, cpbuffer, nread);
	if (nwrite != nread) {
	  failed = true;
	  break;
	}
      }
      if (nread != buffersize) 
	break;

      offset += nread;
    } while (1);

    // free the copy buffer
    free(cpbuffer);
    close(fout);
    
    if (failed) {
      // in case of failure we drop this entry
      unlink(fstPath.c_str());
    }
  }
  replicaClient->Close();

  delete replicaClient;

  // ----------------------------------------------------------------------------------------------------------
  // commit file meta data locally

  XrdCommonFmd* newfmd = gFmdHandler.GetFmd(fId, fsIdTarget,fmd.uid, fmd.gid, fmd.lid,1);
  // inherit the file meta data
  newfmd->Replicate(fmd);

  if (!gFmdHandler.Commit(newfmd)) {
    delete newfmd;
    return EIO;
  }

  delete newfmd;

  // ----------------------------------------------------------------------------------------------------------
  // commit file meta data centrally
  XrdOucString capOpaqueFile="";
  XrdOucString mTimeString="";
  capOpaqueFile += "/?";
  capOpaqueFile += "&mgm.path="; capOpaqueFile += capOpaque.Get("mgm.path");
  capOpaqueFile += "&mgm.fid=";  capOpaqueFile += capOpaque.Get("mgm.fid");
  capOpaqueFile += "&mgm.pcmd=commit";
  capOpaqueFile += "&mgm.size=";
  //  eos_static_crit("filesize is %llu %llu", newfmd->fMd.size, fmd.size);
  char filesize[1024]; sprintf(filesize,"%llu", newfmd->fMd.size);
  capOpaqueFile += filesize;
  capOpaqueFile += "&mgm.mtime=";
  capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, newfmd->fMd.mtime);
  capOpaqueFile += "&mgm.mtime_ns=";
  capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, newfmd->fMd.mtime_ns);
  
  capOpaqueFile += "&mgm.add.fsid=";
  capOpaqueFile += (int)newfmd->fMd.fsid;
  
  XrdOucErrInfo* error = 0;

  rc = gOFS.CallManager(error, capOpaque.Get("mgm.path"),capOpaque.Get("mgm.manager"), capOpaqueFile);
  if (rc) {
    eos_static_err("Unable to commit meta data to central cache");
    return rc;
  }
  return 0;
}

