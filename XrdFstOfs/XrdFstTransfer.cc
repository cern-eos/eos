/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdFstOfs/XrdFstTransfer.hh"
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
  XrdOucEnv capOpaque(opaque.c_str());
  
  XrdOucString replicaUrl = "root://"; 
  replicaUrl += capOpaque.Get("mgm.sourcehostport");
  replicaUrl += "//replicate:";
  replicaUrl += capOpaque.Get("mgm.fid");
  replicaUrl += "?";
  replicaUrl += capability;
  
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
  
  return 0;
}

