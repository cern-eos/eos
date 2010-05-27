/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsReplicaLayout.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
XrdFstOfsReplicaLayout::XrdFstOfsReplicaLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *outerror) : XrdFstOfsLayout(thisFile, "replica", lid, outerror)
{
  nReplica = XrdCommonLayoutId::GetStripeNumber(lid) + 1; // this 1=0x0 16=0xf :-)
  replicaClient = 0;
  replicaUrl = "";
  ioLocal = true;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaLayout::open(const char                *path,
			     XrdSfsFileOpenMode   open_mode,
			     mode_t               create_mode,
			     const XrdSecEntity        *client,
			     const char                *opaque)
{
  const char* index = ofsFile->openOpaque->Get("mgm.replicaindex");

  
  if (index) {
    replicaIndex = atoi(index);
    if ( (replicaIndex<0) || (replicaIndex>XrdCommonLayoutId::kSixteenStripe) ) {
      eos_err("illegal replica index %d", replicaIndex);
     return gOFS.Emsg("ReplicaOpen",*error, EINVAL, "open replica - illegal replica index found", index);
    }
    ioLocal = true;
  } else {
    replicaIndex = -1;
    ioLocal = false;
  }
  XrdOucString reptag = "mgm.url"; reptag += replicaIndex;
  
  const char* rep = ofsFile->capOpaque->Get(reptag.c_str());
  if (!rep) {
    eos_err("Failed to open replica - missing url for replica %s", reptag.c_str());
    return gOFS.Emsg("ReplicaOpen",*error, EINVAL, "open replica - missing url for replica ", reptag.c_str());
  }
  replicaUrl = rep;
  const char* val;
  int envlen;
  XrdOucString remoteOpenOpaque = ofsFile->openOpaque->Env(envlen);

  if ( (val = ofsFile->openOpaque->Get("mgm.replicaindex"))) {
    XrdOucString oldindex = "mgm.replicaindex=";
    XrdOucString newindex = "mgm.replicaindex=";
    oldindex += val;
    newindex += (replicaIndex+1);
    remoteOpenOpaque.replace(oldindex.c_str(),newindex.c_str());
  } else {
    remoteOpenOpaque += "&mgm.replicaindex=";
    remoteOpenOpaque += (replicaIndex+1);
  }


  if (!ofsFile->isRW) {
    // read case
    if (ioLocal) {
      // read from this box
      return ofsFile->openofs(path, open_mode, create_mode, client, opaque);
    } else {
      // read from remote box
      // create remote url
      replicaUrl += "?"; replicaUrl += remoteOpenOpaque;
      // open next replicas 
      replicaClient = new XrdClient(replicaUrl.c_str());
      // open remote file
      if (!replicaClient->Open(0,0, false)) {
	// open failed
	eos_err("Failed to open replica - remote open failed on ", replicaUrl.c_str());
	return gOFS.Emsg("ReplicaOpen",*error, EIO, "open replica - remote open failed ", replicaUrl.c_str());
      }
    }
    return SFS_OK;
  } else {
    // write case
    // check if we are the last one in the chain?
    if ( (replicaIndex+1) < nReplica) {
      // create remote url
      replicaUrl += "?"; replicaUrl += remoteOpenOpaque;
      // open next replicas 
      replicaClient = new XrdClient(replicaUrl.c_str());
      // open remote file
      if (!replicaClient->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | kXR_open_updt | kXR_new, false)) {
	// open failed
	eos_err("Failed to open replica - remote open failed on ", replicaUrl.c_str());
	return gOFS.Emsg("ReplicaOpen",*error, EIO, "open replica - remote open failed ", replicaUrl.c_str());
      }
    }
    return SFS_OK;
  }
}

/*----------------------------------------------------------------------------*/
XrdFstOfsReplicaLayout::~XrdFstOfsReplicaLayout() 
{
  if (replicaClient) {
    delete replicaClient;
  }
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->readofs(offset, buffer,length);
}

/*----------------------------------------------------------------------------*/
int 
XrdFstOfsReplicaLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  int rc1 = SFS_OK;
  int rc2 = 0;

  if (ioLocal) {
    rc1= ofsFile->writeofs(offset, buffer, length);
  }
  if (replicaClient) {
    rc2 = replicaClient->Write(buffer, offset, length);
  }

  if (rc1 <0) {
    eos_err("Failed to write local replica - write failed - %llu %llu %s", offset, length, replicaUrl.c_str());
    return gOFS.Emsg("ReplicaWrite",*error, errno, "write local replica - write failed", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to write remote replica - write failed - %llu %llu %s", offset, length, replicaUrl.c_str());
    return gOFS.Emsg("Repl}icaWrite",*error, EIO, "write remote replica - write failed", replicaUrl.c_str());
  }
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaLayout::truncate(XrdSfsFileOffset offset)
{
  int rc1 = SFS_OK;
  int rc2 = 0;

  if (ioLocal) {
    rc1= ofsFile->truncateofs(offset);
  }

  if (replicaClient) {
    rc2 = replicaClient->Truncate(offset);
  }

  if (rc1 <0) {
    eos_err("Failed to truncate local replica - %llu %s", offset, replicaUrl.c_str());
    return gOFS.Emsg("ReplicaTruncate",*error, errno, "truncate local replica", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to truncate local replica - %llu %s", offset, replicaUrl.c_str());
    return gOFS.Emsg("Repl}icaTruncate",*error, EIO, "truncate remote replica", replicaUrl.c_str());
  }
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaLayout::sync() 
{
  int rc1 = SFS_OK;
  int rc2 = 0;
  if (ioLocal) {
    rc1 = ofsFile->syncofs();
  }

  if (replicaClient) {
    rc2 = replicaClient->Sync();
  }  
  
  if (rc1 <0) {
    eos_err("Failed to sync local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("ReplicaSync",*error, errno, "sync local replica", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to sync local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("Repl}icaSync",*error, EIO, "sync remote replica", replicaUrl.c_str());
  }  
  return rc1;
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaLayout::close()
{
  int rc1 = SFS_OK;
  int rc2 = 0;
  if (ioLocal) {
    rc1 = ofsFile->closeofs();
  }

  if (replicaClient) {
    rc2 = replicaClient->Close();
  }  
  
  if (rc1 <0) {
    eos_err("Failed to close local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("ReplicaClose",*error, errno, "close local replica", replicaUrl.c_str());
  }
  if (!rc2) {
    eos_err("Failed to close local replica - %s", replicaUrl.c_str());
    return gOFS.Emsg("ReplicaClose",*error, EIO, "close remote replica", replicaUrl.c_str());
  }  
  return rc1;
}

/*----------------------------------------------------------------------------*/
