/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsReplicaParLayout.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
XrdFstOfsReplicaParLayout::XrdFstOfsReplicaParLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *outerror) : XrdFstOfsLayout(thisFile, "replica", lid, outerror)
{
  nStripes = XrdCommonLayoutId::GetStripeNumber(lid) + 1; // this 1=0x0 16=0xf :-)

  for (int i=0; i< XrdCommonLayoutId::kSixteenStripe; i++) {
    replicaClient[i] = 0;
    replicaUrl[i] = "";
  }
  ioLocal=false;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaParLayout::open(const char                *path,
			   XrdSfsFileOpenMode   open_mode,
			   mode_t               create_mode,
			   const XrdSecEntity        *client,
			   const char                *opaque)
{

  // no replica index definition indicates that his is gateway access just forwarding to another remote server

  int replicaIndex = 0;
  int replicaHead  = 0;

  bool isGateWay=false;
  bool isHeadServer=false;

  const char* index = ofsFile->openOpaque->Get("mgm.replicaindex");
  
  if (index) {
    replicaIndex = atoi(index);
    if ( (replicaIndex<0) || (replicaIndex>XrdCommonLayoutId::kSixteenStripe) ) {
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
    if ( (replicaHead<0) || (replicaHead>XrdCommonLayoutId::kSixteenStripe) ) {
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

  int envlen;
  XrdOucString remoteOpenOpaque = ofsFile->openOpaque->Env(envlen);

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
	  replicaClient[i] = new XrdClient(replicaUrl[i].c_str());
	  
	  eos_info("Opening Layout Stripe %s\n", replicaUrl[i].c_str());
	  // write case
	  if (!replicaClient[i]->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | kXR_open_updt | kXR_new, false)) {
	    // open failed
	    eos_err("Failed to open stripes - remote open failed on ", replicaUrl[i].c_str());
	    return gOFS.Emsg("ReplicaParOpen",*error, EIO, "open stripes - remote open failed ", replicaUrl[i].c_str());
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
XrdFstOfsReplicaParLayout::~XrdFstOfsReplicaParLayout() 
{
  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      delete replicaClient[i];
    }
  }
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaParLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  int rc1 = 0;
  int rc2 = true;

  if (ioLocal) {
    rc1= ofsFile->readofs(offset, buffer, length);
  } else {
    if (replicaClient) {
      rc2 = replicaClient[0]->Read(buffer, offset, length);
      if (rc2 != length) {
	eos_err("Failed to read remote replica - read failed - %llu %llu %s", offset, length, replicaUrl[0].c_str());
	return gOFS.Emsg("ReplicaParRead",*error, EIO, "read remote replica - read failed", replicaUrl[0].c_str());
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
XrdFstOfsReplicaParLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
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
    return gOFS.Emsg("ReplicaWrite",*error, EIO, "write remote replica - write failed");
  }
  return rc1;

}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaParLayout::truncate(XrdSfsFileOffset offset)
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
    return gOFS.Emsg("ReplicaParTruncate",*error, EIO, "truncate remote replica");
  }
  return rc1;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaParLayout::sync() 
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
    return gOFS.Emsg("ReplicaParSync",*error, EIO, "sync remote replica");
  }  
  return rc1;
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsReplicaParLayout::close()
{
  int rc1 = SFS_OK;
  int rc2 = true;
  if (ioLocal) {
    rc1 = ofsFile->closeofs();
  }

  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Sync()) {
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
    return gOFS.Emsg("ReplicaClose",*error, EIO, "close remote replica", "");
  }  
  return rc1;
}

/*----------------------------------------------------------------------------*/

//  LocalWords:  outerror XrdFstOfsRaid
