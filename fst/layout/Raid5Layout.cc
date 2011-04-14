/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfs.hh"
#include "fst/layout/Raid5Layout.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Raid5Layout::Raid5Layout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *outerror) : Layout(thisFile, "replica", lid, outerror)
{
  nStripes = eos::common::LayoutId::GetStripeNumber(lid) + 1; // this 1=0x0 16=0xf :-)
  stripeWidth = eos::common::LayoutId::GetBlocksize(lid);
 
  for (int i=0; i< eos::common::LayoutId::kSixteenStripe; i++) {
    replicaClient[i] = 0;
    replicaUrl[i] = "";
  }
  fileDegraded=false;
  lastOffset = 0;
  lastParity = 0;
}

/*----------------------------------------------------------------------------*/
int
Raid5Layout::open(const char                *path,
			   XrdSfsFileOpenMode   open_mode,
			   mode_t               create_mode,
			   const XrdSecEntity        *client,
			   const char                *opaque)
{
  if (nStripes < 2) {
    // that makes no sense!
    eos_err("Failed to open raid5 layout - stripe size should be atleast 2");
    return gOFS.Emsg("Raid5Open",*error, EREMOTEIO, "open stripes - stripe size must be atleast 2");
  }

  if (stripeWidth < 64) {
    // that makes no sense!
    eos_err("Failed to open raid5 layout - stripe width should be atleast 64");
    return gOFS.Emsg("Raid5Open",*error, EREMOTEIO, "open stripes - stripe width must be atleast 64");
  }

  for (int i=0; i< nStripes; i++) {
    parityBuffer[i] = new char[stripeWidth];
  }
    
  int nmissing = 0;
  // assign stripe urls'
  for (int i=0; i< nStripes; i++) {
    XrdOucString reptag = "mgm.url"; reptag += i;
    const char* rep = ofsFile->capOpaque->Get(reptag.c_str());
    // when we write we need all replicas, when we read we can afford to miss one!
    if ( (ofsFile->isRW && ( !rep ) ) || ((nmissing > 0 ) && (!rep))) {
      eos_err("Failed to open stripes - missing url for replica %s", reptag.c_str());
      return gOFS.Emsg("Raid5Open",*error, EINVAL, "open stripes - missing url for replica ", reptag.c_str());
    }
    if (!rep) {
      nmissing++;
      fileDegraded=true;
      replicaUrl[i] = "";
    } else {
      replicaUrl[i] = rep;
    }
  }

  for (int i=0; i< nStripes; i++) {
    // open all the replica's available

    if (replicaUrl[i].length()) {
      replicaClient[i] = new XrdClient(replicaUrl[i].c_str());
      
      if (ofsFile->isRW) {
	// write case
	if (!replicaClient[i]->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | kXR_open_updt | kXR_new, false)) {
	  // open failed
	  eos_err("Failed to open stripes - remote open failed on ", replicaUrl[i].c_str());
	  return gOFS.Emsg("Raid5Open",*error, EREMOTEIO, "open stripes - remote open failed ", replicaUrl[i].c_str());
	}
      } else {
	// read case
	if (!replicaClient[i]->Open(0,0 , false)) {
	  // open failed
	  eos_err("Failed to open stripes - remote open failed on ", replicaUrl[i].c_str());
	  return gOFS.Emsg("Raid5Open",*error, EREMOTEIO, "open stripes - remote open failed ", replicaUrl[i].c_str());
	}
      }
    }
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
Raid5Layout::~Raid5Layout() 
{
  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      delete replicaClient[i];
    }
  }
}


/*----------------------------------------------------------------------------*/
int
Raid5Layout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{

  if (fileDegraded) {
    // rebuild using parity
  } else {
    // straight forward read - we don't use asynchronous reads here, but we can do that later!

    // first read must align to stripe width and read a partial page
    int nclient = (offset/(stripeWidth))%(nStripes-1);
    int  nread = stripeWidth - (offset %stripeWidth);
    int aread = 0;

    if ((!(aread=replicaClient[nclient]->Read(buffer, offset, nread))) || (aread != nread)) {
      // read error!
      return gOFS.Emsg("Raid5Read",*error, EREMOTEIO, "read stripe - read failed ", replicaUrl[nclient].c_str());
    }

    offset += aread;
    length -= aread;
    buffer += aread;
    while (length) {
      int nread = ((length)> ((XrdSfsXferSize)stripeWidth))?stripeWidth:length;
      int nclient = (offset/(stripeWidth))%(nStripes-1);
      int aread=0;
      if ((!(aread = replicaClient[nclient]->Read(buffer, offset, nread))) || (aread != nread)) {
	// read error!
	return gOFS.Emsg("Raid5Read",*error, EREMOTEIO, "read stripe - read failed ", replicaUrl[nclient].c_str());
      }
      length -= nread;
      offset += nread;
      buffer += nread;
    }

    // well, everything read 
  }
  return length;
}

/*----------------------------------------------------------------------------*/
int 
Raid5Layout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  // currently we support only sequential writes
  if (offset != lastOffset) {
    return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "write stripe - no sequential write requested ", replicaUrl[0].c_str());
  }

  if (offset % stripeWidth) {
    return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "write stripe - offset is not stripe width aligned", replicaUrl[0].c_str());
  }
  
  while (length) {
    int nclient = (offset / stripeWidth) % (nStripes-1);
    size_t nwrite = (length<stripeWidth)? length: stripeWidth;

    if (!replicaClient[nclient]->Write(buffer, offset, nwrite)) {
      // write error!
      return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "write stripe - write failed ", replicaUrl[nclient].c_str());
    }
    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;

    if (offset == (lastParity + ( (nStripes-1) * stripeWidth ))) {
      // compute parity and commit
      for (unsigned int i=0; i< (unsigned int)(nStripes-1); i++) {
	int aread =0;
	if ((!(aread = replicaClient[i]->Read(parityBuffer[i], lastParity + (i*stripeWidth), stripeWidth))) || (aread != stripeWidth)) {
	  return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "read stripe - read for parity computation failed ", replicaUrl[i].c_str());
	}
      }
      // compute parity
      char* paritybuffer = parityBuffer[nStripes-1];
      memset(paritybuffer,0,stripeWidth);
      for (unsigned int s=0; s< (unsigned int)(nStripes-1); s++) {
	for (int i=0; i< stripeWidth; i++) {
	  *paritybuffer ^= *(parityBuffer[s]+i);
	}
      }
      // write parity
      if (!replicaClient[nStripes-1]->Write(parityBuffer[nStripes-1], lastParity, stripeWidth)) {
	return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "write parity stripe - write for parity failed ", replicaUrl[nStripes-1].c_str());
      }
      lastParity = offset;
    }
  }
  
  lastOffset = offset;

  return 0;
}

/*----------------------------------------------------------------------------*/
int
Raid5Layout::truncate(XrdSfsFileOffset offset)
{
  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      if (!replicaClient[i]->Truncate(offset)) {
	return gOFS.Emsg("Raid5Truncate",*error, EREMOTEIO, "truncate stripe - truncate failed ", replicaUrl[i].c_str());	
      }
    }
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
Raid5Layout::sync() 
{
  for (int i=0; i< nStripes; i++) {
    if (!replicaClient[i]->Sync()) {
      return gOFS.Emsg("Raid5Sync",*error, EREMOTEIO, "sync stripe - sync failed ", replicaUrl[i].c_str());	
    }
  }
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int
Raid5Layout::close()
{
  // write last parity chunk
  if (lastParity != lastOffset) {
    // compute last parity chunk

    // compute parity and commit
    for (unsigned int i=0; i< (unsigned int)(nStripes-1); i++) {
      int aread =0;
      if (!(aread = replicaClient[i]->Read(parityBuffer[i], lastParity + (i*stripeWidth), stripeWidth))) {
	return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "read stripe - read for parity computation failed ", replicaUrl[i].c_str());
      }
      if (aread != stripeWidth) {
	// memset the rest
	memset(parityBuffer[i]+aread, 0, stripeWidth-aread);
      }
    }

    // compute parity
    char* paritybuffer = parityBuffer[nStripes-1];
    memset(paritybuffer,0,stripeWidth);
    for (unsigned int s=0; s< (unsigned int)(nStripes-1); s++) {
      for (int i=0; i< stripeWidth; i++) {
	*paritybuffer ^= *(parityBuffer[s]+i);
      }
    }
    // write parity
    if (!replicaClient[nStripes-1]->Write(parityBuffer[nStripes-1], lastParity, stripeWidth)) {
      return gOFS.Emsg("Raid5Write",*error, EREMOTEIO, "write parity stripe - write for parity failed ", replicaUrl[nStripes-1].c_str());
    }
    lastParity = lastOffset;
  }
  
  for (int i=0; i< nStripes; i++) {
    if (!replicaClient[i]->Close()) {
      return gOFS.Emsg("Raid5Close",*error, EREMOTEIO, "close stripe - close failed ", replicaUrl[i].c_str());	
    }
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END

