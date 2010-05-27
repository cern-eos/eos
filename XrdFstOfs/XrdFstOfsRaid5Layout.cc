/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsRaid5Layout.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
XrdFstOfsRaid5Layout::XrdFstOfsRaid5Layout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *outerror) : XrdFstOfsLayout(thisFile, "replica", lid, outerror)
{
  nStripes = XrdCommonLayoutId::GetStripeNumber(lid) + 1; // this 1=0x0 16=0xf :-)
  stripeWidth = XrdCommonLayoutId::GetStripeWidth(lid) * 1024; // this were kb units
 
  for (int i=0; i< XrdCommonLayoutId::kSixteenStripe; i++) {
    replicaClient[i] = 0;
    replicaUrl[i] = "";
  }
  fileDegraded=false;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsRaid5Layout::open(const char                *path,
			   XrdSfsFileOpenMode   open_mode,
			   mode_t               create_mode,
			   const XrdSecEntity        *client,
			   const char                *opaque)
{
  if (nStripes < 2) {
    // that makes no sense!
    eos_err("Failed to open raid5 layout - stripe size should be atleast 2");
    return gOFS.Emsg("Raid5Open",*error, EIO, "open stripes - stripe size must be atleast 2");
  }

  if (stripeWidth < 64) {
    // that makes no sense!
    eos_err("Failed to open raid5 layout - stripe width should be atleast 64");
    return gOFS.Emsg("Raid5Open",*error, EIO, "open stripes - stripe width must be atleast 64");
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
	  return gOFS.Emsg("Raid5Open",*error, EIO, "open stripes - remote open failed ", replicaUrl[i].c_str());
	}
      } else {
	// read case
	if (!replicaClient[i]->Open(0,0 , false)) {
	  // open failed
	  eos_err("Failed to open stripes - remote open failed on ", replicaUrl[i].c_str());
	  return gOFS.Emsg("Raid5Open",*error, EIO, "open stripes - remote open failed ", replicaUrl[i].c_str());
	}
      }
    }
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
XrdFstOfsRaid5Layout::~XrdFstOfsRaid5Layout() 
{
  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      delete replicaClient[i];
    }
  }
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsRaid5Layout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{

  if (fileDegraded) {
    // rebuild using parity
  } else {
    size_t plength = length;
    off_t  poffset = 0;
    // straight forward read - we don't use asynchronous reads here, but we can do that later!

    // first read must align to stripe width and read a partial page
    int nclient = (offset/(stripeWidth))%nStripes;
    off_t noffset = offset/(nStripes) + offset%(stripeWidth);
    size_t nread = stripeWidth - noffset;
    
    if (!replicaClient[nclient]->Read(buffer, noffset, nread)) {
	// read error!
	return gOFS.Emsg("Raid5Read",*error, EIO, "read stripe - read failed ", replicaUrl[nclient].c_str());
    }
    
    // now we align to the next full page offset
    poffset = offset/(nStripes) + (stripeWidth);
    plength = length - nread;

    while (plength) {
      int nread = ((plength)> ((size_t)stripeWidth))?stripeWidth:plength;
      int nclient = (poffset/(stripeWidth))%nStripes;
      if (!replicaClient[nclient]->Read(buffer+(poffset-offset), poffset, nread)) {
	// read error!
return gOFS.Emsg("Raid5Read",*error, EIO, "read stripe - read failed ", replicaUrl[nclient].c_str());
      }
      plength -= nread;
      poffset += nread;
    }

    // well, everything read 
  }
  return length;
}

/*----------------------------------------------------------------------------*/
int 
XrdFstOfsRaid5Layout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsRaid5Layout::truncate(XrdSfsFileOffset offset)
{
  int nclient = (offset / stripeWidth) % nStripes;
  for (int i=0; i< nStripes; i++) {
    if (replicaClient[i]) {
      off_t newoffset;
      if (nclient == i) {
	// this client is responsible for an incomplete page!
	newoffset = (offset/ nStripes) + offset%(stripeWidth);
      } else {
	newoffset = (offset/ nStripes);
      }
      if (!replicaClient[i]->Truncate(newoffset)) {
	return gOFS.Emsg("Raid5Truncate",*error, EIO, "truncate stripe - truncate failed ", replicaUrl[i].c_str());	
      }
    }
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsRaid5Layout::sync() 
{
  for (int i=0; i< nStripes; i++) {
    if (!replicaClient[i]->Sync()) {
      return gOFS.Emsg("Raid5Sync",*error, EIO, "sync stripe - sync failed ", replicaUrl[i].c_str());	
    }
  }
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsRaid5Layout::close()
{
  for (int i=0; i< nStripes; i++) {
    if (!replicaClient[i]->Close()) {
      return gOFS.Emsg("Raid5Close",*error, EIO, "close stripe - close failed ", replicaUrl[i].c_str());	
    }
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
