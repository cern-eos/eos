#ifndef __XRDFSTOFS_RAID5LAYOUT_HH__
#define __XRDFSTOFS_RAID5LAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdFstOfs/XrdFstOfsFile.hh"
#include "XrdFstOfs/XrdFstOfsLayout.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

class XrdFstOfsRaid5Layout : public XrdFstOfsLayout {
private:
  int nStripes;
  int stripeWidth; // this is given in 1024bytes units
  bool fileDegraded;
  XrdClient* replicaClient[XrdCommonLayoutId::kSixteenStripe];
  XrdOucString replicaUrl[XrdCommonLayoutId::kSixteenStripe];;

public:

  XrdFstOfsRaid5Layout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error);

  virtual int open(const char                *path,
		   XrdSfsFileOpenMode   open_mode,
		   mode_t               create_mode,
		   const XrdSecEntity        *client,
		   const char                *opaque);
  
  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int truncate(XrdSfsFileOffset offset);
  virtual int sync();
  virtual int close();

  virtual ~XrdFstOfsRaid5Layout();
};

#endif
