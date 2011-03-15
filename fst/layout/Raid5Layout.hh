#ifndef __EOSFST_RAID5LAYOUT_HH__
#define __EOSFST_RAID5LAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/Layout.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class Raid5Layout : public Layout {
private:
  off_t lastOffset;
  int nStripes;
  int stripeWidth; // this is given in 1024bytes units
  bool fileDegraded;
  std::vector<char*> parityBuffer;
  off_t lastParity;

  XrdClient* replicaClient[eos::common::LayoutId::kSixteenStripe];
  XrdOucString replicaUrl[eos::common::LayoutId::kSixteenStripe];;

public:

  Raid5Layout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error);

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

  virtual ~Raid5Layout();
};

EOSFSTNAMESPACE_END

#endif
