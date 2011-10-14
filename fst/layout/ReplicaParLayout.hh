#ifndef __EOSFST_REPLICAPARLAYOUT_HH__
#define __EOSFST_REPLICAPARLAYOUT_HH__

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

class ReplicaParLayout : public Layout {
private:
  int nStripes;

  XrdClient* replicaClient[eos::common::LayoutId::kSixteenStripe];
  XrdOucString replicaUrl[eos::common::LayoutId::kSixteenStripe];;
  bool ioLocal;

public:

  ReplicaParLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error);

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
  virtual int remove();

  virtual ~ReplicaParLayout();
};

EOSFSTNAMESPACE_END

#endif
