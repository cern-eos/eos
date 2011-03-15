#ifndef __XRDFSTOFS_PLAINLAYOUT_HH__
#define __XRDFSTOFS_PLAINLAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdFstOfs/XrdFstOfsFile.hh"
#include "XrdFstOfs/XrdFstOfsLayout.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

class XrdFstOfsPlainLayout : public XrdFstOfsLayout{
public:

  XrdFstOfsPlainLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error) : XrdFstOfsLayout(thisFile, "plain", lid, error) {};

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

  virtual ~XrdFstOfsPlainLayout(){};
};

#endif
