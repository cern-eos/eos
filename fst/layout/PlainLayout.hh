#ifndef __EOSFST_PLAINLAYOUT_HH__
#define __EOSFST_PLAINLAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/Layout.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class PlainLayout : public Layout{
public:

  PlainLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error) : Layout(thisFile, "plain", lid, error) {};

  virtual int open(const char                *path,
		   XrdSfsFileOpenMode   open_mode,
		   mode_t               create_mode,
		   const XrdSecEntity        *client,
		   const char                *opaque);

  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int truncate(XrdSfsFileOffset offset);
  virtual int fallocate(XrdSfsFileOffset length);
  virtual int sync();
  virtual int close();

  virtual ~PlainLayout(){};
};

EOSFSTNAMESPACE_END
#endif
