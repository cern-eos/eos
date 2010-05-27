#ifndef __XRDFSTOFS_LAYOUT_HH__
#define __XRDFSTOFS_LAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdFstOfs/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

class XrdFstOfsLayout : public XrdCommonLogId {
protected:
  XrdOucString Name;
  XrdFstOfsFile* ofsFile;
  unsigned int layOutId;
  XrdOucErrInfo* error;

public:

  XrdFstOfsLayout(XrdFstOfsFile* thisFile=0){Name = "";ofsFile = thisFile;}
  XrdFstOfsLayout(XrdFstOfsFile* thisFile,const char* name, int lid, XrdOucErrInfo *outerror){Name = name;ofsFile = thisFile;layOutId = lid; error = outerror;}

  const char* GetName() {return Name.c_str();}
  unsigned int GetLayOutId() { return layOutId;}

  virtual int open(const char                *path,
		   XrdSfsFileOpenMode   open_mode,
		   mode_t               create_mode,
		   const XrdSecEntity        *client,
		   const char                *opaque) = 0;

  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length) = 0;
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length) = 0; 
  virtual int truncate(XrdSfsFileOffset offset) = 0;
  virtual int sync() = 0;
  virtual int close() = 0;

  virtual ~XrdFstOfsLayout(){};
};

#endif
