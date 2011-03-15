#ifndef __EOSFST_LAYOUT_HH__
#define __EOSFST_LAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class XrdFstOfsFile;

class Layout : public eos::common::LogId {
protected:
  XrdOucString Name;
  XrdFstOfsFile* ofsFile;
  unsigned int layOutId;
  XrdOucErrInfo* error;
  bool isEntryServer;

public:

  Layout(XrdFstOfsFile* thisFile=0){Name = "";ofsFile = thisFile;}
  Layout(XrdFstOfsFile* thisFile,const char* name, int lid, XrdOucErrInfo *outerror){Name = name;ofsFile = thisFile;layOutId = lid; error = outerror; isEntryServer=true;}

  const char* GetName() {return Name.c_str();}
  unsigned int GetLayOutId() { return layOutId;}

  virtual int open(const char                *path,
		   XrdSfsFileOpenMode   open_mode,
		   mode_t               create_mode,
		   const XrdSecEntity        *client,
		   const char                *opaque) = 0;

  virtual bool IsEntryServer() { return isEntryServer; }

  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length) = 0;
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length) = 0; 
  virtual int truncate(XrdSfsFileOffset offset) = 0;
  virtual int sync() = 0;
  virtual int close() = 0;
  
  virtual ~Layout(){};
};

EOSFSTNAMESPACE_END

#endif
