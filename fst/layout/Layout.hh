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


// -------------------------------------------------------------------------------------------
// we use this truncate offset (1TB) to indicate that a file should be deleted during the close 
// there is no better interface usable via XrdClient to communicate a deletion on a open file

#define EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN 1024 * 1024 * 1024 * 1024ll

class Layout : public eos::common::LogId {
protected:
  XrdOucString Name;
  XrdOucString LocalReplicaPath;

  XrdFstOfsFile* ofsFile;
  unsigned int layOutId;
  XrdOucErrInfo* error;
  bool isEntryServer;
  int  blockChecksum;

public:

  Layout(XrdFstOfsFile* thisFile=0){Name = "";ofsFile = thisFile;}
  Layout(XrdFstOfsFile* thisFile,const char* name, int lid, XrdOucErrInfo *outerror){
    Name = name; ofsFile = thisFile; layOutId = lid; error = outerror; isEntryServer=true;
    blockChecksum=eos::common::LayoutId::GetBlockChecksum(lid);
    LocalReplicaPath = "";
  }

  const char* GetName() {return Name.c_str();}
  const char* GetLocalReplicaPath() { return LocalReplicaPath.c_str();}

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
  virtual int fallocate(XrdSfsFileOffset lenght) {return 0;}
  virtual int remove() {return 0;} 
  virtual int sync() = 0;
  virtual int close() = 0;
  
  virtual ~Layout(){};
};

EOSFSTNAMESPACE_END

#endif
