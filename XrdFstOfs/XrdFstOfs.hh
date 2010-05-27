#ifndef __XRDFSTOFS_FSTOFS_HH__
#define __XRDFSTOFS_FSTOFS_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCapability/XrdCapability.hh"
#include "XrdCommon/XrdCommonSymKeys.hh"
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdFstOfs/XrdFstOfsClientAdmin.hh"
#include "XrdFstOfs/XrdFstOfsStorage.hh"
#include "XrdFstOfs/XrdFstOfsConfig.hh"
#include "XrdFstOfs/XrdFstOfsChecksumPlugins.hh"
#include "XrdMqOfs/XrdMqMessaging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <sys/mman.h>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstMessaging : public XrdMqMessaging , public XrdCommonLogId {
public:
  XrdFstMessaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus = false, bool advisoryquery = false) : XrdMqMessaging(url,defaultreceiverqueue, advisorystatus, advisoryquery) {}
  virtual ~XrdFstMessaging(){ XrdCommonLogId();}

  virtual void Listen();
  virtual void Process(XrdMqMessage* message);
};

/*----------------------------------------------------------------------------*/
class XrdFstOfsFile : public XrdOfsFile, public XrdCommonLogId {
public:
  int          open(const char                *fileName,
		    XrdSfsFileOpenMode   openMode,
		    mode_t               createMode,
		    const XrdSecEntity        *client,
		    const char                *opaque = 0);
  
  int          close();

  int          read(XrdSfsFileOffset   fileOffset,   // Preread only
		      XrdSfsXferSize     amount);
  
  XrdSfsXferSize read(XrdSfsFileOffset   fileOffset,
		      char              *buffer,
		      XrdSfsXferSize     buffer_size);
  
  int          read(XrdSfsAio *aioparm);
  
  XrdSfsXferSize write(XrdSfsFileOffset   fileOffset,
		       const char        *buffer,
		       XrdSfsXferSize     buffer_size);
  
  int          write(XrdSfsAio *aioparm);

  int          sync();

  int          sync(XrdSfsAio *aiop);

  int          truncate(XrdSfsFileOffset   fileOffset);


  XrdFstOfsFile(const char* user) : XrdOfsFile(user){openOpaque = 0; capOpaque = 0; fstPath=""; XrdCommonLogId(); closed=false; haswrite=false; fMd = 0;checkSum = 0;}
  virtual ~XrdFstOfsFile() {
    close();
    if (openOpaque) {delete openOpaque; openOpaque=0;}
    if (capOpaque)  {delete capOpaque;  capOpaque =0;}
    // unmap the MD record
    if (fMd) {delete fMd; fMd = 0;}
    if (checkSum) { delete checkSum;}
  }

private:
  XrdOucEnv*   openOpaque;
  XrdOucEnv*   capOpaque;
  XrdOucString fstPath;
  XrdOucString Path;
  unsigned long fileId;
  bool         closed;
  bool         haswrite;
  XrdCommonFmd* fMd;
  XrdFstOfsChecksum* checkSum;
};

class XrdFstOfsDirectory : public XrdOfsDirectory, public XrdCommonLogId {
public:
  XrdFstOfsDirectory(const char *user) : XrdOfsDirectory(user){XrdCommonLogId();};
  virtual            ~XrdFstOfsDirectory() {}
};

/*----------------------------------------------------------------------------*/
class XrdFstOfs : public XrdOfs, public XrdCommonLogId {
  friend class XrdFstOfsDirectory;
  friend class XrdFstOfsFile;

private:

public:
  XrdSfsDirectory *newDir(char *user=0) {return (XrdSfsDirectory *) new XrdFstOfsDirectory(user);}
  XrdSfsFile *newFile(char *user=0) {return (XrdSfsFile *) new XrdFstOfsFile(user);}
 
  int Configure(XrdSysError &error);

  XrdFstOfs() {XrdCommonLogId(); }

  XrdSysError*        Eroute;          // used by the 

  // here we mask all illegal operations
  int            chmod(const char             *Name,
		       XrdSfsMode        Mode,
		       XrdOucErrInfo    &out_error,
		       const XrdSecEntity     *client,
		       const char             *opaque = 0) { return SFS_OK;}
  
  int            exists(const char                *fileName,
			XrdSfsFileExistence &exists_flag,
			XrdOucErrInfo       &out_error,
			const XrdSecEntity        *client,
			const char                *opaque = 0) { return SFS_OK;}


  int            fsctl(const int               cmd,
		       const char             *args,
		       XrdOucErrInfo    &out_error,
		       const XrdSecEntity     *client) { return SFS_OK; } 

  int            mkdir(const char             *dirName,
		       XrdSfsMode        Mode,
		       XrdOucErrInfo    &out_error,
		       const XrdSecEntity     *client,
		       const char             *opaque = 0) { return SFS_OK;}
  
  int            prepare(      XrdSfsPrep       &pargs,
			       XrdOucErrInfo    &out_error,
			       const XrdSecEntity     *client = 0) { return SFS_OK;}
  
  
  int            rem(const char             *path,
		     XrdOucErrInfo    &out_error,
		     const XrdSecEntity     *client,
		     const char             *info = 0) ;
  
  int            remdir(const char             *dirName,
			XrdOucErrInfo    &out_error,
			const XrdSecEntity     *client,
			const char             *info = 0) { return SFS_OK;}
  
  int            rename(const char             *oldFileName,
			const char             *newFileName,
			XrdOucErrInfo    &out_error,
			const XrdSecEntity     *client,
			const char             *infoO = 0,
			const char            *infoN = 0) {return SFS_OK;}

  int            stat(const char             *Name,
		      struct stat      *buf,
		      XrdOucErrInfo    &out_error,
		      const XrdSecEntity     *client,
                        const char             *opaque = 0) {memset(buf,0,sizeof(struct stat));return SFS_OK;}

  // this function deals with plugin calls
  int            FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);
  void           ThirdPartySetup(const char* transferdirectory, int slots, int rate);

  void           Boot(XrdOucEnv &env);
  bool           BootFs(XrdOucEnv &env, XrdOucString &response);
  void           SetDebug(XrdOucEnv &env);
  void           AutoBoot();

 
  XrdFstOfsClientAdminManager FstOfsClientAdminManager;
  XrdFstMessaging* FstOfsMessaging;      // -> messaging interface class
  XrdFstOfsStorage* FstOfsStorage;       // -> Meta data & filesytem store object
  virtual ~XrdFstOfs() {};
};

/*----------------------------------------------------------------------------*/
extern XrdFstOfs gOFS;

/*----------------------------------------------------------------------------*/

#endif


