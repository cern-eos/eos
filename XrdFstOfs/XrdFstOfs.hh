#ifndef __XRDFSTOFS_FSTOFS_HH__
#define __XRDFSTOFS_FSTOFS_HH_

/*----------------------------------------------------------------------------*/
#include "XrdCapability/XrdCapability.hh"
#include "XrdCommon/XrdCommonSymKeys.hh"
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdFstOfs/XrdFstOfsClientAdmin.hh"
#include "XrdMqOfs/XrdMqMessaging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
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


  XrdFstOfsFile(const char* user) : XrdOfsFile(user){openOpaque = NULL; capOpaque = NULL; fstPath=""; XrdCommonLogId::XrdCommonLogId(); closed=false;}
  virtual ~XrdFstOfsFile() {
    close();
    if (openOpaque) {delete openOpaque; openOpaque=NULL;}
    if (capOpaque)  {delete capOpaque;  capOpaque =NULL;}
  }

private:
  XrdOucEnv*   openOpaque;
  XrdOucEnv*   capOpaque;
  XrdOucString fstPath;
  unsigned long fileId;
  bool         closed;
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
                        const char             *info = 0) { return SFS_OK;}
  
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

 
  static XrdOucHash<XrdFstOfsClientAdmin> *gClientAdminTable;

  XrdOucString     FstOfsBrokerUrl;      // -> Url of the message broker
  XrdFstMessaging* FstOfsMessaging;      // -> messaging interface class
  XrdOucString     FstDefaultReceiverQueue; // -> Queue where we are sending to by default
  bool             autoBoot;             // -> indicates if the node tries to boot automatically or waits for a boot message from a master
  virtual ~XrdFstOfs() {};
};

/*----------------------------------------------------------------------------*/
extern XrdFstOfs gOFS;
/*----------------------------------------------------------------------------*/

#endif


