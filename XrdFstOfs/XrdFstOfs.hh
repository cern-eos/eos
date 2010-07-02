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
#include "XrdFstOfs/XrdFstOfsLayoutPlugins.hh"
#include "XrdFstOfs/XrdFstOfsFile.hh"
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

  static void* Start(void *pp);

  virtual void Listen();
  virtual void Process(XrdMqMessage* message);
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
  friend class XrdFstOfsLayout;
  friend class XrdFstOfsReplicaLayout;
  friend class XrdFstOfsPlainLayout;
  friend class XrdFstOfsRaid5Layout;
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

  int            _rem(const char             *path,
		     XrdOucErrInfo    &out_error,
		     const XrdSecEntity     *client,
		     XrdOucEnv              *info = 0) ;
  
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

  int            CallManager(XrdOucErrInfo *error, const char* path, const char* manager, XrdOucString &capOpaqueFile);


  // this function deals with plugin calls
  int            FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);

  void           Boot(XrdOucEnv &env);
  bool           BootFs(XrdOucEnv &env, XrdOucString &response);
  void           SetDebug(XrdOucEnv &env);
  void           SendRtLog(XrdMqMessage* message);
  void           AutoBoot();

 
  XrdFstOfsClientAdminManager FstOfsClientAdminManager;
  XrdFstMessaging* FstOfsMessaging;      // -> messaging interface class
  XrdFstOfsStorage* FstOfsStorage;       // -> Meta data & filesytem store object

  XrdSysMutex OpenFidMutex;
  google::sparse_hash_map<unsigned long, google::sparse_hash_map<unsigned long long, unsigned int> > WOpenFid;
  google::sparse_hash_map<unsigned long, google::sparse_hash_map<unsigned long long, unsigned int> > ROpenFid;

  void OpenFidString(unsigned long fsid, XrdOucString &outstring);

  virtual ~XrdFstOfs() {};
};

/*----------------------------------------------------------------------------*/
extern XrdFstOfs gOFS;

/*----------------------------------------------------------------------------*/

#endif


