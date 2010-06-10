#ifndef __XRDMGMOFS_MGMOFS__HH__
#define __XRDMGMOFS_MGMOFS__HH__

/*----------------------------------------------------------------------------*/
#include "XrdCapability/XrdCapability.hh"
#include "XrdCommon/XrdCommonMapping.hh"  
#include "XrdCommon/XrdCommonSymKeys.hh"
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdMqOfs/XrdMqMessaging.hh"
#include "XrdMgmOfs/XrdMgmProcInterface.hh"
#include "XrdMgmOfs/XrdMgmConfigEngine.hh"
#include "Namespace/IView.hh"
#include "Namespace/IFileMDSvc.hh"
#include "Namespace/IContainerMDSvc.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCms/XrdCmsFinder.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOfs/XrdOfsEvr.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
#include <dirent.h>
/*----------------------------------------------------------------------------*/
class XrdMgmMessaging : public XrdMqMessaging, public XrdCommonLogId {
public:
  // we have to clone the base class constructore otherwise we cannot run inside valgrind
  XrdMgmMessaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus=false, bool advisoryquery=false);
  virtual ~XrdMgmMessaging(){}
  
  virtual void Listen();
  virtual void Process(XrdMqMessage* newmessage);

  // listener thread startup
  static void* Start(void*);
};


/*----------------------------------------------------------------------------*/
class XrdMgmOfsDirectory : public XrdSfsDirectory , public XrdCommonLogId
{
public:

        int         open(const char              *dirName,
                         const XrdSecClientName  *client = 0,
                         const char              *opaque = 0);

        int         open(const char              *dirName,
			 XrdCommonMapping::VirtualIdentity &vid,
                         const char              *opaque = 0);

        const char *nextEntry();

        int         Emsg(const char *, XrdOucErrInfo&, int, const char *x,
                            const char *y="");
        int         close();

const   char       *FName() {return (const char *)fname;}

                    XrdMgmOfsDirectory(char *user=0) : XrdSfsDirectory(user)
                                {ateof = 0; fname = 0;
				 d_pnt = &dirent_full.d_entry; XrdCommonMapping::Nobody(vid);
				 XrdCommonLogId();dh =0;
                                }

                   ~XrdMgmOfsDirectory() {}
private:

  char           ateof;
  char          *fname;
  XrdOucString   entry;
  
  struct {struct dirent d_entry;
    char   pad[MAXNAMLEN];   // This is only required for Solaris!
  } dirent_full;
  
  struct dirent *d_pnt;
  
  XrdCommonMapping::VirtualIdentity vid;

  eos::ContainerMD* dh;
  eos::ContainerMD::FileMap::iterator dh_files;
  eos::ContainerMD::ContainerMap::iterator dh_dirs;
};

/*----------------------------------------------------------------------------*/
class XrdSfsAio;
class XrdMgmOfsFile : public XrdSfsFile ,  XrdCommonLogId
{
public:

        int            open(const char                *fileName,
                                  XrdSfsFileOpenMode   openMode,
                                  mode_t               createMode,
                            const XrdSecEntity        *client = 0,
                            const char                *opaque = 0);

        int            close();

        const char    *FName() {return fname;}


        int            Fscmd(const char* path,  const char* path2, const char* orgipath, const XrdSecEntity *client,  XrdOucErrInfo &error, const char* info) { return SFS_OK;}

        int            getMmap(void **Addr, off_t &Size)
                              {if (Addr) Addr = 0; Size = 0; return SFS_OK;}

        int            read(XrdSfsFileOffset   fileOffset,
                            XrdSfsXferSize     preread_sz) {return SFS_OK;}

        XrdSfsXferSize read(XrdSfsFileOffset   fileOffset,
                            char              *buffer,
                            XrdSfsXferSize     buffer_size);

        int            read(XrdSfsAio *aioparm);

        XrdSfsXferSize write(XrdSfsFileOffset   fileOffset,
                             const char        *buffer,
                             XrdSfsXferSize     buffer_size);

        int            write(XrdSfsAio *aioparm);

        int            sync();

        int            sync(XrdSfsAio *aiop);

        int            stat(struct stat *buf);

        int            truncate(XrdSfsFileOffset   fileOffset);

        int            getCXinfo(char cxtype[4], int &cxrsz) {return cxrsz = 0;}

        int            fctl(int, const char*, XrdOucErrInfo&) {return 0;}

        int            Emsg(const char *, XrdOucErrInfo&, int, const char *x,
                            const char *y="");

                       XrdMgmOfsFile(char *user=0) : XrdSfsFile(user)
                                          {oh = 0; fname = 0; openOpaque=0;XrdCommonMapping::Nobody(vid);fileId=0; procCmd=0; XrdCommonLogId();fmd=0;}
                      ~XrdMgmOfsFile() {
			if (oh) close();
			if (openOpaque) {delete openOpaque; openOpaque = 0;}
			if (procCmd) { delete procCmd; procCmd = 0;}
		      }
		      
private:

  int   oh;
  
  char *fname;

  XrdOucEnv *openOpaque;
  unsigned long fileId;

  XrdMgmProcCommand* procCmd;

  eos::FileMD* fmd;

  XrdCommonMapping::VirtualIdentity vid;
};

/*----------------------------------------------------------------------------*/

class XrdMgmOfs : public XrdSfsFileSystem , public XrdCommonLogId
{
  friend class XrdMgmOfsFile;
  friend class XrdMgmOfsDirectory;
  friend class XrdMgmOfsStats;

public:

// Object Allocation Functions
//
        XrdSfsDirectory *newDir(char *user=0)
                        {return (XrdSfsDirectory *)new XrdMgmOfsDirectory(user);}

        XrdSfsFile      *newFile(char *user=0)
                        {return      (XrdSfsFile *)new XrdMgmOfsFile(user);}

// Other Functions
//
        int            chmod(const char             *Name,
                                   XrdSfsMode        Mode,
                                   XrdOucErrInfo    &out_error,
                             const XrdSecEntity *client = 0,
                             const char             *opaque = 0);

        int            exists(const char                *fileName,
                                    XrdSfsFileExistence &exists_flag,
                                    XrdOucErrInfo       &out_error,
                              const XrdSecEntity    *client = 0,
                              const char                *opaque = 0);

        int            _exists(const char                *fileName,
                                    XrdSfsFileExistence &exists_flag,
                                    XrdOucErrInfo       &out_error,
                              const XrdSecEntity    *client = 0,
                              const char                *opaque = 0);

        int            _exists(const char                *fileName,
                                    XrdSfsFileExistence &exists_flag,
                                    XrdOucErrInfo       &out_error
,			            XrdCommonMapping::VirtualIdentity &vid,
                              const char                *opaque = 0);

  enum eFSCTL { kFsctlMgmOfsOffset= 40000};

        int            FSctl(const int               cmd,
			     XrdSfsFSctl            &args,
			     XrdOucErrInfo          &error,
			     const XrdSecEntity     *client);
    
        int            fsctl(const int               cmd,
                             const char             *args,
                                   XrdOucErrInfo    &out_error,
         	             const XrdSecEntity *client = 0);
	  

        int            getStats(char *buff, int blen) {return 0;}

const   char          *getVersion();


        int            mkdir(const char             *dirName,
                                   XrdSfsMode        Mode,
                                   XrdOucErrInfo    &out_error,
                             const XrdSecClientName *client = 0,
                             const char             *opaque = 0);

        int            _mkdir(const char             *dirName,
			      XrdSfsMode        Mode,
			      XrdOucErrInfo    &out_error,
			      XrdCommonMapping::VirtualIdentity &vid,
			      const char             *opaque = 0);

        int       stageprepare(const char           *path, 
			       XrdOucErrInfo        &error, 
			       const XrdSecEntity   *client, 
			       const char* info);
  
        int            prepare(      XrdSfsPrep       &pargs,
                                     XrdOucErrInfo    &out_error,
			     const XrdSecEntity *client = 0);

        int            rem(const char             *path,
                                 XrdOucErrInfo    &out_error,
                           const XrdSecEntity *client = 0,
                           const char             *opaque = 0);

        int            _rem(const char             *path,
			    XrdOucErrInfo    &out_error,
			    XrdCommonMapping::VirtualIdentity &vid,
			    const char             *opaque = 0);

        int            _find(const char             *path,
			     XrdOucErrInfo    &out_error,
			     XrdCommonMapping::VirtualIdentity &vid,
			     std::vector< std::vector<std::string> > &found_dirs,
			     std::vector< std::vector<std::string> > &found_files );
   
        int            remdir(const char             *dirName,
                                    XrdOucErrInfo    &out_error,
                              const XrdSecEntity *client = 0,
                              const char             *opaque = 0);

        int            _remdir(const char             *dirName,
			       XrdOucErrInfo    &out_error,
			       XrdCommonMapping::VirtualIdentity &vid,
			       const char             *opaque = 0);

        int            rename(const char             *oldFileName,
                              const char             *newFileName,
                                    XrdOucErrInfo    &out_error,
                              const XrdSecEntity *client = 0,
                              const char             *opaqueO = 0,
                              const char             *opaqueN = 0);

        int            stat(const char             *Name,
                                  struct stat      *buf,
                                  XrdOucErrInfo    &out_error,
                            const XrdSecEntity *client = 0,
                            const char             *opaque = 0);

        int            _stat(const char             *Name,
                                  struct stat      *buf,
                                  XrdOucErrInfo    &out_error,
 			          XrdCommonMapping::VirtualIdentity &vid,
                            const char             *opaque = 0);


        int            lstat(const char            *Name,
                                  struct stat      *buf,
                                  XrdOucErrInfo    &out_error,
                            const XrdSecEntity *client = 0,
                            const char             *opaque = 0);

        int            stat(const char             *Name,
                                  mode_t           &mode,
                                  XrdOucErrInfo    &out_error,
                            const XrdSecEntity     *client = 0,
                            const char             *opaque = 0)
                       {struct stat bfr;
                        int rc = stat(Name, &bfr, out_error, client,opaque);
                        if (!rc) mode = bfr.st_mode;
                        return rc;
                       }

        int            truncate(const char*, XrdSfsFileOffset, XrdOucErrInfo&, const XrdSecEntity*, const char*);


        int            symlink(const char*, const char*, XrdOucErrInfo&, const XrdSecEntity*, const char*);

        int            readlink(const char*, XrdOucString& ,  XrdOucErrInfo&, const XrdSecEntity*, const char*);

        int            access(const char*, int mode, XrdOucErrInfo&, const XrdSecEntity*, const char*);

        int            utimes(const char*, struct timeval *tvp, XrdOucErrInfo&, const XrdSecEntity*, const char*);

// Common functions
//
static  int            Mkpath(const char *path, mode_t mode, 
			      const char *info=0, XrdSecEntity* client = 0, XrdOucErrInfo* error = 0) { return SFS_ERROR;}

        int            Emsg(const char *, XrdOucErrInfo&, int, const char *x,
                            const char *y="");

                       XrdMgmOfs(XrdSysError *lp);
virtual               ~XrdMgmOfs() {}

virtual int            Configure(XrdSysError &);
virtual bool           Init(XrdSysError &);
        int            Stall(XrdOucErrInfo &error, int stime, const char *msg);
  
        char          *ConfigFN;       
  
        XrdMgmConfigEngine* ConfigEngine;    // storing/restoring configuration
        XrdCapability*  CapabilityEngine;    // -> authorization module for token encryption/decryption
  
        XrdOucString     MgmOfsBrokerUrl;    // -> Url of the message broker
        XrdMgmMessaging* MgmOfsMessaging;    // -> messaging interface class
        XrdOucString     MgmDefaultReceiverQueue; // -> Queue where we are sending to by default
        XrdOucString     MgmOfsName;         // -> mount point of the filesystem
        XrdOucString     MgmOfsTargetPort;   // -> xrootd port where redirections go on the OSTs -default is 1094
        XrdOucString     MgmOfsQueue;        // -> our mgm queue name
        XrdOucString     MgmConfigDir;       // Directory where config files are stored
        XrdOucString     AuthLib;            // -> path to a possible authorizationn library
        bool             authorize;          // -> determins if the autorization should be applied or not
        XrdAccAuthorize *Authorization;      // -> Authorization   Service
        bool             IssueCapability;    // -> defines if the Mgm issues capabilities
  
        eos::IContainerMDSvc  *eosDirectoryService;              // -> changelog for directories
        eos::IFileMDSvc *eosFileService;                         // -> changelog for files
	eos::IView      *eosView;            // -> hierarchical view of the namespace
        XrdSysMutex      eosViewMutex;       // -> mutex making the namespace single threaded
        XrdOucString     MgmMetaLogDir;      //  Directory containing the meta data (change) log files

protected:
        char*            HostName;           // -> our hostname as derived in XrdOfs
        char*            HostPref;           // -> our hostname as derived in XrdOfs without domain
        XrdOucString     ManagerId;          // -> manager id in <host>:<port> format

private:
  static  XrdSysError *eDest;
  XrdCommonMapping::VirtualIdentity vid;
};
/*----------------------------------------------------------------------------*/
extern XrdMgmOfs* gOFS;
/*----------------------------------------------------------------------------*/

#endif
