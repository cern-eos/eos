#ifndef __EOSMGM_MGMOFS__HH__
#define __EOSMGM_MGMOFS__HH__

/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
#include "common/Mapping.hh"  
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/ClientAdmin.hh"
#include "common/GlobalConfig.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mgm/ProcInterface.hh"
#include "mgm/ConfigEngine.hh"
#include "mgm/Stat.hh"
#include "mgm/Iostat.hh"
#include "mgm/Messaging.hh"
#include "namespace/IView.hh"
#include "namespace/IFileMDSvc.hh"
#include "namespace/IContainerMDSvc.hh"
#include "namespace/accounting/FileSystemView.hh"
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

USE_EOSMGMNAMESPACE

/*----------------------------------------------------------------------------*/
class XrdMgmOfsDirectory : public XrdSfsDirectory , public eos::common::LogId
{
public:

        int         open(const char              *dirName,
                         const XrdSecClientName  *client = 0,
                         const char              *opaque = 0);

        int         open(const char              *dirName,
			 eos::common::Mapping::VirtualIdentity &vid,
                         const char              *opaque = 0);

        const char *nextEntry();

        int         Emsg(const char *, XrdOucErrInfo&, int, const char *x,
                            const char *y="");
        int         close();

const   char       *FName() {return (const char *)fname;}

                    XrdMgmOfsDirectory(char *user=0) : XrdSfsDirectory(user)
                                {ateof = 0; fname = 0;
				 d_pnt = &dirent_full.d_entry; eos::common::Mapping::Nobody(vid);
				 eos::common::LogId();dh =0;retDot=retDotDot=false;
                                }

                   ~XrdMgmOfsDirectory() {}
private:

  char           ateof;
  char          *fname;
  XrdOucString   entry;
  bool           retDot;
  bool           retDotDot;

  struct {struct dirent d_entry;
    char   pad[MAXNAMLEN];   // This is only required for Solaris!
  } dirent_full;
  
  struct dirent *d_pnt;
  
  eos::common::Mapping::VirtualIdentity vid;

  eos::ContainerMD* dh;
  eos::ContainerMD::FileMap::iterator dh_files;
  eos::ContainerMD::ContainerMap::iterator dh_dirs;
};

/*----------------------------------------------------------------------------*/
class XrdSfsAio;
class XrdMgmOfsFile : public XrdSfsFile ,  eos::common::LogId
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
                                          {oh = 0; fname = 0; openOpaque=0;eos::common::Mapping::Nobody(vid);fileId=0; procCmd=0; eos::common::LogId();fmd=0;}
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

  ProcCommand* procCmd;

  eos::FileMD* fmd;

  eos::common::Mapping::VirtualIdentity vid;
};

/*----------------------------------------------------------------------------*/

class XrdMgmOfs : public XrdSfsFileSystem , public eos::common::LogId
{
  friend class XrdMgmOfsFile;
  friend class XrdMgmOfsDirectory;
  friend class ProcCommand;
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
                             const XrdSecEntity     *client = 0,
                             const char             *opaque = 0);

        int            _chmod(const char             *Name,
                                   XrdSfsMode        Mode,
                                   XrdOucErrInfo    &out_error,
			           eos::common::Mapping::VirtualIdentity &vid,
                             const char             *opaque = 0);

        int            _chown(const char             *Name,
 			           uid_t              uid,
			           gid_t              gid,
                                   XrdOucErrInfo    &out_error,
			           eos::common::Mapping::VirtualIdentity &vid,
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
                                    XrdOucErrInfo       &out_error,
			            eos::common::Mapping::VirtualIdentity &vid,
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
			      eos::common::Mapping::VirtualIdentity &vid,
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
			    eos::common::Mapping::VirtualIdentity &vid,
			    const char             *opaque = 0);

        int            _find(const char             *path,
			     XrdOucErrInfo    &out_error,
			     XrdOucString &stdErr,
			     eos::common::Mapping::VirtualIdentity &vid,
			     std::vector< std::vector<std::string> > &found_dirs,
			     std::vector< std::vector<std::string> > &found_files,
			     const char* key=0, const char* val=0, bool nofiles=false);

   
        int            remdir(const char             *dirName,
                                    XrdOucErrInfo    &out_error,
                              const XrdSecEntity *client = 0,
                              const char             *opaque = 0);

        int            _remdir(const char             *dirName,
			       XrdOucErrInfo    &out_error,
			       eos::common::Mapping::VirtualIdentity &vid,
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
 			          eos::common::Mapping::VirtualIdentity &vid,
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

        int            utimes(const char*, struct timespec *tvp, XrdOucErrInfo&, const XrdSecEntity*, const char*);
        int            _utimes(const char*, struct timespec *tvp, XrdOucErrInfo&,  eos::common::Mapping::VirtualIdentity &vid, const char* opaque=0);

        int            attr_ls(const char             *path,
			       XrdOucErrInfo    &out_error,
			       const XrdSecEntity     *client,
			       const char             *opaque,
			       eos::ContainerMD::XAttrMap &map);   

        int            attr_set(const char             *path,
				XrdOucErrInfo    &out_error,
				const XrdSecEntity     *client,
				const char             *opaque,
				const char             *key,
				const char             *value);

        int            attr_get(const char             *path,
				XrdOucErrInfo    &out_error,
				const XrdSecEntity     *client,
				const char             *opaque,
				const char             *key,
				XrdOucString           &value);


        int            attr_rem(const char             *path,
				XrdOucErrInfo    &out_error,
			        const XrdSecEntity     *client,
				const char             *opaque,
				const char             *key);
  
        int            _attr_ls(const char             *path,
				XrdOucErrInfo    &out_error,
				eos::common::Mapping::VirtualIdentity &vid,
				const char             *opaque,
				eos::ContainerMD::XAttrMap &map);   

        int            _attr_set(const char             *path,
				 XrdOucErrInfo    &out_error,
				 eos::common::Mapping::VirtualIdentity &vid,
				 const char             *opaque,
				 const char             *key,
				 const char             *value);

        int            _attr_get(const char             *path,
				 XrdOucErrInfo    &out_error,
				 eos::common::Mapping::VirtualIdentity &vid,
				 const char             *opaque,
				 const char             *key,
				 XrdOucString           &value, 
				 bool                    islocked=false);


        int            _attr_rem(const char             *path,
				 XrdOucErrInfo    &out_error,
				 eos::common::Mapping::VirtualIdentity &vid,
				 const char             *opaque,
				 const char             *key);
				 
  
        int            _dropstripe(const char           *path, 
				   XrdOucErrInfo        &error,
				   eos::common::Mapping::VirtualIdentity &vid,
				   unsigned long         fsid, 
				   bool                  forceRemove=false);

        int            _verifystripe(const char           *path, 
				   XrdOucErrInfo        &error,
				   eos::common::Mapping::VirtualIdentity &vid,
				   unsigned long         fsid, 
				   XrdOucString          options);

        int            _movestripe(const char           *path, 
				   XrdOucErrInfo        &error,
				   eos::common::Mapping::VirtualIdentity &vid,
				   unsigned long         sourcefsid,
				   unsigned long         targetfsid,
                                   bool                  expressflag=false);

        int            _copystripe(const char           *path, 
				   XrdOucErrInfo        &error,
				   eos::common::Mapping::VirtualIdentity &vid,
				   unsigned long         sourcefsid,
				   unsigned long         targetfsid,
				   bool                  expressflag=false);

        int            _replicatestripe(const char           *path, 
				   XrdOucErrInfo        &error,
				   eos::common::Mapping::VirtualIdentity &vid,
				   unsigned long         sourcefsid,
				   unsigned long         targetfsid, 
 				   bool                  dropstripe=false,
				   bool                  expressflag=false);


        int            _replicatestripe(eos::FileMD* fmd, 
                                   const char* path,
				   XrdOucErrInfo        &error,
				   eos::common::Mapping::VirtualIdentity &vid,
				   unsigned long         sourcefsid,
				   unsigned long         targetfsid, 
				   bool                  dropstripe=false,
				   bool                  expressflag=false);

  
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
        int            Redirect(XrdOucErrInfo &error, const char* host, int &port);
        bool           ShouldStall(const char* function, eos::common::Mapping::VirtualIdentity &vid, int &stalltime, XrdOucString &stallmsg);
        bool           ShouldRedirect(const char* function, eos::common::Mapping::VirtualIdentity &vid, XrdOucString &host, int &port);

        char          *ConfigFN;       
  
        ConfigEngine*    ConfEngine;         // storing/restoring configuration

        XrdCapability*   CapabilityEngine;   // -> authorization module for token encryption/decryption
  
        XrdOucString     MgmOfsBroker;       // -> Url of the message broker without MGM subject
        XrdOucString     MgmOfsBrokerUrl;    // -> Url of the message broker with MGM subject 
        Messaging*       MgmOfsMessaging;    // -> messaging interface class
        XrdOucString     MgmDefaultReceiverQueue; // -> Queue where we are sending to by default
        XrdOucString     MgmOfsName;         // -> mount point of the filesystem
        XrdOucString     MgmOfsAlias;        // -> alias of this MGM instance
        XrdOucString     MgmOfsTargetPort;   // -> xrootd port where redirections go on the OSTs -default is 1094
        XrdOucString     MgmOfsQueue;        // -> our mgm queue name
        XrdOucString     MgmOfsInstanceName; // -> name of the EOS instance
        XrdOucString     MgmConfigDir;       // Directory where config files are stored
        XrdOucString     AuthLib;            // -> path to a possible authorizationn library
        XrdOucString     MgmNsFileChangeLogFile; // -> path to namespace changelog file for files
        XrdOucString     MgmNsDirChangeLogFile;  // -> path to namespace changelog file for directories
        XrdOucString     MgmConfigQueue;     // -> name of the mgm-wide broadcasted shared hash 
        XrdOucString     AllConfigQueue;     // -> name of the cluster-wide broadcasted shared hash
        XrdOucString     FstConfigQueue;     // -> name of the fst-wide broadcasted shared hash

        XrdOucString     SpaceConfigQueuePrefix;     // -> name of the prefix for space configuration
        XrdOucString     NodeConfigQueuePrefix ;     // -> name of the prefix for node configuration
        XrdOucString     GroupConfigQueuePrefix;     // -> name of the prefix for group configuration
      
        bool             IsReadOnly;         // -> true if this is a read-only redirector 
        bool             IsRedirect;         // -> true if the Redirect function should be called to redirect
        bool             IsStall;            // -> true if the Stall function should be called to send a wait

        bool             authorize;          // -> determins if the autorization should be applied or not
        XrdAccAuthorize *Authorization;      // -> Authorization   Service
        bool             IssueCapability;    // -> defines if the Mgm issues capabilities
  
        eos::IContainerMDSvc  *eosDirectoryService;              // -> changelog for directories
        eos::IFileMDSvc *eosFileService;                         // -> changelog for files
	eos::IView      *eosView;            // -> hierarchical view of the namespace
        eos::FileSystemView *eosFsView;      // -> filesystem view of the namespace
        XrdSysMutex      eosViewMutex;       // -> mutex making the namespace single threaded
        XrdOucString     MgmMetaLogDir;      //  Directory containing the meta data (change) log files
        Stat             MgmStats;           //  Mgm Namespace Statistics
        Iostat           IoStats;            //  Mgm IO Statistics
        bool             IoReportStore;      //  Mgm IO Reports get stored by default into /var/tmp/eos/report
        bool             IoReportNamespace;  //  Mgm IO Reports get stored in a fake namespace attaching each report to a namespace file in <IoReportStorePath>
        XrdOucString     IoReportStorePath;  //  Mgm IO Report store path by default is /var/tmp/eos/report

        google::sparse_hash_map<unsigned long long, time_t> MgmHealMap;
        XrdSysMutex      MgmHealMapMutex;

        eos::common::ClientAdminManager CommonClientAdminManager; // Manager of ClientAdmin's

        XrdMqSharedObjectManager ObjectManager; // -> Shared Hash/Queue ObjectManager

 static void* StartMgmDeletion(void *pp);    //  Deletion Thread Starter
        void  Deletion();                    //  Deletion Function


 static void* StartMgmStats(void *pp);       // Statistics circular buffer thread

 static void* StartMgmFsListener(void *pp);  //  Listener Thread Starter
        void  FsListener();                  //  Listens on filesystem errors

        XrdOucString     ManagerId;          // -> manager id in <host>:<port> format

protected:
        char*            HostName;           // -> our hostname as derived in XrdOfs
        char*            HostPref;           // -> our hostname as derived in XrdOfs without domain

private:
  static  XrdSysError *eDest;
  eos::common::Mapping::VirtualIdentity vid;
};
/*----------------------------------------------------------------------------*/
extern XrdMgmOfs* gOFS;
/*----------------------------------------------------------------------------*/

#endif
