// ----------------------------------------------------------------------
// File: XrdMgmOfs.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSMGM_MGMOFS__HH__
#define __EOSMGM_MGMOFS__HH__

/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
#include "common/Mapping.hh"  
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/GlobalConfig.hh"
#include "common/CommentLog.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mgm/ProcInterface.hh"
#include "mgm/ConfigEngine.hh"
#include "mgm/Stat.hh"
#include "mgm/Iostat.hh"
#include "mgm/Fsck.hh"
#ifdef HAVE_ZMQ
#include "mgm/ZMQ.hh"
#endif
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

#define ACCESS_R 0
#define ACCESS_W 1


#define ACCESSMODE_R int __AccessMode__ = 0
#define ACCESSMODE_W int __AccessMode__ = 1
#define SET_ACCESSMODE_W __AccessMode__ = 1

#define IS_ACCESSMODE_R (__AccessMode__ == 0)
#define IS_ACCESSMODE_W (__AccessMode__ == 1)

#define MAYSTALL { if (gOFS->IsStall) {                                 \
      XrdOucString stallmsg="";                                         \
      int stalltime=0;                                                  \
      if (gOFS->ShouldStall(__FUNCTION__,__AccessMode__, vid, stalltime, stallmsg)) \
        return gOFS->Stall(error,stalltime, stallmsg.c_str());          \
    }                                                                   \
  }

#define MAYREDIRECT { if (gOFS->IsRedirect) {                   \
      int port=0;                                               \
      XrdOucString host="";                                     \
      if (gOFS->ShouldRedirect(__FUNCTION__,__AccessMode__,vid, host,port)) \
        return gOFS->Redirect(error, host.c_str(), port);       \
    }                                                           \
  }


#define NAMESPACEMAP							\
  const char*path = inpath;						\
  const char*info = ininfo;						\
  XrdOucString store_path=path;						\
  gOFS->PathRemap(inpath,store_path);					\
  size_t __i=0;								\
  size_t __n = store_path.length();					\
  for (__i=0;__i<__n;__i++) {						\
    if ( ((store_path[__i] >= 97) && (store_path[__i] <= 122 )) || /* a-z   */ \
	 ((store_path[__i] >= 64) && (store_path[__i] <= 90 ))  || /* @,A-Z */ \
	 ((store_path[__i] >= 48) && (store_path[__i] <= 57 ))  || /* 0-9   */ \
	 (store_path[__i] == 47) || /* / */				\
	 (store_path[__i] == 46) || /* . */				\
	 (store_path[__i] == 20) || /* SPACE */				\
	 (store_path[__i] == 45) || /* - */				\
	 (store_path[__i] == 95) || /* _ */				\
	 (store_path[__i] == 126)|| /* ~ */				\
	 (store_path[__i] == 35) || /* # */				\
	 (store_path[__i] == 58) || /* : */				\
	 (store_path[__i] == 94)    /* ^ */				\
	 ) {								\
      continue;								\
    } else {								\
      break;								\
    }									\
  }									\
  if ( (vid.uid != 0) && (__i != (__n) ) ) { /* root can use all letters */ \
    path = 0;								\
  } else {								\
    const char* pf=0;							\
    if ( ininfo && (pf=strstr(ininfo,"eos.prefix")) ) {			\
      if (!store_path.beginswith("/proc")) {				\
	XrdOucEnv env(pf);						\
	store_path.insert(env.Get("eos.prefix"),0);			\
      }									\
    }									\
    path = store_path.c_str();						\
  }								
  
#define BOUNCE_ILLEGAL_NAMES						\
  if (!path) {								\
    eos_err("illegal character in %s", store_path.c_str());		\
    return Emsg(epname, error, EILSEQ,"accept path name - illegal characters - use only A-Z a-z 0-9 / SPACE .-_~#:^", store_path.c_str()); \
  } 

#define PROC_BOUNCE_ILLEGAL_NAMES					\
  if (!path) {								\
    eos_err("illegal character in %s", store_path.c_str());		\
    retc = EILSEQ;							\
    stdErr += "error: illegal characters - use only use only A-Z a-z 0-9 SPACE .-_~#:^\n"; \
    return SFS_OK;							\
  }

#define REQUIRE_SSS_OR_LOCAL_AUTH					\
  if ( (vid.prot!="sss") && ((vid.host != "localhost") && (vid.host != "localhost.localdomain")) ){ \
    eos_err("system access restricted - not authorized identity used");	\
    return Emsg(epname, error, EACCES,"give access - system access restricted - not authorized identity used"); \
  }

#define BOUNCE_NOT_ALLOWED						\
  /* for root, bin, daemon, admin we allow localhost connects or sss authentication always */ \
  if ( ((vid.uid>3) || ( (vid.prot!="sss") && (vid.host != "localhost") && (vid.host != "localhost.localdomain"))) && (Access::gAllowedUsers.size() || Access::gAllowedGroups.size() || Access::gAllowedHosts.size() )) { \
    if ( (!Access::gAllowedGroups.count(vid.gid)) &&			\
	 (!Access::gAllowedUsers.count(vid.uid)) &&			\
	 (!Access::gAllowedHosts.count(vid.host)) ) {			\
      eos_err("user access restricted - not authorized identity used"); \
      return Emsg(epname, error, EACCES,"give access - user access restricted - not authorized identity used"); \
    }									\
  }

#define PROC_BOUNCE_NOT_ALLOWED						\
  if ((vid.uid>3) &&(Access::gAllowedUsers.size() || Access::gAllowedGroups.size() || Access::gAllowedHosts.size() )) { \
    if ( (!Access::gAllowedGroups.count(vid.gid)) &&			\
	 (!Access::gAllowedUsers.count(vid.uid)) &&			\
	 (!Access::gAllowedHosts.count(vid.host)) ) {			\
      eos_err("user access restricted - not authorized identity used"); \
      retc = EACCES;							\
      stdErr += "error: user access restricted - not authorized identity used";	\
      return SFS_OK;							\
    }									\
  }

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

  const   char       *FName() {return (const char *)dirName.c_str();}

  XrdMgmOfsDirectory(char *user=0, int MonID=0) : XrdSfsDirectory(user,MonID)
  {dirName=""; dh=0;
    d_pnt = &dirent_full.d_entry; eos::common::Mapping::Nobody(vid);
    eos::common::LogId();
  }

  ~XrdMgmOfsDirectory() {}
private:

  struct {struct dirent d_entry;
    char   pad[MAXNAMLEN];   // This is only required for Solaris!
  } dirent_full;
  
  struct dirent *d_pnt;
  std::string dirName;
  eos::common::Mapping::VirtualIdentity vid;

  eos::ContainerMD* dh;
  std::set<std::string> dh_list;
  std::set<std::string>::const_iterator dh_it;
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

  XrdMgmOfsFile(char *user=0, int MonID=0) : XrdSfsFile(user,MonID)
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
  XrdSfsDirectory *newDir(char *user=0, int MonID=0)
  {return (XrdSfsDirectory *)new XrdMgmOfsDirectory(user,MonID);}

  XrdSfsFile      *newFile(char *user=0, int MonID=0)
  {return      (XrdSfsFile *)new XrdMgmOfsFile(user,MonID);}

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
  
  int            chksum(XrdSfsFileSystem::csFunc Func,
                        const char             *csName,
                        const char             *Path,
			XrdOucErrInfo    &out_error,
                        const XrdSecEntity     *client = 0,
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
                       std::map<std::string, std::set<std::string> > &found,
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

  
  int            SendResync(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid);


  // Common functions
  //
  static  int            Mkpath(const char *path, mode_t mode, 
                                const char *info=0, XrdSecEntity* client = 0, XrdOucErrInfo* error = 0) { return SFS_ERROR;}

  int            Emsg(const char *, XrdOucErrInfo&, int, const char *x,
                      const char *y="");

  XrdMgmOfs(XrdSysError *lp);
  virtual               ~XrdMgmOfs() {
}

  virtual int            Configure(XrdSysError &);
  static void*           StaticInitializeFileView(void* arg);
  void*                  InitializeFileView();

  static void*           StaticSignalHandlerThread(void* arg);
  void*                  SignalHandlerThread();

  enum eNamespace { kDown=0, kBooting=1, kBooted=2, kFailed=3, kCompacting=4};
  int                    Initialized;
  time_t                 InitializationTime;
  XrdSysMutex            InitializationMutex;

  static const char*     gNameSpaceState[];

  virtual bool           Init(XrdSysError &);
  int            Stall(XrdOucErrInfo &error, int stime, const char *msg);
  int            Redirect(XrdOucErrInfo &error, const char* host, int &port);
  bool           ShouldStall(const char* function, int accessmode, eos::common::Mapping::VirtualIdentity &vid, int &stalltime, XrdOucString &stallmsg);
  bool           ShouldRedirect(const char* function, int accessmode, eos::common::Mapping::VirtualIdentity &vid, XrdOucString &host, int &port);

  void           UpdateNowInmemoryDirectoryModificationTime(eos::ContainerMD::id_t id);
  void           UpdateInmemoryDirectoryModificationTime(eos::ContainerMD::id_t id, eos::ContainerMD::ctime_t &ctime);

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
  XrdOucString     MgmProcPath;        // Directory with proc files
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
  bool             IsWriteStall;       // -> true if the Stall function should be called to send a wait to everything doing 'writes'

  bool             authorize;          // -> determins if the autorization should be applied or not
  XrdAccAuthorize *Authorization;      // -> Authorization   Service
  bool             IssueCapability;    // -> defines if the Mgm issues capabilities
  
  eos::IContainerMDSvc  *eosDirectoryService;              // -> changelog for directories
  eos::IFileMDSvc *eosFileService;                         // -> changelog for files
  eos::IView      *eosView;            // -> hierarchical view of the namespace
  eos::FileSystemView *eosFsView;      // -> filesystem view of the namespace
  XrdSysMutex      eosViewMutex;       // -> mutex making the namespace single threaded
  eos::common::RWMutex eosViewRWMutex;     // -> rw namespace mutex
  XrdOucString     MgmMetaLogDir;      //  Directory containing the meta data (change) log files
  XrdOucString     MgmTxDir;           //  Directory containing the transfer database and archive 
  XrdOucString     MgmAuthDir;         //  Directory containing exported authentication token
  bool             MgmRedirector;      //  Act's only as a redirector, disables many components in the MGM
  Stat             MgmStats;           //  Mgm Namespace Statistics
  Iostat           IoStats;            //  Mgm IO Statistics
  XrdOucString     IoReportStorePath;  //  Mgm IO Report store path by default is /var/tmp/eos/report
  bool             ErrorLog;           //  Mgm writes error log with cluster collected file into /var/log/eos/error.log
  eos::common::CommentLog* commentLog; //  Mgm writes all proc commands with a comment into /var/log/eos/comments.log

  Fsck             FsCheck;            // Class checking the filesystem
  google::sparse_hash_map<unsigned long long, time_t> MgmHealMap;
  XrdSysMutex      MgmHealMapMutex;


  std::map<eos::common::FileSystem::fsid_t, time_t> DumpmdTimeMap;     // this map stores the last time of a filesystem dump, this information is used to track filesystems which have not been checked decentral by an FST. It is filled in the 'dumpmd' function definde in Procinterface
  XrdSysMutex      DumpmdTimeMapMutex;                                 // mutex protecting the 'dumpmd' time

  eos::common::RWMutex  PathMapMutex;                                  // mutex protecting the path map
  std::map<std::string,std::string> PathMap;                           // containing global path remapping
  void PathRemap(const char* inpath, XrdOucString &outpath);           // map defining global namespace remapping
  bool             AddPathMap(const char* source, const char* target); // add's a mapping to the path map
  void             ResetPathMap();                                     // reset/empty the path map

  // map keeping the modification times of directories, they are either directly inserted from directory/file creation or they are set from a directory listing
  XrdSysMutex      MgmDirectoryModificationTimeMutex;
  google::sparse_hash_map<unsigned long long, struct timespec> MgmDirectoryModificationTime;

  XrdMqSharedObjectManager ObjectManager; // -> Shared Hash/Queue ObjectManager
 
  pthread_t deletion_tid;              // Thead Id of the deletion thread
  pthread_t stats_tid;                 // Thread Id of the stats thread
  pthread_t fslistener_tid;            // Thread ID of the fs listener thread

  static void* StartMgmDeletion(void *pp);    //  Deletion Thread Starter
  void  Deletion();                    //  Deletion Function

  bool  DeleteExternal(eos::common::FileSystem::fsid_t fsid, unsigned long long fid); // send an explicit deletion message to any fsid/fid pair

  static void* StartMgmStats(void *pp);       // Statistics circular buffer thread

  static void* StartMgmFsListener(void *pp);  //  Listener Thread Starter
  void  FsListener();                  //  Listens on filesystem errors

#ifdef HAVE_ZMQ
  ZMQ*  zMQ;                           //  ZMQ processor
#endif

  XrdOucString     ManagerId;          // -> manager id in <host>:<port> format
  XrdOucString     ManagerIp;          // -> manager ip in <xxx.yyy.zzz.vvv> format
  int              ManagerPort;        // -> manager port as number e.g. 1094
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
