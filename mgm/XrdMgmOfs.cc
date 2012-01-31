// ----------------------------------------------------------------------
// File: XrdMgmOfs.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "common/StringConversion.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsTrace.hh"
#include "mgm/XrdMgmOfsSecurity.hh"
#include "mgm/Policy.hh"
#include "mgm/Quota.hh"
#include "mgm/Acl.hh"
/*----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#ifdef __APPLE__
#define ECOMM 70
#endif

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif


/*----------------------------------------------------------------------------*/
XrdSysError     gMgmOfsEroute(0);  
XrdSysError    *XrdMgmOfs::eDest;
XrdOucTrace     gMgmOfsTrace(&gMgmOfsEroute);

const char* XrdMgmOfs::gNameSpaceState[] = {"down", "booting", "booted", "failed"};

XrdMgmOfs* gOFS=0;

/*----------------------------------------------------------------------------*/
void
xrdmgmofs_shutdown(int sig) {

  // handler to shutdown the daemon for valgrinding and clean server stop (e.g. let's time to finish write operations
  eos_static_warning("Shutdown:: grab write mutex");
  gOFS->eosViewRWMutex.TimeoutLockWrite();

  eos_static_warning("Shutdown:: disconnect from broker");
  XrdMqMessaging::gMessageClient.Disconnect();

  if (gOFS->ErrorLog) {
    XrdOucString errorlogkillline="pkill -9 -f \"eos -b console log _MGMID_\"";
    int rrc = system(errorlogkillline.c_str());
    if (WEXITSTATUS(rrc)) {
      eos_static_info("%s returned %d", errorlogkillline.c_str(), rrc);
    }
    XrdOucString errorlogline="eos -b console log _MGMID_ >& /dev/null &";
    rrc = system(errorlogline.c_str());
    if (WEXITSTATUS(rrc)) {
      eos_static_info("%s returned %d", errorlogline.c_str(), rrc);
    }
    eos_static_warning("Shutdown stopping console error log");
  }
  eos_static_warning("Shutdown complete");
  kill(getpid(),9);
}


/*----------------------------------------------------------------------------*/
XrdMgmOfs::XrdMgmOfs(XrdSysError *ep)
{
  eDest = ep;
  ConfigFN  = 0;  
  eos::common::LogId();
  eos::common::LogId::SetSingleShotLogId();

  (void) signal(SIGINT,xrdmgmofs_shutdown);
  (void) signal(SIGTERM,xrdmgmofs_shutdown);
  (void) signal(SIGQUIT,xrdmgmofs_shutdown);
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::Init(XrdSysError &ep)
{
  
  return true;
}

/*----------------------------------------------------------------------------*/
extern "C" 
XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *native_fs, 
                                      XrdSysLogger     *lp,
                                      const char       *configfn)
{
  gMgmOfsEroute.SetPrefix("mgmofs_");
  gMgmOfsEroute.logger(lp);
  
  static XrdMgmOfs myFS(&gMgmOfsEroute);

  XrdOucString vs="MgmOfs (meta data redirector) ";
  vs += VERSION;
  gMgmOfsEroute.Say("++++++ (c) 2010 CERN/IT-DSS ",vs.c_str());
  
  // Initialize the subsystems
  //
  if (!myFS.Init(gMgmOfsEroute) ) return 0;

  gOFS = &myFS;

  // by default enable stalling and redirection
  gOFS->IsStall    = true;
  gOFS->IsRedirect = true;

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
  if ( myFS.Configure(gMgmOfsEroute) ) return 0;


  // Initialize authorization module ServerAcc
  gOFS->CapabilityEngine = (XrdCapability*) XrdAccAuthorizeObject(lp, configfn, 0);
  if (!gOFS->CapabilityEngine) {
    return 0;
  }

  return gOFS;
}

/*----------------------------------------------------------------------------*/
bool 
XrdMgmOfs::ShouldStall(const char* function,  int __AccessMode__, eos::common::Mapping::VirtualIdentity &vid,int &stalltime, XrdOucString &stallmsg) 
{
  // check for user, group or host banning
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  if ( (vid.uid > 3) && 
       ( Access::gBannedUsers.count(vid.uid) ||
         Access::gBannedGroups.count(vid.gid) ||
         Access::gBannedHosts.count(vid.host) || 
         (Access::gAllowedUsers.size()  && Access::gAllowedUsers.count(vid.uid)) ||
         (Access::gAllowedGroups.size() && Access::gAllowedGroups.count(vid.gid)) ||
         (Access::gAllowedHosts.size()  && Access::gAllowedHosts.count(vid.host)) || 
         (Access::gStallRules.size() && (Access::gStallRules.count(std::string("*")))) ||
	 ( IS_ACCESSMODE_R && (Access::gStallRules.count(std::string("r:*")))) ||
	 ( IS_ACCESSMODE_W && (Access::gStallRules.count(std::string("w:*"))))
         )) {
    if (Access::gStallRules.size()) {
      if (Access::gStallRules.count(std::string("*"))) {
	stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
      } else {
	if ( IS_ACCESSMODE_R) {
	  stalltime = atoi(Access::gStallRules[std::string("r:*")].c_str());
	} else {
	  stalltime = atoi(Access::gStallRules[std::string("w:*")].c_str());
	}
      }
    } else {
      stalltime = 300;
    }
    stallmsg="Attention: you are currently hold in this instance and each request is stalled for ";
    stallmsg += (int) stalltime; stallmsg += " seconds ...";
    eos_static_info("info=\"denying access to\" uid=%u gid=%u host=%s", vid.uid,vid.gid,vid.host.c_str());
    return true;
  } else {
    if (Access::gStallRules.size()) {
      if (Access::gStallRules.count(std::string("*"))) {
	if (vid.host != "localhost.localdomain") {
	  stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
	  stallmsg="Attention: you are currently hold in this instance and each request is stalled for ";
	  stallmsg += (int) stalltime; stallmsg += " seconds ...";
	  eos_static_info("info=\"denying access to\" uid=%u gid=%u host=%s", vid.uid,vid.gid,vid.host.c_str());
	  return true;
	}
      }
    }
  }
  eos_static_debug("info=\"allowing access to\" uid=%u gid=%u host=%s", vid.uid,vid.gid,vid.host.c_str());
  return false;
}

bool
XrdMgmOfs::ShouldRedirect(const char* function, int __AccessMode__, eos::common::Mapping::VirtualIdentity &vid,XrdOucString &host, int &port)
{
  if ( (vid.host == "localhost")  || (vid.uid==0))
    return false;
    
  if (Access::gRedirectionRules.size()) {
    bool c1 = Access::gRedirectionRules.count(std::string("*"));
    bool c3 = (IS_ACCESSMODE_R && Access::gRedirectionRules.count(std::string("r:*")));
    bool c2 = (IS_ACCESSMODE_W && Access::gRedirectionRules.count(std::string("w:*")));
    if (c1 || c2 || c3) {
      // redirect
      std::string delimiter=":";
      std::vector<std::string> tokens;
      if (c1) {
	eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("*")],tokens,delimiter);
      } else {
	if (c2) {
	  eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("w:*")],tokens,delimiter);
	} else {
	  if (c3) { 
	    eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("r:*")],tokens,delimiter);
	  }
	}
      }

      if (tokens.size() == 1) {
        host = tokens[0].c_str();
        port = 1094;
      } else {
        host = tokens[0].c_str();
        port = atoi(tokens[1].c_str());
        if (port == 0)
          port = 1094;
      }
      return true;
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::ResetPathMap()
{
  eos::common::RWMutexWriteLock lock(PathMapMutex);
  PathMap.clear();
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::AddPathMap(const char* source, const char* target) {
  eos::common::RWMutexWriteLock lock(PathMapMutex);
  if (PathMap.count(source)) {
    return false;
  } else {
    PathMap[source] = target;
    ConfEngine->SetConfigValue("map",source,target);
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdMgmOfs::PathRemap(const char* inpath, XrdOucString &outpath)
{
  // remaps paths
  eos::common::Path cPath(inpath);

  eos::common::RWMutexReadLock lock(PathMapMutex);
  eos_debug("mappath=%s ndir=%d dirlevel=%d", inpath, PathMap.size(), cPath.GetSubPathSize()-1);

  outpath = inpath;

  // remove double slashes
  while (outpath.replace("//","/")) {}
  
  // append a / to the path
  outpath += "/";

  if (!PathMap.size()) {
    outpath.erase(outpath.length()-1);
    return;
  }

  if (PathMap.count(inpath) ) {
    outpath.replace(inpath,PathMap[inpath].c_str());
    outpath.erase(outpath.length()-1);
    return;
  }

  if (PathMap.count(outpath.c_str())) {
    outpath.replace(outpath.c_str(),PathMap[outpath.c_str()].c_str());
    outpath.erase(outpath.length()-1);
    return;
  }

  if (!cPath.GetSubPathSize()) {
    outpath.erase(outpath.length()-1);
    return;
  }

  for (size_t i=cPath.GetSubPathSize()-1; i>0; i--) {
    if (PathMap.count(cPath.GetSubPath(i))) {
      outpath.replace(cPath.GetSubPath(i),PathMap[cPath.GetSubPath(i)].c_str());
      outpath.erase(outpath.length()-1);
      return;
    }
  }
  outpath.erase(outpath.length()-1);
  return;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfsDirectory::open(const char              *inpath, // In
                             const XrdSecEntity  *client,   // In
                             const char              *info)     // In
/*
  Function: Open the directory `path' and prepare for reading.

  Input:    path      - The fully qualified name of the directory to open.
  cred      - Authentication credentials, if any.
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success, otherwise SFS_ERROR.
*/
{
  static const char *epname = "opendir";
  const char *tident = error.getErrUser();

  XrdOucEnv Open_Env(info);

  NAMESPACEMAP;

  eos_info("path=%s",path);

  AUTHORIZE(client,&Open_Env,AOP_Readdir,"open directory",path,error);

  eos::common::Mapping::IdMap(client,info,tident, vid);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return open(path, vid, info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsDirectory::open(const char              *dir_path, // In
                             eos::common::Mapping::VirtualIdentity &vid, // In
                             const char              *info)     // In
/*
  Function: Open the directory `path' and prepare for reading.

  Input:    path      - The fully qualified name of the directory to open.
  vid       - Virtual identity
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success, otherwise SFS_ERROR.
*/
{
  static const char *epname = "opendir";
  XrdOucEnv Open_Env(info);
  errno = 0;

  EXEC_TIMING_BEGIN("OpenDir");

  eos::common::Path cPath(dir_path);
   
  eos_info("name=opendir path=%s",cPath.GetPath());

  gOFS->MgmStats.Add("OpenDir",vid.uid,vid.gid,1);

  // Open the directory
  bool permok = false;

  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);      
  try {
    eos::ContainerMD::XAttrMap attrmap;
    eos::ContainerMD::FileMap::iterator dh_files;
    eos::ContainerMD::ContainerMap::iterator dh_dirs;

    dh = gOFS->eosView->getContainer(cPath.GetPath());
    permok = dh->access(vid.uid,vid.gid, R_OK|X_OK);

    if (!permok) {
      // get attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for ( it = dh->attributesBegin(); it != dh->attributesEnd(); ++it) {
        attrmap[it->first] = it->second;
      }
      // ACL and permission check
      Acl acl(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
       
      eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d", acl.HasAcl(),acl.CanRead(),acl.CanWrite(),acl.CanWriteOnce(), acl.CanBrowse(),acl.HasEgroup());

      // browse permission by ACL
      if (acl.HasAcl()) {
        if (acl.CanBrowse()) {
          permok = true;
        }
      }
    }
     
    if (permok) {
      // add all the files
      for (dh_files = dh->filesBegin(); dh_files != dh->filesEnd(); dh_files++) {
        // 
        dh_list.insert(dh_files->first);
      }
       
      for (dh_dirs = dh->containersBegin(); dh_dirs != dh->containersEnd(); dh_dirs++) {
        dh_list.insert(dh_dirs->first);
      }
       
      dh_list.insert(".");
      // the root dir has no .. entry
      if (strcmp(dir_path,"/")) {
        dh_list.insert("..");
      }
    }
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
  }
  // check permissions

  if (dh) {
    eos_debug("msg=\"access\" uid=%d gid=%d retc=%d mode=%o", vid.uid,vid.gid,(dh->access(vid.uid,vid.gid, R_OK|X_OK)), dh->getMode());
  }

  // Verify that this object is not already associated with an open directory
  //
  if (!dh) 
    return Emsg(epname, error, errno, 
                "open directory", cPath.GetPath());
   
  if (!permok) {
    errno = EPERM;
    return Emsg(epname, error, errno, 
                "open directory", cPath.GetPath());
  }
   
  dirName = dir_path;

  // Set up values for this directory object
  //
  dh_it = dh_list.begin();

  EXEC_TIMING_END("OpenDir");   
  return  SFS_OK;
}

/*----------------------------------------------------------------------------*/
const char *XrdMgmOfsDirectory::nextEntry()
/*
  Function: Read the next directory entry.

  Input:    None.

  Output:   Upon success, returns the contents of the next directory entry as
  a null terminated string. Returns a null pointer upon EOF or an
  error. To differentiate the two cases, getErrorInfo will return
  0 upon EOF and an actual error code (i.e., not 0) on error.
*/
{
  if (dh_it == dh_list.end()) {
    // no more entry
    return (const char *)0;
  }

  std::set<std::string>::iterator tmp_it = dh_it;
  dh_it++;
  return tmp_it->c_str();
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsDirectory::close()
/*
  Function: Close the directory object.

  Input:    cred       - Authentication credentials, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  //  static const char *epname = "closedir";

  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::open(const char          *inpath,      // In
                        XrdSfsFileOpenMode   open_mode, // In
                        mode_t               Mode,      // In
                        const XrdSecEntity        *client,    // In
                        const char                *info)      // In
/*
  Function: Open the file `path' in the mode indicated by `open_mode'.  

  Input:    path      - The fully qualified name of the file to open.
  open_mode - One of the following flag values:
  SFS_O_RDONLY - Open file for reading.
  SFS_O_WRONLY - Open file for writing.
  SFS_O_RDWR   - Open file for update
  SFS_O_CREAT  - Create the file open in RDWR mode
  SFS_O_TRUNC  - Trunc  the file open in RDWR mode
  Mode      - The Posix access mode bits to be assigned to the file.
  These bits correspond to the standard Unix permission
  bits (e.g., 744 == "rwxr--r--"). Mode may also conatin
  SFS_O_MKPTH is the full path is to be created. The
  agument is ignored unless open_mode = SFS_O_CREAT.
  client    - Authentication credentials, if any.
  info      - Opaque information to be used as seen fit.

  Output:   Returns OOSS_OK upon success, otherwise SFS_ERROR is returned.
*/
{
  static const char *epname = "open";
  const char *tident = error.getErrUser();
  errno = 0;

  EXEC_TIMING_BEGIN("Open");
  SetLogId(logId, tident);
  
  eos::common::Mapping::IdMap(client,info,tident,vid);

  SetLogId(logId, vid, tident);

  NAMESPACEMAP;

  int open_flag = 0;
  
  int isRW = 0;
  int isRewrite = 0;
  bool isCreation = false;

  int crOpts = (Mode & SFS_O_MKPTH) ? XRDOSS_mkpath : 0;

  // Set the actual open mode and find mode
  //
  if (open_mode & SFS_O_CREAT) open_mode = SFS_O_CREAT;
  else if (open_mode & SFS_O_TRUNC) open_mode = SFS_O_TRUNC;
  


  switch(open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                      SFS_O_CREAT  | SFS_O_TRUNC))
    {
    case SFS_O_CREAT:  open_flag   = O_RDWR     | O_CREAT  | O_EXCL;
      crOpts     |= XRDOSS_new;
      isRW = 1;
      break;
    case SFS_O_TRUNC:  open_flag  |= O_RDWR     | O_CREAT     | O_TRUNC;
      isRW = 1;
      break;
    case SFS_O_RDONLY: open_flag = O_RDONLY; 
      isRW = 0;
      break;
    case SFS_O_WRONLY: open_flag = O_WRONLY; 
      isRW = 1;
      break;
    case SFS_O_RDWR:   open_flag = O_RDWR;   
      isRW = 1;
      break;
    default:           open_flag = O_RDONLY; 
      isRW = 0;
      break;
    }
  
  if (isRW) {
    eos_info("op=write trunc=%d path=%s info=%s",open_mode & SFS_O_TRUNC, path,info);
  } else {
    eos_info("op=read path=%s info=%s",path,info);
  }

  ACCESSMODE_R;
  if (isRW) {
    SET_ACCESSMODE_W;
  }

  MAYSTALL;
  MAYREDIRECT;

  openOpaque = new XrdOucEnv(info);
  //  const int AMode = S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH; // 775
  //  char *opname;
  //  int aop=0;
  
  //  mode_t acc_mode = Mode & S_IAMB;
  
  int rcode=SFS_ERROR;
  
  XrdOucString redirectionhost="invalid?";

  XrdOucString targethost="";
  int targetport = atoi(gOFS->MgmOfsTargetPort.c_str());

  int ecode=0;
  unsigned long fmdlid=0;
  unsigned long long cid = 0;
  
  eos_debug("mode=%x create=%x truncate=%x", open_mode, SFS_O_CREAT, SFS_O_TRUNC);

  // proc filter
  if (ProcInterface::IsProcAccess(path)) {
    gOFS->MgmStats.Add("OpenProc",vid.uid,vid.gid,1);  
    if (!ProcInterface::Authorize(path, info, vid, client)) {
      return Emsg(epname, error, EPERM, "execute proc command - you don't have the requested permissions for that operation ", path);      
    } else {
      procCmd = new ProcCommand();
      procCmd->SetLogId(logId,vid, tident);
      return procCmd->open(path, info, vid, &error);
    }
  }

  gOFS->MgmStats.Add("Open",vid.uid,vid.gid,1);  

  eos_debug("authorize start");

  if (open_flag & O_CREAT) {
    AUTHORIZE(client,openOpaque,AOP_Create,"create",path,error);
  } else {
    AUTHORIZE(client,openOpaque,(isRW?AOP_Update:AOP_Read),"open",path,error);
  }

  eos_debug("msg=\"authorize done\"");

  eos::common::Path cPath(path);

  // check if we have to create the full path
  if (Mode & SFS_O_MKPTH) {
    eos_debug("msg=\"SFS_O_MKPTH was requested\"");

    XrdSfsFileExistence file_exists;
    int ec = gOFS->_exists(cPath.GetParentPath(),file_exists,error,vid,0);
    
    // check if that is a file
    if  ((!ec) && (file_exists!=XrdSfsFileExistNo) && (file_exists!=XrdSfsFileExistIsDirectory)) {
      return Emsg(epname, error, ENOTDIR, "open file - parent path is not a directory", cPath.GetParentPath());
    }
    // if it does not exist try to create the path!
    if ((!ec) && (file_exists==XrdSfsFileExistNo)) {
      ec = gOFS->_mkdir(cPath.GetParentPath(),Mode,error,vid,info);
      if (ec) {
	gOFS->MgmStats.Add("OpenFailedPermission",vid.uid,vid.gid,1);  
        return SFS_ERROR;
      }
    }
  }
  

  // get the directory meta data if exists
  eos::ContainerMD* dmd=0;
  eos::ContainerMD::XAttrMap attrmap;
  Acl acl;
  bool stdpermcheck=false;

  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    //-------------------------------------------
    try {
      dmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      // get the attributes out
      eos::ContainerMD::XAttrMap::const_iterator it;
      for ( it = dmd->attributesBegin(); it != dmd->attributesEnd(); ++it) {
	attrmap[it->first] = it->second;
      }
      if (dmd) {
	fmd = dmd->findFile(cPath.GetName());
	if (!fmd) {
	  if (dmd->findContainer(cPath.GetName())) {
	    errno = EISDIR;
	  } else {
	    errno = ENOENT;
	  }
	} else {
	  fileId = fmd->getId();
	  fmdlid = fmd->getLayoutId();
	  cid    = fmd->getContainerId();
	}
      }
      else
	fmd = 0;
      
    } catch( eos::MDException &e ) {
      dmd = 0;
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
    };
    
    //-------------------------------------------
    // check permissions
    
    if (!dmd) {
      if (cPath.GetSubPath(2)) {
	eos_info("info=\"checking l2 path\" path=%s", cPath.GetSubPath(2));
	//-------------------------------------------
	// check if we have a redirection setting at level 2 in the namespace
	//-------------------------------------------
	try {
	  dmd = gOFS->eosView->getContainer(cPath.GetSubPath(2));
	  // get the attributes out
	  eos::ContainerMD::XAttrMap::const_iterator it;
	  for ( it = dmd->attributesBegin(); it != dmd->attributesEnd(); ++it) {
	    attrmap[it->first] = it->second;
	  }
	} catch( eos::MDException &e ) {
	  dmd = 0;
	  errno = e.getErrno();
	  eos_debug("msg=\"exception\" ec=%d emsg=%s\n", e.getErrno(),e.getMessage().str().c_str());
	};	      
	//-------------------------------------------
	if (attrmap.count("sys.redirect.enoent")) {
	  // there is a redirection setting here
	  redirectionhost = "";
	  redirectionhost = attrmap["sys.redirect.enoent"].c_str();
	  int portpos = 0;
	  if ( (portpos = redirectionhost.find(":")) != STR_NPOS) {
	    XrdOucString port = redirectionhost;
	    port.erase(0,portpos+1);
	    ecode = atoi(port.c_str());
	    redirectionhost.erase(portpos);
	  } else {
	    ecode = 1094;
	  }
	  rcode = SFS_REDIRECT;
	  error.setErrInfo(ecode, redirectionhost.c_str());
	  gOFS->MgmStats.Add("RedirectENOENT",vid.uid,vid.gid,1);  
	  eos_info("info=\"redirecting\" hostport=%s:%d", redirectionhost.c_str(), ecode);
	  return rcode;
	}    
      }
      gOFS->MgmStats.Add("OpenFailedENOENT",vid.uid,vid.gid,1);  
      return Emsg(epname, error, errno, "open file", path);
    } 
    
    // ACL and permission check
    acl.Set(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
    eos_info("acl=%d r=%d w=%d wo=%d egroup=%d", acl.HasAcl(),acl.CanRead(),acl.CanWrite(),acl.CanWriteOnce(), acl.HasEgroup());
    if (acl.HasAcl()) {
      if (isRW) {
	// write case
	if ( (!acl.CanWrite()) && (!acl.CanWriteOnce()) ) {
	  // we have to check the standard permissions
	  stdpermcheck = true;
	}
      } else {
	// read case
	if ( (!acl.CanRead()) ) {
	  // we have to check the standard permissions
	  stdpermcheck = true;
	} 
      }
    } else {
      stdpermcheck = true;
    }
    
    if (stdpermcheck && (!dmd->access(vid.uid, vid.gid, (isRW)?W_OK | X_OK:R_OK | X_OK))) {
      errno = EPERM;  
      gOFS->MgmStats.Add("OpenFailedPermission",vid.uid,vid.gid,1);  
      return Emsg(epname, error, errno, "open file", path);      
    }
    
    // store the in-memory modification time
    gOFS->MgmDirectoryModificationTimeMutex.Lock();
    // we get the current time, but we don't update the creation time
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts);
    gOFS->MgmDirectoryModificationTime[dmd->getId()].tv_sec = ts.tv_sec;
    gOFS->MgmDirectoryModificationTime[dmd->getId()].tv_nsec = ts.tv_nsec;
    gOFS->MgmDirectoryModificationTimeMutex.UnLock();
    //-------------------------------------------    
  }

  
  if (isRW) {
    if ((open_mode & SFS_O_TRUNC) && fmd) {
      // check if this directory is write-once for the mapped user
      if (acl.HasAcl()) {
        if (acl.CanWriteOnce()) {
          // this is a write once user
          return Emsg(epname, error, EEXIST,"overwrite existing file - you are write-once user");
        } else {
          if ((!stdpermcheck) && (!acl.CanWrite())) {
            return Emsg(epname, error, EPERM,"overwrite existing file - you have no write permission");
          }
        }
      }
      
      // drop the old file and create a new truncated one
      if (gOFS->_rem(path,error, vid, info)) {
        return Emsg(epname, error, errno,"remove file for truncation", path);
      }
      
      // invalidate the record
      fmd = 0;
      gOFS->MgmStats.Add("OpenWriteTruncate",vid.uid,vid.gid,1);  
    } else {
      if (!(fmd) && ((open_flag & O_CREAT)))  {
        gOFS->MgmStats.Add("OpenWriteCreate",vid.uid,vid.gid,1);  
      } else {
        if (acl.HasAcl()) {
          if (acl.CanWriteOnce()) {
            // this is a write once user
            return Emsg(epname, error, EEXIST,"overwrite existing file - you are write-once user");
          } else {
            if ((!stdpermcheck) && (!acl.CanWrite())) {
              return Emsg(epname, error, EPERM,"overwrite existing file - you have no write permission");
            }
          }
        }
        
        gOFS->MgmStats.Add("OpenWrite",vid.uid,vid.gid,1);  
      }
    }


    // write case
    if ((!fmd)) {
      if (!(open_flag & O_CREAT))  {
        // write open of not existing file without creation flag
        return Emsg(epname, error, errno, "open file", path);      
      } else {
        // creation of a new file

	{
	  //-------------------------------------------
	  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);	 
	  try {
	    // we create files with the uid/gid of the parent directory
	    fmd = gOFS->eosView->createFile(path, vid.uid, vid.gid);
	    fileId = fmd->getId();
	    fmdlid = fmd->getLayoutId();
	    cid    = fmd->getContainerId();
	  } catch( eos::MDException &e ) {
	    fmd = 0;
	    errno = e.getErrno();
	    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
	  };	 
	  //-------------------------------------------
	}
        
        if (!fmd) {
          // creation failed
	  gOFS->MgmStats.Add("OpenFailedCreate",vid.uid,vid.gid,1);  
          return Emsg(epname, error, errno, "create file", path);      
        }
        isCreation = true;
      }
    } else {
      // we attached to an existing file
      if (fmd && (open_flag & O_EXCL))  {
        gOFS->MgmStats.Add("OpenFailedExists",vid.uid,vid.gid,1);  
        return Emsg(epname, error, EEXIST, "create file", path);
      }
    }
  } else {
    if ((!fmd) && (attrmap.count("sys.redirect.enoent"))) {
      // there is a redirection setting here
      redirectionhost = "";
      redirectionhost = attrmap["sys.redirect.enoent"].c_str();
      int portpos = 0;
      if ( (portpos = redirectionhost.find(":")) != STR_NPOS) {
        XrdOucString port = redirectionhost;
        port.erase(0,portpos+1);
        ecode = atoi(port.c_str());
        redirectionhost.erase(portpos);
      } else {
        ecode = 1094;
      }
      rcode = SFS_REDIRECT;
      error.setErrInfo(ecode, redirectionhost.c_str());
      gOFS->MgmStats.Add("RedirectENOENT",vid.uid,vid.gid,1);
      return rcode;
    }
    if ((!fmd)) {
      gOFS->MgmStats.Add("OpenFailedENOENT",vid.uid,vid.gid,1);  
      return Emsg(epname, error, errno, "open file", path);      
    }
    gOFS->MgmStats.Add("OpenRead",vid.uid,vid.gid,1);  
  }
  
  // construct capability
  XrdOucString capability = "";

  if (isRW) {
    if (isRewrite) {
      capability += "&mgm.access=update";
    } else {
      capability += "&mgm.access=create";
    }
  } else {
    capability += "&mgm.access=read";
  }

  // forward some allowed user opaque tags

 
  unsigned long layoutId = (isCreation)?eos::common::LayoutId::kPlain:fmdlid;
  unsigned long forcedFsId = 0; // the client can force to read a file on a defined file system
  unsigned long fsIndex = 0; // this is the filesystem defining the client access point in the selection vector - for writes it is always 0, for reads it comes out of the FileAccess function
  XrdOucString space = "default";

  unsigned long newlayoutId=0;
  // select space and layout according to policies
  Policy::GetLayoutAndSpace(path, attrmap, vid, newlayoutId, space, *openOpaque, forcedFsId);


  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex); // lock order 1
  eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);         // lock order 2

  SpaceQuota* quotaspace = Quota::GetSpaceQuota(space.c_str(),false);

  if (!quotaspace) {
    gOFS->MgmStats.Add("OpenFailedQuota",vid.uid,vid.gid,1);  
    return Emsg(epname, error, EINVAL, "get quota space ", space.c_str());
  }


  if (isCreation || ( (open_mode == SFS_O_TRUNC) && (!fmd->getNumLocation()))) {
    eos_info("blocksize=%llu lid=%x", eos::common::LayoutId::GetBlocksize(newlayoutId), newlayoutId);
    layoutId = newlayoutId;
    // set the layout and commit new meta data 
    fmd->setLayoutId(layoutId);
    fmd->setSize(0);
    {
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      //-------------------------------------------      
      try {
	gOFS->eosView->updateFileStore(fmd);
	
	SpaceQuota* space = Quota::GetResponsibleSpaceQuota(path);
	if (space) {
	  eos::QuotaNode* quotanode = 0;
	  quotanode = space->GetQuotaNode();
	  if (quotanode) {
	    quotanode->addFile(fmd);
	  }
	}
      } catch( eos::MDException &e ) {
	errno = e.getErrno();
	std::string errmsg = e.getMessage().str();
	eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());	
	gOFS->MgmStats.Add("OpenFailedQuota",vid.uid,vid.gid,1);  
	return Emsg(epname, error, errno, "open file", errmsg.c_str());      
      }     
      //-------------------------------------------
    }
  }
  
  capability += "&mgm.ruid=";       capability+=(int)vid.uid; 
  capability += "&mgm.rgid=";       capability+=(int)vid.gid;
  capability += "&mgm.uid=";      capability+=(int)vid.uid_list[0]; 
  capability += "&mgm.gid=";      capability+=(int)vid.gid_list[0];
  capability += "&mgm.path=";      capability += path;
  capability += "&mgm.manager=";   capability += gOFS->ManagerId.c_str();
  capability += "&mgm.fid=";    XrdOucString hexfid; eos::common::FileId::Fid2Hex(fileId,hexfid);capability += hexfid;

  XrdOucString sizestring;
  capability += "&mgm.cid=";       capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
  
  if (attrmap.count("user.tag")) {
    capability += "&mgm.container="; 
    capability += attrmap["user.tag"].c_str();
  }

  unsigned long long bookingsize; // the size which will be reserved with a placement of one replica for that file
  unsigned long long targetsize = 0;

  if (attrmap.count("sys.forced.bookingsize")) {
    // we allow only a system attribute not to get fooled by a user
    bookingsize = strtoull(attrmap["sys.forced.bookingsize"].c_str(),0,10);
  }  else {
    if (attrmap.count("user.forced.bookingsize")) {
      bookingsize = strtoull(attrmap["user.forced.bookingsize"].c_str(),0,10);
    } else {
      bookingsize = 1024ll; // 1k as default
      if (openOpaque->Get("eos.bookingsize")) {
        bookingsize = strtoull(openOpaque->Get("eos.bookingsize"),0,10);
      } else {
        if (openOpaque->Get("oss.asize")) {
          bookingsize = strtoull(openOpaque->Get("oss.asize"),0,10);
        }
      }
    }
  }

  if (openOpaque->Get("oss.asize")) {
    targetsize = strtoull(openOpaque->Get("oss.asize"),0,10);
  }

  if (openOpaque->Get("eos.targetsize")) {
    targetsize = strtoull(openOpaque->Get("eos.targetsize"),0,10);
  }

  eos::mgm::FileSystem* filesystem = 0;

  std::vector<unsigned int> selectedfs;
  std::vector<unsigned int> unavailfs;  // file systems which are unavailable during a read operation
  std::vector<unsigned int>::const_iterator sfs;

  int retc = 0;

  // ************************************************************************************************
  if (isCreation || ( (open_mode == SFS_O_TRUNC) && (!fmd->getNumLocation()))) {
    // ************************************************************************************************
    // place a new file 
    const char* containertag = 0;
    if (attrmap.count("user.tag")) {
      containertag = attrmap["user.tag"].c_str();
    }
    retc = quotaspace->FilePlacement(path, vid.uid, vid.gid, containertag, layoutId, selectedfs, open_mode & SFS_O_TRUNC, -1, bookingsize);
  } else {
    // ************************************************************************************************
    // access existing file

    // fill the vector with the existing locations
    for (unsigned int i=0; i< fmd->getNumLocation(); i++) {
      int loc = fmd->getLocation(i);
      if (loc) 
        selectedfs.push_back(loc);
    }

    if (! selectedfs.size()) {
      // this file has not a single existing replica
      gOFS->MgmStats.Add("OpenFileOffline",vid.uid,vid.gid,1);  
      return Emsg(epname, error, ENODEV,  "open - no replica exists", path);        
    }

    retc = quotaspace->FileAccess(vid.uid, vid.gid, forcedFsId, space.c_str(), layoutId, selectedfs, fsIndex, isRW, fmd->getSize(),unavailfs);

    if (retc == EXDEV) {
      // --------------------------------------------------------------
      // indicating that the layout requires the replacement of stripes
      // --------------------------------------------------------------
      
      retc = 0; // TODO: we currently don't support repair on the fly mode
    }
  }

  if (retc) {
    // if we don't have quota we don't bounce the client back
    if (retc != ENOSPC) {
      // check if we should try to heal offline replicas (rw mode only)
      if ((!isCreation) && isRW && attrmap.count("sys.heal.unavailable")) {
        int nmaxheal = atoi(attrmap["sys.heal.unavailable"].c_str());
        int nheal=0;
        gOFS->MgmHealMapMutex.Lock();
        if (gOFS->MgmHealMap.count(fileId)) 
          nheal = gOFS->MgmHealMap[fileId];
        
        // if there was already a healing
        if ( nheal >= nmaxheal ) {
          // we tried nmaxheal times to heal, so we abort now and return an error to the client
          gOFS->MgmHealMap.erase(fileId);
          gOFS->MgmHealMap.resize(0);
          gOFS->MgmHealMapMutex.UnLock();
          gOFS->MgmStats.Add("OpenFailedHeal",vid.uid,vid.gid,1);  
          XrdOucString msg = "heal file with inaccesible replica's after "; msg += (int) nmaxheal; msg += " tries - giving up";
          eos_info(msg.c_str());
          return Emsg(epname, error, ENOSR, msg.c_str(), path);     
        } else {
          // increase the heal counter for that file id
          gOFS->MgmHealMap[fileId] = nheal+1;
          ProcCommand* procCmd = new ProcCommand();
          if (procCmd) {
            // issue the adjustreplica command as root
            eos::common::Mapping::VirtualIdentity vidroot;
            eos::common::Mapping::Copy(vid, vidroot);
            eos::common::Mapping::Root(vidroot);
            XrdOucString cmd = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.file.express=1&mgm.path="; cmd += path;
            procCmd->open("/proc/user/",cmd.c_str(), vidroot, &error);
            procCmd->close();
            delete procCmd;

            int stalltime = 60; // 1 min by default
            if (attrmap.count("sys.stall.unavailable")) {
              stalltime = atoi(attrmap["sys.stall.unavailable"].c_str());
            }
            gOFS->MgmStats.Add("OpenStalledHeal",vid.uid,vid.gid,1);  
            eos_info("attr=sys info=\"stalling file\" path=%s rw=%d stalltime=%d nstall=%d", path, isRW, stalltime, nheal);
            gOFS->MgmHealMapMutex.UnLock();
            return gOFS->Stall(error, stalltime, "Required filesystems are currently unavailable!");
          } else {
            gOFS->MgmHealMapMutex.UnLock();
            return Emsg(epname, error, ENOMEM,  "allocate memory for proc command", path);          
          }
        }
      }

      // check if the dir attributes tell us to let clients rebounce
      if (attrmap.count("sys.stall.unavailable")) {
        int stalltime = atoi(attrmap["sys.stall.unavailable"].c_str());
        
        if (stalltime) {
          // stall the client
          gOFS->MgmStats.Add("OpenStalled",vid.uid,vid.gid,1);  
          eos_info("attr=sys info=\"stalling file since replica's are down\" path=%s rw=%d",path, isRW);
          return gOFS->Stall(error, stalltime, "Required filesystems are currently unavailable!");
        }
      }
      
      if (attrmap.count("user.stall.unavailable")) {
        int stalltime = atoi(attrmap["user.stall.unavailable"].c_str());
        if (stalltime) {
          // stall the client
          gOFS->MgmStats.Add("OpenStalled",vid.uid,vid.gid,1);  
          eos_info("attr=user info=\"stalling file since replica's are down\" path=%s rw=%d",path, isRW);
          return gOFS->Stall(error, stalltime, "Required filesystems are currently unavailable!");
        }
      }

      if ((attrmap.count("sys.redirect.enonet"))) {
        // there is a redirection setting here if files are unaccessible
        redirectionhost = "";
        redirectionhost = attrmap["sys.redirect.enonet"].c_str();
        int portpos = 0;
        if ( (portpos = redirectionhost.find(":")) != STR_NPOS) {
          XrdOucString port = redirectionhost;
          port.erase(0,portpos+1);
          ecode = atoi(port.c_str());
          redirectionhost.erase(portpos);
        } else {
          ecode = 1094;
        }
        rcode = SFS_REDIRECT;
        error.setErrInfo(ecode, redirectionhost.c_str());
        gOFS->MgmStats.Add("RedirectENONET",vid.uid,vid.gid,1);  
        return rcode;
      }
      gOFS->MgmStats.Add("OpenFileOffline",vid.uid,vid.gid,1);  
    } else {
      if (isCreation) {
        // we will remove the created file in the namespace
        gOFS->_rem(cPath.GetPath(),error, vid, 0);
      }

      gOFS->MgmStats.Add("OpenFailedQuota",vid.uid,vid.gid,1);  
    }

    if (isRW) {
      return Emsg(epname, error, retc, "access quota space ", path);
    } else {
      return Emsg(epname, error, retc, "open file ", path);
    }
  }

  // ************************************************************************************************
  // get the redirection host from the first entry in the vector

  if (!selectedfs[fsIndex]) {
    eos_err("0 filesystem in selection");
    return Emsg(epname, error, ENONET,  "received filesystem id 0", path);          
  }

  if (FsView::gFsView.mIdView.count(selectedfs[fsIndex]))
    filesystem = FsView::gFsView.mIdView[selectedfs[fsIndex]];
  else
    return Emsg(epname, error, ENONET,  "received non-existent filesystem", path);          

  targethost = filesystem->GetString("host").c_str();
  targetport = atoi(filesystem->GetString("port").c_str());
  
  redirectionhost= targethost;
  redirectionhost+= "?";


  // rebuild the layout ID (for read it should indicate only the number of available stripes for reading);
  newlayoutId = eos::common::LayoutId::GetId(eos::common::LayoutId::GetLayoutType(layoutId), eos::common::LayoutId::GetChecksum(layoutId), (int)selectedfs.size(), eos::common::LayoutId::GetBlocksizeType(layoutId), eos::common::LayoutId::GetBlockChecksum(layoutId));
  capability += "&mgm.lid=";    
  capability += (int)newlayoutId;
  // space to be prebooked/allocated
  capability += "&mgm.bookingsize=";
  capability += eos::common::StringConversion::GetSizeString(sizestring,bookingsize);

  // expected size of the target file on close
  if (targetsize) {
    capability += "&mgm.targetsize=";
    capability += eos::common::StringConversion::GetSizeString(sizestring, targetsize);
  }
  
  if ( eos::common::LayoutId::GetLayoutType(layoutId) == eos::common::LayoutId::kPlain ) {
    capability += "&mgm.fsid="; capability += (int)filesystem->GetId();
    capability += "&mgm.localprefix="; capability+= filesystem->GetPath().c_str();
  }

  if ((eos::common::LayoutId::GetLayoutType(layoutId) == eos::common::LayoutId::kReplica) || 
      (eos::common::LayoutId::GetLayoutType(layoutId) == eos::common::LayoutId::kRaidDP) || 
      (eos::common::LayoutId::GetLayoutType(layoutId) == eos::common::LayoutId::kReedS)) {
    capability += "&mgm.fsid="; capability += (int)filesystem->GetId();
    capability += "&mgm.localprefix="; capability+= filesystem->GetPath().c_str();
    
    eos::mgm::FileSystem* repfilesystem = 0;
    // put all the replica urls into the capability
    for ( int i = 0; i < (int)selectedfs.size(); i++) {
      if (!selectedfs[i]) 
        eos_err("0 filesystem in replica vector");
      if (FsView::gFsView.mIdView.count(selectedfs[i]))
        repfilesystem = FsView::gFsView.mIdView[selectedfs[i]];
      else
        repfilesystem = 0;

      if (!repfilesystem) {
        return Emsg(epname, error, EINVAL, "get replica filesystem information",path);
      }
      capability += "&mgm.url"; capability += i; capability += "=root://";
      XrdOucString replicahost=""; int replicaport = 0;

      // ------------------------------------------------------------------- 
      // logic to mask 'offline' filesystems
      // ------------------------------------------------------------------- 
      bool exclude=false;
      for (size_t k = 0; k < unavailfs.size(); k++) {
	if (selectedfs[i] == unavailfs[k]) {
	  exclude=true;
	  break;
	}
      }

      if (exclude) {
	replicahost = "__offline_";
	replicahost += repfilesystem->GetString("host").c_str();
      } else {
	replicahost = repfilesystem->GetString("host").c_str();
      }

      replicaport = atoi(repfilesystem->GetString("port").c_str());

      capability += replicahost; capability += ":"; capability += replicaport; capability += "//";
      // add replica fsid
      capability += "&mgm.fsid"; capability += i; capability += "="; capability += (int)repfilesystem->GetId();
      capability += "&mgm.localprefix"; capability += i; capability += "=";capability+= repfilesystem->GetPath().c_str();

      eos_debug("Redirection Url %d => %s", i, replicahost.c_str());
    }
  }
  
  // encrypt capability
  XrdOucEnv  incapability(capability.c_str());
  XrdOucEnv* capabilityenv = 0;
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  int caprc=0;
  if ((caprc=gCapabilityEngine.Create(&incapability, capabilityenv, symkey))) {
    return Emsg(epname, error, caprc, "sign capability", path);
  }
  
  int caplen=0;
  redirectionhost+=capabilityenv->Env(caplen);
  redirectionhost+= "&mgm.logid="; redirectionhost+=this->logId;
  if (openOpaque->Get("eos.blockchecksum")) {
    redirectionhost += "&mgm.blockchecksum=";
    redirectionhost += openOpaque->Get("eos.blockchecksum");
  }
  if (openOpaque->Get("eos.checksum")) {
    redirectionhost += "&mgm.checksum=";
    redirectionhost += openOpaque->Get("eos.checksum");
  }


  // for the moment we redirect only on storage nodes
  redirectionhost+= "&mgm.replicaindex="; redirectionhost += (int)fsIndex;
  redirectionhost+= "&mgm.replicahead="; redirectionhost += (int)fsIndex;

  // always redirect
  ecode = targetport;
  rcode = SFS_REDIRECT;
  error.setErrInfo(ecode,redirectionhost.c_str());

  if (redirectionhost.length() > (int)XrdOucEI::Max_Error_Len) {
    return Emsg(epname, error, ENOMEM, "open file - capability exceeds 2kb limit", path);
  }

  //  ZTRACE(open, "Return redirection " << redirectionhost.c_str() << "Targetport: " << ecode);

  eos_info("info=\"redirection\" hostport=%s:%d", redirectionhost.c_str(), ecode);

  if (capabilityenv)
    delete capabilityenv;

  EXEC_TIMING_END("Open");

  return rcode;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::close()
/*
  Function: Close the file object.
    
  Input:    None
    
  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  //  static const char *epname = "close";

  oh = -1;
  if (fname) {free(fname); fname = 0;}

  if (procCmd) {
    procCmd->close();
    return SFS_OK;
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize XrdMgmOfsFile::read(XrdSfsFileOffset  offset,    // In
                                   char             *buff,      // Out
                                   XrdSfsXferSize    blen)      // In
/*
  Function: Read `blen' bytes at `offset' into 'buff' and return the actual
  number of bytes read.

  Input:    offset    - The absolute byte offset at which to start the read.
  buff      - Address of the buffer in which to place the data.
  blen      - The size of the buffer. This is the maximum number
  of bytes that will be read from 'fd'.

  Output:   Returns the number of bytes read upon success and SFS_ERROR o/w.
*/
{
  static const char *epname = "read";
  
  // Make sure the offset is not too large
  //
#if _FILE_OFFSET_BITS!=64
  if (offset >  0x000000007fffffff)
    return Emsg(epname, error, EFBIG, "read", fname);
#endif

  if (procCmd) {
    return procCmd->read(offset, buff, blen);
  }

  return Emsg(epname, error, EOPNOTSUPP, "read", fname);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::read(XrdSfsAio *aiop)
{
  static const char *epname = "read";
  // Execute this request in a synchronous fashion
  //
  return Emsg(epname, error, EOPNOTSUPP, "read", fname);
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize XrdMgmOfsFile::write(XrdSfsFileOffset   offset,    // In
                                    const char        *buff,      // In
                                    XrdSfsXferSize     blen)      // In
/*
  Function: Write `blen' bytes at `offset' from 'buff' and return the actual
  number of bytes written.

  Input:    offset    - The absolute byte offset at which to start the write.
  buff      - Address of the buffer from which to get the data.
  blen      - The size of the buffer. This is the maximum number
  of bytes that will be written to 'fd'.

  Output:   Returns the number of bytes written upon success and SFS_ERROR o/w.

  Notes:    An error return may be delayed until the next write(), close(), or
  sync() call.
*/
{
  static const char *epname = "write";
  
  
  // Make sure the offset is not too large
  //
#if _FILE_OFFSET_BITS!=64
  if (offset >  0x000000007fffffff)
    return Emsg(epname, error, EFBIG, "write", fname);
#endif

  return Emsg(epname, error, EOPNOTSUPP, "write", fname);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::write(XrdSfsAio *aiop)
{
  static const char *epname = "write";
  // Execute this request in a synchronous fashion
  //
  return Emsg(epname, error, EOPNOTSUPP, "write", fname);
}

/*----------------------------------------------------------------------------*/
/*
  Function: Return file status information

  Input:    buf         - The stat structure to hold the results

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
int XrdMgmOfsFile::stat(struct stat     *buf)         // Out
{
  static const char *epname = "stat";

  if (procCmd) 
    return procCmd->stat(buf);

  return Emsg(epname, error, EOPNOTSUPP, "stat", fname);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::sync()
/*
  Function: Commit all unwritten bytes to physical media.

  Input:    None

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "sync";
  return Emsg(epname, error, EOPNOTSUPP, "sync", fname);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::sync(XrdSfsAio *aiop)
{
  static const char *epname = "sync";
  // Execute this request in a synchronous fashion
  //
  return Emsg(epname, error, EOPNOTSUPP, "sync", fname);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::truncate(XrdSfsFileOffset  flen)  // In
/*
  Function: Set the length of the file object to 'flen' bytes.

  Input:    flen      - The new size of the file.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.

  Notes:    If 'flen' is smaller than the current size of the file, the file
  is made smaller and the data past 'flen' is discarded. If 'flen'
  is larger than the current size of the file, a hole is created
  (i.e., the file is logically extended by filling the extra bytes 
  with zeroes).
*/
{
  static const char *epname = "trunc";
  // Make sure the offset is not too larg
  //
#if _FILE_OFFSET_BITS!=64
  if (flen >  0x000000007fffffff)
    return Emsg(epname, error, EFBIG, "truncate", fname);
#endif

  return Emsg(epname, error, EOPNOTSUPP, "truncate", fname);
}

/*----------------------------------------------------------------------------*/
int            
XrdMgmOfs::chksum(      XrdSfsFileSystem::csFunc            Func,
			    const char             *csName,
			    const char             *inpath,
			    XrdOucErrInfo          &error,
			    const XrdSecEntity     *client,
			    const char             *opaque)
  /*
  Function: Compute and return file checksum.

  Input:    Func      - Function to be performed:
                        csCalc   - Return precomputed or computed checksum.
                        csGet    - Return precomputed checksum.
                        csSize   - Verify csName and get its size.
            path      - Pathname of file for csCalc and csSize.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            opaque    - Opaque information to be used as seen fit.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
  */
{
  static const char *epname = "chksum";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient();

  XrdOucEnv Open_Env(opaque);

  XrdOucEnv  cksEnv(opaque,0,client);
  char buff[MAXPATHLEN+8];
  int rc;
  
  XrdOucString CheckSumName = csName;

  // retrieve meta data for <path>

  gOFS->MgmStats.Add("Checksum",vid.uid,vid.gid,1);

  // A csSize request is issued usually once to verify everything is working. We
  // take this opportunity to also verify the checksum name.
  //

  rc = 0;

  if ((Func == XrdSfsFileSystem::csSize)) {
    if (CheckSumName == "eos") {
      // just return the length
      error.setErrCode(20); 
      return SFS_OK;
    } else {
      strcpy(buff, csName); strcat(buff, " checksum not supported.");
      error.setErrInfo(ENOTSUP, buff);
      return SFS_ERROR;
    }
  }
  
  NAMESPACEMAP;

  XTRACE(stat, path,csName);

  AUTHORIZE(client,&Open_Env,AOP_Stat,"stat",path,error);

  eos::common::Mapping::IdMap(client,opaque,tident,vid);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  eos_info("path=%s",inpath);

  //-------------------------------------------
  errno =0;
  eos::FileMD* fmd = 0;
  eos::common::Path cPath(path);

  // Everything else requires a path
  //

  if (!path){
    strcpy(buff, csName);
    strcat(buff, " checksum path not specified.");
    error.setErrInfo(EINVAL, buff);
    return SFS_ERROR;
  }

  
  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  
  try {
    fmd = gOFS->eosView->getFile(cPath.GetPath());
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
  }

  if (!fmd) {
    // file does not exist
    *buff = 0;
    rc = ENOENT;
    error.setErrInfo(rc, "no such file or directory");
    return SFS_ERROR;    
  }

  // Now determine what to do
  //
  if ( (Func == XrdSfsFileSystem::csCalc ) ||
       (Func == XrdSfsFileSystem::csGet  ) ) {
  } else {
    error.setErrInfo(EINVAL, "Invalid checksum function.");
    return SFS_ERROR;
  }

  // copy the checksum buffer
  const char *hv = "0123456789abcdef";
  size_t j = 0;
  for (size_t i = 0; i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++) {
    buff[j++] = hv[(fmd->getChecksum().getDataPtr()[i] >> 4) & 0x0f];
    buff[j++] = hv[ fmd->getChecksum().getDataPtr()[i]       & 0x0f];
  }
  buff[j] = '\0';
  eos_info("checksum is %s", buff);
  error.setErrInfo(0, buff);
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::chmod(const char                *inpath,    // In
                     XrdSfsMode        Mode,    // In
                     XrdOucErrInfo    &error,   // Out
                     const XrdSecEntity     *client,  // In
                     const char             *info)    // In
/*
  Function: Change the mode on a file or directory.

  Input:    path      - Is the fully qualified name of the file to be removed.
  einfo     - Error information object to hold error details.
  client    - Authentication credentials, if any.
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "chmod";
  const char *tident = error.getErrUser(); 
  //  mode_t acc_mode = Mode & S_IAMB;
  
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdOucEnv chmod_Env(info);

  NAMESPACEMAP;

  XTRACE(chmod, path,"");

  AUTHORIZE(client,&chmod_Env,AOP_Chmod,"chmod",path,error);


  eos::common::Mapping::IdMap(client,info,tident,vid);

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  
  return _chmod(path,Mode, error,vid,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_chmod(const char               *path,    // In
                      XrdSfsMode        Mode,    // In
                      XrdOucErrInfo    &error,   // Out
                      eos::common::Mapping::VirtualIdentity &vid,   // In
                      const char             *info)    // In

{
  static const char *epname = "chmod";

  EXEC_TIMING_BEGIN("Chmod");

  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  eos::ContainerMD* cmd = 0;
  eos::ContainerMD* pcmd = 0;
  eos::ContainerMD::XAttrMap attrmap;

  errno = 0;

  gOFS->MgmStats.Add("Chmod",vid.uid,vid.gid,1);  

  eos_info("path=%s mode=%o",path, Mode);

  eos::common::Path cPath(path);  

  try {
    cmd = gOFS->eosView->getContainer(path);
    pcmd = gOFS->eosView->getContainer(cPath.GetParentPath());

    eos::ContainerMD::XAttrMap::const_iterator it;
    for ( it = pcmd->attributesBegin(); it != pcmd->attributesEnd(); ++it) {
      attrmap[it->first] = it->second;
    }
    // acl of the parent!
    Acl acl(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);

    if ( (cmd->getCUid() == vid.uid) ||  // the owner
	 (!vid.uid) ||                   // the root user
	 (vid.uid ==3) ||                // the admin user
	 (vid.gid == 4) ||               // the admin group
	 (acl.CanChmod()) ) {            // the chmod ACL entry
      // change the permission mask, but make sure it is set to a directory
      if (Mode & S_IFREG) 
        Mode ^= S_IFREG;
      if ( (Mode & S_ISUID) ) {
	Mode ^= S_ISUID;
      } else {
	if (! (Mode & S_ISGID) ) {
	  Mode |= S_ISGID;
	}
      }
      cmd->setMode(Mode | S_IFDIR);
      eosView->updateContainerStore(cmd);
    } else {
      errno = EPERM;
    }
  } catch ( eos::MDException &e ) {
    errno = e.getErrno();
  };
  
  //-------------------------------------------

  if (cmd  && (!errno)) {
    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }
  
  return Emsg(epname, error, errno, "chmod", path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_chown(const char               *path,    // In
                      uid_t             uid,     // In
                      gid_t             gid,     // In
                      XrdOucErrInfo    &error,   // Out
                      eos::common::Mapping::VirtualIdentity &vid,   // In
                      const char             *info)    // In

{
  static const char *epname = "chown";

  EXEC_TIMING_BEGIN("Chown");

  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  eos::ContainerMD* cmd = 0;
  eos::FileMD* fmd = 0;
  errno = 0;

  gOFS->MgmStats.Add("Chown",vid.uid,vid.gid,1);  

  eos_info("path=%s uid=%u gid=%u",path, uid,gid);
  
  // try as a directory
  try {
    cmd = gOFS->eosView->getContainer(path);
    if ( (vid.uid) && (vid.uid != 3) && (vid.gid != 4) && !cmd->access(vid.uid,vid.gid,W_OK)) {
      errno = EPERM;
    } else {
      // change the owner
      cmd->setCUid(uid);
      if (((!vid.uid) || (vid.uid ==3) || (vid.gid == 4)) && gid) {
        // change the group
        cmd->setCGid(gid);
      }
      eosView->updateContainerStore(cmd);
    }
  } catch ( eos::MDException &e ) {
    errno = e.getErrno();
  };
  
  if (!cmd) {
    errno = 0;
    try {
      // try as a file
      eos::common::Path cPath(path);
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());

      SpaceQuota* space = Quota::GetResponsibleSpaceQuota(cPath.GetParentPath());
      eos::QuotaNode* quotanode = 0;
      if (space) {
        quotanode = space->GetQuotaNode();
      }

      if ( (vid.uid) && (!vid.sudoer) && (vid.uid != 3) && (vid.gid !=4) ) {
        errno = EPERM;
      } else {
        fmd = gOFS->eosView->getFile(path);

        // substract the file
        if (quotanode) {
          quotanode->removeFile(fmd);
        }

        // change the owner
        fmd->setCUid(uid);

        // re-add the file
        if (quotanode) {
          quotanode->addFile(fmd);
        }

        if (!vid.uid) {
          if (gid) {
            // change the group
            fmd->setCGid(gid);
          } else {
            if (!uid)
              fmd->setCGid(uid);
          }
        }

        eosView->updateFileStore(fmd);
      }
    } catch ( eos::MDException &e ) {
      errno = e.getErrno();
    };
  }

  //-------------------------------------------

  if (cmd  && (!errno)) {
    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }
  
  return Emsg(epname, error, errno, "chown", path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::exists(const char                *inpath,        // In
                      XrdSfsFileExistence &file_exists, // Out
                      XrdOucErrInfo       &error,       // Out
                      const XrdSecEntity    *client,          // In
                      const char                *info)        // In

{
  static const char *epname = "exists";
  const char *tident = error.getErrUser();


  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdOucEnv exists_Env(info);

  NAMESPACEMAP;

  XTRACE(exists, path,"");

  AUTHORIZE(client,&exists_Env,AOP_Stat,"execute exists",path,error);

  eos::common::Mapping::IdMap(client,info,tident,vid);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  
  return _exists(path,file_exists,error,vid,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_exists(const char                *path,        // In
                       XrdSfsFileExistence &file_exists, // Out
                       XrdOucErrInfo       &error,       // Out
                       const XrdSecEntity       *client,       // In
                       const char                *info)        // In
/*
  Function: Determine if file 'path' actually exists.

  Input:    path        - Is the fully qualified name of the file to be tested.
            file_exists - Is the address of the variable to hold the status of
                          'path' when success is returned. The values may be:
                          XrdSfsFileExistIsDirectory - file not found but path is valid.
                          XrdSfsFileExistIsFile      - file found.
                          XrdSfsFileExistNo          - neither file nor directory.
            einfo       - Error information object holding the details.
            client      - Authentication credentials, if any.
            info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.

  Notes:    When failure occurs, 'file_exists' is not modified.
*/
{
  // try if that is directory
  EXEC_TIMING_BEGIN("Exists");

  gOFS->MgmStats.Add("Exists",vid.uid,vid.gid,1);  

  eos::ContainerMD* cmd = 0;

  {
    //-------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try {
      cmd = gOFS->eosView->getContainer(path);
    } catch ( eos::MDException &e ) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());      
    };
    //-------------------------------------------
  }

  if (!cmd) {
    // try if that is a file
    //-------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);   
    eos::FileMD* fmd = 0;
    try {
      fmd = gOFS->eosView->getFile(path);
    } catch( eos::MDException &e ) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
    }
    //-------------------------------------------

    if (!fmd) {
      file_exists=XrdSfsFileExistNo;
    } else {
      file_exists=XrdSfsFileExistIsFile;
    }
  } else {
    file_exists = XrdSfsFileExistIsDirectory;
  }

  if (file_exists == XrdSfsFileExistNo) {
    // get the parent directory
    eos::common::Path cPath(path);
    eos::ContainerMD* dir=0;
    eos::ContainerMD::XAttrMap attrmap;
    
    //-------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);    
    try {
      dir = eosView->getContainer(cPath.GetParentPath());
      eos::ContainerMD::XAttrMap::const_iterator it;
      for ( it = dir->attributesBegin(); it != dir->attributesEnd(); ++it) {
        attrmap[it->first] = it->second;
      }
    } catch( eos::MDException &e ) {
      dir = 0;
    }    
    //-------------------------------------------
    
    if (dir) {
      XrdOucString redirectionhost="invalid?";
      int ecode=0;
      int rcode=SFS_OK;
      if (attrmap.count("sys.redirect.enoent")) {
        // there is a redirection setting here
        redirectionhost = "";
        redirectionhost = attrmap["sys.redirect.enoent"].c_str();
        int portpos = 0;
        if ( (portpos = redirectionhost.find(":")) != STR_NPOS) {
          XrdOucString port = redirectionhost;
          port.erase(0,portpos+1);
          ecode = atoi(port.c_str());
          redirectionhost.erase(portpos);
        } else {
          ecode = 1094;
        }
        rcode = SFS_REDIRECT;
        error.setErrInfo(ecode, redirectionhost.c_str());
        gOFS->MgmStats.Add("RedirectENOENT",vid.uid,vid.gid,1);
        return rcode;
      }
    }
  }

  EXEC_TIMING_END("Exists");  
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_exists(const char                *path,        // In
                       XrdSfsFileExistence &file_exists, // Out
                       XrdOucErrInfo       &error,       // Out
                       eos::common::Mapping::VirtualIdentity &vid,   // In
                       const char                *info)        // In
/*
  Function: Determine if file 'path' actually exists.

  Input:    path        - Is the fully qualified name of the file to be tested.
  :            file_exists - Is the address of the variable to hold the status of
  'path' when success is returned. The values may be:
  XrdSfsFileExistIsDirectory - file not found but path is valid.
  XrdSfsFileExistIsFile      - file found.
  XrdSfsFileExistNo          - neither file nor directory.
  einfo       - Error information object holding the details.
  vid         - Virtual Identity
  info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.

  Notes:    When failure occurs, 'file_exists' is not modified.
*/
{
  EXEC_TIMING_BEGIN("Exists");

  gOFS->MgmStats.Add("Exists",vid.uid,vid.gid,1);  

  eos::ContainerMD* cmd = 0;
  
  // try if that is directory
  {
    //-------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try {
      cmd = gOFS->eosView->getContainer(path);
    } catch ( eos::MDException &e ) {
      cmd = 0;
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());      
    };  
    //-------------------------------------------
  }

  if (!cmd) {
    // try if that is a file
    //-------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);    
    eos::FileMD* fmd = 0;
    try {
      fmd = gOFS->eosView->getFile(path);
    } catch( eos::MDException &e ) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());      
    }
    //-------------------------------------------

    if (!fmd) {
      file_exists=XrdSfsFileExistNo;
    } else {
      file_exists=XrdSfsFileExistIsFile;
    }
  } else {
    file_exists = XrdSfsFileExistIsDirectory;
  }

  EXEC_TIMING_END("Exists");  
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
const char *XrdMgmOfs::getVersion() {
  static XrdOucString FullVersion = XrdVERSION;
  FullVersion += " MgmOfs "; FullVersion += VERSION;
  return FullVersion.c_str();
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::mkdir(const char              *inpath,    // In
                     XrdSfsMode        Mode,    // In
                     XrdOucErrInfo    &error,   // Out
                     const XrdSecEntity     *client,  // In
                     const char             *info)    // In
{
  static const char *epname = "mkdir";
  const char *tident = error.getErrUser();
  
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;
  
  XrdOucEnv mkdir_Env(info);
  
  NAMESPACEMAP; 

  XTRACE(mkdir, path,"");
  
  eos::common::Mapping::IdMap(client,info,tident,vid);

  eos_info("path=%s",path);
  
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  
  return  _mkdir(path,Mode,error,vid,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_mkdir(const char            *path,    // In
                      XrdSfsMode             Mode,    // In
                      XrdOucErrInfo         &error,   // Out
                      eos::common::Mapping::VirtualIdentity &vid, // In
                      const char            *info)    // In
/*
  Function: Create a directory entry.

  Input:    path      - Is the fully qualified name of the file to be removed.
  Mode      - Is the POSIX mode setting for the directory. If the
  mode contains SFS_O_MKPTH, the full path is created.
  einfo     - Error information object to hold error details.
  vid       - Virtual Identity
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "_mkdir";
  mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
  errno = 0;

  EXEC_TIMING_BEGIN("Mkdir");

  gOFS->MgmStats.Add("Mkdir",vid.uid,vid.gid,1);  

  //  const char *tident = error.getErrUser();

  XrdOucString spath= path;

  eos_info("path=%s\n", spath.c_str());

  if (!spath.beginswith("/")) {
    errno = EINVAL;
    return Emsg(epname,error,EINVAL,"create directory - you have to specifiy an absolute pathname",path);
  }

  bool recurse = false;
  
  eos::common::Path cPath(path);
  bool noParent=false;

  eos::ContainerMD* dir=0;
  eos::ContainerMD::XAttrMap attrmap;
  eos::ContainerMD* copydir=0;

  {
    //-------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    
    // check for the parent directory
    if (spath != "/") {
      try {
	dir = eosView->getContainer(cPath.GetParentPath());
	copydir = new eos::ContainerMD(*dir);
	dir = copydir;
	eos::ContainerMD::XAttrMap::const_iterator it;
	for ( it = dir->attributesBegin(); it != dir->attributesEnd(); ++it) {
	  attrmap[it->first] = it->second;
	}
      } catch( eos::MDException &e ) {
	dir = 0;
	eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());       
	noParent = true;
      }
    }
    
    // check permission
    if (dir ) {
      // ACL and permission check
      Acl acl(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
      
      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d", acl.HasAcl(),acl.CanRead(),acl.CanWrite(),acl.CanWriteOnce(), acl.HasEgroup());
      bool stdpermcheck=false;
      if (acl.HasAcl()) {
	if ( (!acl.CanWrite()) && (!acl.CanWriteOnce()) ) {
	  // we have to check the standard permissions
	  stdpermcheck = true;
	}
      } else {
	stdpermcheck = true;
      }
      
      // admin's can always create a directory
      if (stdpermcheck && 
	  (!dir->access(vid.uid,vid.gid, X_OK|W_OK))
	  ) {
	if (copydir) delete copydir;
	errno = EPERM;
	
      return Emsg(epname, error, EPERM, "create parent directory", cPath.GetParentPath());
      }
    }
  }
  
  // check if the path exists anyway
  if (Mode & SFS_O_MKPTH) {
    recurse = true;
    eos_debug("SFS_O_MKPATH set",path);
    // short cut if it exists already
    eos::ContainerMD* fulldir=0;
    if (dir) {
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      // only if the parent exists, the full path can exist!
      try {
        fulldir = eosView->getContainer(path);
      } catch( eos::MDException &e ) {
        fulldir = 0;
	eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());       
      }
      if (fulldir) {
        if (copydir) delete copydir;
        EXEC_TIMING_END("Exists");      
        return SFS_OK;
      }
    }
  }
  
  eos_debug("mkdir path=%s deepness=%d dirname=%s basename=%s",path, cPath.GetSubPathSize(), cPath.GetParentPath(), cPath.GetName());
  eos::ContainerMD* newdir = 0;

  if (noParent) {
    if (recurse) {
      int i,j;
      // go the paths up until one exists!
      for (i=cPath.GetSubPathSize()-1;i>=0; i--) {
	eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
        attrmap.clear();
        eos_debug("testing path %s", cPath.GetSubPath(i));
        try {
          if (copydir) delete copydir;    
          dir = eosView->getContainer(cPath.GetSubPath(i));
          copydir = new eos::ContainerMD(*dir);
          eos::ContainerMD::XAttrMap::const_iterator it;
          for ( it = dir->attributesBegin(); it != dir->attributesEnd(); ++it) {
            attrmap[it->first] = it->second;
          }
        } catch( eos::MDException &e ) {
          dir = 0;
        }
        if (dir)
          break;
      }
      // that is really a serious problem!
      if (!dir) {
        if (copydir) delete copydir;
        eos_crit("didn't find any parent path traversing the namespace");
        errno = ENODATA;       
        //-------------------------------------------
        return Emsg(epname, error, ENODATA, "create directory", cPath.GetSubPath(i));
      }
   
      // ACL and permission check
      Acl acl(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
      
      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d", acl.HasAcl(),acl.CanRead(),acl.CanWrite(),acl.CanWriteOnce(), acl.HasEgroup());
      bool stdpermcheck=false;
      if (acl.HasAcl()) {
        if ( (!acl.CanWrite()) && (!acl.CanWriteOnce()) ) {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      } else {
        stdpermcheck = true;
      }
      
      if (stdpermcheck && (!dir->access(vid.uid,vid.gid, X_OK|W_OK))) {
        if (copydir) delete copydir;
        errno = EPERM;

        return Emsg(epname, error, EPERM, "create parent directory", cPath.GetParentPath());
      }
   
      
      for (j=i+1; j< (int)cPath.GetSubPathSize(); j++) {
	eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
        try {
          eos_debug("creating path %s", cPath.GetSubPath(j));
          newdir = eosView->createContainer(cPath.GetSubPath(j), recurse);
          newdir->setCUid(vid.uid);
          newdir->setCGid(vid.gid);
          newdir->setMode(dir->getMode());
          
          if (dir->getMode() & S_ISGID) {
            // inherit the attributes
            eos::ContainerMD::XAttrMap::const_iterator it;
            for (it = dir->attributesBegin(); it != dir->attributesEnd() ; ++it) {
              newdir->setAttribute( it->first, it->second);
            }
          }
          // commit
          eosView->updateContainerStore(newdir);
        } catch( eos::MDException &e ) {
          errno = e.getErrno();
	  eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());          
        }

        dir = newdir;
        if (dir) {
          if (copydir) delete copydir;
          copydir = new eos::ContainerMD(*dir);
          dir = copydir;
        }
        
        if (!newdir) {
          if (copydir) delete copydir;
          return Emsg(epname,error,errno,"mkdir",path);
        }
      }
    } else {
      if (copydir) delete copydir;
      errno = ENOENT;
      return Emsg(epname,error,errno,"mkdir",path);
    }
  }

  // this might not be needed, but it is detected by coverty
  if (!dir) {   
    return Emsg(epname,error,errno,"mkdir",path);
  }

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);    
  try {
    newdir = eosView->createContainer(path);
    newdir->setCUid(vid.uid);
    newdir->setCGid(vid.gid);
    newdir->setMode(acc_mode);
    newdir->setMode(dir->getMode());

    // store the in-memory modification time
    gOFS->MgmDirectoryModificationTimeMutex.Lock();
    eos::ContainerMD::ctime_t ctime;
    newdir->getCTime(ctime);
    gOFS->MgmDirectoryModificationTime[dir->getId()].tv_sec = ctime.tv_sec;
    gOFS->MgmDirectoryModificationTime[dir->getId()].tv_nsec = ctime.tv_nsec;
    gOFS->MgmDirectoryModificationTimeMutex.UnLock();

    if (dir->getMode() & S_ISGID) {
      // inherit the attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = dir->attributesBegin(); it != dir->attributesEnd() ; ++it) {
        newdir->setAttribute( it->first, it->second);
      }
    }
    // commit on disk
    eosView->updateContainerStore(newdir);
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());  
  }

  if (copydir) delete copydir;

  if (!newdir) {
    return Emsg(epname,error,errno,"mkdir",path);
  }

  EXEC_TIMING_END("Mkdir");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::prepare( XrdSfsPrep       &pargs,
                        XrdOucErrInfo    &error,
                        const XrdSecEntity *client)
{
  //  static const char *epname = "prepare";
  const char *tident = error.getErrUser();  

  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::IdMap(client,0,tident, vid);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::rem(const char             *inpath,    // In
                   XrdOucErrInfo    &error,   // Out
                   const XrdSecEntity *client,  // In
                   const char             *info)    // In
/*
  Function: Delete a file from the namespace.

  Input:    path      - Is the fully qualified name of the file to be removed.
  einfo     - Error information object to hold error details.
  client    - Authentication credentials, if any.
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "rem";
  const char *tident = error.getErrUser();
    
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP; 

  XTRACE(remove, path,"");
  
  XrdOucEnv env(info);
  
  AUTHORIZE(client,&env,AOP_Delete,"remove",path,error);
  
  XTRACE(remove, path,"");
  
  eos::common::Mapping::IdMap(client,info,tident,vid);

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  
  return _rem(path,error,vid,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_rem(   const char             *path,    // In
                       XrdOucErrInfo          &error,   // Out
                       eos::common::Mapping::VirtualIdentity &vid, //In
                       const char             *info)    // In
/*
  Function: Delete a file from the namespace.

  Input:    path      - Is the fully qualified name of the dir to be removed.
  einfo     - Error information object to hold error details.
  client    - Authentication credentials, if any.
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "rem";
  
  EXEC_TIMING_BEGIN("Rm");

  eos_info("path=%s vid.uid=%u vid.gid=%u", path, vid.uid, vid.gid);

  gOFS->MgmStats.Add("Rm",vid.uid,vid.gid,1);  

  // Perform the actual deletion
  //
  const char *tident = error.getErrUser();
  errno = 0;
  
  XTRACE(remove, path,"");


  XrdSfsFileExistence file_exists;
  if ((_exists(path,file_exists,error,vid,0))) {
    return SFS_ERROR;
  }

  if (file_exists!=XrdSfsFileExistIsFile) {
    if (file_exists == XrdSfsFileExistIsDirectory)
      errno = EISDIR;
    else
      errno = ENOENT;

    return Emsg(epname, error, errno,"remove",path);
  }

  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

  // free the booked quota
  eos::FileMD* fmd=0;
  eos::ContainerMD* container=0;

  eos::ContainerMD::XAttrMap attrmap;
  Acl acl;

  try { 
    fmd = gOFS->eosView->getFile(path);
  } catch ( eos::MDException &e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());    
  }

  if (fmd) {
    try {
      container = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      // get the attributes out
      eos::ContainerMD::XAttrMap::const_iterator it;
      for ( it = container->attributesBegin(); it != container->attributesEnd(); ++it) {
        attrmap[it->first] = it->second;
      }
    } catch ( eos::MDException &e ) {
      container = 0;
    }

    // ACL and permission check
    acl.Set(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
    bool stdpermcheck=false;
    if (acl.HasAcl()) {
      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d", acl.HasAcl(),acl.CanRead(),acl.CanWrite(),acl.CanWriteOnce(), acl.HasEgroup());

      if ( (!acl.CanWrite()) && (!acl.CanWriteOnce()) ) {
        // we have to check the standard permissions
        stdpermcheck = true;
      }
    } else {
      stdpermcheck = true;
    }
    
    if (container) {
      if (stdpermcheck && (!container->access(vid.uid, vid.gid, W_OK | X_OK))) {
        errno = EPERM;
        return Emsg(epname, error, errno, "remove file", path);      
      }
      
      // check if this directory is write-once for the mapped user
      if (acl.CanWriteOnce() && (fmd->getSize())) {
        errno = EPERM;
        // this is a write once user
        return Emsg(epname, error, EPERM,"remove existing file - you are write-once user");
      }

      if ( (vid.uid) && (vid.uid != container->getCUid()) && (vid.uid !=3) && (vid.gid != 4) && (acl.CanNotDelete()) ) {
	errno = EPERM;
	// deletion is forbidden for not-owner
        return Emsg(epname, error, EPERM,"remove existing file - ACL forbids file deletion");
      }
      if ( (!stdpermcheck) && (!acl.CanWrite())) {
        errno = EPERM;
        // this user is not allowed to write
        return Emsg(epname, error, EPERM,"remove existing file - you don't have write permissions");
      }

      eos::QuotaNode* quotanode = 0;
      try {
        quotanode = gOFS->eosView->getQuotaNode(container);
	if (quotanode) {
	  quotanode->removeFile(fmd);
	}
      } catch ( eos::MDException &e ) {
        quotanode = 0;
      }
    }
  }

  try {
    gOFS->eosView->unlinkFile(path);
    if ((!fmd->getNumUnlinkedLocation()) && (!fmd->getNumLocation())) {
      gOFS->eosView->removeFile( fmd );
    }

    if (container) {
      struct timespec ts;
      eos::common::Timing::GetTimeSpec(ts);
      // update the in-memory modification time
      gOFS->MgmDirectoryModificationTimeMutex.Lock();
      gOFS->MgmDirectoryModificationTime[container->getId()].tv_sec = ts.tv_sec;
      gOFS->MgmDirectoryModificationTime[container->getId()].tv_nsec = ts.tv_nsec;
      gOFS->MgmDirectoryModificationTimeMutex.UnLock();
    }
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());   
  };

  EXEC_TIMING_END("Rm");

  if (errno) 
    return Emsg(epname, error, errno, "remove", path);
  else 
    return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::remdir(const char             *inpath,    // In
                      XrdOucErrInfo    &error,   // Out
                      const XrdSecEntity *client,  // In
                      const char             *info)    // In
/*
  Function: Delete a directory from the namespace.

  Input:    path      - Is the fully qualified name of the dir to be removed.
  einfo     - Error information object to hold error details.
  client    - Authentication credentials, if any.
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "remdir";
  const char *tident = error.getErrUser();
      
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;
  
  XrdOucEnv remdir_Env(info);
  
  XrdSecEntity mappedclient();

  NAMESPACEMAP;

  XTRACE(remove, path,"");
  
  AUTHORIZE(client,&remdir_Env,AOP_Delete,"remove",path, error);

  eos::common::Mapping::IdMap(client,info,tident,vid);
  
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _remdir(path,error,vid,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_remdir(const char             *path,    // In
                       XrdOucErrInfo          &error,   // Out
                       eos::common::Mapping::VirtualIdentity &vid, // In
                       const char             *info)    // In
/*
  Function: Delete a directory from the namespace.

  Input:    path      - Is the fully qualified name of the dir to be removed.
  einfo     - Error information object to hold error details.
  vid       - Virtual Identity
  info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "remdir";
  errno = 0;

  EXEC_TIMING_BEGIN("RmDir");

  gOFS->MgmStats.Add("RmDir",vid.uid,vid.gid,1);  

  eos::ContainerMD* dhpar=0;
  eos::ContainerMD* dh=0;

  eos::ContainerMD::id_t dh_id=0;
  eos::ContainerMD::id_t dhpar_id=0;

  eos::common::Path cPath(path);
  eos::ContainerMD::XAttrMap attrmap;

   
  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
 
  try {
    dhpar = gOFS->eosView->getContainer(cPath.GetParentPath());
    dhpar_id = dhpar->getId();
    eos::ContainerMD::XAttrMap::const_iterator it;
    for ( it = dhpar->attributesBegin(); it != dhpar->attributesEnd(); ++it) {
      attrmap[it->first] = it->second;
    }
    dh = gOFS->eosView->getContainer(path);
    dh_id = dh->getId();
  } catch( eos::MDException &e ) {
    dhpar = 0;
    dh=0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());  
  }

  Acl acl(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
   
  bool stdpermcheck=false;
  bool aclok=false;
  if (acl.HasAcl()) {
    if ( (dh->getCUid() != vid.uid) &&
	 (vid.uid) &&                   // not the root user
	 (vid.uid != 3) &&              // not the admin user
	 (vid.gid != 4) &&              // notthe admin group
	 (acl.CanNotDelete()) ) {
      // deletion is explicitly forbidden
      errno = EPERM;
      return Emsg(epname, error, EPERM, "rmdir by ACL", path);
    }
    
    if ( (!acl.CanWrite()) ) {
      // we have to check the standard permissions
      stdpermcheck = true;
    } else {
      aclok=true;
    }
  } else {
    stdpermcheck = true;
  }
   
   
  // check permissions
  bool permok = stdpermcheck?(dhpar?(dhpar->access(vid.uid,vid.gid, X_OK|W_OK)): false):aclok;

  // check existence

  if (!dh) {
    errno = ENOENT;
    return Emsg(epname, error, errno, "rmdir", path);
  }
   
  if (!permok) {
    errno = EPERM;
    return Emsg(epname, error, errno, "rmdir", path);
  }
    
  try {
    // remove the in-memory modification time of the deleted directory
    gOFS->MgmDirectoryModificationTimeMutex.Lock();
    gOFS->MgmDirectoryModificationTime.erase(dh_id);
    struct timespec ts;
    // update the in-memory modification time of the parent directory
    eos::common::Timing::GetTimeSpec(ts);
    gOFS->MgmDirectoryModificationTime[dhpar_id].tv_sec = ts.tv_sec;
    gOFS->MgmDirectoryModificationTime[dhpar_id].tv_nsec = ts.tv_nsec;
    gOFS->MgmDirectoryModificationTimeMutex.UnLock();

    eosView->removeContainer(path);
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());   
  }

  EXEC_TIMING_END("RmDir");

  if (errno) {
    return Emsg(epname, error, errno, "rmdir", path);
  } else {
    return SFS_OK;
  }
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::rename(const char             *old_name,  // In
                      const char             *new_name,  // In
                      XrdOucErrInfo    &error,     //Out
                      const XrdSecEntity *client,    // In
                      const char             *infoO,     // In
                      const char             *infoN)     // In
/*
  Function: Renames a file/directory with name 'old_name' to 'new_name'.

  Input:    old_name  - Is the fully qualified name of the file to be renamed.
  new_name  - Is the fully qualified name that the file is to have.
  error     - Error information structure, if an error occurs.
  client    - Authentication credentials, if any.
  info      - old_name opaque information, if any.
  info      - new_name opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "rename";
  const char *tident = error.getErrUser();
  errno = 0;

  EXEC_TIMING_BEGIN("Rename");
   
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdOucString source, destination;
  XrdOucString oldn,newn;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  
  AUTHORIZE(client,&renameo_Env,AOP_Update,"rename",old_name, error);
  AUTHORIZE(client,&renamen_Env,AOP_Update,"rename",new_name, error);

  // we need to add also the namespace re mapping here ...
  oldn = old_name;
  newn = new_name;

  eos::common::Mapping::IdMap(client,infoO,tident,vid);

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  //  int r1,r2;
  
  //  r1=r2=SFS_OK;


  gOFS->MgmStats.Add("Rename",vid.uid,vid.gid,1);  

  // check if dest is existing
  //  XrdSfsFileExistence file_exists;

  //  if (!_exists(newn.c_str(),file_exists,error,vid,infoN)) {
  //    // it exists
  //    if (file_exists == XrdSfsFileExistIsDirectory) {
  //      // we have to path the destination name since the target is a directory
  //      XrdOucString sourcebase = oldn.c_str();
  //      int npos = oldn.rfind("/");
  //      if (npos == STR_NPOS) {
  //    return Emsg(epname, error, EINVAL, "rename", oldn.c_str());
  //      }
  //      sourcebase.assign(oldn, npos);
  //      newn+= "/";
  //      newn+= sourcebase;
  //      while (newn.replace("//","/")) {};
  //    }
  //    if (file_exists == XrdSfsFileExistIsFile) {
  // remove the target file first!
  //      int remrc = 0;//_rem(newn.c_str(),error,&mappedclient,infoN);
  //      if (remrc) {
  //    return remrc;
  //      }
  //    }
  //  }

  //  r1 = XrdMgmOfsUFS::Rename(oldn.c_str(), newn.c_str());

  //  if (r1) 
  //    return Emsg(epname, error, serrno, "rename", oldn.c_str());

  EXEC_TIMING_END("Rename");

  return Emsg(epname, error, EOPNOTSUPP, "rename", oldn.c_str());
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::stat(const char              *inpath,      // In
                    struct stat             *buf,         // Out
                    XrdOucErrInfo           &error,       // Out
                    const XrdSecEntity      *client,      // In
                    const char              *info)        // In
/*
  Function: Get info on 'path'.

  Input:    path        - Is the fully qualified name of the file to be tested.
  buf         - The stat structiure to hold the results
  error       - Error information object holding the details.
  client      - Authentication credentials, if any.
  info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "stat";
  const char *tident = error.getErrUser(); 

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient();

  XrdOucEnv Open_Env(info);

  NAMESPACEMAP; 
  
  XTRACE(stat, path,"");

  AUTHORIZE(client,&Open_Env,AOP_Stat,"stat",path,error);

  eos::common::Mapping::IdMap(client,info,tident,vid);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return _stat(path, buf, error, vid, info);  
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_stat(const char              *path,        // In
                     struct stat       *buf,         // Out
                     XrdOucErrInfo     &error,       // Out
                     eos::common::Mapping::VirtualIdentity &vid,  // In
                     const char              *info)        // In
{
  static const char *epname = "_stat";

  EXEC_TIMING_BEGIN("Stat");


  gOFS->MgmStats.Add("Stat",vid.uid,vid.gid,1);  

  //-------------------------------------------

  // try if that is a file
  errno =0;
  eos::FileMD* fmd = 0; 
  eos::common::Path cPath(path);

  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  try {
    fmd = gOFS->eosView->getFile(cPath.GetPath());
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());   
  }
  
  //-------------------------------------------
  if (fmd) {
    eos::FileMD fmdCopy(*fmd);
    fmd = &fmdCopy;   
    memset(buf, 0, sizeof(struct stat));
    
    buf->st_dev     = 0xcaff;
    buf->st_ino     = fmd->getId() << 28;
    buf->st_mode    = S_IFREG;
    buf->st_mode    |= (S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR );
    buf->st_nlink   = 1;
    buf->st_uid     = fmd->getCUid();
    buf->st_gid     = fmd->getCGid();
    buf->st_rdev    = 0;     /* device type (if inode device) */
    buf->st_size    = fmd->getSize();
    buf->st_blksize = 512;
    buf->st_blocks  = fmd->getSize() / 512;
    eos::FileMD::ctime_t atime;
    
    // adding also nanosecond to stat struct
    fmd->getCTime(atime);
    buf->st_ctime   = atime.tv_sec;
    buf->st_ctim.tv_sec   = atime.tv_sec;
    buf->st_ctim.tv_nsec   = atime.tv_nsec;
    
    fmd->getMTime(atime);
    buf->st_mtime   = atime.tv_sec;
    buf->st_mtim.tv_sec   = atime.tv_sec;
    buf->st_mtim.tv_nsec   = atime.tv_nsec;
    
    buf->st_atime   = atime.tv_sec;
    buf->st_atim.tv_sec   = atime.tv_sec;
    buf->st_atim.tv_nsec  = atime.tv_nsec;
    EXEC_TIMING_END("Stat");    
    return SFS_OK;
  }
  
  // try if that is directory
  eos::ContainerMD* cmd = 0;
  errno = 0;

  //-------------------------------------------
  try {
    cmd = gOFS->eosView->getContainer(cPath.GetPath());
    
    memset(buf, 0, sizeof(struct stat));
    
    buf->st_dev     = 0xcaff;
    buf->st_ino     = cmd->getId();
    buf->st_mode    = cmd->getMode();
    if (cmd->attributesBegin() != cmd->attributesEnd()) {
      buf->st_mode |= S_ISVTX;
    }
    buf->st_nlink   = cmd->getNumContainers() + cmd->getNumFiles() + 1;
    buf->st_uid     = cmd->getCUid();
    buf->st_gid     = cmd->getCGid();
    buf->st_rdev    = 0;     /* device type (if inode device) */
    buf->st_size    = cmd->getNumContainers();
    buf->st_blksize = 0;
    buf->st_blocks  = 0;
    eos::ContainerMD::ctime_t atime;
    cmd->getCTime(atime);

    buf->st_atime   = atime.tv_sec;
    
    buf->st_mtime   = atime.tv_sec;
    buf->st_ctime   = atime.tv_sec;
    
    buf->st_atim.tv_sec   = atime.tv_sec;
    buf->st_mtim.tv_sec   = atime.tv_sec;
    buf->st_ctim.tv_sec   = atime.tv_sec;
    buf->st_atim.tv_nsec   = atime.tv_nsec;
    buf->st_mtim.tv_nsec   = atime.tv_nsec;
    buf->st_ctim.tv_nsec   = atime.tv_nsec;
    
    // if we have a cached modification time, return that one
    // -->
    gOFS->MgmDirectoryModificationTimeMutex.Lock();
    if (gOFS->MgmDirectoryModificationTime.count(buf->st_ino)) {
      buf->st_mtime = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_sec;
      buf->st_mtim.tv_sec = buf->st_mtime;
      buf->st_mtim.tv_nsec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_nsec;
    }
    gOFS->MgmDirectoryModificationTimeMutex.UnLock();  
    // --|
    return SFS_OK;
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());    
    return Emsg(epname, error, errno, "stat", cPath.GetPath());
  }
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::lstat(const char              *path,        // In
                     struct stat             *buf,         // Out
                     XrdOucErrInfo           &error,       // Out
                     const XrdSecEntity      *client,      // In
                     const char              *info)        // In
/*
  Function: Get info on 'path'.

  Input:    path        - Is the fully qualified name of the file to be tested.
  buf         - The stat structiure to hold the results
  error       - Error information object holding the details.
  client      - Authentication credentials, if any.
  info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  // no symbolic links yet
  return stat(path,buf,error,client,info);
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::truncate(const char*, 
                        XrdSfsFileOffset, 
                        XrdOucErrInfo& error, 
                        const XrdSecEntity* client,  
                        const char* path)
{
  static const char *epname = "truncate";
  const char *tident = error.getErrUser(); 
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::IdMap(client,0,tident, vid);
  
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Truncate",vid.uid,vid.gid,1);  
  return Emsg(epname, error, EOPNOTSUPP, "truncate", path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::readlink(const char             *inpath,      // In
                        XrdOucString        &linkpath,    // Out
                        XrdOucErrInfo       &error,       // Out
                        const XrdSecEntity  *client,       // In
                        const char          *info)        // In
{
  static const char *epname = "readlink";
  const char *tident = error.getErrUser(); 

  XrdOucEnv rl_Env(info);

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP; 

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&rl_Env,AOP_Stat,"readlink",path,error);
  
  eos::common::Mapping::IdMap(client,info,tident,vid);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("ReadLink",vid.uid,vid.gid,1);  

  return Emsg(epname, error, EOPNOTSUPP, "readlink", path); 
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::symlink(const char            *inpath,        // In
                       const char           *linkpath,    // In
                       XrdOucErrInfo        &error,       // Out
                       const XrdSecEntity   *client,      // In
                       const char           *info)        // In
{
  static const char *epname = "symlink";
  const char *tident = error.getErrUser(); 

  XrdOucEnv sl_Env(info);

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP; 

  XTRACE(fsctl, path,"");

  XrdOucString source, destination;

  AUTHORIZE(client,&sl_Env,AOP_Create,"symlink",linkpath,error);
  
  // we only need to map absolut links
  source = path;

  eos::common::Mapping::IdMap(client,info,tident,vid);

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Symlink",vid.uid,vid.gid,1);  

  return Emsg(epname, error, EOPNOTSUPP, "symlink", path); 
}

/*----------------------------------------------------------------------------*/
/* This function operations ONLY on directories !!!                           */
/*----------------------------------------------------------------------------*/
int XrdMgmOfs::access( const char            *inpath,        // In
                       int                   mode,        // In
                       XrdOucErrInfo        &error,       // Out
                       const XrdSecEntity   *client,      // In
                       const char           *info)        // In
{
  static const char *epname = "access";
  const char *tident = error.getErrUser(); 

  XrdOucEnv access_Env(info);

  NAMESPACEMAP; 

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Stat,"access",path,error);

  eos::common::Mapping::IdMap(client,info,tident,vid);  

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Access",vid.uid,vid.gid,1);  


  eos::common::Path cPath(path);

  eos::ContainerMD* dh;
  eos::FileMD* fh;
  bool permok = false;
  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);      
  try {
    fh = gOFS->eosView->getFile(cPath.GetPath());
  } catch( eos::MDException &e ) {
    fh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
  }
  
  try {
    eos::ContainerMD::XAttrMap attrmap;
    if (fh) {
      // if this is a file we check the access on the parent directory
      dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    } else {
      // if this is not a file we assume it is a directory
      dh = gOFS->eosView->getContainer(cPath.GetPath());
    }
    permok = dh->access(vid.uid,vid.gid, mode);

    if (!permok) {
      // get attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for ( it = dh->attributesBegin(); it != dh->attributesEnd(); ++it) {
        attrmap[it->first] = it->second;
      }
      // ACL and permission check
      Acl acl(attrmap.count("sys.acl")?attrmap["sys.acl"]:std::string(""),attrmap.count("user.acl")?attrmap["user.acl"]:std::string(""),vid);
       
      eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d", acl.HasAcl(),acl.CanRead(),acl.CanWrite(),acl.CanWriteOnce(), acl.CanBrowse(),acl.HasEgroup());

      // browse permission by ACL
      if (acl.HasAcl()) {
	if ( (mode & W_OK) && acl.CanWrite()) {
	  permok = true;
	}

	if ( (mode & R_OK) && acl.CanRead()) {
	  permok = true;
	}

        if ( (mode & W_OK) && acl.CanBrowse()) {
          permok = true;
        }
      }
    }
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
  }

  // check permissions

  if (!dh) {
    eos_debug("msg=\"access\" errno=ENOENT");
    return Emsg(epname, error, ENOENT, "access", path); 
  }

  if (dh) {
    eos_debug("msg=\"access\" uid=%d gid=%d retc=%d mode=%o", vid.uid,vid.gid,(dh->access(vid.uid,vid.gid, R_OK|X_OK)), dh->getMode());
  }
  
  if ( dh && (mode & F_OK) ) {
    return SFS_OK;
  }
    
  if ( dh && permok ) {
    return SFS_OK;
  }

  if ( dh && (!permok) ) {
    return Emsg(epname, error, EACCES, "access", path); 
  }
  
  return Emsg(epname, error, EOPNOTSUPP, "access", path); 
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::utimes(  const char            *inpath,        // In
                        struct timespec      *tvp,         // In
                        XrdOucErrInfo       &error,        // Out
                        const XrdSecEntity  *client,       // In
                        const char          *info)         // In
{
  static const char *epname = "utimes";
  const char *tident = error.getErrUser(); 

  XrdOucEnv utimes_Env(info);

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&utimes_Env,AOP_Update,"set utimes",path,error);

  eos::common::Mapping::IdMap(client,info,tident,vid);  

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _utimes(path,tvp, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_utimes(  const char          *path,        // In
                         struct timespec      *tvp,         // In
                         XrdOucErrInfo       &error,       // Out
                         eos::common::Mapping::VirtualIdentity &vid, // In
                         const char          *info)        // In
{
  bool done=false;
  eos::ContainerMD* cmd=0;
 
  EXEC_TIMING_BEGIN("Utimes");    

  gOFS->MgmStats.Add("Utimes",vid.uid,vid.gid,1);  

  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try {
    cmd = gOFS->eosView->getContainer(path);
    // we use creation time as modification time ... hmmm ...
    cmd->setCTime(tvp[1]);
    eosView->updateContainerStore(cmd);
    done = true;
  } catch( eos::MDException &e ) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());    
  }

  if (!cmd) {
    eos::FileMD* fmd = 0;
    // try as a file
    try {
      fmd = gOFS->eosView->getFile(path);
      fmd->setMTime(tvp[1]);
      eosView->updateFileStore(fmd);
      done = true;
    } catch( eos::MDException &e ) {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());      
    }
  }

  EXEC_TIMING_END("Utimes");      

  if (!done) {
    return Emsg("utimes", error, errno, "set utimes", path);
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_find(const char       *path,             // In 
                     XrdOucErrInfo    &out_error,        // Out
                     XrdOucString     &stdErr,
                     eos::common::Mapping::VirtualIdentity &vid, // In
		     std::map<std::string, std::set<std::string> > &found,		     
                     const char* key, const char* val, bool nofiles
                     )
{
  std::vector< std::vector<std::string> > found_dirs;

  // try if that is directory
  eos::ContainerMD* cmd = 0;
  std::string Path = path;
  XrdOucString sPath = path;
  errno = 0;

  EXEC_TIMING_BEGIN("Find");      

  gOFS->MgmStats.Add("Find",vid.uid,vid.gid,1);  

  if (!(sPath.endswith('/')))
    Path += "/";
  
  found_dirs.resize(1);
  found_dirs[0].resize(1);
  found_dirs[0][0] = Path.c_str();
  int deepness = 0;

  // users cannot return more than 250k files and 50k dirs with one find

  static unsigned long long finddiruserlimit  = 50000;
  static unsigned long long findfileuserlimit = 100000;

  unsigned long long filesfound=0;
  unsigned long long dirsfound=0;

  bool limitresult = false;
  bool limited = false;

  if ( (vid.uid != 0) && (! eos::common::Mapping::HasUid(3, vid.uid_list)) && (! eos::common::Mapping::HasGid(4, vid.gid_list)) && (! vid.sudoer) ) {
    limitresult = true;
  }

  do {
    bool permok = false;

    found_dirs.resize(deepness+2);
    // loop over all directories in that deepness
    for (unsigned int i=0; i< found_dirs[deepness].size(); i++) {
      Path = found_dirs[deepness][i].c_str();
      eos_static_debug("Listing files in directory %s", Path.c_str());
      //-------------------------------------------
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      try {
        cmd = gOFS->eosView->getContainer(Path.c_str());
        permok = cmd->access(vid.uid,vid.gid, R_OK|X_OK);
      } catch( eos::MDException &e ) {
        errno = e.getErrno();
        cmd = 0;
	eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());       
      }
      
      if (cmd) {
        if (!permok) {
          stdErr += "error: no permissions to read directory "; stdErr += Path.c_str(); stdErr += "\n";         
          continue;
        }

        // add all children into the 2D vectors
        eos::ContainerMD::ContainerMap::iterator dit;
        for ( dit = cmd->containersBegin(); dit != cmd->containersEnd(); ++dit) {
          std::string fpath = Path.c_str(); fpath += dit->second->getName(); fpath+="/";
          // check if we select by tag
          if (key) {
            std::string sval = val;
            XrdOucString attr="";
            if(!gOFS->_attr_get(fpath.c_str(), out_error, vid, (const char*) 0, key, attr, true)) {
              if (attr == val) {
                found_dirs[deepness+1].push_back(fpath.c_str());
		found[fpath].size();
              }
            }
          } else {
	    if (limitresult) {
	      // apply the user limits for non root/admin/sudoers
	      if (dirsfound >= finddiruserlimit) {
		stdErr += "warning: find results are limited for users to ndirs="; stdErr += (int)finddiruserlimit; 
		stdErr += " -  result is truncated!\n";
		limited = true;
		break;
	      }
	    }
            found_dirs[deepness+1].push_back(fpath.c_str());
	    found[fpath].size();
            dirsfound++;
          }
        }

        if (!nofiles) {
          eos::ContainerMD::FileMap::iterator fit;
          for ( fit = cmd->filesBegin(); fit != cmd->filesEnd(); ++fit) {
	    if (limitresult) {
	      // apply the user limits for non root/admin/sudoers
	      if (filesfound >= findfileuserlimit) {
		stdErr += "warning: find results are limited for users to nfiles="; stdErr += (int)findfileuserlimit; 
		stdErr += " -  result is truncated!\n";
		limited = true;
		break;
	      }
	    }
	    found[Path].insert(fit->second->getName());
            filesfound++;
          }
        }
      }
      if (limited) {
	break;
      }
    }

    deepness++;
    if (limited) {
      break;
    }
  } while (found_dirs[deepness].size());
  //-------------------------------------------  
  if (!nofiles) {
    // if the result is empty, maybe this was a find by file
    if (!found.size()) {
      XrdSfsFileExistence file_exists;
      if (((_exists(Path.c_str(),file_exists,out_error,vid,0))==SFS_OK) && (file_exists==XrdSfsFileExistIsFile)) {
	eos::common::Path cPath(Path.c_str());
	found[cPath.GetParentPath()].insert(cPath.GetName());
      }
    }
  }
  //-------------------------------------------  
  // include also the directory which was specified in the query if it is accessible and a directory since it can evt. be missing if it is empty
  XrdSfsFileExistence dir_exists;
  if (((_exists(Path.c_str(),dir_exists,out_error,vid,0))==SFS_OK) && (dir_exists==XrdSfsFileExistIsDirectory)) {
    eos::common::Path cPath(Path.c_str());
    found[Path.c_str()].size();
  }

  EXEC_TIMING_END("Find");      
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::Emsg(const char    *pfx,    // Message prefix value
                    XrdOucErrInfo &einfo,  // Place to put text & error code
                    int            ecode,  // The error code
                    const char    *op,     // Operation being performed
                    const char    *target) // The target (e.g., fname)
{
  char *etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  //
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
    {sprintf(unkbuff, "reason unknown (%d)", ecode); etext = unkbuff;}

  // Format the error message
  //
  
  
  snprintf(buffer,sizeof(buffer),"Unable to %s %s; %s", op, target, etext);

  if ( (ecode == EIDRM) || (ecode == ENODATA) ) {
    eos_debug("Unable to %s %s; %s", op, target, etext);
  } else {
    if (( !strcmp(op,"stat"))) {
      eos_debug("Unable to %s %s; %s", op, target, etext);
    } else {
      eos_err("Unable to %s %s; %s", op, target, etext);
    }
  }
   
  // Print it out if debugging is enabled
  //
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // Place the error message in the error object and return
  //
  einfo.setErrInfo(ecode, buffer);

  return SFS_ERROR;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsDirectory::Emsg(const char    *pfx,    // Message prefix value
                             XrdOucErrInfo &einfo,  // Place to put text & error code
                             int            ecode,  // The error code
                             const char    *op,     // Operation being performed
                             const char    *target) // The target (e.g., fname)
{
  char *etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  //
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
    {sprintf(unkbuff, "reason unknown (%d)", ecode); etext = unkbuff;}

  // Format the error message
  //
  snprintf(buffer,sizeof(buffer),"Unable to %s %s; %s", op, target, etext);

  eos_err("Unable to %s %s; %s", op, target, etext);

  // Print it out if debugging is enabled
  //
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // Place the error message in the error object and return
  //
  einfo.setErrInfo(ecode, buffer);

  return SFS_ERROR;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::Emsg(const char    *pfx,    // Message prefix value
                        XrdOucErrInfo &einfo,  // Place to put text & error code
                        int            ecode,  // The error code
                        const char    *op,     // Operation being performed
                        const char    *target) // The target (e.g., fname)
{
  char *etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  //
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
    {sprintf(unkbuff, "reason unknown (%d)", ecode); etext = unkbuff;}

  // Format the error message
  //
  snprintf(buffer,sizeof(buffer),"Unable to %s %s; %s", op, target, etext);

  eos_err("Unable to %s %s; %s", op, target, etext);

  // Print it out if debugging is enabled
  //
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // Place the error message in the error object and return
  //
  einfo.setErrInfo(ecode, buffer);

  return SFS_ERROR;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::Stall(XrdOucErrInfo   &error, // Error text & code
                     int              stime, // Seconds to stall
                     const char      *msg)   // Message to give
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  
  EPNAME("Stall");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Stall " <<stime <<": " << smessage.c_str());

  // Place the error message in the error object and return
  //
  error.setErrInfo(0, smessage.c_str());
  
  // All done
  //
  return stime;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::Redirect(XrdOucErrInfo   &error, // Error text & code
                        const char* host,
                        int &port)
{
  EPNAME("Redirect");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Redirect " <<host <<":" << port);

  // Place the error message in the error object and return
  //
  error.setErrInfo(port,host);
  
  // All done
  //
  return SFS_REDIRECT;
}


int       
XrdMgmOfs::fsctl(const int               cmd,
                 const char             *args,
                 XrdOucErrInfo          &error,
                 const XrdSecEntity *client)
{
  const char *tident = error.getErrUser();

  eos::common::LogId ThreadLogId;
  ThreadLogId.SetSingleShotLogId(tident);

  eos_thread_info("cmd=%d args=%s", cmd,args);

  int opcode = cmd & SFS_FSCTL_CMD;
  if ((opcode == SFS_FSCTL_LOCATE)) {

    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';//(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp,"[::%s] ",(char*)gOFS->ManagerId.c_str());
    error.setErrInfo(strlen(locResp)+3, (const char **)Resp, 2);
    return SFS_DATA;
  }

  if ((opcode == SFS_FSCTL_STATLS)) {
    int blen=0;
    char* buff = error.getMsgBuff(blen);
    XrdOucString space = "default";
    
    eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
    
    unsigned long long freebytes = 0;
    unsigned long long maxbytes  = 0;
    
    // take the sum's from all file systems in 'default'
    if (FsView::gFsView.mSpaceView.count("default")) {
      space = "default";
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
      freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes");
      maxbytes  = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity");
    } 

    static const char *Resp="oss.cgroup=%s&oss.space=%lld&oss.free=%lld"
      "&oss.maxf=%lld&oss.used=%lld&oss.quota=%lld";

    blen = snprintf(buff,blen,Resp,space.c_str(),maxbytes, freebytes,64 * 1024*1024*1024LL  /* fake 64BG */, 
		    maxbytes-freebytes, maxbytes);
    
    error.setErrCode(blen+1);
    return SFS_DATA;
  }


  return Emsg("fsctl", error, EOPNOTSUPP, "fsctl", args);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::FSctl(const int               cmd,
                 XrdSfsFSctl            &args,
                 XrdOucErrInfo          &error,
                 const XrdSecEntity     *client) 
{  
  char ipath[16384];
  char iopaque[16384];
  
  static const char *epname = "FSctl";
  const char *tident = error.getErrUser();

  eos::common::Mapping::VirtualIdentity vid;

  eos::common::Mapping::IdMap(client,"",tident,vid);  
  
  eos::common::LogId ThreadLogId;
  ThreadLogId.SetSingleShotLogId(tident);

  if (args.Arg1Len) {
    if (args.Arg1Len < 16384) {
      strncpy(ipath,args.Arg1,args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      return gOFS->Emsg(epname, error, EINVAL, "convert path argument - string too long", "");
    }
  } else {
    ipath[0] = 0;
  }
  
  if (args.Arg2Len) {
    if (args.Arg2Len < 16384) {
      strncpy(iopaque,args.Arg2,args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    } else {
      return gOFS->Emsg(epname, error, EINVAL, "convert opaque argument - string too long", "");
    }
  } else {
    iopaque[0] = 0;
  }

  const char* inpath = ipath;

  NAMESPACEMAP;
  
  // from here on we can deal with XrdOucString which is more 'comfortable'
  XrdOucString spath    = path;
  XrdOucString opaque  = iopaque;
  XrdOucString result  = "";
  XrdOucEnv env(opaque.c_str());

  eos_thread_debug("path=%s opaque=%s", spath.c_str(), opaque.c_str());

  if ((cmd == SFS_FSCTL_LOCATE)) {

    ACCESSMODE_R;
    MAYSTALL;
    MAYREDIRECT;

    // check if this file exists
    XrdSfsFileExistence file_exists;
    if ((_exists(spath.c_str(),file_exists,error,client,0)) || (file_exists!=XrdSfsFileExistIsFile)) {
      return SFS_ERROR;
    }
   
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';//(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp,"[::%s] ",(char*)gOFS->ManagerId.c_str());
    error.setErrInfo(strlen(locResp)+3, (const char **)Resp, 2);
    ZTRACE(fsctl,"located at headnode: " << locResp);
    return SFS_DATA;
  }

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return Emsg("fsctl", error, EOPNOTSUPP, "fsctl", inpath);  
  }

  const char* scmd;

  if ((scmd = env.Get("mgm.pcmd"))) {
    XrdOucString execmd = scmd;

    if (execmd == "commit") {
      
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Commit");      

      char* asize  = env.Get("mgm.size");
      char* spath  = env.Get("mgm.path");
      char* afid   = env.Get("mgm.fid");
      char* afsid  = env.Get("mgm.add.fsid");
      char* amtime =     env.Get("mgm.mtime");
      char* amtimensec = env.Get("mgm.mtime_ns"); 
      char* alogid     = env.Get("mgm.logid");

      if (alogid) {
	ThreadLogId.SetLogId(alogid, tident);
      }

      XrdOucString averifychecksum = env.Get("mgm.verify.checksum");
      XrdOucString acommitchecksum = env.Get("mgm.commit.checksum");
      XrdOucString averifysize     = env.Get("mgm.verify.size");
      XrdOucString acommitsize     = env.Get("mgm.commit.size");
      XrdOucString adropfsid       = env.Get("mgm.drop.fsid");
      XrdOucString areplication    = env.Get("mgm.replication");
      
      bool verifychecksum = (averifychecksum=="1");
      bool commitchecksum = (acommitchecksum=="1");
      bool verifysize     = (averifysize=="1");
      bool commitsize     = (acommitsize=="1");
      bool replication    = (areplication=="1");

      char* checksum = env.Get("mgm.checksum");
      char  binchecksum[SHA_DIGEST_LENGTH];
      memset(binchecksum, 0, sizeof(binchecksum));
      unsigned long dropfsid  = 0;
      if ( adropfsid.length() ) {
        dropfsid = strtoul(adropfsid.c_str(),0,10);
      }

      if (checksum) {
        for (unsigned int i=0; i< strlen(checksum); i+=2) {
          // hex2binary conversion
          char hex[3];
          hex[0] = checksum[i];hex[1] = checksum[i+1];hex[2] = 0;
          binchecksum[i/2] = strtol(hex,0,16);
        }
      }
      if (asize && afid && spath && afsid && amtime && amtimensec) {
        unsigned long long size = strtoull(asize,0,10);
        unsigned long long fid  = strtoull(afid,0,16);
        unsigned long fsid      = strtoul (afsid,0,10);
        unsigned long mtime     = strtoul (amtime,0,10);
        unsigned long mtimens   = strtoul (amtimensec,0,10);

        
        eos::Buffer checksumbuffer;
        checksumbuffer.putData(binchecksum, SHA_DIGEST_LENGTH);

        if (checksum) {
          eos_thread_info("subcmd=commit path=%s size=%s fid=%s fsid=%s dropfsid=%llu checksum=%s mtime=%s mtime.nsec=%s", spath, asize, afid, afsid, dropfsid, checksum, amtime, amtimensec);
        } else {
          eos_thread_info("subcmd=commit path=%s size=%s fid=%s fsid=%s dropfsid=%llu mtime=%s mtime.nsec=%s", spath, asize, afid, afsid, dropfsid, amtime, amtimensec);
        }
        
        // get the file meta data if exists
        eos::FileMD *fmd = 0;
        eos::ContainerMD::id_t cid=0;

	// -------------------------------------------
	// keep the lock order View=>Quota=>Namespace
        // -------------------------------------------
        eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
        // -------------------------------------------
	eos::common::RWMutexWriteLock nslock(gOFS->eosViewRWMutex);      
        try {
          fmd = gOFS->eosFileService->getFileMD(fid);
        } catch( eos::MDException &e ) {
          errno = e.getErrno();
	  eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());          
        }

        if (!fmd) {          
          // uups, no such file anymore
	  if (errno == ENOENT) {
            return Emsg(epname,error, ENOENT, "commit filesize change - file is already removed [EIDRM]","");
	  } else {
	    return Emsg(epname,error,errno,"commit filesize change",spath);
	  }
        } else {
	  unsigned long lid = fmd->getLayoutId();

          // check if fsid and fid are ok 
          if (fmd->getId() != fid ) {            
            eos_thread_notice("commit for fid=%lu but fid=%lu", fmd->getId(), fid);
            gOFS->MgmStats.Add("CommitFailedFid",0,0,1);
            return Emsg(epname,error, EINVAL,"commit filesize change - file id is wrong [EINVAL]", spath);
          }
          
          // check if this file is already unlinked from the visible namespace
          if (!(cid=fmd->getContainerId())) {          
                
            eos_thread_warning("commit for fid=%lu but file is disconnected from any container", fmd->getId());
            gOFS->MgmStats.Add("CommitFailedUnlinked",0,0,1);  
            return Emsg(epname,error, EIDRM, "commit filesize change - file is already removed [EIDRM]","");
          } else {
	    // store the in-memory modification time
	    gOFS->MgmDirectoryModificationTimeMutex.Lock();
	    // we get the current time, but we don't update the creation time
	    struct timespec ts;
	    eos::common::Timing::GetTimeSpec(ts);
	    gOFS->MgmDirectoryModificationTime[cid].tv_sec = ts.tv_sec;
	    gOFS->MgmDirectoryModificationTime[cid].tv_nsec = ts.tv_nsec;
	    gOFS->MgmDirectoryModificationTimeMutex.UnLock();
	    //-------------------------------------------    
	  }

          // check if this commit comes from a transfer and if the size/checksum is ok
          if (replication) {
            if (fmd->getSize() != size) {
              //-------------------------------------------

              eos_thread_err("replication for fid=%lu resulted in a different file size on fsid=%llu - rejecting replica", fmd->getId(), fsid);
              gOFS->MgmStats.Add("ReplicaFailedSize",0,0,1);
              return Emsg(epname, error, EBADE, "commit replica - file size is wrong [EBADE]","");
            }

	    if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) {
	      // we check the checksum only for replica layouts
	      bool cxError=false;
	      for (int i=0 ; i< SHA_DIGEST_LENGTH; i++) {
		if (fmd->getChecksum().getDataPtr()[i] != checksumbuffer.getDataPtr()[i]) {
		  cxError=true;
		}
	      }
	      if (cxError) {
		//-------------------------------------------
		eos_thread_err("replication for fid=%lu resulted in a different checksum on fsid=%llu - rejecting replica", fmd->getId(), fsid);
		gOFS->MgmStats.Add("ReplicaFailedChecksum",0,0,1);
		return Emsg(epname, error, EBADR, "commit replica - file checksum is wrong [EBADR]","");
	      }
	    }
          }

          if (verifysize) {
            // check if we saw a file size change or checksum change
            if (fmd->getSize() != size) {
              eos_thread_err("commit for fid=%lu gave a file size change after verification on fsid=%llu", fmd->getId(), fsid);
            }
          }
          
          if (checksum) {
            if (verifychecksum) {
              bool cxError=false;
              for (int i=0 ; i< SHA_DIGEST_LENGTH; i++) {
                if (fmd->getChecksum().getDataPtr()[i] != checksumbuffer.getDataPtr()[i]) {
                  cxError=true;
                }
              }
              if (cxError) {
                eos_thread_err("commit for fid=%lu gave a different checksum after verification on fsid=%llu", fmd->getId(), fsid);
              }
            }
          }

          {
            SpaceQuota* space = Quota::GetResponsibleSpaceQuota(spath);
            eos::QuotaNode* quotanode = 0;
            if (space) {
              quotanode = space->GetQuotaNode();
              // free previous quota
              if (quotanode)
                quotanode->removeFile(fmd);
            }           
            fmd->addLocation(fsid);
            // if fsid is in the deletion list, we try to remove it if there is something in the deletion list
            if (fmd->getNumUnlinkedLocation()) {
              fmd->removeLocation(fsid);
            }
            
	    if (dropfsid) {
	      eos_thread_debug("commit: dropping replica on fs %lu", dropfsid);
	      fmd->unlinkLocation((unsigned short)dropfsid);
	    }
          
            if (commitsize) {
              fmd->setSize(size);
            }

            if (quotanode) {
              quotanode->addFile(fmd);
            }
          }
          
          
          if (commitchecksum)
            fmd->setChecksum(checksumbuffer);

          //      fmd->setMTimeNow();
          eos::FileMD::ctime_t mt;
          mt.tv_sec  = mtime;
          mt.tv_nsec = mtimens;
          fmd->setMTime(mt);     
          
          eos_thread_debug("commit: setting size to %llu", fmd->getSize());
          //-------------------------------------------
          try {     
            gOFS->eosView->updateFileStore(fmd);
          }  catch( eos::MDException &e ) {
            errno = e.getErrno();
            std::string errmsg = e.getMessage().str();
	    eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());           
            gOFS->MgmStats.Add("CommitFailedNamespace",0,0,1);  
            return Emsg(epname, error, errno, "commit filesize change", errmsg.c_str());      
          }      
        }
      } else {
        int envlen=0;
        eos_thread_err("commit message does not contain all meta information: %s", env.Env(envlen));
        gOFS->MgmStats.Add("CommitFailedParameters",0,0,1);  
        if (spath) {
          return  Emsg(epname,error,EINVAL,"commit filesize change - size,fid,fsid,mtime not complete",spath);
        } else {
          return  Emsg(epname,error,EINVAL,"commit filesize change - size,fid,fsid,mtime,path not complete","unknown");
        }
      }
      gOFS->MgmStats.Add("Commit",0,0,1);  
      const char* ok = "OK";
      error.setErrInfo(strlen(ok)+1,ok);
      EXEC_TIMING_END("Commit");      
      return SFS_DATA;
    }
    
    if (execmd == "drop") {

      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Drop");      
      // drops a replica
      int envlen;
      eos_thread_debug("drop request for %s",env.Env(envlen));
      char* afid   = env.Get("mgm.fid");      
      char* afsid  = env.Get("mgm.fsid");
      if (afid && afsid) {
        unsigned long fsid      = strtoul (afsid,0,10);
        
        //-------------------------------------------
	eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);     
        eos::FileMD* fmd = 0;
        eos::ContainerMD* container = 0;
        eos::QuotaNode* quotanode = 0;

        try { 
          fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
        } catch (...) {
          eos_thread_warning("no meta record exists anymore for fid=%s", afid);
          fmd = 0;
        }

        if (fmd) {
          try {
            container = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
          } catch ( eos::MDException &e ) {
            container = 0;
          }
        }

        if (container) {
          try {
            quotanode = gOFS->eosView->getQuotaNode(container);
	    if (quotanode) {
	      quotanode->removeFile(fmd);
	    }
          } catch ( eos::MDException &e ) {
            quotanode = 0;
          }
	}

        if (fmd) {
          try {
            eos_thread_debug("removing location %u of fid=%s", fsid,afid);
            fmd->removeLocation(fsid);
            gOFS->eosView->updateFileStore(fmd);
            
            // after update we have to get the new address - who knows ...
            fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
            if (quotanode) 
              quotanode->addFile(fmd);

            // finally delete the record if all replicas are dropped
            if ((!fmd->getNumUnlinkedLocation()) && (!fmd->getNumLocation())) {
              gOFS->eosView->removeFile( fmd );
              if (quotanode) {
                // if we were still attached to a container, we can now detach and count the file as removed
                quotanode->removeFile(fmd);
              }
            }
          } catch (...) {
            eos_thread_warning("no meta record exists anymore for fid=%s", afid);
          };
        }

        gOFS->MgmStats.Add("Drop",vid.uid,vid.gid,1);  
        
        const char* ok = "OK";
        error.setErrInfo(strlen(ok)+1,ok);
        EXEC_TIMING_END("Drop");      
        return SFS_DATA;
      }
    }

    if (execmd == "stat") {

      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      struct stat buf;

      int retc = lstat(spath.c_str(),
		      &buf,  
		      error, 
		      client,
		      0);

      if (retc == SFS_OK) {
        char statinfo[16384];
        // convert into a char stream
        sprintf(statinfo,"stat: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu\n",
                (unsigned long long) buf.st_dev,
                (unsigned long long) buf.st_ino,
                (unsigned long long) buf.st_mode,
                (unsigned long long) buf.st_nlink,
                (unsigned long long) buf.st_uid,
                (unsigned long long) buf.st_gid,
                (unsigned long long) buf.st_rdev,
                (unsigned long long) buf.st_size,
                (unsigned long long) buf.st_blksize,
                (unsigned long long) buf.st_blocks,
                (unsigned long long) buf.st_atime,
                (unsigned long long) buf.st_mtime,
                (unsigned long long) buf.st_ctime,
                (unsigned long long) buf.st_atim.tv_nsec,
                (unsigned long long) buf.st_mtim.tv_nsec,
                (unsigned long long) buf.st_ctim.tv_nsec);

        error.setErrInfo(strlen(statinfo)+1,statinfo);
        return SFS_DATA;
      } else {
        XrdOucString response="stat: retc=";
        response += errno;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      } 
    }

    if (execmd == "chmod") {

      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      char* smode;
      if ((smode = env.Get("mode"))) {
        struct stat buf;

        // check if it is a file or directory ....
        int retc = lstat(spath.c_str(),
                         &buf,  
                         error, 
                         client,
                         0);

        // if it is a file ....
        if (!retc && S_ISREG(buf.st_mode)) {
          // since we don't have permissions on files, we just acknoledge as ok
          XrdOucString response="chmod: retc=0";
          error.setErrInfo(response.length()+1,response.c_str());
          return SFS_DATA;
        }


        XrdSfsMode newmode = atoi(smode);
        retc =  _chmod(spath.c_str(),
                       newmode,
                       error,
                       vid);

        XrdOucString response="chmod: retc=";
        response += retc;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      } else {
        XrdOucString response="chmod: retc=";
        response += EINVAL;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "chown") {

      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      char* suid;
      char* sgid;
      if ((suid = env.Get("uid")) && (sgid = env.Get("gid"))) {
        uid_t uid = atoi(suid);
        gid_t gid = atoi(sgid);

        int retc =  _chown(spath.c_str(),
                           uid,
                           gid,
                           error,
                           vid);
        XrdOucString response="chmod: retc=";
        response += retc;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      } else {
        XrdOucString response="chmod: retc=";
        response += EINVAL;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "symlink") {

      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      /*
        char* destination;
        char* source;
        if ((destination = env.Get("linkdest"))) {
        if ((source = env.Get("linksource"))) {
        int retc = symlink(source,destination,error,client,0);
        XrdOucString response="sysmlink: retc=";
        response += retc;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
        }
        }
        XrdOucString response="symlink: retc=";
        response += EINVAL;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      */
    }

    if (execmd == "readlink") {

      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      /*
        XrdOucString linkpath="";
        int retc=readlink(path.c_str(),linkpath,error,client,0);
        XrdOucString response="readlink: retc=";
        response += retc;
        response += " link=";
        response += linkpath;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      */
    }

    if (execmd == "access") {

      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      char* smode;
        if ((smode = env.Get("mode"))) {
        int newmode = atoi(smode);
        int retc =0;
	if (access(spath.c_str(),newmode, error,client,0)) {
	  retc = error.getErrInfo();
	}
        XrdOucString response="access: retc=";
        response += retc;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
        } else {
        XrdOucString response="access: retc=";
        response += EINVAL;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
        }
    }

    if (execmd == "utimes") {

      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;
      
      char* tv1_sec;
      char* tv1_nsec;
      char* tv2_sec;
      char* tv2_nsec;

      tv1_sec  = env.Get("tv1_sec");
      tv1_nsec = env.Get("tv1_nsec");
      tv2_sec  = env.Get("tv2_sec");
      tv2_nsec = env.Get("tv2_nsec");

      struct timespec tvp[2];
      if (tv1_sec && tv1_nsec && tv2_sec && tv2_nsec) {
        tvp[0].tv_sec  = strtol(tv1_sec,0,10);
        tvp[0].tv_nsec = strtol(tv1_nsec,0,10);
        tvp[1].tv_sec  = strtol(tv2_sec,0,10);
        tvp[1].tv_nsec = strtol(tv2_nsec,0,10);

        int retc = utimes(spath.c_str(), 
                          tvp,
                          error, 
                          client,
                          0);

        XrdOucString response="utimes: retc=";
        response += retc;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      } else {
        XrdOucString response="utimes: retc=";
        response += EINVAL;
        error.setErrInfo(response.length()+1,response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "checksum") {

      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      // get the checksum 
      XrdOucString checksum="";
      eos::FileMD* fmd=0;
      int retc=0;
      //-------------------------------------------
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);      
      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
        for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
          char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
          checksum += hb;
        }
      } catch ( eos::MDException &e ) {
        errno = e.getErrno();
	eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());       
      }

      if (!fmd) {
        retc = errno;
      } else {
        retc = 0;
      }
         
      XrdOucString response="checksum: "; response += checksum;
      response += " retc="; response += retc;
      error.setErrInfo(response.length()+1, response.c_str());
      return SFS_DATA;
    }

    if (execmd == "statvfs") {

      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Statvfs",vid.uid,vid.gid,1);
      XrdOucString space = env.Get("path");
      static XrdSysMutex statvfsmutex;
      static unsigned long long freebytes = 0;
      static unsigned long long freefiles = 0;
      static unsigned long long maxbytes  = 0;
      static unsigned long long maxfiles  = 0;
     
      static time_t laststat = 0;

      XrdOucString response ="";
      
      if (!space.length()) {
        response = "df: retc=";
        response += EINVAL;
      } else {
        statvfsmutex.Lock();

        // here we put some cache to avoid too heavy space recomputations
        if (  (time(NULL) - laststat) > ( 10 + (int)rand()/RAND_MAX) ) {
	  SpaceQuota* spacequota = 0;
	  {
	    eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
	    spacequota = Quota::GetResponsibleSpaceQuota(space.c_str());
	  }
          
          if (!spacequota) {
            // take the sum's from all file systems in 'default'
            if (FsView::gFsView.mSpaceView.count("default")) {
              eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
              freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes");
              freefiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.ffree");
              
              maxbytes  = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity");
              maxfiles  = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.files");
            } 
          } else {
            freebytes = spacequota->GetPhysicalFreeBytes();
            freefiles = spacequota->GetPhysicalFreeFiles();
            maxbytes  = spacequota->GetPhysicalMaxBytes();
            maxfiles  = spacequota->GetPhysicalMaxFiles();
          }
          laststat = time(NULL);
        }
        statvfsmutex.UnLock();
        response = "statvfs: retc=0";
        char val[1025]; 
        snprintf(val,1024,"%llu", freebytes);
        response += " f_avail_bytes="; response += val;
        snprintf(val,1024,"%llu", freefiles);
        response += " f_avail_files="; response += val;
        snprintf(val,1024,"%llu", maxbytes);
        response += " f_max_bytes=";   response += val;
        snprintf(val,1024,"%llu", maxfiles);
        response += " f_max_files=";   response += val;
        error.setErrInfo(response.length()+1,response.c_str());
      }
      return SFS_DATA;
    }

    if (execmd == "xattr") {

      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      const char* sub_cmd;
      struct stat buf;

      // check if it is a file or directory ....
      int retc = lstat(spath.c_str(),
                       &buf,  
                       error, 
                       client,
                       0);

      if (!retc && S_ISDIR(buf.st_mode)) {            //extended attributes for directories
        if ((sub_cmd = env.Get("mgm.subcmd"))) {
          XrdOucString subcmd = sub_cmd;
          if (subcmd == "ls") {  //listxattr
            eos::ContainerMD::XAttrMap map;
            int rc = gOFS->attr_ls(spath.c_str(), error, client, (const char *) 0, map);
      
            XrdOucString response = "lsxattr: retc=";
            response += rc; response += " ";
            if (rc == SFS_OK) {
              for (std::map<std::string, std::string>::iterator iter = map.begin(); iter != map.end(); iter++){
                response += iter->first.c_str(); 
                response += "&"; 
              }
              response += "\0";
	      while(response.replace("user.","tmp.")){}
	      while(response.replace("tmp.","user.eos.")){}
	      while(response.replace("sys.","user.admin.")){}
            }
            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
          else if (subcmd == "get") {  //getxattr
            XrdOucString value;
            XrdOucString key = env.Get("mgm.xattrname");
	    key.replace("user.admin.","sys.");
	    key.replace("user.eos.","user.");
            int rc = gOFS->attr_get(spath.c_str(), error, client, (const char*) 0, key.c_str(), value);
                                  
            XrdOucString response = "getxattr: retc=";
            response += rc;
          
            if (rc == SFS_OK) {
              response += " value=";
              response += value;
            }

            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
          else if (subcmd == "set") {  //setxattr
            XrdOucString key = env.Get("mgm.xattrname");
            XrdOucString value = env.Get("mgm.xattrvalue");
	    key.replace("user.admin.","sys.");
	    key.replace("user.eos.","user.");
            int rc = gOFS->attr_set(spath.c_str(), error, client, (const char *) 0, key.c_str(), value.c_str());
      
            XrdOucString response = "setxattr: retc=";
            response += rc;

            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
          else if (subcmd == "rm") {  // rmxattr
            XrdOucString key = env.Get("mgm.xattrname");
	    key.replace("user.admin.","sys.");
	    key.replace("user.eos.","user.");
            int rc = gOFS->attr_rem(spath.c_str(), error, client, (const char *) 0, key.c_str());

            XrdOucString response = "rmxattr: retc=";
            response += rc;
          
            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
        }
      }
      else if (!retc && S_ISREG(buf.st_mode)) {  //extended attributes for files
        //-------------------------------------------
	eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);         
        eos::FileMD* fmd = 0;
        try {
          fmd = gOFS->eosView->getFile(spath.c_str());
        } catch( eos::MDException &e ) {
	  eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());         
        }
  
        if ((sub_cmd = env.Get("mgm.subcmd"))) {
          XrdOucString subcmd = sub_cmd;
          char* char_key = NULL;
          XrdOucString key;
          XrdOucString response;

          if (subcmd == "ls") {      //listxattr
            response = "lsxattr: retc=0 ";
            response += "user.eos.cid"; response += "&";
            response += "user.eos.fid"; response += "&";
            response += "user.eos.lid"; response += "&";
            response += "user.eos.XStype"; response += "&";
            response += "user.eos.XS"; response += "&";
            error.setErrInfo(response.length() + 1, response.c_str());
          }
          else if (subcmd == "get") { //getxattr
            char_key = env.Get("mgm.xattrname");
            key = char_key;
            response = "getxattr: retc=";
            
            if (key.find("eos.cid") != STR_NPOS) {
              XrdOucString sizestring;
              response += "0 "; response += "value=";
              response += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)fmd->getContainerId());
            }
            else if (key.find("eos.fid") != STR_NPOS) {
              char fid[32];
              response += "0 "; response += "value=";
              snprintf(fid,32,"%llu",(unsigned long long) fmd->getId());
              response += fid;
            }
            else if (key.find("eos.XStype") != STR_NPOS){
              response += "0 "; response += "value=";
              response += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
            }
            else if (key.find("eos.XS") != STR_NPOS){
              response += "0 "; response += "value=";
              char hb[3]; 
	      for (unsigned int i = 0; i < SHA_DIGEST_LENGTH; i++) {
                if ((i + 1) == SHA_DIGEST_LENGTH)
                  sprintf(hb,"%02x ", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
                else 
                  sprintf(hb,"%02x_", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
		response += hb;
	      }

            }
            else if (key.find("eos.lid") != STR_NPOS){
              response += "0 "; response += "value=";
              response += eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
            }
            else 
              response += "1 ";     

            error.setErrInfo(response.length() + 1, response.c_str());
          }
          else if (subcmd == "rm") {  //rmxattr
            response += "1 "; //error
          }
          else if (subcmd == "set") { //setxattr
            response += "1 "; //error
          }
                       
          return SFS_DATA;
        }         
        return SFS_DATA;
      }
    }

    if (execmd == "schedule2balance") {
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Scheduled2Balance");      
      gOFS->MgmStats.Add("Schedule2Balance",0,0,1);         

      XrdOucString sfsid       = env.Get("mgm.target.fsid");
      XrdOucString sfreebytes  = env.Get("mgm.target.freebytes");
      char* alogid             = env.Get("mgm.logid");
      char* simulate           = env.Get("mgm.simulate"); // used to test the routing

      // static map with iterator position for the next group scheduling and it's mutex
      static std::map<std::string, size_t> sGroupCycle;
      static XrdSysMutex sGroupCycleMutex;
      static XrdSysMutex sScheduledFidMutex;
      static std::map<eos::common::FileSystem::fsid_t, time_t> sScheduledFid;

      if (alogid) {
	ThreadLogId.SetLogId(alogid,tident);
      }
      
      if ((!sfsid.length()) || (!sfreebytes.length())) {
	gOFS->MgmStats.Add("SchedulingFailedBalance",0,0,1);         
	return Emsg(epname, error, EINVAL, "unable to schedule - missing parameters [EINVAL]");
      }

      eos::common::FileSystem::fsid_t target_fsid = atoi(sfsid.c_str());
      eos::common::FileSystem::fsid_t source_fsid = 0;
      eos::common::FileSystem::fs_snapshot target_snapshot;
      eos::common::FileSystem::fs_snapshot source_snapshot;
      eos::common::FileSystem* target_fs = 0;

      unsigned long long freebytes = (sfreebytes.c_str())?strtoull(sfreebytes.c_str(),0,10):0;

      eos_thread_info("cmd=schedule2balance fsid=%d freebytes=%llu logid=%s", target_fsid, freebytes, alogid?alogid:"");

      while(1)
      // lock the view and get the filesystem information for the target where be balance to
      {
	eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
	target_fs = FsView::gFsView.mIdView[target_fsid];
	if (!target_fs) {
	  eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
	  gOFS->MgmStats.Add("SchedulingFailedBalance",0,0,1);         
	  return Emsg(epname, error, EINVAL, "unable to schedule - filesystem ID is not known");
	}
	target_fs->SnapShotFileSystem(target_snapshot);
	FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

	size_t groupsize = FsView::gFsView.mGroupView.size();
	if (!group) {
	  eos_thread_err("group=%s is not in group view", target_snapshot.mGroup.c_str());
	  gOFS->MgmStats.Add("SchedulingFailedBalance",0,0,1);         
	  return Emsg(epname, error, EINVAL, "unable to schedule - group is not known [EINVAL]");
	}

	eos_thread_debug("group=%s", target_snapshot.mGroup.c_str());

	// select the next fs in the group to get a file to move
	size_t gposition=0;
	sGroupCycleMutex.Lock();
	if (sGroupCycle.count(target_snapshot.mGroup)) {
	  gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
	} else {
	  gposition = 0;
	  sGroupCycle[target_snapshot.mGroup]=0;
	}
	// shift the iterator for the next schedule call to the following filesystem in the group
	sGroupCycle[target_snapshot.mGroup]++;
	sGroupCycle[target_snapshot.mGroup]%= groupsize;
	sGroupCycleMutex.UnLock();

	eos_thread_debug("group=%s cycle=%lu", target_snapshot.mGroup.c_str(), gposition);
	// try to find a file, which is smaller than the free bytes and has no replica on the target filesystem
	// we start at a random position not to move data of the same period to a single disk

	group = FsView::gFsView.mGroupView[target_snapshot.mGroup];	
	FsGroup::const_iterator group_iterator;
	group_iterator = group->begin();
	std::advance(group_iterator, gposition);

	eos::common::FileSystem* source_fs = 0;
	for (size_t n=0; n< group->size(); n++) {
	  // skip over the target file system, that isn't usable
	  if (*group_iterator == target_fsid) {
	    source_fs = 0;
	    group_iterator++; if (group_iterator == group->end()) group_iterator = group->begin();
	    continue;
	  }
	  source_fs = FsView::gFsView.mIdView[*group_iterator];
	  if (!source_fs)
	    continue;
	  source_fs->SnapShotFileSystem(source_snapshot);
	  source_fsid = *group_iterator;
	  if ( (source_snapshot.mDiskFilled < source_snapshot.mNominalFilled) || // this is not a source since it is empty
	       (source_snapshot.mStatus       != eos::common::FileSystem::kBooted) ||   // this filesystem is not readable
	       (source_snapshot.mConfigStatus < eos::common::FileSystem::kRO) ||
	       (source_snapshot.mErrCode      != 0 ) ||
	       (source_fs->GetActiveStatus(source_snapshot) == eos::common::FileSystem::kOffline) ) {	    	    
	      source_fs = 0;
	      group_iterator++; if (group_iterator == group->end()) group_iterator = group->begin();
	      continue;
	  }
	  break;
	}

	if (!source_fs) {
	  eos_thread_debug("no source available");
	  gOFS->MgmStats.Add("SchedulingFailedBalance",0,0,1);         
	  error.setErrInfo(0,"");
	  return SFS_DATA;
	  //	  return Emsg(epname, error, EINVAL, "unable to schedule - no source available [ENODATA]");
	}
	source_fs->SnapShotFileSystem(source_snapshot);

	eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

	eos::FileSystemView::FileList source_filelist;
	eos::FileSystemView::FileList target_filelist;

	try {
	  source_filelist = gOFS->eosFsView->getFileList(source_fsid);
	} catch ( eos::MDException &e ) {
	  source_filelist.set_deleted_key( 0 );
	  source_filelist.set_empty_key(0xffffffffffffffff);
	}

	try {
	  target_filelist = gOFS->eosFsView->getFileList(target_fsid);
	} catch ( eos::MDException &e ) {
	  target_filelist.set_deleted_key( 0 );
	  target_filelist.set_empty_key(0xffffffffffffffff);
	}

	unsigned long long nfids = (unsigned long long) source_filelist.size();

	eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u n_source_fids=%llu", target_snapshot.mGroup.c_str(), gposition , source_fsid, target_fsid, nfids);
	unsigned long long rpos = (unsigned long long ) (( 0.999999 * random()* nfids )/RAND_MAX);
	eos::FileSystemView::FileIterator fit = source_filelist.begin();
	std::advance (fit,rpos);
	while (fit != source_filelist.end()) {
	  // check that the taret does not has this file
	  eos::FileMD::id_t fid = *fit;
	  if (target_filelist.count(fid)) {
	    // iterate to the next file, we have this file already
	    fit++;
	    continue;
	  } else {
	    // check that this file has not been scheduled during the 1h period
	    XrdSysMutexHelper sLock(sScheduledFidMutex);
	    if (sScheduledFid.size() > 100000) {
	      // do some cleanup
	      std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it1;
	      std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it2;
	      it1 = it2 = sScheduledFid.begin();
	      do {
		it1 = it2;
		it2++;
		if (it1->second < time(NULL)) {
		  sScheduledFid.erase(it1);
		}
	      } while (it2 != sScheduledFid.end());
	    }
	    if ( (sScheduledFid.count(fid) && ((sScheduledFid[fid] > (time(NULL))))) ) {
	      // iterate to the next file, we have scheduled this file during the last hour or anyway it is empty
	      fit++;
	      continue;
	    } else {
	      eos::FileMD* fmd = 0;
	      unsigned long long cid  = 0;
	      unsigned long	long size = 0;
	      long unsigned int  lid  = 0;
	      uid_t uid=0;
	      gid_t gid=0;
	      std::string fullpath = "";

	      try {
		fmd = gOFS->eosFileService->getFileMD(fid);
		fullpath = gOFS->eosView->getUri(fmd);
		fmd = gOFS->eosFileService->getFileMD(fid);
		lid = fmd->getLayoutId();
		cid = fmd->getContainerId();
		size = fmd->getSize();
		uid  = fmd->getCUid();
		gid  = fmd->getCGid(); 
	      } catch ( eos::MDException &e ) {
		fmd = 0;
	      }
	      
	      if (fmd) {
		if ((size>0) && (size < freebytes)) {
		  if (!simulate) {
		    // this file fits, let's return the capability to transfer it 
		    sScheduledFid[fid] = time(NULL) + 3600;
		  }

		  // we can schedule fid from source => target_it
		  eos_thread_info("subcmd=scheduling fid=%llx source_fsid=%u target_fsid=%u", fid, source_fsid, target_fsid);
		  
		  XrdOucString source_capability="";
		  XrdOucString sizestring;
		  source_capability += "mgm.access=read";
		  source_capability += "&mgm.lid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)lid&0xffffff0f);
		  // make's it a plain replica
		  source_capability += "&mgm.cid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
		  source_capability += "&mgm.ruid=";       source_capability+=(int)1;
		  source_capability += "&mgm.rgid=";       source_capability+=(int)1;
		  source_capability += "&mgm.uid=";        source_capability+=(int)1;
		  source_capability += "&mgm.gid=";        source_capability+=(int)1;
		  source_capability += "&mgm.path=";       source_capability += fullpath.c_str();
		  source_capability += "&mgm.manager=";    source_capability += gOFS->ManagerId.c_str();
		  source_capability += "&mgm.fid=";
		  XrdOucString hexfid; eos::common::FileId::Fid2Hex(fid,hexfid);source_capability += hexfid;
		  source_capability += "&mgm.drainfsid=";  source_capability += (int)source_fsid;
		  
		  // build the source_capability contents
		  source_capability += "&mgm.localprefix=";       source_capability += source_snapshot.mPath.c_str();
		  source_capability += "&mgm.fsid=";              source_capability += (int)source_snapshot.mId;
		  source_capability += "&mgm.sourcehostport=";    source_capability += source_snapshot.mHostPort.c_str();
		  
		  XrdOucString target_capability="";
		  target_capability += "mgm.access=write";
		  target_capability += "&mgm.lid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid&0xffffff0f); 
		  // make's it a plain replica
		  target_capability += "&mgm.source.lid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid);
		  target_capability += "&mgm.source.ruid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)uid);
		  target_capability += "&mgm.source.rgid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)gid);
		  
		  target_capability += "&mgm.cid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
		  target_capability += "&mgm.ruid=";       target_capability+=(int)1;
		  target_capability += "&mgm.rgid=";       target_capability+=(int)1;
		  target_capability += "&mgm.uid=";        target_capability+=(int)1;
		  target_capability += "&mgm.gid=";        target_capability+=(int)1;
		  target_capability += "&mgm.path=";       target_capability += fullpath.c_str();
		  target_capability += "&mgm.manager=";    target_capability += gOFS->ManagerId.c_str();
		  target_capability += "&mgm.fid=";
		  target_capability += hexfid;
		  target_capability += "&mgm.drainfsid=";  target_capability += (int)source_fsid;
		  
		  // build the target_capability contents
		  target_capability += "&mgm.localprefix=";       target_capability += target_snapshot.mPath.c_str();
		  target_capability += "&mgm.fsid=";              target_capability += (int)target_snapshot.mId;
		  target_capability += "&mgm.targethostport=";    target_capability += target_snapshot.mHostPort.c_str();
		  target_capability += "&mgm.bookingsize=";       target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
		  // issue a source_capability
		  XrdOucEnv insource_capability(source_capability.c_str());
		  XrdOucEnv intarget_capability(target_capability.c_str());
		  XrdOucEnv* source_capabilityenv = 0;
		  XrdOucEnv* target_capabilityenv = 0;
		  XrdOucString fullcapability="";
		  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
		  
		  int caprc=0;
		  if ((caprc=gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
		      (caprc=gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey))){
		    eos_thread_err("unable to create source/target capability - errno=%u", caprc);
		    gOFS->MgmStats.Add("SchedulingFailedBalance",0,0,1);         
		    return Emsg(epname, error, caprc, "create source/target capability [EADV]");
		  } else {
		    int caplen = 0;
		    XrdOucString source_cap = source_capabilityenv->Env(caplen);
		    XrdOucString target_cap = target_capabilityenv->Env(caplen);
		    source_cap.replace("cap.sym","source.cap.sym");
		    target_cap.replace("cap.sym","target.cap.sym");
		    source_cap.replace("cap.msg","source.cap.msg");
		    target_cap.replace("cap.msg","target.cap.msg");
		    source_cap += "&source.url=root://"; source_cap += source_snapshot.mHostPort.c_str();source_cap += "//replicate:"; source_cap += hexfid;
		    target_cap += "&target.url=root://"; target_cap += target_snapshot.mHostPort.c_str();target_cap += "//replicate:"; target_cap += hexfid;
		    fullcapability += source_cap;
		    fullcapability += target_cap;

		    // send submitted response
		    XrdOucString response="submitted";
		    error.setErrInfo(response.length()+1, response.c_str());

		    eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());
		    
		    if (!simulate) {
		      if (target_fs->GetBalanceQueue()->Add(txjob)) {
			eos_thread_info("cmd=queued fid=%x source_fs=%u target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
			eos_thread_debug("job=%s", fullcapability.c_str());
		      }
		    }
		    
		    if (txjob) {
		      delete txjob;
		    }

		    if (source_capabilityenv)
		      delete source_capabilityenv;
		    if (target_capabilityenv)
		      delete target_capabilityenv;

		    gOFS->MgmStats.Add("Scheduled2Balance",0,0,1);         
		    EXEC_TIMING_END("Scheduled2Balance");      
		    return SFS_DATA;
		  }
		} else {
		  fit++;
		  continue;
		}
	      } else {
		fit++;
		continue;
	      }
	    }
	  }
	}
	break;
      }
      gOFS->MgmStats.Add("SchedulingFailedBalance",0,0,1);
      error.setErrInfo(0,"");
      return SFS_DATA;
      //      return Emsg(epname, error, ENODATA, "schedule any file [ENODATA]");
    }

    if (execmd == "schedule2drain") {
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Scheduled2Drain");      
      gOFS->MgmStats.Add("Schedule2Drain",0,0,1);         

      XrdOucString sfsid       = env.Get("mgm.target.fsid");
      XrdOucString sfreebytes  = env.Get("mgm.target.freebytes");
      char* alogid             = env.Get("mgm.logid");
      char* simulate           = env.Get("mgm.simulate"); // used to test the routing

      // static map with iterator position for the next group scheduling and it's mutex
      static std::map<std::string, size_t> sGroupCycle;
      static XrdSysMutex sGroupCycleMutex;
      static XrdSysMutex sScheduledFidMutex;
      static std::map<eos::common::FileSystem::fsid_t, time_t> sScheduledFid;

      if (alogid) {
	ThreadLogId.SetLogId(alogid,tident);
      }
      
      if ((!sfsid.length()) || (!sfreebytes.length())) {
	gOFS->MgmStats.Add("SchedulingFailedDrain",0,0,1);         
	return Emsg(epname, error, EINVAL, "unable to schedule - missing parameters [EINVAL]");
      }

      eos::common::FileSystem::fsid_t target_fsid = atoi(sfsid.c_str());
      eos::common::FileSystem::fsid_t source_fsid = 0;
      eos::common::FileSystem::fs_snapshot target_snapshot;
      eos::common::FileSystem::fs_snapshot source_snapshot;
      eos::common::FileSystem::fs_snapshot replica_source_snapshot;
      eos::common::FileSystem* target_fs = 0;

      unsigned long long freebytes = (sfreebytes.c_str())?strtoull(sfreebytes.c_str(),0,10):0;

      eos_thread_info("cmd=schedule2drain fsid=%d freebytes=%llu logid=%s", target_fsid, freebytes, alogid?alogid:"");

      while(1)
      // lock the view and get the filesystem information for the target where be balance to
      {
	eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
	target_fs = FsView::gFsView.mIdView[target_fsid];
	if (!target_fs) {
	  eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
	  gOFS->MgmStats.Add("SchedulingFailedDrain",0,0,1);         
	  return Emsg(epname, error, EINVAL, "unable to schedule - filesystem ID is not known");
	}
	target_fs->SnapShotFileSystem(target_snapshot);
	FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

	size_t groupsize = FsView::gFsView.mGroupView.size();
	if (!group) {
	  eos_thread_err("group=%s is not in group view", target_snapshot.mGroup.c_str());
	  gOFS->MgmStats.Add("SchedulingFailedDrain",0,0,1);         
	  return Emsg(epname, error, EINVAL, "unable to schedule - group is not known [EINVAL]");
	}

	eos_thread_debug("group=%s", target_snapshot.mGroup.c_str());

	// select the next fs in the group to get a file to move
	size_t gposition=0;
	{
	  XrdSysMutexHelper(sGroupCycleMutex);
	  if (sGroupCycle.count(target_snapshot.mGroup)) {
	    gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
	  } else {
	    gposition = 0;
	    sGroupCycle[target_snapshot.mGroup]=0;
	  }
	  // shift the iterator for the next schedule call to the following filesystem in the group
	  sGroupCycle[target_snapshot.mGroup]++;
	  sGroupCycle[target_snapshot.mGroup]%= groupsize;
	}
	  
	eos_thread_debug("group=%s cycle=%lu", target_snapshot.mGroup.c_str(), gposition);
	// try to find a file, which is smaller than the free bytes and has no replica on the target filesystem
	// we start at a random position not to move data of the same period to a single disk

	group = FsView::gFsView.mGroupView[target_snapshot.mGroup];	
	FsGroup::const_iterator group_iterator;
	group_iterator = group->begin();
	std::advance(group_iterator, gposition);

	eos::common::FileSystem* source_fs = 0;
	for (size_t n=0; n< group->size(); n++) {
	  // look for a filesystem in drain mode
	  if ( (eos::common::FileSystem::GetDrainStatusFromString(FsView::gFsView.mIdView[*group_iterator]->GetString("stat.drain").c_str()) != eos::common::FileSystem::kDraining ) &&
	       (eos::common::FileSystem::GetDrainStatusFromString(FsView::gFsView.mIdView[*group_iterator]->GetString("stat.drain").c_str()) != eos::common::FileSystem::kDrainStalling ) ) {
	    source_fs = 0;
	    group_iterator++; if (group_iterator == group->end()) group_iterator = group->begin();
	    continue;
	  }
	  source_fs = FsView::gFsView.mIdView[*group_iterator];
	  if (!source_fs)
	    continue;
	  source_fs->SnapShotFileSystem(source_snapshot);
	  source_fsid = *group_iterator;
	}

	if (!source_fs) {
	  eos_thread_debug("no source available");
	  gOFS->MgmStats.Add("SchedulingFailedDrain",0,0,1);         
	  error.setErrInfo(0,"");
	  return SFS_DATA;
	  //	  return Emsg(epname, error, EINVAL, "unable to schedule - no source available [ENODATA]");
	}
	source_fs->SnapShotFileSystem(source_snapshot);

	// -------------------------------------------
	// keep the lock order View=>Quota=>Namespace
        // -------------------------------------------
	eos::common::RWMutexReadLock gLock(Quota::gQuotaMutex);
        // -------------------------------------------
	eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
        // -------------------------------------------

	eos::FileSystemView::FileList source_filelist;
	eos::FileSystemView::FileList target_filelist;

	try {
	  source_filelist = gOFS->eosFsView->getFileList(source_fsid);
	} catch ( eos::MDException &e ) {
	  source_filelist.set_deleted_key( 0 );
	  source_filelist.set_empty_key(0xffffffffffffffff);
	}

	try {
	  target_filelist = gOFS->eosFsView->getFileList(target_fsid);
	} catch ( eos::MDException &e ) {
	  target_filelist.set_deleted_key( 0 );
	  target_filelist.set_empty_key(0xffffffffffffffff);
	}

	unsigned long long nfids = (unsigned long long) source_filelist.size();

	eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u n_source_fids=%llu", target_snapshot.mGroup.c_str(), gposition , source_fsid, target_fsid, nfids);

	// give the oldest file first
	eos::FileSystemView::FileIterator fit = source_filelist.begin();
	while (fit != source_filelist.end()) {
	  eos_thread_debug("checking fid %llx", *fit);
	  // check that the target does not have this file
	  eos::FileMD::id_t fid = *fit;
	  if (target_filelist.count(fid)) {
	    // iterate to the next file, we have this file already
	    fit++;
	    continue;
	  } else {
	    // check that this file has not been scheduled during the 1h period
	    XrdSysMutexHelper sLock(sScheduledFidMutex);
	    if (sScheduledFid.size() > 100000) {
	      // do some cleanup
	      std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it1;
	      std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it2;
	      it1 = it2 = sScheduledFid.begin();
	      do {
		it1 = it2;
		it2++;
		if (it1->second < time(NULL)) {
		  sScheduledFid.erase(it1);
		}
	      } while (it2 != sScheduledFid.end());
	    }
	    if ( (sScheduledFid.count(fid) && ((sScheduledFid[fid] > (time(NULL))))) ) {
	      // iterate to the next file, we have scheduled this file during the last hour or anyway it is empty
	      fit++;
	      eos_thread_debug("file %llx has already been scheduled at %lu", fid, sScheduledFid[fid]);
	      continue;
	    } else {
	      eos::FileMD* fmd = 0;
	      unsigned long long cid  = 0;
	      unsigned long	long size = 0;
	      long unsigned int  lid  = 0;
	      uid_t uid=0;
	      gid_t gid=0;
	      std::string fullpath = "";
	      std::vector<unsigned int> locationfs;

	      try {
		fmd = gOFS->eosFileService->getFileMD(fid);
		fullpath = gOFS->eosView->getUri(fmd);
		fmd = gOFS->eosFileService->getFileMD(fid);
		lid = fmd->getLayoutId();
		cid = fmd->getContainerId();
		size = fmd->getSize();
		uid  = fmd->getCUid();
		gid  = fmd->getCGid(); 

		
		eos::FileMD::LocationVector::const_iterator lociter;
		for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
		  // ignore filesystem id 0
		  if ((*lociter)) {
		    if ( (source_snapshot.mId == *lociter)) {
		      if (source_snapshot.mConfigStatus == eos::common::FileSystem::kDrain) {
			// only add filesystems which are not in drain dead to the possible locations
			locationfs.push_back(*lociter);
		      }
		    } else {
		      locationfs.push_back(*lociter);
		    }
		  }
		}
	      } catch ( eos::MDException &e ) {
		fmd = 0;
	      }
	      	      
	      if (fmd) {
		// get the access scheduled
		XrdOucString sizestring="";
		long unsigned int fsindex = 0;
                
		// get the responsible quota space
		SpaceQuota* space = Quota::GetSpaceQuota(source_snapshot.mSpace.c_str(), false);
		if (space) {
		  eos_thread_debug("space=%s", space->GetSpaceName());
		} else {
		  eos_thread_err("No responsible space for |%s|", source_snapshot.mSpace.c_str());
		}

		// schedule access to that file with the original layout
		int retc=0;
		std::vector<unsigned int> unavailfs; // not used
		if ((!space) || (retc=space->FileAccess((uid_t)0,(gid_t)0,(long unsigned int)0, (const char*) 0, lid, locationfs, fsindex, false, (long long unsigned)0, unavailfs))) {
		  // inaccessible files we let retry after 60 minutes
		  eos_thread_err("no access to file %llx retc=%d", fid, retc);
		  sScheduledFid[fid] = time(NULL) + 3600;
		  // try with next file
		  fit++;
		  continue;
		} 
		
		if ((size < freebytes)) {
		  eos::common::FileSystem* replica_source_fs = 0;
		  replica_source_fs = FsView::gFsView.mIdView[locationfs[fsindex]];
		  if (!replica_source_fs) {
		    fit++;
		    continue;
		  }
		  replica_source_fs->SnapShotFileSystem(replica_source_snapshot);

		  if (!simulate) {
		    // this file fits, let's return the capability to transfer it 
		    sScheduledFid[fid] = time(NULL) + 3600;
		  }
		  
		  // we can schedule fid from replica_source => target_it
		  eos_thread_info("subcmd=scheduling fid=%llx drain_fsid=%u replica_source_fsid=%u target_fsid=%u", fid, source_fsid, locationfs[fsindex], target_fsid);
		  
		  XrdOucString replica_source_capability="";
		  XrdOucString sizestring;
		  replica_source_capability += "mgm.access=read";
		  replica_source_capability += "&mgm.lid=";        replica_source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)lid&0xffffff0f);
		  // make's it a plain replica
		  replica_source_capability += "&mgm.cid=";        replica_source_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
		  replica_source_capability += "&mgm.ruid=";       replica_source_capability+=(int)1;
		  replica_source_capability += "&mgm.rgid=";       replica_source_capability+=(int)1;
		  replica_source_capability += "&mgm.uid=";        replica_source_capability+=(int)1;
		  replica_source_capability += "&mgm.gid=";        replica_source_capability+=(int)1;
		  replica_source_capability += "&mgm.path=";       replica_source_capability += fullpath.c_str();
		  replica_source_capability += "&mgm.manager=";    replica_source_capability += gOFS->ManagerId.c_str();
		  replica_source_capability += "&mgm.fid=";
		  XrdOucString hexfid; eos::common::FileId::Fid2Hex(fid,hexfid);replica_source_capability += hexfid;
		  replica_source_capability += "&mgm.drainfsid=";  replica_source_capability += (int)source_fsid;
		  
		  // build the replica_source_capability contents
		  replica_source_capability += "&mgm.localprefix=";       replica_source_capability += replica_source_snapshot.mPath.c_str();
		  replica_source_capability += "&mgm.fsid=";              replica_source_capability += (int)replica_source_snapshot.mId;
		  replica_source_capability += "&mgm.sourcehostport=";    replica_source_capability += replica_source_snapshot.mHostPort.c_str();
		  
		  XrdOucString target_capability="";
		  target_capability += "mgm.access=write";
		  target_capability += "&mgm.lid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid&0xffffff0f); 
		  // make's it a plain replica
		  target_capability += "&mgm.source.lid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid);
		  target_capability += "&mgm.source.ruid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)uid);
		  target_capability += "&mgm.source.rgid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)gid);
		  
		  target_capability += "&mgm.cid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
		  target_capability += "&mgm.ruid=";       target_capability+=(int)1;
		  target_capability += "&mgm.rgid=";       target_capability+=(int)1;
		  target_capability += "&mgm.uid=";        target_capability+=(int)1;
		  target_capability += "&mgm.gid=";        target_capability+=(int)1;
		  target_capability += "&mgm.path=";       target_capability += fullpath.c_str();
		  target_capability += "&mgm.manager=";    target_capability += gOFS->ManagerId.c_str();
		  target_capability += "&mgm.fid=";
		  target_capability += hexfid;
		  target_capability += "&mgm.drainfsid=";  target_capability += (int)source_fsid;
		  
		  // build the target_capability contents
		  target_capability += "&mgm.localprefix=";       target_capability += target_snapshot.mPath.c_str();
		  target_capability += "&mgm.fsid=";              target_capability += (int)target_snapshot.mId;
		  target_capability += "&mgm.targethostport=";    target_capability += target_snapshot.mHostPort.c_str();
		  target_capability += "&mgm.bookingsize=";       target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
		  // issue a replica_source_capability
		  XrdOucEnv insource_capability(replica_source_capability.c_str());
		  XrdOucEnv intarget_capability(target_capability.c_str());
		  XrdOucEnv* source_capabilityenv = 0;
		  XrdOucEnv* target_capabilityenv = 0;
		  XrdOucString fullcapability="";
		  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
		  
		  int caprc=0;
		  if ((caprc=gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
		      (caprc=gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey))){
		    eos_thread_err("unable to create source/target capability - errno=%u", caprc);
		    gOFS->MgmStats.Add("SchedulingFailedDrain",0,0,1);         
		    return Emsg(epname, error, caprc, "create source/target capability [EADV]");
		  } else {
		    int caplen = 0;
		    XrdOucString source_cap = source_capabilityenv->Env(caplen);
		    XrdOucString target_cap = target_capabilityenv->Env(caplen);
		    source_cap.replace("cap.sym","source.cap.sym");
		    target_cap.replace("cap.sym","target.cap.sym");
		    source_cap.replace("cap.msg","source.cap.msg");
		    target_cap.replace("cap.msg","target.cap.msg");
		    source_cap += "&source.url=root://"; source_cap += replica_source_snapshot.mHostPort.c_str();source_cap += "//replicate:"; source_cap += hexfid;
		    target_cap += "&target.url=root://"; target_cap += target_snapshot.mHostPort.c_str();target_cap += "//replicate:"; target_cap += hexfid;
		    fullcapability += source_cap;
		    fullcapability += target_cap;
		    // send submitted response
		    XrdOucString response="submitted";
		    error.setErrInfo(response.length()+1, response.c_str());

		    eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());

		    if (!simulate) {
		      if (target_fs->GetDrainQueue()->Add(txjob)) {
			eos_thread_info("cmd=queued fid=%x source_fs=%u target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
			eos_thread_debug("job=%s", fullcapability.c_str());
		      }
		    }

		    if (txjob) {
		      delete txjob;
		    }

		    if (source_capabilityenv)
		      delete source_capabilityenv;
		    if (target_capabilityenv)
		      delete target_capabilityenv;

		    gOFS->MgmStats.Add("Scheduled2Drain",0,0,1);         
		    EXEC_TIMING_END("Scheduled2Drain");      
		    return SFS_DATA;
		  }
		} else {
		  fit++;
		  continue;
		}
	      } else {
		fit++;
		continue;
	      }
	    }
	  }
	}
	break;
      }
      gOFS->MgmStats.Add("SchedulingFailedDrain",0,0,1);         
      error.setErrInfo(0,"");
      return SFS_DATA;
      //      return Emsg(epname, error, ENODATA, "schedule any file [ENODATA]");
    }
    eos_thread_err("No implementation for %s", execmd.c_str());
  }

  return  Emsg(epname,error,EINVAL,"execute FSctl command",spath.c_str());  
}


/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_ls(const char             *inpath,
                   XrdOucErrInfo          &error,
                   const XrdSecEntity     *client,
                   const char             *info,
                   eos::ContainerMD::XAttrMap &map)
{
  static const char *epname = "attr_ls";
  const char *tident = error.getErrUser(); 
  XrdOucEnv access_Env(info);
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP; 

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Stat,"access",path,error);
  
  eos::common::Mapping::IdMap(client,info,tident,vid);  

  return _attr_ls(path, error,vid,info,map);
}   

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_set(const char             *inpath,
                    XrdOucErrInfo          &error,
                    const XrdSecEntity     *client,
                    const char             *info,
                    const char             *key,
                    const char             *value)
{
  static const char *epname = "attr_set";
  const char *tident = error.getErrUser(); 
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdOucEnv access_Env(info);

  NAMESPACEMAP;

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Update,"update",path,error);
  
  eos::common::Mapping::IdMap(client,info,tident,vid);  
  
  return _attr_set(path, error,vid,info,key,value);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_get(const char             *inpath,
		    XrdOucErrInfo          &error,
		    const XrdSecEntity     *client,
		    const char             *info,
		    const char             *key,
		    XrdOucString           &value)
{
  static const char *epname = "attr_get";
  const char *tident = error.getErrUser(); 
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdOucEnv access_Env(info);

  NAMESPACEMAP;

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Stat,"access",path,error);
  
  eos::common::Mapping::IdMap(client,info,tident,vid);  

  return _attr_get(path, error,vid,info,key,value);
}

/*----------------------------------------------------------------------------*/
int         
XrdMgmOfs::attr_rem(const char             *inpath,
		    XrdOucErrInfo          &error,
		    const XrdSecEntity     *client,
		    const char             *info,
		    const char             *key)
{
  static const char *epname = "attr_rm";
  const char *tident = error.getErrUser(); 
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdOucEnv access_Env(info); 

  NAMESPACEMAP;

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Delete,"delete",path,error);
  
  eos::common::Mapping::IdMap(client,info,tident,vid);  
  
  return _attr_rem(path, error,vid,info,key);
}

/*----------------------------------------------------------------------------*/  
int
XrdMgmOfs::_attr_ls(const char             *path,
                    XrdOucErrInfo    &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char             *info,
                    eos::ContainerMD::XAttrMap &map)
{
  static const char *epname = "attr_ls";  
  eos::ContainerMD *dh=0;
  errno = 0;
  
  EXEC_TIMING_BEGIN("AttrLs");      

  gOFS->MgmStats.Add("AttrLs",vid.uid,vid.gid,1);  
  
  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);      
  try {
    dh = gOFS->eosView->getContainer(path);
    eos::ContainerMD::XAttrMap::const_iterator it;
    for ( it=dh->attributesBegin(); it != dh->attributesEnd(); ++it) {
      XrdOucString key = it->first.c_str();
      // we don't show sys.* attributes to others than root
      //      if ( key.beginswith("sys.") && (!vid.sudoer) )
      //        continue;
      map[it->first] = it->second;
    }
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());    
  }
  // check permissions
  if (dh && (!dh->access(vid.uid,vid.gid, X_OK|R_OK)))
    if (!errno)errno = EPERM;


  EXEC_TIMING_END("AttrLs");      

  if (errno) 
    return  Emsg(epname,error,errno,"list attributes",path);  

  return SFS_OK;
}   

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_set(const char             *path,
                     XrdOucErrInfo    &error,
                     eos::common::Mapping::VirtualIdentity &vid,
                     const char             *info,
                     const char             *key,
                     const char             *value)
{
  static const char *epname = "attr_set";  
  eos::ContainerMD *dh=0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrSet");      

  gOFS->MgmStats.Add("AttrSet",vid.uid,vid.gid,1);  

  if ( !key || !value) 
    return  Emsg(epname,error,EINVAL,"set attribute",path);  

  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);      
  try {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    if ( Key.beginswith("sys.") && (!vid.sudoer) )
      errno = EPERM;
    else {
      // check permissions in case of user attributes
      if (dh && Key.beginswith("user.") && (vid.uid != dh->getCUid()) && (!vid.sudoer)) {
	errno = EPERM;
      } else {
	dh->setAttribute(key,value);
	eosView->updateContainerStore(dh);
      }
    }
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("AttrSet");      
 
  if (errno) 
    return  Emsg(epname,error,errno,"list attributes",path);  

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_get(const char             *path,
		     XrdOucErrInfo          &error,
		     eos::common::Mapping::VirtualIdentity &vid,
		     const char             *info,
		     const char             *key,
		     XrdOucString           &value,
		     bool                    islocked)
{
  static const char *epname = "attr_get";  
  eos::ContainerMD *dh=0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrGet");      

  gOFS->MgmStats.Add("AttrGet",vid.uid,vid.gid,1);  

  if ( !key) 
    return  Emsg(epname,error,EINVAL,"get attribute",path);  

  value = "";

  //------------------------------------------- 
  if(!islocked) gOFS->eosViewRWMutex.LockRead();
  try {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    //    if ( Key.beginswith("sys.") && (!vid.sudoer) )
    //      errno = EPERM;
    //    else 
      value = (dh->getAttribute(key)).c_str();
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());  
  }
  // check permissions
  if (dh && (!dh->access(vid.uid,vid.gid, X_OK|R_OK)))
    if (!errno) errno = EPERM;
  
  if (!islocked) gOFS->eosViewRWMutex.UnLockRead();

  EXEC_TIMING_END("AttrGet");        

  if (errno) 
    return  Emsg(epname,error,errno,"list attributes",path);;  

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_rem(const char             *path,
                     XrdOucErrInfo    &error,
                     eos::common::Mapping::VirtualIdentity &vid,
                     const char             *info,
                     const char             *key)
{
  static const char *epname = "attr_rm";  
  eos::ContainerMD *dh=0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrRm");      

  gOFS->MgmStats.Add("AttrRm",vid.uid,vid.gid,1);  

  if ( !key ) 
    return  Emsg(epname,error,EINVAL,"delete attribute",path);  

  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);       
  try {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    if ( Key.beginswith("sys.") && (!vid.sudoer) )
      errno = EPERM;
    else 
      dh->removeAttribute(key);
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());   
  }
  // check permissions
  if (dh && (!dh->access(vid.uid,vid.gid, X_OK|R_OK)))
    if (!errno) errno = EPERM;

  EXEC_TIMING_END("AttrRm");      
  
  if (errno) 
    return  Emsg(epname,error,errno,"remove attribute",path);  

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_verifystripe(const char             *path,
                         XrdOucErrInfo          &error,
                         eos::common::Mapping::VirtualIdentity &vid,
                         unsigned long           fsid,
                         XrdOucString            option)
{
  static const char *epname = "verifystripe";  
  eos::ContainerMD *dh=0;
  eos::FileMD *fmd=0;

  EXEC_TIMING_BEGIN("VerifyStripe");      

  errno = 0;
  unsigned long long fid=0;
  unsigned long long cid=0;
  int lid=0;

  eos::ContainerMD::XAttrMap attrmap;

  gOFS->MgmStats.Add("VerifyStripe",vid.uid,vid.gid,1);  

  eos_debug("verify");
  eos::common::Path cPath(path);
  
  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);        
  try {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    eos::ContainerMD::XAttrMap::const_iterator it;
    for ( it = dh->attributesBegin(); it != dh->attributesEnd(); ++it) {
      attrmap[it->first] = it->second;
    }
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());   
  }
  
  // check permissions
  if (dh && (!dh->access(vid.uid,vid.gid, X_OK|W_OK)))
    if (!errno) errno = EPERM;
  
  
  if (errno) {   
    return  Emsg(epname,error,errno,"drop stripe",path);  
  }
    
  // get the file
  try {
    fmd = gOFS->eosView->getFile(path);
    
    // we don't check anymore if we know about this location, we just send to the filesystem, because we want to have a method to register a not commited replica
    //    if (fmd->hasLocation(fsid)) {
    //      eos_debug("verifying location %u", fsid);
    //      errno = 0;
    //    } else {
    //      errno = ENOENT;
    //    }
    fid = fmd->getId();
    lid = fmd->getLayoutId();
    cid = fmd->getContainerId();           
  } catch( eos::MDException &e ) {
      fmd = 0;
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());     
  } 

  if (!errno) {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* verifyfilesystem = 0;
    if (FsView::gFsView.mIdView.count(fsid)) {
      verifyfilesystem = FsView::gFsView.mIdView[fsid];
    }
    if (!verifyfilesystem) {
      errno = EINVAL;
      return  Emsg(epname,error,ENOENT,"verify stripe - filesystem does not exist",fmd->getName().c_str());  
    }

    XrdOucString receiver    = verifyfilesystem->GetQueue().c_str();
    XrdOucString opaquestring = "";
    // build the opaquestring contents
    opaquestring += "&mgm.localprefix=";       opaquestring += verifyfilesystem->GetPath().c_str();
    opaquestring += "&mgm.fid=";XrdOucString hexfid; eos::common::FileId::Fid2Hex(fid,hexfid);opaquestring += hexfid;
    opaquestring += "&mgm.manager=";           
    opaquestring += gOFS->ManagerId.c_str();
    opaquestring += "&mgm.access=verify";
    opaquestring += "&mgm.fsid=";              opaquestring += (int)verifyfilesystem->GetId();
    if (attrmap.count("user.tag")) {
      opaquestring += "&mgm.container=";
      opaquestring += attrmap["user.tag"].c_str();
    }
    XrdOucString sizestring="";
    opaquestring += "&mgm.cid=";               opaquestring += eos::common::StringConversion::GetSizeString(sizestring,cid);
    opaquestring += "&mgm.path=";              opaquestring += path;
    opaquestring += "&mgm.lid=";               opaquestring += lid;

    if (option.length()) {
      opaquestring += option;
    }

    XrdMqMessage message("verifycation");
    XrdOucString msgbody = "mgm.cmd=verify"; 

    msgbody += opaquestring;

    // we send deletions in bunches of max 1000 for efficiency
    message.SetBody(msgbody.c_str());   

    if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str())) {
      eos_static_err("unable to send verification message to %s", receiver.c_str());
      errno = ECOMM;
    } else {
      errno = 0;
    }
  }

  EXEC_TIMING_END("VerifyStripe");      
  
  if (errno) 
    return  Emsg(epname,error,errno,"verify stripe",path);  

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropstripe(const char             *path,
                       XrdOucErrInfo          &error,
                       eos::common::Mapping::VirtualIdentity &vid,
                       unsigned long           fsid,
                       bool                    forceRemove)
{
  static const char *epname = "dropstripe";  
  eos::ContainerMD *dh=0;
  eos::FileMD *fmd=0;
  errno = 0;

  EXEC_TIMING_BEGIN("DropStripe");

  gOFS->MgmStats.Add("DropStripe",vid.uid,vid.gid,1);  

  eos_debug("drop");
  eos::common::Path cPath(path);
  //-------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);      
  try {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid,vid.gid, X_OK|W_OK)))
    if (!errno) errno = EPERM;

  if (errno) {    
    return  Emsg(epname,error,errno,"drop stripe",path);  
  }

  // get the file
  try {
    fmd = gOFS->eosView->getFile(path);
    if (!forceRemove) {
      // we only unlink a location
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
        gOFS->eosView->updateFileStore(fmd);
        eos_debug("unlinking location %u", fsid);
      } else {
        errno = ENOENT;
      }
    } else {
      // we unlink and remove a location by force
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
      }
      fmd->removeLocation(fsid);
      gOFS->eosView->updateFileStore(fmd);
      eos_debug("removing/unlinking location %u", fsid);
    }
  } catch( eos::MDException &e ) {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());   
  } 

  EXEC_TIMING_END("DropStripe");
  
  if (errno) 
    return  Emsg(epname,error,errno,"drop stripe",path);  

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_movestripe(const char             *path,
                       XrdOucErrInfo          &error,
                       eos::common::Mapping::VirtualIdentity &vid,
                       unsigned long           sourcefsid,
                       unsigned long           targetfsid,
                       bool                    expressflag)
{
  EXEC_TIMING_BEGIN("MoveStripe");    
  int retc = _replicatestripe(path, error,vid,sourcefsid,targetfsid,true, expressflag);
  EXEC_TIMING_END("MoveStripe");    
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_copystripe(const char             *path,
                       XrdOucErrInfo          &error,
                       eos::common::Mapping::VirtualIdentity &vid,
                       unsigned long           sourcefsid,
                       unsigned long           targetfsid, 
                       bool                    expressflag)
{
  EXEC_TIMING_BEGIN("CopyStripe");    
  int retc =  _replicatestripe(path, error,vid,sourcefsid,targetfsid,false, expressflag);
  EXEC_TIMING_END("CopyStripe");    
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe(const char             *path,
                            XrdOucErrInfo          &error,
                            eos::common::Mapping::VirtualIdentity &vid,
                            unsigned long           sourcefsid,
                            unsigned long           targetfsid, 
                            bool                    dropsource,
                            bool                    expressflag)
{
  static const char *epname = "replicatestripe";  
  eos::ContainerMD *dh=0;
  errno = 0;
  
  EXEC_TIMING_BEGIN("ReplicateStripe");
  
  eos::common::Path cPath(path);

  eos_debug("replicating %s from %u=>%u [drop=%d]", path, sourcefsid,targetfsid,dropsource);
  //-------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);       
  try {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
  } catch( eos::MDException &e ) {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str());  
  }

  // check permissions
  if (dh && (!dh->access(vid.uid,vid.gid, X_OK|W_OK)))
    if (!errno) errno = EPERM;

  eos::FileMD* fmd=0;

  // get the file
  try {
    fmd = gOFS->eosView->getFile(path);
    if (fmd->hasLocation(sourcefsid)) {
      if (fmd->hasLocation(targetfsid)) {
        errno = EEXIST;
      } 
    } else {
      // this replica does not exist!
      errno = ENODATA;
    }
  } catch( eos::MDException &e ) {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),e.getMessage().str().c_str()); 
  }
  
  if (errno) {    
    //-------------------------------------------
    return  Emsg(epname,error,errno,"replicate stripe",path);    
  }

  // make a copy of the file meta data to release the lock
  eos::FileMD fmdCopy(*fmd);
  fmd = &fmdCopy;

  //-------------------------------------------

  int retc =  _replicatestripe(fmd, path, error, vid, sourcefsid, targetfsid, dropsource, expressflag);

  EXEC_TIMING_END("ReplicateStripe");

  return retc;

}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe(eos::FileMD            *fmd, 
                            const char*             path,
                            XrdOucErrInfo          &error,
                            eos::common::Mapping::VirtualIdentity &vid,
                            unsigned long           sourcefsid,
                            unsigned long           targetfsid, 
                            bool                    dropsource,
                            bool                    expressflag)
{
  static const char *epname = "replicatestripe";  
  unsigned long long fid = fmd->getId();
  unsigned long long cid = fmd->getContainerId();
  long unsigned int  lid = fmd->getLayoutId();
  uid_t              uid = fmd->getCUid();
  gid_t              gid = fmd->getCGid();

  unsigned long long size = fmd->getSize();

  if (dropsource) 
    gOFS->MgmStats.Add("MoveStripe",vid.uid,vid.gid,1);  
  else 
    gOFS->MgmStats.Add("CopyStripe",vid.uid,vid.gid,1);  
 
  if ( (!sourcefsid) || (!targetfsid) ) {
    eos_err("illegal fsid sourcefsid=%u targetfsid=%u", sourcefsid, targetfsid);
    return Emsg(epname,error, EINVAL, "illegal source/target fsid", fmd->getName().c_str());
  }

  eos::mgm::FileSystem* sourcefilesystem = 0;
  eos::mgm::FileSystem* targetfilesystem = 0;

  
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  if (FsView::gFsView.mIdView.count(sourcefsid)) {
    sourcefilesystem = FsView::gFsView.mIdView[sourcefsid];
  }

  if (FsView::gFsView.mIdView.count(targetfsid)) {
    targetfilesystem = FsView::gFsView.mIdView[targetfsid];
  }

  if (!sourcefilesystem) {
    errno = EINVAL;
    return  Emsg(epname,error,ENOENT,"replicate stripe - source filesystem does not exist",fmd->getName().c_str());  
  }

  if (!targetfilesystem) {
    errno = EINVAL;
    return  Emsg(epname,error,ENOENT,"replicate stripe - target filesystem does not exist",fmd->getName().c_str());  
  }

  // snapshot the filesystems
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem::fs_snapshot target_snapshot;
  sourcefilesystem->SnapShotFileSystem(source_snapshot);
  targetfilesystem->SnapShotFileSystem(target_snapshot);
  
  // build a transfer capability
  XrdOucString source_capability="";
  XrdOucString sizestring;
  source_capability += "mgm.access=read";
  source_capability += "&mgm.lid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)lid&0xffffff0f);
  // make's it a plain replica
  source_capability += "&mgm.cid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
  source_capability += "&mgm.ruid=";       source_capability+=(int)1;
  source_capability += "&mgm.rgid=";       source_capability+=(int)1;
  source_capability += "&mgm.uid=";        source_capability+=(int)1;
  source_capability += "&mgm.gid=";        source_capability+=(int)1;
  source_capability += "&mgm.path=";       source_capability += path;
  source_capability += "&mgm.manager=";    source_capability += gOFS->ManagerId.c_str();
  source_capability += "&mgm.fid=";
  XrdOucString hexfid; eos::common::FileId::Fid2Hex(fid,hexfid);source_capability += hexfid;

  // this is a move of a replica
  if (dropsource) {
    source_capability += "&mgm.drainfsid=";  source_capability += (int)source_snapshot.mId;
  }
  
  // build the source_capability contents
  source_capability += "&mgm.localprefix=";       source_capability += source_snapshot.mPath.c_str();
  source_capability += "&mgm.fsid=";              source_capability += (int)source_snapshot.mId;
  source_capability += "&mgm.sourcehostport=";    source_capability += source_snapshot.mHostPort.c_str();
  
  XrdOucString target_capability="";
  target_capability += "mgm.access=write";
  target_capability += "&mgm.lid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid&0xffffff0f); 
  // make's it a plain replica
  target_capability += "&mgm.cid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
  target_capability += "&mgm.ruid=";       target_capability+=(int)1;
  target_capability += "&mgm.rgid=";       target_capability+=(int)1;
  target_capability += "&mgm.uid=";        target_capability+=(int)1;
  target_capability += "&mgm.gid=";        target_capability+=(int)1;
  target_capability += "&mgm.path=";       target_capability += path;
  target_capability += "&mgm.manager=";    target_capability += gOFS->ManagerId.c_str();
  target_capability += "&mgm.fid=";
  target_capability += hexfid; 
  if (dropsource) {
    target_capability += "&mgm.drainfsid=";  target_capability += (int)source_snapshot.mId;
  }

  target_capability += "&mgm.source.lid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid);
  target_capability += "&mgm.source.ruid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)uid);
  target_capability += "&mgm.source.rgid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)gid);
  
  // build the target_capability contents
  target_capability += "&mgm.localprefix=";       target_capability += target_snapshot.mPath.c_str();
  target_capability += "&mgm.fsid=";              target_capability += (int)target_snapshot.mId;
  target_capability += "&mgm.targethostport=";    target_capability += target_snapshot.mHostPort.c_str();
  target_capability += "&mgm.bookingsize=";       target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
  // issue a source_capability
  XrdOucEnv insource_capability(source_capability.c_str());
  XrdOucEnv intarget_capability(target_capability.c_str());
  XrdOucEnv* source_capabilityenv = 0;
  XrdOucEnv* target_capabilityenv = 0;
  XrdOucString fullcapability="";
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  
  int caprc=0;
  if ((caprc=gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
      (caprc=gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey))){
    eos_err("unable to create source/target capability - errno=%u", caprc);
    errno = caprc;
  } else {
    errno = 0;
    int caplen = 0;
    XrdOucString source_cap = source_capabilityenv->Env(caplen);
    XrdOucString target_cap = target_capabilityenv->Env(caplen);
    source_cap.replace("cap.sym","source.cap.sym");
    target_cap.replace("cap.sym","target.cap.sym");
    source_cap.replace("cap.msg","source.cap.msg");
    target_cap.replace("cap.msg","target.cap.msg");
    source_cap += "&source.url=root://"; source_cap += source_snapshot.mHostPort.c_str();source_cap += "//replicate:"; source_cap += hexfid;
    target_cap += "&target.url=root://"; target_cap += target_snapshot.mHostPort.c_str();target_cap += "//replicate:"; target_cap += hexfid;
    fullcapability += source_cap;
    fullcapability += target_cap;
    
    eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());
    
    bool sub = targetfilesystem->GetExternQueue()->Add(txjob);
    eos_info("info=\"submitted transfer job\" subretc=%d fxid=%s fid=%llu cap=%s\n", sub, hexfid.c_str(), fid, fullcapability.c_str());

    if (!sub) 
      errno = ENXIO;

    if (txjob)
      delete txjob;
    else {
      eos_err("Couldn't create transfer job to replicate stripe of %s", path);
      errno = ENOMEM;
    }

    if (source_capabilityenv)
      delete source_capabilityenv;
    if (target_capabilityenv)
      delete target_capabilityenv;
  }
  
  
  if (errno) 
    return  Emsg(epname,error,errno,"replicate stripe",fmd->getName().c_str());  

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::StartMgmDeletion(void *pp) 
{
  XrdMgmOfs* ofs = (XrdMgmOfs*)pp;
  ofs->Deletion();
  return 0;
}


/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::StartMgmStats(void *pp) 
{
  XrdMgmOfs* ofs = (XrdMgmOfs*)pp;
  ofs->MgmStats.Circulate();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::StartMgmFsListener(void *pp) 
{
  XrdMgmOfs* ofs = (XrdMgmOfs*)pp;
  ofs->FsListener();
  return 0;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::Deletion() 
{
  // thread distributing deletions
  while (1) {
    XrdSysTimer sleeper;
    sleeper.Snooze(60);   
    eos_static_debug("running deletion");
    std::vector <unsigned int> fslist;
    // get a list of file Ids

    std::map<eos::common::FileSystem::fsid_t, eos::mgm::FileSystem*>::const_iterator it;

    {
      // lock the filesystem view for reading
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      
      for (it= FsView::gFsView.mIdView.begin() ; it != FsView::gFsView.mIdView.end(); ++it) {
        fslist.push_back(it->first);
      }
    }
    
    for (unsigned int i=0 ; i< fslist.size(); i++) {
      // loop over all file systems

      //-------------------------------------------
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);      
      std::pair<eos::FileSystemView::FileIterator, eos::FileSystemView::FileIterator> unlinkpair;
      try {
        unlinkpair = eosFsView->getUnlinkedFiles( fslist[i] );
        XrdMqMessage message("deletion");
        eos::FileSystemView::FileIterator it;
        int ndeleted=0;

        eos::mgm::FileSystem* fs = 0;
        XrdOucString receiver="";
        XrdOucString msgbody = "mgm.cmd=drop"; 
        XrdOucString capability ="";
        XrdOucString idlist="";

        for ( it = unlinkpair.first; it != unlinkpair.second; ++it) {
          eos_static_info("deleting fid %u", *it);
          ndeleted++;
        
          // loop over all files and emit a deletion message
          if (!fs) {
            // set the file system only for the first file to relax the mutex contention
            if (!fslist[i]) {
              eos_err("0 filesystem in deletion list");
              continue ;
            }

            if (FsView::gFsView.mIdView.count(fslist[i])) {
              fs = FsView::gFsView.mIdView[fslist[i]];
            } else {
              fs = 0;
            }

            if (fs) {
              // check the state of the filesystem (if it can actually delete in this moment!)
              //              if ( (fs->GetConfigStatus() <= eos::common::FileSystem::kOff) || 
              //                   (fs->GetBootStatus()  != eos::common::FileSystem::kBooted) ) {
              //                // we don't need to send messages, this one is anyway down
              //                break;
              //              }

              if ( (fs->GetActiveStatus() == eos::common::FileSystem::kOffline) ) {
                break;
              }
              
              capability += "&mgm.access=delete";
              capability += "&mgm.manager=" ; capability += gOFS->ManagerId.c_str();
              capability += "&mgm.fsid="; 
              capability += (int) fs->GetId();
              capability += "&mgm.localprefix=";
              capability += fs->GetPath().c_str();
              capability += "&mgm.fids=";
              receiver    = fs->GetQueue().c_str();
            }
          }
          
          XrdOucString sfid="";
          XrdOucString hexfid=""; eos::common::FileId::Fid2Hex(*it,hexfid);
          idlist += hexfid;
          idlist += ",";
          
          if (ndeleted > 1024) {
            XrdOucString refcapability = capability;
            refcapability += idlist;
            XrdOucEnv incapability(refcapability.c_str());
            XrdOucEnv* capabilityenv = 0;
            eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
            
            int caprc=0;
            if ((caprc=gCapabilityEngine.Create(&incapability, capabilityenv, symkey))) {
              eos_static_err("unable to create capability - errno=%u", caprc);
            } else {
              int caplen = 0;
              msgbody += capabilityenv->Env(caplen);
              // we send deletions in bunches of max 1024 for efficiency
              message.SetBody(msgbody.c_str());
              
              if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str())) {
                eos_static_err("unable to send deletion message to %s", receiver.c_str());
              }
            }
            idlist = "";
            ndeleted = 0;
            msgbody = "mgm.cmd=drop";
            if (capabilityenv)
              delete capabilityenv;
          } 
        }

        // send the remaining ids
        if (idlist.length()) {
          XrdOucString refcapability = capability;
          refcapability += idlist;
          XrdOucEnv incapability(refcapability.c_str());
          XrdOucEnv* capabilityenv = 0;
          eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
          
          int caprc=0;
          if ((caprc=gCapabilityEngine.Create(&incapability, capabilityenv, symkey))) {
            eos_static_err("unable to create capability - errno=%u", caprc);
          } else {
            int caplen = 0;
            msgbody += capabilityenv->Env(caplen);
            // we send deletions in bunches of max 1000 for efficiency
            message.SetBody(msgbody.c_str());
            if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str())) {
              eos_static_err("unable to send deletion message to %s", receiver.c_str());
            }
          }
        }
      } catch (...) {
        eos_static_debug("nothing to delete in fs %d", fslist[i]);
      }     
      //-------------------------------------------
    }
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMgmOfs::DeleteExternal(eos::common::FileSystem::fsid_t fsid, unsigned long long fid)
{
  // -------------------------------------------------------------------------
  // send an explicit deletion message to any fsid/fid pair
  // -------------------------------------------------------------------------
  
  
  XrdMqMessage message("deletion");
  
  eos::mgm::FileSystem* fs = 0;
  XrdOucString receiver="";
  XrdOucString msgbody = "mgm.cmd=drop"; 
  XrdOucString capability ="";
  XrdOucString idlist="";

  // get the filesystem from the FS view
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    if (FsView::gFsView.mIdView.count(fsid)) {
      fs = FsView::gFsView.mIdView[fsid];
      if (fs) {
        capability += "&mgm.access=delete";
        capability += "&mgm.manager=" ; capability += gOFS->ManagerId.c_str();
        capability += "&mgm.fsid="; 
        capability += (int) fs->GetId();
        capability += "&mgm.localprefix=";
        capability += fs->GetPath().c_str();
        capability += "&mgm.fids=";
        XrdOucString hexfid=""; eos::common::FileId::Fid2Hex(fid,hexfid);
        capability += hexfid;
        receiver    = fs->GetQueue().c_str();
      }
    }
  }

  bool ok =false;

  if (fs) {
    XrdOucEnv incapability(capability.c_str());
    XrdOucEnv* capabilityenv = 0;
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
    
    int caprc=0;
    if ((caprc=gCapabilityEngine.Create(&incapability, capabilityenv, symkey))) {
      eos_static_err("unable to create capability - errno=%u", caprc);
    } else {
      int caplen = 0;
      msgbody += capabilityenv->Env(caplen);
      // we send deletions in bunches of max 1024 for efficiency
      message.SetBody(msgbody.c_str());
      if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str())) {
        eos_static_err("unable to send deletion message to %s", receiver.c_str());
      } else {
        ok = true;
      }
    }
  }  
  return ok;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::FsListener() 
{
  XrdSysTimer sleeper;
  sleeper.Snooze(5);  
  // thread listening on filesystem errors
  do {
    XrdSysTimer sleeper;
    sleeper.Snooze(1);
    gOFS->ObjectManager.SubjectsMutex.Lock(); 
    // listens on modifications on filesystem objects
    while (gOFS->ObjectManager.ModificationSubjects.size()) {
      std::string newsubject = gOFS->ObjectManager.ModificationSubjects.front();
      eos_static_debug("received modification on subject %s\n", newsubject.c_str());
      gOFS->ObjectManager.ModificationSubjects.pop_front();
      gOFS->ObjectManager.SubjectsMutex.UnLock();
      // if this is an error status on a file system, check if the filesystem is > drained state and in this case launch a drain job with
      // the opserror flag by calling StartDrainJob
      // We use directly the ObjectManager Interface because it is more handy with the available information we have at this point

      std::string key   = newsubject;
      std::string queue = newsubject;
      size_t dpos = 0;
      if ((dpos = queue.find(";"))!= std::string::npos){
        key.erase(0,dpos+1);
        queue.erase(dpos);
      }
      eos::common::FileSystem::fsid_t fsid=0;
      FileSystem* fs = 0;
      long long errc = 0;
      std::string configstatus="";
      std::string bootstatus="";
      int cfgstatus=0;
      int bstatus=0;

      // read the id from the hash and the current error value
      gOFS->ObjectManager.HashMutex.LockRead();
      XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(),"hash");
      if (hash) {
        fsid = (eos::common::FileSystem::fsid_t) hash->GetLongLong("id");
        errc = (int)hash->GetLongLong("stat.errc");
        configstatus = hash->Get("configstatus");
        bootstatus   = hash->Get("stat.boot");
        cfgstatus = eos::common::FileSystem::GetConfigStatusFromString(configstatus.c_str());
        bstatus   = eos::common::FileSystem::GetStatusFromString(bootstatus.c_str());
      }
      gOFS->ObjectManager.HashMutex.UnLockRead();

      if (fsid && errc && (cfgstatus >= eos::common::FileSystem::kRO) && (bstatus == eos::common::FileSystem::kOpsError) ) {
        // this is the case we take action and explicitly ask to start a drain job
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        if (FsView::gFsView.mIdView.count(fsid))
          fs = FsView::gFsView.mIdView[fsid];
        else 
          fs = 0;
        if (fs) {
          fs->StartDrainJob();
        }
      }
      if (fsid && (!errc)) {
        // make sure there is no drain job triggered by a previous filesystem errc!=0
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        if (FsView::gFsView.mIdView.count(fsid))
          fs = FsView::gFsView.mIdView[fsid];
        else
          fs = 0;
        if (fs) {
          fs->StopDrainJob();
        }
      }
      gOFS->ObjectManager.SubjectsMutex.Lock(); 
    }
    gOFS->ObjectManager.SubjectsMutex.UnLock();
  } while (1);
}
