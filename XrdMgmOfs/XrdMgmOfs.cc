/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonMapping.hh"
#include "XrdCommon/XrdCommonFileId.hh"
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
#include "XrdMgmOfs/XrdMgmOfsTrace.hh"
#include "XrdMgmOfs/XrdMgmOfsSecurity.hh"
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
#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif

/*----------------------------------------------------------------------------*/
XrdSysError     gMgmOfsEroute(0);  
XrdSysError    *XrdMgmOfs::eDest;
XrdOucTrace     gMgmOfsTrace(&gMgmOfsEroute);


XrdMgmOfs* gOFS=0;

/*----------------------------------------------------------------------------*/
XrdMgmOfs::XrdMgmOfs(XrdSysError *ep)
{
  eDest = ep;
  ConfigFN  = 0;  
  XrdCommonLogId();
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::Init(XrdSysError &ep)
{
  
  return true;
}

/*----------------------------------------------------------------------------*/

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

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
  if ( myFS.Configure(gMgmOfsEroute) ) return 0;

  gOFS = &myFS;

  // Initialize authorization module ServerAcc
  gOFS->CapabilityEngine = (XrdCapability*) XrdAccAuthorizeObject(lp, configfn, NULL);
  if (!gOFS->CapabilityEngine) {
    return 0;
  }

  return gOFS;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsDirectory::open(const char              *dir_path, // In
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
   const char* rpath;
   XrdSecEntity mappedclient;
   XrdOucEnv Open_Env(info);

   AUTHORIZE(client,&Open_Env,AOP_Readdir,"open directory",dir_path,error);

   XrdCommonMapping::RoleMap(client,info,mappedclient,tident, uid, gid, ruid, rgid);

// Verify that this object is not already associated with an open directory
//
   if (dh) return
	     Emsg(epname, error, EADDRINUSE, 
                             "open directory", dir_path);
   
   // Set up values for this directory object
   //
   ateof = 0;
   fname = strdup(dir_path);

   
// Open the directory and get it's id
//

   return  Emsg(epname,error,EOPNOTSUPP,"open directory",dir_path);
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
    static const char *epname = "nextEntry";
    struct dirent *rp;
    int retc;

// Lock the direcrtory and do any required tracing
//
   if (!dh) 
      {Emsg(epname,error,EBADF,"read directory",fname);
       return (const char *)0;
      }

   // Check if we are at EOF (once there we stay there)
   //
   if (ateof) return (const char *)0;
   
   Emsg(epname,error,EOPNOTSUPP,"read directory",fname);
   return (const char *)0;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfsDirectory::close()
/*
  Function: Close the directory object.

  Input:    cred       - Authentication credentials, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "closedir";
  
  // Release the handle
  //
  
  Emsg(epname, error, EOPNOTSUPP, "close directory", fname);
  return SFS_ERROR;
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfsFile::open(const char          *path,      // In
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
  
  XrdSecEntity mappedclient;

  SetLogId(logId, tident);
  eos_info("path=%s info=%s",path,info);

  //  XTRACE(open, path,"");
  
  // if the clients sends tried info, we have to dump it
  //  if (info) 
    //    XTRACE(open, info,"");

    //  ZTRACE(open, "Doing Rolemap");

  eos_debug("rolemap start");

  XrdCommonMapping::RoleMap(client,info,mappedclient,tident, uid, gid, ruid, rgid);

  SetLogId(logId, uid, gid, uid, gid, tident);

  //ZTRACE(open, "Did Rolemap");
  eos_debug("rolemap done");

  openOpaque = new XrdOucEnv(info);
  const int AMode = S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH; // 775
  char *opname;
  int aop=0;
  
  mode_t acc_mode = Mode & S_IAMB;
  int retc, open_flag = 0;
  
  int isRW = 0;
  int isRewrite = 0;
  
  int crOpts = (Mode & SFS_O_MKPTH ? XRDOSS_mkpath : 0);
  
  int rcode=SFS_ERROR;
  
  XrdOucString redirectionhost="";
  int ecode=0;
  
  //  ZTRACE(open, std::hex <<open_mode <<"-" <<std::oct <<Mode <<std::dec <<" fn = " <<path);

  eos_debug("mode=%x", open_mode);

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
  
  // proc filter
  if (XrdMgmProcInterface::IsProcAccess(path)) {
    if (!XrdMgmProcInterface::Authorize(path, info, uid, gid, client)) {
      return Emsg(epname, error, EPERM, "execute proc command - you don't have the requested permissions for that operation ", path);      
    } else {
      procCmd = new XrdMgmProcCommand();
      procCmd->SetLogId(logId,uid,gid,uid,gid, tident);
      return procCmd->open(path, info, uid, gid, &error);
    }
  }

  //  ZTRACE(open,"doing authorize");  
  eos_debug("authorize start");

  if (open_flag & O_CREAT) {
    AUTHORIZE(client,openOpaque,AOP_Create,"create",path,error);
  } else {
    AUTHORIZE(client,openOpaque,(isRW?AOP_Update:AOP_Read),"open",path,error);
  }

  //  ZTRACE(open, "Authorized");

  eos_debug("authorize done");

  redirectionhost= "localhost?";

  
  // construct capability
  
  XrdOucString capability = "";
  capability +=  "mgm.uid=";       capability+=(int)uid; 
  capability += "&mgm.gid=";       capability+=(int)gid;
  capability += "&mgm.ruid=";      capability+=(int)uid; 
  capability += "&mgm.rgid=";      capability+=(int)gid;
  capability += "&mgm.path=";      capability += path;
  capability += "&mgm.manager=";   capability += gOFS->ManagerId.c_str();
  capability += "&mgm.fid=";    XrdOucString hexfid; XrdCommonFileId::Fid2Hex(fileId,hexfid);capability += hexfid;
  capability += "&mgm.localprefix="; capability+= "/var/tmp/ost/";
  capability += "&mgm.lid=";       capability += XrdCommonLayoutId::kPlain;

  // encrypt capability
  XrdOucEnv  incapability(capability.c_str());
  XrdOucEnv* capabilityenv = NULL;
  XrdCommonSymKey* symkey = gXrdCommonSymKeyStore.GetCurrentKey();

  int caprc=0;
  if ((caprc=gCapabilityEngine.Create(&incapability, capabilityenv, symkey))) {
    return Emsg(epname, error, caprc, "sign capability", path);
  }
  
  int caplen=0;
  redirectionhost+=capabilityenv->Env(caplen);
  redirectionhost+= "&mgm.logid="; redirectionhost+=this->logId;

  
  // always redirect
  ecode = atoi(gOFS->MgmOfsTargetPort.c_str());
  rcode = SFS_REDIRECT;
  error.setErrInfo(ecode,redirectionhost.c_str());
  //  ZTRACE(open, "Return redirection " << redirectionhost.c_str() << "Targetport: " << ecode);

  eos_info("redirection=%s:%d", redirectionhost.c_str(), ecode);

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
  static const char *epname = "close";

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
  XrdSfsXferSize nbytes;
  
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
  XrdSfsXferSize nbytes;
  
  
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
int XrdMgmOfs::chmod(const char             *path,    // In
                              XrdSfsMode        Mode,    // In
                              XrdOucErrInfo    &error,   // Out
                        const XrdSecEntity *client,  // In
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
  mode_t acc_mode = Mode & S_IAMB;

  XrdOucEnv chmod_Env(info);

  XrdSecEntity mappedclient;

  XTRACE(chmod, path,"");

  AUTHORIZE(client,&chmod_Env,AOP_Chmod,"chmod",path,error);

  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);
  
  return Emsg(epname,error,EOPNOTSUPP,"chmod",path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::exists(const char                *path,        // In
                               XrdSfsFileExistence &file_exists, // Out
                               XrdOucErrInfo       &error,       // Out
                         const XrdSecEntity    *client,          // In
                         const char                *info)        // In

{
  static const char *epname = "exists";
  const char *tident = error.getErrUser();

  XrdOucEnv exists_Env(info);

  XrdSecEntity mappedclient;

  XTRACE(exists, path,"");

  AUTHORIZE(client,&exists_Env,AOP_Stat,"execute exists",path,error);

  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);
  
  return _exists(path,file_exists,error,&mappedclient,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_exists(const char                *path,        // In
                               XrdSfsFileExistence &file_exists, // Out
                               XrdOucErrInfo       &error,       // Out
                         const XrdSecEntity    *client,          // In
                         const char                *info)        // In
/*
  Function: Determine if file 'path' actually exists.

  Input:    path        - Is the fully qualified name of the file to be tested.
            file_exists - Is the address of the variable to hold the status of
                          'path' when success is returned. The values may be:
                          XrdSfsFileExistsIsDirectory - file not found but path is valid.
                          XrdSfsFileExistsIsFile      - file found.
                          XrdSfsFileExistsIsNo        - neither file nor directory.
            einfo       - Error information object holding the details.
            client      - Authentication credentials, if any.
            info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.

  Notes:    When failure occurs, 'file_exists' is not modified.
*/
{
   static const char *epname = "exists";

// Now try to find the file or directory
//
//   if (!XrdMgmOfsUFS::Statfn(path, &fstat) )
//      {     if (S_ISDIR(fstat.filemode)) file_exists=XrdSfsFileExistIsDirectory;
//       else if (S_ISREG(fstat.filemode)) file_exists=XrdSfsFileExistIsFile;
//       else                             file_exists=XrdSfsFileExistNo;
//       return SFS_OK;
//      }
//   if (serrno == ENOENT)
//      {file_exists=XrdSfsFileExistNo;
//       return SFS_OK;
//      }

// An error occured, return the error info
//
   return Emsg(epname,error,EOPNOTSUPP,"exists",path);
}


/*----------------------------------------------------------------------------*/
const char *XrdMgmOfs::getVersion() {
  static XrdOucString FullVersion = XrdVERSION;
  FullVersion += " MgmOfs "; FullVersion += PACKAGE_VERSION;
  return FullVersion.c_str();
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::mkdir(const char             *path,    // In
                              XrdSfsMode        Mode,    // In
                              XrdOucErrInfo    &error,   // Out
                        const XrdSecEntity     *client,  // In
                        const char             *info)    // In
{
   static const char *epname = "mkdir";
   const char *tident = error.getErrUser();
   XrdOucEnv mkdir_Env(info);
   
   XrdSecEntity mappedclient;

   XTRACE(mkdir, path,"");

   XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

   int r1 = _mkdir(path,Mode,error,&mappedclient,info);
   int r2 = SFS_OK;

   return (r1 | r2);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_mkdir(const char            *path,    // In
                              XrdSfsMode        Mode,    // In
                              XrdOucErrInfo    &error,   // Out
                        const XrdSecEntity     *client,  // In
                        const char             *info)    // In
/*
  Function: Create a directory entry.

  Input:    path      - Is the fully qualified name of the file to be removed.
            Mode      - Is the POSIX mode setting for the directory. If the
                        mode contains SFS_O_MKPTH, the full path is created.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "mkdir";
  mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
  const char *tident = error.getErrUser();
  
  // Create the path if it does not already exist
  //
  if (Mode & SFS_O_MKPTH) {
    char actual_path[4096], *local_path, *next_path;
    unsigned int plen;
    // Extract out the path we should make
    //
    if (!(plen = strlen(path))) return -ENOENT;
    if (plen >= sizeof(actual_path)) return -ENAMETOOLONG;
    strcpy(actual_path, path);
    if (actual_path[plen-1] == '/') actual_path[plen-1] = '\0';
    
    // Typically, the path exist. So, do a quick check before launching into it
    //
    if (!(local_path = rindex(actual_path, (int)'/'))
	||  local_path == actual_path) return 0;
    *local_path = '\0';
    //     if (XrdMgmOfsUFS::Statfn(actual_path, &buf)) {
    //       *local_path = '/';
    // Start creating directories starting with the root. Notice that we will not
    // do anything with the last component. The caller is responsible for that.
    //
    local_path = actual_path+1;
    while((next_path = index(local_path, int('/'))))
      { int rc;
      *next_path = '\0';
      //	   if ((rc=XrdMgmOfsUFS::Mkdir(actual_path,S_IRWXU)) && serrno != EEXIST)
      //	     return -serrno;
      // Set acl on directory
      //	   if (!rc) {
      //	     if (client) SETACL(actual_path,(*client),0);
      //	   }
      
      *next_path = '/';
      local_path = next_path+1;
      }
  }

  // Perform the actual creation
  //
  //  if (XrdMgmOfsUFS::Mkdir(path, acc_mode) && (serrno != EEXIST))
  //    return Emsg(epname,error,serrno,"create directory",path);
  // Set acl on directory
  //  if (client)SETACL(path,(*client),0); 
  
  // All done
  return Emsg(epname,error,EOPNOTSUPP,"mkdir",path);
}

/*----------------------------------------------------------------------------*/

int XrdMgmOfs::Mkpath(const char *path, mode_t mode, const char *info,XrdSecEntity* client, XrdOucErrInfo* error )
/*
  Function: Create a directory path

  Input:    path        - Is the fully qualified name of the new path.
            mode        - The new mode that each new directory is to have.
            info        - Opaque information, of any.

  Output:   Returns 0 upon success and -errno upon failure.
*/
{
    char actual_path[4096], *local_path, *next_path;
    unsigned int plen;
    static const char *epname = "Mkpath";

// Extract out the path we should make
//
   if (!(plen = strlen(path))) return -ENOENT;
   if (plen >= sizeof(actual_path)) return -ENAMETOOLONG;
   strcpy(actual_path, path);
   if (actual_path[plen-1] == '/') actual_path[plen-1] = '\0';

// Typically, the path exist. So, do a quick check before launching into it
//
   if (!(local_path = rindex(actual_path, (int)'/'))
   ||  local_path == actual_path) return 0;
   *local_path = '\0';

   //   if (!XrdMgmOfsUFS::Statfn(actual_path, &buf)) return 0;
   *local_path = '/';

// Start creating directories starting with the root. Notice that we will not
// do anything with the last component. The caller is responsible for that.
//
   local_path = actual_path+1;
   while((next_path = index(local_path, int('/')))) {
     *next_path = '\0';
     if (1) {
     //     if (XrdMgmOfsUFS::Statfn(actual_path, &buf)) {
       if (client && error) {
	 const char *tident = (*error).getErrUser();
	 //	 if ( (XrdxCastor2FS->_mkdir(actual_path,mode,(*error),client,info)) )
	 //	   return -serrno;
       } else {
	 //	    if (XrdMgmOfsUFS::Mkdir(actual_path,mode) && (serrno != EEXIST))
	 //	      return -serrno;
       }
     }
     // Set acl on directory
     *next_path = '/';
     local_path = next_path+1;
   }

// All done
//
   return 0;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::prepare( XrdSfsPrep       &pargs,
			   XrdOucErrInfo    &error,
			   const XrdSecEntity *client)
{
  static const char *epname = "prepare";
  const char *tident = error.getErrUser();  
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::rem(const char             *path,    // In
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
   XrdSecEntity mappedclient;

   int nkept = 0;

   XTRACE(remove, path,"");

   XrdOucEnv env(info);
   
   AUTHORIZE(client,&env,AOP_Delete,"remove",path,error);

   XTRACE(remove, path,"");

   XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

   return _rem(path,error,&mappedclient,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_rem(   const char             *path,    // In
                               XrdOucErrInfo    &error,   // Out
                         const XrdSecEntity *client,  // In
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
  // Perform the actual deletion
  //
  const char *tident = error.getErrUser();
  
  XTRACE(remove, path,"");
  
  return Emsg(epname, error, EOPNOTSUPP, "remove", path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::remdir(const char             *path,    // In
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
   XrdOucEnv remdir_Env(info);

   XrdSecEntity mappedclient;

   XTRACE(remove, path,"");

   AUTHORIZE(client,&remdir_Env,AOP_Delete,"remove",path, error);

   XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

   return _remdir(path,error,&mappedclient,info);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::_remdir(const char             *path,    // In
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
   
   return Emsg(epname, error, EOPNOTSUPP, "remove", path);
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

  XrdOucString source, destination;
  XrdOucString oldn,newn;
  XrdSecEntity mappedclient;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  
  AUTHORIZE(client,&renameo_Env,AOP_Update,"rename",old_name, error);
  AUTHORIZE(client,&renamen_Env,AOP_Update,"rename",new_name, error);

  XrdCommonMapping::RoleMap(client,infoO,mappedclient,tident,uid,gid,ruid,rgid);


  int r1,r2;
  
  r1=r2=SFS_OK;


  // check if dest is existing
  XrdSfsFileExistence file_exists;

  if (!_exists(newn.c_str(),file_exists,error,&mappedclient,infoN)) {
    // it exists
    if (file_exists == XrdSfsFileExistIsDirectory) {
      // we have to path the destination name since the target is a directory
      XrdOucString sourcebase = oldn.c_str();
      int npos = oldn.rfind("/");
      if (npos == STR_NPOS) {
	return Emsg(epname, error, EINVAL, "rename", oldn.c_str());
      }
      sourcebase.assign(oldn, npos);
      newn+= "/";
      newn+= sourcebase;
      while (newn.replace("//","/")) {};
    }
    if (file_exists == XrdSfsFileExistIsFile) {
      // remove the target file first!
      int remrc = 0;// _rem(newn.c_str(),error,&mappedclient,infoN);
      if (remrc) {
	return remrc;
      }
    }
  }

  //  r1 = XrdMgmOfsUFS::Rename(oldn.c_str(), newn.c_str());

  //  if (r1) 
  //    return Emsg(epname, error, serrno, "rename", oldn.c_str());

  return Emsg(epname, error, EOPNOTSUPP, "rename", oldn.c_str());
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::stat(const char              *path,        // In
                             struct stat       *buf,         // Out
                             XrdOucErrInfo     &error,       // Out
                       const XrdSecEntity  *client,          // In
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
  XrdSecEntity mappedclient;

  XrdOucEnv Open_Env(info);
  
  XTRACE(stat, path,"");

  AUTHORIZE(client,&Open_Env,AOP_Stat,"stat",path,error);

  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

  return Emsg(epname, error, EOPNOTSUPP, "stat", path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::lstat(const char              *path,        // In
			      struct stat       *buf,         // Out
                              XrdOucErrInfo     &error,       // Out
                            const XrdSecEntity  *client,      // In
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
  static const char *epname = "lstat";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv lstat_Env(info);

  XTRACE(stat, path,"");

  AUTHORIZE(client,&lstat_Env,AOP_Stat,"lstat",path,error);

  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

  return Emsg(epname, error, EOPNOTSUPP, "lstat", path); 
}


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::truncate(const char*, 
			   XrdSfsFileOffset, 
			   XrdOucErrInfo& error, 
			   const XrdSecEntity*, 
			   const char* path)
{
  static const char *epname = "truncate";
  return Emsg(epname, error, EOPNOTSUPP, "truncate", path);
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::readlink(const char          *path,        // In
			   XrdOucString        &linkpath,    // Out
			   XrdOucErrInfo       &error,       // Out
			   const XrdSecEntity  *client,       // In
			   const char          *info)        // In
{
  static const char *epname = "readlink";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv rl_Env(info);

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&rl_Env,AOP_Stat,"readlink",path,error);
  
  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

  return Emsg(epname, error, EOPNOTSUPP, "readlink", path); 
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::symlink(const char           *path,        // In
			  const char           *linkpath,    // In
			  XrdOucErrInfo        &error,       // Out
			  const XrdSecEntity   *client,      // In
			  const char           *info)        // In
{
  static const char *epname = "symlink";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv sl_Env(info);

  XTRACE(fsctl, path,"");

  XrdOucString source, destination;

  AUTHORIZE(client,&sl_Env,AOP_Create,"symlink",linkpath,error);
  
  // we only need to map absolut links
  source = path;

  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

  return Emsg(epname, error, EOPNOTSUPP, "symlink", path); 
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::access( const char           *path,        // In
                          int                   mode,        // In
			  XrdOucErrInfo        &error,       // Out
			  const XrdSecEntity   *client,      // In
			  const char           *info)        // In
{
  static const char *epname = "access";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv access_Env(info);

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Stat,"access",path,error);
  
  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

  return Emsg(epname, error, EOPNOTSUPP, "access", path); 
}

/*----------------------------------------------------------------------------*/
int XrdMgmOfs::utimes(  const char          *path,        // In
			   struct timeval      *tvp,         // In
			   XrdOucErrInfo       &error,       // Out
			   const XrdSecEntity  *client,       // In
			   const char          *info)        // In
{
  static const char *epname = "utimes";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv utimes_Env(info);

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&utimes_Env,AOP_Update,"set utimes",path,error);
  
  XrdCommonMapping::RoleMap(client,info,mappedclient,tident,uid,gid,ruid,rgid);

  return Emsg(epname, error, EOPNOTSUPP, "utimes", path); 
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

   eos_err(buffer);

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

   eos_err(buffer);

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

   eos_err(buffer);

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

int            
XrdMgmOfs::fsctl(const int               cmd,
		    const char             *args,
		    XrdOucErrInfo    &error,
		    const XrdSecEntity *client)
  
{  
  static const char *epname = "fsctl";
  const char *tident = error.getErrUser(); 

  XrdOucString path = args;
  XrdOucString opaque;
  opaque.assign(path,path.find("?")+1);
  path.erase(path.find("?"));

  XrdOucEnv env(opaque.c_str());
  const char* scmd;

  ZTRACE(fsctl,args);
  if ((cmd == SFS_FSCTL_LOCATE)) {
    // check if this file exists
    XrdSfsFileExistence file_exists;
    if ((_exists(path.c_str(),file_exists,error,client,0)) || (file_exists!=XrdSfsFileExistIsFile)) {
      return SFS_ERROR;
    }
    
    char locResp[4096];
    int  locRlen = 4096;
    struct stat fstat;
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';//(fstat.st_mode & S_IFBLK == S_IFBLK ? 's' : 'S');
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';//(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp,"[::%s] ",(char*)gOFS->ManagerId.c_str());
    error.setErrInfo(strlen(locResp)+3, (const char **)Resp, 2);
    ZTRACE(fsctl,"located at headnode: " << locResp);
    return SFS_DATA;
  }
  

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return SFS_ERROR;
  }


  if ((scmd = env.Get("pcmd"))) {
    XrdOucString execmd = scmd;

    if (execmd == "stat") {
      /*      struct stat buf;

      int retc = lstat(path.c_str(),
		      &buf,  
		      error, 
		      client,
		      0);
      
      if (retc == SFS_OK) {
	char statinfo[16384];
	// convert into a char stream
	sprintf(statinfo,"stat: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %u %u %u\n",
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
		(unsigned long long) buf.st_ctime);
	error.setErrInfo(strlen(statinfo)+1,statinfo);
	return SFS_DATA;
      */
    }

    if (execmd == "chmod") {
      /*
      char* smode;
      if ((smode = env.Get("mode"))) {
 	XrdSfsMode newmode = atoi(smode);
	int retc = chmod(path.c_str(),
			newmode,  
			error, 
			client,
			0);
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
      */
    }

    if (execmd == "symlink") {
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
      /*
      char* smode;
      if ((smode = env.Get("mode"))) {
	int newmode = atoi(smode);
	int retc = access(path.c_str(),newmode, error,client,0);
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
      */
    }

    if (execmd == "utimes") {
      /*
      char* tv1_sec;
      char* tv1_usec;
      char* tv2_sec;
      char* tv2_usec;

      tv1_sec  = env.Get("tv1_sec");
      tv1_usec = env.Get("tv1_usec");
      tv2_sec  = env.Get("tv2_sec");
      tv2_usec = env.Get("tv2_usec");

      struct timeval tvp[2];
      if (tv1_sec && tv1_usec && tv2_sec && tv2_usec) {
	tvp[0].tv_sec  = strtol(tv1_sec,0,10);
	tvp[0].tv_usec = strtol(tv1_usec,0,10);
	tvp[1].tv_sec  = strtol(tv2_sec,0,10);
	tvp[1].tv_usec = strtol(tv2_usec,0,10);

	int retc = utimes(path.c_str(), 
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
	error.setErrInfo(response.lngth()+1,response.c_str());
	return SFS_DATA;
      }
      */
    }

  }
  
  return SFS_ERROR;
}


/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
