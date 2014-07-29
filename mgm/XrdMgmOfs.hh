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

/*----------------------------------------------------------------------------*/
/**
 * @file   XrdMgmOfs.hh
 *
 * @brief  XRootD OFS plugin implementing meta data handling of EOS
 *
 * This class is the OFS plugin which is implemented the meta data and
 * management server part. To understand the functionality of the MGM you start
 * here. The class implements three objects, the MgmOfs object for generic
 * meta data operations, configuration and EOS thread daemon startup,
 * the MgmOfsFile class which implements operations on files - in the case of
 * an MGM this is mainly 'open' for redirection to an FST. There is one exception
 * the so called EOS 'proc' commands. Every EOS shell command is implemented as
 * an 'open for read', 'read', 'close' sequence where the open actually executes
 * an EOS shell command and the read streams the result back. For details of
 * this REST like request/response format look at the ProcInterface class and
 * the implementations in the mgm/proc directory. The XrdMgmDirectory class
 * is provided to implement the POSIX like 'open', 'readdir', 'closedir' syntax.
 * The MGM code uses mainly three global mutexes given in the order they have
 * to be used:
 * - eos::common::RWMutexXXXLock lock(FsView::gFsView.ViewMutex)  : lock 1
 * - eos::common::RWMutexXXXLock lock(Quota::gQuotaMutex)         : lock 2
 * - eos::common::RWMutexXXXLock lock(gOFS->eosViewRWMutex)       : lock 3
 * The XXX is either Read or Write depending what has to be done on the
 * objects they are protecting. The first mutex is the file system view object
 * (FsView.cc) which contains the current state of the storage
 * filesystem/node/group/space configuration. The second mutex is protecting
 * the quota configuration and scheduling. The last mutex is protecting the
 * namespace.
 * The implementation uses a bunch of convenience macros to cut the code short.
 * These macro's filter/map path names, apply redirection, stalling rules and
 * require certain authentication credentials to be able to run some function.
 * The MgmOfs functions are always split into the main entry function e.g.
 * "::access" and an internal function "::_access". The main function applies
 * typically the mentioned macros and converts the XRootD client identity object
 * into an EOS virtual identity. The interal function requires an EOS virtual
 * identity and contains the full implementation. This allows to apply
 * mapping & stall/redirection rules once and use the interval function
 * implementation from other main functions e.g. the "rename" function can use
 * the "_access" internal function to check some permissions etc.
 * The MGM run's the following sub-services
 * (implemented by objects and threaded daemons):
 * - Fsck
 * - Balancer
 * - Iostat
 * - Messaging
 * - Vst
 * - Deletion
 * - Filesystem Listener
 * - Httpd
 * - Recycler
 * - LRU
 *
 * Many functions in the MgmOfs interface take CGI parameters. The supported
 * CGI parameter are:
 * "eos.ruid" - uid role the client wants
 * "eos.rgid" - gid role the client wants
 * "eos.space" - space a user wants to use for scheduling a write
 * "eos.checksum" - checksum a file should have
 * "eos.lfn" - use this name as path name not the path parameter (used by prefix
 * redirector MGM's ...
 * "eos.bookingsize" - reserve the requested bytes in a file placement
 * "eos.cli.access=pio" - ask for a parallel open (changes the response of an open for RAIN layouts)
 * "eos.app" - set the application name reported by monitoring
 * "eos.targetsize" - expected size of a file to be uploaded
 * "eos.blockchecksum=ignore" - disable block checksum verification
 *
 * All path related functions take as parameters 'inpath' and 'ininfo'. These
 * parameters are remapped by the NAMESPACEMAP macro to path & info variables
 * which are not visible in the declaration of each function!
 */
/*----------------------------------------------------------------------------*/

#ifndef __EOSMGM_MGMOFS__HH__
#define __EOSMGM_MGMOFS__HH__

/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
#include "common/Mapping.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/GlobalConfig.hh"
#include "common/CommentLog.hh"
#include "common/LinuxStat.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mgm/ConfigEngine.hh"
#include "mgm/Stat.hh"
#include "mgm/Iostat.hh"
#include "mgm/Fsck.hh"
#include "mgm/LRU.hh"
#include "mgm/Master.hh"
#include "mgm/Egroup.hh"
#include "mgm/Recycle.hh"
#ifdef HAVE_ZMQ
#include "mgm/ZMQ.hh"
#endif
#include "mgm/Messaging.hh"
#include "mgm/VstMessaging.hh"
#include "mgm/ProcInterface.hh"
#include "mgm/http/HttpServer.hh"
#include "namespace/IView.hh"
#include "namespace/IFileMDSvc.hh"
#include "namespace/IContainerMDSvc.hh"
#include "namespace/views/HierarchicalView.hh"
#include "namespace/accounting/FileSystemView.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
#include <dirent.h>
#include "zmq.hpp"
/*----------------------------------------------------------------------------*/

USE_EOSMGMNAMESPACE

/*----------------------------------------------------------------------------*/
//! Class implementing atomic meta data commands
/*----------------------------------------------------------------------------*/
class XrdMgmOfs : public XrdSfsFileSystem, public eos::common::LogId {
  friend class XrdMgmOfsFile;
  friend class XrdMgmOfsDirectory;
  friend class ProcCommand;

public:
  // ---------------------------------------------------------------------------
  // Object Allocation Functions
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // return a MGM directory object
  // ---------------------------------------------------------------------------

  XrdSfsDirectory *
  newDir (char *user = 0, int MonID = 0);

  // ---------------------------------------------------------------------------
  // return a MGM file object
  // ---------------------------------------------------------------------------
  XrdSfsFile *
  newFile (char *user = 0, int MonID = 0);

  // ---------------------------------------------------------------------------
  // meta data functions
  // - the _XYZ functions are the internal version of XYZ when XrdSecEntity
  // - objects have been mapped to VirtuIdentity's.
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // chmod by client
  // ---------------------------------------------------------------------------
  int chmod (const char *Name,
             XrdSfsMode Mode,
             XrdOucErrInfo &out_error,
             const XrdSecEntity *client = 0,
             const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // chmod by vid
  // ---------------------------------------------------------------------------
  int _chmod (const char *Name,
              XrdSfsMode Mode,
              XrdOucErrInfo &out_error,
              eos::common::Mapping::VirtualIdentity &vid,
              const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // chown by vid
  // ---------------------------------------------------------------------------
  int _chown (const char *Name,
              uid_t uid,
              gid_t gid,
              XrdOucErrInfo &out_error,
              eos::common::Mapping::VirtualIdentity &vid,
              const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // checksum by client
  // ---------------------------------------------------------------------------
  int chksum (XrdSfsFileSystem::csFunc Func,
              const char *csName,
              const char *Path,
              XrdOucErrInfo &out_error,
              const XrdSecEntity *client = 0,
              const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // check if file exists by client
  // ---------------------------------------------------------------------------
  int exists (const char *fileName,
              XrdSfsFileExistence &exists_flag,
              XrdOucErrInfo &out_error,
              const XrdSecEntity *client = 0,
              const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // check if file exists by client bypassing authorization/mapping/bouncing
  // ---------------------------------------------------------------------------
  int _exists (const char *fileName,
               XrdSfsFileExistence &exists_flag,
               XrdOucErrInfo &out_error,
               const XrdSecEntity *client = 0,
               const char *opaque = 0);

  // ---------------------------------------------------------------------------
  //! check if file eixsts by vid
  // ---------------------------------------------------------------------------
  int
  _exists (const char *fileName,
           XrdSfsFileExistence &exists_flag,
           XrdOucErrInfo &out_error,
           eos::common::Mapping::VirtualIdentity &vid,
           const char *opaque = 0);

  enum eFSCTL {
    kFsctlMgmOfsOffset = 40000
  };

  // ---------------------------------------------------------------------------
  // EOS plugin call fan-out function
  // ---------------------------------------------------------------------------
  int FSctl (const int cmd,
             XrdSfsFSctl &args,
             XrdOucErrInfo &error,
             const XrdSecEntity *client);

  // ---------------------------------------------------------------------------
  // fsctl
  // ---------------------------------------------------------------------------
  int fsctl (const int cmd,
             const char *args,
             XrdOucErrInfo &out_error,
             const XrdSecEntity *client = 0);

  // ---------------------------------------------------------------------------
  //! get stats function (fake ok)
  // ---------------------------------------------------------------------------

  int
  getStats (char *buff, int blen)
  {
    return 0;
  }

  // ---------------------------------------------------------------------------
  // get version
  // ---------------------------------------------------------------------------
  const char *getVersion ();


  // ---------------------------------------------------------------------------
  // create directory
  // ---------------------------------------------------------------------------
  int mkdir (const char *dirName,
             XrdSfsMode Mode,
             XrdOucErrInfo &out_error,
             const XrdSecClientName *client = 0,
             const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // create directory by vid
  // ---------------------------------------------------------------------------
  int _mkdir (const char *dirName,
              XrdSfsMode Mode,
              XrdOucErrInfo &out_error,
              eos::common::Mapping::VirtualIdentity &vid,
              const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // prepare/stage function
  // ---------------------------------------------------------------------------
  int prepare (XrdSfsPrep &pargs,
               XrdOucErrInfo &out_error,
               const XrdSecEntity *client = 0);

  // ---------------------------------------------------------------------------
  // delete file
  // ---------------------------------------------------------------------------
  int rem (const char *path,
           XrdOucErrInfo &out_error,
           const XrdSecEntity *client = 0,
           const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // delete file by vid
  // ---------------------------------------------------------------------------
  int _rem (const char *path,
            XrdOucErrInfo &out_error,
            eos::common::Mapping::VirtualIdentity &vid,
            const char *opaque = 0,
            bool simulate = false,
            bool keepversion = false, 
	    bool lock_quota = true);

  // ---------------------------------------------------------------------------
  // find files internal function
  // ---------------------------------------------------------------------------
  int _find (const char *path,
             XrdOucErrInfo &out_error,
             XrdOucString &stdErr,
             eos::common::Mapping::VirtualIdentity &vid,
             std::map<std::string, std::set<std::string> > &found,
             const char* key = 0,
             const char* val = 0,
             bool nofiles = false,
             time_t millisleep = 0,
             bool nscounter = true,
	     int maxdepth = 0
             );

  // ---------------------------------------------------------------------------
  // delete dir
  // ---------------------------------------------------------------------------
  int remdir (const char *dirName,
              XrdOucErrInfo &out_error,
              const XrdSecEntity *client = 0,
              const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // delete dir by vid
  // ---------------------------------------------------------------------------
  int _remdir (const char *dirName,
               XrdOucErrInfo &out_error,
               eos::common::Mapping::VirtualIdentity &vid,
               const char *opaque = 0,
               bool simulate = false, 
	       bool lock_quota = true);

  // ---------------------------------------------------------------------------
  // rename file
  // ---------------------------------------------------------------------------
  int rename (const char *oldFileName,
              const char *newFileName,
              XrdOucErrInfo &out_error,
              const XrdSecEntity *client = 0,
              const char *opaqueO = 0,
              const char *opaqueN = 0);

  // ---------------------------------------------------------------------------
  // rename file by vid
  // ---------------------------------------------------------------------------
  int rename (const char *oldFileName,
              const char *newFileName,
              XrdOucErrInfo &out_error,
              eos::common::Mapping::VirtualIdentity &vid,
              const char *opaqueO = 0,
              const char *opaqueN = 0,
              bool overwrite = false);

  // ---------------------------------------------------------------------------
  // rename file by vid
  // ---------------------------------------------------------------------------
  int _rename (const char *oldFileName,
               const char *newFileName,
               XrdOucErrInfo &out_error,
               eos::common::Mapping::VirtualIdentity &vid,
               const char *opaqueO = 0,
               const char *opaqueN = 0,
               bool updateCTime = false,
               bool checkQuota = false,
               bool overwrite = false,
	       bool lock_quota = true);

  // ---------------------------------------------------------------------------
  // stat file
  // ---------------------------------------------------------------------------
  int stat (const char *Name,
            struct stat *buf,
            XrdOucErrInfo &out_error,
            std::string* etag,
            const XrdSecEntity *client = 0,
            const char *opaque = 0
            );

  int stat (const char *Name,
            struct stat *buf,
            XrdOucErrInfo &out_error,
            const XrdSecEntity *client = 0,
            const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // stat file by vid
  // ---------------------------------------------------------------------------
  int _stat (const char *Name,
             struct stat *buf,
             XrdOucErrInfo &out_error,
             eos::common::Mapping::VirtualIdentity &vid,
             const char *opaque = 0,
             std::string* etag = 0);


  // ---------------------------------------------------------------------------
  // stat file to retrieve mode
  // ---------------------------------------------------------------------------

  int
  stat (const char *Name,
        mode_t &mode,
        XrdOucErrInfo &out_error,
        const XrdSecEntity *client = 0,
        const char *opaque = 0)
  {
    struct stat bfr;
    int rc = stat(Name, &bfr, out_error, client, opaque);
    if (!rc) mode = bfr.st_mode;
    return rc;
  }

  // ---------------------------------------------------------------------------
  // stat link
  // ---------------------------------------------------------------------------
  int lstat (const char *Name,
             struct stat *buf,
             XrdOucErrInfo &out_error,
             const XrdSecEntity *client = 0,
             const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // truncate a file (not supported)
  // ---------------------------------------------------------------------------
  int truncate (const char*, XrdSfsFileOffset, XrdOucErrInfo&, const XrdSecEntity*, const char*);

  // ---------------------------------------------------------------------------
  // check access permissions
  // ---------------------------------------------------------------------------
  int access (const char*, int mode, XrdOucErrInfo&, const XrdSecEntity*, const char*);

  // ---------------------------------------------------------------------------
  // check access permissions by vid
  // ---------------------------------------------------------------------------
  int _access (const char*, int mode, XrdOucErrInfo&, eos::common::Mapping::VirtualIdentity &vid, const char*);

  // ---------------------------------------------------------------------------
  // set utimes
  // ---------------------------------------------------------------------------
  int utimes (const char*, struct timespec *tvp, XrdOucErrInfo&, const XrdSecEntity*, const char*);
  // ---------------------------------------------------------------------------
  // set utimes by vid
  // ---------------------------------------------------------------------------
  int _utimes (const char*, struct timespec *tvp, XrdOucErrInfo&, eos::common::Mapping::VirtualIdentity &vid, const char* opaque = 0);

  // ---------------------------------------------------------------------------
  // touch a file
  // ---------------------------------------------------------------------------
  int _touch (const char *path,
              XrdOucErrInfo &error,
              eos::common::Mapping::VirtualIdentity &vid,
              const char *ininfo = 0);

  // ---------------------------------------------------------------------------
  // list extended attributes of a directory
  // ---------------------------------------------------------------------------
  int attr_ls (const char *path,
               XrdOucErrInfo &out_error,
               const XrdSecEntity *client,
               const char *opaque,
               eos::ContainerMD::XAttrMap &map);

  // ---------------------------------------------------------------------------
  // set extended attribute of a directory
  // ---------------------------------------------------------------------------
  int attr_set (const char *path,
                XrdOucErrInfo &out_error,
                const XrdSecEntity *client,
                const char *opaque,
                const char *key,
                const char *value);

  // ---------------------------------------------------------------------------
  // get extended attribute of a directory
  // ---------------------------------------------------------------------------
  int attr_get (const char *path,
                XrdOucErrInfo &out_error,
                const XrdSecEntity *client,
                const char *opaque,
                const char *key,
                XrdOucString &value);

  // ---------------------------------------------------------------------------
  // remove extended attribute of a directory
  // ---------------------------------------------------------------------------
  int attr_rem (const char *path,
                XrdOucErrInfo &out_error,
                const XrdSecEntity *client,
                const char *opaque,
                const char *key);

  // ---------------------------------------------------------------------------
  // list extended attributes by vid
  // ---------------------------------------------------------------------------
  int _attr_ls (const char *path,
                XrdOucErrInfo &out_error,
                eos::common::Mapping::VirtualIdentity &vid,
                const char *opaque,
                eos::ContainerMD::XAttrMap &map,
		bool lock=true);

  // ---------------------------------------------------------------------------
  // set extended attribute by vid
  // ---------------------------------------------------------------------------
  int _attr_set (const char *path,
                 XrdOucErrInfo &out_error,
                 eos::common::Mapping::VirtualIdentity &vid,
                 const char *opaque,
                 const char *key,
                 const char *value);

  // ---------------------------------------------------------------------------
  // get extended attribute by vid
  // ---------------------------------------------------------------------------
  int _attr_get (const char *path,
                 XrdOucErrInfo &out_error,
                 eos::common::Mapping::VirtualIdentity &vid,
                 const char *opaque,
                 const char *key,
                 XrdOucString &value,
                 bool islocked = false);

  // ---------------------------------------------------------------------------
  // remove extended attribute by vid
  // ---------------------------------------------------------------------------
  int _attr_rem (const char *path,
                 XrdOucErrInfo &out_error,
                 eos::common::Mapping::VirtualIdentity &vid,
                 const char *opaque,
                 const char *key);

  // ---------------------------------------------------------------------------
  // clear all extended attributes by vid
  // ---------------------------------------------------------------------------

  int _attr_clear (const char *path,
                   XrdOucErrInfo &out_error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *opaque);

  // ---------------------------------------------------------------------------
  // drop stripe by vid
  // ---------------------------------------------------------------------------
  int _dropstripe (const char *path,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   unsigned long fsid,
                   bool forceRemove = false);

  // ---------------------------------------------------------------------------
  // verify stripe by vid
  // ---------------------------------------------------------------------------
  int _verifystripe (const char *path,
                     XrdOucErrInfo &error,
                     eos::common::Mapping::VirtualIdentity &vid,
                     unsigned long fsid,
                     XrdOucString options);

  // ---------------------------------------------------------------------------
  // move stripe by vid
  // ---------------------------------------------------------------------------
  int _movestripe (const char *path,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   unsigned long sourcefsid,
                   unsigned long targetfsid,
                   bool expressflag = false);

  // ---------------------------------------------------------------------------
  // copy stripe by vid
  // ---------------------------------------------------------------------------
  int _copystripe (const char *path,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   unsigned long sourcefsid,
                   unsigned long targetfsid,
                   bool expressflag = false);

  // ---------------------------------------------------------------------------
  // replicate stripe by vid
  // ---------------------------------------------------------------------------
  int _replicatestripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        unsigned long sourcefsid,
                        unsigned long targetfsid,
                        bool dropstripe = false,
                        bool expressflag = false);

  // ---------------------------------------------------------------------------
  // replicate stripe providing file meta data by vid
  // ---------------------------------------------------------------------------
  int _replicatestripe (eos::FileMD* fmd,
                        const char* path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        unsigned long sourcefsid,
                        unsigned long targetfsid,
                        bool dropstripe = false,
                        bool expressflag = false);

  // ---------------------------------------------------------------------------
  // merge one file into another one (used by conversion)
  // ---------------------------------------------------------------------------

  int merge (const char* src_path,
             const char* dst_path,
             XrdOucErrInfo &error,
             eos::common::Mapping::VirtualIdentity &vid
             );

  // ---------------------------------------------------------------------------
  // create a versioned file
  // ---------------------------------------------------------------------------

  int Version (eos::common::FileId::fileid_t fileid,
               XrdOucErrInfo &error,
               eos::common::Mapping::VirtualIdentity &vid,
               int max_versions,
               XrdOucString* versionedname = 0,
               bool simulate = false);

  // ---------------------------------------------------------------------------
  // purge versioned files to max_versions
  // ---------------------------------------------------------------------------

  int PurgeVersion (const char* versiondir,
                    XrdOucErrInfo &error,
                    int max_versions);

  // ---------------------------------------------------------------------------
  // send resync command to a file system
  // ---------------------------------------------------------------------------
  int SendResync (eos::common::FileId::fileid_t fid,
                  eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  // static Mkpath is not supported
  // ---------------------------------------------------------------------------

  static int
  Mkpath (const char *path,
          mode_t mode,
          const char *info = 0,
          XrdSecEntity* client = 0,
          XrdOucErrInfo* error = 0)
  {
    return SFS_ERROR;
  }

  // ---------------------------------------------------------------------------
  // make a file sharing path with signature
  // ---------------------------------------------------------------------------

  std::string CreateSharePath (const char* path,
                               const char* info,
                               time_t expires,
                               XrdOucErrInfo &error,
                               eos::common::Mapping::VirtualIdentity &vid);

  // ---------------------------------------------------------------------------
  // verify a file sharing path with signature
  // ---------------------------------------------------------------------------

  bool VerifySharePath (const char* path,
                        XrdOucEnv* opaque);


  // ---------------------------------------------------------------------------
  // create Ofs error messsage
  // ---------------------------------------------------------------------------
  int Emsg (const char *, XrdOucErrInfo&, int, const char *x,
            const char *y = "");

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  XrdMgmOfs (XrdSysError *lp);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  virtual
  ~XrdMgmOfs ()
  {
  }

  // ---------------------------------------------------------------------------
  // Configuration routine
  // ---------------------------------------------------------------------------
  virtual int Configure (XrdSysError &);

  // ---------------------------------------------------------------------------
  // Namespace file view initialization thread start function
  // ---------------------------------------------------------------------------
  static void* StaticInitializeFileView (void* arg);

  // ---------------------------------------------------------------------------
  // Namepsace file view boot function
  // ---------------------------------------------------------------------------
  void* InitializeFileView ();

  // ---------------------------------------------------------------------------
  // Signal handler thread start function
  // ---------------------------------------------------------------------------
  static void* StaticSignalHandlerThread (void* arg);

  // ---------------------------------------------------------------------------
  // Signal handler thread function
  // ---------------------------------------------------------------------------
  void* SignalHandlerThread ();


  // ---------------------------------------------------------------------------
  // Initialization function
  // ---------------------------------------------------------------------------
  virtual bool Init (XrdSysError &);

  // ---------------------------------------------------------------------------
  // Create Stall response
  // ---------------------------------------------------------------------------
  int Stall (XrdOucErrInfo &error, int stime, const char *msg);

  // ---------------------------------------------------------------------------
  // Create Redirection response
  // ---------------------------------------------------------------------------
  int Redirect (XrdOucErrInfo &error, const char* host, int &port);

  // ---------------------------------------------------------------------------
  // Test if a client needs to be stalled
  // ---------------------------------------------------------------------------
  bool ShouldStall (const char* function,
                    int accessmode,
                    eos::common::Mapping::VirtualIdentity &vid,
                    int &stalltime, XrdOucString &stallmsg);

  // ---------------------------------------------------------------------------
  // Test if a  client should be redirected
  // ---------------------------------------------------------------------------
  bool ShouldRedirect (const char* function,
                       int accessmode,
                       eos::common::Mapping::VirtualIdentity &vid,
                       XrdOucString &host,
                       int &port);

  // ---------------------------------------------------------------------------
  // Test for a stall rule
  // ---------------------------------------------------------------------------
  bool HasStall (const char* path,
                 const char* rule,
                 int &stalltime,
                 XrdOucString &stallmsg);

  // ---------------------------------------------------------------------------
  // Test for a redirection rule
  // ---------------------------------------------------------------------------
  bool HasRedirect (const char* path,
                    const char* rule,
                    XrdOucString &host,
                    int &port);

  // ---------------------------------------------------------------------------
  // Update emulated in-memory directory modification time with the current time
  // ---------------------------------------------------------------------------
  void UpdateNowInmemoryDirectoryModificationTime (eos::ContainerMD::id_t id);

  // ---------------------------------------------------------------------------
  // Update emulated in-memory directory modification time with a given time
  // ---------------------------------------------------------------------------
  void UpdateInmemoryDirectoryModificationTime (eos::ContainerMD::id_t id,
                                                eos::ContainerMD::ctime_t &ctime);

  // ---------------------------------------------------------------------------
  // Retrieve a mapping for a given path
  // ---------------------------------------------------------------------------
  void PathRemap (const char* inpath, XrdOucString &outpath); // global namespace remapping

  // ---------------------------------------------------------------------------
  // Add a path mapping rule
  // ---------------------------------------------------------------------------
  bool AddPathMap (const char* source, const char* target); // add a mapping to the path map

  // ---------------------------------------------------------------------------
  // Reset path mapping
  // ---------------------------------------------------------------------------
  void ResetPathMap (); // reset/empty the path map

  // ---------------------------------------------------------------------------
  // Send an explicit deletion message to any fsid/fid pair
  // ---------------------------------------------------------------------------
  bool DeleteExternal (eos::common::FileSystem::fsid_t fsid,
                       unsigned long long fid);

  // ---------------------------------------------------------------------------
  // Statistics circular buffer thread startup function
  // ---------------------------------------------------------------------------
  static void* StartMgmStats (void *pp);

  // ---------------------------------------------------------------------------
  // Filesystem error/config listener thread startup function
  // ---------------------------------------------------------------------------
  static void* StartMgmFsConfigListener (void *pp);

  // ---------------------------------------------------------------------------
  // Filesystem error and configuration change listener thread function
  // ---------------------------------------------------------------------------
  void FsConfigListener ();

  // ---------------------------------------------------------------------------
  // configuration variables
  // ---------------------------------------------------------------------------
  char *ConfigFN; //< name of the configuration file

  ConfigEngine* ConfEngine; //< storing/restoring configuration

  XrdCapability* CapabilityEngine; //< authorization module for token encryption/decryption

  XrdOucString MgmOfsBroker; //< Url of the message broker without MGM subject
  XrdOucString MgmOfsBrokerUrl; //< Url of the message broker with MGM subject
  XrdOucString MgmOfsVstBrokerUrl; //< Url of the message broker
  XrdOucString MgmArchiveDstUrl; ///< URL where all archives are saved
  Messaging* MgmOfsMessaging; //< messaging interface class
  VstMessaging* MgmOfsVstMessaging; //< admin messaging interface class
  XrdOucString MgmDefaultReceiverQueue; //< Queue where we are sending to by default
  XrdOucString MgmOfsName; //< mount point of the filesystem
  XrdOucString MgmOfsAlias; //< alias of this MGM instance
  XrdOucString MgmOfsTargetPort; //< xrootd port where redirections go on the OSTs -default is 1094
  XrdOucString MgmOfsQueue; //< our mgm queue name
  XrdOucString MgmOfsInstanceName; //< name of the EOS instance
  XrdOucString MgmConfigDir; //< Directory where config files are stored
  XrdOucString MgmConfigAutoLoad; //< Name of the automatically loaded configuration file
  XrdOucString MgmProcPath; //< Directory with proc files
  XrdOucString MgmProcConversionPath; //< Directory with conversion files (used as temporary files when a layout is changed using third party copy)
  XrdOucString MgmProcMasterPath; //< Full path to the master indication proc file
  XrdOucString MgmProcArchivePath; ///< Directory where archive dir inodes are
                                   ///< saved for fast find functionality
  XrdOucString AuthLib; //< path to a possible authorizationn library
  XrdOucString MgmNsFileChangeLogFile; //< path to namespace changelog file for files
  XrdOucString MgmNsDirChangeLogFile; //< path to namespace changelog file for directories
  XrdOucString MgmConfigQueue; //< name of the mgm-wide broadcasted shared hash
  XrdOucString AllConfigQueue; //< name of the cluster-wide broadcasted shared hash
  XrdOucString FstConfigQueue; //< name of the fst-wide broadcasted shared hash

  XrdOucString SpaceConfigQueuePrefix; //< name of the prefix for space configuration
  XrdOucString NodeConfigQueuePrefix; //< name of the prefix for node configuration
  XrdOucString GroupConfigQueuePrefix; //< name of the prefix for group configuration

  XrdOucString MgmTxDir; //<  Directory containing the transfer database and archive
  XrdOucString MgmAuthDir; //< Directory containing exported authentication token

  XrdOucString ManagerId; //< manager id in <host>:<port> format
  XrdOucString ManagerIp; //< manager ip in <xxx.yyy.zzz.vvv> format
  int ManagerPort; //< manager port as number e.g. 1094

  eos::common::LinuxStat::linux_stat_t LinuxStatsStartup; // => process state after namespace load time

  time_t StartTime; //< out starttime
  char* HostName; //< our hostname as derived in XrdOfs
  char* HostPref; //< our hostname as derived in XrdOfs without domain

  static XrdSysError *eDest; //< error routing object

#ifdef HAVE_ZMQ
  ZMQ* zMQ; //<  ZMQ processor
#endif

  // ---------------------------------------------------------------------------
  // namespace specific variables
  // ---------------------------------------------------------------------------

  enum eNamespace {
    kDown = 0, kBooting = 1, kBooted = 2, kFailed = 3, kCompacting = 4
  };

  int Initialized; //< indicating the initialization state of the namespace with the above enum
  time_t InitializationTime; //< time of the initialization
  XrdSysMutex InitializationMutex; //< mutex protecting above variables
  bool Shutdown; //< true if the shutdown function was called => avoid to join some threads
  bool RemoveStallRuleAfterBoot; //< indicates that after a boot there shouldn't be a stall rule for all alias '*'
  static const char* gNameSpaceState[]; //< const strings to print the namespace boot state as in eNamespace

  // ---------------------------------------------------------------------------
  // state variables
  // ---------------------------------------------------------------------------

  bool IsReadOnly; //< true if this is a read-only redirector
  bool IsRedirect; //< true if the Redirect function should be called to redirect
  bool IsStall; //< true if the Stall function should be called to send a wait
  bool IsWriteStall; //< true if the Stall function should be called to send a wait to everything doing 'writes'
  bool authorize; //< determins if the autorization should be applied or not
  bool IssueCapability; //< defines if the Mgm issues capabilities
  bool MgmRedirector; //<  Act's only as a redirector, disables many components in the MGM
  bool ErrorLog; //<  Mgm writes error log with cluster collected file into /var/log/eos/error.log if <true>

  // ---------------------------------------------------------------------------
  // namespace variables
  // ---------------------------------------------------------------------------
  eos::ChangeLogContainerMDSvc *eosDirectoryService; //< changelog for directories
  eos::ChangeLogFileMDSvc *eosFileService; //< changelog for files
  eos::IView *eosView; //< hierarchical view of the namespace
  eos::FileSystemView *eosFsView; //< filesystem view of the namespace
  XrdSysMutex eosViewMutex; //< mutex making the namespace single threaded
  eos::common::RWMutex eosViewRWMutex; //< rw namespace mutex
  XrdOucString MgmMetaLogDir; //  Directory containing the meta data (change) log files

  // ---------------------------------------------------------------------------
  // thread variables
  // ---------------------------------------------------------------------------
  pthread_t deletion_tid; //< Thead Id of the deletion thread
  pthread_t stats_tid; //< Thread Id of the stats thread
  pthread_t fsconfiglistener_tid; //< Thread ID of the fs listener/config change thread

  // ---------------------------------------------------------------------------
  // class objects
  // ---------------------------------------------------------------------------
  XrdAccAuthorize *Authorization; //< Authorization   Service

  Stat MgmStats; //<  Mgm Namespace Statistics

  Iostat IoStats; //<  Mgm IO Statistics

  XrdOucString IoReportStorePath; //<  Mgm IO Report store path by default is /var/tmp/eos/report

  eos::common::CommentLog* commentLog; //<  Class implementing comment log: mgm writes all proc commands with a comment into /var/log/eos/comments.log

  Fsck FsCheck; //<  Class checking the filesystem
  google::sparse_hash_map<unsigned long long, time_t> MgmHealMap; //< map remembering 'healing' inodes

  XrdSysMutex MgmHealMapMutex; //< mutex protecting the help map

  Master MgmMaster; //<  Master/Slave configuration/failover class

  std::map<eos::common::FileSystem::fsid_t, time_t> DumpmdTimeMap; //< this map stores the last time of a filesystem dump, this information is used to track filesystems which have not been checked decentral by an FST. It is filled in the 'dumpmd' function definde in Procinterface

  XrdSysMutex DumpmdTimeMapMutex; //< mutex protecting the 'dumpmd' time

  eos::common::RWMutex PathMapMutex; //< mutex protecting the path map

  std::map<std::string, std::string> PathMap; //< containing global path remapping

  XrdMqSharedObjectManager ObjectManager; //< Shared Hash/Queue ObjectManager

  // map keeping the modification times of directories, they are either directly inserted from directory/file creation or they are set from a directory listing
  XrdSysMutex MgmDirectoryModificationTimeMutex; //<  mutex protecting Directory Modificatino Time map MgmDirectoryModificationTime

  google::sparse_hash_map<unsigned long long, struct timespec> MgmDirectoryModificationTime;

  HttpServer Httpd; //<  Http daemon if available

  LRU LRUd; //< LRU object running the LRU policy engine

  Egroup EgroupRefresh; //<  Egroup refresh object running asynchronous Egroup fetch thread

  Recycle Recycler; //<  Recycle object running the recycle bin deletion thread

  bool UTF8; //< true if running in less restrictive character set mode

  static const std::string msArchiveFname; ///< archive filename
  static const std::string msArchiveEndpoint; ///< archive connection endpoint
  
private:

  eos::common::Mapping::VirtualIdentity vid; //< virtual identity
};
/*----------------------------------------------------------------------------*/
extern XrdMgmOfs* gOFS; //< global handle to XrdMgmOfs object
/*----------------------------------------------------------------------------*/

#endif
