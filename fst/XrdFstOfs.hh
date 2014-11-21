// ----------------------------------------------------------------------
// File: XrdFstOfs.hh
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

#ifndef __XRDFSTOFS_FSTOFS_HH__
#define __XRDFSTOFS_FSTOFS_HH__

/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfsFile.hh"
#include "authz/XrdCapability.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/StringConversion.hh"
#include "fst/Namespace.hh"
#include "fst/storage/Storage.hh"
#include "fst/Config.hh"
#include "fst/Messaging.hh"
#include "fst/http/HttpServer.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"

/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "Xrd/XrdScheduler.hh"
/*----------------------------------------------------------------------------*/
#include <sys/mman.h>
#include <queue>
#include <fts.h>
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Apple does not know these errno's
//------------------------------------------------------------------------------
#ifdef __APPLE__
#define EBADE 52
#define EBADR 53
#define EADV 68
#define EREMOTEIO 121
#define ENOKEY 126
#endif

EOSFSTNAMESPACE_BEGIN

//! Forward declarations
class ReplicaParLayout;
class RaidMetaLayout;

//------------------------------------------------------------------------------
//! Class XrdFstOfs
//------------------------------------------------------------------------------
class XrdFstOfs : public XrdOfs, public eos::common::LogId
{
  friend class XrdFstOfsFile;
  friend class ReplicaParLayout;
  friend class RaidMetaLayout;

public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdFstOfs ();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdFstOfs ();


  //----------------------------------------------------------------------------
  //! Get new OFS directory object
  //!
  //! @param user User information
  //! @param MonID Monitoring ID
  //!
  //! @return OFS directory object (NULL)
  //----------------------------------------------------------------------------
  XrdSfsDirectory* newDir (char* user = 0, int MonID = 0);


  //----------------------------------------------------------------------------
  //! Get new OFS file object
  //!
  //! @param user User information
  //! @param MonID Monitoring ID
  //!
  //! @return OFS file object
  //----------------------------------------------------------------------------
  XrdSfsFile* newFile (char* user = 0, int MonID = 0);


  //----------------------------------------------------------------------------
  //! Configure OFS layer by parsing the info in the configuration file
  //!
  //! @param error Error object
  //----------------------------------------------------------------------------
  int Configure (XrdSysError& error);


  //----------------------------------------------------------------------------
  //! Query file system info
  //----------------------------------------------------------------------------
  int fsctl (const int cmd,
             const char* args,
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client);

  //----------------------------------------------------------------------------
  //         ****** Here we mask all illegal operations ******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Return memory mapping for file, if any.
  //!
  //! @param Addr Address of memory location
  //! @param Size Size of the file or zero if not memory mapped.
  //!
  //! @return  Returns SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int
  getMap(void **Addr, off_t &Size)
  {
    return SFS_OK;
  }


  //----------------------------------------------------------------------------
  //! Chmod
  //----------------------------------------------------------------------------
  int
  chmod (const char* path,
         XrdSfsMode Mode,
         XrdOucErrInfo& out_error,
         const XrdSecEntity* client,
         const char* opaque = 0)
  {
    EPNAME("chmod");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! File exists
  //----------------------------------------------------------------------------
  int
  exists (const char* path,
          XrdSfsFileExistence& exists_flag,
          XrdOucErrInfo& out_error,
          const XrdSecEntity* client,
          const char* opaque = 0)
  {
    EPNAME("exists");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! Mkdir
  //----------------------------------------------------------------------------
  int mkdir (const char* path,
             XrdSfsMode Mode,
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client,
             const char* opaque = 0)
  {
    EPNAME("mkdir");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! Prepare request
  //----------------------------------------------------------------------------
  int
  prepare (XrdSfsPrep& pargs,
           XrdOucErrInfo& out_error,
           const XrdSecEntity* client = 0)
  {
    EPNAME("prepare");
    return Emsg(epname, out_error, ENOSYS, epname);
  }


  //----------------------------------------------------------------------------
  //! Remove directory
  //----------------------------------------------------------------------------
  int remdir (const char* path,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client,
              const char* info = 0)
  {
    EPNAME("remdir");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! Rename
  //----------------------------------------------------------------------------
  int rename (const char* oldFileName,
          const char* newFileName,
          XrdOucErrInfo& out_error,
          const XrdSecEntity* client,
          const char* infoO = 0,
          const char* infoN = 0)
  {
    EPNAME("rename");
    return Emsg(epname, out_error, ENOSYS, epname, oldFileName);
  }


  //----------------------------------------------------------------------------
  //! Remove path - interface function
  //----------------------------------------------------------------------------
  int rem (const char* path,
           XrdOucErrInfo& out_error,
           const XrdSecEntity* client,
           const char* info = 0);


  //----------------------------------------------------------------------------
  //! Remove path - low-level function
  //----------------------------------------------------------------------------
  int _rem (const char* path,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client,
            XrdOucEnv* info = 0,
            const char* fstPath = 0,
            unsigned long long fid = 0,
            unsigned long fsid = 0,
            bool ignoreifnotexist = false);


  //----------------------------------------------------------------------------
  //! Get checksum - we publish checksums at the MGM
  //! @brief retrieve a checksum
  //!
  //! @param func function to be performed 'csCalc','csGet' or 'csSize'
  //! @param csName name of the checksum
  //! @param error error object
  //! @param client XRootD authentication object
  //! @param ininfo CGI
  //!
  //! @return SFS_OK on success otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int chksum (XrdSfsFileSystem::csFunc Func,
              const char *csName,
              const char *Path,
              XrdOucErrInfo &out_error,
              const XrdSecEntity *client = 0,
              const char *opaque = 0);


  //----------------------------------------------------------------------------
  //! Stat path
  //----------------------------------------------------------------------------
  int stat (const char* path,
            struct stat* buf,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client,
            const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Call back to MGM node
  //----------------------------------------------------------------------------
  int CallManager (XrdOucErrInfo* error,
                   const char* path,
                   const char* manager,
                   XrdOucString& capOpaqueFile,
                   XrdOucString* return_result = 0,
                   unsigned short timeout=0);


  //----------------------------------------------------------------------------
  //! Function dealing with plugin calls
  //----------------------------------------------------------------------------
  int FSctl (int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);


  //----------------------------------------------------------------------------
  //! Allows to switch on error simulation in the OfsFile stack
  //!
  //! @param tag
  //----------------------------------------------------------------------------
  void SetSimulationError (const char* tag);

  void SetDebug (XrdOucEnv& env);

  void SendRtLog (XrdMqMessage* message);

  void SendFsck (XrdMqMessage* message);

  int Stall (XrdOucErrInfo& error, int stime, const char* msg);

  int Redirect (XrdOucErrInfo& error, const char* host, int& port);

  void OpenFidString (unsigned long fsid, XrdOucString& outstring);

  static void xrdfstofs_shutdown (int sig);

  static void xrdfstofs_stacktrace (int sig);

  XrdSysError* Eroute;
  eos::fst::Messaging* Messaging; ///< messaging interface class
  eos::fst::Storage* Storage; ///< Meta data & filesytem store object

  XrdSysMutex OpenFidMutex;

  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
                          google::sparse_hash_map<unsigned long long, unsigned int> > WOpenFid;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
                          google::sparse_hash_map<unsigned long long, unsigned int> > ROpenFid;

  XrdSysMutex XSLockFidMutex;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
                          google::sparse_hash_map<unsigned long long, unsigned int> > XSLockFid;

  //----------------------------------------------------------------------------
  //! Information saved for TPC transfers
  //----------------------------------------------------------------------------
  struct TpcInfo
  {
    std::string path;
    std::string opaque;
    std::string capability;
    std::string key;
    std::string src;
    std::string dst;
    std::string org;
    std::string lfn;
    time_t expires;
  };

  XrdSysMutex TpcMapMutex; ///< mutex protecting a Tpc Map

  //! A vector map pointing from tpc key => tpc information for reads, [0]
  //! are readers [1] are writers
  std::vector<google::sparse_hash_map<std::string, struct TpcInfo >> TpcMap;

  XrdSysMutex ReportQueueMutex;

  //! Queue where file transaction reports get stored and picked up by a thread
  //!running in Storage
  std::queue <XrdOucString> ReportQueue;

  XrdSysMutex ErrorReportQueueMutex;
  //! Queue where log error are stored and picked up by a thread running in Storage
  std::queue <XrdOucString> ErrorReportQueue;

  XrdSysMutex WrittenFilesQueueMutex;

  //! Queue where modified/written files get stored and picked up by a thread
  //! running in Storage
  std::queue<struct Fmd> WrittenFilesQueue;

  XrdMqSharedObjectManager ObjectManager; ///< managing shared objects
  XrdMqSharedObjectChangeNotifier ObjectNotifier; ///< notifying any shared object changes;
  XrdScheduler* TransferScheduler; ///< TransferScheduler
  XrdSysMutex TransferSchedulerMutex; ///< protecting the TransferScheduler

  bool Simulate_IO_read_error; ///< simulate an IO error on read
  bool Simulate_IO_write_error; ///< simulate an IO error on write
  bool Simulate_XS_read_error; ///< simulate a checksum error on read
  bool Simulate_XS_write_error; ///< simulate a checksum error on write

  static XrdSysMutex ShutdownMutex; ///< protecting Shutdown variable
  static bool Shutdown; ///< indicating if a shutdown procedure is running

  HttpServer* httpd; ///< embedded http server
  const char* mHostName; ///< FST hostname

 private:

};

//------------------------------------------------------------------------------
//! Global FST OFS handle
//------------------------------------------------------------------------------
extern XrdFstOfs gOFS;

EOSFSTNAMESPACE_END
#endif
