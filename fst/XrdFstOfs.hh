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

#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/Config.hh"
#include "common/Logging.hh"
#include "fst/FmdDbMap.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include <sys/mman.h>
#include <queue>
#include <memory>
#include <chrono>
#ifdef HAVE_FST_WITH_QUARKDB
#include "qclient/QClient.hh"
#endif

//------------------------------------------------------------------------------
//! Apple does not know these errnos
//------------------------------------------------------------------------------
#ifdef __APPLE__
#define EBADE 52
#define EBADR 53
#define EADV 68
#define EREMOTEIO 121
#define ENOKEY 126
#endif

class XrdOucEnv;
class XrdScheduler;

EOSFSTNAMESPACE_BEGIN

// Forward declarations
class ReplicaParLayout;
class RaidMetaLayout;
class HttpServer;
class Storage;
class Messaging;

//------------------------------------------------------------------------------
//! Class XrdFstOfs
//------------------------------------------------------------------------------
class XrdFstOfs : public XrdOfs, public eos::common::LogId
{
  friend class XrdFstOfsFile;
  friend class ReplicaParLayout;
  friend class RaidMetaLayout;

public:
  static XrdSysMutex sShutdownMutex; ///< Protecting Shutdown variable
  static bool sShutdown; ///< True if shutdown procedure is running

  //----------------------------------------------------------------------------
  //! FST shutdown procedure
  //!
  //! @param sig
  //----------------------------------------------------------------------------
  static void xrdfstofs_shutdown(int sig);

  //----------------------------------------------------------------------------
  //! Get stacktrace from crashing process
  //!
  //! @param sig
  //----------------------------------------------------------------------------
  static void xrdfstofs_stacktrace(int sig);

  //----------------------------------------------------------------------------
  //! FST "graceful" shutdown procedure. The FST will wait for a configurable
  //! amount of time for readers and writers before shutting down.
  //!
  //! @param sig signal to trigger this handler
  //----------------------------------------------------------------------------
  static void xrdfstofs_graceful_shutdown(int sig);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdFstOfs();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdFstOfs();

  //----------------------------------------------------------------------------
  //! Get new OFS directory object
  //!
  //! @param user User information
  //! @param MonID Monitoring ID
  //!
  //! @return OFS directory object (NULL)
  //----------------------------------------------------------------------------
  XrdSfsDirectory* newDir(char* user = 0, int MonID = 0);

  //----------------------------------------------------------------------------
  //! Get new OFS file object
  //!
  //! @param user User information
  //! @param MonID Monitoring ID
  //!
  //! @return OFS file object
  //----------------------------------------------------------------------------
  XrdSfsFile* newFile(char* user = 0, int MonID = 0);

  //----------------------------------------------------------------------------
  //! Configure OFS object
  //!
  //! @param error error output object
  //! @param envP environment for the configuration
  //!
  //! @return 0 if successful, otherwise -1
  //----------------------------------------------------------------------------
  int Configure(XrdSysError& error, XrdOucEnv* envP);

  //----------------------------------------------------------------------------
  //! fsctl command
  //----------------------------------------------------------------------------
  int fsctl(const int cmd, const char* args, XrdOucErrInfo& out_error,
            const XrdSecEntity* client);

  //----------------------------------------------------------------------------
  //         ****** Here we mask all illegal operations ******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Return memory mapping for file, if any.
  //!
  //! @param Addr Address of memory location
  //! @param Size Size of the file or zero if not memory mapped.
  //----------------------------------------------------------------------------
  int
  getMap(void** Addr, off_t& Size)
  {
    return SFS_OK;
  }

  //----------------------------------------------------------------------------
  //! Chmod
  //----------------------------------------------------------------------------
  int
  chmod(const char* path,
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
  exists(const char* path,
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
  int mkdir(const char* path,
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
  prepare(XrdSfsPrep& pargs,
          XrdOucErrInfo& out_error,
          const XrdSecEntity* client = 0)
  {
    EPNAME("prepare");
    return Emsg(epname, out_error, ENOSYS, epname);
  }

  //----------------------------------------------------------------------------
  //! Remove directory
  //----------------------------------------------------------------------------
  int remdir(const char* path,
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
  int rename(const char* oldFileName,
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
  int rem(const char* path,
          XrdOucErrInfo& out_error,
          const XrdSecEntity* client,
          const char* info = 0);

  //----------------------------------------------------------------------------
  //! Remove path - low-level function
  //----------------------------------------------------------------------------
  int _rem(const char* path,
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
  int chksum(XrdSfsFileSystem::csFunc Func,
             const char* csName,
             const char* Path,
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client = 0,
             const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Stat path
  //----------------------------------------------------------------------------
  int stat(const char* path,
           struct stat* buf,
           XrdOucErrInfo& out_error,
           const XrdSecEntity* client,
           const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Call back to MGM node
  //----------------------------------------------------------------------------
  int CallManager(XrdOucErrInfo* error,
                  const char* path,
                  const char* manager,
                  XrdOucString& capOpaqueFile,
                  XrdOucString* return_result = 0,
                  unsigned short timeout = 0);

  //----------------------------------------------------------------------------
  //! Function dealing with plugin calls
  //----------------------------------------------------------------------------
  int FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);

  //----------------------------------------------------------------------------
  //! Wait for ongoing IO operations to finish
  //!
  //! @param timeout maximum timeout you're willing to wait
  //!
  //! @return true if all operations finished within the timeout, otherwise
  //!         false
  //----------------------------------------------------------------------------
  bool WaitForOngoingIO(std::chrono::seconds timeout);

  //----------------------------------------------------------------------------
  //! Allows to switch on error simulation in the OFS stack
  //!
  //! @param tag type of simulation eroor
  //----------------------------------------------------------------------------
  void SetSimulationError(const char* tag);

  void SetDebug(XrdOucEnv& env);

  void SendRtLog(XrdMqMessage* message);

  void SendFsck(XrdMqMessage* message);

  int Stall(XrdOucErrInfo& error, int stime, const char* msg);

  int Redirect(XrdOucErrInfo& error, const char* host, int& port);

  XrdSysError* Eroute;
  eos::fst::Messaging* Messaging; ///< messaging interface class
  eos::fst::Storage* Storage; ///< Meta data & filesytem store object
  XrdSysMutex OpenFidMutex;

  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
         google::sparse_hash_map<unsigned long long,
         unsigned int> > WOpenFid;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
         google::sparse_hash_map<unsigned long long,
         unsigned int> > ROpenFid;
  //! Map to forbid deleteOnClose for creates if 1+X open had a successfull close
  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
         google::sparse_hash_map<unsigned long long,
         bool> > WNoDeleteOnCloseFid;

  XrdSysMutex XSLockFidMutex;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t,
         google::sparse_hash_map<unsigned long long,
         unsigned int> > XSLockFid;

  //! Queue where file transaction reports get stored and picked up by a
  //! thread running in the Storage class.
  XrdSysMutex ReportQueueMutex;
  std::queue<XrdOucString> ReportQueue;
  //! Queue where log error are stored and picked up by a thread running in Storage
  XrdSysMutex ErrorReportQueueMutex;
  std::queue<XrdOucString> ErrorReportQueue;
  XrdSysMutex WrittenFilesQueueMutex;
  std::queue<struct Fmd> WrittenFilesQueue;
  XrdMqSharedObjectManager ObjectManager; ///< Managing shared objects
  //! Notifying any shared object changes
  XrdMqSharedObjectChangeNotifier ObjectNotifier;
  XrdScheduler* TransferScheduler; ///< TransferScheduler
  XrdSysMutex TransferSchedulerMutex; ///< protecting the TransferScheduler
  XrdOucString eoscpTransferLog; ///< eoscp.log full path
  const char* mHostName; ///< FST hostname
#ifdef HAVE_FST_WITH_QUARKDB
  std::unique_ptr<qclient::QClient> pQcl; ///< Qclient object
#endif

private:
  HttpServer* mHttpd; ///< Embedded http server
  bool Simulate_IO_read_error; ///< simulate an IO error on read
  bool Simulate_IO_write_error; ///< simulate an IO error on write
  bool Simulate_XS_read_error; ///< simulate a checksum error on read
  bool Simulate_XS_write_error; ///< simulate a checksum error on write
  bool Simulate_FMD_open_error; ///< simulate a fmd mismatch error on open

  //----------------------------------------------------------------------------
  //! Information saved for TPC transfers
  //----------------------------------------------------------------------------
  struct TpcInfo {
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

  //! A vector map pointing from tpc key => tpc information for reads, [0]
  //! are readers [1] are writers
  std::vector<google::sparse_hash_map<std::string, struct TpcInfo >> TpcMap;
  XrdSysMutex TpcMapMutex; ///< Mutex protecting the Tpc map
};

//------------------------------------------------------------------------------
//! Global OFS handle
//------------------------------------------------------------------------------
extern XrdFstOfs gOFS;

EOSFSTNAMESPACE_END
#endif
