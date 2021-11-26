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
#include "fst/utils/OpenFileTracker.hh"
#include "fst/utils/TpcInfo.hh"
#include "common/Fmd.hh"
#include "common/Logging.hh"
#include "common/XrdConnPool.hh"
#include "common/ThreadPool.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mq/MessagingRealm.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "qclient/shared/SharedManager.hh"
#include <google/sparse_hash_map>
#include <sys/mman.h>
#include <queue>
#include <memory>
#include <chrono>

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

namespace qclient
{
class QClient;
}

class XrdOucEnv;
class XrdScheduler;

EOSFSTNAMESPACE_BEGIN

// Forward declarations
class ReplicaParLayout;
class RainMetaLayout;
class HttpServer;
class Storage;
class Messaging;
class XrdFstOfsFile;
class FmdHandler;

//------------------------------------------------------------------------------
//! Class XrdFstOfs
//------------------------------------------------------------------------------
class XrdFstOfs : public XrdOfs, public eos::common::LogId
{
  friend class XrdFstOfsFile;
  friend class ReplicaParLayout;
  friend class RainMetaLayout;

public:
  std::atomic<bool> sShutdown; ///< True if shutdown procedure is running

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
  //! Print coverage data
  //!
  //! @param sig
  //----------------------------------------------------------------------------
  static void xrdfstofs_coverage(int sig);

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
           bool ignoreifnotexist = false, 
	   std::string* deletion_report = 0);

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
                  unsigned short timeout = 0,
                  bool use_xrd_conn_pool = false,
                  bool retry = true);

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
  //! @param tag type of simulation error
  //----------------------------------------------------------------------------
  void SetSimulationError(const std::string& tag);

  //----------------------------------------------------------------------------
  //! Request broadcasts from all the registered queues
  //----------------------------------------------------------------------------
  void RequestBroadcasts();

  //----------------------------------------------------------------------------
  //! Get node geotag (each token needs to be less then 8 characters)
  //!
  //! @return geotag value
  //----------------------------------------------------------------------------
  inline std::string GetGeoTag() const
  {
    return mGeoTag;
  }

  //----------------------------------------------------------------------------
  //! Query MGM for the list of deletions
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_OK
  //----------------------------------------------------------------------------
  int Query2Delete();

  int Stall(XrdOucErrInfo& error, int stime, const char* msg);

  int Redirect(XrdOucErrInfo& error, const char* host, int& port);

  std::string MakeDeletionReport(eos::common::FileSystem::fsid_t fsid,
				 unsigned long long fid,
				 struct stat& deletion_stat, 
				 bool viamq=true);

  XrdSysError* Eroute;
  eos::fst::Messaging* Messaging; ///< messaging interface class
  eos::fst::Storage* Storage; ///< Meta data & filesystem store object
  mutable XrdSysMutex OpenFidMutex;

  eos::fst::OpenFileTracker openedForWriting;
  eos::fst::OpenFileTracker openedForReading;
  eos::fst::OpenFileTracker runningCreation;

  //! Map to forbid deleteOnClose for creates if 1+X open had a successful close
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
  std::queue<eos::common::FmdHelper> WrittenFilesQueue;
  XrdMqSharedObjectManager ObjectManager; ///< Managing shared objects
  std::unique_ptr<qclient::SharedManager> mQSOM; ///< MQ on QDB - can be null
  //! Notifying any shared object changes
  XrdMqSharedObjectChangeNotifier ObjectNotifier;
  std::unique_ptr<mq::MessagingRealm> mMessagingRealm;
  XrdScheduler* TransferScheduler; ///< TransferScheduler
  XrdSysMutex TransferSchedulerMutex; ///< protecting the TransferScheduler
  XrdOucString eoscpTransferLog; ///< eoscp.log full path
  const char* mHostName; ///< FST hostname
  QdbContactDetails mQdbContactDetails; ///< QDB contact details
  std::shared_ptr<qclient::QClient> mFsckQcl; ///< Qclient used for fsck
  bool mMqOnQdb; ///< Are we using QDB as an MQ?
  int mHttpdPort; ///< listening port of the http server
  //! Embedded http server if available
  std::unique_ptr<eos::fst::HttpServer> mHttpd;
  std::chrono::seconds mTpcKeyValidity {120}; ///< TPC key validity
  std::string mMgmAlias; ///< MGM alias
  std::shared_ptr<FmdHandler> mFmdHandler; // <File Metadata Handler

  // @note
  // All of the commands below are going to be deprecated and replaced by XRootD
  // query commands which are handled in the FSctl method
  void SetDebug(XrdOucEnv& env);

  void SendFsck(XrdMqMessage* message);

  void SendRtLog(XrdMqMessage* message);

  void DoResync(XrdOucEnv& env);

  void DoDrop(XrdOucEnv& env);

  void DoVerify(XrdOucEnv& env);

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  std::string mGeoTag; ///< Node geotag
  //! XrdOucBuffPool object for managing redirection buffers >= 2kb
  XrdOucBuffPool mXrdBuffPool;
  //! Thread pool for async file close operations
  eos::common::ThreadPool mCloseThreadPool;
  //! Xrd connection pool for interaction with the MGM, used from CallManager
  std::unique_ptr<eos::common::XrdConnPool> mMgmXrdPool;
  std::atomic<bool> mSimIoReadErr; ///< simulate an IO error on read
  std::atomic<bool> mSimIoWriteErr; ///< simulate an IO error on write
  std::atomic<bool> mSimXsReadErr; ///< simulate a checksum error on read
  std::atomic<bool> mSimXsWriteErr; ///< simulate a checksum error on write
  std::atomic<bool> mSimFmdOpenErr; ///< simulate a fmd mismatch error on open
  std::atomic<uint64_t> mSimErrIoReadOff; ///< Simulate IO error offset on rd
  std::atomic<uint64_t> mSimErrIoWriteOff;///< Simulate IO error offset on wr
  std::atomic<uint64_t> mSimDiskWriting;///< Do not really write IO to disk

  //! A vector map pointing from tpc key => tpc information for reads, [0]
  //! are readers [1] are writers
  std::vector<google::sparse_hash_map<std::string, struct TpcInfo >> TpcMap;
  XrdSysMutex TpcMapMutex; ///< Mutex protecting the Tpc map

  //----------------------------------------------------------------------------
  //! Compute adler checksum of given keytab file
  //!
  //! @param kt_path absolute path to keytab file
  //!
  //! @return string representing the checksum or "unaccessible" if keytab
  //!         is unavailable
  //----------------------------------------------------------------------------
  std::string GetKeytabChecksum(const std::string& kt_path) const;

  //----------------------------------------------------------------------------
  //! Get simulation error offset. Parse the last characters and return the
  //! desired offset e.g. io_read_8M should return 8MB
  //!
  //! @param input string encoding the error type and optionally the offset
  //!
  //! @return return offset from which the error should be reported or 0 if no
  //!         such offset if provided
  //----------------------------------------------------------------------------
  uint64_t GetSimulationErrorOffset(const std::string& input) const;

  //----------------------------------------------------------------------------
  //! Update the TPC key validity value (default 120)
  //----------------------------------------------------------------------------
  void UpdateTpcKeyValidity();

  //----------------------------------------------------------------------------
  //! Create directory hierarchy
  //!
  //! @param dir_hierarchy given directory hierarcht
  //! @param mode mode bits for the newly created directories
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool CreateDirHierarchy(const std::string& dir_hierarchy,
                          mode_t mode) const;

  //----------------------------------------------------------------------------
  //! Handle debug query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_OK or SFS_DATA and the
  //!        err_obj is populated with the response
  //----------------------------------------------------------------------------
  int HandleDebug(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Handle fsck query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_OK or SFS_DATA and the
  //!        err_obj is populated with the response
  //----------------------------------------------------------------------------
  int HandleFsck(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Handle resync query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_OK or SFS_DATA and the
  //!        err_obj is populated with the response
  //----------------------------------------------------------------------------
  int HandleResync(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Handle rtlog query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_DATA and the err_obj is
  //!        populated with the response
  //----------------------------------------------------------------------------
  int HandleRtlog(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Handle verify query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_DATA and the err_obj is
  //!        populated with the response
  //----------------------------------------------------------------------------
  int HandleVerify(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Handle drop file query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_DATA and the err_obj is
  //!        populated with the response
  //----------------------------------------------------------------------------
  int HandleDropFile(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Handle clean orphans query
  //!
  //! @param env ecoding of the query command
  //! @param err_obj object holding the response for the query
  //!
  //! @param return SFS_ERROR if failed, otherwise SFS_DATA and the err_obj is
  //!        populated with the response. "OK" if successful.
  //----------------------------------------------------------------------------
  int HandleCleanOrphans(XrdOucEnv& env, XrdOucErrInfo& err_obj);

  //----------------------------------------------------------------------------
  //! Set various XrdCl timeouts more appropriate for the EOS use-case but still
  //! allow the env variables to override them
  //----------------------------------------------------------------------------
  static void SetXrdClTimeouts();
};

//------------------------------------------------------------------------------
//! Global OFS handle
//------------------------------------------------------------------------------
extern XrdFstOfs gOFS;

EOSFSTNAMESPACE_END
#endif
