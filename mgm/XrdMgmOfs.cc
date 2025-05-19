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

#include "common/CommentLog.hh"
#include "common/Constants.hh"
#include "common/Mapping.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/SecEntity.hh"
#include "common/StackTrace.hh"
#include "common/SymKeys.hh"
#include "common/ParseUtils.hh"
#include "common/http/OwnCloud.hh"
#include "common/JeMallocHandler.hh"
#include "common/plugin_manager/Plugin.hh"
#include "common/plugin_manager/DynamicLibrary.hh"
#include "common/plugin_manager/PluginManager.hh"
#include "common/Strerror_r_wrapper.hh"
#include "common/BufferManager.hh"
#include "common/BehaviourConfig.hh"
#include "common/Definitions.hh"
#include "namespace/Constants.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/RenameSafetyCheck.hh"
#include "namespace/utils/Stat.hh"
#include "namespace/utils/Etag.hh"
#include "grpc/GrpcServer.hh"
#include "grpc/GrpcWncServer.hh"
#include "grpc/GrpcRestGwServer.hh"
#include "mgm/AdminSocket.hh"
#include "mgm/Stat.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmAuthz.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/XrdMgmOfsTrace.hh"
#include "mgm/XrdMgmOfsSecurity.hh"
#include "mgm/CommandMap.hh"
#include "mgm/Policy.hh"
#include "mgm/Quota.hh"
#include "mgm/Acl.hh"
#include "mgm/Workflow.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/Recycle.hh"
#include "mgm/Devices.hh"
#include "mgm/PathRouting.hh"
#include "mgm/Macros.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/Egroup.hh"
#include "mgm/http/HttpServer.hh"
#include "mgm/ZMQ.hh"
#include "mgm/Iostat.hh"
#include "mgm/LRU.hh"
#include "mgm/WFE.hh"
#include "mgm/fsck/Fsck.hh"
#include "mgm/IMaster.hh"
#include "mgm/convert/ConverterDriver.hh"
#include "mgm/FuseServer/FusexCastBatch.hh"
#include "mgm/tgc/RealTapeGcMgm.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include "mgm/tracker/ReplicationTracker.hh"
#include "mgm/XrdMgmOfs/fsctl/CommitHelper.hh"
#include "mgm/XattrLock.hh"
#include "mgm/auth/AccessChecker.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/bulk-request/prepare/manager/PrepareManager.hh"
#include "mgm/placement/FsScheduler.hh"
#include "mq/SharedHashWrapper.hh"
#include "mq/FsChangeListener.hh"
#include "mq/GlobalConfigChangeListener.hh"
#include "mq/MessagingRealm.hh"
#include "mq/QdbListener.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.hh"
#include <XrdVersion.hh>
#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucBuffer.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucTokenizer.hh>
#include <XrdOuc/XrdOucTList.hh>
#include <XrdOuc/XrdOucTrace.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdSec/XrdSecInterface.hh>
#include <XrdSfs/XrdSfsAio.hh>
#include <XrdSfs/XrdSfsFlags.hh>
#include "private/XrdSfs/XrdSfsFAttr.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "mgm/bulk-request/dao/factories/AbstractDAOFactory.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/bulk-request/business/BulkRequestBusiness.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/dao/proc/ProcDirectoryBulkRequestLocations.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/bulk-request/prepare/query-prepare/QueryPrepareResult.hh"
#include "mgm/bulk-request/dao/proc/cleaner/BulkRequestProcCleaner.hh"
#include "mgm/bulk-request/utils/json/QueryPrepareResponseJson.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include "mgm/utils/AttrHelper.hh"

#ifdef __APPLE__
#define ECOMM 70
#endif

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif

// Initialize static variables
XrdSysError* XrdMgmOfs::eDest;
thread_local eos::common::LogId XrdMgmOfs::tlLogId;
XrdSysError gMgmOfsEroute(0);
XrdOucTrace gMgmOfsTrace(&gMgmOfsEroute);
XrdMgmOfs* gOFS = 0;

const char* k_mdino = "sys.eos.mdino";
const char* k_nlink = "sys.eos.nlink";

// Set the version information
XrdVERSIONINFO(XrdSfsGetFileSystem, MgmOfs);
XrdVERSIONINFO(XrdSfsGetFileSystem2, MgmOfs);

//------------------------------------------------------------------------------
// Convert NamespaceState to string
//------------------------------------------------------------------------------
std::string namespaceStateToString(NamespaceState st)
{
  switch (st) {
  case NamespaceState::kDown: {
    return "down";
  }

  case NamespaceState::kBooting: {
    return "booting";
  }

  case NamespaceState::kBooted: {
    return "booted";
  }

  case NamespaceState::kFailed: {
    return "failed";
  }

  case NamespaceState::kCompacting: {
    return "compacting";
  }
  }

  return "(invalid)";
}

extern "C" {

#ifdef COVERAGE_BUILD
// Forward declaration of gcov flush API
  extern "C" void __gcov_dump(void);
#endif

  //------------------------------------------------------------------------------
  // XrdAccAuthorizeObject() is called to obtain an instance of the auth object
  // that will be used for all subsequent authorization decisions. If it returns
  // a null pointer; initialization fails and the program exits. The args are:
  //
  // lp    -> XrdSysLogger to be tied to an XrdSysError object for messages
  // cfn   -> The name of the configuration file
  // parm  -> Paramexters specified on the authlib directive. If none it is zero.
  //------------------------------------------------------------------------------
  XrdAccAuthorize* XrdAccAuthorizeObject(XrdSysLogger* lp,
                                         const char*   cfn,
                                         const char*   parm);

  //------------------------------------------------------------------------------
  //! Filesystem Plugin factory function
  //!
  //! @param native_fs (not used)
  //! @param lp the logger object
  //! @param configfn the configuration file name
  //!
  //! @returns configures and returns our MgmOfs object
  //------------------------------------------------------------------------------
  XrdSfsFileSystem*
  XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
                      XrdSysLogger* lp,
                      const char* configfn)
  {
    if (gOFS) {
      // File system object already initalized
      return gOFS;
    }

    gMgmOfsEroute.SetPrefix("MgmOfs_");
    gMgmOfsEroute.logger(lp);
    static XrdMgmOfs myFS(&gMgmOfsEroute);
    XrdOucString vs = "MgmOfs (meta data redirector) ";
    vs += VERSION;
    gMgmOfsEroute.Say("++++++ (c) 2015 CERN/IT-DSS ", vs.c_str());

    // Initialize the subsystems
    if (!myFS.Init(gMgmOfsEroute)) {
      return nullptr;
    }

    // Disable XRootd log rotation
    lp->setRotate(0);
    gOFS = &myFS;
    // By default enable stalling and redirection
    gOFS->IsStall = true;
    gOFS->IsRedirect = true;
    myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : nullptr);

    if (myFS.Configure(gMgmOfsEroute)) {
      return nullptr;
    }

    // Initialize authorization plugin XrdMgmAuthz
    gOFS->mMgmAuthz = (XrdMgmAuthz*) XrdAccAuthorizeObject(lp, configfn,
                      nullptr);

    if (!gOFS->mMgmAuthz) {
      return nullptr;
    }

    return gOFS;
  }

//------------------------------------------------------------------------------
//! Filesystem Plugin factory function
//!
//! @description FileSystem2 version, to allow passing configuration info back
//!              to XRootD. Configure with: xrootd.fslib -2 libXrdEosMgm.so
//!
//! @param native_fs (not used)
//! @param lp the logger object
//! @param configfn the configuration file name
//! @param envP pass configuration information back to XrdXrootd
//!
//! @returns configures and returns our MgmOfs object
//------------------------------------------------------------------------------
  XrdSfsFileSystem*
  XrdSfsGetFileSystem2(XrdSfsFileSystem* native_fs,
                       XrdSysLogger* lp,
                       const char* configfn,
                       XrdOucEnv* envP)
  {
    // Initialise gOFS
    XrdSfsGetFileSystem(native_fs, lp, configfn);

    // Tell XRootD that MgmOfs implements the Prepare plugin
    if (envP != nullptr) {
      envP->Put("XRD_PrepHandler", "1");
    }

    return gOFS;
  }

} // extern "C"

/******************************************************************************/
/* MGM Meta Data Interface                                                    */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor MGM Ofs
//------------------------------------------------------------------------------
XrdMgmOfs::XrdMgmOfs(XrdSysError* ep):
  ConfigFN(0), ConfEngine(0), mCapabilityValidity(3600),
  mMgmMessaging(nullptr), ManagerPort(1094), LinuxStatsStartup{0},
  HostName(0), HostPref(0), mNamespaceState(NamespaceState::kDown),
  mFileInitTime(0), mTotalInitTime(time(nullptr)), mStartTime(time(nullptr)),
  Shutdown(false), mBootFileId(0), mBootContainerId(0), IsRedirect(true),
  IsStall(true), mAuthorize(false), mAuthLib(""), mTapeEnabled(false),
  mReqIdMax(64),
  MgmRedirector(false), mErrLogEnabled(true), eosDirectoryService(0),
  eosFileService(0), eosView(0), eosFsView(0), eosContainerAccounting(0),
  eosSyncTimeAccounting(0), mFrontendPort(0), mNumAuthThreads(0),
  mFrontendLocalhost(1), zMQ(nullptr), mExtAuthz(nullptr),
  MgmStatsPtr(new eos::mgm::Stat()),  MgmStats(*MgmStatsPtr),
  mFsckEngine(new Fsck()), mMaster(nullptr),
  mRouting(new eos::mgm::PathRouting()), mConverterDriver(),
  mHttpd(nullptr), GRPCd(nullptr), WNCd(nullptr), mRestGrpcSrv(nullptr),
  mLRUEngine(new eos::mgm::LRU()),
  WFEPtr(new eos::mgm::WFE()), WFEd(*WFEPtr), mFstGwHost(""),
  mFstGwPort(0), mQdbCluster(""), mHttpdPort(8000),
  mFusexPort(1100), mGRPCPort(50051), mWncPort(50052),
  mRestGrpcPort(50054),
  mFidTracker(std::chrono::seconds(600), std::chrono::seconds(3600)),
  mBehaviourCfg(new eos::common::BehaviourConfig()),
  mDoneOrderlyShutdown(false),
  mXrdBuffPool(2 * eos::common::KB, 2 * eos::common::MB, 8, 64),
  mJeMallocHandler(new eos::common::JeMallocHandler()),
  protowfusegrpc(false)
{
  eDest = ep;
  ConfigFN = 0;
  enforceRecycleBin = false;

  if (getenv("EOS_MGM_HTTP_PORT")) {
    mHttpdPort = strtol(getenv("EOS_MGM_HTTP_PORT"), 0, 10);
  }

  if (getenv("EOS_MGM_FUSEX_PORT")) {
    mFusexPort = strtol(getenv("EOS_MGM_FUSEX_PORT"), 0, 10);
  }

  if (getenv("EOS_MGM_GRPC_PORT")) {
    mGRPCPort = strtol(getenv("EOS_MGM_GRPC_PORT"), 0, 10);
  }

  if (getenv("EOS_MGM_WNC_PORT")) {
    mWncPort = strtol(getenv("EOS_MGM_WNC_PORT"), 0, 10);
  }

  if (getenv("EOS_MGM_FUSE_BOOKING_SIZE")) {
    mFusePlacementBooking = strtol(getenv("EOS_MGM_FUSE_BOOKING_SIZE"), 0, 10);
  } else {
    mFusePlacementBooking = 5 * 1024 * 1024 * 1024ll;
  }

  mRestApiManager = std::make_unique<rest::RestApiManager>();
  eos::common::LogId::SetSingleShotLogId();
  mZmqContext = new zmq::context_t(1);
  IoStats.reset(new eos::mgm::Iostat());
  mHttpd.reset(new eos::mgm::HttpServer(mHttpdPort));

  if (mGRPCPort) {
    GRPCd.reset(new eos::mgm::GrpcServer(mGRPCPort));
  }

  if (mWncPort) {
    WNCd.reset(new eos::mgm::GrpcWncServer(mWncPort));
  }

  if (getenv("EOS_MGM_REST_GRPC_PORT")) {
    mRestGrpcPort = strtol(getenv("EOS_MGM_REST_GRPC_PORT"), 0, 10);
  }

  if (mRestGrpcPort) {
    const char* ptr = getenv("EOS_MGM_ENABLE_REST_API");

    if (ptr && strncmp(ptr, "1", 1) == 0) {
      mRestGrpcSrv.reset(new eos::mgm::GrpcRestGwServer(mRestGrpcPort));
    }
  }

  EgroupRefresh.reset(new eos::mgm::Egroup());
  Recycler.reset(new eos::mgm::Recycle());
  mDeviceTracker.reset(new eos::mgm::Devices());
  mTapeGcMgm.reset(new tgc::RealTapeGcMgm(*this));
  mTapeGc.reset(new tgc::MultiSpaceTapeGc(*mTapeGcMgm));
  mFsScheduler.reset(new eos::mgm::placement::FSScheduler());
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdMgmOfs::~XrdMgmOfs()
{
  OrderlyShutdown();
  eos_warning("%s", "msg=\"finished destructor\"");

  if (HostName) {
    free(HostName);
  }
}

//------------------------------------------------------------------------------
// Destroy member objects and clean up threads
//------------------------------------------------------------------------------
void
XrdMgmOfs::OrderlyShutdown()
{
  if (mDoneOrderlyShutdown) {
    eos_warning("%s", "msg=\"skipping already done shutdown procedure\"");
    return;
  }

  auto start_ts = std::chrono::steady_clock::now();
  mDoneOrderlyShutdown = true;
  {
    eos_warning("%s", "msg=\"set stall rule of all ns operations\"");
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    Access::gStallRules[std::string("*")] = "300";
  }
  gOFS->mTracker.SetAcceptingRequests(false);
  gOFS->mTracker.SpinUntilNoRequestsInFlight(true,
      std::chrono::milliseconds(100));
  eos_warning("%s", "msg=\"stopping error logger thread\"");
  mErrLoggerTid.join();
  eos_warning("%s", "msg=\"stopping fs listener thread\"");
  auto stop_fsconfiglistener = std::thread([&]() {
    mFsConfigTid.join();
  });

  if (!mMessagingRealm->haveQDB()) {
    // We now need to signal to the FsConfigListener thread to unblock it
    XrdMqSharedObjectChangeNotifier::Subscriber*
    subscriber = ObjectNotifier.GetSubscriberFromCatalog("fsconfiglistener", false);

    if (subscriber) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      XrdSysMutexHelper lock(subscriber->mSubjMtx);
      subscriber->mSubjSem.Post();
    }
  }

  stop_fsconfiglistener.join();
  eos_warning("%s", "msg=\"disable configuration engine autosave\"");
  ConfEngine->SetAutoSave(false);
  FsView::gFsView.SetConfigEngine(nullptr);
  eos_warning("%s", "msg=\"stop routing\"");

  if (mRouting) {
    mRouting.reset();
  }

  eos_warning("%s", "msg=\"stopping the stats collecting thread\"");
  eos_warning("%s", "msg=\"stopping archive submitter\"");
  mSubmitterTid.join();

  if (mZmqContext) {
    eos_warning("%s", "msg=\"closing the ZMQ context\"");
    mZmqContext->close();
    eos_warning("%s", "msg=\"joining the master and worker auth threads\"");
    mAuthMasterTid.join();

    for (const auto& auth_tid : mVectTid) {
      XrdSysThread::Join(auth_tid, nullptr);
    }

    mVectTid.clear();
    eos_warning("%s", "msg=\"deleting the ZMQ context\"");
    delete mZmqContext;
  }

  eos_warning("%s", "msg=\"stopping converter engine\"");
  mConverterDriver->Stop();
  eos_warning("%s", "msg=\"stopping central drainning\"");
  mDrainEngine.Stop();
  eos_warning("%s", "msg=\"stopping geotree engine updater\"");
  mGeoTreeEngine->StopUpdater();

  if (IoStats) {
    eos_warning("%s", "msg=\"stopping and deleting IoStats\"");
    IoStats.reset();
  }

  eos_warning("%s", "msg=\"stopping fusex server\"");
  zMQ->gFuseServer.shutdown();
  // TODO: for now removing this since it breaks Centos8/9 shutdown
  /*  if (zMQ) {
    delete zMQ;
    zMQ = nullptr;
  }
  */
  eos_warning("%s", "msg=\"stopping FSCK service\"");
  mFsckEngine->Stop();
  eos_warning("%s", "msg=\"stopping messaging\"");

  if (mMgmMessaging) {
    delete mMgmMessaging;
    mMgmMessaging = nullptr;
  }

  if (Recycler) {
    eos_warning("%s", "msg=\"stopping and deleting recycler server\"");
    Recycler.reset();
  }

  if (WFEPtr) {
    eos_warning("%s", "msg=\"stopping and deleting the WFE engine\"");
    WFEPtr.reset();
  }

  eos_warning("%s", "msg=\"stopping and deleting the LRU engine\"");
  mLRUEngine.reset();

  if (EgroupRefresh) {
    eos_warning("%s", "msg=\"stopping and deleting egroup refresh thread\"");
    EgroupRefresh.reset();
  }

  if (mHttpd) {
    eos_warning("%s", "msg=\"stopping and deleting HTTP daemon\"");
    mHttpd.reset();
  }

  if (WNCd) {
    eos_warning("%s", "msg=\"stopping gRPC server for EOS-wnc\"");
    WNCd.reset();
  }

  if (!mMessagingRealm->haveQDB()) {
    eos_warning("%s", "msg=\"stopping the shared object notifier thread\"");
    ObjectNotifier.Stop();
  }

  eos_warning("%s", "msg=\"cleanup quota information\"");
  (void) Quota::CleanUp();
  eos_warning("%s", "msg=\"graceful shutdown of the FsView\"");
  FsView::gFsView.StopHeartBeat();
  FsView::gFsView.Clear();

  if (mErrLogEnabled) {
    eos_warning("%s", "msg=\"error log kill\"");
    std::string errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
    int rrc = system(errorlogkillline.c_str());

    if (WEXITSTATUS(rrc)) {
      eos_static_info("%s returned %d", errorlogkillline.c_str(), rrc);
    }
  }

  if (gOFS->mNamespaceState == NamespaceState::kBooted) {
    eos_warning("%s", "msg=\"finalizing namespace views\"");

    try {
      gOFS->eosDirectoryService = nullptr;
      gOFS->eosFileService = nullptr;
      gOFS->eosView = nullptr;
      gOFS->eosFsView = nullptr;
      gOFS->eosContainerAccounting = nullptr;
      gOFS->eosSyncTimeAccounting = nullptr;
      gOFS->namespaceGroup.reset();
    } catch (eos::MDException& e) {
      // we don't really care about any exception here!
    }
  }

  eos_warning("%s", "msg=\"stopping master-slave supervisor thread\"");

  if (mMaster) {
    mMaster.reset();
  }

  auto end_ts = std::chrono::steady_clock::now();
  eos_warning("msg=\"finished orderly shutdown in %llu seconds\"",
              std::chrono::duration_cast<std::chrono::seconds>
              (end_ts - start_ts).count());
}

//------------------------------------------------------------------------------
// This is just kept to be compatible with standard OFS plugins, but it is not
// used for the moment.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::Init(XrdSysError& ep)
{
  return true;
}

//------------------------------------------------------------------------------
// Return a MGM directory object
//------------------------------------------------------------------------------
XrdSfsDirectory*
XrdMgmOfs::newDir(char* user, int MonID)
{
  return (XrdSfsDirectory*)new XrdMgmOfsDirectory(user, MonID);
}

//------------------------------------------------------------------------------
// Return MGM file object
//------------------------------------------------------------------------------
XrdSfsFile*
XrdMgmOfs::newFile(char* user, int MonID)
{
  return (XrdSfsFile*)new XrdMgmOfsFile(user, MonID);
}

//------------------------------------------------------------------------------
// Notify filesystem that a client has disconnected
//------------------------------------------------------------------------------
void
XrdMgmOfs::Disc(const XrdSecEntity* client)
{
  if (client) {
    ProcInterface::DropSubmittedCmd(client->tident);
  }
}

//------------------------------------------------------------------------------
// Implementation Source Code Includes
//------------------------------------------------------------------------------
#include "XrdMgmOfs/Access.cc"
#include "XrdMgmOfs/Attr.cc"
#include "XrdMgmOfs/FAttr.cc"
#include "XrdMgmOfs/Auth.cc"
#include "XrdMgmOfs/Chksum.cc"
#include "XrdMgmOfs/Chmod.cc"
#include "XrdMgmOfs/Chown.cc"
#include "XrdMgmOfs/Coverage.cc"
#include "XrdMgmOfs/DeleteExternal.cc"
#include "XrdMgmOfs/DropReplica.cc"
#include "XrdMgmOfs/Exists.cc"
#include "XrdMgmOfs/Find.cc"
#include "XrdMgmOfs/FsConfigListener.cc"
#include "XrdMgmOfs/ErrorLogListener.cc"
#include "XrdMgmOfs/Fsctl.cc"
#include "XrdMgmOfs/Link.cc"
#include "XrdMgmOfs/Mkdir.cc"
#include "XrdMgmOfs/PathMap.cc"
#include "XrdMgmOfs/QoS.cc"
#include "XrdMgmOfs/Remdir.cc"
#include "XrdMgmOfs/Rename.cc"
#include "XrdMgmOfs/Rm.cc"
#include "XrdMgmOfs/SharedPath.cc"
#include "XrdMgmOfs/ShouldRedirect.cc"
#include "XrdMgmOfs/ShouldRoute.cc"
#include "XrdMgmOfs/ShouldStall.cc"
#include "XrdMgmOfs/Shutdown.cc"
#include "XrdMgmOfs/Stacktrace.cc"
#include "XrdMgmOfs/Stat.cc"
#include "XrdMgmOfs/Stripes.cc"
#include "XrdMgmOfs/Touch.cc"
#include "XrdMgmOfs/Utimes.cc"
#include "XrdMgmOfs/Version.cc"

//------------------------------------------------------------------------------
// Test for stall rule
//------------------------------------------------------------------------------
bool
XrdMgmOfs::HasStall(const char* path,
                    const char* rule,
                    int& stalltime,
                    XrdOucString& stallmsg)
{
  if (!rule) {
    return false;
  }

  eos::common::RWMutexReadLock lock(Access::gAccessMutex);

  if (Access::gStallRules.count(std::string(rule))) {
    stalltime = atoi(Access::gStallRules[std::string(rule)].c_str());
    stallmsg =
      "Attention: you are currently hold in this instance and each request is stalled for ";
    stallmsg += (int) stalltime;
    stallmsg += " seconds after an errno of type: ";
    stallmsg += rule;
    eos_static_info("info=\"stalling\" path=\"%s\" errno=\"%s\"", path, rule);
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Test for redirection rule
//------------------------------------------------------------------------------
bool
XrdMgmOfs::HasRedirect(const char* path, const char* rule, std::string& host,
                       int& port)
{
  if (!rule) {
    return false;
  }

  std::string srule = rule;
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);

  if (Access::gRedirectionRules.count(srule)) {
    std::string delimiter = ":";
    std::vector<std::string> tokens;
    eos::common::StringConversion::Tokenize(Access::gRedirectionRules[srule],
                                            tokens, delimiter);

    if (tokens.size() == 1) {
      host = tokens[0].c_str();
      port = 1094;
    } else {
      host = tokens[0].c_str();
      port = atoi(tokens[1].c_str());

      if (port == 0) {
        port = 1094;
      }
    }

    eos_static_info("info=\"redirect\" path=\"%s\" host=%s port=%d errno=%s",
                    path, host.c_str(), port, rule);

    if (srule == "ENONET") {
      gOFS->MgmStats.Add("RedirectENONET", 0, 0, 1);
    } else if (srule == "ENOENT") {
      gOFS->MgmStats.Add("RedirectENOENT", 0, 0, 1);
    } else if (srule == "ENETUNREACH") {
      gOFS->MgmStats.Add("RedirectENETUNREACH", 0, 0, 1);
    }

    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Return the version of the MGM software
//------------------------------------------------------------------------------
const char*
XrdMgmOfs::getVersion()
{
  static XrdOucString FullVersion = XrdVERSION;
  FullVersion += " MgmOfs ";
  FullVersion += VERSION;
  return FullVersion.c_str();
}

//-------------------------------------------------------------------------------------
// Prepare a file or query the status of a previous prepare request
//-------------------------------------------------------------------------------------
int
XrdMgmOfs::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                   const XrdSecEntity* client)
{
  if (pargs.opts & Prep_QUERY) {
    return _prepare_query(pargs, error, client);
  } else {
    return _prepare(pargs, error, client);
  }
}


//--------------------------------------------------------------------------------------
// Prepare a file
//
// EOS will call a prepare workflow if defined
//--------------------------------------------------------------------------------------
int
XrdMgmOfs::_prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                    const XrdSecEntity* client)
{
  USE_EOSBULKNAMESPACE;
  PrepareManager pm(std::make_unique<RealMgmFileSystemInterface>(gOFS));
  int prepareRetCode = pm.prepare(pargs, error, client);
  return prepareRetCode;
}


//-------------------------------------------------------------------------------------------
// Query the status of a previous prepare request
//-------------------------------------------------------------------------------------------
int
XrdMgmOfs::_prepare_query(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                          const XrdSecEntity* client)
{
  USE_EOSBULKNAMESPACE;
  RealMgmFileSystemInterface mgmFsInterface(gOFS);
  PrepareManager pm(std::make_unique<RealMgmFileSystemInterface>(gOFS));
  std::unique_ptr<QueryPrepareResult> result = pm.queryPrepare(pargs, error,
      client);

  if (result->hasQueryPrepareFinished()) {
    //Create the JSON response
    bulk::QueryPrepareResponseJson jsonQueryPrepareResponse;
    std::stringstream json_ss;
    auto queryPrepareResponse = result->getResponse();
    queryPrepareResponse->setJsonifier(
      std::make_shared<bulk::QueryPrepareResponseJson>());
    jsonQueryPrepareResponse.jsonify(queryPrepareResponse.get(), json_ss);
    // Send the reply. XRootD requires that we put it into a buffer that can be released with free().
    auto  json_len = json_ss.str().length();
    char* json_buf = reinterpret_cast<char*>(malloc(json_len));
    strncpy(json_buf, json_ss.str().c_str(), json_len);
    // Ownership of this buffer is passed to xrd_buff which has a Recycle() method.
    XrdOucBuffer* xrd_buff = new XrdOucBuffer(json_buf, json_len);
    // Ownership of xrd_buff is passed to error. Note that as we are returning SFS_DATA, the first
    // parameter is the buffer length rather than an error code.
    error.setErrInfo(xrd_buff->BuffSize(), xrd_buff);
  }

  return result->getReturnCode();
}


//------------------------------------------------------------------------------
//! Truncate a file (not supported in EOS, only via the file interface)
//------------------------------------------------------------------------------
int
XrdMgmOfs::truncate(const char*,
                    XrdSfsFileOffset,
                    XrdOucErrInfo& error,
                    const XrdSecEntity* client,
                    const char* path)
{
  static const char* epname = "truncate";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, 0, tident, vid);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* ininfo = "";
    MAYREDIRECT;
  }
  gOFS->MgmStats.Add("Truncate", vid.uid, vid.gid, 1);
  return Emsg(epname, error, EOPNOTSUPP, "truncate", path);
}

//------------------------------------------------------------------------------
// Return error message
//------------------------------------------------------------------------------
int
XrdMgmOfs::Emsg(const char* pfx,
                XrdOucErrInfo& einfo,
                int ecode,
                const char* op,
                const char* target)
{
  char etext[128], buffer[4096];

  // Get the reason for the error
  if (ecode < 0) {
    ecode = -ecode;
  }

  if (eos::common::strerror_r(ecode, etext, sizeof(etext))) {
    sprintf(etext, "reason unknown (%d)", ecode);
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);

  if ((ecode == EIDRM) || (ecode == ENODATA)) {
    eos_debug("Unable to %s %s; %s", op, target, etext);
  } else {
    if ((!strcmp(op, "stat")) || (((!strcmp(pfx, "attr_get")) ||
                                   (!strcmp(pfx, "attr_ls")) ||
                                   (!strcmp(pfx, "FuseX"))) && (ecode == ENOENT))) {
      eos_debug("Unable to %s %s; %s", op, target, etext);
    } else {
      eos_err("Unable to %s %s; %s", op, target, etext);
    }
  }

  // Print it out if debugging is enabled
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif
  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Create stall response
//------------------------------------------------------------------------------
int
XrdMgmOfs::Stall(XrdOucErrInfo& error,
                 int stime,
                 const char* msg)

{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  EPNAME("Stall");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Stall " << stime << ": " << smessage.c_str());
  // Place the error message in the error object and return
  error.setErrInfo(0, smessage.c_str());
  return stime;
}

//------------------------------------------------------------------------------
// Create redirect response
//------------------------------------------------------------------------------
int
XrdMgmOfs::Redirect(XrdOucErrInfo& error,
                    const char* host,
                    int& port,
                    const char* path,
                    bool collapse)
{
  EPNAME("Redirect");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Redirect " << host << ":" << port);

  if (collapse && strlen(path)) {
    std::string url = "root://";
    url += host;
    url += ":";
    url += std::to_string(port);
    url += "/";
    url += path;
    error.setErrInfo(~(~(-1) | kXR_collapseRedir), url.c_str());
  } else {
    // Place the error message in the error object and return
    error.setErrInfo(port, host);
  }

  return SFS_REDIRECT;
}

//------------------------------------------------------------------------------
// Start a thread that will queue, build and submit backup operations to the
// archiver daemon.
//------------------------------------------------------------------------------
void
XrdMgmOfs::ArchiveSubmitterThread(ThreadAssistant& assistant) noexcept
{
  ProcCommand pcmd;
  std::string job_opaque;
  XrdOucString std_out, std_err;
  int max, running, pending;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  eos_debug("msg=\"starting archive/backup submitter thread\"");
  std::ostringstream cmd_json;
  cmd_json << "{\"cmd\": \"stats\", "
           << "\"opt\": \"\", "
           << "\"uid\": \"0\", "
           << "\"gid\": \"0\" }";

  while (!assistant.terminationRequested()) {
    {
      XrdSysMutexHelper lock(mJobsQMutex);

      if (!mPendingBkps.empty()) {
        // Check if archiver has slots available
        if (!pcmd.ArchiveExecuteCmd(cmd_json.str())) {
          std_out.resize(0);
          std_err.resize(0);
          pcmd.AddOutput(std_out, std_err);

          if ((sscanf(std_out.c_str(), "max=%i running=%i pending=%i",
                      &max, &running, &pending) == 3)) {
            while ((running + pending < max) && !mPendingBkps.empty()) {
              running++;
              job_opaque = mPendingBkps.back();
              mPendingBkps.pop_back();
              job_opaque += "&mgm.backup.create=1";

              if (pcmd.open("/proc/admin", job_opaque.c_str(), root_vid, 0)) {
                pcmd.AddOutput(std_out, std_err);
                eos_err("failed backup, msg=\"%s\"", std_err.c_str());
              }
            }
          }
        } else {
          eos_err("failed to send stats command to archive daemon");
        }
      }
    }
    assistant.wait_for(std::chrono::seconds(5));
  }

  eos_warning("%s", "msg=\"shutdown archive submitter\"");
}

//------------------------------------------------------------------------------
// Submit backup job
//------------------------------------------------------------------------------
bool
XrdMgmOfs::SubmitBackupJob(const std::string& job_opaque)
{
  XrdSysMutexHelper lock(mJobsQMutex);
  auto it = std::find(mPendingBkps.begin(), mPendingBkps.end(), job_opaque);

  if (it == mPendingBkps.end()) {
    mPendingBkps.push_front(job_opaque);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Get vector of pending backups
//------------------------------------------------------------------------------
std::vector<ProcCommand::ArchDirStatus>
XrdMgmOfs::GetPendingBkps()
{
  std::vector<ProcCommand::ArchDirStatus> bkps;
  XrdSysMutexHelper lock(mJobsQMutex);

  for (auto it = mPendingBkps.begin(); it != mPendingBkps.end(); ++it) {
    XrdOucEnv opaque(it->c_str());
    bkps.emplace_back("N/A", "N/A", opaque.Get("mgm.backup.dst"), "backup",
                      "pending at MGM");
  }

  return bkps;
}

//------------------------------------------------------------------------------
// Discover/search for a service provided to the plugins by the platform
//------------------------------------------------------------------------------
int32_t
XrdMgmOfs::DiscoverPlatformServices(const char* svc_name, void* opaque)
{
  std::string sname = svc_name;

  if (sname == "NsViewMutex") {
    PF_Discovery_Service* ns_lock = (PF_Discovery_Service*)(opaque);
    // TODO (esindril): Use this code when we drop SLC6 support @todo
    //std::string htype = std::to_string(typeid(&gOFS->eosViewRWMutex).hash_code());
    std::string htype = "eos::common::RWMutex*";
    ns_lock->objType = (char*)calloc(htype.length() + 1, sizeof(char));
    (void) strcpy(ns_lock->objType, htype.c_str());
    ns_lock->ptrService = static_cast<void*>(&gOFS->eosViewRWMutex);
  } else {
    return EINVAL;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Cast a change message to all fusex clients about a deletion of an entry
//------------------------------------------------------------------------------
void
XrdMgmOfs::FuseXCastDeletion(eos::ContainerIdentifier id,
                             const std::string& name)
{
  struct timespec pt_mtime {
    0, 0
  };
  gOFS->zMQ->gFuseServer.Cap().BroadcastDeletionFromExternal(
    id.getUnderlyingUInt64(), name, pt_mtime);
}



//------------------------------------------------------------------------------
void
XrdMgmOfs::FuseXCastRefresh(eos::ContainerIdentifier id,
                            eos::ContainerIdentifier parentid)
{
  gOFS->zMQ->gFuseServer.Cap().BroadcastRefreshFromExternal(
    id.getUnderlyingUInt64(), parentid.getUnderlyingUInt64());
}

void
XrdMgmOfs::FuseXCastRefresh(eos::FileIdentifier id,
                            eos::ContainerIdentifier parentid)
{
  gOFS->zMQ->gFuseServer.Cap().BroadcastRefreshFromExternal(
    eos::common::FileId::FidToInode(id.getUnderlyingUInt64()),
    parentid.getUnderlyingUInt64());
}

//------------------------------------------------------------------------------
// Cast a MD object to clients
//------------------------------------------------------------------------------
void
XrdMgmOfs::FuseXCastMD(eos::ContainerIdentifier id,
                       eos::ContainerIdentifier parentid,
                       struct timespec& pt_mtime,
                       bool lock)
{
  eos::fusex::md dir;
  static eos::common::VirtualIdentity root_vid =
    eos::common::VirtualIdentity::Root();

  if (!gOFS->zMQ->gFuseServer.FillContainerMD(id.getUnderlyingUInt64(), dir,
      root_vid, lock)) {
    gOFS->zMQ->gFuseServer.Cap().BroadcastMD(dir, dir.md_ino(), dir.md_pino(),
        dir.clock(), pt_mtime);
  }
}

void
XrdMgmOfs::FuseXCastMD(eos::FileIdentifier id,
                       eos::ContainerIdentifier parentid,
                       struct timespec& pt_mtime,
                       bool lock
                      )
{
  eos::fusex::md file;
  static eos::common::VirtualIdentity root_vid =
    eos::common::VirtualIdentity::Root();

  if (gOFS->zMQ->gFuseServer.FillFileMD(eos::common::FileId::FidToInode(
                                          id.getUnderlyingUInt64()), file, root_vid, lock)) {
    gOFS->zMQ->gFuseServer.Cap().BroadcastMD(file, file.md_ino(), file.md_pino(),
        file.clock(), pt_mtime);
  }
}


//------------------------------------------------------------------------------
// Check if namespace is booted
//------------------------------------------------------------------------------
bool
XrdMgmOfs::IsNsBooted() const
{
  return ((mNamespaceState == NamespaceState::kBooted) ||
          (mNamespaceState == NamespaceState::kCompacting));
}

//------------------------------------------------------------------------------
// Convert error code to string representation
//------------------------------------------------------------------------------
std::string
XrdMgmOfs::MacroStringError(int errcode)
{
  if (errcode == ENOTCONN) {
    return "ENOTCONN";
  } else if (errcode == EPROTO) {
    return "EPROTO";
  } else if (errcode == EAGAIN) {
    return "EAGAIN";
  } else {
    return "EINVAL";
  }
}

//------------------------------------------------------------------------------
// Write report record for final deletion (to IoStats)
//------------------------------------------------------------------------------
void
XrdMgmOfs::WriteRmRecord(const std::shared_ptr<eos::IFileMD>& fmd,
                         const char* full_path)
{
  struct timespec ts_now;
  char report[16384];
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  fmd->getCTime(ctime);
  fmd->getMTime(mtime);
  const std::string enc_path =
    eos::common::StringConversion::curl_default_escaped(full_path);
  eos::common::Timing::GetTimeSpec(ts_now);
  snprintf(report, sizeof(report) - 1,
           "log=%s&path=%s&"
           "host=%s&fid=%llu&fxid=%08llx&"
           "ruid=%u&rgid=%u&"
           "del_ts=%lu&del_tns=%lu&"
           "dc_ts=%lu&dc_tns=%lu&"
           "dm_ts=%lu&dm_tns=%lu&"
           "dsize=%lu&sec.app=rm",
           this->logId,
           (enc_path.empty() ? "N/A" : enc_path.c_str()),
           gOFS->ManagerId.c_str(), (unsigned long long) fmd->getId(),
           (unsigned long long) fmd->getId(), fmd->getCUid(), fmd->getCGid(),
           ts_now.tv_sec, ts_now.tv_nsec, ctime.tv_sec, ctime.tv_nsec,
           mtime.tv_sec, mtime.tv_nsec, fmd->getSize());
  std::string record = report;

  if (IoStats) {
    IoStats->WriteRecord(record);
  }
}

//------------------------------------------------------------------------------
// Write report record for recycle bin deletion (to IoStats)
//------------------------------------------------------------------------------
void
XrdMgmOfs::WriteRecycleRecord(const std::shared_ptr<eos::IFileMD>& fmd)
{
  struct timespec ts_now;
  char report[16384];
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  fmd->getCTime(ctime);
  fmd->getMTime(mtime);
  eos::common::Timing::GetTimeSpec(ts_now);
  snprintf(report, sizeof(report) - 1,
           "log=%s&"
           "host=%s&fid=%llu&fxid=%08llx&"
           "ruid=%u&rgid=%u&"
           "del_ts=%lu&del_tns=%lu&"
           "dc_ts=%lu&dc_tns=%lu&"
           "dm_ts=%lu&dm_tns=%lu&"
           "dsize=%lu&sec.app=recycle",
           this->logId, gOFS->ManagerId.c_str(), (unsigned long long) fmd->getId(),
           (unsigned long long) fmd->getId(), fmd->getCUid(), fmd->getCGid(),
           ts_now.tv_sec, ts_now.tv_nsec, ctime.tv_sec, ctime.tv_nsec,
           mtime.tv_sec, mtime.tv_nsec, fmd->getSize());
  std::string record = report;

  if (IoStats) {
    IoStats->WriteRecord(record);
  }
}

//------------------------------------------------------------------------------
// Check if a host was tried already in a given URL with a given error
//------------------------------------------------------------------------------
bool
XrdMgmOfs::Tried(XrdCl::URL& url, std::string& host, const char* terr)
{
  XrdCl::URL::ParamsMap params = url.GetParams();
  std::string tried_hosts = params["tried"];
  std::string tried_rc = params["triedrc"];
  std::vector<std::string> v_hosts;
  std::vector<std::string> v_rc;
  eos::common::StringConversion::Tokenize(tried_hosts,
                                          v_hosts, ",");
  eos::common::StringConversion::Tokenize(tried_rc,
                                          v_rc, ",");

  for (size_t i = 0; i < v_hosts.size(); ++i) {
    if ((v_hosts[i] == host) &&
        (i < v_rc.size()) &&
        ((v_rc[i] == std::string(terr)) || (std::string(terr) == "*"))) {
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Wait until namespace is booted - thread cancellation point
//------------------------------------------------------------------------------
void
XrdMgmOfs::WaitUntilNamespaceIsBooted()
{
  XrdSysThread::SetCancelDeferred();

  while (gOFS->mNamespaceState != NamespaceState::kBooted) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    XrdSysThread::CancelPoint();
  }
}

//------------------------------------------------------------------------------
// Wait until namespace is booted
//------------------------------------------------------------------------------
void
XrdMgmOfs::WaitUntilNamespaceIsBooted(ThreadAssistant& assistant)
{
  while (gOFS->mNamespaceState != NamespaceState::kBooted) {
    assistant.wait_for(std::chrono::seconds(1));

    if (assistant.terminationRequested()) {
      break;
    }
  }
}

//------------------------------------------------------------------------------
// Return string representation of prepare options
//------------------------------------------------------------------------------
std::string
XrdMgmOfs::prepareOptsToString(const int opts)
{
  std::ostringstream result;
  const int priority = opts & Prep_PMASK;

  switch (priority) {
  case Prep_PRTY0:
    result << "PRTY0";
    break;

  case Prep_PRTY1:
    result << "PRTY1";
    break;

  case Prep_PRTY2:
    result << "PRTY2";
    break;

  case Prep_PRTY3:
    result << "PRTY3";
    break;

  default:
    result << "PRTYUNKNOWN";
  }

  const int send_mask = 12;
  const int send = opts & send_mask;

  switch (send) {
  case 0:
    break;

  case Prep_SENDAOK:
    result << ",SENDAOK";
    break;

  case Prep_SENDERR:
    result << ",SENDERR";
    break;

  case Prep_SENDACK:
    result << ",SENDACK";
    break;

  default:
    result << ",SENDUNKNOWN";
  }

  if (opts & Prep_WMODE) {
    result << ",WMODE";
  }

  if (opts & Prep_STAGE) {
    result << ",STAGE";
  }

  if (opts & Prep_COLOC) {
    result << ",COLOC";
  }

  if (opts & Prep_FRESH) {
    result << ",FRESH";
  }

#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5

  if (opts & Prep_CANCEL) {
    result << ",CANCEL";
  }

  if (opts & Prep_QUERY) {
    result << ",QUERY";
  }

  if (opts & Prep_EVICT) {
    result << ",EVICT";
  }

#endif
  return result.str();
}

//------------------------------------------------------------------------------
// Populate file error object with redirection information that can be
// longer than 2kb. For this we need to use the XrdOucBuffer interface.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::SetRedirectionInfo(XrdOucErrInfo& err_obj,
                              const std::string& rdr_info, int rdr_port)
{
  if (rdr_info.empty() || (rdr_port == 0)) {
    err_obj.setErrInfo(EINVAL, "no redirection info available");
    return false;
  }

  // If size < 2kb just set it directly
  if (rdr_info.length() < 2 * 1024) {
    err_obj.setErrInfo(rdr_port, rdr_info.c_str());
    return true;
  }

  // Otherwise use the XrdOucBuffPool to manage XrdOucBuffer objects that
  // can hold redirection info >= 2kb
  const uint32_t aligned_sz = eos::common::GetPowerCeil(rdr_info.length() + 1,
                              2 * eos::common::KB);
  XrdOucBuffer* buff = mXrdBuffPool.Alloc(aligned_sz);

  if (buff == nullptr) {
    eos_static_err("msg=\"requested redirection buffer allocation size too "
                   "big\" req_sz=%llu max_sz=%i", rdr_info.length(),
                   mXrdBuffPool.MaxSize());
    err_obj.setErrInfo(EINVAL, "redirection buffer too big");
    return false;
  }

  (void) strcpy(buff->Buffer(), rdr_info.c_str());
  buff->SetLen(rdr_info.length() + 1);
  err_obj.setErrInfo(rdr_port, buff);
  return true;
}

//------------------------------------------------------------------------------
// Send query (XrdFileSystem::Query) to the given endpoint and collect the
// repsonse
//------------------------------------------------------------------------------
int
XrdMgmOfs::SendQuery(const std::string& hostname, int port,
                     const std::string& request, std::string& response,
                     uint16_t timeout)
{
  std::ostringstream oss;
  oss << "root://" << hostname << ":" << port << "/?xrd.wantprot=sss";
  XrdCl::URL url(oss.str());

  if (!url.IsValid()) {
    eos_static_err("msg=\"invalid url\" url=\"%s\"", oss.str().c_str());
    return EINVAL;
  }

  XrdCl::Buffer arg;
  XrdCl::Buffer* raw_resp {nullptr};
  XrdCl::FileSystem fs {url};
  arg.FromString(request);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                        raw_resp, timeout);
  std::unique_ptr<XrdCl::Buffer> resp(raw_resp);
  raw_resp = nullptr;

  if (!status.IsOK()) {
    eos_static_err("msg=\"failed query request\" request=\"%s\" status=\"%s\"",
                   request.c_str(), status.ToStr().c_str());
    return -1;
  }

  if (resp && resp->GetBuffer()) {
    response = resp->GetBuffer();
  }

  return 0;
}

//----------------------------------------------------------------------------
// Broadcast query (XrdFileSystem::Query) to the given endpoints and collect
// the responses
//----------------------------------------------------------------------------
int
XrdMgmOfs::BroadcastQuery(const std::string& request,
                          std::set<std::string>& endpoints,
                          std::map<std::string, std::pair<int, std::string>>&
                          responses, uint16_t timeout)
{
  std::atomic<int> g_retc = 0; // overall return code
  class QueryRespHandler: public XrdCl::ResponseHandler
  {
  public:
    //------------------------------------------------------------------------
    //! Constructor
    //------------------------------------------------------------------------
    QueryRespHandler(const std::string& endpoint,
                     std::map<std::string, std::pair<int, std::string>>& responses,
                     std::mutex& mutex, std::condition_variable& cv,
                     std::atomic<int>& retc):
      mEndpoint(endpoint), mRespMap(responses), mMutexMap(mutex), mCv(cv),
      mRetc(retc)
    {}

    //------------------------------------------------------------------------
    //! Called when a response to associated request arrives or an error
    //! occurs
    //!
    //! @param status   status of the request
    //! @param response an object associated with the response
    //------------------------------------------------------------------------
    void HandleResponse(XrdCl::XRootDStatus* status, XrdCl::AnyObject* response)
    {
      int retc = 0;
      std::string resp;

      if (status->IsOK()) {
        if (response) {
          XrdCl::Buffer* buffer {nullptr};
          response->Get(buffer);
          resp = buffer->GetBuffer();
        }
      } else {
        retc = (status->errNo ? status->errNo : ENOMSG);
        resp = status->GetErrorMessage();
        mRetc = 1;
      }

      if (response) {
        delete response;
      }

      delete status;
      {
        // Add info to the global map and notify main thread
        std::unique_lock<std::mutex> lock(mMutexMap);
        mRespMap.emplace(mEndpoint, std::make_pair(retc, std::move(resp)));
      }
      mCv.notify_one();
    }

  private:
    std::string mEndpoint;
    std::map<std::string, std::pair<int, std::string>>& mRespMap;
    std::mutex& mMutexMap;
    std::condition_variable& mCv;
    std::atomic<int>& mRetc;
  };

  // Collect all the FST endpoints if nothing specified
  if (endpoints.empty()) {
    endpoints = FsView::gFsView.CollectEndpoints("*");
  }

  size_t num_resp = endpoints.size();
  std::mutex mutex;
  std::condition_variable cv;
  std::map<XrdCl::FileSystem*, QueryRespHandler*> queries;

  for (const auto& ep : endpoints) {
    std::ostringstream oss;
    oss << "root://" << ep << "/?xrd.wantprot=sss";
    XrdCl::URL url(oss.str());

    if (!url.IsValid()) {
      eos_static_err("msg=\"invalid url\" url=\"%s\"", oss.str().c_str());
      std::unique_lock<std::mutex> lock(mutex);
      responses.emplace(ep, std::make_pair(EINVAL, "invalid url"));
      continue;
    }

    auto pair = queries.emplace(new XrdCl::FileSystem(url),
                                new QueryRespHandler(ep, responses, mutex, cv, g_retc));

    if (!pair.second) {
      eos_static_err("msg=\"failed to insert query\" endpoint=\"%s\"",
                     ep.c_str());
      std::unique_lock<std::mutex> lock(mutex);
      responses.emplace(ep, std::make_pair(EINVAL, "failed query insert"));
      continue;
    }

    //! const_cast
    auto* fs = pair.first->first;
    auto* handler = pair.first->second;
    XrdCl::Buffer arg;
    arg.FromString(request);
    XrdCl::XRootDStatus status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg,
                                           handler, timeout);

    if (!status.IsOK()) {
      eos_static_err("msg=\"failed to send query\" endpoint=\"%s\"",
                     ep.c_str());
      std::unique_lock<std::mutex> lock(mutex);
      responses.emplace(ep, std::make_pair(EINVAL, "failed query send"));
      continue;
    }
  }

  {
    // Wait for all the responses to be received
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] {return (num_resp == responses.size());});
  }

  // Clean up memory
  for (const auto& elem : queries) {
    delete elem.first;
    delete elem.second;
  }

  return g_retc;
}

//------------------------------------------------------------------------------
// Send a resync command for a file identified by id and filesystem
//------------------------------------------------------------------------------
int
XrdMgmOfs::QueryResync(eos::common::FileId::fileid_t fid,
                       eos::common::FileSystem::fsid_t fsid, bool force)
{
  int fst_port;
  std::string fst_host;
  std::string fst_queue;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!fs) {
      eos_err("msg=\"no resync msg sent, no such file system\" fsid=%lu", fsid);
      return -1;
    }

    fst_host = fs->GetHost();
    fst_queue = fs->GetQueue();
    fst_port = fs->getCoreParams().getLocator().getPort();
  }
  EXEC_TIMING_BEGIN("QueryResync");
  gOFS->MgmStats.Add("QueryResync", vid.uid, vid.gid, 1);
  std::string request =
    SSTR("/?fst.pcmd=resync"
         << "&fst.resync.fsid=" << fsid
         << "&fst.resync.fxid=" << eos::common::FileId::Fid2Hex(fid)
         << "&fst.resync.force=" << force);
  std::string response;
  int query_retc = gOFS->SendQuery(fst_host, fst_port, request, response);
  (void) response;
  EXEC_TIMING_END("QueryResync");
  return query_retc;
}

//------------------------------------------------------------------------------
// Remove file/container metadata object that was already deleted before
// but now it's still in the namespace detached from any parent
//------------------------------------------------------------------------------
bool
XrdMgmOfs::RemoveDetached(uint64_t id, bool is_dir, bool force,
                          std::string& msg) const
{
  errno = 0;

  if (is_dir) {
    try {
      std::shared_ptr<eos::IContainerMD> cont = gOFS->eosDirectoryService->getContainerMD(id);
      eos::MDLocking::ContainerWriteLock contLock(cont);

      if (cont->getParentId()) {
        gOFS->eosDirectoryService->removeContainer(cont.get());
        return true;
      } else {
        msg = "error: container is attached id=" + std::to_string(id);
        return false;
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"caught exception\" errno=%d msg=\"%s\"",
                e.getErrno(), e.getMessage().str().c_str());
      msg = "error: " + e.getMessage().str() + '\n';
      return false;
    }
  } else {
    try {
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      std::shared_ptr<eos::IFileMD> file = gOFS->eosFileService->getFileMD(id);
      // Only one operation: no need to take a long file lock
      auto contId = file->getContainerId();

      if (contId) {
        // Double check if the parent container really exists. It could be
        // that the file is attached to a container which is already deleted.
        try {
          (void) gOFS->eosDirectoryService->getContainerMD(contId);
          msg = "error: file fxid=" + eos::common::FileId::Fid2Hex(id) +
                " is attached to cid=" + std::to_string(contId);
          return false;
        } catch (const eos::MDException& e) {
          // This means the parent container does not exist so we can safely
          // remove this file entry.
        }
      }

      // Write lock the file
      eos::MDLocking::FileWriteLock fileLock(file);
      // If any of the unlink locations is a file systems that doesn't exist
      // anymore then just remove it
      auto unlink_locs = file->getUnlinkedLocations();

      for (const auto& uloc : unlink_locs) {
        if (FsView::gFsView.mIdView.lookupByID(uloc) == nullptr) {
          file->removeLocation(uloc);
        }
      }

      // If there are no more locations we can also delete the file object
      if (file->getUnlinkedLocations().empty()) {
        gOFS->eosFileService->removeFile(file.get());
        msg = "info: file object removed from namespace";
      } else {
        // Move the unlinked locations to the locations list and back so
        // that we notify the listener for disk deletion
        unlink_locs = file->getUnlinkedLocations();

        for (const auto& uloc : unlink_locs) {
          file->addLocation(uloc);
        }

        file->unlinkAllLocations();

        if (force) {
          gOFS->eosFileService->removeFile(file.get());
          msg = "info: file force removed from namespace, best-effort disk "
                "deletion(s)";
        } else {
          msg = "info: file locations unlinked, waiting for disk deletion(s)";
        }
      }

      return true;
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"caught exception\" errno=%d msg=\"%s\"",
                e.getErrno(), e.getMessage().str().c_str());
      msg = "error: " + e.getMessage().str() + '\n';
      return false;
    }
  }
}

//----------------------------------------------------------------------------
// Query to determine if current node is acting as master
//----------------------------------------------------------------------------
int
XrdMgmOfs::IsMaster(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  static const char* epname = "IsMaster";

  // TODO (esindril): maybe enable SSS at some point
  // REQUIRE_SSS_OR_LOCAL_AUTH;

  if (!gOFS->mMaster->IsMaster()) {
    return Emsg(epname, error, ENOENT, "find master file [ENOENT]", "");
  }

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  return SFS_DATA;
}
