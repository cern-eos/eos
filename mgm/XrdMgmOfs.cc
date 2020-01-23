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
#include "namespace/Constants.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/RenameSafetyCheck.hh"
#include "namespace/utils/Attributes.hh"
#include "authz/XrdCapability.hh"
#include "grpc/GrpcServer.hh"
#include "mgm/AdminSocket.hh"
#include "mgm/Stat.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/XrdMgmOfs.hh"
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
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/Recycle.hh"
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
#include "mgm/Master.hh"
#include "mgm/tracker/ReplicationTracker.hh"
#include "mgm/inspector/FileInspector.hh"
#include "mgm/XrdMgmOfs/fsctl/CommitHelper.hh"
#include "mgm/QueryPrepareResponse.hh"
#include "mq/SharedHashWrapper.hh"
#include "mq/FileSystemChangeListener.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/utils/Stat.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/Etag.hh"
#include "namespace/utils/Attributes.hh"
#include <XrdVersion.hh>
#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucBuffer.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucTokenizer.hh>
#include <XrdOuc/XrdOucTrace.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdSec/XrdSecInterface.hh>
#include <XrdSfs/XrdSfsAio.hh>
#include <XrdSfs/XrdSfsFlags.hh>
#include "google/protobuf/io/zero_copy_stream_impl.h"

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
  extern "C" void __gcov_flush();
#endif

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
      // initialize filesystems only once
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
    // Did we pass the "-2" option to the plugin in the config file?
    gOFS->IsFileSystem2 = false;
    // By default enable stalling and redirection
    gOFS->IsStall = true;
    gOFS->IsRedirect = true;
    myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : nullptr);

    if (myFS.Configure(gMgmOfsEroute)) {
      return nullptr;
    }

    // Initialize authorization module ServerAcc
    gOFS->CapabilityEngine = (XrdCapability*) XrdAccAuthorizeObject(lp, configfn,
                             nullptr);

    if (!gOFS->CapabilityEngine) {
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
    gOFS->IsFileSystem2 = true;

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
  ConfigFN(0), ConfEngine(0), CapabilityEngine(0),
  mCapabilityValidity(3600), MgmOfsMessaging(0),
  ManagerPort(1094), LinuxStatsStartup{0}, HostName(0), HostPref(0),
  mNamespaceState(NamespaceState::kDown), mFileInitTime(0),
  mTotalInitTime(time(nullptr)), mStartTime(time(nullptr)), Shutdown(false),
  mBootFileId(0), mBootContainerId(0), IsRedirect(true), IsStall(true),
  mAuthorize(false), mAuthLib(""), MgmRedirector(false),
  ErrorLog(true), eosDirectoryService(0), eosFileService(0), eosView(0),
  eosFsView(0), eosContainerAccounting(0), eosSyncTimeAccounting(0),
  mStatsTid(0), mFrontendPort(0), mNumAuthThreads(0),
  zMQ(nullptr), Authorization(0), MgmStatsPtr(new eos::mgm::Stat()),
  MgmStats(*MgmStatsPtr), mFsckEngine(new Fsck()), mMaster(nullptr),
  mRouting(new eos::mgm::PathRouting()), mLRUEngine(new eos::mgm::LRU()),
  WFEPtr(new eos::mgm::WFE()), WFEd(*WFEPtr), UTF8(false), mFstGwHost(""),
  mFstGwPort(0), mQdbCluster(""), mHttpdPort(8000),
  mFusexPort(1100), mGRPCPort(50051),
  mBalancingTracker(std::chrono::seconds(600), std::chrono::seconds(3600)),
  mDrainTracker(std::chrono::seconds(600), std::chrono::seconds(3600)),
  mJeMallocHandler(new eos::common::JeMallocHandler()),
  mDoneOrderlyShutdown(false)
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

  eos::common::LogId::SetSingleShotLogId();
  mZmqContext = new zmq::context_t(1);
  IoStats.reset(new eos::mgm::Iostat());
  // export pointer to this object ... sigh ...
  setenv("EOSMGMOFS", std::to_string((unsigned long long)this).c_str(), 1);

  if (mHttpdPort)  {
    mHttpd.reset(new eos::mgm::HttpServer(mHttpdPort));
  }

  if (mGRPCPort) {
    GRPCd.reset(new eos::mgm::GrpcServer(mGRPCPort));
  }

  EgroupRefresh.reset(new eos::mgm::Egroup());
  Recycler.reset(new eos::mgm::Recycle());
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdMgmOfs::~XrdMgmOfs()
{
  OrderlyShutdown();
  eos_warning("%s", "msg=\"finished destructor\"");
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
  eos_warning("%s", "msg=\"disable configuration engine autosave\"");
  ConfEngine->SetAutoSave(false);
  FsView::gFsView.SetConfigEngine(nullptr);
  eos_warning("%s", "msg=\"stop routing\"");

  if (mRouting) {
    mRouting.reset();
  }

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

  eos_warning("%s", "msg=\"stopping central drainning\"");
  mDrainEngine.Stop();
  eos_warning("%s", "msg=\"stopping geotree engine updater\"");
  gGeoTreeEngine.StopUpdater();

  if (IoStats) {
    eos_warning("%s", "msg=\"stopping and deleting IoStats\"");
    IoStats.reset();
  }

  eos_warning("%s", "msg=\"stopping fusex server\"");
  zMQ->gFuseServer.shutdown();

  if (zMQ) {
    delete zMQ;
    zMQ = nullptr;
  }

  eos_warning("%s", "msg=\"stopping FSCK service\"");
  mFsckEngine->Stop();
  eos_warning("%s", "msg=\"stopping messaging\"");

  if (MgmOfsMessaging) {
    delete MgmOfsMessaging;
    MgmOfsMessaging = nullptr;
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

  eos_warning("%s", "msg=\"stopping the transfer engine threads\"");
  gTransferEngine.Stop();
  eos_warning("%s", "msg=\"stopping fs listener thread\"");
  auto stop_fsconfiglistener = std::thread([&]() {
    mFsConfigTid.join();
  });
  // We now need to signal to the FsConfigListener thread to unblock it
  XrdMqSharedObjectChangeNotifier::Subscriber*
  subscriber = ObjectNotifier.GetSubscriberFromCatalog("fsconfiglistener", false);

  if (subscriber) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    XrdSysMutexHelper lock(subscriber->mSubjMtx);
    subscriber->mSubjSem.Post();
  }

  stop_fsconfiglistener.join();
  eos_warning("%s", "msg=\"stopping the shared object notifier thread\"");
  ObjectNotifier.Stop();
  eos_warning("%s", "msg=\"cleanup quota information\"");
  (void) Quota::CleanUp();
  eos_warning("%s", "msg=\"graceful shutdown of the FsView\"");
  FsView::gFsView.StopHeartBeat();
  FsView::gFsView.Clear();

  if (gOFS->ErrorLog) {
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
#include "XrdMgmOfs/Auth.cc"
#include "XrdMgmOfs/Chksum.cc"
#include "XrdMgmOfs/Chmod.cc"
#include "XrdMgmOfs/Chown.cc"
#include "XrdMgmOfs/Coverage.cc"
#include "XrdMgmOfs/DeleteExternal.cc"
#include "XrdMgmOfs/Exists.cc"
#include "XrdMgmOfs/Find.cc"
#include "XrdMgmOfs/FsConfigListener.cc"
#include "XrdMgmOfs/Fsctl.cc"
#include "XrdMgmOfs/Link.cc"
#include "XrdMgmOfs/Merge.cc"
#include "XrdMgmOfs/Mkdir.cc"
#include "XrdMgmOfs/PathMap.cc"
#include "XrdMgmOfs/Remdir.cc"
#include "XrdMgmOfs/Rename.cc"
#include "XrdMgmOfs/Rm.cc"
#include "XrdMgmOfs/SendResync.cc"
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

//------------------------------------------------------------------------------
// Returns the number of elements within the specified XrdOucTList
//------------------------------------------------------------------------------
static unsigned int countNbElementsInXrdOucTList(const XrdOucTList* listPtr)
{
  int count = 0;

  while (listPtr) {
    count++;
    listPtr = listPtr->next;
  }

  return count;
}

//-------------------------------------------------------------------------------------
// Prepare a file or query the status of a previous prepare request
//-------------------------------------------------------------------------------------
int
XrdMgmOfs::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error, const XrdSecEntity* client)
{
  if(pargs.opts & Prep_QUERY) {
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
XrdMgmOfs::_prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error, const XrdSecEntity* client)
{
  EXEC_TIMING_BEGIN("Prepare");
  eos_info("prepareOpts=\"%s\"", prepareOptsToString(pargs.opts).c_str());
  static const char* epname = "prepare";
  const char* tident = error.getErrUser();
  eos::common::VirtualIdentity vid;
  XrdOucTList* pptr = pargs.paths;
  XrdOucTList* optr = pargs.oinfo;
  std::string info;
  info = (optr ? (optr->text ? optr->text : "") : "");
  eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }
  {
    const int nbFilesInPrepareRequest = countNbElementsInXrdOucTList(pargs.paths);
    gOFS->MgmStats.Add("Prepare", vid.uid, vid.gid, nbFilesInPrepareRequest);
  }
  std::string cmd = "mgm.pcmd=event";
  std::list<std::pair<char**, char**>> pathsWithPrepare;
  // Initialise the request ID for the Prepare request to the one provided by XRootD
  XrdOucString reqid(pargs.reqid);
  // Validate the event type
  std::string event;
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5
  // Strip "quality of service" bits from pargs.opts so that only the action to
  // be taken is left
  const int pargsOptsQoS = Prep_PMASK | Prep_SENDAOK | Prep_SENDERR | Prep_SENDACK
                           | Prep_WMODE | Prep_COLOC | Prep_FRESH;
  const int pargsOptsAction = pargs.opts & ~pargsOptsQoS;

  // The XRootD prepare actions are mutually exclusive
  switch (pargsOptsAction) {
  case 0: // No flags means stage file in from tape
  case Prep_STAGE:
    event = "sync::prepare";

    if (gOFS->IsFileSystem2) {
      // Override the XRootD-supplied request ID. The request ID can be any arbitrary string, so long as
      // it is guaranteed to be unique for each request.
      //
      // Note: To use the default request ID supplied in pargs.reqid, return SFS_OK instead of SFS_DATA.
      //       Overriding is only possible in the case of PREPARE. In the case of ABORT and QUERY requests,
      //       pargs.reqid should contain the request ID that was returned by the corresponding PREPARE.
      // This is a placeholder. To use this feature, EOS should generate a unique ID here.
      reqid = "eos:" + reqid;
    }
    break;

  case Prep_CANCEL:
    event = "sync::abort_prepare";
    break;

  case Prep_EVICT:
    event = "sync::evict_prepare";
    break;

  default:
    // More than one flag was set or there is an unknown flag
    Emsg(epname, error, EINVAL, "prepare - invalid value for pargs.opts =",
         std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#else

  // The XRootD prepare flags are mutually exclusive
  switch (pargs.opts) {
  case 0: // No flags means stage file in from tape
  case Prep_STAGE:
    event = "sync::prepare";
    break;

  case Prep_FRESH:
    event = "sync::abort_prepare";
    break;

  default:
    // More than one flag was set or there is an unknown flag
    Emsg(epname, error, EINVAL, "prepare - invalid value for pargs.opts =",
         std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#endif

  // check that all files exist
  while (pptr) {
    XrdOucString prep_path = (pptr->text ? pptr->text : "");
    std::string orig_path = prep_path.c_str();
    eos_info("msg =\"checking file exists\" path=\"%s\"", prep_path.c_str());
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      prep_path = path;
    }
    {
      const char* path = prep_path.c_str();
      const char* ininfo = "";
      MAYREDIRECT;
    }
    XrdSfsFileExistence check;

    if (prep_path.length() == 0) {
      Emsg(epname, error, ENOENT,
           "prepare - path empty or uses forbidden characters:",
           orig_path.c_str());
      return SFS_ERROR;
    }

    if (_exists(prep_path.c_str(), check, error, client, "") ||
        (check != XrdSfsFileExistIsFile)) {
      if (check != XrdSfsFileExistIsFile) {
        Emsg(epname, error, ENOENT,
             "prepare - file does not exist or is not accessible to you",
             prep_path.c_str());
      }

      return SFS_ERROR;
    }

    eos::IContainerMD::XAttrMap attributes;

    if (!event.empty() &&
        _attr_ls(eos::common::Path(prep_path.c_str()).GetParentPath(), error, vid,
                 nullptr, attributes) == 0) {
      bool foundPrepareTag = false;
      std::string eventAttr = "sys.workflow." + event;

      for (const auto& attrEntry : attributes) {
        foundPrepareTag |= attrEntry.first.find(eventAttr) == 0;
      }

      if (foundPrepareTag) {
        pathsWithPrepare.emplace_back(&(pptr->text),
                                      optr != nullptr ? & (optr->text) : nullptr);
      } else {
        // don't do workflow if no such tag
        pptr = pptr->next;

        if (optr) {
          optr = optr->next;
        }

        continue;
      }
    } else {
      // don't do workflow if event not set or we can't check attributes
      pptr = pptr->next;

      if (optr) {
        optr = optr->next;
      }

      continue;
    }

    // check that we have write permission on path
    if (gOFS->_access(prep_path.c_str(), W_OK | P_OK, error, vid, "")) {
      return Emsg(epname, error, EPERM,
                  "prepare - you don't have write and workflow permission",
                  prep_path.c_str());
    }

    pptr = pptr->next;

    if (optr) {
      optr = optr->next;
    }
  }

  for (auto& pathPair : pathsWithPrepare) {
    XrdOucString prep_path = (*pathPair.first ? *pathPair.first : "");
    XrdOucString prep_info = pathPair.second != nullptr ? (*pathPair.second ?
                             *pathPair.second : "") : "";
    eos_info("msg=\"about to trigger WFE\" path=\"%s\" info=\"%s\"",
             prep_path.c_str(), prep_info.c_str());
    XrdOucEnv prep_env(prep_info.c_str());
    prep_info = cmd.c_str();
    prep_info += "&mgm.event=";
    prep_info += event.c_str();
    prep_info += "&mgm.workflow=";

    if (prep_env.Get("eos.workflow")) {
      prep_info += prep_env.Get("eos.workflow");
    } else {
      prep_info += "default";
    }

    prep_info += "&mgm.fid=0&mgm.path=";
    prep_info += prep_path.c_str();
    prep_info += "&mgm.logid=";
    prep_info += this->logId;
    prep_info += "&mgm.ruid=";
    prep_info += (int)vid.uid;
    prep_info += "&mgm.rgid=";
    prep_info += (int)vid.gid;
    prep_info += "&mgm.reqid=";
    prep_info += reqid.c_str();

    if (prep_env.Get("activity")) {
      prep_info += "&activity=";
      prep_info += prep_env.Get("activity");
    }

    XrdSecEntity lClient(vid.prot.c_str());
    lClient.name = (char*) vid.name.c_str();
    lClient.tident = (char*) vid.tident.c_str();
    lClient.host = (char*) vid.host.c_str();
    XrdOucString lSec = "&mgm.sec=";
    lSec += eos::common::SecEntity::ToKey(&lClient,
                                          "eos").c_str();
    prep_info += lSec;
    XrdSfsFSctl args;
    args.Arg1 = prep_path.c_str();
    args.Arg1Len = prep_path.length();
    args.Arg2 = prep_info.c_str();
    args.Arg2Len = prep_info.length();
    auto ret_wfe = XrdMgmOfs::FSctl(SFS_FSCTL_PLUGIN, args, error, &lClient);

    // Log errors but continue to process the rest of the files in the list
    if (ret_wfe != SFS_DATA) {
      eos_err("Unable to prepare - synchronous prepare workflow error %s; %s",
              prep_path.c_str(), error.getErrText());
    }
  }

  int retc = SFS_OK;
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5

  // If we generated our own request ID, return it to the client
  if (gOFS->IsFileSystem2 && (pargs.opts & Prep_STAGE)) {
    // If we return SFS_DATA, the first parameter is the length of the buffer, not the error code
    error.setErrInfo(reqid.length() + 1, reqid.c_str());
    retc = SFS_DATA;
  }

#endif
  EXEC_TIMING_END("Prepare");
  return retc;
}


//-------------------------------------------------------------------------------------------
// Query the status of a previous prepare request
//-------------------------------------------------------------------------------------------
int
XrdMgmOfs::_prepare_query(XrdSfsPrep& pargs, XrdOucErrInfo& error, const XrdSecEntity* client)
{
  EXEC_TIMING_BEGIN("QueryPrepare");
  ACCESSMODE_R;

  eos::common::VirtualIdentity vid;
  {
    const char* tident = error.getErrUser();
    XrdOucTList *optr = pargs.oinfo;
    std::string info(optr && optr->text ? optr->text : "");
    eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
  }
  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }
  int path_cnt = 0;
  for(XrdOucTList *pptr = pargs.paths; pptr; pptr = pptr->next) ++path_cnt;
  gOFS->MgmStats.Add("QueryPrepare", vid.uid, vid.gid, path_cnt);

  // ID of the original prepare request. We don't need this to look up the list of files in
  // the request, as they are provided in the arguments. Anyway we return it in the reply
  // as a convenience for the client to track which prepare request the query applies to.
  XrdOucString reqid(pargs.reqid);

  std::vector<QueryPrepareResponse> response;

  // Set the response for each file in the list
  for(XrdOucTList *pptr = pargs.paths; pptr; pptr = pptr->next) {
    if(!pptr->text) continue;
    response.push_back(QueryPrepareResponse(pptr->text));
    auto &rsp = response.back();

    // check if the file exists
    XrdOucString prep_path;
    {
      const char* inpath = rsp.path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      prep_path = path;
    }
    {
      const char* path = rsp.path.c_str();
      const char* ininfo = "";
      MAYREDIRECT;
    }
    if(prep_path.length() == 0) {
      rsp.error_text = "path empty or uses forbidden characters";
      continue;
    }
    XrdSfsFileExistence check;
    if(_exists(prep_path.c_str(), check, error, client, "") || check != XrdSfsFileExistIsFile) {
      rsp.error_text = "file does not exist or is not accessible to you";
      continue;
    }
    rsp.is_exists = true;

    // Check file state (online/offline)
    XrdOucErrInfo xrd_error;
    struct stat buf;
    if(_stat(rsp.path.c_str(), &buf, xrd_error, vid, nullptr, nullptr, false)) {
      rsp.error_text = xrd_error.getErrText();
      continue;
    }
    _stat_set_flags(&buf);
    rsp.is_online = !(buf.st_rdev & XRDSFS_OFFLINE);

    // Check file status in the extended attributes
    eos::IFileMD::XAttrMap xattrs;
    if(_attr_ls(eos::common::Path(prep_path.c_str()).GetPath(), xrd_error, vid, nullptr, xattrs) == 0) {
      auto xattr_it = xattrs.find(eos::common::RETRIEVE_REQID_ATTR_NAME);
      if(xattr_it != xattrs.end()) {
        // has file been requested? (not necessarily with this request ID)
        rsp.is_requested = !xattr_it->second.empty();
        // and is this specific request ID present in the request?
        rsp.is_reqid_present = (xattr_it->second.find(reqid.c_str()) != string::npos);
      }
      xattr_it = xattrs.find(eos::common::RETRIEVE_REQTIME_ATTR_NAME);
      if(xattr_it != xattrs.end()) rsp.request_time = xattr_it->second;
      xattr_it = xattrs.find(eos::common::RETRIEVE_ERROR_ATTR_NAME);
      if(xattr_it != xattrs.end()) rsp.error_text = xattr_it->second;
    } else {
      // failed to read extended attributes
      rsp.error_text = xrd_error.getErrText();
      continue;
    }
  }

  // Build a JSON reply in the following format :
  // { request ID, [ array of response objects, one for each file ] }
  std::stringstream json_ss;
  json_ss << "{"
          << "\"request_id\":\"" << reqid << "\","
          << "\"responses\":[";

  bool is_first(true);
  for(auto &r : response) {
    if(is_first) { is_first = false; } else { json_ss << ","; }
    json_ss << r;
  }

  json_ss << "]"
          << "}";

  // Send the reply. XRootD requires that we put it into a buffer that can be released with free().
  auto  json_len = json_ss.str().length();
  char* json_buf = reinterpret_cast<char*>(malloc(json_len));
  strncpy(json_buf, json_ss.str().c_str(), json_len);
  // Ownership of this buffer is passed to xrd_buff which has a Recycle() method.
  XrdOucBuffer* xrd_buff = new XrdOucBuffer(json_buf, json_len);
  // Ownership of xrd_buff is passed to error. Note that as we are returning SFS_DATA, the first
  // parameter is the buffer length rather than an error code.
  error.setErrInfo(xrd_buff->BuffSize(), xrd_buff);

  EXEC_TIMING_END("QueryPrepare");
  return SFS_DATA;
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
                    int& port)
{
  EPNAME("Redirect");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Redirect " << host << ":" << port);
  // Place the error message in the error object and return
  error.setErrInfo(port, host);
  return SFS_REDIRECT;
}

//------------------------------------------------------------------------------
// Statistics circular buffer thread startup function
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StartMgmStats(void* pp)
{
  XrdMgmOfs* ofs = (XrdMgmOfs*) pp;
  ofs->MgmStats.Circulate();
  return 0;
}

//------------------------------------------------------------------------------
// Start a thread that will queue, build and submit backup operations to the
// archiver daemon.
//------------------------------------------------------------------------------
void
XrdMgmOfs::StartArchiveSubmitter(ThreadAssistant& assistant) noexcept
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
    // TODO (esindril): Use this code when we drop SLC6 support
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
// Cast a change message to all fusex clients regarding a file change
//------------------------------------------------------------------------------
void
XrdMgmOfs::FuseXCastFile(eos::FileIdentifier id)
{
  gOFS->zMQ->gFuseServer.Cap().BroadcastReleaseFromExternal(
    eos::common::FileId::FidToInode(id.getUnderlyingUInt64()));
}

//------------------------------------------------------------------------------
// Cast a change message to all fusex clients regarding a container change
//------------------------------------------------------------------------------
void
XrdMgmOfs::FuseXCastContainer(eos::ContainerIdentifier id)
{
  gOFS->zMQ->gFuseServer.Cap().BroadcastReleaseFromExternal(
    id.getUnderlyingUInt64());
}

//------------------------------------------------------------------------------
// Cast a change message to all fusex clients about a deletion of an entry
//------------------------------------------------------------------------------
void
XrdMgmOfs::FuseXCastDeletion(eos::ContainerIdentifier id,
                             const std::string& name)
{
  gOFS->zMQ->gFuseServer.Cap().BroadcastDeletionFromExternal(
    id.getUnderlyingUInt64(), name);
}


//------------------------------------------------------------------------------
// Cast a refresh message to all fusex clients about a refresh of an entry
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

//----------------------------------------------------------------------------
// Check if name space is booted
//----------------------------------------------------------------------------
bool
XrdMgmOfs::IsNsBooted() const
{
  return ((mNamespaceState == NamespaceState::kBooted) ||
          (mNamespaceState == NamespaceState::kCompacting));
}

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
XrdMgmOfs::WriteRmRecord(const std::shared_ptr<eos::IFileMD>& fmd)
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
           "dsize=%lu&sec.app=rm",
           this->logId, gOFS->ManagerId.c_str(), (unsigned long long) fmd->getId(),
           (unsigned long long) fmd->getId(), fmd->getCUid(), fmd->getCGid(),
           ts_now.tv_sec, ts_now.tv_nsec, ctime.tv_sec, ctime.tv_nsec,
           mtime.tv_sec, mtime.tv_nsec, fmd->getSize());
  std::string record = report;
  IoStats->WriteRecord(record);
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
  IoStats->WriteRecord(record);
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
        (v_rc[i] == std::string(terr))) {
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
