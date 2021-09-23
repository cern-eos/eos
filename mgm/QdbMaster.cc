/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "mgm/QdbMaster.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/Access.hh"
#include "mgm/WFE.hh"
#include "mgm/fsck/Fsck.hh"
#include "mgm/LRU.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/interface/INamespaceGroup.hh"
#include "common/plugin_manager/PluginManager.hh"
#include "common/IntervalStopwatch.hh"
#include <qclient/QClient.hh>

EOSMGMNAMESPACE_BEGIN

std::string QdbMaster::sLeaseKey {"master_lease"};

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QdbMaster::QdbMaster(const eos::QdbContactDetails& qdb_info,
                     const std::string& host_port):
  mOneOff(true), mIdentity(host_port), mMasterIdentity(),
  mIsMaster(false),  mConfigLoaded(false),
  mAcquireDelay(0)
{
  mQcl = std::make_unique<qclient::QClient>(qdb_info.members,
         qdb_info.constructOptions());
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
QdbMaster::~QdbMaster()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Init method to determine the current master/slave state
//------------------------------------------------------------------------------
bool
QdbMaster::Init()
{
  gOFS->NsInQDB = true;
  gOFS->mNamespaceState = NamespaceState::kBooting;
  mThread.reset(&QdbMaster::Supervisor, this);
  return true;
}

//------------------------------------------------------------------------------
// Boot namespace
//------------------------------------------------------------------------------
bool
QdbMaster::BootNamespace()
{
  using eos::common::PluginManager;
  // Load the namepace implementation from external library
  PluginManager& pm = PluginManager::GetInstance();
  PF_PlatformServices& pm_svc = pm.GetPlatformServices();
  pm_svc.invokeService = &XrdMgmOfs::DiscoverPlatformServices;
  gOFS->namespaceGroup.reset(static_cast<INamespaceGroup*>
                             (pm.CreateObject("NamespaceGroup")));
  // Collect namespace options, and initialize namespace group
  std::map<std::string, std::string> namespaceConfig;
  std::string err;

  if (gOFS->mQdbCluster.empty()) {
    eos_alert("%s", "msg=\"mgmofs.qdbcluster configuration is missing\"");
    gOFS->mNamespaceState = NamespaceState::kFailed;
    return false;
  }

  std::string instance_id =
    SSTR(gOFS->MgmOfsInstanceName << ":" << gOFS->ManagerPort);
  namespaceConfig["queue_path"] = "/var/eos/ns-queue/";
  namespaceConfig["qdb_cluster"] = gOFS->mQdbCluster;
  namespaceConfig["qdb_password"] = gOFS->mQdbPassword;
  namespaceConfig["qdb_flusher_md"] = SSTR(instance_id << "_md");
  namespaceConfig["qdb_flusher_quota"] = SSTR(instance_id << "_quota");
  fillNamespaceCacheConfig(gOFS->ConfEngine, namespaceConfig);

  if (!gOFS->namespaceGroup->initialize(&gOFS->eosViewRWMutex, namespaceConfig,
                                        err)) {
    eos_err("msg=\"could not initialize namespace group, err: %s\"", err.c_str());
    return false;
  }

  // Fetch all required services out of namespace group
  gOFS->eosDirectoryService = gOFS->namespaceGroup->getContainerService();
  gOFS->eosFileService = gOFS->namespaceGroup->getFileService();
  gOFS->eosView = gOFS->namespaceGroup->getHierarchicalView();
  gOFS->eosFsView = gOFS->namespaceGroup->getFilesystemView();
  gOFS->eosContainerAccounting =
    gOFS->namespaceGroup->getContainerAccountingView();
  gOFS->eosSyncTimeAccounting = gOFS->namespaceGroup->getSyncTimeAccountingView();

  if (!gOFS->eosDirectoryService || !gOFS->eosFileService || !gOFS->eosView ||
      !gOFS->eosFsView || !gOFS->eosContainerAccounting ||
      !gOFS->eosSyncTimeAccounting) {
    MasterLog(eos_log(LOG_ERR, "namespace implementation could not be loaded using "
                      "the provided library plugin - one of the required "
                      "namespace views could not be created"));
    gOFS->mNamespaceState = NamespaceState::kFailed;
    return false;
  }

  time_t tstart = time(nullptr);

  try {
    gOFS->eosDirectoryService->configure(namespaceConfig);
    gOFS->eosFileService->configure(namespaceConfig);
    gOFS->eosFsView->configure(namespaceConfig);
    gOFS->eosView->configure(namespaceConfig);
    gOFS->eosFileService->setQuotaStats(gOFS->eosView->getQuotaStats());
    gOFS->eosDirectoryService->setQuotaStats(gOFS->eosView->getQuotaStats());
    gOFS->eosView->getQuotaStats()->registerSizeMapper(Quota::MapSizeCB);
    gOFS->eosView->initialize1();
    gOFS->mBootContainerId = gOFS->eosDirectoryService->getFirstFreeId();
    MasterLog(eos_log(LOG_NOTICE,
                      "msg=\"container initialization done\" duration=%ds",
                      (time(nullptr) - tstart)));
  } catch (eos::MDException& e) {
    MasterLog(eos_log(LOG_NOTICE,
                      "msg=\"container initialization failed\" duration=%ds, "
                      "errc=%d, reason=\"%s\"", (time(nullptr) - tstart),
                      e.getErrno(), e.getMessage().str().c_str()));
    gOFS->mNamespaceState = NamespaceState::kFailed;
    return false;
  } catch (const std::runtime_error& qdb_err) {
    MasterLog(eos_log(LOG_NOTICE,
                      "msg=\"container initialization failed unable to connect to "
                      "QuarkDB cluster\" reason=\"%s\"", qdb_err.what()));
    gOFS->mNamespaceState = NamespaceState::kFailed;
    return false;
  }

  // Initialize the file view
  gOFS->mFileInitTime = time(nullptr);

  try {
    eos_notice("%s", "msg=\"eos file view initialize2 starting ...\"");
    eos::common::RWMutexWriteLock wr_view_lock(gOFS->eosViewRWMutex, __FUNCTION__,
        __LINE__, __FILE__);
    gOFS->eosView->initialize2();
    eos_notice("msg=\"file view initialize2 done\" duration=%ds",
               time(nullptr) - gOFS->mFileInitTime);
    gOFS->mBootFileId = gOFS->eosFileService->getFirstFreeId();
  } catch (eos::MDException& e) {
    eos_crit("msg=\"file view initialize2 failed\" duration=%ds, "
             "errc=%d reason=\"%s\"", (time(nullptr) - gOFS->mFileInitTime),
             e.getErrno(), e.getMessage().str().c_str());
    gOFS->mNamespaceState = NamespaceState::kFailed;
    return false;;
  }

  gOFS->namespaceGroup->startCacheRefreshListener();
  gOFS->mFileInitTime = time(nullptr) - gOFS->mFileInitTime;
  gOFS->mTotalInitTime = time(nullptr) - gOFS->mTotalInitTime;
  gOFS->mNamespaceState = NamespaceState::kBooted;
  eos_static_alert("msg=\"QDB namespace booted\"");

  // Get process status after boot
  if (!eos::common::LinuxStat::GetStat(gOFS->LinuxStatsStartup)) {
    eos_err("%s", "msg=\"failed to grab /proc/self/stat information\"");
  }

  while (mOneOff) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    eos_info("%s", "msg=\"wait for the supervisor to run once\"");
  }

  return true;
}

//------------------------------------------------------------------------------
// Configure QDB lease timeouts
//------------------------------------------------------------------------------
void
QdbMaster::ConfigureTimeouts(uint64_t& master_init_lease)
{
  if (getenv("EOS_QDB_MASTER_INIT_LEASE_MS")) {
    master_init_lease = std::stoull(getenv("EOS_QDB_MASTER_INIT_LEASE_MS"));
  }

  if (getenv("EOS_QDB_MASTER_LEASE_MS")) {
    mLeaseValidity = std::chrono::milliseconds
                     (std::stoull(getenv("EOS_QDB_MASTER_LEASE_MS")));

    if (mLeaseValidity > std::chrono::minutes(5)) {
      eos_warning("%s", "msg=\"QDB master lease validity set to the "
                  "maximum of 5 minutes\"");
      mLeaseValidity = std::chrono::minutes(5);
    }

    if (master_init_lease < (uint64_t)mLeaseValidity.count()) {
      eos_warning("%s", "msg=\"QDB master init lease validity modified"
                  " to the value of the QDB master lease\"");
      master_init_lease = mLeaseValidity.count();
    }
  }
}

//------------------------------------------------------------------------------
// Thread supervising the master/slave status
//------------------------------------------------------------------------------
void
QdbMaster::Supervisor(ThreadAssistant& assistant) noexcept
{
  bool new_is_master = false;
  std::string old_master_id;
  uint64_t master_init_lease = 30000; // 30 seconds
  ConfigureTimeouts(master_init_lease);
  eos_notice("%s", "msg=\"set up booting stall rule\"");
  RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE);
  Access::StallInfo old_stall;
  Access::StallInfo new_stall("*", "100", "namespace is booting", true);
  Access::SetStallRule(new_stall, old_stall);
  // @todo (esindril) handle case when config contains stall rules

  // Wait for the namespace to boot and the config to load
  while ((gOFS->mNamespaceState != NamespaceState::kBooted) &&
         !assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(1));
    eos_info("msg=\"waiting for namespace boot\" mNamespaceState=%s",
             namespaceStateToString(gOFS->mNamespaceState).c_str());
  }

  // Loop updating the master status
  while (!assistant.terminationRequested()) {
    old_master_id = GetMasterId();
    new_is_master = AcquireLeaseWithDelay();
    UpdateMasterId(GetLeaseHolder());
    eos_info("old_is_master=%s, is_master=%s, old_master_id=%s, master_id=%s",
             mIsMaster.load() ? "true" : "false",
             new_is_master ? "true" : "false",
             old_master_id.c_str(), GetMasterId().c_str());

    // Run one-off after boot
    if (mOneOff) {
      if (new_is_master) {
        // Increase the lease validity for the transition
        if (!AcquireLease(master_init_lease)) {
          eos_err("%s", "msg=\"failed to renew lease during transition\"");
          continue;
        }

        SlaveToMaster();
      } else {
        MasterToSlave();
      }

      eos_notice("%s", "msg=\"remove booting stall rule\"");
      Access::StallInfo dummy_stall;
      Access::SetStallRule(old_stall, dummy_stall);
      mOneOff = false;
    } else {
      // There was a master-slave transition
      if (mIsMaster != new_is_master) {
        if (mIsMaster) {
          MasterToSlave();
        } else {
          // Increase the lease validity for the transition
          if (!AcquireLease(master_init_lease)) {
            eos_err("%s", "msg=\"failed to renew lease during transition\"");
            continue;
          }

          SlaveToMaster();
        }
      } else {
        std::string new_master_id = GetMasterId();

        // Update new master if we released the lease on purpose
        if (!new_is_master && (new_master_id == mIdentity)) {
          new_master_id.clear();
        }

        // There was a change in the master identity or the current master
        // could not update the lease
        if (!new_master_id.empty() && (old_master_id != new_master_id) &&
            (new_master_id != mIdentity)) {
          Access::SetMasterToSlaveRules(new_master_id);
        }
      }
    }

    // If there is a master then wait a bit
    if (!GetMasterId().empty()) {
      std::chrono::milliseconds wait_ms(mLeaseValidity.count() / 2);
      assistant.wait_for(wait_ms);
    }
  }

  RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE);
}

//------------------------------------------------------------------------------
// Slave to master transition
//------------------------------------------------------------------------------
void
QdbMaster::SlaveToMaster()
{
  eos_info("%s", "msg=\"start slave to master transition\"");
  Access::StallInfo old_stall; // to be discarded
  Access::StallInfo new_stall("*", "5", "slave->master transition", true);
  Access::SetStallRule(new_stall, old_stall);
  gOFS->mTracker.SetAcceptingRequests(false);
  gOFS->mTracker.SpinUntilNoRequestsInFlight(true,
      std::chrono::milliseconds(100));
  // Force refresh the inode provider to get the latest inode values from QDB
  gOFS->eosFileService->configure({{constants::sKeyInodeRefresh, "true"}});
  gOFS->eosFileService->initialize();
  gOFS->eosDirectoryService->initialize();
  std::string std_out, std_err;
  // We are the master and we broadcast every configuration change
  gOFS->ObjectManager.EnableBroadCast(true);

  if (!ApplyMasterConfig(std_out, std_err, Transition::kSlaveToMaster)) {
    eos_err("%s", "msg=\"failed to apply master configuration\"");
    std::abort();
  }

  Quota::LoadNodes();
  EnableNsCaching();
  WFE::MoveFromRBackToQ();
  // Notify all the nodes about the new master identity
  FsView::gFsView.BroadcastMasterId(GetMasterId());
  mIsMaster = true;

  if (gOFS->mConverterDriver) {
    gOFS->mConverterDriver->Start();
  }

  gOFS->mLRUEngine->Start();
  Access::RemoveStallRule("*");
  Access::SetSlaveToMasterRules();
  gOFS->mTracker.SetAcceptingRequests(true);
  CreateStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE);

  // Start tape garbage collector, only if tape is configured and enabled
  if (gOFS->mTapeEnabled) {
    try {
      gOFS->mTapeGc->start();
    } catch (std::exception& ex) {
      std::ostringstream msg;
      msg << "msg=\"Failed to start tape-aware garbage collection: " << ex.what() << "\"";
      eos_crit(msg.str().c_str());
      std::abort();
    } catch (...) {
      eos_crit("msg=\"Failed to start tape-aware garbage collection: Caught an unknown exception\"");
      std::abort();
    }
  }

  eos_info("%s", "msg=\"finished slave to master transition\"");
}

//------------------------------------------------------------------------------
// Master to slave transition
//------------------------------------------------------------------------------
void
QdbMaster::MasterToSlave()
{
  eos_info("%s", "msg=\"master to slave transition\"");
  RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE);
  mIsMaster = false;
  UpdateMasterId("");
  gOFS->mDrainEngine.Stop();
  gOFS->mFsckEngine->Stop();
  gOFS->mLRUEngine->Stop();

  if (gOFS->mConverterDriver) {
    gOFS->mConverterDriver->Stop();
  }

  Access::StallInfo old_stall; // to be discarded
  Access::StallInfo new_stall("*", "5", "master->slave transition", true);
  Access::SetStallRule(new_stall, old_stall);
  gOFS->mTracker.SetAcceptingRequests(false);
  gOFS->mTracker.SpinUntilNoRequestsInFlight(true,
      std::chrono::milliseconds(100));
  // We are the slave, we just listen and don't broadcast anything
  gOFS->ObjectManager.EnableBroadCast(false);
  DisableNsCaching();

  // When we boot the first time also load the config
  if (mOneOff) {
    std::string std_out, std_err;

    if (!ApplyMasterConfig(std_out, std_err, Transition::kSlaveToMaster)) {
      eos_err("%s", "msg=\"failed to apply configuration\"");
      std::abort();
    }
  }

  // Stop the tape garbage collector if tape is configured and enabled
  if (gOFS->mTapeEnabled) {
    try {
      gOFS->mTapeGc->stop();
    } catch (std::exception& ex) {
      std::ostringstream msg;
      msg << "msg=\"Failed to stop tape-aware garbage collection: " << ex.what() << "\"";
      eos_err(msg.str().c_str());
    } catch (...) {
      eos_err("msg=\"Failed to stop tape-aware garbage collection: Caught an unknown exception\"");
    }
  }

  gOFS->mTracker.SetAcceptingRequests(true);
}

//------------------------------------------------------------------------------
// Apply configuration setting
//------------------------------------------------------------------------------
bool
QdbMaster::ApplyMasterConfig(std::string& stdOut, std::string& stdErr,
                             Transition::Type transitiontype)
{
  static std::mutex sequential_mutex;
  std::unique_lock<std::mutex> lock(sequential_mutex);
  gOFS->mFsckEngine->Stop();
  gOFS->mDrainEngine.Stop();
  gOFS->mDrainEngine.Start();
  gOFS->ConfEngine->SetConfigDir(gOFS->MgmConfigDir.c_str());
  // Take care of setting the config engine for FsView to null while applying
  // the config otherwise we deadlock since the FsView will try to set config
  // keys
  eos::mgm::ConfigResetMonitor fsview_cfg_reset_monitor;

  if (gOFS->MgmConfigAutoLoad.length()) {
    eos_static_info("autoload config=%s", gOFS->MgmConfigAutoLoad.c_str());
    std::string configenv = gOFS->MgmConfigAutoLoad.c_str();
    XrdOucString stdErr = "";

    if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr, false)) {
      eos_crit("msg=\"failed config autoload\" config=\"%s\" err=\"%s\"",
               gOFS->MgmConfigAutoLoad.c_str(), stdErr.c_str());
    } else {
      mConfigLoaded = true;
      eos_static_info("msg=\"successful config autoload\" config=\"%s\"",
                      gOFS->MgmConfigAutoLoad.c_str());
    }
  }

  gOFS->SetupGlobalConfig();
  return mConfigLoaded;
}

//------------------------------------------------------------------------------
// Try to acquire lease
//------------------------------------------------------------------------------
bool
QdbMaster::AcquireLease(uint64_t validity_msec)
{
  using eos::common::StringConversion;
  std::string timeout = std::to_string(validity_msec ? validity_msec :
                                       mLeaseValidity.count());
  eos::common::IntervalStopwatch stop_watch;
  std::future<qclient::redisReplyPtr> f =
    mQcl->exec("lease-acquire", sLeaseKey, mIdentity, timeout);
  qclient::redisReplyPtr reply = f.get();
  eos_info("msg=\"qclient acquire lease call took %llums\"",
           stop_watch.timeIntoCycle().count());

  if (reply == nullptr) {
    return false;
  }

  std::string reply_msg(reply->str, reply->len);

  if ((reply_msg == "ACQUIRED") ||
      (reply_msg == "RENEWED")) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Try to acquire lease with delay. If the mAcquireDelay timestamp is set
// then we skip trying to acquire the lease until the delay has expired.
//------------------------------------------------------------------------------
bool
QdbMaster::AcquireLeaseWithDelay()
{
  bool is_master = false;

  if (mAcquireDelay != 0) {
    if (mAcquireDelay >= time(nullptr)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      eos_info("%s", "msg=\"enforce lease acquire delay\"");
    } else {
      mAcquireDelay = 0;
      is_master = AcquireLease();
    }
  } else {
    is_master = AcquireLease();
  }

  return is_master;
}


//----------------------------------------------------------------------------
// Release lease
//----------------------------------------------------------------------------
void
QdbMaster::ReleaseLease()
{
  std::future<qclient::redisReplyPtr> f = mQcl->exec("lease-release", sLeaseKey);
  qclient::redisReplyPtr reply = f.get();
  (void) reply;
}

//----------------------------------------------------------------------------
// Get the identity of the current lease holder
//----------------------------------------------------------------------------
std::string
QdbMaster::GetLeaseHolder()
{
  std::string holder;
  std::future<qclient::redisReplyPtr> f = mQcl->exec("lease-get", sLeaseKey);
  qclient::redisReplyPtr reply = f.get();

  if ((reply == nullptr) || (reply->type == REDIS_REPLY_NIL)) {
    eos_err("%s", "msg=\"lease-get is NULL\"");
    return holder;
  }

  std::string reply_msg = std::string(reply->element[0]->str,
                                      reply->element[0]->len);
  eos_debug("lease-get reply: %s", reply_msg.c_str());
  std::string tag {"HOLDER: "};
  size_t pos = reply_msg.find(tag);

  if (pos == std::string::npos) {
    return holder;
  }

  pos += tag.length();
  size_t pos_end = reply_msg.find('\n', pos);

  if (pos_end == std::string::npos) {
    holder = reply_msg.substr(pos);
  } else {
    holder = reply_msg.substr(pos, pos_end - pos + 1);
  }

  return holder;
}

//------------------------------------------------------------------------------
// Set the new master hostname
//------------------------------------------------------------------------------
bool
QdbMaster::SetMasterId(const std::string& hostname, int port,
                       std::string& err_msg)
{
  using namespace std::chrono;
  std::string new_id = hostname + std::to_string(port);

  if (mIsMaster) {
    if (new_id != mIdentity) {
      // Introduce delay in acquiring the lease so that we give the opportunity
      // to other nodes to become the master
      mAcquireDelay = time(nullptr) + 2 *
                      duration_cast<seconds>(mLeaseValidity).count();
    }
  } else {
    err_msg = "error: currently this node is not acting as a master";
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if remove master is OK
//----------------------------------------------------------------------------
bool
QdbMaster::IsRemoteMasterOk() const
{
  std::string master_id = GetMasterId();

  // If we're master or remote master id is empty then fail
  if ((mIsMaster && (master_id == mIdentity)) || master_id.empty()) {
    return false;
  }

  std::ostringstream oss;
  oss << "root://" << master_id << "//dummy?xrd.wantprot=sss,unix";
  XrdCl::URL url(oss.str());

  if (!url.IsValid()) {
    eos_err("msg=\"invalid remote master\" id=%s", master_id.c_str());
    return false;
  }

  // Check if node is reachable
  XrdCl::FileSystem fs(url);
  XrdCl::XRootDStatus st = fs.Ping(1);

  if (!st.IsOK()) {
    eos_err("msg=\"remote master not reachable\" id=%s", master_id.c_str());
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Show the current master/slave run configuration (used by ns stat)
//------------------------------------------------------------------------------
std::string
QdbMaster::PrintOut()
{
  std::ostringstream oss;
  oss << "is_master=" << (mIsMaster ? "true" : "false")
      << " master_id=" << GetMasterId();
  return oss.str();
}

//------------------------------------------------------------------------------
// Disable namespace caching
//------------------------------------------------------------------------------
void
QdbMaster::DisableNsCaching()
{
  std::map<std::string, std::string> map_cfg;
  map_cfg[constants::sMaxNumCacheFiles] = "0";
  map_cfg[constants::sMaxNumCacheDirs] = "0";
  gOFS->eosFileService->configure(map_cfg);
  gOFS->eosDirectoryService->configure(map_cfg);
}

//------------------------------------------------------------------------------
// Enable namespace caching with default values
//------------------------------------------------------------------------------
void
QdbMaster::EnableNsCaching()
{
  std::map<std::string, std::string> map_cfg;
  fillNamespaceCacheConfig(gOFS->ConfEngine, map_cfg);
  gOFS->eosFileService->configure(map_cfg);
  gOFS->eosDirectoryService->configure(map_cfg);
}

EOSMGMNAMESPACE_END
