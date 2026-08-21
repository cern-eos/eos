// ----------------------------------------------------------------------
// File: FileSystem.cc
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

#include "mgm/filesystem/FileSystem.hh"
#include "common/Constants.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mq/FsChangeListener.hh"
#include "mq/MessagingRealm.hh"
#include "qclient/shared/SharedHashSubscription.hh"

EOSMGMNAMESPACE_BEGIN

const std::string FileSystem::sNumBalanceTxTag = "local.balancer.running";
const std::string FileSystem::sGeotagTag = "stat.geotag";
const std::string FileSystem::sErrcTag = "stat.errc";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystem::FileSystem(const common::FileSystemLocator& locator,
                       mq::MessagingRealm* msr) :
  eos::common::FileSystem(locator, msr)
{
  eos_static_info("msg=\"create file system\" queue_path=%s",
                  locator.getQueuePath().c_str());
  // Register with FsChangeListeners interested in key updates related
  // to this file system object
  RegisterWithExistingListeners();
  mq::SharedHashWrapper hash(mRealm, mHashLocator);
  mSharedHash = hash.getHash();
  mSubscription = hash.subscribe();

  if (mSubscription) {
    using namespace std::placeholders;
    mSubscription->attachCallback(std::bind(&FileSystem::ProcessUpdateCb,
                                            this, _1));
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileSystem::~FileSystem()
{
  // Make sure we wait for any ongoing callbacks
  if (mSubscription) {
    mSubscription->detachCallback();
  }

  UnregisterFromListeners();
}

//------------------------------------------------------------------------------
// Register with interested listeners - called when a new object is created
// and there are already existing FS listeners in the system
//------------------------------------------------------------------------------
void
FileSystem::RegisterWithExistingListeners()
{
  auto map_interests = mRealm->GetInterestedListeners(mLocator.getQueuePath());

  for (auto& elem : map_interests) {
    auto& fs_listener = elem.first;
    const auto& set_interests = elem.second;
    eos_static_info("msg=\"register with existing fs listener\" listener=%s "
                    "fs_queue_path=%s", fs_listener->GetName().c_str(),
                    mLocator.getQueuePath().c_str());
    eos::common::RWMutexWriteLock wr_lock(mRWMutex);

    for (const auto& interest : set_interests) {
      mMapListeners[interest].insert(fs_listener);
    }
  }
}

//------------------------------------------------------------------------------
// Unregister from all the listeners
//------------------------------------------------------------------------------
void
FileSystem::UnregisterFromListeners()
{
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);

  for (const auto& elem : mMapListeners) {
    for (auto& listener : elem.second) {
      eos_static_info("msg=\"unsubscribe and detach from listener\" "
                      "interest=\"%s\" listener_name=\"%s\" fs_queue_path=%s ",
                      elem.first.c_str(), listener->GetName().c_str(),
                      mLocator.getQueuePath().c_str());
      listener->unsubscribe(mLocator.getQueuePath(), {elem.first});
    }
  }

  mMapListeners.clear();
}

//------------------------------------------------------------------------------
// Attach file system change listener
//------------------------------------------------------------------------------
bool
FileSystem::AttachFsListener(std::shared_ptr<eos::mq::FsChangeListener>
                             fs_listener,
                             const std::set<std::string>& interests)
{
  if ((fs_listener == nullptr) || interests.empty()) {
    return false;
  }

  eos_static_info("msg=\"attaching fs listener\" listener_name=%s "
                  "fs_queue_path=%s", fs_listener->GetName().c_str(),
                  mLocator.getQueuePath().c_str());
  // Update the listener
  fs_listener->subscribe(mLocator.getQueuePath(), interests);
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);

  for (const auto& interest : interests) {
    mMapListeners[interest].insert(fs_listener);
  }

  return true;
}

//------------------------------------------------------------------------------
// Detach file system change listener
//------------------------------------------------------------------------------
bool
FileSystem::DetachFsListener(std::shared_ptr<eos::mq::FsChangeListener>
                             fs_listener,
                             const std::set<std::string>& interests)
{
  if ((fs_listener == nullptr) || interests.empty()) {
    return false;
  }

  eos_static_info("msg=\"detaching fs listener\" listener_name=%s "
                  "fs_queue_path=%s", fs_listener->GetName().c_str(),
                  mLocator.getQueuePath().c_str());
  // Update the listener
  (void) fs_listener->unsubscribe(mLocator.getQueuePath(), interests);
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);

  for (const auto& interest : interests) {
    auto it = mMapListeners.find(interest);

    // Erase listener
    if (it != mMapListeners.end()) {
      it->second.erase(fs_listener);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Notify file system change listener interested in the given update
//------------------------------------------------------------------------------
void
FileSystem::NotifyFsListener(qclient::SharedHashUpdate&& upd)
{
  eos::common::RWMutexReadLock rd_lock(mRWMutex);
  auto it = mMapListeners.find(upd.key);

  if (it != mMapListeners.end()) {
    eos::mq::FsChangeListener::Event event;
    event.fileSystemQueue = GetQueuePath();
    event.key = upd.key;
    event.deletion = upd.value.empty();

    for (auto& listener : it->second) {
      listener->NotifyEvent(event);
    }
  }
}

//------------------------------------------------------------------------------
// Process shared hash update
//------------------------------------------------------------------------------
void
FileSystem::ProcessUpdateCb(qclient::SharedHashUpdate&& upd)
{
  NotifyFsListener(std::move(upd));
}

//------------------------------------------------------------------------------
// Apply a legacy configuration status
//------------------------------------------------------------------------------
bool
FileSystem::SetLegacyConfigStatus(eos::common::ConfigStatus status,
                                  const std::string* status_comment, bool wait)
{
  using eos::common::ConfigStatus;
  using eos::common::FsLifecycle;
  // The one word rolls three orthogonal settings into one. Take it apart here,
  // at the edge, so nothing below this point has to know the legacy vocabulary
  FsLifecycle lifecycle = FsLifecycle::kActive;

  if (status == ConfigStatus::kEmpty) {
    lifecycle = FsLifecycle::kEmpty;
  } else if (status == ConfigStatus::kOff) {
    lifecycle = FsLifecycle::kOff;
  }

  // The drain request goes first, and a refusal aborts the whole change: the
  // status ladder behaved the same way, leaving the file system as it was
  // when the engine would not start
  if (!SetDrainRequested(status == ConfigStatus::kDrain)) {
    return false;
  }

  return DoSetSchedOps(eos::common::DeriveMaskFromLegacy(status), lifecycle,
                       status_comment, wait);
}

//------------------------------------------------------------------------------
// Set the operations this file system accepts
//------------------------------------------------------------------------------
bool
FileSystem::SetSchedOps(eos::common::FsOpMask ops, const std::string* status_comment,
                        bool wait)
{
  return DoSetSchedOps(ops, GetLifecycle(), status_comment, wait);
}

//------------------------------------------------------------------------------
// Store the permission mask, the lifecycle and the derived legacy status
//------------------------------------------------------------------------------
bool
FileSystem::DoSetSchedOps(eos::common::FsOpMask ops, eos::common::FsLifecycle lifecycle,
                          const std::string* status_comment, bool wait)
{
  // Only the master owns the configuration
  if (!ShouldBroadCast()) {
    return true;
  }

  eos::common::FileSystemUpdateBatch batch;
  batch.setStringDurable(eos::common::FS_SCHED_OPS_NAME,
                         eos::common::FormatSchedMask(ops));
  batch.setStringDurable(eos::common::FS_LIFECYCLE_NAME,
                         eos::common::FileSystem::GetLifecycleAsString(lifecycle));
  // Publish the legacy projection in the same batch. Everything that still
  // reads configstatus - the FSTs, the geotree engine, the capacity sums and
  // monitoring - therefore sees a value that is always a function of the mask.
  const bool is_empty = (lifecycle == eos::common::FsLifecycle::kEmpty);
  batch.setStringDurable("configstatus",
                         eos::common::FileSystem::GetConfigStatusAsString(
                             eos::common::DeriveLegacyConfigStatus(ops, is_empty)));

  if (status_comment) {
    batch.setStringDurable("statuscomment", *status_comment);
  }

  return applyBatch(batch, wait);
}

//------------------------------------------------------------------------------
// Request or stop draining of this file system
//------------------------------------------------------------------------------
bool
FileSystem::SetDrainRequested(bool requested)
{
  // Only the master drains
  if (!ShouldBroadCast()) {
    return true;
  }

  std::string out_msg;

  if (requested) {
    if (!gOFS->mDrainEngine.StartFsDrain(this, 0, out_msg)) {
      eos_static_err("%s", out_msg.c_str());
      return false;
    }
  } else {
    if (!gOFS->mDrainEngine.StopFsDrain(this, out_msg)) {
      eos_static_debug("%s", out_msg.c_str());
      // Drain already stopped, make sure the drain status is cleared too if
      // this was a finished drain i.e. drained, failed or expired
      const eos::common::DrainStatus st = GetDrainStatus();

      if ((st == eos::common::DrainStatus::kDrained) ||
          (st == eos::common::DrainStatus::kDrainFailed) ||
          (st == eos::common::DrainStatus::kDrainExpired)) {
        SetDrainStatus(eos::common::DrainStatus::kNoDrain);
      }
    }
  }

  eos::common::FileSystemUpdateBatch batch;
  batch.setStringDurable(eos::common::FS_DRAIN_REQUESTED_NAME, requested ? "1" : "0");
  return applyBatch(batch, false);
}

//------------------------------------------------------------------------------
// Set the lifecycle of this file system
//------------------------------------------------------------------------------
bool
FileSystem::SetLifecycle(eos::common::FsLifecycle lifecycle)
{
  if (!ShouldBroadCast()) {
    return true;
  }

  eos::common::FileSystemUpdateBatch batch;
  batch.setStringDurable(eos::common::FS_LIFECYCLE_NAME,
                         eos::common::FileSystem::GetLifecycleAsString(lifecycle));
  return applyBatch(batch, false);
}

//------------------------------------------------------------------------------
// Get the current broadcasting setting
//------------------------------------------------------------------------------
bool
FileSystem::ShouldBroadCast()
{
  return (mRealm ? mRealm->ShouldBroadcast() : false);
}

//------------------------------------------------------------------------------
// Increment number of running balancing transfers
//------------------------------------------------------------------------------
void
FileSystem::IncrementBalanceTx()
{
  ++mNumBalanceTx;
  SetLongLongLocal(sNumBalanceTxTag, (int64_t)mNumBalanceTx);
}

//------------------------------------------------------------------------------
// Decrement number of running balancing transfers
//------------------------------------------------------------------------------
void
FileSystem::DecrementBalanceTx()
{
  --mNumBalanceTx;
  SetLongLongLocal(sNumBalanceTxTag, (int64_t)mNumBalanceTx);
}

EOSMGMNAMESPACE_END
