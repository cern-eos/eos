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

#include "mgm/FileSystem.hh"
#include "mq/MessagingRealm.hh"
#include "mq/FsChangeListener.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
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

  if (mRealm->haveQDB()) {
    // Register with FsChangeListeners interested in key updates related
    // to this file system object
    RegisterWithExistingListeners();
    // Subscribe to the underlying SharedHash object to get updates
    mSubscription = mq::SharedHashWrapper(mRealm, mHashLocator).subscribe();

    if (mSubscription) {
      using namespace std::placeholders;
      mSubscription->attachCallback(std::bind(&FileSystem::ProcessUpdateCb,
                                              this, _1));
    }
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

//----------------------------------------------------------------------------
// Set the configuration status of a file system
//----------------------------------------------------------------------------
bool
FileSystem::SetConfigStatus(eos::common::ConfigStatus new_status)
{
  using eos::mgm::FsView;
  using eos::common::DrainStatus;
  eos::common::ConfigStatus old_status = GetConfigStatus();
  int drain_tx = IsDrainTransition(old_status, new_status);

  // Only master drains
  if (ShouldBroadCast()) {
    std::string out_msg;

    if (drain_tx > 0) {
      if (!gOFS->mDrainEngine.StartFsDrain(this, 0, out_msg)) {
        eos_static_err("%s", out_msg.c_str());
        return false;
      }
    } else {
      if (!gOFS->mDrainEngine.StopFsDrain(this, out_msg)) {
        eos_static_debug("%s", out_msg.c_str());
        // Drain already stopped make sure we also update the drain status
        // if this was a finished drain ie. has status drained or failed
        DrainStatus st = GetDrainStatus();

        if ((st == DrainStatus::kDrained) ||
            (st == DrainStatus::kDrainFailed) ||
            (st == DrainStatus::kDrainExpired)) {
          SetDrainStatus(eos::common::DrainStatus::kNoDrain);
        }
      }
    }
  }

  std::string val = eos::common::FileSystem::GetConfigStatusAsString(new_status);
  return eos::common::FileSystem::SetString("configstatus", val.c_str());
}

//------------------------------------------------------------------------------
// Set a 'key' describing the filesystem
//------------------------------------------------------------------------------
bool
FileSystem::SetString(const char* key, const char* str, bool broadcast)
{
  std::string skey = key;

  if (skey == "configstatus") {
    return SetConfigStatus(GetConfigStatusFromString(str));
  }

  return eos::common::FileSystem::SetString(key, str, broadcast);
}

//------------------------------------------------------------------------------
// Check if this is a drain transition i.e. enables or disabled draining
//------------------------------------------------------------------------------
int
FileSystem::IsDrainTransition(const eos::common::ConfigStatus old,
                              const eos::common::ConfigStatus status)
{
  using namespace eos::common;

  // Enable draining
  if (((old != ConfigStatus::kDrain) &&
       (old != ConfigStatus::kDrainDead) &&
       ((status == ConfigStatus::kDrain) ||
        (status == ConfigStatus::kDrainDead))) ||
      (((old == ConfigStatus::kDrain) ||
        (old == ConfigStatus::kDrainDead)) &&
       (status == old))) {
    return 1;
  }

  // Stop draining
  if (((old == common::ConfigStatus::kDrain) ||
       (old == common::ConfigStatus::kDrainDead)) &&
      ((status != common::ConfigStatus::kDrain) &&
       (status != common::ConfigStatus::kDrainDead))) {
    return -1;
  }

  // Not a drain transition
  return 0;
}

//------------------------------------------------------------------------------
// Get the current broadcasting setting
//------------------------------------------------------------------------------
bool
FileSystem::ShouldBroadCast()
{
  if (mRealm) {
    if (mRealm->getSom()) {
      return mRealm->getSom()->ShouldBroadCast();
    } else {
      // @note (esindril) to review when active-passive is actually enabled
      return true;
    }
  } else {
    return false;
  }
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
