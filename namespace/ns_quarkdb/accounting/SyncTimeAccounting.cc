/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/accounting/SyncTimeAccounting.hh"
#include <iostream>
#include <chrono>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SyncTimeAccounting::SyncTimeAccounting(IContainerMDSvc* svc,
                                       eos::common::RWMutex* ns_mutex,
                                       uint32_t update_interval):
  mAccumulateIndx(0), mCommitIndx(1), mShutdown(false),
  mUpdateIntervalSec(update_interval), mContainerMDSvc(svc),
  gNsRwMutex(ns_mutex)
{
  mBatch.resize(2);

  // Enable updates if update interval is not 0
  if (mUpdateIntervalSec) {
    mThread = std::thread(&SyncTimeAccounting::PropagateUpdates, this);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SyncTimeAccounting::~SyncTimeAccounting()
{
  mShutdown = true;

  if (mUpdateIntervalSec) {
    mThread.join();
  }
}

//------------------------------------------------------------------------------
// Notify the me about the changes in the main view
//------------------------------------------------------------------------------
void
SyncTimeAccounting::containerMDChanged(IContainerMD* obj, Action type)
{
  switch (type) {
  case IContainerMDChangeListener::MTimeChange:
    QueueForUpdate(obj->getId());
    break;

  default:
    break;
  }
}

//------------------------------------------------------------------------------
// Queue container object for update
//------------------------------------------------------------------------------
void
SyncTimeAccounting::QueueForUpdate(IContainerMD::id_t id)
{
  std::lock_guard<std::mutex> scope_lock(mMutexBatch);
  auto& batch = mBatch[mAccumulateIndx];
  auto it_map = batch.mMap.find(id);

  if (it_map != batch.mMap.end()) {
    auto& it_lst = it_map->second;
    // Move it from current location to the end of the list (most recent)
    batch.mLstUpd.splice(batch.mLstUpd.end(), batch.mLstUpd, it_lst);
  } else {
    auto it_new = batch.mLstUpd.emplace(batch.mLstUpd.end(), id);
    batch.mMap[id] = it_new;
  }
}

//------------------------------------------------------------------------------
// Propagate the sync time
//------------------------------------------------------------------------------
void
SyncTimeAccounting::PropagateUpdates()
{
  while (true) {
    if (mShutdown) {
      break;
    }

    {
      // Update the indexes to have the async thread working on the batch to
      // commit and the incoming updates to go to the batch to update
      std::lock_guard<std::mutex> scope_lock(mMutexBatch);
      std::swap(mAccumulateIndx, mCommitIndx);
    }

    uint16_t deepness = 0;
    IContainerMD::id_t id = 0;
    std::set<IContainerMD::id_t> upd_nodes;
    auto& lst = mBatch[mCommitIndx].mLstUpd;

    // Start updating form the last node (most recent) and also collect the
    // nodes that we've updated so that older updates don't propagate further
    // up than strictly necessary.
    for (auto it_id = lst.rbegin(); it_id != lst.rend(); ++it_id) {
      deepness = 0;
      id = *it_id;

      if (id == 0u) {
        continue;
      }

      eos_debug("Container_id=%lu sync time", id);
      IContainerMD::ctime_t mtime {0};
      eos::common::RWMutexWriteLock wr_lock(*gNsRwMutex);

      while ((id > 1) && (deepness < 255)) {
        std::shared_ptr<IContainerMD> cont;

        // If node is already in the set of updates then don't bother
        // propagating this update
        if (upd_nodes.count(id)) {
          break;
        }

        try {
          cont = mContainerMDSvc->getContainerMD(id);

          // Only traverse if there there is an attribute saying so
          if (!cont->hasAttribute("sys.mtime.propagation")) {
            break;
          }

          if (deepness == 0u) {
            cont->getMTime(mtime);
          }

          if (!cont->setTMTime(mtime) && deepness) {
            break;
          }

          (void) upd_nodes.insert(id);
          mContainerMDSvc->updateStore(cont.get());
        } catch (MDException& e) {
          cont = nullptr;
          break;
        }

        id = cont->getParentId();
        ++deepness;
      }
    }

    // Clean up the batch
    mBatch[mCommitIndx].Clean();

    if (mUpdateIntervalSec) {
      std::this_thread::sleep_for(std::chrono::seconds(mUpdateIntervalSec));
    } else {
      break;
    }
  }
}

EOSNSNAMESPACE_END
