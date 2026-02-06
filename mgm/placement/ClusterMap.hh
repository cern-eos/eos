// ----------------------------------------------------------------------
// File: ClusterMap.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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


#pragma once
#include <vector>
#include <memory>
#include "mgm/placement/ClusterDataTypes.hh"
#include "common/concurrency/RCULite.hh"
#include "common/concurrency/AtomicUniquePtr.h"

namespace eos::mgm::placement {


class StorageHandler;
// cluster_rcu_mutex_t is compatible with std::shared_mutex ap
// and hence can be used with std::shared_lock and std::unique_lock
// It is possible to just change cluster_rcu_mutex_t to anything
// that conforms to std::shared_mutex api
using RCUMutexT = eos::common::RCUMutexT<>;


class ClusterMgr {
public:

  struct ClusterDataPtr {
    ClusterDataPtr(ClusterData* data_,
                   RCUMutexT& rcu_domain_):
      data(data_), rlock(rcu_domain_)
    {}

    ~ClusterDataPtr() = default;

    const ClusterData& operator()() const {
      return *data;
    }

    ClusterData* operator->() const {
      return data;
    }

    operator bool() const {
      return data != nullptr;
    }

  private:
    ClusterData* data;
    eos::common::RCUReadLock<RCUMutexT> rlock;
  };

  ClusterMgr() = default;

  StorageHandler getStorageHandler(size_t max_buckets=256);
  StorageHandler getStorageHandlerWithData();
  epoch_id_t getCurrentEpoch() const { return mCurrentEpoch; }

  ClusterDataPtr getClusterData();

  bool setDiskStatus(fsid_t disk_id, ConfigStatus status);
  bool setDiskStatus(fsid_t disk_id, ActiveStatus status);
  bool setDiskWeight(fsid_t disk_id, uint8_t weight);
  // Not meant to be called directly! use storage handler, we might consider
  // making this private and friending if this is abused
  void addClusterData(ClusterData&& data);
  std::string getStateStr(std::string_view type);
private:
  eos::common::atomic_unique_ptr<ClusterData> mClusterData;
  std::atomic<epoch_id_t> mCurrentEpoch {0};
  RCUMutexT cluster_mgr_rcu;
};

class StorageHandler {
public:
  StorageHandler(ClusterMgr& mgr, size_t max_buckets=256) :
      mClusterMgr(mgr)
  { mData.buckets.resize(max_buckets); }

  StorageHandler(ClusterMgr& mgr, ClusterData&& data) :
      mClusterMgr(mgr), mData(std::move(data))
  {}

  bool addBucket(uint8_t bucket_type, item_id_t bucket_id,
                 item_id_t parent_bucket_id=0);

  bool addDisk(Disk d, item_id_t bucket_id, std::string_view tag="");

  // We store disks sequentially with index as fsid - 1;
  bool addDiskSequential(Disk d, item_id_t bucket_id, std::string_view tag="");

  bool isValidBucketID(item_id_t bucket_id) const;

  ~StorageHandler() {
    mClusterMgr.addClusterData(std::move(mData));
  }

  // helpers for storing geotag data
  void addGeoTag(item_id_t item_id, std::string_view tag);
  uint64_t getUniqueHash(std::string_view tag);
private:
  ClusterMgr& mClusterMgr;
  ClusterData mData;
};

} // namespace eos::mgm::placement
