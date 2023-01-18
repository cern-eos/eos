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

namespace eos::mgm::placement {


class StorageHandler;

class ClusterMgr {
public:
  ClusterMgr() : mCurrentIndex(0), mCurrentEpoch(0), mEpochSize(50) {
    mEpochClusterData.reserve(mEpochSize);
  }

  StorageHandler getStorageHandler(size_t max_buckets=256);
  StorageHandler getStorageHandlerWithData();
  epoch_id_t getCurrentEpoch() const { return mCurrentEpoch; }
  void trimOldEpochs(uint64_t epochs_to_keep = 10);

  std::shared_ptr<ClusterData> getClusterData(epoch_id_t epoch);
  std::shared_ptr<ClusterData> getClusterData();

  bool setDiskStatus(fsid_t disk_id, DiskStatus status);
  // Not meant to be called directly! use storage handler, we might consider
  // making this private and friending if this is abused
  void addClusterData(ClusterData&& data);

private:
  bool mWrapAround{false};
  std::mutex mClusterDataWMtx;
  std::atomic<uint32_t> mCurrentIndex{0};
  std::atomic<epoch_id_t> mCurrentEpoch{0};
  size_t mEpochSize;
  std::vector<std::shared_ptr<ClusterData>> mEpochClusterData;
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

  bool addDisk(Disk d, item_id_t bucket_id);

  // We store disks sequentially with index as fsid - 1;
  bool addDiskSequential(Disk d, item_id_t bucket_id);

  bool isValidBucketID(item_id_t bucket_id) const;

  ~StorageHandler() {
    mClusterMgr.addClusterData(std::move(mData));
  }
private:
  ClusterMgr& mClusterMgr;
  ClusterData mData;
};

} // namespace eos::mgm::placement
