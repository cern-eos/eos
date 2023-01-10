// ----------------------------------------------------------------------
// File: ClusterMap.cc
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


#include "mgm/placement/ClusterMap.hh"

namespace eos::mgm::placement {

StorageHandler
ClusterMgr::getStorageHandler()
{
  return StorageHandler(*this);
}

void
ClusterMgr::trimOldEpochs(uint64_t epochs_to_keep)
{
  if (epochs_to_keep > mEpochClusterData.size()) {
    return;
  }
  mEpochClusterData.erase(mEpochClusterData.begin(),
                          mEpochClusterData.end() - epochs_to_keep);
  mStartEpoch += epochs_to_keep;
}


std::shared_ptr<ClusterData>
ClusterMgr::getClusterData(epoch_id_t epoch)
{
  auto start_epoch = mStartEpoch.load(std::memory_order_acquire);
  if (epoch < start_epoch) {
    return nullptr;
  }
  return mEpochClusterData[epoch - mStartEpoch - 1];
}

std::shared_ptr<ClusterData>
ClusterMgr::getClusterData()
{
  auto current_epoch = mCurrentEpoch.load(std::memory_order_acquire);
  auto start_epoch = mStartEpoch.load(std::memory_order_acquire);
  if (current_epoch == 0) {
    return nullptr;
  }

  return mEpochClusterData.at(current_epoch - start_epoch - 1);
}

void
ClusterMgr::addClusterData(ClusterData&& data)
{
  if (mEpochClusterData.size() == mEpochClusterData.capacity()) {
    trimOldEpochs();
  }

  mEpochClusterData.emplace_back(std::make_shared<ClusterData>
      (std::move(data)));
  mCurrentEpoch++;
}

bool
StorageHandler::isValidBucketID(item_id_t bucket_id) const
{
  return bucket_id < 0 &&
         (size_t(-bucket_id) < mData.buckets.size());
}

bool
StorageHandler::addBucket(uint8_t bucket_type, item_id_t bucket_id,
                          item_id_t parent_bucket_id)
{
  if (bucket_id > 0 || parent_bucket_id > 0) {
    return false;
  }
  int32_t index = -bucket_id;
  int32_t parent_index = -parent_bucket_id;

  // This cast is safe, we'd already checked that the value is +ve
  if ((size_t)index > mData.buckets.size()) {
    mData.buckets.resize(index+1);
  }

  mData.buckets.at(index) = Bucket(bucket_id, bucket_type);

  // Handle special case when the parent is the root && we're adding root
  if (parent_index != bucket_id) {
    mData.buckets[parent_index].items.push_back(bucket_id);
  }
  return true;
}

bool
StorageHandler::addDisk(Disk disk, item_id_t bucket_id)
{
  if (disk.id == mData.disks.size() + 1)  {
    return addDiskSequential(disk, bucket_id);
  }

  if (!isValidBucketID(bucket_id) || disk.id == 0) {
    return false;
  }


  size_t insert_pos = disk.id - 1;
  if (disk.id > mData.disks.size()) {
    mData.disks.resize(disk.id);
  }

  mData.disks[insert_pos] = disk;
  mData.buckets[-bucket_id].items.push_back(disk.id);
  mData.buckets[-bucket_id].total_weight += disk.weight;
  return true;
}

bool
StorageHandler::addDiskSequential(Disk disk, item_id_t bucket_id)
{
  if (!isValidBucketID(bucket_id) || disk.id == 0) {
    return false;
  }

  mData.disks.push_back(disk);
  mData.buckets[-bucket_id].items.push_back(disk.id);
  mData.buckets[-bucket_id].total_weight += disk.weight;
  return true;
}


} // namespace eos::mgm::placement
