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
ClusterMgr::getStorageHandler(size_t max_buckets)
{
  return StorageHandler(*this, max_buckets);
}

std::shared_ptr<ClusterData>
ClusterMgr::getClusterData(epoch_id_t epoch)
{
  if (mCurrentEpoch == 0) {
    return nullptr;
  }

  auto current_index = mCurrentIndex.load(std::memory_order_acquire);
  auto current_epoch = mCurrentEpoch.load(std::memory_order_acquire);

  if (epoch > current_epoch || epoch < (current_epoch - mEpochSize)) {
    return nullptr;
  }

  auto epoch_diff = (current_epoch - epoch);
  auto circular_index = current_index - epoch_diff;
  if (circular_index < 0) {
    circular_index += mEpochSize;
  }
  return mEpochClusterData[circular_index];
}

std::shared_ptr<ClusterData>
ClusterMgr::getClusterData()
{
  auto current_epoch = mCurrentEpoch.load(std::memory_order_acquire);
  if (current_epoch == 0) {
    return nullptr;
  }

  auto current_index = mCurrentIndex.load(std::memory_order_acquire);

  if (current_index == 0) {
    // Unlikely to happen, we commit the transaction only after vector is
    // updated, so this can only happen in the rare first epoch case where
    // the data is requested after we update current_epoch but before we update the counter
    return mEpochClusterData[0];
  }
  return mEpochClusterData.at(current_index - 1);
}

void
ClusterMgr::addClusterData(ClusterData&& data)
{
  std::scoped_lock wlock(mClusterDataWMtx);
  auto current_index = mCurrentIndex.load(std::memory_order_acquire);
  if (!mWrapAround &&
      (mEpochClusterData.size() == mEpochClusterData.capacity())) {
    mWrapAround = true;
    current_index = 0;
  }

  if (mWrapAround) {
    if (current_index == mEpochClusterData.size()) {
      current_index = 0;
    }
    mEpochClusterData[current_index] = std::make_shared<ClusterData>(std::move(data));
  } else {
    mEpochClusterData.emplace_back(std::make_shared<ClusterData>
                                   (std::move(data)));
  }

  mCurrentEpoch++;
  mCurrentIndex.store(current_index + 1, std::memory_order_release);

}


bool
ClusterMgr::setDiskStatus(fsid_t disk_id, DiskStatus status)
{

  auto cluster_data = getClusterData();
  cluster_data->setDiskStatus(disk_id, status);
  return true;
}

StorageHandler
ClusterMgr::getStorageHandlerWithData()
{
  if (mCurrentEpoch == 0) {
    return getStorageHandler();
  }
  auto cluster_data = getClusterData();
  ClusterData cluster_data_copy(*cluster_data);
  return StorageHandler(*this, std::move(cluster_data_copy));
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
