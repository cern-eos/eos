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

#include <shared_mutex>

namespace eos::mgm::placement
{

StorageHandler
ClusterMgr::getStorageHandler(size_t max_buckets)
{
  return StorageHandler(*this, max_buckets);
}

ClusterMgr::ClusterDataPtr
ClusterMgr::getClusterData()
{
  return {mClusterData.get(), cluster_mgr_rcu};
}

void
ClusterMgr::addClusterData(ClusterData&& data)
{
  // mClusterData is an atomic unique ptr, so reset returns a ptr
  // whose deletion we need to do outside the lock
  ClusterData* old_ptr {nullptr};
  {
    std::unique_lock l(cluster_mgr_rcu);
    old_ptr = mClusterData.reset(new ClusterData(std::move(data)));
    mCurrentEpoch.fetch_add(1, std::memory_order_release);
  }
  delete old_ptr;
}


bool
ClusterMgr::setDiskStatus(fsid_t disk_id, ConfigStatus status)
{
  std::shared_lock rlock(cluster_mgr_rcu);
  return mClusterData->setDiskStatus(disk_id, status);
}

bool
ClusterMgr::setDiskStatus(fsid_t disk_id, ActiveStatus status)
{
  std::shared_lock rlock(cluster_mgr_rcu);
  return mClusterData->setDiskStatus(disk_id, status);
}

bool
ClusterMgr::setDiskWeight(fsid_t disk_id, uint8_t weight)
{
  std::shared_lock rlock(cluster_mgr_rcu);
  if (mClusterData->setDiskWeight(disk_id, weight)) {
    mCurrentEpoch.fetch_add(1, std::memory_order_release);
    return true;
  }

  return false;
}

StorageHandler
ClusterMgr::getStorageHandlerWithData()
{
  if (!mClusterData) {
    return getStorageHandler();
  }

  auto cluster_data = getClusterData();
  ClusterData cluster_data_copy(cluster_data());
  return StorageHandler(*this, std::move(cluster_data_copy));
}

std::string
ClusterMgr::getStateStr(std::string_view type)
{
  using namespace std::string_view_literals;
  std::stringstream ss;
  std::shared_lock rlock(cluster_mgr_rcu);

  if (type == "bucket"sv || type == "all"sv) {
    ss << mClusterData->getBucketsAsString();
  }
  if (type == "disk"sv || type == "all"sv) {
    ss << mClusterData->getDisksAsString();
  }

  return ss.str();
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
  if ((size_t)index >= mData.buckets.size()) {
    mData.buckets.resize(index + 1);
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
