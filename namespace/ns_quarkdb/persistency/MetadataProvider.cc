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

//------------------------------------------------------------------------------
// @author Georgios Bitzes <georgios.bitzes@cern.ch>
// @brief Asynchronous metadata retrieval from QDB, with caching support.
//------------------------------------------------------------------------------

#include "MetadataProvider.hh"
#include "MetadataProviderShard.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include <folly/Executor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataProvider::MetadataProvider(const QdbContactDetails& contactDetails,
                                   IContainerMDSvc* contsvc, IFileMDSvc* filesvc)
{
  mExecutor.reset(new folly::IOThreadPoolExecutor(16));

  for(size_t i = 0; i < kShards; i++) {
    mQcl.emplace_back(std::make_unique<qclient::QClient>(contactDetails.members, contactDetails.constructOptions()));
    mShards.emplace_back(new MetadataProviderShard(mQcl.back().get(), contsvc, filesvc, mExecutor.get()));
  }
}

//------------------------------------------------------------------------------
// Retrieve ContainerMD by ID.
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
MetadataProvider::retrieveContainerMD(ContainerIdentifier id)
{
  return pickShard(id)->retrieveContainerMD(id);
}

//------------------------------------------------------------------------------
// Retrieve FileMD by ID.
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr>
MetadataProvider::retrieveFileMD(FileIdentifier id)
{
  return pickShard(id)->retrieveFileMD(id);
}

//------------------------------------------------------------------------------
// Drop cached FileID - return true if found
//------------------------------------------------------------------------------
bool
MetadataProvider::dropCachedFileID(FileIdentifier id)
{
  return pickShard(id)->dropCachedFileID(id);
}

//------------------------------------------------------------------------------
// Drop cached ContainerID - return true if found
//------------------------------------------------------------------------------
bool
MetadataProvider::dropCachedContainerID(ContainerIdentifier id)
{
  return pickShard(id)->dropCachedContainerID(id);
}

//----------------------------------------------------------------------------
// Check if a FileMD exists with the given id
//----------------------------------------------------------------------------
folly::Future<bool>
MetadataProvider::hasFileMD(FileIdentifier id)
{
  return pickShard(id)->hasFileMD(id);
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void
MetadataProvider::insertFileMD(FileIdentifier id, IFileMDPtr item)
{
  return pickShard(id)->insertFileMD(id, item);
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void
MetadataProvider::insertContainerMD(ContainerIdentifier id,
                                    IContainerMDPtr item)
{
  return pickShard(id)->insertContainerMD(id, item);
}

//------------------------------------------------------------------------------
// Change file cache size.
//------------------------------------------------------------------------------
void MetadataProvider::setFileMDCacheNum(uint64_t max_num)
{
  uint64_t max_num_per_shard = max_num / kShards;

  if(max_num == UINT64_MAX) {
    max_num_per_shard = UINT64_MAX;
  }

  for(size_t i = 0; i < mShards.size(); i++) {
    mShards[i]->setFileMDCacheNum(max_num_per_shard);
  }
}

//------------------------------------------------------------------------------
// Change container cache size.
//------------------------------------------------------------------------------
void MetadataProvider::setContainerMDCacheNum(uint64_t max_num)
{
  uint64_t max_num_per_shard = max_num / kShards;

  if(max_num == UINT64_MAX) {
    max_num_per_shard = UINT64_MAX;
  }

  for(size_t i = 0; i < mShards.size(); i++) {
    mShards[i]->setContainerMDCacheNum(max_num_per_shard);
  }
}

//------------------------------------------------------------------------------
// Add a CacheStatistics object into another
//------------------------------------------------------------------------------
void aggregateStatistics(CacheStatistics &global, const CacheStatistics local) {
  global.occupancy += local.occupancy;
  global.maxNum += local.maxNum;
  global.numRequests += local.numRequests;
  global.numHits += local.numHits;
  global.inFlight += local.inFlight;
}

//------------------------------------------------------------------------------
// Get file cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProvider::getFileMDCacheStats()
{
  CacheStatistics globalStats;
  globalStats.enabled = true;

  for(size_t i = 0; i < mShards.size(); i++) {
    aggregateStatistics(globalStats, mShards[i]->getFileMDCacheStats());
  }

  return globalStats;
}

//------------------------------------------------------------------------------
// Get container cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProvider::getContainerMDCacheStats()
{
  CacheStatistics globalStats;
  globalStats.enabled = true;

  for(size_t i = 0; i < mShards.size(); i++) {
    aggregateStatistics(globalStats, mShards[i]->getContainerMDCacheStats());
  }

  return globalStats;
}

//------------------------------------------------------------------------------
//! Pick shard based on FileIdentifier
//------------------------------------------------------------------------------
MetadataProviderShard* MetadataProvider::pickShard(FileIdentifier id) {
  return (mShards[id.getUnderlyingUInt64() % kShards]).get();
}

//------------------------------------------------------------------------------
//! Pick shard based on ContainerIdentifier
//------------------------------------------------------------------------------
MetadataProviderShard* MetadataProvider::pickShard(ContainerIdentifier id) {
  return (mShards[id.getUnderlyingUInt64() % kShards]).get();
}


EOSNSNAMESPACE_END
