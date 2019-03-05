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
  mShard.reset(new MetadataProviderShard(contactDetails, contsvc, filesvc,
    mExecutor.get()));
}

//------------------------------------------------------------------------------
// Retrieve ContainerMD by ID.
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
MetadataProvider::retrieveContainerMD(ContainerIdentifier id)
{
  return mShard->retrieveContainerMD(id);
}

//------------------------------------------------------------------------------
// Retrieve FileMD by ID.
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr>
MetadataProvider::retrieveFileMD(FileIdentifier id)
{
  return mShard->retrieveFileMD(id);
}

//----------------------------------------------------------------------------
// Check if a FileMD exists with the given id
//----------------------------------------------------------------------------
folly::Future<bool>
MetadataProvider::hasFileMD(FileIdentifier id)
{
  return mShard->hasFileMD(id);
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void
MetadataProvider::insertFileMD(FileIdentifier id, IFileMDPtr item)
{
  return mShard->insertFileMD(id, item);
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void
MetadataProvider::insertContainerMD(ContainerIdentifier id,
                                    IContainerMDPtr item)
{
  return mShard->insertContainerMD(id, item);
}

//------------------------------------------------------------------------------
// Change file cache size.
//------------------------------------------------------------------------------
void MetadataProvider::setFileMDCacheNum(uint64_t max_num)
{
  return mShard->setFileMDCacheNum(max_num);
}

//------------------------------------------------------------------------------
// Change container cache size.
//------------------------------------------------------------------------------
void MetadataProvider::setContainerMDCacheNum(uint64_t max_num)
{
  return mShard->setContainerMDCacheNum(max_num);
}

//------------------------------------------------------------------------------
// Get file cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProvider::getFileMDCacheStats()
{
  return mShard->getFileMDCacheStats();
}

//------------------------------------------------------------------------------
// Get container cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProvider::getContainerMDCacheStats()
{
  return mShard->getContainerMDCacheStats();
}

EOSNSNAMESPACE_END
