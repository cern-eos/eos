/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include <folly/Executor.h>
#include "MetadataFetcher.hh"
#include "MetadataProviderShard.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/MDException.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "common/Assert.hh"
#include <functional>

using std::placeholders::_1;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataProviderShard::MetadataProviderShard(qclient::QClient *qcl,
  IContainerMDSvc* contsvc, IFileMDSvc* filesvc, folly::Executor *exec)
  : mContSvc(contsvc), mFileSvc(filesvc), mContainerCache(3e5), mFileCache(3e6)
{
  mExecutor = exec;
  mQcl = qcl;
}

//------------------------------------------------------------------------------
// Retrieve ContainerMD by ID.
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
MetadataProviderShard::retrieveContainerMD(ContainerIdentifier id)
{
  std::unique_lock<std::mutex> lock(mMutex);
  // A ContainerMD can be in three states: Not in cache, inside in-flight cache,
  // and cached. Is it inside in-flight cache?
  auto it = mInFlightContainers.find(id);

  if (it != mInFlightContainers.end()) {
    // Cache hit: A container with such ID has been staged already. Once a
    // response arrives, all futures tied to that container will be activated
    // automatically, with the same IContainerMDPtr.
    return it->second.getFuture();
  }

  // Nope.. is it inside the long-lived cache?
  IContainerMDPtr result = mContainerCache.get(id);

  if (result) {
    lock.unlock();

    // Handle special case where we're dealing with a tombstone.
    if (result->isDeleted()) {
      return folly::makeFuture<IContainerMDPtr>
             (make_mdexception(ENOENT, "Container #" << id.getUnderlyingUInt64()
                               << " does not exist (found deletion tombstone)"));
    }

    return folly::makeFuture<IContainerMDPtr>(std::move(result));
  }

  // Nope, need to fetch, and insert into the in-flight staging area. Merge
  // three asynchronous operations into one.
  folly::Future<eos::ns::ContainerMdProto> protoFut =
    MetadataFetcher::getContainerFromId(*mQcl, id);
  folly::Future<IContainerMD::FileMap> fileMapFut =
    MetadataFetcher::getFileMap(*mQcl, id);
  folly::Future<IContainerMD::ContainerMap> containerMapFut =
    MetadataFetcher::getContainerMap(*mQcl, id);
  folly::Future<IContainerMDPtr> fut =
    folly::collect(protoFut, fileMapFut, containerMapFut)
    .via(mExecutor)
    .thenValue(std::bind(&MetadataProviderShard::processIncomingContainerMD, this, id, _1))
  .thenError([this, id](const folly::exception_wrapper & e) {
    // If the operation failed, clear the in-flight cache.
    std::lock_guard<std::mutex> lock(mMutex);
    mInFlightContainers.erase(id);
    return folly::makeFuture<IContainerMDPtr>(e);
  });
  mInFlightContainers[id] = folly::FutureSplitter<IContainerMDPtr>(std::move(
                              fut));
  return mInFlightContainers[id].getFuture();
}

//------------------------------------------------------------------------------
// Retrieve FileMD by ID.
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr>
MetadataProviderShard::retrieveFileMD(FileIdentifier id)
{
  std::unique_lock<std::mutex> lock(mMutex);

  // Are we asking for fid=0? Illegal, short-circuit without even contacting
  // QDB. Indicates possible bug elsewhere in the MGM.
  if(id == FileIdentifier(0)) {
    eos_static_warning("Attempted to retrieve fid=0!");
    return folly::makeFuture<IFileMDPtr>
            (make_mdexception(ENOENT, "File #" << id.getUnderlyingUInt64()
            << " does not exist (fid=0 is illegal)"));
  }

  // A FileMD can be in three states: Not in cache, inside in-flight cache,
  // and cached. Is it inside in-flight cache?
  auto it = mInFlightFiles.find(id);

  if (it != mInFlightFiles.end()) {
    // Cache hit: A container with such ID has been staged already. Once a
    // response arrives, all futures tied to that container will be activated
    // automatically, with the same IContainerMDPtr.
    return it->second.getFuture();
  }

  // Nope.. is it inside the long-lived cache?
  IFileMDPtr result = mFileCache.get(id);

  if (result) {
    lock.unlock();

    // Handle special case where we're dealing with a tombstone.
    if (result->isDeleted()) {
      return folly::makeFuture<IFileMDPtr>
             (make_mdexception(ENOENT, "File #" << id.getUnderlyingUInt64()
                               << " does not exist (found deletion tombstone)"));
    }

    return folly::makeFuture<IFileMDPtr>(std::move(result));
  }

  // Nope, need to fetch, and insert into the in-flight staging area.
  folly::Future<IFileMDPtr> fut = MetadataFetcher::getFileFromId(*mQcl, id)
                                  .via(mExecutor)
                                  .thenValue(std::bind(&MetadataProviderShard::processIncomingFileMdProto, this, id, _1))
  .thenError([this, id](const folly::exception_wrapper & e) {
    // If the operation failed, clear the in-flight cache.
    std::lock_guard<std::mutex> lock(mMutex);
    mInFlightFiles.erase(id);
    return folly::makeFuture<IFileMDPtr>(e);
  });
  mInFlightFiles[id] = folly::FutureSplitter<IFileMDPtr>(std::move(fut));
  return mInFlightFiles[id].getFuture();
}

//------------------------------------------------------------------------------
// Drop cached FileID - return true if found
//------------------------------------------------------------------------------
bool
MetadataProviderShard::dropCachedFileID(FileIdentifier id)
{
  std::unique_lock<std::mutex> lock(mMutex);
  return mFileCache.remove(id);
}

//------------------------------------------------------------------------------
// Drop cached ContainerID - return true if found
//------------------------------------------------------------------------------
bool
MetadataProviderShard::dropCachedContainerID(ContainerIdentifier id)
{
  std::unique_lock<std::mutex> lock(mMutex);
  return mContainerCache.remove(id);
}

//----------------------------------------------------------------------------
// Check if a FileMD exists with the given id
//----------------------------------------------------------------------------
folly::Future<bool>
MetadataProviderShard::hasFileMD(FileIdentifier id)
{
  return MetadataFetcher::doesFileMdExist(*mQcl, id);
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void
MetadataProviderShard::insertFileMD(FileIdentifier id, IFileMDPtr item)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mFileCache.put(id, item);
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void
MetadataProviderShard::insertContainerMD(ContainerIdentifier id,
                                    IContainerMDPtr item)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mContainerCache.put(id, item);
}

//------------------------------------------------------------------------------
// Change file cache size.
//------------------------------------------------------------------------------
void MetadataProviderShard::setFileMDCacheNum(uint64_t max_num)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mFileCache.set_max_num(max_num);
}

//------------------------------------------------------------------------------
// Change container cache size.
//------------------------------------------------------------------------------
void MetadataProviderShard::setContainerMDCacheNum(uint64_t max_num)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mContainerCache.set_max_num(max_num);
}

//------------------------------------------------------------------------------
// Turn a (ContainerMDProto, FileMap, ContainerMap) triplet into a
// ContainerMDPtr, and insert into the cache.
//------------------------------------------------------------------------------
IContainerMDPtr
MetadataProviderShard::processIncomingContainerMD(ContainerIdentifier id,
    std::tuple <
    eos::ns::ContainerMdProto,
    IContainerMD::FileMap,
    IContainerMD::ContainerMap
    > tup)
{
  std::lock_guard<std::mutex> lock(mMutex);
  // Unpack tuple. (sigh)
  eos::ns::ContainerMdProto& proto = std::get<0>(tup);
  IContainerMD::FileMap& fileMap = std::get<1>(tup);
  IContainerMD::ContainerMap& containerMap = std::get<2>(tup);
  // Things look sane?
  eos_assert(proto.id() == id.getUnderlyingUInt64());
  // Yep, construct ContainerMD object..
  QuarkContainerMD* containerMD = new QuarkContainerMD(0, mFileSvc, mContSvc);
  containerMD->initialize(std::move(proto), std::move(fileMap),
                          std::move(containerMap));
  // Drop inFlightContainers future..
  auto it = mInFlightContainers.find(id);
  eos_assert(it != mInFlightContainers.end());
  mInFlightContainers.erase(it);
  // Insert into the cache ...
  IContainerMDPtr item { containerMD };
  mContainerCache.put(id, item);
  return item;
}

//------------------------------------------------------------------------------
// Turn an incoming FileMDProto into FileMD, removing from the inFlight
// staging area, and inserting into the cache.
//------------------------------------------------------------------------------
IFileMDPtr
MetadataProviderShard::processIncomingFileMdProto(FileIdentifier id,
    eos::ns::FileMdProto proto)
{
  std::lock_guard<std::mutex> lock(mMutex);
  // Things look sane?
  eos_assert(proto.id() == id.getUnderlyingUInt64());
  // Yep, construct FileMD object..
  QuarkFileMD* fileMD = new QuarkFileMD(0, mFileSvc);
  fileMD->initialize(std::move(proto));
  // Drop inFlightFiles future..
  auto it = mInFlightFiles.find(id);
  eos_assert(it != mInFlightFiles.end());
  mInFlightFiles.erase(it);
  // Insert into the cache ...
  IFileMDPtr item { fileMD };
  mFileCache.put(id, item);
  return item;
}

//------------------------------------------------------------------------------
// Get file cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProviderShard::getFileMDCacheStats()
{
  CacheStatistics stats;
  stats.enabled = true;
  stats.occupancy = mFileCache.size();
  stats.maxNum = mFileCache.get_max_num();

  std::lock_guard<std::mutex> lock(mMutex);
  stats.inFlight = mInFlightFiles.size();
  return stats;
}

//------------------------------------------------------------------------------
// Get container cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProviderShard::getContainerMDCacheStats()
{
  CacheStatistics stats;
  stats.enabled = true;
  stats.occupancy = mContainerCache.size();
  stats.maxNum = mContainerCache.get_max_num();

  std::lock_guard<std::mutex> lock(mMutex);
  stats.inFlight = mInFlightContainers.size();
  return stats;
}

EOSNSNAMESPACE_END
