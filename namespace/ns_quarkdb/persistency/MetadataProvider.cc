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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Asynchronous metadata retrieval from QDB, with caching support.
//------------------------------------------------------------------------------

#include <folly/Executor.h>
#include "MetadataFetcher.hh"
#include "MetadataProvider.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/MDException.hh"
#include "common/Assert.hh"
#include <functional>
#include <folly/executors/IOThreadPoolExecutor.h>

using std::placeholders::_1;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MetadataProvider::MetadataProvider(qclient::QClient &qcl,
  IContainerMDSvc *contsvc, IFileMDSvc *filesvc)
: mQcl(qcl), mContSvc(contsvc), mFileSvc(filesvc), mContainerCache(1e7), mFileCache(1e8) {

  mExecutor.reset(new folly::IOThreadPoolExecutor(16));
}

//------------------------------------------------------------------------------
// Retrieve ContainerMD by ID.
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr> MetadataProvider::retrieveContainerMD(IContainerMD::id_t id) {
  std::lock_guard<std::mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // A ContainerMD can be in three states: Not in cache, inside in-flight cache,
  // and cached. Is it inside in-flight cache?
  //----------------------------------------------------------------------------
  auto it = mInFlightContainers.find(ContainerIdentifier(id));
  if(it != mInFlightContainers.end()) {
    //--------------------------------------------------------------------------
    // Cache hit: A container with such ID has been staged already. Once a
    // response arrives, all futures tied to that container will be activated
    // automatically, with the same IContainerMDPtr.
    //--------------------------------------------------------------------------
    return it->second.getFuture();
  }

  //----------------------------------------------------------------------------
  // Nope.. is it inside the long-lived cache?
  //----------------------------------------------------------------------------
  IContainerMDPtr result = mContainerCache.get(ContainerIdentifier(id));
  if(result) {
    //--------------------------------------------------------------------------
    // Handle special case where we're dealing with a tombstone.
    //--------------------------------------------------------------------------
    if(result->isDeleted()) {
      return folly::makeFuture<IContainerMDPtr>(make_mdexception(ENOENT, "Container #" << id << " does not exist (found deletion tombstone)"));
    }

    return folly::makeFuture<IContainerMDPtr>(std::move(result));
  }

  //----------------------------------------------------------------------------
  // Nope, need to fetch, and insert into the in-flight staging area. Merge
  // three asynchronous operations into one.
  //----------------------------------------------------------------------------
  folly::Future<eos::ns::ContainerMdProto> protoFut = MetadataFetcher::getContainerFromId(mQcl, id);
  folly::Future<IContainerMD::FileMap> fileMapFut = MetadataFetcher::getFilesInContainer(mQcl, id);
  folly::Future<IContainerMD::ContainerMap> containerMapFut = MetadataFetcher::getSubContainers(mQcl, id);

  folly::Future<IContainerMDPtr> fut = folly::collect(protoFut, fileMapFut, containerMapFut)
    .via(mExecutor.get())
    .then(std::bind(&MetadataProvider::processIncomingContainerMD, this, id, _1))
    .onError([this, id](const folly::exception_wrapper& e) {
      //------------------------------------------------------------------------
      //! If the operation failed, clear the in-flight cache.
      //------------------------------------------------------------------------
      std::lock_guard<std::mutex> lock(mMutex);
      mInFlightContainers.erase(ContainerIdentifier(id));
      return folly::makeFuture<IContainerMDPtr>(e);
    } );

  mInFlightContainers[ContainerIdentifier(id)] = folly::FutureSplitter<IContainerMDPtr>(std::move(fut));
  return mInFlightContainers[ContainerIdentifier(id)].getFuture();
}

//------------------------------------------------------------------------------
// Retrieve FileMD by ID.
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr> MetadataProvider::retrieveFileMD(IFileMD::id_t id) {
  std::lock_guard<std::mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // A FileMD can be in three states: Not in cache, inside in-flight cache,
  // and cached. Is it inside in-flight cache?
  //----------------------------------------------------------------------------
  auto it = mInFlightFiles.find(FileIdentifier(id));
  if(it != mInFlightFiles.end()) {
    //--------------------------------------------------------------------------
    // Cache hit: A container with such ID has been staged already. Once a
    // response arrives, all futures tied to that container will be activated
    // automatically, with the same IContainerMDPtr.
    //--------------------------------------------------------------------------
    return it->second.getFuture();
  }

  //----------------------------------------------------------------------------
  // Nope.. is it inside the long-lived cache?
  //----------------------------------------------------------------------------
  IFileMDPtr result = mFileCache.get(FileIdentifier(id));
  if(result) {
    //--------------------------------------------------------------------------
    // Handle special case where we're dealing with a tombstone.
    //--------------------------------------------------------------------------
    if(result->isDeleted()) {
      return folly::makeFuture<IFileMDPtr>(make_mdexception(ENOENT, "File #" << id << " does not exist (found deletion tombstone)"));
    }

    return folly::makeFuture<IFileMDPtr>(std::move(result));
  }

  //----------------------------------------------------------------------------
  // Nope, need to fetch, and insert into the in-flight staging area.
  //----------------------------------------------------------------------------
  folly::Future<IFileMDPtr> fut = MetadataFetcher::getFileFromId(mQcl, id)
    .via(mExecutor.get())
    .then(std::bind(&MetadataProvider::processIncomingFileMdProto, this, id, _1))
    .onError([this, id](const folly::exception_wrapper& e) {
      //------------------------------------------------------------------------
      //! If the operation failed, clear the in-flight cache.
      //------------------------------------------------------------------------
      std::lock_guard<std::mutex> lock(mMutex);
      mInFlightFiles.erase(FileIdentifier(id));
      return folly::makeFuture<IFileMDPtr>(e);
    } );

  mInFlightFiles[FileIdentifier(id)] = folly::FutureSplitter<IFileMDPtr>(std::move(fut));
  return mInFlightFiles[FileIdentifier(id)].getFuture();
}

//------------------------------------------------------------------------------
// Insert newly created item into the cache.
//------------------------------------------------------------------------------
void MetadataProvider::insertFileMD(IFileMD::id_t id, IFileMDPtr item) {
  std::lock_guard<std::mutex> lock(mMutex);
  mFileCache.put(FileIdentifier(id), item);
}

//----------------------------------------------------------------------------
//! Insert newly created item into the cache.
//----------------------------------------------------------------------------
void MetadataProvider::insertContainerMD(IContainerMD::id_t id, IContainerMDPtr item) {
  std::lock_guard<std::mutex> lock(mMutex);
  mContainerCache.put(ContainerIdentifier(id), item);
}

//------------------------------------------------------------------------------
// Change file cache size.
//------------------------------------------------------------------------------
void MetadataProvider::setFileMDCacheSize(uint64_t size) {
  std::lock_guard<std::mutex> lock(mMutex);
  mFileCache.set_max_size(size);
}

//------------------------------------------------------------------------------
// Change container cache size.
//------------------------------------------------------------------------------
void MetadataProvider::setContainerMDCacheSize(uint64_t size) {
  std::lock_guard<std::mutex> lock(mMutex);
  mContainerCache.set_max_size(size);
}

//------------------------------------------------------------------------------
// Turn a (ContainerMDProto, FileMap, ContainerMap) triplet into a
// ContainerMDPtr, and insert into the cache.
//------------------------------------------------------------------------------
IContainerMDPtr MetadataProvider::processIncomingContainerMD(IContainerMD::id_t id,
    std::tuple<
      eos::ns::ContainerMdProto,
      IContainerMD::FileMap,
      IContainerMD::ContainerMap
    > tup
  ) {

  std::lock_guard<std::mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // Unpack tuple. (sigh)
  //----------------------------------------------------------------------------
  eos::ns::ContainerMdProto &proto = std::get<0>(tup);
  IContainerMD::FileMap &fileMap = std::get<1>(tup);
  IContainerMD::ContainerMap &containerMap = std::get<2>(tup);

  //----------------------------------------------------------------------------
  // Things look sane?
  //----------------------------------------------------------------------------
  eos_assert(proto.id() == id);

  //----------------------------------------------------------------------------
  // Yep, construct ContainerMD object..
  //----------------------------------------------------------------------------
  ContainerMD *containerMD = new ContainerMD(0, mFileSvc, mContSvc);
  containerMD->initialize(std::move(proto), std::move(fileMap), std::move(containerMap));

  //----------------------------------------------------------------------------
  // Drop inFlightContainers future..
  //----------------------------------------------------------------------------
  auto it = mInFlightContainers.find(ContainerIdentifier(id));
  eos_assert(it != mInFlightContainers.end());
  mInFlightContainers.erase(it);

  //----------------------------------------------------------------------------
  // Insert into the cache..
  //----------------------------------------------------------------------------
  IContainerMDPtr item { containerMD };
  mContainerCache.put(ContainerIdentifier(id), item);
  return item;
}

//------------------------------------------------------------------------------
// Turn an incoming FileMDProto into FileMD, removing from the inFlight
// staging area, and inserting into the cache.
//------------------------------------------------------------------------------
IFileMDPtr MetadataProvider::processIncomingFileMdProto(IFileMD::id_t id, eos::ns::FileMdProto proto) {
  std::lock_guard<std::mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // Things look sane?
  //----------------------------------------------------------------------------
  eos_assert(proto.id() == id);

  //----------------------------------------------------------------------------
  // Yep, construct FileMD object..
  //----------------------------------------------------------------------------
  FileMD *fileMD = new FileMD(0, mFileSvc);
  fileMD->initialize(std::move(proto));

  //----------------------------------------------------------------------------
  // Drop inFlightFiles future..
  //----------------------------------------------------------------------------
  auto it = mInFlightFiles.find(FileIdentifier(id));
  eos_assert(it != mInFlightFiles.end());
  mInFlightFiles.erase(it);

  //----------------------------------------------------------------------------
  // Insert into the cache..
  //----------------------------------------------------------------------------
  IFileMDPtr item { fileMD };
  mFileCache.put(FileIdentifier(id), item);
  return item;
}

//------------------------------------------------------------------------------
//! Get file cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProvider::getFileMDCacheStats() {
  CacheStatistics stats;
  stats.enabled = true;
  stats.occupancy = mFileCache.size();
  stats.maxSize = mFileCache.get_max_size();
  return stats;
}

//------------------------------------------------------------------------------
//! Get container cache statistics
//------------------------------------------------------------------------------
CacheStatistics MetadataProvider::getContainerMDCacheStats() {
  CacheStatistics stats;
  stats.enabled = true;
  stats.occupancy = mContainerCache.size();
  stats.maxSize = mContainerCache.get_max_size();
  return stats;
}

EOSNSNAMESPACE_END
