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

#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemHandler.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "common/Assert.hh"
#include "qclient/QSet.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor.
//------------------------------------------------------------------------------
FileSystemHandler::FileSystemHandler(IFileMD::location_t loc, folly::Executor *executor,
  qclient::QClient *qcl, MetadataFlusher *flusher, bool unlinked)
: location(loc), pExecutor(executor), pQcl(qcl), pFlusher(flusher)
{
  if(unlinked) {
      target = Target::kUnlinked;
  }
  else {
      target = Target::kRegular;
  }

  mContents.set_deleted_key(0);
  mContents.set_empty_key(0xffffffffffffffffll);
}

//------------------------------------------------------------------------------
// Constructor for the special case of "no replica list".
//------------------------------------------------------------------------------
FileSystemHandler::FileSystemHandler(folly::Executor* executor, qclient::QClient *qcl,
  MetadataFlusher *flusher, IsNoReplicaListTag tag)
: location(0), pExecutor(executor), pQcl(qcl), pFlusher(flusher)
{
  target = Target::kNoReplicaList;

  mContents.set_deleted_key(0);
  mContents.set_empty_key(0xffffffffffffffffll);
}

//------------------------------------------------------------------------------
//! Ensure contents have been loaded into the cache. If so, returns
//! immediatelly. Otherwise, does requests to QDB to retrieve its contents.
//! Return value: "this" pointer.
//------------------------------------------------------------------------------
FileSystemHandler* FileSystemHandler::ensureContentsLoaded() {
  return ensureContentsLoadedAsync().get();
}

//------------------------------------------------------------------------------
//! Ensure contents have been loaded into the cache. If so, returns
//! immediatelly. Otherwise, does requests to QDB to retrieve its contents.
//! Return value: "this" pointer.
//------------------------------------------------------------------------------
folly::Future<FileSystemHandler*> FileSystemHandler::ensureContentsLoadedAsync() {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if(mCacheStatus == CacheStatus::kNotLoaded) {
    mCacheStatus = CacheStatus::kInFlight;
    mSplitter = folly::FutureSplitter<FileSystemHandler*>(std::move(
      folly::via(pExecutor).then(&FileSystemHandler::triggerCacheLoad, this)
    ));

    lock.unlock();
    return mSplitter.getFuture();
  }

  return mSplitter.getFuture();
}

//------------------------------------------------------------------------------
//! Return redis key holding our target filesystem list.
//------------------------------------------------------------------------------
std::string FileSystemHandler::getRedisKey() const {
  if(target == Target::kRegular) {
    return eos::RequestBuilder::keyFilesystemFiles(location);
  }
  else if(target == Target::kUnlinked) {
    return eos::RequestBuilder::keyFilesystemUnlinked(location);
  }

  eos_assert(target == Target::kNoReplicaList);
  return fsview::sNoReplicaPrefix;
}

//------------------------------------------------------------------------------
//! Trigger load. Must only be called once.
//------------------------------------------------------------------------------
FileSystemHandler* FileSystemHandler::triggerCacheLoad() {
  eos_assert(mCacheStatus == CacheStatus::kInFlight);

  IFsView::FileList temporaryContents;
  temporaryContents.set_deleted_key(0);
  temporaryContents.set_empty_key(0xffffffffffffffffll);

  qclient::QSet qset(*pQcl, getRedisKey());

  for(auto it = qset.getIterator(); it.valid(); it.next()) {
    // TODO(gbitzes): Error checking for string -> integer conversion
    temporaryContents.insert(std::stoull(it.getElement()));
  }

  // Now merge under lock. This is because we may have extra entries inside
  // mContents due to the inherent race condition of intervening writes to
  // mContents during loading of temporaryContents, which may or may not have
  // ended up in temporaryContents.

  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  for(auto it = temporaryContents.begin(); it != temporaryContents.end(); it++) {
    mContents.insert(*it);
  }

  mCacheStatus = CacheStatus::kLoaded;
  return this;
}

//------------------------------------------------------------------------------
//! Insert item.
//------------------------------------------------------------------------------
void FileSystemHandler::insert(FileIdentifier identifier) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mContents.insert(identifier.getUnderlyingUInt64());
  lock.unlock();

  pFlusher->sadd(getRedisKey(), std::to_string(identifier.getUnderlyingUInt64()));
}

//------------------------------------------------------------------------------
//! Erase item.
//------------------------------------------------------------------------------
void FileSystemHandler::erase(FileIdentifier identifier) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mContents.insert(identifier.getUnderlyingUInt64());
  lock.unlock();

  pFlusher->srem(getRedisKey(), std::to_string(identifier.getUnderlyingUInt64()));
}

//------------------------------------------------------------------------------
//! Get size. Careful when calling this function, it'll load all contents if
//! not already there.
//------------------------------------------------------------------------------
uint64_t FileSystemHandler::size() {
  ensureContentsLoaded();

  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mContents.size();
}

//------------------------------------------------------------------------------
//! Return iterator for this file system.
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
FileSystemHandler::getFileList() {
  ensureContentsLoaded();

  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new eos::FileListIterator(mContents, mMutex));
}


EOSNSNAMESPACE_END
