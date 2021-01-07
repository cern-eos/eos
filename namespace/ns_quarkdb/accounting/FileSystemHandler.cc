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
#include "namespace/utils/FileListRandomPicker.hh"
#include "common/Assert.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor.
//------------------------------------------------------------------------------
FileSystemHandler::FileSystemHandler(IFileMD::location_t loc,
                                     folly::Executor* executor,
                                     qclient::QClient* qcl,
                                     MetadataFlusher* flusher,
                                     bool unlinked)
  : location(loc), pExecutor(executor), pQcl(qcl), pFlusher(flusher)
{
  if (unlinked) {
    target = Target::kUnlinked;
  } else {
    target = Target::kRegular;
  }

  mContents.set_deleted_key(0);
  mContents.set_empty_key(0xffffffffffffffffll);
}

//------------------------------------------------------------------------------
// Constructor for the special case of "no replica list".
//------------------------------------------------------------------------------
FileSystemHandler::FileSystemHandler(folly::Executor* executor,
                                     qclient::QClient* qcl,
                                     MetadataFlusher* flusher,
                                     IsNoReplicaListTag tag)
  : location(0), pExecutor(executor), pQcl(qcl), pFlusher(flusher)
{
  target = Target::kNoReplicaList;
  mContents.set_deleted_key(0);
  mContents.set_empty_key(0xffffffffffffffffll);
}

//------------------------------------------------------------------------------
// Ensure contents have been loaded into the cache. If so, returns
// immediatelly. Otherwise, does requests to QDB to retrieve its contents.
// Return value: "this" pointer.
//------------------------------------------------------------------------------
FileSystemHandler* FileSystemHandler::ensureContentsLoaded()
{
  return ensureContentsLoadedAsync().get();
}

//------------------------------------------------------------------------------
// Ensure contents have been loaded into the cache. If so, returns
// immediatelly. Otherwise, does requests to QDB to retrieve its contents.
// Return value: "this" pointer.
//------------------------------------------------------------------------------
folly::Future<FileSystemHandler*>
FileSystemHandler::ensureContentsLoadedAsync()
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (mCacheStatus == CacheStatus::kNotLoaded) {
    mChangeList.clear();
    mCacheStatus = CacheStatus::kInFlight;
    mSplitter = folly::FutureSplitter<FileSystemHandler*>(
                  folly::via(pExecutor).then(&FileSystemHandler::triggerCacheLoad, this));
    lock.unlock();
    return mSplitter.getFuture();
  }

  return mSplitter.getFuture();
}

//------------------------------------------------------------------------------
// Return redis key holding our target filesystem list.
//------------------------------------------------------------------------------
std::string FileSystemHandler::getRedisKey() const
{
  if (target == Target::kRegular) {
    return eos::RequestBuilder::keyFilesystemFiles(location);
  } else if (target == Target::kUnlinked) {
    return eos::RequestBuilder::keyFilesystemUnlinked(location);
  }

  eos_assert(target == Target::kNoReplicaList);
  return fsview::sNoReplicaPrefix;
}

//------------------------------------------------------------------------------
// Trigger load. Must only be called once.
//------------------------------------------------------------------------------
FileSystemHandler* FileSystemHandler::triggerCacheLoad()
{
  pFlusher->synchronize();
  IFsView::FileList temporaryContents;
  temporaryContents.set_deleted_key(0);
  temporaryContents.set_empty_key(0xffffffffffffffffll);

  for (auto it = getStreamingFileList(); it->valid(); it->next()) {
    temporaryContents.insert(it->getElement());
  }

  // Now merge under lock, and additionally apply all entries we might have
  // missed between triggering the cache load, and receiving the contents.
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  eos_assert(mCacheStatus == CacheStatus::kInFlight);
  mContents = std::move(temporaryContents);
  mChangeList.apply(mContents);
  mChangeList.clear();
  mCacheStatus = CacheStatus::kLoaded;
  mContents.resize(0);
  return this;
}

//------------------------------------------------------------------------------
// Insert item.
//------------------------------------------------------------------------------
void FileSystemHandler::insert(FileIdentifier identifier)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (mCacheStatus == CacheStatus::kNotLoaded) {
    // discard, we're not storing the results in-memory at all
  } else if (mCacheStatus == CacheStatus::kInFlight) {
    // record into our ChangeList to apply later, once we've received the
    // contents. This write is racing against cache loading, and may or may
    // not be reflected in the contents.
    mChangeList.push_back(identifier.getUnderlyingUInt64());
  } else {
    eos_assert(mCacheStatus == CacheStatus::kLoaded);
    // Write directly into mContents
    mContents.insert(identifier.getUnderlyingUInt64());
  }

  lock.unlock();
  pFlusher->sadd(getRedisKey(), std::to_string(identifier.getUnderlyingUInt64()));
}

//------------------------------------------------------------------------------
// Erase item.
//------------------------------------------------------------------------------
void FileSystemHandler::erase(FileIdentifier identifier)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (mCacheStatus == CacheStatus::kNotLoaded) {
    // discard, we're not storing the results in-memory at all
  } else if (mCacheStatus == CacheStatus::kInFlight) {
    // record into our ChangeList to apply later, once we've received the
    // contents. This write is racing against cache loading, and may or may
    // not be reflected in the contents.
    mChangeList.erase(identifier.getUnderlyingUInt64());
  } else {
    eos_assert(mCacheStatus == CacheStatus::kLoaded);
    // Write directly into mContents
    mContents.erase(identifier.getUnderlyingUInt64());
    mContents.resize(0);
  }

  lock.unlock();
  pFlusher->srem(getRedisKey(), std::to_string(identifier.getUnderlyingUInt64()));
}

//------------------------------------------------------------------------------
// Get size. Careful when calling this function, it'll load all contents if
// not already there.
//------------------------------------------------------------------------------
uint64_t FileSystemHandler::size()
{
  ensureContentsLoaded();
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mContents.size();
}

//------------------------------------------------------------------------------
// Return iterator for this file system.
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemHandler::getFileList()
{
  ensureContentsLoaded();
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new eos::FileListIterator(mContents, mMutex));
}

//------------------------------------------------------------------------------
// Return streaming iterator for this file system.
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemHandler::getStreamingFileList()
{
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new eos::StreamingFileListIterator(*pQcl, getRedisKey()));
}

//------------------------------------------------------------------------------
// Delete the entire filelist.
//------------------------------------------------------------------------------
void FileSystemHandler::nuke()
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mContents.clear();
  mContents.resize(0);
  pFlusher->del(getRedisKey());
}

//----------------------------------------------------------------------------
// Get an approximately random file in the filelist.
//----------------------------------------------------------------------------
bool FileSystemHandler::getApproximatelyRandomFile(IFileMD::id_t& res)
{
  ensureContentsLoaded();
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return pickRandomFile(mContents, res);
}

//----------------------------------------------------------------------------
// Check whether a given id_t is contained in this filelist
//----------------------------------------------------------------------------
bool FileSystemHandler::hasFileId(IFileMD::id_t file)
{
  ensureContentsLoaded();
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mContents.find(file) != mContents.end();
}

EOSNSNAMESPACE_END
