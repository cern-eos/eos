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
//! @brief Class handling caching and access of individual filesystems
//------------------------------------------------------------------------------

#ifndef EOS_NS_FILESYSTEM_HANDLER_HH
#define EOS_NS_FILESYSTEM_HANDLER_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/accounting/SetChangeList.hh"
#include "qclient/structures/QSet.hh"
#include <folly/futures/FutureSplitter.h>
#include <folly/executors/Async.h>
#include "common/Assert.hh"
#include "common/SteadyClock.hh"

namespace qclient
{
class QClient;
}

EOSNSNAMESPACE_BEGIN

class MetadataFlusher;
struct IsNoReplicaListTag {};

//------------------------------------------------------------------------------
//! Iterator to go through the contents of a FileSystemHandler. Keeps
//! the corresponding list read-locked during its lifetime.
//------------------------------------------------------------------------------
class FileListIterator : public ICollectionIterator<IFileMD::id_t>
{
public:

  //----------------------------------------------------------------------------
  //! Constructor.
  //----------------------------------------------------------------------------
  FileListIterator(const IFsView::FileList& fileList,
                   std::shared_timed_mutex& mtx)
    : pFileList(fileList), mLock(mtx)
  {
    mIterator = pFileList.begin();
  }

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  virtual ~FileListIterator() {}

  //----------------------------------------------------------------------------
  //! Check whether the iterator is still valid.
  //----------------------------------------------------------------------------
  virtual bool valid() override
  {
    return mIterator != pFileList.end();
  }

  //----------------------------------------------------------------------------
  //! Get current element.
  //----------------------------------------------------------------------------
  virtual IFileMD::id_t getElement() override
  {
    return *mIterator;
  }

  //----------------------------------------------------------------------------
  //! Progress iterator.
  //----------------------------------------------------------------------------
  virtual void next() override
  {
    mIterator++;
  }

private:
  const IFsView::FileList& pFileList;
  std::shared_lock<std::shared_timed_mutex> mLock;
  IFsView::FileList::const_iterator mIterator;
};

//------------------------------------------------------------------------------
//! Streaming iterator to go through the contents of a FileSystemHandler.
//!
//! Elements which are added, or deleted while iteration is ongoing, may or
//! may not be in the results.
//!
//! Also, watch out for races related to the flusher.. Use only if a weakly
//! consistent view is acceptable.
//------------------------------------------------------------------------------
class StreamingFileListIterator : public ICollectionIterator<IFileMD::id_t>
{
public:

  //----------------------------------------------------------------------------
  //! Constructor.
  //----------------------------------------------------------------------------
  StreamingFileListIterator(qclient::QClient& qcl, const std::string& key)
    : mQSet(qcl, key), it(mQSet.getIterator()) {}

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  virtual ~StreamingFileListIterator() {}

  //----------------------------------------------------------------------------
  //! Check whether the iterator is still valid.
  //----------------------------------------------------------------------------
  virtual bool valid() override
  {
    return it.valid();
  }

  //----------------------------------------------------------------------------
  //! Get current element.
  //----------------------------------------------------------------------------
  virtual IFileMD::id_t getElement() override
  {
    return std::stoull(it.getElement());
  }

  //----------------------------------------------------------------------------
  //! Progress iterator.
  //----------------------------------------------------------------------------
  virtual void next() override
  {
    return it.next();
  }

private:
  qclient::QSet mQSet;
  qclient::QSet::Iterator it;
};


class FileSystemHandler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param location file system ID
  //! @param qcl QClient object to use for loading the view from QDB
  //! @param flusher Flusher object for propagating updates to the backend
  //! @param unlinked whether we want the unlinked file list, or the regular one
  //! @param fake_clock if true is fake clock implementation for tests
  //----------------------------------------------------------------------------
  FileSystemHandler(IFileMD::location_t location, folly::Executor* pExecutor,
                    qclient::QClient* qcl, MetadataFlusher* flusher,
                    bool unlinked, bool fake_clock = false);

  //----------------------------------------------------------------------------
  //! Constructor for the special case of "no replica list".
  //!
  //! @param location file system ID
  //! @param qcl QClient object to use for loading the view from QDB
  //! @param flusher Flusher object for propagating updates to the backend
  //! @param Tag for dispatching to this constructor overload
  //----------------------------------------------------------------------------
  FileSystemHandler(folly::Executor* pExecutor, qclient::QClient* qcl,
                    MetadataFlusher* flusher, IsNoReplicaListTag tag);

  //----------------------------------------------------------------------------
  //! Ensure contents have been loaded into the cache. If so, returns
  //! immediatelly. Otherwise, does requests to QDB to retrieve its contents.
  //! Return value: "this" pointer.
  //----------------------------------------------------------------------------
  FileSystemHandler* ensureContentsLoaded();

  //----------------------------------------------------------------------------
  //! Async version of ensureContentsLoaded.
  //----------------------------------------------------------------------------
  folly::Future<FileSystemHandler*> ensureContentsLoadedAsync();

  //----------------------------------------------------------------------------
  //! Insert item.
  //----------------------------------------------------------------------------
  void insert(FileIdentifier identifier);

  //----------------------------------------------------------------------------
  //! Erase item.
  //----------------------------------------------------------------------------
  void erase(FileIdentifier identifier);

  //----------------------------------------------------------------------------
  //! Get number of file entries stored on this particular file system
  //----------------------------------------------------------------------------
  uint64_t size();

  //----------------------------------------------------------------------------
  //! Return redis key holding our target filesystem list.
  //----------------------------------------------------------------------------
  std::string getRedisKey() const;

  //----------------------------------------------------------------------------
  //! Return iterator for this file system. Note that the iterator keeps
  //! this filesystem read-locked during its entire lifetime.
  //----------------------------------------------------------------------------
  std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
      getFileList();

  //----------------------------------------------------------------------------
  //! Retrieve streaming iterator to go through the contents of a
  //! FileSystemHandler.
  //!
  //! Elements which are added, or deleted while iteration is ongoing, may or
  //! may not be in the results.
  //!
  //! Also, watch out for races related to the flusher.. Use only if a weakly
  //! consistent view is acceptable.
  //----------------------------------------------------------------------------
  std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
      getStreamingFileList();

  //----------------------------------------------------------------------------
  //! Delete the entire filelist.
  //----------------------------------------------------------------------------
  void nuke();

  //----------------------------------------------------------------------------
  //! Get an approximately random file in the filelist.
  //----------------------------------------------------------------------------
  bool getApproximatelyRandomFile(IFileMD::id_t& res);

  //----------------------------------------------------------------------------
  //! Check whether a given id_t is contained in this filelist
  //----------------------------------------------------------------------------
  bool hasFileId(IFileMD::id_t file);

  //----------------------------------------------------------------------------
  //! Clear cache if given timeout is exceeded
  //!
  //! @param inactive_timeout timeout in seconds since the last time there was
  //!        a call that required the entries to be actually loaded in memory.
  //!        If inactive timeout is 0 then the cache is cleared immediately. By
  //!        default once every 30 minutes.
  //----------------------------------------------------------------------------
  void clearCache(std::chrono::seconds inactive_timeout =
                    std::chrono::seconds(30 * 60));

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  //----------------------------------------------------------------------------
  //! Cache status states
  //----------------------------------------------------------------------------
  enum class CacheStatus {
    kNotLoaded,
    kInFlight,
    kLoaded
  };

  CacheStatus mCacheStatus =
    CacheStatus::kNotLoaded; ///< Stores caching status for this fs

  enum class Target {
    kRegular,
    kUnlinked,
    kNoReplicaList
  };

  Target target;                            ///< The filesystem list type this class is targetting.
  IFileMD::location_t location;             ///< Filesystem ID, if available
  folly::Executor* pExecutor;               ///< Folly executor
  qclient::QClient* pQcl;                   ///< QClient object
  MetadataFlusher* pFlusher;                ///< Metadata flusher object
  mutable std::shared_timed_mutex mMutex;           ///< Object mutex
  //! Actual contents. May be incomplete if mCacheStatus != kLoaded.
  IFsView::FileList mContents;
  //! ChangeList for what happens when cache loading is in progress.
  SetChangeList<IFileMD::id_t> mChangeList;
  folly::FutureSplitter<FileSystemHandler*> mSplitter;
  //! Timestamp of the last mandatory cache load attempt. This value will be
  //! used to decide when the cache contents can be dropped.
  std::atomic<uint64_t> mLastCacheLoadTS;
  eos::common::SteadyClock mClock;

  //----------------------------------------------------------------------------
  //! Trigger cache load. Must only be called once.
  //----------------------------------------------------------------------------
  FileSystemHandler* triggerCacheLoad();

  //----------------------------------------------------------------------------
  //! Get cache status
  //----------------------------------------------------------------------------
  inline CacheStatus getCacheStatus() const
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    return mCacheStatus;
  }
};

EOSNSNAMESPACE_END

#endif
