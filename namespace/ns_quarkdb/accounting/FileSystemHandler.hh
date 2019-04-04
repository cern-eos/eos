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
#include "qclient/QSet.hh"
#include <folly/futures/FutureSplitter.h>
#include <folly/executors/Async.h>
#include "common/Assert.hh"

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
  virtual IFileMD::id_t getElement()
  {
    return *mIterator;
  }

  //----------------------------------------------------------------------------
  //! Progress iterator.
  //----------------------------------------------------------------------------
  virtual void next()
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
  virtual IFileMD::id_t getElement()
  {
    return std::stoull(it.getElement());
  }

  //----------------------------------------------------------------------------
  //! Progress iterator.
  //----------------------------------------------------------------------------
  virtual void next()
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
  //----------------------------------------------------------------------------
  FileSystemHandler(IFileMD::location_t location, folly::Executor* pExecutor,
                    qclient::QClient* qcl, MetadataFlusher *flusher,
                    bool unlinked);

  //----------------------------------------------------------------------------
  //! Constructor for the special case of "no replica list".
  //!
  //! @param location file system ID
  //! @param qcl QClient object to use for loading the view from QDB
  //! @param flusher Flusher object for propagating updates to the backend
  //! @param Tag for dispatching to this constructor overload
  //----------------------------------------------------------------------------
  FileSystemHandler(folly::Executor* pExecutor, qclient::QClient* qcl,
                    MetadataFlusher *flusher, IsNoReplicaListTag tag);

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
  //! Get size. Careful with calling this function, it'll load all contents if
  //! not already there.
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

private:
  //----------------------------------------------------------------------------
  //! Trigger cache load. Must only be called once.
  //----------------------------------------------------------------------------
  FileSystemHandler* triggerCacheLoad();

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
  MetadataFlusher *pFlusher;                ///< Metadata flusher object
  std::shared_timed_mutex mMutex;           ///< Object mutex
  IFsView::FileList
  mContents;              ///< Actual contents. May be incomplete if mCacheStatus != kLoaded.
  SetChangeList<IFileMD::id_t>
  mChangeList; ///< ChangeList for what happens when cache loading is in progress.

  folly::FutureSplitter<FileSystemHandler*> mSplitter;
};

EOSNSNAMESPACE_END

#endif
