/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/ConfigurationParser.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "common/StringTokenizer.hh"
#include "common/Logging.hh"
#include "qclient/structures/QScanner.hh"
#include "qclient/structures/QSet.hh"
#include <iostream>
#include <folly/executors/IOThreadPoolExecutor.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkFileSystemView::QuarkFileSystemView(qclient::QClient* qcl,
    MetadataFlusher* flusher)
  : pFlusher(flusher), pQcl(qcl), mExecutor(new folly::IOThreadPoolExecutor(8))
{ }

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void
QuarkFileSystemView::configure(const std::map<std::string, std::string>& config)
{
  // No configuration to read, everything we need has been passed to the
  // constructor already.
  auto start = std::time(nullptr);
  loadFromBackend();
  auto end = std::time(nullptr);
  std::chrono::seconds duration(end - start);
  eos_static_info("msg=\"FileSystemView loadFromBackend\" duration=%llus",
                  duration.count());
  mNoReplicas.reset(new FileSystemHandler(mExecutor.get(), pQcl, pFlusher,
                                          IsNoReplicaListTag()));
}

//------------------------------------------------------------------------------
// Notify the me about changes in the main view
//------------------------------------------------------------------------------
void
QuarkFileSystemView::fileMDChanged(IFileMDChangeListener::Event* e)
{
  std::string key, val;
  QuarkFileMD* file = static_cast<QuarkFileMD*>(e->file);
  qclient::QSet fs_set;

  switch (e->action) {
  //----------------------------------------------------------------------------
  // New file has been created
  //----------------------------------------------------------------------------
  case IFileMDChangeListener::Created:
    if (!file->isLink()) {
      mNoReplicas->insert(file->getIdentifier());
    }

    break;

  //----------------------------------------------------------------------------
  // File has been deleted
  //----------------------------------------------------------------------------
  case IFileMDChangeListener::Deleted: {
    mNoReplicas->erase(file->getIdentifier());
    break;
  }

  //----------------------------------------------------------------------------
  // Add location
  //----------------------------------------------------------------------------
  case IFileMDChangeListener::LocationAdded: {
    FileSystemHandler* handler = initializeRegularFilelist(e->location);
    handler->insert(file->getIdentifier());
    mNoReplicas->erase(file->getIdentifier());
    break;
  }

  //----------------------------------------------------------------------------
  // Remove location.
  //
  // Perform destructive actions (ie erase) at the end.
  // This ensures that if we crash in the middle, we don't lose data, just
  // become inconsistent.
  //----------------------------------------------------------------------------
  case IFileMDChangeListener::LocationRemoved: {
    if (!file->getNumUnlinkedLocation() && !file->getNumLocation()) {
      mNoReplicas->insert(file->getIdentifier());
    }

    FileSystemHandler* handlerUnlinked = fetchUnlinkedFilelistIfExists(e->location);

    if (handlerUnlinked) {
      handlerUnlinked->erase(file->getIdentifier());
    }

    break;
  }

  //----------------------------------------------------------------------------
  // Unlink location.
  //
  // Perform destructive actions (ie erase) at the end.
  // This ensures that if we crash in the middle, we don't lose data, just
  // become inconsistent.
  //----------------------------------------------------------------------------
  case IFileMDChangeListener::LocationUnlinked: {
    FileSystemHandler* handlerUnlinked = initializeUnlinkedFilelist(e->location);
    handlerUnlinked->insert(file->getIdentifier());
    FileSystemHandler* handlerRegular = fetchRegularFilelistIfExists(e->location);

    if (handlerRegular) {
      handlerRegular->erase(file->getIdentifier());
    }

    break;
  }

  default:
    break;
  }
}

//------------------------------------------------------------------------------
// Recheck the current file object and make any modifications necessary so
// that the information is consistent in the back-end KV store.
//------------------------------------------------------------------------------
bool
QuarkFileSystemView::fileMDCheck(IFileMD* file)
{
  std::string key;
  IFileMD::LocationVector replica_locs = file->getLocations();
  IFileMD::LocationVector unlink_locs = file->getUnlinkedLocations();
  bool has_no_replicas = replica_locs.empty() && unlink_locs.empty();
  std::string cursor {"0"};
  std::pair<std::string, std::vector<std::string>> reply;
  qclient::AsyncHandler ah;
  qclient::QSet no_replica_set(*pQcl, fsview::sNoReplicaPrefix);

  // If file has no replicas make sure it's accounted for
  if (has_no_replicas) {
    no_replica_set.sadd_async(std::to_string(file->getId()), &ah);
  } else {
    no_replica_set.srem_async(std::to_string(file->getId()), &ah);
  }

  // Make sure all active locations are accounted for
  qclient::QSet replica_set(*pQcl, "");

  for (IFileMD::location_t location : replica_locs) {
    replica_set.setKey(eos::RequestBuilder::keyFilesystemFiles(location));
    replica_set.sadd_async(std::to_string(file->getId()), &ah);
  }

  // Make sure all unlinked locations are accounted for.
  qclient::QSet unlink_set(*pQcl, "");

  for (IFileMD::location_t location : unlink_locs) {
    unlink_set.setKey(eos::RequestBuilder::keyFilesystemUnlinked(location));
    unlink_set.sadd_async(std::to_string(file->getId()), &ah);
  }

  // Make sure there's no other filesystems that erroneously contain this file.
  for (auto it = this->getFileSystemIterator(); it->valid(); it->next()) {
    IFileMD::location_t fsid = it->getElement();

    if (std::find(replica_locs.begin(), replica_locs.end(),
                  fsid) == replica_locs.end()) {
      replica_set.setKey(eos::RequestBuilder::keyFilesystemFiles(fsid));
      replica_set.srem_async(std::to_string(file->getId()), &ah);
    }

    if (std::find(unlink_locs.begin(), unlink_locs.end(),
                  fsid) == unlink_locs.end()) {
      unlink_set.setKey(eos::RequestBuilder::keyFilesystemUnlinked(fsid));
      unlink_set.srem_async(std::to_string(file->getId()), &ah);
    }
  }

  // Wait for all async responses
  return ah.Wait();
}

//------------------------------------------------------------------------------
// Get iterator object to run through all currently active filesystem IDs
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
    QuarkFileSystemView::getFileSystemIterator()
{
  std::unique_lock<std::mutex> lock(mMutex);
  return std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
         (new ListFileSystemIterator(mFiles));
}

//----------------------------------------------------------------------------
// Get iterator to list of files on a particular file system
//----------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    QuarkFileSystemView::getFileList(IFileMD::location_t location)
{
  FileSystemHandler* handler = fetchRegularFilelistIfExists(location);

  if (handler) {
    return handler->getFileList();
  }

  return nullptr;
}

//----------------------------------------------------------------------------
// Get streaming iterator to list of files on a particular file system
//----------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    QuarkFileSystemView::getStreamingFileList(IFileMD::location_t location)
{
  FileSystemHandler* handler = fetchRegularFilelistIfExists(location);

  if (handler) {
    return handler->getStreamingFileList();
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Erase an entry from all filesystem view collections
//------------------------------------------------------------------------------
void
QuarkFileSystemView::eraseEntry(IFileMD::location_t location, IFileMD::id_t fid)
{
  {
    FileSystemHandler* handler = fetchRegularFilelistIfExists(location);

    if (handler) {
      if (handler->hasFileId(fid)) {
        handler->erase(FileIdentifier(fid));
      }
    }
  }
  {
    FileSystemHandler* handler = fetchUnlinkedFilelistIfExists(location);

    if (handler) {
      if (handler->hasFileId(fid)) {
        handler->erase(FileIdentifier(fid));
      }
    }
  }
  mNoReplicas->erase(FileIdentifier(fid));
  return ;
}


//----------------------------------------------------------------------------
// Get an approximately random file residing within the given filesystem.
//----------------------------------------------------------------------------
bool QuarkFileSystemView::getApproximatelyRandomFileInFs(IFileMD::location_t
    location,
    IFileMD::id_t& retval)
{
  FileSystemHandler* handler = fetchRegularFilelistIfExists(location);

  if (handler) {
    return handler->getApproximatelyRandomFile(retval);
  }

  return false;
}

//------------------------------------------------------------------------------
// Get iterator to list of unlinked files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    QuarkFileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  FileSystemHandler* handlerUnlinked = fetchUnlinkedFilelistIfExists(location);

  if (handlerUnlinked) {
    return handlerUnlinked->getFileList();
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Get iterator to list of files without replicas
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    QuarkFileSystemView::getNoReplicasFileList()
{
  return mNoReplicas->getFileList();
}

//------------------------------------------------------------------------------
// Get number of files with no replicas
//------------------------------------------------------------------------------
uint64_t
QuarkFileSystemView::getNumNoReplicasFiles()
{
  return mNoReplicas->size();
}

//------------------------------------------------------------------------------
// Get number of files on the given file system
//------------------------------------------------------------------------------
uint64_t
QuarkFileSystemView::getNumFilesOnFs(IFileMD::location_t fs_id)
{
  FileSystemHandler* handler = fetchRegularFilelistIfExists(fs_id);

  if (handler) {
    return handler->size();
  }

  return 0ull;
}

//------------------------------------------------------------------------------
// Get number of unlinked files on the given file system
//------------------------------------------------------------------------------
uint64_t
QuarkFileSystemView::getNumUnlinkedFilesOnFs(IFileMD::location_t fs_id)
{
  FileSystemHandler* handlerUnlinked = fetchUnlinkedFilelistIfExists(fs_id);

  if (handlerUnlinked) {
    return handlerUnlinked->size();
  }

  return 0ull;
}

//------------------------------------------------------------------------------
// Check if file system has file id
//------------------------------------------------------------------------------
bool
QuarkFileSystemView::hasFileId(IFileMD::id_t fid, IFileMD::location_t fs_id)
{
  FileSystemHandler* handler = fetchRegularFilelistIfExists(fs_id);

  if (handler) {
    return handler->hasFileId(fid);
  }

  return false;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
QuarkFileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  FileSystemHandler* handlerUnlinked = fetchUnlinkedFilelistIfExists(location);

  if (!handlerUnlinked) {
    return false;
  }

  handlerUnlinked->nuke();
  return true;
}

//----------------------------------------------------------------------------
// Parse an fs set key, returning its id and whether it points to "files" or
// "unlinked"
//----------------------------------------------------------------------------
bool parseFsId(const std::string& str, IFileMD::location_t& fsid,
               bool& unlinked)
{
  std::vector<std::string> parts =
    eos::common::StringTokenizer::split<std::vector<std::string>>(str, ':');

  if (parts.size() != 3) {
    return false;
  }

  if (parts[0] + ":" != fsview::sPrefix) {
    return false;
  }

  fsid = std::stoull(parts[1]);

  if (parts[2] == fsview::sFilesSuffix) {
    unlinked = false;
  } else if (parts[2] == fsview::sUnlinkedSuffix) {
    unlinked = true;
  } else {
    return false;
  }

  return true;
}

//----------------------------------------------------------------------------
// Get iterator object to run through all currently active filesystem IDs
//----------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
    QuarkFileSystemView::getQdbFileSystemIterator(const std::string& pattern)
{
  qclient::QScanner replicaSets(*pQcl, pattern);
  std::set<IFileMD::location_t> uniqueFilesytems;

  for (; replicaSets.valid(); replicaSets.next()) {
    // Extract fsid from key
    IFileMD::location_t fsid;
    bool unused;

    if (!parseFsId(replicaSets.getValue(), fsid, unused)) {
      eos_static_crit("Unable to parse key: %s", replicaSets.getValue().c_str());
      continue;
    }

    uniqueFilesytems.insert(fsid);
  }

  return std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
         (new QdbFileSystemIterator(std::move(uniqueFilesytems)));
}

//------------------------------------------------------------------------------
// Get iterator to list of files without replicas
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    QuarkFileSystemView::getStreamingNoReplicasFileList()
{
  return mNoReplicas->getStreamingFileList();
}

//------------------------------------------------------------------------------
// Load view from backend
//------------------------------------------------------------------------------
void
QuarkFileSystemView::loadFromBackend()
{
  std::vector<std::string> patterns {
    fsview::sPrefix + "*:files",
    fsview::sPrefix + "*:unlinked" };

  for (const auto& pattern : patterns) {
    for (auto it = getQdbFileSystemIterator(pattern);
         (it && it->valid()); it->next()) {
      IFileMD::location_t fsid = it->getElement();

      if (pattern.find("unlinked") != std::string::npos) {
        initializeUnlinkedFilelist(fsid);
      } else {
        initializeRegularFilelist(fsid);
      }
    }
  }
}

//------------------------------------------------------------------------------
//! Initialize FileSystemHandler for given filesystem ID, if not already
//! initialized. Otherwise, do nothing.
//!
//! In any case, return pointer to the corresponding FileSystemHandler.
//!
//! @param fsid file system id
//------------------------------------------------------------------------------
FileSystemHandler* QuarkFileSystemView::initializeRegularFilelist(
  IFileMD::location_t fsid)
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto iter = mFiles.find(fsid);

  if (iter != mFiles.end()) {
    // Found
    return iter->second.get();
  }

  mFiles[fsid].reset(new FileSystemHandler(fsid, mExecutor.get(), pQcl, pFlusher,
                     false));
  return mFiles[fsid].get();
}

//------------------------------------------------------------------------------
//! Fetch FileSystemHandler for a given filesystem ID, but do not initialize
//! if it doesn't exist, give back nullptr.
//!
//! @param fsid file system id
//------------------------------------------------------------------------------
FileSystemHandler* QuarkFileSystemView::fetchRegularFilelistIfExists(
  IFileMD::location_t fsid)
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto iter = mFiles.find(fsid);

  if (iter == mFiles.end()) {
    return nullptr;
  }

  return iter->second.get();
}

//------------------------------------------------------------------------------
//! Initialize unlinked FileSystemHandler for given filesystem ID,
//! if not already initialized. Otherwise, do nothing.
//!
//! In any case, return pointer to the corresponding FileSystemHandler.
//!
//! @param fsid file system id
//------------------------------------------------------------------------------
FileSystemHandler* QuarkFileSystemView::initializeUnlinkedFilelist(
  IFileMD::location_t fsid)
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto iter = mUnlinkedFiles.find(fsid);

  if (iter != mUnlinkedFiles.end()) {
    // Found
    return iter->second.get();
  }

  mUnlinkedFiles[fsid].reset(new FileSystemHandler(fsid, mExecutor.get(), pQcl,
                             pFlusher, true));
  return mUnlinkedFiles[fsid].get();
}

//------------------------------------------------------------------------------
//! Fetch unlinked FileSystemHandler for a given filesystem ID, but do not
//! initialize if it doesn't exist, give back nullptr.
//!
//! @param fsid file system id
//------------------------------------------------------------------------------
FileSystemHandler* QuarkFileSystemView::fetchUnlinkedFilelistIfExists(
  IFileMD::location_t fsid)
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto iter = mUnlinkedFiles.find(fsid);

  if (iter == mUnlinkedFiles.end()) {
    return nullptr;
  }

  return iter->second.get();
}


EOSNSNAMESPACE_END
