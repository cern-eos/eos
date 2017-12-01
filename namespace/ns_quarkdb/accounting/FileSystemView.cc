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
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "common/StringTokenizer.hh"
#include "common/Logging.hh"
#include "qclient/QScanner.hh"
#include <iostream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemView::FileSystemView():
  pFlusher(nullptr), pQcl(nullptr)
{
  pNoReplicas.set_empty_key(0xffffffffffffffffll);
  pNoReplicas.set_deleted_key(0);
}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void
FileSystemView::configure(const std::map<std::string, std::string>& config)
{
  std::string qdb_cluster;
  std::string qdb_flusher_id;
  const std::string key_cluster = "qdb_cluster";
  const std::string key_flusher = "qdb_flusher_md";

  if ((pQcl == nullptr) && (pFlusher == nullptr)) {
    if ((config.find(key_cluster) != config.end()) &&
        (config.find(key_flusher) != config.end())) {
      qdb_cluster = config.at(key_cluster);
      qdb_flusher_id = config.at(key_flusher);
    } else {
      eos::MDException e(EINVAL);
      e.getMessage() << __FUNCTION__  << " No " << key_cluster << " or "
                     << key_flusher << " configuration info provided";
      throw e;
    }

    qclient::Members qdb_members;

    if (!qdb_members.parse(qdb_cluster)) {
      eos::MDException e(EINVAL);
      e.getMessage() << __FUNCTION__ << " Failed to parse qdbcluster members: "
                     << qdb_cluster;
      throw e;
    }

    pQcl = BackendClient::getInstance(qdb_members);
    pNoReplicasSet.setClient(*pQcl);
    pNoReplicasSet.setKey(fsview::sNoReplicaPrefix);
    pFlusher = MetadataFlusherFactory::getInstance(qdb_flusher_id, qdb_members);
  }

  auto start = std::time(nullptr);
  loadFromBackend();
  auto end = std::time(nullptr);
  std::chrono::seconds duration(end - start);
  std::cerr << "FileSystemView loadingFromBackend duration: "
            << duration.count() << " seconds" << std::endl;
}

//------------------------------------------------------------------------------
// Notify the me about changes in the main view
//------------------------------------------------------------------------------
void
FileSystemView::fileMDChanged(IFileMDChangeListener::Event* e)
{
  std::string key, val;
  FileMD* file = static_cast<FileMD*>(e->file);
  qclient::QSet fs_set;

  switch (e->action) {
  // New file has been created
  case IFileMDChangeListener::Created:
    if (!file->isLink()) {
      pNoReplicas.insert(file->getId());
      pFlusher->sadd(fsview::sNoReplicaPrefix, std::to_string(file->getId()));
    }

    break;

  // File has been deleted
  case IFileMDChangeListener::Deleted: {
    pNoReplicas.erase(file->getId());
    pFlusher->srem(fsview::sNoReplicaPrefix, std::to_string(file->getId()));
    break;
  }

  // Add location
  case IFileMDChangeListener::LocationAdded: {
    auto it = pFiles.find(e->location);

    if (it == pFiles.end()) {
      auto pair = pFiles.emplace(e->location, IFsView::FileList());
      auto& file_set = pair.first->second;
      file_set.set_deleted_key(0);
      file_set.set_empty_key(0xffffffffffffffffll);
      file_set.insert(file->getId());
    } else {
      it->second.insert(file->getId());
    }

    pNoReplicas.erase(file->getId());
    // Commit to the backend
    key = keyFilesystemFiles(e->location);
    val = std::to_string(file->getId());
    pFlusher->sadd(key, val);
    pFlusher->srem(fsview::sNoReplicaPrefix, val);
    break;
  }

  // Replace location
  case IFileMDChangeListener::LocationReplaced: {
    auto it = pFiles.find(e->oldLocation);

    if (it != pFiles.end()) {
      it->second.erase(file->getId());
    }

    it = pFiles.find(e->location);

    if (it == pFiles.end()) {
      auto pair = pFiles.emplace(e->location, IFsView::FileList());
      auto& file_set = pair.first->second;
      file_set.set_deleted_key(0);
      file_set.set_empty_key(0xffffffffffffffffll);
      file_set.insert(file->getId());
    } else {
      it->second.insert(file->getId());
    }

    key = keyFilesystemFiles(e->oldLocation);
    val = std::to_string(file->getId());
    pFlusher->srem(key, val);
    key = keyFilesystemFiles(e->location);
    pFlusher->sadd(key, val);
    break;
  }

  // Remove location
  case IFileMDChangeListener::LocationRemoved: {
    auto it = pUnlinkedFiles.find(e->location);

    if (it != pUnlinkedFiles.end()) {
      it->second.erase(file->getId());
    }

    key = keyFilesystemUnlinked(e->location);
    val = std::to_string(file->getId());
    pFlusher->srem(key, val);

    if (!file->getNumUnlinkedLocation() && !file->getNumLocation()) {
      pNoReplicas.insert(file->getId());
      pFlusher->sadd(fsview::sNoReplicaPrefix, val);
    }

    break;
  }

  // Unlink location
  case IFileMDChangeListener::LocationUnlinked: {
    auto it = pFiles.find(e->location);

    if (it != pFiles.end()) {
      it->second.erase(file->getId());
    }

    it = pUnlinkedFiles.find(e->location);

    if (it == pUnlinkedFiles.end()) {
      auto pair = pUnlinkedFiles.emplace(e->location, IFsView::FileList());
      auto& file_set = pair.first->second;
      file_set.set_deleted_key(0);
      file_set.set_empty_key(0xffffffffffffffffll);
      file_set.insert(file->getId());
    } else {
      it->second.insert(file->getId());
    }

    key = keyFilesystemFiles(e->location);
    val = std::to_string(e->file->getId());
    pFlusher->srem(key, val);
    key = keyFilesystemUnlinked(e->location);
    pFlusher->sadd(key, val);
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
FileSystemView::fileMDCheck(IFileMD* file)
{
  std::string key;
  IFileMD::LocationVector replica_locs = file->getLocations();
  IFileMD::LocationVector unlink_locs = file->getUnlinkedLocations();
  bool has_no_replicas = replica_locs.empty() && unlink_locs.empty();
  std::string cursor {"0"};
  std::pair<std::string, std::vector<std::string>> reply;
  qclient::AsyncHandler ah;

  // If file has no replicas make sure it's accounted for
  if (has_no_replicas) {
    pNoReplicasSet.sadd_async(file->getId(), &ah);
  } else {
    pNoReplicasSet.srem_async(file->getId(), &ah);
  }

  // Make sure all active locations are accounted for
  qclient::QSet replica_set(*pQcl, "");

  for (IFileMD::location_t location : replica_locs) {
    replica_set.setKey(keyFilesystemFiles(location));
    replica_set.sadd_async(file->getId(), &ah);
  }

  // Make sure all unlinked locations are accounted for.
  qclient::QSet unlink_set(*pQcl, "");

  for (IFileMD::location_t location : unlink_locs) {
    unlink_set.setKey(keyFilesystemUnlinked(location));
    unlink_set.sadd_async(file->getId(), &ah);
  }

  // Make sure there's no other filesystems that erroneously contain this file.
  for (auto it = this->getFileSystemIterator(); it->valid(); it->next()) {
    IFileMD::location_t fsid = it->getElement();

    if (std::find(replica_locs.begin(), replica_locs.end(),
                  fsid) == replica_locs.end()) {
      replica_set.setKey(keyFilesystemFiles(fsid));
      replica_set.srem_async(file->getId(), &ah);
    }

    if (std::find(unlink_locs.begin(), unlink_locs.end(),
                  fsid) == unlink_locs.end()) {
      unlink_set.setKey(keyFilesystemUnlinked(fsid));
      unlink_set.srem_async(file->getId(), &ah);
    }
  }

  // Wait for all async responses
  return ah.Wait();
}

//------------------------------------------------------------------------------
// Get iterator object to run through all currently active filesystem IDs
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
    FileSystemView::getFileSystemIterator()
{
  return std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
         (new ListFileSystemIterator(pFiles));
}

//----------------------------------------------------------------------------
// Get iterator to list of files on a particular file system
//----------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getFileList(IFileMD::location_t location)
{
  if (pFiles.find(location) == pFiles.end()) {
    return nullptr;
  }

  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(pFiles[location]));
}

//------------------------------------------------------------------------------
// Get iterator to list of unlinked files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  if (pUnlinkedFiles.find(location) == pUnlinkedFiles.end()) {
    return nullptr;
  }

  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(pUnlinkedFiles[location]));
}

//------------------------------------------------------------------------------
// Get iterator to list of files without replicas
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getNoReplicasFileList()
{
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(pNoReplicas));
}

//------------------------------------------------------------------------------
// Get number of files with no replicas
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumNoReplicasFiles()
{
  return pNoReplicas.size();
}

//------------------------------------------------------------------------------
// Get number of files on the given file system
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumFilesOnFs(IFileMD::location_t fs_id)
{
  auto it = pFiles.find(fs_id);

  if (it == pFiles.end()) {
    return 0ull;
  } else {
    return it->second.size();
  }
}

//------------------------------------------------------------------------------
// Get number of unlinked files on the given file system
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumUnlinkedFilesOnFs(IFileMD::location_t fs_id)
{
  auto it = pUnlinkedFiles.find(fs_id);

  if (it == pUnlinkedFiles.end()) {
    return 0ull;
  } else {
    return it->second.size();
  }
}

//------------------------------------------------------------------------------
// Check if file system has file id
//------------------------------------------------------------------------------
bool
FileSystemView::hasFileId(IFileMD::id_t fid, IFileMD::location_t fs_id) const
{
  auto it = pFiles.find(fs_id);

  if (it != pFiles.end()) {
    auto& files_set = it->second;

    if (files_set.find(fid) != files_set.end()) {
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  auto it = pUnlinkedFiles.find(location);

  if (it != pUnlinkedFiles.end()) {
    it->second.clear();
    it->second.resize(0);
  }

  std::string key = keyFilesystemUnlinked(location);
  pFlusher->del(key);
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
    FileSystemView::getQdbFileSystemIterator(const std::string& pattern)
{
  qclient::QScanner replicaSets(*pQcl, pattern);
  std::set<IFileMD::location_t> uniqueFilesytems;
  std::vector<std::string> results;

  while (replicaSets.next(results)) {
    for (std::string& rep : results) {
      // extract fsid from key
      IFileMD::location_t fsid;
      bool unused;

      if (!parseFsId(rep, fsid, unused)) {
        eos_static_crit("Unable to parse redis key: %s", rep.c_str());
        continue;
      }

      uniqueFilesytems.insert(fsid);
    }
  }

  return std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
         (new QdbFileSystemIterator(std::move(uniqueFilesytems)));
}

//------------------------------------------------------------------------------
// Get iterator to list of files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getQdbFileList(IFileMD::location_t location)
{
  std::string key = keyFilesystemFiles(location);
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new QdbFileIterator(*pQcl, key));
}

//------------------------------------------------------------------------------
// Get iterator to list of unlinked files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getQdbUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = keyFilesystemUnlinked(location);
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new QdbFileIterator(*pQcl, key));
}

//------------------------------------------------------------------------------
// Get iterator to list of files without replicas
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getQdbNoReplicasFileList()
{
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new QdbFileIterator(*pQcl, fsview::sNoReplicaPrefix));
}

//------------------------------------------------------------------------------
// Load view from backend
//------------------------------------------------------------------------------
void
FileSystemView::loadFromBackend()
{
  std::vector<std::string> patterns {
    fsview::sPrefix + "*:files",
    fsview::sPrefix + "*:unlinked" };

  for (const auto& pattern : patterns) {
    for (auto it = getQdbFileSystemIterator(pattern);
         (it && it->valid()); it->next()) {
      IFileMD::location_t fsid = it->getElement();

      if (pattern.find("unlinked") != std::string::npos) {
        auto pair = pUnlinkedFiles.emplace(fsid, IFsView::FileList());
        auto& set_ids = pair.first->second;
        set_ids.set_deleted_key(0);
        set_ids.set_empty_key(0xffffffffffffffffll);
      } else {
        auto pair = pFiles.emplace(fsid, IFsView::FileList());
        auto& set_ids = pair.first->second;
        set_ids.set_deleted_key(0);
        set_ids.set_empty_key(0xffffffffffffffffll);
      }
    }
  }

  // Load from the backend the files on each file system
  for (auto& elem : pFiles) {
    auto& set_ids = elem.second;

    for (auto it = getQdbFileList(elem.first);
         (it && it->valid()); it->next()) {
      set_ids.insert(it->getElement());
    }
  }

  // Load from the backend the unlinked files on each file system
  for (auto& elem : pUnlinkedFiles) {
    auto& set_ids = elem.second;

    for (auto it = getQdbUnlinkedFileList(elem.first);
         (it && it->valid()); it->next()) {
      set_ids.insert(it->getElement());
    }
  }

  // Load the no replica files
  for (auto it = getQdbNoReplicasFileList();
       (it && it->valid()); it->next()) {
    pNoReplicas.insert(it->getElement());
  }
}

EOSNSNAMESPACE_END
