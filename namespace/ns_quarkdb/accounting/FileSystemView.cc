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
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileSystemView::~FileSystemView()
{
  // @todo (esindril): this should be droppped and the flusher should
  // synchronize in his destructor
  if (pFlusher) {
    pFlusher->synchronize();
  }
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
    pFlusher->sadd(fsview::sNoReplicaPrefix, std::to_string(file->getId()));
    break;

  // File has been deleted
  case IFileMDChangeListener::Deleted:
    pFlusher->srem(fsview::sNoReplicaPrefix, std::to_string(file->getId()));
    break;

  // Add location
  case IFileMDChangeListener::LocationAdded:
    key = keyFilesystemFiles(e->location);
    val = std::to_string(file->getId());
    pFlusher->sadd(key, val);
    pFlusher->srem(fsview::sNoReplicaPrefix, val);
    break;

  // Replace location
  case IFileMDChangeListener::LocationReplaced:
    key = keyFilesystemFiles(e->oldLocation);
    val = std::to_string(file->getId());
    pFlusher->srem(key, val);
    key = keyFilesystemFiles(e->location);
    pFlusher->sadd(key, val);
    break;

  // Remove location
  case IFileMDChangeListener::LocationRemoved:
    key = keyFilesystemUnlinked(e->location);
    val = std::to_string(file->getId());
    pFlusher->srem(key, val);

    if (!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation()) {
      pFlusher->sadd(fsview::sNoReplicaPrefix, val);
    }

    break;

  // Unlink location
  case IFileMDChangeListener::LocationUnlinked:
    key = keyFilesystemFiles(e->location);
    val = std::to_string(e->file->getId());
    pFlusher->srem(key, val);
    key = keyFilesystemUnlinked(e->location);
    pFlusher->sadd(key, val);
    break;

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
  pFlusher->synchronize();
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
// Get iterator to list of files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getFileList(IFileMD::location_t location)
{
  pFlusher->synchronize();
  std::string key = keyFilesystemFiles(location);
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(*pQcl, key));
}

//------------------------------------------------------------------------------
// Get iterator to list of unlinked files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  pFlusher->synchronize();
  std::string key = keyFilesystemUnlinked(location);
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(*pQcl, key));
}

//------------------------------------------------------------------------------
// Get iterator to list of files without replicas
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getNoReplicasFileList()
{
  pFlusher->synchronize();
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(*pQcl, fsview::sNoReplicaPrefix));
}

//------------------------------------------------------------------------------
// Get number of files with no replicas
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumNoReplicasFiles()
{
  pFlusher->synchronize();

  try {
    return pNoReplicasSet.scard();
  } catch (std::runtime_error& qdb_err) {
    return 0ull;
  }
}

//------------------------------------------------------------------------------
// Get number of files on the given file system
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumFilesOnFs(IFileMD::location_t fs_id)
{
  pFlusher->synchronize();
  std::string key = keyFilesystemFiles(fs_id);

  try {
    qclient::QSet files_set(*pQcl, key);
    return files_set.scard();
  } catch (std::runtime_error& qdb_err) {
    return 0ull;
  }
}

//------------------------------------------------------------------------------
// Get number of unlinked files on the given file system
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumUnlinkedFilesOnFs(IFileMD::location_t fs_id)
{
  pFlusher->synchronize();
  const std::string key = keyFilesystemUnlinked(fs_id);

  try {
    qclient::QSet files_set(*pQcl, key);
    return files_set.scard();
  } catch (const std::runtime_error& qdb_err) {
    return 0ull;
  }
}


//------------------------------------------------------------------------------
// Check if file system has file id
//------------------------------------------------------------------------------
bool
FileSystemView::hasFileId(IFileMD::id_t fid, IFileMD::location_t fs_id) const
{
  pFlusher->synchronize();
  const std::string key = keyFilesystemFiles(fs_id);

  try {
    qclient::QSet files_set(*pQcl, key);
    return files_set.sismember(fid);
  } catch (const std::runtime_error& qdb_err) {
    return false;
  }
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  pFlusher->synchronize();
  std::string key = keyFilesystemUnlinked(location);
  return (pQcl->del(key) >= 0);
}

//----------------------------------------------------------------------------
// Get iterator object to run through all currently active filesystem IDs
//----------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::location_t>>
    FileSystemView::getFileSystemIterator()
{
  pFlusher->synchronize();
  qclient::QScanner replicaSets(*pQcl, fsview::sPrefix + "*:*");
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
         (new FileSystemIterator(std::move(uniqueFilesytems)));
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

EOSNSNAMESPACE_END
