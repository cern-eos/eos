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
#include <iostream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemView::FileSystemView():
  pQcl(BackendClient::getInstance()),
  pNoReplicasSet(*pQcl, fsview::sNoReplicaPrefix),
  pFsIdsSet(*pQcl, fsview::sSetFsIds)
{
  // empty
}

//------------------------------------------------------------------------------
// Notify the me about the changes in the main view
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
    file->Register(pNoReplicasSet.sadd_async(file->getId()),
                   pNoReplicasSet.getClient());
    break;

  // File has been deleted
  case IFileMDChangeListener::Deleted:
    file->Register(pNoReplicasSet.srem_async(file->getId()),
                   pNoReplicasSet.getClient());
    break;

  // Add location
  case IFileMDChangeListener::LocationAdded:
    val = std::to_string(e->location);
    file->Register(pFsIdsSet.sadd_async(val), pFsIdsSet.getClient());
    key = val + fsview::sFilesSuffix;
    val = std::to_string(file->getId());
    fs_set = qclient::QSet(*pQcl, key);
    file->Register(fs_set.sadd_async(val), fs_set.getClient());
    file->Register(pNoReplicasSet.srem_async(val), pNoReplicasSet.getClient());
    break;

  // Replace location
  case IFileMDChangeListener::LocationReplaced:
    key = std::to_string(e->oldLocation) + fsview::sFilesSuffix;
    val = std::to_string(file->getId());
    fs_set = qclient::QSet(*pQcl, key);
    file->Register(fs_set.srem_async(val), fs_set.getClient());
    key = std::to_string(e->location) + fsview::sFilesSuffix;
    fs_set.setKey(key);
    file->Register(fs_set.sadd_async(val), fs_set.getClient());
    break;

  // Remove location
  case IFileMDChangeListener::LocationRemoved:
    key = std::to_string(e->location) + fsview::sUnlinkedSuffix;
    val = std::to_string(file->getId());
    fs_set = qclient::QSet(*pQcl, key);
    // TODO: this could be done asynchronously if first exists is false
    fs_set.srem(val);

    if (!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation()) {
      file->Register(pNoReplicasSet.sadd_async(val), pNoReplicasSet.getClient());
    }

    // Cleanup fsid if it doesn't hold any files anymore
    key = std::to_string(e->location) + fsview::sFilesSuffix;

    if (pQcl->exists(key) == 0) {
      key = std::to_string(e->location) + fsview::sUnlinkedSuffix;

      if (pQcl->exists(key) == 0) {
        // FS does not hold any file replicas or unlinked ones, remove it
        key = std::to_string(e->location);
        pFsIdsSet.srem(key);
      }
    }

    break;

  // Unlink location
  case IFileMDChangeListener::LocationUnlinked:
    key = std::to_string(e->location) + fsview::sFilesSuffix;
    val = std::to_string(e->file->getId());
    fs_set = qclient::QSet(*pQcl, key);
    file->Register(fs_set.srem_async(val), fs_set.getClient());
    key = std::to_string(e->location) + fsview::sUnlinkedSuffix;
    fs_set.setKey(key);
    file->Register(fs_set.sadd_async(val), fs_set.getClient());
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
  std::string key;
  IFileMD::LocationVector replica_locs = file->getLocations();
  IFileMD::LocationVector unlink_locs = file->getUnlinkedLocations();
  bool has_no_replicas = replica_locs.empty() && unlink_locs.empty();
  std::string cursor {"0"};
  std::pair<std::string, std::vector<std::string>> reply;
  std::atomic<bool> has_error{false};
  qclient::AsyncHandler ah;

  // If file has no replicas make sure it's accounted for
  if (has_no_replicas) {
    ah.Register(pNoReplicasSet.sadd_async(file->getId()),
                pNoReplicasSet.getClient());
  } else {
    ah.Register(pNoReplicasSet.srem_async(file->getId()),
                pNoReplicasSet.getClient());
  }

  qclient::QSet replica_set(*pQcl, "");
  qclient::QSet unlink_set(*pQcl, "");
  IFileMD::id_t fsid;

  do {
    reply = pFsIdsSet.sscan(cursor);
    cursor = reply.first;

    for (auto && sfsid : reply.second) {
      fsid = std::stoull(sfsid);
      // Deal with the fs replica set
      key = sfsid + fsview::sFilesSuffix;
      replica_set.setKey(key);

      if (std::find(replica_locs.begin(), replica_locs.end(), fsid) !=
          replica_locs.end()) {
        ah.Register(replica_set.sadd_async(file->getId()),
                    replica_set.getClient());
      } else {
        ah.Register(replica_set.srem_async(file->getId()),
                    replica_set.getClient());
      }

      // Deal with the fs unlinked set
      key = sfsid + fsview::sUnlinkedSuffix;
      unlink_set.setKey(key);

      if (std::find(unlink_locs.begin(), unlink_locs.end(), fsid) !=
          unlink_locs.end()) {
        ah.Register(unlink_set.sadd_async(file->getId()),
                    unlink_set.getClient());
      } else {
        ah.Register(unlink_set.srem_async(file->getId()),
                    unlink_set.getClient());
      }
    }
  } while (cursor != "0");

  // Wait for all async responses
  (void) ah.Wait();
  // Clean up all the fsids that don't hold any files either replicas
  // or unlinked
  std::list<std::string> to_remove;

  do {
    reply = pFsIdsSet.sscan(cursor);
    cursor = reply.first;

    for (auto && sfsid : reply.second) {
      fsid = std::stoull(sfsid);
      key = sfsid + fsview::sFilesSuffix;
      replica_set.setKey(key);
      key = sfsid + fsview::sUnlinkedSuffix;
      unlink_set.setKey(key);

      if ((replica_set.scard() == 0) && (unlink_set.scard() == 0)) {
        to_remove.emplace_back(sfsid);
      }
    }
  } while (cursor != "0");

  // Drop all the unused fs ids
  if (pFsIdsSet.srem(to_remove) != (long long int)to_remove.size()) {
    has_error = true;
  }

  return !has_error;
}

//------------------------------------------------------------------------------
// Get set of files on filesystem
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + fsview::sFilesSuffix;
  IFsView::FileList set_files;
  set_files.set_empty_key(-1);
  std::pair<std::string, std::vector<std::string>> reply;
  std::string cursor {"0"};
  long long count = 10000;
  qclient::QSet fs_set(*pQcl, key);

  do {
    reply = fs_set.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_files.insert(std::stoul(elem));
    }
  } while (cursor != "0");

  return set_files;
}

//------------------------------------------------------------------------------
// Get set of unlinked files
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + fsview::sUnlinkedSuffix;
  IFsView::FileList set_unlinked;
  set_unlinked.set_empty_key(-1);
  std::pair<std::string, std::vector<std::string>> reply;
  std::string cursor = {"0"};
  long long count = 10000;
  qclient::QSet fs_set(*pQcl, key);

  do {
    reply = fs_set.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_unlinked.insert(std::stoul(elem));
    }
  } while (cursor != "0");

  return set_unlinked;
}

//------------------------------------------------------------------------------
// Get set of files without replicas
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getNoReplicasFileList()
{
  IFsView::FileList set_noreplicas;
  set_noreplicas.set_empty_key(-1);
  std::pair<std::string, std::vector<std::string>> reply;
  std::string cursor {"0"};
  long long count = 10000;

  do {
    reply = pNoReplicasSet.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_noreplicas.insert(std::stoul(elem));
    }
  } while (cursor != "0");

  return set_noreplicas;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + fsview::sUnlinkedSuffix;
  return (pQcl->del(key) >= 0);
}

//------------------------------------------------------------------------------
// Get number of file systems
//------------------------------------------------------------------------------
size_t
FileSystemView::getNumFileSystems()
{
  try {
    return (size_t) pFsIdsSet.scard();
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Initialize for testing purposes
//------------------------------------------------------------------------------
void
FileSystemView::initialize(const std::map<std::string, std::string>& config)
{
  const std::string key_host = "qdb_host";
  const std::string key_port = "qdb_port";
  std::string host{""};
  uint32_t port{0};

  if (config.find(key_host) != config.end()) {
    host = config.find(key_host)->second;
  }

  if (config.find(key_port) != config.end()) {
    port = std::stoul(config.find(key_port)->second);
  }

  pQcl = BackendClient::getInstance(host, port);
  pNoReplicasSet.setClient(*pQcl);
  pFsIdsSet.setClient(*pQcl);
}

EOSNSNAMESPACE_END
