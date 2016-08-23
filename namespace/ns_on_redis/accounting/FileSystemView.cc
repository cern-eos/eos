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

#include "namespace/ns_on_redis/accounting/FileSystemView.hh"
#include "namespace/ns_on_redis/Constants.hh"
#include "namespace/ns_on_redis/FileMD.hh"
#include <iostream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemView::FileSystemView():
  pRedox(RedisClient::getInstance()),
  pNoReplicasSet(*pRedox, fsview::sNoReplicaPrefix),
  pFsIdsSet(*pRedox, fsview::sSetFsIds)
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
  redox::RedoxSet fs_set;

  switch (e->action) {
  // New file has been created
  case IFileMDChangeListener::Created:
    pNoReplicasSet.sadd(file->getId(), file->mWrapperCb());
    break;

  // File has been deleted
  case IFileMDChangeListener::Deleted:
    pNoReplicasSet.srem(file->getId(), file->mWrapperCb());
    break;

  // Add location
  case IFileMDChangeListener::LocationAdded:
    val = std::to_string(e->location);
    pFsIdsSet.sadd(val, file->mWrapperCb());
    key = val + fsview::sFilesSuffix;
    val = std::to_string(file->getId());
    fs_set = redox::RedoxSet(*pRedox, key);
    fs_set.sadd(val, file->mWrapperCb());
    pNoReplicasSet.srem(val, file->mWrapperCb());
    break;

  // Replace location
  case IFileMDChangeListener::LocationReplaced:
    key = std::to_string(e->oldLocation) + fsview::sFilesSuffix;
    val = std::to_string(file->getId());
    fs_set = redox::RedoxSet(*pRedox, key);
    fs_set.srem(val, file->mWrapperCb());
    key = std::to_string(e->location) + fsview::sFilesSuffix;
    fs_set.setKey(key);
    fs_set.sadd(val, file->mWrapperCb());
    break;

  // Remove location
  case IFileMDChangeListener::LocationRemoved:
    key = std::to_string(e->location) + fsview::sUnlinkedSuffix;
    val = std::to_string(file->getId());
    fs_set = redox::RedoxSet(*pRedox, key);
    fs_set.srem(val);

    if (!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation()) {
      pNoReplicasSet.sadd(val, file->mWrapperCb());
    }

    // Cleanup fsid if it doesn't hold any files anymore
    key = std::to_string(e->location) + fsview::sFilesSuffix;

    if (!pRedox->exists(key)) {
      key = std::to_string(e->location) + fsview::sUnlinkedSuffix;

      if (!pRedox->exists(key)) {
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
    fs_set = redox::RedoxSet(*pRedox, key);
    fs_set.srem(val, file->mWrapperCb());
    key = std::to_string(e->location) + fsview::sUnlinkedSuffix;
    fs_set.setKey(key);
    fs_set.sadd(val, file->mWrapperCb());
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
  long long cursor = 0;
  std::pair<long long, std::vector<std::string>> reply;
  // Variables used for the asynchronous callbacks
  std::atomic<bool> has_error{false};
  std::mutex mutex;
  std::condition_variable cond_var;
  std::atomic<std::int32_t> num_async_req{0};
  // Function called by the redox client when async response arrives
  auto callback = [&](redox::Command<int>& c) {
    if (!c.ok()) {
      std::unique_lock<std::mutex> lock(mutex);
      has_error = true;
    }

    if (--num_async_req == 0) {
      cond_var.notify_one();
    }
  };
  // Wrapper callback that accounts for the number of issued requests
  auto wrapper_cb = [&]() -> decltype(callback) {
    num_async_req++;
    return callback;
  };

  // If file has no replicas make sure it's accounted for
  if (has_no_replicas) {
    pNoReplicasSet.sadd(file->getId(), wrapper_cb());
  } else {
    pNoReplicasSet.srem(file->getId(), wrapper_cb());
  }

  redox::RedoxSet replica_set(*pRedox, "");
  redox::RedoxSet unlink_set(*pRedox, "");
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
        replica_set.sadd(file->getId(), wrapper_cb());
      } else {
        replica_set.srem(file->getId(), wrapper_cb());
      }

      // Deal with the fs unlinked set
      key = sfsid + fsview::sUnlinkedSuffix;
      unlink_set.setKey(key);

      if (std::find(unlink_locs.begin(), unlink_locs.end(), fsid) !=
          unlink_locs.end()) {
        unlink_set.sadd(file->getId(), wrapper_cb());
      } else {
        unlink_set.srem(file->getId(), wrapper_cb());
      }
    }
  } while (cursor);

  {
    // Wait for all async responses
    std::unique_lock<std::mutex> lock(mutex);

    while (num_async_req) {
      cond_var.wait(lock);
    }
  }
  // Clean up all the fsids that don't hold any files either replicas
  // or unlinked
  std::vector<std::string> to_remove;

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
  } while (cursor);

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
  std::pair<long long, std::vector<std::string>> reply;
  long long cursor = 0, count = 10000;
  redox::RedoxSet fs_set(*pRedox, key);

  do {
    reply = fs_set.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_files.insert(std::stoul(elem));
    }
  } while (cursor);

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
  std::pair<long long, std::vector<std::string>> reply;
  long long cursor = 0, count = 10000;
  redox::RedoxSet fs_set(*pRedox, key);

  do {
    reply = fs_set.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_unlinked.insert(std::stoul(elem));
    }
  } while (cursor);

  return set_unlinked;
}

//------------------------------------------------------------------------------
// Get set of files without replicas
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getNoReplicasFileList()
{
  IFsView::FileList set_noreplicas;
  std::pair<long long, std::vector<std::string>> reply;
  long long cursor = 0, count = 10000;

  do {
    reply = pNoReplicasSet.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_noreplicas.insert(std::stoul(elem));
    }
  } while (cursor);

  return set_noreplicas;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + fsview::sUnlinkedSuffix;
  return pRedox->del(key);
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
  const std::string key_host = "redis_host";
  const std::string key_port = "redis_port";
  std::string host{""};
  uint32_t port{0};

  if (config.find(key_host) != config.end()) {
    host = config.find(key_host)->second;
  }

  if (config.find(key_port) != config.end()) {
    port = std::stoul(config.find(key_port)->second);
  }

  pRedox = RedisClient::getInstance(host, port);
  pNoReplicasSet.setClient(*pRedox);
  pFsIdsSet.setClient(*pRedox);
}

EOSNSNAMESPACE_END
