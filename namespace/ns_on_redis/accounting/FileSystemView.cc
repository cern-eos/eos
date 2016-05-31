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
#include "namespace/ns_on_redis/FileMD.hh"
#include <iostream>

EOSNSNAMESPACE_BEGIN

const std::string FileSystemView::sSetFsIds = "fsview_set_fsid";
const std::string FileSystemView::sFilesSuffix = ":fsview_files";
const std::string FileSystemView::sUnlinkedSuffix = ":fsview_unlinked";
const std::string FileSystemView::sNoReplicaPrefix = "fsview_noreplicas";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemView::FileSystemView()
{
  pRedox = RedisClient::getInstance();
}

//------------------------------------------------------------------------------
// Notify the me about the changes in the main view
//------------------------------------------------------------------------------
void FileSystemView::fileMDChanged(IFileMDChangeListener::Event *e)
{
  std::string key, val;
  FileMD* file = static_cast<FileMD*>(e->file);

  switch( e->action )
  {
    // New file has been created
    case IFileMDChangeListener::Created:
      pRedox->sadd(sNoReplicaPrefix, file->getId(), file->mWrapperCb());
      break;

    // File has been deleted
    case IFileMDChangeListener::Deleted:
      // Note: for this type oc action we only have the file id
      pRedox->srem(sNoReplicaPrefix, e->fileId);
      break;

    // Add location
    case IFileMDChangeListener::LocationAdded:
      val = std::to_string(e->location);
      pRedox->sadd(sSetFsIds, val, file->mWrapperCb());
      key = val + sFilesSuffix;
      val = std::to_string(file->getId());
      pRedox->sadd(key, val, file->mWrapperCb());
      pRedox->srem(sNoReplicaPrefix, val, file->mWrapperCb());
      break;

    // Replace location
    case IFileMDChangeListener::LocationReplaced:
      key = std::to_string(e->oldLocation)+ sFilesSuffix;
      val = std::to_string(file->getId());
      pRedox->srem(key, val, file->mWrapperCb());
      key = std::to_string(e->location) + sFilesSuffix;
      pRedox->sadd(key, val, file->mWrapperCb());
      break;

    // Remove location
    case IFileMDChangeListener::LocationRemoved:
      key = std::to_string(e->location) + sUnlinkedSuffix;
      val = std::to_string(file->getId());
      file->SetConsistent(false);
      pRedox->srem(key, val);

      if(!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation())
	pRedox->sadd(sNoReplicaPrefix, val, file->mWrapperCb());

      // Cleanup fsid if it doesn't hold any files anymore
      key = std::to_string(e->location) + sFilesSuffix;

      if (!pRedox->exists(key))
      {
	key = std::to_string(e->location) + sUnlinkedSuffix;

	if (!pRedox->exists(key))
	{
	  // FS does not hold any file replicas or unlinked ones, remove it
	  key = std::to_string(e->location);
	  pRedox->srem(sSetFsIds, key);
	}
      }
      break;

    // Unlink location
    case IFileMDChangeListener::LocationUnlinked:
      key = std::to_string(e->location) + sFilesSuffix;
      val = std::to_string(e->file->getId());
      pRedox->srem(key, val, file->mWrapperCb());
      key = std::to_string(e->location) + sUnlinkedSuffix;
      pRedox->sadd(key, val, file->mWrapperCb());
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
  std::pair< long long, std::vector<std::string> > reply;
  // Variables used for the asynchronous callbacks
  std::atomic<bool> has_error {false};
  std::mutex mutex;
  std::condition_variable cond_var;
  std::atomic<std::int32_t> num_async_req {0};
  // Function called by the redox client when async response arrives
  auto callback = [&](redox::Command<int>& c) {
    if (!c.ok())
    {
      std::unique_lock<std::mutex> lock(mutex);
      has_error = true;
    }

    if (--num_async_req == 0)
      cond_var.notify_one();
  };

  // Wrapper callback that accounts for the number of issued requests
  auto wrapper_cb = [&]() -> decltype(callback) {
    num_async_req++;
    return callback;
  };

  // If file has no replicas make sure it's accounted for
  if (has_no_replicas)
  {
    pRedox->sadd(sNoReplicaPrefix, file->getId(), wrapper_cb());
  }
  else
  {
    pRedox->srem(sNoReplicaPrefix, file->getId(), wrapper_cb());
  }

  do {
    reply = pRedox->sscan(sSetFsIds, cursor);
    cursor = reply.first;
    IFileMD::id_t fsid;

    for (auto&& sfsid: reply.second)
    {
      fsid = std::stoull(sfsid);

      // Deal with the fs replica set
      key = sfsid + sFilesSuffix;

      if (std::find(replica_locs.begin(), replica_locs.end(), fsid) !=
	  replica_locs.end())
      {
	pRedox->sadd(key, file->getId(), wrapper_cb());
      }
      else
      {
	pRedox->srem(key, file->getId(), wrapper_cb());
      }

      // Deal with the fs unlinked set
      key = sfsid + sUnlinkedSuffix;

      if (std::find(unlink_locs.begin(), unlink_locs.end(), fsid) !=
	       unlink_locs.end())
      {
	pRedox->sadd(key, file->getId(), wrapper_cb());
      }
      else
      {
	pRedox->srem(key, file->getId(), wrapper_cb());
      }
    }
  }
  while (cursor);

  {
    // Wait for all async responses
    std::unique_lock<std::mutex> lock(mutex);
    while (num_async_req)
      cond_var.wait(lock);
  }

  return !has_error;
}

//------------------------------------------------------------------------------
// Get set of files on filesystem
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + sFilesSuffix;
  IFsView::FileList set_files;
  std::pair<long long, std::vector<std::string>> reply;
  long long cursor = 0, count = 10000;

  do
  {
    reply = pRedox->sscan(key, cursor, count);
    cursor = reply.first;

    for (const auto& elem: reply.second)
      set_files.insert(std::stoul(elem));
  }
  while (cursor);

  return set_files;
}

//------------------------------------------------------------------------------
// Get set of unlinked files
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + sUnlinkedSuffix;
  IFsView::FileList set_unlinked;
  std::pair<long long, std::vector<std::string>> reply;
  long long cursor = 0, count = 10000;

  do
  {
    reply = pRedox->sscan(key, cursor, count);
    cursor = reply.first;

    for (const auto& elem: reply.second)
      set_unlinked.insert(std::stoul(elem));
  }
  while (cursor);

  return set_unlinked;
}

//------------------------------------------------------------------------------
// Get set of files without replicas
//------------------------------------------------------------------------------
IFsView::FileList FileSystemView::getNoReplicasFileList()
{
  IFsView::FileList set_noreplicas;
  std::pair<long long, std::vector<std::string>> reply;
  long long cursor = 0, count = 10000;

  do
  {
    reply = pRedox->sscan(sNoReplicaPrefix, cursor, count);
    cursor = reply.first;

    for (const auto&  elem: reply.second)
      set_noreplicas.insert(std::stoul(elem));
  }
  while (cursor);

  return set_noreplicas;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + sUnlinkedSuffix;
  return pRedox->del(key);
}

//------------------------------------------------------------------------------
// Get number of file systems
//------------------------------------------------------------------------------
size_t FileSystemView::getNumFileSystems()
{
  try
  {
    return (size_t) pRedox->scard(sSetFsIds);
  }
  catch (std::runtime_error &e)
  {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Initialize for testing purposes
//------------------------------------------------------------------------------
void
FileSystemView::initialize(const std::map<std::string, std::string>& config)
{
  std::string key_host = "redis_host";
  std::string key_port = "redis_port";
  std::string host {""};
  uint32_t port {0};

  if (config.find(key_host) != config.end())
    host = config.find(key_host)->second;

  if (config.find(key_port) != config.end())
    port = std::stoul(config.find(key_port)->second);

  pRedox = RedisClient::getInstance(host, port);
}

EOSNSNAMESPACE_END
