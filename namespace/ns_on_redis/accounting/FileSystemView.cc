/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
      file->mNumAsyncReq++;
      pRedox->sadd(sNoReplicaPrefix, file->getId(), file->mNotificationCb);
      break;

    // File has been deleted
    case IFileMDChangeListener::Deleted:
      // TODO: no file object in this case - make it a bit more consistent
      pRedox->srem(sNoReplicaPrefix, e->fileId);
      break;

    // Add location
    case IFileMDChangeListener::LocationAdded:
      val = std::to_string(e->location);
      file->mNumAsyncReq++;
      pRedox->sadd(sSetFsIds, val, file->mNotificationCb);
      key = val + sFilesSuffix;
      val = std::to_string(e->file->getId());
      file->mNumAsyncReq++;
      pRedox->sadd(key, val, file->mNotificationCb);
      file->mNumAsyncReq++;
      pRedox->srem(sNoReplicaPrefix, val, file->mNotificationCb);
      break;

    // Replace location
    case IFileMDChangeListener::LocationReplaced:
      key = std::to_string(e->oldLocation)+ sFilesSuffix;
      val = std::to_string(e->file->getId());
      file->mNumAsyncReq++;
      pRedox->srem(key, val, file->mNotificationCb);
      key = std::to_string(e->location) + sFilesSuffix;
      file->mNumAsyncReq++;
      pRedox->sadd(key, val, file->mNotificationCb);
      break;

    // Remove location
    case IFileMDChangeListener::LocationRemoved:
      key = std::to_string(e->location) + sUnlinkedSuffix;
      val = std::to_string(e->file->getId());
      pRedox->srem(key, val);

      if(!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation())
      {
	file->mNumAsyncReq++;
	pRedox->sadd(sNoReplicaPrefix, val, file->mNotificationCb);
      }

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
      pRedox->srem(key, val);
      key = std::to_string(e->location) + sUnlinkedSuffix;
      pRedox->sadd(key, val);
      break;

    default:
      break;
  }
}

//------------------------------------------------------------------------------
// Get set of files on filesystem
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + sFilesSuffix;

  if (!pRedox->exists(key))
  {
    MDException e( ENOENT );
    e.getMessage() << "Location " << key  << " does not exist" << std::endl;
    throw( e );
  }

  // TODO: use sscan to read in the members
  IFsView::FileList set_files;
  std::set<std::string> files = pRedox->smembers(key);

  for (const auto& elem: files)
    set_files.insert(std::stoul(elem));

  return set_files;
}

//------------------------------------------------------------------------------
// Get set of unlinked files
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = std::to_string(location) + sUnlinkedSuffix;
  // TODO: use sscan to read in the members
  std::set<std::string> unlinked = pRedox->smembers(key);
  IFsView::FileList set_unlinked;

  for (const auto& elem: unlinked)
    set_unlinked.insert(std::stoul(elem));

  return set_unlinked;
}

//------------------------------------------------------------------------------
// Get set of files without replicas
//------------------------------------------------------------------------------
IFsView::FileList FileSystemView::getNoReplicasFileList()
{
  IFsView::FileList set_noreplicas;
  // TODO: use sscan to read in the members
  std::set<std::string> noreplicas = pRedox->smembers(sNoReplicaPrefix);

  for (const auto&  elem: noreplicas)
    set_noreplicas.insert(std::stoul(elem));

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
