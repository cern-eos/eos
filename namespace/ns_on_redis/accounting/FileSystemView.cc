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

#include <iostream>

EOSNSNAMESPACE_BEGIN

const std::string FileSystemView::sSetFsIds = "fsview_set_fsid";
const std::string FileSystemView::sFilesPrefix = "fsview_files:";
const std::string FileSystemView::sUnlinkedPrefix = "fsview_unlinked:";
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

  switch( e->action )
  {
    // New file has been created
    case IFileMDChangeListener::Created:
      pRedox->sadd(sNoReplicaPrefix, std::to_string(e->file->getId()));
      break;

    // File has been deleted
    case IFileMDChangeListener::Deleted:
      pRedox->srem(sNoReplicaPrefix, std::to_string(e->fileId));
      break;

    // Add location
    case IFileMDChangeListener::LocationAdded:
      // Store fsid if it doesn't exist
      val = std::to_string(e->location);

      if (!pRedox->sismember(sSetFsIds, val))
	pRedox->sadd(sSetFsIds, val);

      key = sFilesPrefix + val;
      val = std::to_string(e->file->getId());
      pRedox->sadd(key, val);
      pRedox->sadd(sNoReplicaPrefix, val);
      break;

    // Replace location
    case IFileMDChangeListener::LocationReplaced:
      key = sFilesPrefix + std::to_string(e->oldLocation);

      if (!pRedox->exists(key))
	return; // inconstency, we should probably crash here...

      val = std::to_string(e->file->getId());
      pRedox->srem(key, val);
      key = sFilesPrefix + std::to_string(e->location);
      pRedox->sadd(key, val);
      break;

    // Remove location
    case IFileMDChangeListener::LocationRemoved:
      key = sUnlinkedPrefix + std::to_string(e->location);

      if (!pRedox->exists(key))
	return; // incostency, we should probably crash here...

      val = std::to_string(e->file->getId());
      pRedox->srem(key, val);

      if( !e->file->getNumUnlinkedLocation() && !e->file->getNumLocation() )
	pRedox->srem(sNoReplicaPrefix, val);

      break;

      // Unlink location
    case IFileMDChangeListener::LocationUnlinked:
      key = sFilesPrefix + std::to_string(e->location);

      if (!pRedox->exists(key))
	return; // incostency, we should probably crash here...

      val = std::to_string(e->file->getId());
      pRedox->srem(key, val);
      key = sUnlinkedPrefix + std::to_string(e->location);
      pRedox->sadd(key, val);
      break;

    default:
      break;
  }
}

//------------------------------------------------------------------------------
// Notify me about files when recovering from changelog
//------------------------------------------------------------------------------
void FileSystemView::fileMDRead( IFileMD *obj )
{
  IFileMD::LocationVector::const_iterator it;
  IFileMD::LocationVector loc_vect = obj->getLocations();
  std::string key, val;

  for( it = loc_vect.begin(); it != loc_vect.end(); ++it )
  {
    // Store fsid if it doesn't exist
    key = sSetFsIds;
    val = std::to_string(*it);

    if (!pRedox->sismember(key, val))
      pRedox->sadd(key, val);

    // Add file to corresponding fs file set
    key = sFilesPrefix + val;
    pRedox->sadd(key, std::to_string(obj->getId()));
  }

  IFileMD::LocationVector unlink_vect = obj->getUnlinkedLocations();

  for( it = unlink_vect.begin(); it != unlink_vect.end(); ++it )
  {
    key = sUnlinkedPrefix + std::to_string(*it);
    pRedox->sadd(key, std::to_string(obj->getId()));
  }

  if( obj->getNumLocation() == 0 && obj->getNumUnlinkedLocation() == 0 )
    pRedox->sadd(sNoReplicaPrefix, std::to_string(obj->getId()));
}

//------------------------------------------------------------------------------
// Get set of files on filesystem
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getFileList(IFileMD::location_t location)
{
  std::string key = sFilesPrefix + std::to_string(location);

  if (!pRedox->exists(key))
  {
    MDException e( ENOENT );
    e.getMessage() << "Location does not exist" << std::endl;
    throw( e );
  }

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
  std::string key = sUnlinkedPrefix + std::to_string(location);
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
  std::string key = sUnlinkedPrefix + std::to_string(location);
  return pRedox->del(key);
}


//------------------------------------------------------------------------------
// Get number of file systems
//------------------------------------------------------------------------------
size_t FileSystemView::getNumFileSystems()
{
  return (size_t) pRedox->scard(sSetFsIds);
}

//------------------------------------------------------------------------------
// Initialize
//------------------------------------------------------------------------------
void FileSystemView::initialize()
{
}

//------------------------------------------------------------------------------
// Finalize
//------------------------------------------------------------------------------
void FileSystemView::finalize()
{
}

EOSNSNAMESPACE_END
