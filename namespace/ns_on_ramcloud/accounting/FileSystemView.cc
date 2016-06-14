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

#include "namespace/ns_on_ramcloud/RamCloudClient.hh"
#include "namespace/ns_on_ramcloud/accounting/FileSystemView.hh"
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
}

//------------------------------------------------------------------------------
// Notify the me about the changes in the main view
//------------------------------------------------------------------------------
void FileSystemView::fileMDChanged(IFileMDChangeListener::Event *e)
{
  std::string key, val;
  uint64_t table_id;
  bool table_exists = true;
  RAMCloud::RamCloud* client = getRamCloudClient();

  switch( e->action )
  {
    // New file has been created
    case IFileMDChangeListener::Created:
      try {
	table_id = client->getTableId(sNoReplicaPrefix.c_str());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	table_id = client->createTable(sNoReplicaPrefix.c_str());
      }

      try {
	std::string key = std::to_string(e->file->getId());
	client->write(table_id, static_cast<const void*>(key.c_str()),
		     key.length(), static_cast<const char*>(0));
      }
      catch (RAMCloud::ClientException& e) {}
      break;

    // File has been deleted
    case IFileMDChangeListener::Deleted:
      try {
	table_id = client->getTableId(sNoReplicaPrefix.c_str());
	key = std::to_string(e->fileId);
	client->remove(table_id, static_cast<const void*>(key.c_str()),
		       key.length());
      }
      catch (RAMCloud::TableDoesntExistException& e) {}
      break;

    // Add location
    case IFileMDChangeListener::LocationAdded:
      // Store fsid if it doesn't exist
      val = std::to_string(e->location);

      // Create table if it doesn't exist and add the fsid
      try {
	table_id = client->getTableId(sSetFsIds.c_str());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	table_id = client->createTable(sSetFsIds.c_str());
      }

      try {
	RAMCloud::Buffer bval;
	client->read(table_id, static_cast<const void*>(val.c_str()),
		     val.length(), &bval);
      }
      catch (RAMCloud::ClientException& e) {
	client->write(table_id, static_cast<const void*>(val.c_str()),
		      val.length(), nullptr);
      }

      // Add file id to the fsid table holding all the files in the current
      // filesystem
      key = sFilesPrefix + val;
      val = std::to_string(e->file->getId());

      // Get table_id for the curent fs
      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::ClientException& e) {
	table_id = client->createTable(key.c_str());
      }

      try {
	client->write(table_id, static_cast<const void*>(val.c_str()),
		      val.length(), nullptr);
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }

      // Remove file id from the no replicas table
      try {
	table_id = client->getTableId(sNoReplicaPrefix.c_str());
	client->remove(table_id, static_cast<const void*>(val.c_str()),
		      val.length());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	table_id = client->createTable(sNoReplicaPrefix.c_str());
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }

      break;

    // Replace location
    case IFileMDChangeListener::LocationReplaced:
      key = sFilesPrefix + std::to_string(e->oldLocation);

      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::ClientException& e) {
	return; // inconsistency, we should probably crash here
      }

      val = std::to_string(e->file->getId());
      try {
	client->remove(table_id, static_cast<const void*>(val.c_str()),
		       val.length());
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }

      key = sFilesPrefix + std::to_string(e->location);

      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::ClientException& e) {
	table_id = client->createTable(key.c_str());
      }

      try {
	client->write(table_id, static_cast<const void*>(val.c_str()),
		      val.length(), nullptr);
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }

      break;

    // Remove location
    case IFileMDChangeListener::LocationRemoved:
      key = sUnlinkedPrefix + std::to_string(e->location);

      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	return; // incostency, we should probably crash here...
      }

      val = std::to_string(e->file->getId());
      try {
	client->remove(table_id, static_cast<const char*>(val.c_str()),
		       val.length());
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }

      if( !e->file->getNumUnlinkedLocation() && !e->file->getNumLocation() )
      {
	try {
	  table_id = client->getTableId(sNoReplicaPrefix.c_str());
	}
	catch (RAMCloud::TableDoesntExistException& e) {
	  table_id = client->createTable(sNoReplicaPrefix.c_str());
	}

	try {
	  client->write(table_id, static_cast<const void*>(val.c_str()),
			val.length(), nullptr);
	}
	catch (RAMCloud::ClientException& e)  {
	  // TODO: take some action in case it fails
	}
      }

      // Cleanup fsid if it doesn't hold any files anymore
      key = sFilesPrefix + std::to_string(e->location);

      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	table_exists = false;
      }

      if (!table_exists || isEmptyTable(table_id))
      {
	table_exists = true;

	try {
	  key = sUnlinkedPrefix + std::to_string(e->location);
	  table_id = client->getTableId(key.c_str());
	}
	catch (RAMCloud::TableDoesntExistException& e) {
	  table_exists = false;
	}

	if (!table_exists || isEmptyTable(table_id))
	{
	  // Fs does not hold any file replicas or unlinked ones, remove it
	  key = std::to_string(e->location);
	  try {
	    table_id = client->getTableId(sSetFsIds.c_str());
	    client->remove(table_id, static_cast<const void*>(key.c_str()),
			   key.length());
	  }
	  catch (RAMCloud::TableDoesntExistException& e) {
	    // Do nothing
	  }
	  catch (RAMCloud::ClientException& e) {
	    // TODO: take some action in case it fails
	  }
	}
      }

      break;

      // Unlink location
    case IFileMDChangeListener::LocationUnlinked:
      key = sFilesPrefix + std::to_string(e->location);

      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	return; // incostency, we should probably crash here...
      }

      val = std::to_string(e->file->getId());

      try {
	client->remove(table_id, static_cast<const void*>(val.c_str()),
		       val.length());
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }

      // Add file id to the list of unlinked ones
      key = sUnlinkedPrefix + std::to_string(e->location);

      try {
	table_id = client->getTableId(key.c_str());
      }
      catch (RAMCloud::TableDoesntExistException& e) {
	table_id = client->createTable(key.c_str());
      }

      try {
	client->write(table_id, static_cast<const void*>(val.c_str()),
		      val.length(), nullptr);
      }
      catch (RAMCloud::ClientException& e) {
	// TODO: take some action in case it fails
      }
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
  RAMCloud::RamCloud* client = getRamCloudClient();
  IFileMD::LocationVector::const_iterator it;
  IFileMD::LocationVector loc_vect = obj->getLocations();
  std::string key, val;
  uint64_t table_id;

  for( it = loc_vect.begin(); it != loc_vect.end(); ++it )
  {
    // Store fsid if it doesn't exist
    key = sSetFsIds;
    val = std::to_string(*it);

    // Get filesystem table holding the file ids
    try {
      table_id = client->getTableId(key.c_str());
    }
    catch (RAMCloud::TableDoesntExistException& e) {
      table_id = client->createTable(key.c_str());
    }

    try {
      client->write(table_id, static_cast<const void*>(val.c_str()), val.length(),
		    nullptr);
    }
    catch (RAMCloud::ClientException& e) {
      // TODO: take some action in case it fails
    }

    // Add file to corresponding fs file set
    key = sFilesPrefix + val;

    try {
      table_id = client->getTableId(key.c_str());
    }
    catch (RAMCloud::TableDoesntExistException& e) {
      table_id = client->createTable(key.c_str());
    }

    try {
      key = std::to_string(obj->getId());
      client->write(table_id, static_cast<const void*>(key.c_str()),
		    key.length(), nullptr);
    }
    catch (RAMCloud::ClientException& e) {
      // TODO: take some action in case it fails
    }
  }

  IFileMD::LocationVector unlink_vect = obj->getUnlinkedLocations();

  for( it = unlink_vect.begin(); it != unlink_vect.end(); ++it )
  {
    key = sUnlinkedPrefix + std::to_string(*it);

    try {
      table_id = client->getTableId(key.c_str());
    }
    catch (RAMCloud::TableDoesntExistException& e) {
      table_id = client->createTable(key.c_str());
    }

    try {
      key = std::to_string(obj->getId());
      client->write(table_id, static_cast<const void*>(key.c_str()),
		    key.length(), nullptr);
    }
    catch (RAMCloud::ClientException& e) {
      // TODO: take some action in case it fails
    }
  }

  if( obj->getNumLocation() == 0 && obj->getNumUnlinkedLocation() == 0 )
  {
    try {
      table_id = client->getTableId(sNoReplicaPrefix.c_str());
    }
    catch (RAMCloud::TableDoesntExistException& e) {
      table_id = client->createTable(sNoReplicaPrefix.c_str());
    }

    try {
      key = std::to_string(obj->getId());
      client->write(table_id, static_cast<const void*>(key.c_str()),
		    key.length(), nullptr);
    }
    catch (RAMCloud::ClientException& e) {
      // TODO: take some action in case it fails
    }
  }
}

//------------------------------------------------------------------------------
// Get set of files on filesystem
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getFileList(IFileMD::location_t location)
{
  uint64_t table_id;
  RAMCloud::RamCloud* client = getRamCloudClient();
  std::string key = sFilesPrefix + std::to_string(location);

  try {
    table_id = client->getTableId(key.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    MDException e( ENOENT );
    e.getMessage() << "Location " << key  << " does not exist" << std::endl;
    throw( e );
  }

  IFsView::FileList set_files;

  try {
    uint32_t size = 0;
    const void* buffer = 0;
    RAMCloud::TableEnumerator iter(*client, table_id, true);

    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      RAMCloud::Object object(buffer, size);
      set_files.emplace(atol(static_cast<const char*>(object.getKey())));
    }
  }
  catch (RAMCloud::ClientException& e) {
    // TODO: take some action in case it fails
  }

  return set_files;
}

//------------------------------------------------------------------------------
// Get set of unlinked files
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  uint64_t table_id;
  RAMCloud::RamCloud* client = getRamCloudClient();
  IFsView::FileList set_unlinked;
  std::string key = sUnlinkedPrefix + std::to_string(location);

  try {
    table_id = client->getTableId(key.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return set_unlinked;
  }

  try {
    uint32_t size = 0;
    const void* buffer = 0;
    RAMCloud::TableEnumerator iter(*client, table_id, true);

    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      RAMCloud::Object object(buffer, size);
      set_unlinked.emplace(atol(static_cast<const char*>(object.getKey())));
    }
  }
  catch (RAMCloud::ClientException& e) {
    // TODO: take some action in case it fails
  }

  return set_unlinked;
}

//------------------------------------------------------------------------------
// Get set of files without replicas
//------------------------------------------------------------------------------
IFsView::FileList FileSystemView::getNoReplicasFileList()
{
  uint64_t table_id;
  IFsView::FileList set_noreplicas;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_id = client->getTableId(sNoReplicaPrefix.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return set_noreplicas;
  }

  try {
    uint32_t size = 0;
    const void* buffer = 0;
    RAMCloud::TableEnumerator iter(*client, table_id, true);

    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      RAMCloud::Object object(buffer, size);
      set_noreplicas.emplace(atol(static_cast<const char*>(object.getKey())));
    }
  }
  catch (RAMCloud::ClientException& e) {
    // TODO: take some action in case it fails
  }

  return set_noreplicas;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  RAMCloud::RamCloud* client = getRamCloudClient();
  std::string key = sUnlinkedPrefix + std::to_string(location);

  try {
    client->dropTable(key.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return true;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get number of file systems
//------------------------------------------------------------------------------
size_t FileSystemView::getNumFileSystems()
{
  ssize_t num_fs = 0;
  uint32_t table_id;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    uint32_t size = 0;
    const void* buffer = 0;
    table_id = client->getTableId(sSetFsIds.c_str());
    RAMCloud::TableEnumerator iter(*client, table_id, true);

    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      num_fs++;
    }
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return num_fs;
  }
  catch (RAMCloud::ClientException& e) {
    // TODO: take some action in case it fails
  }
  return num_fs;
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

//------------------------------------------------------------------------------
// Initialize for testing purposes
//------------------------------------------------------------------------------
void
FileSystemView::initialize(const std::map<std::string, std::string>& config)
{
}

EOSNSNAMESPACE_END
