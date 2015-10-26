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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   The filesystem view over the stored files
//------------------------------------------------------------------------------

#include "namespace/ns_on_redis/accounting/FileSystemView.hh"
#include <iostream>

static const std::string REDIS_HOST = "localhost";
static const int REDIS_PORT = 6380;

namespace eos
{

  const std::string FileSystemView::sFilesPrefix = "fsview_files:";
  const std::string FileSystemView::sUnlinkedPrefix = "fsview_unlinked:";
  const std::string FileSystemView::sNoReplicaPrefix = "fsview_noreplicas";

  //----------------------------------------------------------------------------
  // Resize
  //----------------------------------------------------------------------------
  template<class Cont>
  static void resize( Cont &d, size_t size )
  {
    size_t oldSize = d.size();
    if( size <= oldSize )
      return;
    d.resize(size);
    for( size_t i = oldSize; i < size; ++i )
    {
      d[i].set_deleted_key( 0 );
      d[i].set_empty_key(0xffffffffffffffffll);
    }
  }

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  FileSystemView::FileSystemView()
  {
    pNoReplicas.set_deleted_key( 0 );
    pNoReplicas.set_empty_key(0xffffffffffffffffll);

    // Connect to the server
    pRedox.connect(REDIS_HOST, REDIS_PORT);  // TODO: catch exceptions
  }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  FileSystemView:: ~FileSystemView()
  {
    pRedox.disconnect();
  }

  //----------------------------------------------------------------------------
  // Notify the me about the changes in the main view
  //----------------------------------------------------------------------------
  void FileSystemView::fileMDChanged( IFileMDChangeListener::Event *e )
  {
    std::string key, val;

    switch( e->action )
    {
      //------------------------------------------------------------------------
      // New file has been created
      //------------------------------------------------------------------------
      case IFileMDChangeListener::Created:
	pNoReplicas.insert( e->file->getId() );
	// Update Redis
	pRedox.sadd(sNoReplicaPrefix, std::to_string(e->file->getId()));
	break;

      //------------------------------------------------------------------------
      // File has been deleted
      //------------------------------------------------------------------------
      case IFileMDChangeListener::Deleted:
	pNoReplicas.erase( e->fileId );
	// Update Redis
	pRedox.srem(sNoReplicaPrefix, std::to_string(e->fileId));
	break;

      //------------------------------------------------------------------------
      // Add location
      //------------------------------------------------------------------------
      case IFileMDChangeListener::LocationAdded:
	resize( pFiles, e->location+1 );
	resize( pUnlinkedFiles, e->location+1 );
	pFiles[e->location].insert( e->file->getId() );
	pNoReplicas.erase( e->file->getId() );
	// Update Redis
	key = sFilesPrefix + std::to_string(e->location);
	val = std::to_string(e->file->getId());
	pRedox.sadd(key, val);
	pRedox.sadd(sNoReplicaPrefix, val);
	break;

      //------------------------------------------------------------------------
      // Replace location
      //------------------------------------------------------------------------
      case IFileMDChangeListener::LocationReplaced:
	if( e->oldLocation >= pFiles.size() )
	  return; // incostency, we should probably crash here...

	resize( pFiles, e->location+1 );
	resize( pUnlinkedFiles, e->location+1 );
	pFiles[e->oldLocation].erase( e->file->getId() );
	pFiles[e->location].insert( e->file->getId() );
	// Update Redis
	key = sFilesPrefix + std::to_string(e->oldLocation);
	val = std::to_string(e->file->getId());
	pRedox.srem(key, val);
	key = sFilesPrefix + std::to_string(e->location);
	pRedox.sadd(key, val);
	break;

      //------------------------------------------------------------------------
      // Remove location
      //------------------------------------------------------------------------
      case IFileMDChangeListener::LocationRemoved:
	if( e->location >= pUnlinkedFiles.size() )
	  return; // incostency, we should probably crash here...

	pUnlinkedFiles[e->location].erase( e->file->getId() );
	// Update Redis
	key = sUnlinkedPrefix + std::to_string(e->location);
	val = std::to_string(e->file->getId());
	pRedox.srem(key, val);

	if( !e->file->getNumUnlinkedLocation() && !e->file->getNumLocation() )
	{
	  pNoReplicas.insert( e->file->getId() );
	  // Update Redis
	  pRedox.srem(sNoReplicaPrefix, val);
	}
	break;

      //------------------------------------------------------------------------
      // Unlink location
      //------------------------------------------------------------------------
      case IFileMDChangeListener::LocationUnlinked:
	if( e->location >= pFiles.size() )
	  return; // incostency, we should probably crash here...
	pFiles[e->location].erase( e->file->getId() );
	pUnlinkedFiles[e->location].insert( e->file->getId() );
	// Update Redis
	key = sFilesPrefix + std::to_string(e->location);
	val = std::to_string(e->file->getId());
	pRedox.srem(key, val);
	key = sUnlinkedPrefix + std::to_string(e->location);
	pRedox.sadd(key, val);
	break;

      default:
	break;
    }
  }

  //----------------------------------------------------------------------------
  // Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  void FileSystemView::fileMDRead( IFileMD *obj )
  {
    IFileMD::LocationVector::const_iterator it;
    IFileMD::LocationVector loc_vect = obj->getLocations();
    std::string key;

    for( it = loc_vect.begin(); it != loc_vect.end(); ++it )
    {
      resize( pFiles, *it+1 );
      resize( pUnlinkedFiles, *it+1 );
      pFiles[*it].insert( obj->getId() );
      // Update Redis
      key = sFilesPrefix + std::to_string(*it);
      pRedox.sadd(key, std::to_string(obj->getId()));
    }

    IFileMD::LocationVector unlink_vect = obj->getUnlinkedLocations();

    for( it = unlink_vect.begin(); it != unlink_vect.end(); ++it )
    {
      resize( pFiles, *it+1 );
      resize( pUnlinkedFiles, *it+1 );
      pUnlinkedFiles[*it].insert( obj->getId() );
      // Update Redis
      key = sUnlinkedPrefix + std::to_string(*it);
      pRedox.sadd(key, std::to_string(obj->getId()));
    }

    if( obj->getNumLocation() == 0 && obj->getNumUnlinkedLocation() == 0 )
    {
      pNoReplicas.insert( obj->getId() );
      // Update Redis
      pRedox.sadd(sNoReplicaPrefix, std::to_string(obj->getId()));
    }
  }

  //----------------------------------------------------------------------------
  // Return reference to a list of files
  //----------------------------------------------------------------------------
  const FileSystemView::FileList &FileSystemView::getFileList(
      IFileMD::location_t location )
  {
    if( pFiles.size() <= location )
    {
      MDException e( ENOENT );
      e.getMessage() << "Location does not exist" << std::endl;
      throw( e );
    }
    return pFiles[location];
  }

  //----------------------------------------------------------------------------
  // Return reference to a list of unlinked files
  //----------------------------------------------------------------------------
  const FileSystemView::FileList &FileSystemView::getUnlinkedFileList(
					      IFileMD::location_t location )
  {
    if( pUnlinkedFiles.size() <= location )
    {
      MDException e( ENOENT );
      e.getMessage() << "Location does not exist" << std::endl;
      throw( e );
    }

    return pUnlinkedFiles[location];
  }

  //----------------------------------------------------------------------------
  // Initialize
  //----------------------------------------------------------------------------
  void FileSystemView::initialize()
  {
  }

  //----------------------------------------------------------------------------
  // Finalize
  //----------------------------------------------------------------------------
  void FileSystemView::finalize()
  {
    pFiles.clear();
    pUnlinkedFiles.clear();
    pNoReplicas.clear();
  }
}
