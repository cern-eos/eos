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
// desc:   Change log based FileMD service
//------------------------------------------------------------------------------

#include "ChangeLogFileMDSvc.hh"
#include "ChangeLogConstants.hh"

#include <iostream>

namespace eos
{
  //------------------------------------------------------------------------
  // Initizlize the file service
  //------------------------------------------------------------------------
  void ChangeLogFileMDSvc::initialize() throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Rescan the changelog
    //--------------------------------------------------------------------------
    pChangeLog->open( pChangeLogPath,
                      ChangeLogFile::Create | ChangeLogFile::Append,
                      FILE_LOG_MAGIC );
    FileMDScanner scanner( pIdMap );
    pChangeLog->scanAllRecords( &scanner );
    pFirstFreeId = scanner.getLargestId()+1;

    //--------------------------------------------------------------------------
    // Recreate the files
    //--------------------------------------------------------------------------
    IdMap::iterator it;
    for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
    {
      Buffer buffer;
      pChangeLog->readRecord( it->second.logOffset, buffer );
      FileMD *file = new FileMD( 0, this );
      file->deserialize( buffer );
      it->second.ptr = file;
      ListenerList::iterator it;
      for( it = pListeners.begin(); it != pListeners.end(); ++it )
        (*it)->fileMDRead( file );
    }
  }

  //------------------------------------------------------------------------
  // Configure the file service
  //------------------------------------------------------------------------
  void ChangeLogFileMDSvc::configure(
                             std::map<std::string, std::string> &config )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Configure the changelog
    //--------------------------------------------------------------------------
    std::map<std::string, std::string>::iterator it;
    it = config.find( "changelog_path" );
    if( it == config.end() )
    {
      MDException e( EINVAL );
      e.getMessage() << "changelog_path not specified" ;
      throw e;
    }
    pChangeLogPath = it->second;
  }

  //------------------------------------------------------------------------
  // Finalize the file service
  //------------------------------------------------------------------------
  void ChangeLogFileMDSvc::finalize() throw( MDException )
  {
    pChangeLog->close();
    IdMap::iterator it;
    for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
      delete it->second.ptr;
    pIdMap.clear();
  }

  //----------------------------------------------------------------------------
  // Get the file metadata information for the given file ID
  //----------------------------------------------------------------------------
  FileMD *ChangeLogFileMDSvc::getFileMD( FileMD::id_t id ) throw( MDException )
  {
    IdMap::iterator it = pIdMap.find( id );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "File #" << id << " not found";
      throw e;
    }
    return it->second.ptr;
  }

  //----------------------------------------------------------------------------
  // Create new file metadata object
  //----------------------------------------------------------------------------
  FileMD *ChangeLogFileMDSvc::createFile() throw( MDException )
  {
    FileMD *file = new FileMD( pFirstFreeId++, this );
    pIdMap.insert( std::make_pair( file->getId(), DataInfo( 0, file ) ) );
    return file;
  }

  //----------------------------------------------------------------------------
  // Update the file metadata
  //----------------------------------------------------------------------------
  void ChangeLogFileMDSvc::updateStore( FileMD *obj ) throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Find the object in the map
    //--------------------------------------------------------------------------
    IdMap::iterator it = pIdMap.find( obj->getId() );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "File #" << obj->getId() << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Store the file in the changelog and notify the listener
    //--------------------------------------------------------------------------
    eos::Buffer buffer;
    obj->serialize( buffer );
    it->second.logOffset = pChangeLog->storeRecord( eos::UPDATE_RECORD_MAGIC,
                                                    buffer );
    IFileMDChangeListener::Event e( obj, IFileMDChangeListener::Updated );
    notifyListeners( &e );
  }

  //----------------------------------------------------------------------------
  // Remove object from the store
  //----------------------------------------------------------------------------
  void ChangeLogFileMDSvc::removeFile( FileMD *obj ) throw( MDException )
  {
    removeFile( obj->getId() );
  }

  //----------------------------------------------------------------------------
  // Remove object from the store
  //----------------------------------------------------------------------------
  void ChangeLogFileMDSvc::removeFile( FileMD::id_t fileId )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Find the object in the map
    //--------------------------------------------------------------------------
    IdMap::iterator it = pIdMap.find( fileId );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "File #" << fileId << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Store the file in the changelog and notify the listener
    //--------------------------------------------------------------------------
    eos::Buffer buffer;
    buffer.putData( &fileId, sizeof( FileMD::id_t ) );
    pChangeLog->storeRecord( eos::DELETE_RECORD_MAGIC, buffer );
    IFileMDChangeListener::Event e( fileId, IFileMDChangeListener::Deleted );
    notifyListeners( &e );
    delete it->second.ptr;
    pIdMap.erase( it );
  }

  //----------------------------------------------------------------------------
  // Add file listener
  //----------------------------------------------------------------------------
  void ChangeLogFileMDSvc::addChangeListener( IFileMDChangeListener *listener )
  {
    pListeners.push_back( listener );
  }

  //----------------------------------------------------------------------------
  // Visit all the files
  //----------------------------------------------------------------------------
  void ChangeLogFileMDSvc::visit( IFileVisitor *visitor )
  {
    IdMap::iterator it;
    for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
      visitor->visitFile( it->second.ptr );
  }

  //----------------------------------------------------------------------------
  // Scan the changelog and put the appropriate data in the lookup table
  //----------------------------------------------------------------------------
  void ChangeLogFileMDSvc::FileMDScanner::processRecord(
                                                        uint64_t offset, char type, const Buffer &buffer )
  {
    //--------------------------------------------------------------------------
    // Update
    //--------------------------------------------------------------------------
    if( type == UPDATE_RECORD_MAGIC )
    {
      FileMD::id_t id;
      buffer.grabData( 0, &id, sizeof( FileMD::id_t ) );
      pIdMap[id] = DataInfo( offset, 0 );
      if( pLargestId < id ) pLargestId = id;
    }

    //--------------------------------------------------------------------------
    // Deletion
    //--------------------------------------------------------------------------
    else if( type == DELETE_RECORD_MAGIC )
    {
      FileMD::id_t id;
      buffer.grabData( 0, &id, sizeof( FileMD::id_t ) );
      IdMap::iterator it = pIdMap.find( id );
      if( it != pIdMap.end() )
        pIdMap.erase( it );
      if( pLargestId < id ) pLargestId = id;
    }
  }

}

