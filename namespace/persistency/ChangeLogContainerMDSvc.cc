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
// desc:   Change log based ContainerMD service
//------------------------------------------------------------------------------

#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogConstants.hh"

#include <iostream>
#include <memory>

namespace eos
{
  //----------------------------------------------------------------------------
  // Initizlize the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::initialize() throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Rescan the changelog
    //--------------------------------------------------------------------------
    pChangeLog->open( pChangeLogPath,
                      ChangeLogFile::Create | ChangeLogFile::Append,
                      CONTAINER_LOG_MAGIC );
    ContainerMDScanner scanner( pIdMap );
    pChangeLog->scanAllRecords( &scanner );
    pFirstFreeId = scanner.getLargestId()+1;

    //--------------------------------------------------------------------------
    // Recreate the container structure
    //--------------------------------------------------------------------------
    IdMap::iterator it;
    ContainerList   orphans;
    ContainerList   nameConflicts;
    for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
    {
      if( it->second.ptr )
        continue;
      recreateContainer( it, orphans, nameConflicts );
    }

    //--------------------------------------------------------------------------
    // Deal with broken containers
    //--------------------------------------------------------------------------
    attachBroken( getLostFoundContainer( "orphans" ), orphans );
    attachBroken( getLostFoundContainer( "name_conflicts" ), nameConflicts );
  }

  //----------------------------------------------------------------------------
  // Configure the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::configure(
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

  //----------------------------------------------------------------------------
  // Finalize the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::finalize() throw( MDException )
  {
    pChangeLog->close();
    IdMap::iterator it;
    for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
      delete it->second.ptr;
    pIdMap.clear();
  }

  //----------------------------------------------------------------------------
  // Get the container metadata information
  //----------------------------------------------------------------------------
  ContainerMD *ChangeLogContainerMDSvc::getContainerMD( ContainerMD::id_t id )
    throw( MDException )
  {
    IdMap::iterator it = pIdMap.find( id );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container #" << id << " not found";
      throw e;
    }
    return it->second.ptr;
  }

  //----------------------------------------------------------------------------
  // Create a new container metadata object
  //----------------------------------------------------------------------------
  ContainerMD *ChangeLogContainerMDSvc::createContainer() throw( MDException )
  {
    ContainerMD *cont = new ContainerMD( pFirstFreeId++ );
    pIdMap.insert( std::make_pair( cont->getId(), DataInfo( 0, cont ) ) );
    return cont;
  }

  //----------------------------------------------------------------------------
  // Update the contaienr metadata in the backing store
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::updateStore( ContainerMD *obj )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Find the object in the map
    //--------------------------------------------------------------------------
    IdMap::iterator it = pIdMap.find( obj->getId() );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container #" << obj->getId() << " not found. ";
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
    notifyListeners( obj, IContainerMDChangeListener::Updated );
  }

  //----------------------------------------------------------------------------
  // Remove object from the store
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::removeContainer( ContainerMD *obj )
    throw( MDException )
  {
    removeContainer( obj->getId() );
  }

  //----------------------------------------------------------------------------
  // Remove object from the store
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::removeContainer( ContainerMD::id_t containerId )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Find the object in the map
    //--------------------------------------------------------------------------
    IdMap::iterator it = pIdMap.find( containerId );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container #" << containerId << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Store the file in the changelog and notify the listener
    //--------------------------------------------------------------------------
    eos::Buffer buffer;
    buffer.putData( &containerId, sizeof( ContainerMD::id_t ) );
    pChangeLog->storeRecord( eos::DELETE_RECORD_MAGIC, buffer );
    notifyListeners( it->second.ptr, IContainerMDChangeListener::Deleted );
    delete it->second.ptr;
    pIdMap.erase( it );
  }

  //----------------------------------------------------------------------------
  // Add change listener
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::addChangeListener(
                                     IContainerMDChangeListener *listener )
  {
    pListeners.push_back( listener );
  }

  //----------------------------------------------------------------------------
  // Recreate the container
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::recreateContainer( IdMap::iterator &it,
                                                   ContainerList   &orphans,
                                                   ContainerList   &nameConflicts )
  {
    Buffer buffer;
    pChangeLog->readRecord( it->second.logOffset, buffer );
    ContainerMD *container = new ContainerMD( 0 );
    container->deserialize( buffer );
    it->second.ptr = container;

    //--------------------------------------------------------------------------
    // For non-root containers recreate the parent
    //--------------------------------------------------------------------------
    if( container->getId() != container->getParentId() )
    {
      IdMap::iterator parentIt = pIdMap.find( container->getParentId() );

      if( parentIt == pIdMap.end() )
      {
        orphans.push_back( container );
        return;
      }

      if( !(parentIt->second.ptr) )
        recreateContainer( parentIt, orphans, nameConflicts );

      ContainerMD *parent = parentIt->second.ptr;
      if( !parent->findContainer( container->getName() ) )
        parent->addContainer( container );
      else
        nameConflicts.push_back( container );
    }
  }

  //------------------------------------------------------------------------
  // Create container in parent
  //------------------------------------------------------------------------
  ContainerMD *ChangeLogContainerMDSvc::createInParent(
                const std::string &name,
                ContainerMD       *parent )
                throw( MDException )
  {
      ContainerMD *container = createContainer();
      container->setName( name );
      parent->addContainer( container );
      updateStore( container );
      return container;
  }

  //----------------------------------------------------------------------------
  // Get the lost+found container, create if necessary
  //----------------------------------------------------------------------------
  ContainerMD *ChangeLogContainerMDSvc::getLostFound() throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Get root
    //--------------------------------------------------------------------------
    ContainerMD *root = 0;
    try
    {
      root = getContainerMD( 1 );
    }
    catch( MDException &e )
    {
      root = createContainer();
      root->setParentId( root->getId() );
      updateStore( root );
    }

    //--------------------------------------------------------------------------
    // Get or create lost+found if necessary
    //--------------------------------------------------------------------------
    ContainerMD *lostFound = root->findContainer( "lost+found" );
    if( lostFound )
      return lostFound;
    return createInParent( "lost+found", root );
  }

  //----------------------------------------------------------------------------
  // Get the orphans container
  //----------------------------------------------------------------------------
  ContainerMD *ChangeLogContainerMDSvc::getLostFoundContainer(
                const std::string &name ) throw( MDException )
  {
    ContainerMD *lostFound = getLostFound();

    if( name.empty() )
      return lostFound;

    ContainerMD *cont = lostFound->findContainer( name );
    if( cont )
      return cont;
    return createInParent( name, lostFound );
  }

  //----------------------------------------------------------------------------
  // Attach broken containers to lost+found
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::attachBroken( ContainerMD   *parent,
                                              ContainerList &broken )
  {
    ContainerList::iterator it;
    for( it = broken.begin(); it != broken.end(); ++it )
    {
      std::ostringstream s1, s2;
      s1 << (*it)->getParentId();
      ContainerMD *cont = parent->findContainer( s1.str() );
      if( !cont )
        cont = createInParent( s1.str(), parent );

      s2 << (*it)->getName() << "." << (*it)->getId();
      (*it)->setName( s2.str() );
      cont->addContainer( *it );
    }
  }

  //----------------------------------------------------------------------------
  // Scan the changelog and put the appropriate data in the lookup table
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::ContainerMDScanner::processRecord(
                           uint64_t offset, char type, const Buffer &buffer )
  {
    //--------------------------------------------------------------------------
    // Update
    //--------------------------------------------------------------------------
    if( type == UPDATE_RECORD_MAGIC )
    {
      ContainerMD::id_t id;
      buffer.grabData( 0, &id, sizeof( ContainerMD::id_t ) );
      pIdMap[id] = DataInfo( offset, 0 );
      if( pLargestId < id ) pLargestId = id;
    }

    //--------------------------------------------------------------------------
    // Deletion
    //--------------------------------------------------------------------------
    else if( type == DELETE_RECORD_MAGIC )
    {
      ContainerMD::id_t id;
      buffer.grabData( 0, &id, sizeof( ContainerMD::id_t ) );
      IdMap::iterator it = pIdMap.find( id );
      if( it != pIdMap.end() )
        pIdMap.erase( it );
      if( pLargestId < id ) pLargestId = id;
    }
  }
}
