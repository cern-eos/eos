//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Change log based ContainerMD service
//------------------------------------------------------------------------------

#include "ChangeLogContainerMDSvc.hh"
#include "ChangeLogConstants.hh"

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
    pChangeLog->open( pChangeLogPath );
    ContainerMDScanner scanner( pIdMap );
    pChangeLog->scanAllRecords( &scanner );
    pFirstFreeId = scanner.getLargestId()+1;

    //--------------------------------------------------------------------------
    // Recreate the containers
    //--------------------------------------------------------------------------
    IdMap::iterator it;
    for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
    {
      if( it->second.ptr )
        continue;
      recreateContainer( it );
    }
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
      MDException e;
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
      MDException e;
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
      MDException e;
      e.getMessage() << "Container #" << obj->getId() << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Store the file in the changelog and notify the listener
    //--------------------------------------------------------------------------
    eos::Buffer buffer;
    obj->serialize( buffer );
    it->second.logOffset = pChangeLog->storeRecord( eos::UPDATE_RECORD, buffer );
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
      MDException e;
      e.getMessage() << "Container #" << containerId << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Store the file in the changelog and notify the listener
    //--------------------------------------------------------------------------
    eos::Buffer buffer;
    buffer.putData( &containerId, sizeof( ContainerMD::id_t ) );
    pChangeLog->storeRecord( eos::DELETE_RECORD, buffer );
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
  void ChangeLogContainerMDSvc::recreateContainer( IdMap::iterator &it )
  {
    Buffer buffer;
    pChangeLog->readRecord( it->second.logOffset, buffer );
    ContainerMD *container = new ContainerMD( 0 );
    std::auto_ptr<ContainerMD> ptr( container );
    container->deserialize( buffer );

    //--------------------------------------------------------------------------
    // For non-root containers recreate the parent
    //--------------------------------------------------------------------------
    if( container->getId() != container->getParentId() )
    {
      IdMap::iterator parentIt = pIdMap.find( container->getParentId() );

      if( parentIt == pIdMap.end() )
      {
        MDException e;
        e.getMessage() << "Parent of the container #" << container->getId();
        e.getMessage() << " does not exist (#" << container->getParentId();
        e.getMessage() << ")";
        throw e;
      }

      if( !(parentIt->second.ptr) )
        recreateContainer( parentIt );

      parentIt->second.ptr->addContainer( container );
    }
    it->second.ptr = container;
    ptr.release();
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
    if( type == UPDATE_RECORD )
    {
      ContainerMD::id_t id;
      buffer.grabData( 0, &id, sizeof( ContainerMD::id_t ) );
      pIdMap[id] = DataInfo( offset, 0 );
      if( pLargestId < id ) pLargestId = id;
    }

    //--------------------------------------------------------------------------
    // Deletion
    //--------------------------------------------------------------------------
    else if( type == DELETE_RECORD )
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
