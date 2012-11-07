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
#include "namespace/utils/Locking.hh"

#include <set>
#include <memory>

//------------------------------------------------------------------------------
// Follower
//------------------------------------------------------------------------------
namespace eos
{
  class ContainerMDFollower: public eos::ILogRecordScanner
  {
    public:
      ContainerMDFollower( eos::ChangeLogContainerMDSvc *contSvc ):
        pContSvc( contSvc ) {}

      //------------------------------------------------------------------------
      // Unpack new data and put it in the queue
      //------------------------------------------------------------------------
      virtual bool processRecord( uint64_t offset, char type,
                                  const eos::Buffer &buffer )
      {
        //----------------------------------------------------------------------
        // Update
        //----------------------------------------------------------------------
        if( type == UPDATE_RECORD_MAGIC )
        {
          ContainerMD *container = new ContainerMD( 0 );
          container->deserialize( (Buffer&)buffer );
          ContMap::iterator it = pUpdated.find( container->getId() );
          if( it != pUpdated.end() )
          {
            delete it->second;
            it->second = container;
          }
          else
            pUpdated[container->getId()] = container;

          pDeleted.erase( container->getId() );
        }

        //----------------------------------------------------------------------
        // Deletion
        //----------------------------------------------------------------------
        else if( type == DELETE_RECORD_MAGIC )
        {
          ContainerMD::id_t id;
          buffer.grabData( 0, &id, sizeof( ContainerMD::id_t ) );
          ContMap::iterator it = pUpdated.find( id );
          if( it != pUpdated.end() )
          {
            delete it->second;
            pUpdated.erase( it );
          }
          pDeleted.insert( id );
        }
        return true;
      }

      //------------------------------------------------------------------------
      // Try to commit the data in the queue to the service
      //------------------------------------------------------------------------
      void commit()
      {
        pContSvc->getSlaveLock()->writeLock();
        ChangeLogContainerMDSvc::IdMap *idMap = &pContSvc->pIdMap;

        //----------------------------------------------------------------------
        // Handle deletions
        //----------------------------------------------------------------------
        std::set<eos::ContainerMD::id_t>::iterator itD;
        std::list<ContainerMD::id_t> processed;
        for( itD = pDeleted.begin(); itD != pDeleted.end(); ++itD )
        {
          ChangeLogContainerMDSvc::IdMap::iterator it;
          it = idMap->find( *itD );
          if( it == idMap->end() )
          {
            processed.push_back( *itD );
            continue;
          }

          if( it->second.ptr->getNumContainers() ||
              it->second.ptr->getNumFiles() )
            continue;

          ChangeLogContainerMDSvc::IdMap::iterator itP;
          itP = idMap->find( it->second.ptr->getParentId() );
          if( itP != idMap->end() )
          {
            // make sure it's the same pointer - cover name conflicts
            ContainerMD *cont = itP->second.ptr->findContainer( it->second.ptr->getName() );
            if( cont == it->second.ptr )
              itP->second.ptr->removeContainer( it->second.ptr->getName() );
          }
          delete it->second.ptr;
          idMap->erase( it );
          processed.push_back( *itD );
        }

        std::list<ContainerMD::id_t>::iterator itPro;
        for( itPro = processed.begin(); itPro != processed.end(); ++itPro )
          pDeleted.erase( *itPro );

        //----------------------------------------------------------------------
        // Handle updates
        //----------------------------------------------------------------------
        ContMap::iterator itU;
        for( itU = pUpdated.begin(); itU != pUpdated.end(); ++itU )
        {
          ChangeLogContainerMDSvc::IdMap::iterator it;
          ChangeLogContainerMDSvc::IdMap::iterator itP;
          ContainerMD *currentCont = itU->second;
          it = idMap->find( currentCont->getId() );
          if( it == idMap->end() )
          {
            (*idMap)[currentCont->getId()] =
              ChangeLogContainerMDSvc::DataInfo( 0, currentCont );
            itP = idMap->find( currentCont->getParentId() );
            if( itP != idMap->end() )
              itP->second.ptr->addContainer( currentCont );
          }
          else
          {
            (*it->second.ptr) = *currentCont;
            delete currentCont;
          }
        }
        pUpdated.clear();
        pContSvc->getSlaveLock()->unLock();
      }

    private:
      typedef std::map<eos::ContainerMD::id_t, eos::ContainerMD*> ContMap;
      ContMap                           pUpdated;
      std::set<eos::ContainerMD::id_t>  pDeleted;
      eos::ChangeLogContainerMDSvc     *pContSvc;
  };
}

extern "C"
{
  //----------------------------------------------------------------------------
  // Follow the change log
  //----------------------------------------------------------------------------
  static void *followerThread( void *data )
  {
    eos::ChangeLogContainerMDSvc *contSvc = reinterpret_cast<eos::ChangeLogContainerMDSvc*>( data );
    uint64_t                      offset  = contSvc->getFollowOffset();
    eos::ChangeLogFile           *file    = contSvc->getChangeLog();
    uint32_t                      pollInt = contSvc->getFollowPollInterval();

    eos::ContainerMDFollower f( contSvc );
    pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, 0 );
    while( 1 )
    {
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, 0 );
      offset = file->follow( &f, offset );
      f.commit();
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, 0 );
      usleep( pollInt );
    }
    return 0;
  }
}

namespace eos
{
  //----------------------------------------------------------------------------
  // Initizlize the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::initialize() throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Decide on how to open the change log
    //--------------------------------------------------------------------------
    int logOpenFlags = 0;
    if( pSlaveMode )
    {
      if( !pSlaveLock )
      {
        MDException e( EINVAL );
        e.getMessage() << "ContainerMDSvc: slave lock not set";
        throw e;
      }
      logOpenFlags = ChangeLogFile::ReadOnly;
    }
    else
      logOpenFlags = ChangeLogFile::Create | ChangeLogFile::Append;

    //--------------------------------------------------------------------------
    // Rescan the change log if needed
    //
    // In the master mode we go throug the entire file
    // In the slave mode up untill the compaction mark or not at all
    // if the compaction mark is not present
    //--------------------------------------------------------------------------
    pChangeLog->open( pChangeLogPath, logOpenFlags, CONTAINER_LOG_MAGIC );
    bool logIsCompacted = (pChangeLog->getUserFlags() & LOG_FLAG_COMPACTED);
    pFollowStart = pChangeLog->getFirstOffset();

    if( !pSlaveMode || logIsCompacted )
    {
      ContainerMDScanner scanner( pIdMap, pSlaveMode );
      pFollowStart = pChangeLog->scanAllRecords( &scanner );
      pFirstFreeId = scanner.getLargestId()+1;

      //------------------------------------------------------------------------
      // Recreate the container structure
      //------------------------------------------------------------------------
      IdMap::iterator it;
      ContainerList   orphans;
      ContainerList   nameConflicts;
      for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
      {
        if( it->second.ptr )
          continue;
        recreateContainer( it, orphans, nameConflicts );
      }

      //------------------------------------------------------------------------
      // Deal with broken containers if we're not in the slave mode
      //------------------------------------------------------------------------
      if( !pSlaveMode )
      {
        attachBroken( getLostFoundContainer( "orphans" ), orphans );
        attachBroken( getLostFoundContainer( "name_conflicts" ), nameConflicts );
      }
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
      MDException e( EINVAL );
      e.getMessage() << "changelog_path not specified" ;
      throw e;
    }
    pChangeLogPath = it->second;

    //--------------------------------------------------------------------------
    // Check whether we should run in the slave mode
    //--------------------------------------------------------------------------
    it = config.find( "slave_mode" );
    if( it != config.end() && it->second == "true" )
    {
      pSlaveMode = true;
      int32_t pollInterval = 1000;
      it = config.find( "poll_interval_us" );
      if( it != config.end() )
      {
        pollInterval = strtol( it->second.c_str(), 0, 0 );
        if( pollInterval == 0 ) pollInterval = 1000;
      }
    }
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
  // Start the slave
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::startSlave() throw( MDException )
  {
    if( !pSlaveMode )
    {
      MDException e( errno );
      e.getMessage() << "ContainerMDSvc: not in slave mode";
      throw e;
    }

    if( pthread_create( &pFollowerThread, 0, followerThread, this ) != 0 )
    {
      MDException e( errno );
      e.getMessage() << "ContainerMDSvc: unable to start the slave follower: ";
      e.getMessage() << strerror( errno );
      throw e;
    }
    pSlaveStarted = true;
  }

  //----------------------------------------------------------------------------
  // Stop the slave mode
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::stopSlave() throw( MDException )
  {
    if( !pSlaveMode )
    {
      MDException e( errno );
      e.getMessage() << "ContainerMDSvc: not in slave mode";
      throw e;
    }

    if( !pSlaveStarted )
    {
      MDException e( errno );
      e.getMessage() << "ContainerMDSvc: the slave follower is not started";
      throw e;
    }

    if( pthread_cancel( pFollowerThread ) != 0 )
    {
      MDException e( errno );
      e.getMessage() << "ContainerMDSvc: unable to cancel the slave follower: ";
      e.getMessage() << strerror( errno );
      throw e;
    }

    if( pthread_join( pFollowerThread, 0 ) != 0 )
    {
      MDException e( errno );
      e.getMessage() << "ContainerMDSvc: unable to join the slave follower: ";
      e.getMessage() << strerror( errno );
      throw e;
    }
    pSlaveStarted = false;
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
      ContainerMD *child  = parent->findContainer( container->getName() );
      if( !child )
        parent->addContainer( container );
      else
      {
        nameConflicts.push_back( child );
        parent->addContainer( container );
      }
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
  bool ChangeLogContainerMDSvc::ContainerMDScanner::processRecord(
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

    //--------------------------------------------------------------------------
    // Compaction mark - we stop scanning here
    //--------------------------------------------------------------------------
    else if( type == COMPACT_STAMP_RECORD_MAGIC )
    {
      if( pSlaveMode )
        return false;
    }
    return true;
  }
}
