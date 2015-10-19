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

#include "namespace/Constants.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/utils/Locking.hh"
#include "namespace/utils/ThreadUtils.hh"
#include "namespace/ns_in_memory/FileMD.hh"
#include "namespace/ns_in_memory/ContainerMD.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogConstants.hh"

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
        pContSvc( contSvc )
      {
        pQuotaStats = pContSvc->pQuotaStats;
      }

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
          ContainerMD *container = new ContainerMD(IContainerMD::id_t(0));
          container->deserialize( (Buffer&)buffer );
          ContMap::iterator it = pUpdated.find( container->getId() );
          if( it != pUpdated.end() )
          {
            delete it->second;
            it->second = container;
          }
          else
            pUpdated[container->getId()] = container;

          //--------------------------------------------------------------------
          // Remember the largest container ID
          //--------------------------------------------------------------------
          if( container->getId() >= pContSvc->pFirstFreeId )
            pContSvc->pFirstFreeId = container->getId() + 1;

          pDeleted.erase( container->getId() );
        }

        //----------------------------------------------------------------------
        // Deletion
        //----------------------------------------------------------------------
        else if( type == DELETE_RECORD_MAGIC )
        {
          IContainerMD::id_t id;
          buffer.grabData( 0, &id, sizeof( IContainerMD::id_t ) );
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
        std::set<eos::IContainerMD::id_t>::iterator itD;
        std::list<IContainerMD::id_t> processed;
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
            IContainerMD *cont = itP->second.ptr->findContainer( it->second.ptr->getName() );
            if( cont == it->second.ptr )
              itP->second.ptr->removeContainer( it->second.ptr->getName() );
          }
          delete it->second.ptr;
          idMap->erase( it );
          processed.push_back( *itD );
        }

        std::list<IContainerMD::id_t>::iterator itPro;
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
          IContainerMD *currentCont = itU->second;
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
            if (it->second.ptr->getParentId() == currentCont->getParentId() )
            {
              // ---------------------------------------------------------------
              // update within the same parent directory
              // ---------------------------------------------------------------
              if (currentCont->getName() == it->second.ptr->getName())
              {
                // -------------------------------------------------------------
                // meta data change - keeping directory name
                // -------------------------------------------------------------
                (*it->second.ptr) = *currentCont;
                delete currentCont;
              }
              else
              {
                // -------------------------------------------------------------
                // directory rename
                // -------------------------------------------------------------
                itP = idMap->find(currentCont->getParentId());
                if (itP != idMap->end())
                {
                  // -----------------------------------------------------------
                  // remove container with old name
                  // -----------------------------------------------------------
                  itP->second.ptr->removeContainer(it->second.ptr->getName());
                  delete it->second.ptr;
                  // -----------------------------------------------------------
                  // add container with new name
                  // -----------------------------------------------------------
                  itP->second.ptr->addContainer(currentCont);
                  // -----------------------------------------------------------
                  // update idmap pointer to the container
                  // -----------------------------------------------------------
                  (*idMap)[currentCont->getId()] = ChangeLogContainerMDSvc::DataInfo(0, currentCont);
                }
              }
            }
            else
            {
              // ---------------------------------------------------------------
              // STEP 1 container move (moving a subtree)
              // ---------------------------------------------------------------
              ChangeLogContainerMDSvc::IdMap::iterator itNP;
              // ---------------------------------------------------------------
              // get the old and the new parent container
              // ---------------------------------------------------------------
              itP = idMap->find(it->second.ptr->getParentId());
              itNP = idMap->find(currentCont->getParentId());
              if ((itP != idMap->end()) && (itNP != idMap->end()))
              {
                // -------------------------------------------------------------
                // substract all the files in the old tree from their quota node
                // -------------------------------------------------------------
                size_t deepness = 0;
                ContainerMD::ContainerMap::iterator cIt;
                ContainerMD::FileMap::iterator fIt;
                std::vector<std::set<IContainerMD*> > dirTree;
                dirTree.resize(1);
                dirTree[0].insert(it->second.ptr);
                do
                {
                  // -----------------------------------------------------------
                  // we run down the directory hierarchy starting at deepness 0
                  // until there are no more subcontainer found
                  // -----------------------------------------------------------
                  dirTree.resize(deepness + 2);

                  std::set<IContainerMD*>::const_iterator dIt;
                  // -----------------------------------------------------------
                  // loop over all attached directories in that deepness
                  // -----------------------------------------------------------
                  for (dIt = dirTree[deepness].begin();
                       dIt != dirTree[deepness].end();
                       dIt++)
                  {
                    // ---------------------------------------------------------
                    // attach the sub-container at the next deepness level
                    // ---------------------------------------------------------
                    for (auto dmd = (*dIt)->beginSubContainer(); dmd;
                         dmd = (*dIt)->nextSubContainer())
                    {
                      dirTree[deepness + 1].insert(dmd);
                    }
                    // ---------------------------------------------------------
                    // remove every file from it's quota node
                    // ---------------------------------------------------------

                    for (auto fmd = (*dIt)->beginFile(); fmd; fmd = (*dIt)->nextFile())
                    {
                      IQuotaNode *node = getQuotaNode(*dIt);

                      if (node)
                        node->removeFile(fmd);
                    }
                  }
                  deepness++;
                }
                while (dirTree[deepness].size());
                // -------------------------------------------------------------
                // STEP 2 move the source container
                // -------------------------------------------------------------

                // -------------------------------------------------------------
                // first remove from the old parent container
                // -------------------------------------------------------------
                itP->second.ptr->removeContainer(it->second.ptr->getName());
                // -------------------------------------------------------------
                // copy the meta data
                // -------------------------------------------------------------
                (*it->second.ptr) = *currentCont;
                // -------------------------------------------------------------
                // add to the new parent container
                // -------------------------------------------------------------
                itNP->second.ptr->addContainer(it->second.ptr);
                delete currentCont;

                // -------------------------------------------------------------
                // STEP 3 add all the files in the new tree to new quota node
                // -------------------------------------------------------------
                deepness = 0;

                // -------------------------------------------------------------
                // we still have the full hierarchy stored and start at deepn. 0
                // -------------------------------------------------------------
                while (dirTree[deepness].size())
                {
                  std::set<IContainerMD*>::const_iterator dIt;

                  // -----------------------------------------------------------
                  // loop over all attached directories in that deepness
                  // -----------------------------------------------------------
                  for (dIt = dirTree[deepness].begin();
                       dIt != dirTree[deepness].end();
                       dIt++)
                  {
                    // ---------------------------------------------------------
                    // remove every file from it's quota node
                    // --------------------------------------------------------
                    for (auto fmd = (*dIt)->beginFile(); fmd; fmd = (*dIt)->nextFile())
                    {
                      IQuotaNode *node = getQuotaNode(*dIt);

                      if (node)
                        node->addFile(fmd);
                    }
                  }
                  deepness++;
                }
                // -------------------------------------------------------------
                // all done - clean up
                // -------------------------------------------------------------
                dirTree.clear();
              }
            }
          }
        }
        pUpdated.clear();
        pContSvc->getSlaveLock()->unLock();
      }

    private:

      //------------------------------------------------------------------------
      // Get quota node id concerning given container
      //------------------------------------------------------------------------
      IQuotaNode *
      getQuotaNode (const IContainerMD *container)
      {
        // Initial sanity check
        if (!container)
          return 0;

        if (!pQuotaStats)
          return 0;

        // Search for the node
        const IContainerMD *current = container;

        while ( ( current->getId() != 1 ) &&
                ( (current->getFlags() & QUOTA_NODE_FLAG) == 0) )
          current = pContSvc->getContainerMD(current->getParentId());

        //----------------------------------------------------------------------
        // We have either found a quota node or reached root without finding one
        // so we need to double check whether the current container has an
        // associated quota node
        //----------------------------------------------------------------------
        if ((current->getFlags() & QUOTA_NODE_FLAG) == 0)
          return 0;

        IQuotaNode *node = pQuotaStats->getQuotaNode(current->getId());
        if (node)
          return node;

        return pQuotaStats->registerNewNode(current->getId());
      }

      typedef std::map<IContainerMD::id_t, eos::IContainerMD*> ContMap;
      ContMap                           pUpdated;
      std::set<eos::IContainerMD::id_t>  pDeleted;
      eos::ChangeLogContainerMDSvc     *pContSvc;
      IQuotaStats                       *pQuotaStats;
  };
}

extern "C"
{
  //----------------------------------------------------------------------------
  // Follow the change log
  //----------------------------------------------------------------------------
  static void *followerThread( void *data )
  {
    eos::ThreadUtils::blockAIOSignals();
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
      contSvc->setFollowOffset(offset);
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, 0 );
      file->wait(pollInt);
    }
    return 0;
  }
}

//------------------------------------------------------------------------------
// Helper structures for online compacting
//------------------------------------------------------------------------------
namespace
{
  //----------------------------------------------------------------------------
  // Store info about old and new offset for a given file id
  //----------------------------------------------------------------------------
  struct ContainerRecordData
  {
    ContainerRecordData() : offset(0), newOffset(0), containerId(0) { }

    ContainerRecordData(uint64_t o, eos::IContainerMD::id_t i, uint64_t no = 0) :
      offset(o), newOffset(no), containerId(i) { }
    uint64_t offset;
    uint64_t newOffset;
    eos::IContainerMD::id_t containerId;
  };

  //----------------------------------------------------------------------------
  // Carry the data between compacting stages
  //----------------------------------------------------------------------------
  struct ContainerCompactingData
  {

    ContainerCompactingData() :
      newLog(new eos::ChangeLogFile()),
      originalLog(0),
      newRecord(0) { }

    ~ContainerCompactingData()
    {
      delete newLog;
    }
    std::string logFileName;
    eos::ChangeLogFile *newLog;
    eos::ChangeLogFile *originalLog;
    std::vector<ContainerRecordData> records;
    uint64_t newRecord;
  };

  //----------------------------------------------------------------------------
  // Compare record data objects in order to sort them
  //----------------------------------------------------------------------------
  struct ContainerOffsetComparator
  {

    bool operator () (const ContainerRecordData &a, const ContainerRecordData &b)
    {
      return a.offset < b.offset;
    }
  };

  //----------------------------------------------------------------------------
  // Process the records being scanned and copy them to the new log
  //----------------------------------------------------------------------------
  class ContainerUpdateHandler : public eos::ILogRecordScanner
  {
  public:

    //------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------
    ContainerUpdateHandler (std::map<eos::IContainerMD::id_t,
                            ContainerRecordData> &updates,
                            eos::ChangeLogFile *newLog) :
      pUpdates (updates), pNewLog (newLog) { }

    //------------------------------------------------------------------------
    // Process the records
    //------------------------------------------------------------------------
    virtual bool
    processRecord (uint64_t offset,
                   char type,
                   const eos::Buffer &buffer)
    {
      // Write to the new change log - we need to cast - nasty, but safe in
      // this case
      uint64_t newOffset = pNewLog->storeRecord(type, (eos::Buffer&)buffer);

      // Put the right stuff in the updates map
      eos::IContainerMD::id_t id;
      buffer.grabData(0, &id, sizeof ( eos::IFileMD::id_t));

      if (type == eos::UPDATE_RECORD_MAGIC)
        pUpdates[id] = ContainerRecordData(offset, id, newOffset);
      else if (type == eos::DELETE_RECORD_MAGIC)
        pUpdates.erase(id);

      return true;
    }

  private:
    std::map<eos::IContainerMD::id_t, ContainerRecordData> &pUpdates;
    eos::ChangeLogFile *pNewLog;
    uint64_t pCounter;
  };
}

namespace eos
{
  //----------------------------------------------------------------------------
  // Initizlize the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::initialize()
  {
    // Decide on how to open the change log
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

    // Rescan the change log if needed
    //
    // In the master mode we go throug the entire file
    // In the slave mode up until the compaction mark or not at all
    // if the compaction mark is not present
    pChangeLog->open( pChangeLogPath, logOpenFlags, CONTAINER_LOG_MAGIC );
    bool logIsCompacted = (pChangeLog->getUserFlags() & LOG_FLAG_COMPACTED);
    pFollowStart = pChangeLog->getFirstOffset();

    if( !pSlaveMode || logIsCompacted )
    {
      ContainerMDScanner scanner( pIdMap, pSlaveMode );
      pFollowStart = pChangeLog->scanAllRecords( &scanner , pAutoRepair );
      pFirstFreeId = scanner.getLargestId()+1;

      // Recreate the container structure
      IdMap::iterator it;
      ContainerList   orphans;
      ContainerList   nameConflicts;
      for( it = pIdMap.begin(); it != pIdMap.end(); ++it )
      {
        if( it->second.ptr )
          continue;
        recreateContainer( it, orphans, nameConflicts );
      }

      // Deal with broken containers if we're not in the slave mode
      if( !pSlaveMode )
      {
        attachBroken( getLostFoundContainer( "orphans" ), orphans );
        attachBroken( getLostFoundContainer( "name_conflicts" ), nameConflicts );
      }
    }
  }

  //----------------------------------------------------------------------------
  // Make a transition from slave to master
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::slave2Master(
            std::map<std::string, std::string> &config)
  {
    // Find the new changelog path
    std::map<std::string, std::string>::iterator it;
    it = config.find( "changelog_path" );
    if( it == config.end() )
    {
      MDException e( EINVAL );
      e.getMessage() << "changelog_path not specified";
      throw e;
    }

    if( it->second == pChangeLogPath )
    {
      MDException e( EINVAL );
      e.getMessage() << "changelog_path must differ from the original ";
      e.getMessage() << "changelog_path";
      throw e;
    }

    // Copy the current changelog file to the previous name
    std::string tmpChangeLogPath     = pChangeLogPath;
    tmpChangeLogPath += ".tmp";
    std::string currentChangeLogPath = pChangeLogPath;

    std::string copyCmd = "cp -f ";
    copyCmd += currentChangeLogPath.c_str();
    copyCmd += " ";
    copyCmd += tmpChangeLogPath.c_str();

    eos::common::ShellCmd scmd(copyCmd.c_str());
    eos::common::cmd_status rc = scmd.wait(60);

    if( rc.exit_code )
    {
      MDException e( EIO ) ;
      e.getMessage() << "Failed to copy the current change log file <";
      e.getMessage() << pChangeLogPath << ">";
    }

    // Redefine the valid changelog path
    pChangeLogPath = it->second;

    // Rename the current changelog file to the new file name
    if( rename( currentChangeLogPath.c_str(), pChangeLogPath.c_str() ) )
    {
      MDException e( EINVAL );
      e.getMessage() << "Failed to rename changelog file from <";
      e.getMessage() << currentChangeLogPath << "> to <" << pChangeLogPath;
      throw e;
    }

    // Rename the temp changelog file to the new file name
    if( rename( tmpChangeLogPath.c_str(), currentChangeLogPath.c_str() ) )
    {
      MDException e( EINVAL );
      e.getMessage() << "Failed to rename changelog file from <";
      e.getMessage() << tmpChangeLogPath << "> to <" << currentChangeLogPath;
      throw e;
    }

    // Stop the follower thread
    stopSlave();

    // Reopen changelog file in writable mode = close + open (append)
    pChangeLog->close( ) ;
    int logOpenFlags = ChangeLogFile::Create | ChangeLogFile::Append;
    pChangeLog->open( pChangeLogPath, logOpenFlags, CONTAINER_LOG_MAGIC );
  }

  //----------------------------------------------------------------------------
  //! Switch the namespace to read-only mode
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::makeReadOnly()
  {
    pChangeLog->close( ) ;

    int logOpenFlags = ChangeLogFile::ReadOnly;
    pChangeLog->open( pChangeLogPath, logOpenFlags, CONTAINER_LOG_MAGIC );
  }

  //----------------------------------------------------------------------------
  // Configure the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::configure(
      std::map<std::string, std::string> &config )
  {
    // Configure the changelog
    std::map<std::string, std::string>::iterator it;
    it = config.find( "changelog_path" );

    if( it == config.end() )
    {
      MDException e( EINVAL );
      e.getMessage() << "changelog_path not specified" ;
      throw e;
    }
    pChangeLogPath = it->second;

    // Check whether we should run in the slave mode
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

    pAutoRepair = false;
    it = config.find( "auto_repair" );
    if (it != config.end() && it->second == "true" )
      pAutoRepair = true;
  }

  //----------------------------------------------------------------------------
  // Finalize the container service
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::finalize()
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
  IContainerMD *ChangeLogContainerMDSvc::getContainerMD( IContainerMD::id_t id )

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
  IContainerMD *ChangeLogContainerMDSvc::createContainer()
  {
    IContainerMD *cont = new ContainerMD( pFirstFreeId++ );
    pIdMap.insert( std::make_pair( cont->getId(), DataInfo( 0, cont ) ) );
    return cont;
  }

  //----------------------------------------------------------------------------
  // Update the contaienr metadata in the backing store
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::updateStore( IContainerMD *obj )
  {
    // Find the object in the map
    IdMap::iterator it = pIdMap.find( obj->getId() );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container #" << obj->getId() << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    // Store the file in the changelog and notify the listener
    eos::Buffer buffer;
    dynamic_cast<ContainerMD*>(obj)->serialize(buffer);
    it->second.logOffset = pChangeLog->storeRecord( eos::UPDATE_RECORD_MAGIC,
                                                    buffer );
    notifyListeners( obj, IContainerMDChangeListener::Updated );
  }

  //----------------------------------------------------------------------------
  // Remove object from the store
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::removeContainer( IContainerMD *obj )
  {
    removeContainer( obj->getId() );
  }

  //----------------------------------------------------------------------------
  // Remove object from the store
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::removeContainer( IContainerMD::id_t containerId )
  {
    // Find the object in the map
    IdMap::iterator it = pIdMap.find( containerId );
    if( it == pIdMap.end() )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container #" << containerId << " not found. ";
      e.getMessage() << "The object was not created in this store!";
      throw e;
    }

    // Store the file in the changelog and notify the listener
    eos::Buffer buffer;
    buffer.putData( &containerId, sizeof( IContainerMD::id_t ) );
    pChangeLog->storeRecord( eos::DELETE_RECORD_MAGIC, buffer );
    notifyListeners( it->second.ptr, IContainerMDChangeListener::Deleted );
    delete it->second.ptr;
    pIdMap.erase( it );
  }

  //----------------------------------------------------------------------------
  // Add change listener
  //----------------------------------------------------------------------------
  void
  ChangeLogContainerMDSvc::addChangeListener(IContainerMDChangeListener *listener)
  {
    pListeners.push_back( listener );
  }

  //----------------------------------------------------------------------------
  // Prepare for online compacting.
  //----------------------------------------------------------------------------
  void *
  ChangeLogContainerMDSvc::compactPrepare (const std::string &newLogFileName) const
  {
    // Try to open a new log file for writing
    ::ContainerCompactingData *data = new ::ContainerCompactingData();
    try
    {
      data->newLog->open(newLogFileName, ChangeLogFile::Create,
                   CONTAINER_LOG_MAGIC);
      data->logFileName = newLogFileName;
      data->originalLog = pChangeLog;
      data->newRecord = pChangeLog->getNextOffset();
    }
    catch (MDException &e)
    {
      delete data;
      throw;
    }

    // Get the list of records
    IdMap::const_iterator it;
    for (it = pIdMap.begin(); it != pIdMap.end(); ++it)
      data->records.push_back(::ContainerRecordData(it->second.logOffset, it->first));
    return data;
  }

  //----------------------------------------------------------------------------
  // Do the compacting.
  //----------------------------------------------------------------------------
  void
  ChangeLogContainerMDSvc::compact (void *&compactingData)
  {
    // Sort the records to avoid random seeks
    ::ContainerCompactingData *data = (::ContainerCompactingData*)compactingData;
    if (!data)
      {
        MDException e(EINVAL);
        e.getMessage() << "Compacting data incorrect";
        throw e;
      }
    std::sort(data->records.begin(), data->records.end(),
              ::ContainerOffsetComparator());

    // Copy the records to the new container
    try
    {
      std::vector<ContainerRecordData>::iterator it;
      for (it = data->records.begin(); it != data->records.end(); ++it)
      {
        Buffer buff;
        uint8_t type;
        type = data->originalLog->readRecord(it->offset, buff);
        it->newOffset = data->newLog->storeRecord(type, buff);
      }
    }
    catch (MDException &e)
    {
      data->newLog->close();
      delete data;
      compactingData = 0;
      throw;
    }
  }

  //----------------------------------------------------------------------------
  // Commit the compacting information.
  //----------------------------------------------------------------------------
  void
  ChangeLogContainerMDSvc::compactCommit (void *compactingData, bool autorepair)
  {
    ::ContainerCompactingData *data = (::ContainerCompactingData*)compactingData;
    if (!data)
    {
      MDException e(EINVAL);
      e.getMessage() << "Compacting data incorrect";
      throw e;
    }

    // Copy the part of the old log that has been appended after we
    // prepared
    std::map<eos::IContainerMD::id_t, ContainerRecordData> updates;
    try
    {
      ::ContainerUpdateHandler updateHandler(updates, data->newLog);
      data->originalLog->scanAllRecordsAtOffset(&updateHandler,
                                                data->newRecord,
                                                autorepair);
    }
    catch (MDException &e)
    {
      data->newLog->close();
      delete data;
      throw;
    }

    // Looks like we're all good and we won't be throwing any exceptions any
    // more so we may get to updating the in-memory structures.
    //
    // We start with the originally copied records
    uint64_t containerCounter = 0;
    IdMap::iterator it;
    std::vector<ContainerRecordData>::iterator itO;
    for (itO = data->records.begin(); itO != data->records.end(); ++itO)
    {
      // Check if we still have the container, if not, it must have been deleted
      // so we don't care
      it = pIdMap.find(itO->containerId);
      if (it == pIdMap.end())
        continue;

      // If the original offset does not match it means that we must have
      // be updated later, if not we've messed up so we die in order not
      // to lose data
      assert(it->second.logOffset >= itO->offset);
      if (it->second.logOffset == itO->offset)
      {
        it->second.logOffset = itO->newOffset;
        ++containerCounter;
      }
    }

    // Now we handle updates, if we don't have the container, we're messed up,
    // if the original offsets don't match we're messed up too
    std::map<IContainerMD::id_t, ContainerRecordData>::iterator itU;
    for (itU = updates.begin(); itU != updates.end(); ++itU)
    {
      it = pIdMap.find(itU->second.containerId);
      assert(it != pIdMap.end());
      assert(it->second.logOffset == itU->second.offset);

      it->second.logOffset = itU->second.newOffset;
      ++containerCounter;
    }

    assert(containerCounter == pIdMap.size());

    // Replace the logs
    pChangeLog = data->newLog;
    pChangeLog->addCompactionMark();
    pChangeLogPath = data->logFileName;
    data->newLog = 0;
    data->originalLog->close();
    delete data;
  }

  //----------------------------------------------------------------------------
  // Start the slave
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::startSlave()
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
  void ChangeLogContainerMDSvc::stopSlave()
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
    pSlaveMode = false;
    pFollowerThread = 0;
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
    ContainerMD *container = new ContainerMD( IContainerMD::id_t(0) );
    container->deserialize( buffer );
    it->second.ptr = container;

    // For non-root containers recreate the parent
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

      IContainerMD *parent = parentIt->second.ptr;
      IContainerMD *child  = parent->findContainer( container->getName() );
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
  IContainerMD *ChangeLogContainerMDSvc::createInParent(
      const std::string &name,
      IContainerMD* parent )

  {
      IContainerMD *container = createContainer();
      container->setName( name );
      parent->addContainer( container );
      updateStore( container );
      return container;
  }

  //----------------------------------------------------------------------------
  // Get the lost+found container, create if necessary
  //----------------------------------------------------------------------------
  IContainerMD *ChangeLogContainerMDSvc::getLostFound()
  {
    // Get root
    IContainerMD *root = 0;
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

    // Get or create lost+found if necessary
    IContainerMD *lostFound = root->findContainer( "lost+found" );

    if( lostFound )
      return lostFound;

    return createInParent( "lost+found", root );
  }

  //----------------------------------------------------------------------------
  // Get the orphans container
  //----------------------------------------------------------------------------
  IContainerMD *ChangeLogContainerMDSvc::getLostFoundContainer(
                const std::string &name )
  {
    IContainerMD *lostFound = getLostFound();

    if( name.empty() )
      return lostFound;

    IContainerMD *cont = lostFound->findContainer( name );

    if( cont )
      return cont;

    return createInParent( name, lostFound );
  }

  //----------------------------------------------------------------------------
  // Attach broken containers to lost+found
  //----------------------------------------------------------------------------
  void ChangeLogContainerMDSvc::attachBroken( IContainerMD   *parent,
                                              ContainerList &broken )
  {
    ContainerList::iterator it;
    for( it = broken.begin(); it != broken.end(); ++it )
    {
      std::ostringstream s1, s2;
      s1 << (*it)->getParentId();
      IContainerMD *cont = parent->findContainer( s1.str() );

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
    // Update
    if( type == UPDATE_RECORD_MAGIC )
    {
      IContainerMD::id_t id;
      buffer.grabData( 0, &id, sizeof( IContainerMD::id_t ) );
      pIdMap[id] = DataInfo( offset, 0 );
      if( pLargestId < id ) pLargestId = id;
    }
    // Deletion
    else if( type == DELETE_RECORD_MAGIC )
    {
      IContainerMD::id_t id;
      buffer.grabData( 0, &id, sizeof( IContainerMD::id_t ) );
      IdMap::iterator it = pIdMap.find( id );
      if( it != pIdMap.end() )
        pIdMap.erase( it );
      if( pLargestId < id ) pLargestId = id;
    }

    // Compaction mark - we stop scanning here
    else if( type == COMPACT_STAMP_RECORD_MAGIC )
    {
      if( pSlaveMode )
        return false;
    }
    return true;
  }


  //----------------------------------------------------------------------------
  // Get changelog warning messages
  //----------------------------------------------------------------------------
  std::vector<std::string>
  ChangeLogContainerMDSvc::getWarningMessages()
  {
    return pChangeLog->getWarningMessages();
  }

  //----------------------------------------------------------------------------
  // Clear changelog warning messages
  //----------------------------------------------------------------------------
  void
  ChangeLogContainerMDSvc::clearWarningMessages()
  {
    pChangeLog->clearWarningMessages();
  }
}
