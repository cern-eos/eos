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

#ifndef EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH
#define EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH

#include "namespace/ContainerMD.hh"
#include "namespace/MDException.hh"
#include "namespace/IContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFile.hh"
#include "namespace/accounting/QuotaStats.hh"

#include "namespace/persistency/hashtable/PersistentHashtable.hh"

#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <list>
#include <map>
#include <pthread.h>

namespace eos
{
  class LockHandler;

  hash_value_t hash_id_t (const void* contents);
  int comp_id_t (const void* a, const void* b);

  //----------------------------------------------------------------------------
  //! ChangeLog based container metadata service
  //----------------------------------------------------------------------------
  class ChangeLogContainerMDSvc: public IContainerMDSvc
  {
    friend class ContainerMDFollower;
    friend class FileMDFollower;
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ChangeLogContainerMDSvc(): pFirstFreeId( 0 ),
        pIdMap ( 1000000, hash_id_t, comp_id_t ),
        pSlaveLock( 0 ),
        pSlaveMode( false ), pSlaveStarted( false ), pSlavePoll( 1000 ),
        pFollowStart( 0 ), pQuotaStats( 0 ), pAutoRepair( 0 )
      {
        pChangeLog = new ChangeLogFile;
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~ChangeLogContainerMDSvc()
      {
        delete pChangeLog;
      }

      //------------------------------------------------------------------------
      //! Initizlize the container service
      //------------------------------------------------------------------------
      virtual void initialize() throw( MDException );

      //------------------------------------------------------------------------
      //! Make a transition from slave to master
      // -----------------------------------------------------------------------
      virtual void slave2Master( std::map<std::string, std::string> &config )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Switch the namespace to read-only mode
      //------------------------------------------------------------------------
      virtual void makeReadOnly()
        throw( MDException );

      //------------------------------------------------------------------------
      //! Configure the container service
      //------------------------------------------------------------------------
      virtual void configure( std::map<std::string, std::string> &config )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Finalize the container service
      //------------------------------------------------------------------------
      virtual void finalize() throw( MDException );

      //------------------------------------------------------------------------
      //! Get the container metadata information for the given container ID
      //------------------------------------------------------------------------
      virtual ContainerMD *getContainerMD( ContainerMD::id_t id )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Create new container metadata object with an assigned id, the user has
      //! to fill all the remaining fields
      //------------------------------------------------------------------------
      virtual ContainerMD *createContainer() throw( MDException );

      //------------------------------------------------------------------------
      //! Update the contaienr metadata in the backing store after the
      //! ContainerMD object has been changed
      //------------------------------------------------------------------------
      virtual void updateStore( ContainerMD *obj ) throw( MDException );

      //------------------------------------------------------------------------
      //! Remove object from the store
      //------------------------------------------------------------------------
      virtual void removeContainer( ContainerMD *obj ) throw( MDException );

      //------------------------------------------------------------------------
      //! Remove object from the store
      //------------------------------------------------------------------------
      virtual void removeContainer( ContainerMD::id_t containerId )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get number of containers
      //------------------------------------------------------------------------
      virtual uint64_t getNumContainers() const
      {
        return pIdMap.size();
      }

      //------------------------------------------------------------------------
      //! Add file listener that will be notified about all of the changes in
      //! the store
      //------------------------------------------------------------------------
      virtual void addChangeListener( IContainerMDChangeListener *listener );

      //------------------------------------------------------------------------
      //! Prepare for online compacting.
      //!
      //! No external file metadata mutation may occur while the method is
      //! running.
      //!
      //! @param  newLogFileName name for the compacted log file
      //! @return                compacting information that needs to be passed
      //!                        to other functions
      //! @throw  MDException    preparation stage failed, cannot proceed with
      //!                        compacting
      //------------------------------------------------------------------------
      void *compactPrepare (const std::string &newLogFileName) const
        throw ( MDException);

      //------------------------------------------------------------------------
      //! Do the compacting.
      //!
      //! This does not access any of the in-memory structures so any external
      //! metadata operations (including mutations) may happen while it is
      //! running.
      //!
      //! @param  compactingData state information returned by CompactPrepare
      //! @throw  MDException    failure, cannot proceed with CompactCommit
      //------------------------------------------------------------------------
      static void compact (void *&compactingData) throw ( MDException);

      //------------------------------------------------------------------------
      //! Commit the compacting infomrmation.
      //!
      //! Updates the metadata structures. Needs an exclusive lock on the
      //! namespace. After successfull completion the new compacted
      //! log will be used for all the new data
      //!
      //! @param compactingData state information obtained from CompactPrepare
      //!                       and modified by Compact
      //! @param autorepair     indicates to skip broken records
      //! @throw MDExcetion     failure, results of the compacting are
      //!                       are discarded, the old log will be used for
      //------------------------------------------------------------------------
      void compactCommit (void *compactingData, bool autorepair=false) throw ( MDException);

      //------------------------------------------------------------------------
      //! Register slave lock
      //------------------------------------------------------------------------
      void setSlaveLock( LockHandler *slaveLock )
      {
        pSlaveLock = slaveLock;
      }

      //------------------------------------------------------------------------
      //! Get slave lock
      //------------------------------------------------------------------------
      LockHandler *getSlaveLock()
      {
        return pSlaveLock;
      }

      //------------------------------------------------------------------------
      //! get slave mode
      //------------------------------------------------------------------------
      bool getSlaveMode() 
      {
        return pSlaveMode;
      }

      //------------------------------------------------------------------------
      //! Start the slave
      //------------------------------------------------------------------------
      void startSlave() throw( MDException );

      //------------------------------------------------------------------------
      //! Stop the slave mode
      //------------------------------------------------------------------------
      void stopSlave() throw( MDException );

      //------------------------------------------------------------------------
      //! Create container in parent
      //------------------------------------------------------------------------
      ContainerMD *createInParent( const std::string &name,
                                   ContainerMD       *parent )
                    throw( MDException );

      //------------------------------------------------------------------------
      //! Get the lost+found container, create if necessary
      //------------------------------------------------------------------------
      ContainerMD *getLostFound() throw( MDException );

      //------------------------------------------------------------------------
      //! Get the orphans container
      //------------------------------------------------------------------------
      ContainerMD *getLostFoundContainer( const std::string &name )
                    throw( MDException );

      //------------------------------------------------------------------------
      //! Get the change log
      //------------------------------------------------------------------------
      ChangeLogFile *getChangeLog()
      {
        return pChangeLog;
      }

      //------------------------------------------------------------------------
      //! Get the following offset
      //------------------------------------------------------------------------
      uint64_t getFollowOffset() const
      {
        return pFollowStart;
      }

      //------------------------------------------------------------------------
      //! Set the following offset
      //------------------------------------------------------------------------
      void setFollowOffset(uint64_t offset) 
      {
        pFollowStart = offset;
      }

      //------------------------------------------------------------------------
      //! Get the following poll interval
      //------------------------------------------------------------------------
      uint64_t getFollowPollInterval() const
      {
        return pSlavePoll;
      }

      //------------------------------------------------------------------------
      //! Set the QuotaStats object for the follower
      //------------------------------------------------------------------------
      void setQuotaStats( QuotaStats *quotaStats )
      {
        pQuotaStats = quotaStats;
      }

    private:
      //------------------------------------------------------------------------
      // Placeholder for the record info
      //------------------------------------------------------------------------
      struct DataInfo
      {
        DataInfo(): logOffset(0), ptr(0) {} // for some reason needed by sparse_hash_map::erase
        DataInfo( uint64_t logOffset, ContainerMD *ptr )
        {
          this->logOffset = logOffset;
          this->ptr       = ptr;
        }
        uint64_t     logOffset;
        ContainerMD *ptr;
      };

      typedef PersistentHashtable<ContainerMD::id_t, DataInfo> IdMap;
      typedef std::list<IContainerMDChangeListener*>           ListenerList;
      typedef std::list<ContainerMD*>                          ContainerList;

      //------------------------------------------------------------------------
      // Changelog record scanner
      //------------------------------------------------------------------------
      class ContainerMDScanner: public ILogRecordScanner
      {
        public:
          ContainerMDScanner( IdMap &idMap, bool slaveMode ):
            pIdMap( idMap ), pLargestId( 0 ), pSlaveMode( slaveMode )
          {}
          virtual bool processRecord( uint64_t offset, char type,
                                      const Buffer &buffer );
          ContainerMD::id_t getLargestId() const
          {
            return pLargestId;
          }
        private:
          IdMap             &pIdMap;
          ContainerMD::id_t  pLargestId;
          bool               pSlaveMode;
      };

      //------------------------------------------------------------------------
      // Notify the listeners about the change
      //------------------------------------------------------------------------
      void notifyListeners( ContainerMD *obj,
                            IContainerMDChangeListener::Action a )
      {
        ListenerList::iterator it;
        for( it = pListeners.begin(); it != pListeners.end(); ++it )
          (*it)->containerMDChanged( obj, a );
      }

      //------------------------------------------------------------------------
      // Recreate the container structure recursively and create the list
      // of orphans and name conflicts
      //------------------------------------------------------------------------
      void recreateContainer( IdMap::iterator &it,
                              ContainerList   &orphans,
                              ContainerList   &nameConflicts );

      //------------------------------------------------------------------------
      // Attach broken containers to lost+found
      //------------------------------------------------------------------------
      void attachBroken( ContainerMD *parent, ContainerList &broken );

      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      ContainerMD::id_t  pFirstFreeId;
      std::string        pChangeLogPath;
      ChangeLogFile     *pChangeLog;
      IdMap              pIdMap;
      ListenerList       pListeners;
      pthread_t          pFollowerThread;
      LockHandler       *pSlaveLock;
      bool               pSlaveMode;
      bool               pSlaveStarted;
      int32_t            pSlavePoll;
      uint64_t           pFollowStart;
      QuotaStats        *pQuotaStats;
      bool               pAutoRepair;
  };
}

#endif // EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH
