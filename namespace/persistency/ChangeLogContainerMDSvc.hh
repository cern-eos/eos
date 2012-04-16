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

#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <list>
#include <map>

namespace eos
{
  //----------------------------------------------------------------------------
  //! ChangeLog based container metadata service
  //----------------------------------------------------------------------------
  class ChangeLogContainerMDSvc: public IContainerMDSvc
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ChangeLogContainerMDSvc(): pFirstFreeId( 0 )
      {
        pIdMap.set_deleted_key( 0 );
        pIdMap.set_empty_key( 0xffffffffffffffffll );
        pIdMap.resize(1000000);
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

      typedef google::dense_hash_map<ContainerMD::id_t, DataInfo> IdMap;
      typedef std::list<IContainerMDChangeListener*>              ListenerList;

      //------------------------------------------------------------------------
      // Changelog record scanner
      //------------------------------------------------------------------------
      class ContainerMDScanner: public ILogRecordScanner
      {
        public:
          ContainerMDScanner( IdMap &idMap ): pIdMap( idMap ), pLargestId( 0 )
          {}
          virtual void processRecord( uint64_t offset, char type,
                                      const Buffer &buffer );
          ContainerMD::id_t getLargestId() const
          {
            return pLargestId;
          }
        private:
          IdMap             &pIdMap;
          ContainerMD::id_t  pLargestId;
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
      // Recreate the container
      //------------------------------------------------------------------------
      void recreateContainer( IdMap::iterator &it );

      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      ContainerMD::id_t  pFirstFreeId;
      std::string        pChangeLogPath;
      ChangeLogFile     *pChangeLog;
      IdMap              pIdMap;
      ListenerList       pListeners;
  };
}

#endif // EOS_NS_CHANGE_LOG_FILE_MD_SVC_HH
