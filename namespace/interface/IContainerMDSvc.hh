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
// desc:   ContainerMD service interface
//------------------------------------------------------------------------------

#ifndef EOS_NS_I_CONTAINER_MD_SVC_HH
#define EOS_NS_I_CONTAINER_MD_SVC_HH

#include <map>
#include <string>
#include "namespace/MDException.hh"

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class IContainerMD;

//----------------------------------------------------------------------------
//! Interface for the listener that is notified about all of the
//! actions performed in a IContainerMDSvc
//----------------------------------------------------------------------------
class IContainerMDChangeListener
{
 public:
  enum Action
  {
    Updated = 0,
    Deleted,
    Created
  };

  virtual void containerMDChanged(IContainerMD* obj, Action type);
};

//----------------------------------------------------------------------------
//! Interface for class responsible for managing the metadata information
//! concerning containers. It is responsible for assigning container IDs and
//! managing storage of the metadata. Could be implemented as a change log or
//! db based store or as an interface to memcached or some other caching
//! system or key value store
//----------------------------------------------------------------------------
class IContainerMDSvc
{
 public:
  virtual ~IContainerMDSvc() {}

  //------------------------------------------------------------------------
  //! Initizlize the container service
  //------------------------------------------------------------------------
  virtual void initialize() throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Configure the container service
  //------------------------------------------------------------------------
  virtual void configure(std::map<std::string, std::string>& config)
      throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Finalize the container service
  //------------------------------------------------------------------------
  virtual void finalize() throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
  //------------------------------------------------------------------------
  virtual IContainerMD* getContainerMD(IContainerMD::id_t id)
      throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //------------------------------------------------------------------------
  virtual IContainerMD* createContainer() throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj) throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Remove object from the store
  //------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj) throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Remove object from the store
  //------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD::id_t containerId)
      throw(MDException) = 0;

  //------------------------------------------------------------------------
  //! Get number of containers
  //------------------------------------------------------------------------
  virtual uint64_t getNumContainers() const = 0;

  //------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener) = 0;
};

EOSNSNAMESPACE_END

#endif // EOS_NS_I_CONTAINER_MD_SVC_HH
