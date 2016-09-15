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

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/MDException.hh"
#include <map>
#include <string>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class IQuotaStats;

//----------------------------------------------------------------------------
//! Interface for the listener that is notified about all of the
//! actions performed in a IContainerMDSvc
//----------------------------------------------------------------------------
class IContainerMDChangeListener
{
public:
  enum Action {
    Updated = 0,
    Deleted,
    Created,
    MTimeChange
  };

  virtual ~IContainerMDChangeListener() {}
  virtual void containerMDChanged(IContainerMD* obj, Action type) = 0;
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
  virtual void initialize() = 0;

  //------------------------------------------------------------------------
  //! Configure the container service
  //------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config) = 0;

  //------------------------------------------------------------------------
  //! Finalize the container service
  //------------------------------------------------------------------------
  virtual void finalize() = 0;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  getContainerMD(IContainerMD::id_t id) = 0;

  //------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer() = 0;

  //------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj) = 0;

  //------------------------------------------------------------------------
  //! Remove object from the store
  //------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj) = 0;

  //------------------------------------------------------------------------
  //! Get number of containers
  //------------------------------------------------------------------------
  virtual uint64_t getNumContainers() = 0;

  //------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener) = 0;

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //!
  //! @param quota_stats object implementing the IQuotaStats interface
  //----------------------------------------------------------------------------
  virtual void setQuotaStats(IQuotaStats* quota_stats) = 0;

  //----------------------------------------------------------------------------
  //! Notify all subscribed listener
  //----------------------------------------------------------------------------
  virtual void notifyListeners(IContainerMD* obj,
                               IContainerMDChangeListener::Action a) = 0;

  //----------------------------------------------------------------------------
  //! Get the orphans container
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  getLostFoundContainer(const std::string& name) = 0;

  //------------------------------------------------------------------------
  // Create container in parent
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  createInParent(const std::string& name, IContainerMD* parent) = 0;

  //----------------------------------------------------------------------------
  //! Set file metadata service
  //----------------------------------------------------------------------------
  virtual void setFileMDService(IFileMDSvc* file_svc) = 0;

  //----------------------------------------------------------------------------
  //! Set container accounting
  //---------------------------------------------------------------------------
  virtual void
  setContainerAccounting(IFileMDChangeListener* containerAccounting) = 0;

  //----------------------------------------------------------------------------
  //! Get first free container id
  //----------------------------------------------------------------------------
  virtual IContainerMD::id_t getFirstFreeId() const = 0;
};

EOSNSNAMESPACE_END

#endif // EOS_NS_I_CONTAINER_MD_SVC_HH
