/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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
//! author Elvin Sindrilaru <esindril@cern.ch>
//! @brief Filesystem-based container metadata service
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FS_CONTAINER_MD_SVC_HH__
#define __EOS_NS_FS_CONTAINER_MD_SVC_HH__

#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IChLogContainerMDSvc.hh"
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <list>
#include <map>
#include <pthread.h>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class LockHandler;

//------------------------------------------------------------------------------
//! Filesystem-based container metadata service
//------------------------------------------------------------------------------
class FsContainerMDSvc:public IContainerMDSvc
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FsContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Initizlize the container service
  //----------------------------------------------------------------------------
  virtual void initialize();

  //----------------------------------------------------------------------------
  //! Configure the container service
  //----------------------------------------------------------------------------
  virtual void configure(std::map<std::string, std::string>& config);

  //----------------------------------------------------------------------------
  //! Finalize the container service
  //----------------------------------------------------------------------------
  virtual void finalize();

  //----------------------------------------------------------------------------
  //! Get the container metadata information for the given path
  //!
  //! @param rel_path relative path with respect to the root location
  //!
  //! @return IContainerMD object
  //----------------------------------------------------------------------------
  IContainerMD* getContainerMD(const std::string& rel_path);

  //----------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //----------------------------------------------------------------------------
  virtual IContainerMD* createContainer();

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj);

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  virtual uint64_t getNumContainers() const;

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //----------------------------------------------------------------------------
  virtual void setQuotaStats(IQuotaStats* quotaStats)
  {
    pQuotaStats = quotaStats;
  }

  //----------------------------------------------------------------------------
  //----------------------------------------------------------------------------
  //  !!! THE FOLLOWING FUNCTIONS ARE NOT OR DON'T NEED TO BE IMPLEMENTED !!!
  //----------------------------------------------------------------------------
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Update the container metadata in the backing store after the
  //! ContainerMD object has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IContainerMD*)
  {
    std::cerr << "This function is not implemented" << std::endl;
    return;
  };

  //----------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID
  //!
  //! @return IContainerMD object
  //----------------------------------------------------------------------------
  virtual IContainerMD* getContainerMD(IContainerMD::id_t id)
  {
    std::cerr << "This function returns a NULL pointer" << std::endl;
    return (IContainerMD*)0;
  };

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD::id_t containerId);
  {
    std::cerr << "This function is not implemented." << std::endl;
    return;
  }

  //----------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //----------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener)
  {
    std::cerr << "This function is not implemented" << std::endl;
    return;
  }

 private:

  typedef std::list<IContainerMDChangeListener*> ListenerList;
  typedef std::list<IContainerMD*>               ContainerList;

  //----------------------------------------------------------------------------
  //! Data members
  //----------------------------------------------------------------------------
  ListenerList       pListeners;
  IQuotaStats*       pQuotaStats;
  std::string        mMountPath;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FS_CONTAINER_MD_SVC_HH__
