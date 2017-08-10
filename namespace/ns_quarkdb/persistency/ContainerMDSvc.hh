/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Container metadata service based on Redis
//------------------------------------------------------------------------------

#ifndef __EOS_NS_CONTAINER_MD_SVC_HH__
#define __EOS_NS_CONTAINER_MD_SVC_HH__

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include <list>
#include <map>

EOSNSNAMESPACE_BEGIN

class ContainerMD;

//------------------------------------------------------------------------------
//! Container metadata service based on Redis
//------------------------------------------------------------------------------
class ContainerMDSvc : public IContainerMDSvc
{
  friend ContainerMD;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ContainerMDSvc() {};

  //----------------------------------------------------------------------------
  //! Initizlize the container service
  //----------------------------------------------------------------------------
  virtual void initialize();

  //----------------------------------------------------------------------------
  //! Configure the container service
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config);

  //----------------------------------------------------------------------------
  //! Finalize the container service
  //----------------------------------------------------------------------------
  virtual void finalize() {};

  //----------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> getContainerMD(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer();

  //----------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj);

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj);

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  virtual uint64_t getNumContainers();

  //----------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //----------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener);

  //----------------------------------------------------------------------------
  //! Create container in parent
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> createInParent(const std::string& name,
      IContainerMD* parent);

  //----------------------------------------------------------------------------
  //! Get the lost+found container, create if necessary
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFound();

  //----------------------------------------------------------------------------
  //! Get the orphans container
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFoundContainer(const std::string& name);

  //----------------------------------------------------------------------------
  //! Set file metadata service
  //----------------------------------------------------------------------------
  void
  setFileMDService(IFileMDSvc* file_svc)
  {
    pFileSvc = file_svc;
  }

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //----------------------------------------------------------------------------
  void
  setQuotaStats(IQuotaStats* quotaStats)
  {
    pQuotaStats = quotaStats;
  }

  //----------------------------------------------------------------------------
  //! Set container accounting
  //----------------------------------------------------------------------------
  void setContainerAccounting(IFileMDChangeListener* containerAccounting)
  {
    // TODO(esindril): add implementation
  }

  //----------------------------------------------------------------------------
  //! Get first free container id
  //----------------------------------------------------------------------------
  IContainerMD::id_t getFirstFreeId();


private:
  typedef std::list<IContainerMDChangeListener*> ListenerList;

  //----------------------------------------------------------------------------
  //! Notify the listeners about the change
  //----------------------------------------------------------------------------
  void notifyListeners(IContainerMD* obj, IContainerMDChangeListener::Action a);

  //----------------------------------------------------------------------------
  //! Get container bucket which is computed as the id of the container modulo
  //! the number of container buckets.
  //!
  //! @param id container id
  //!
  //! @return container bucket key
  //----------------------------------------------------------------------------
  std::string getBucketKey(IContainerMD::id_t id) const;

  static std::uint64_t sNumContBuckets; ///< Number of buckets power of 2
  ListenerList pListeners;   ///< List of listeners to be notified
  IQuotaStats* pQuotaStats;  ///< Quota view
  IFileMDSvc* pFileSvc;      ///< File metadata service
  qclient::QClient* pQcl;    ///< QClient object
  qclient::QHash mMetaMap ;  ///< Map holding metainfo about the namespace
  NextInodeProvider inodeProvider; ///< provides next free inode
  std::string pBkndHost;     ///< Backend host
  uint32_t pBkndPort;        ///< Backend port
  LRU<IContainerMD::id_t, IContainerMD> mContainerCache;
  // TODO: decide on how to ensure container consistency in case of a crash
  qclient::QSet pCheckConts; ///< Set of container idsd to be checked
};

EOSNSNAMESPACE_END

#endif // __EOS_CONTAINER_MD_SVC_HH__
