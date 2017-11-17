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
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
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
  //! Get container bucket which is computed as the id of the container modulo
  //! the number of container buckets.
  //!
  //! @param id container id
  //!
  //! @return container bucket key
  //----------------------------------------------------------------------------
  static std::string getBucketKey(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Initizlize the container service
  //----------------------------------------------------------------------------
  virtual void initialize() override;

  //----------------------------------------------------------------------------
  //! Configure the container service
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config)
  override;

  //----------------------------------------------------------------------------
  //! Finalize the container service
  //----------------------------------------------------------------------------
  virtual void finalize() override {};

  //----------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  getContainerMD(IContainerMD::id_t id) override
  {
    return getContainerMD(id, 0);
  }

  //------------------------------------------------------------------------
  //! Get the container metadata information for the given ID and clock
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  getContainerMD(IContainerMD::id_t id, uint64_t* clock) override;

  //----------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer() override;

  //----------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj) override;

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj) override;

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  virtual uint64_t getNumContainers() override;

  //----------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //----------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener) override;

  //----------------------------------------------------------------------------
  //! Create container in parent
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> createInParent(const std::string& name,
      IContainerMD* parent) override;

  //----------------------------------------------------------------------------
  //! Get the lost+found container, create if necessary
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFound();

  //----------------------------------------------------------------------------
  //! Get the orphans container
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFoundContainer(const std::string& name)
  override;

  //----------------------------------------------------------------------------
  //! Set file metadata service
  //----------------------------------------------------------------------------
  void
  setFileMDService(IFileMDSvc* file_svc) override
  {
    pFileSvc = file_svc;
  }

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //----------------------------------------------------------------------------
  void
  setQuotaStats(IQuotaStats* quotaStats) override
  {
    pQuotaStats = quotaStats;
  }

  //----------------------------------------------------------------------------
  //! Get first free container id
  //----------------------------------------------------------------------------
  IContainerMD::id_t getFirstFreeId() override;

private:
  typedef std::list<IContainerMDChangeListener*> ListenerList;

  //----------------------------------------------------------------------------
  //! Set container accounting - no-op for this type of object since any type
  //! of size accounting in done when manipulating files or using the
  //! AddTree/RemoveTree interface of the continaer accounting view.
  //----------------------------------------------------------------------------
  void setContainerAccounting(IFileMDChangeListener* containerAccounting) override
  {
    return;
  }

  //----------------------------------------------------------------------------
  //! Notify the listeners about the change
  //----------------------------------------------------------------------------
  void notifyListeners(IContainerMD* obj, IContainerMDChangeListener::Action a)
  override;

  static std::uint64_t sNumContBuckets; ///< Number of buckets power of 2
  ListenerList pListeners;   ///< List of listeners to be notified
  IQuotaStats* pQuotaStats;  ///< Quota view
  IFileMDSvc* pFileSvc;      ///< File metadata service
  qclient::QClient* pQcl;    ///< QClient object
  MetadataFlusher* pFlusher; ///< Metadata flusher object
  qclient::QHash mMetaMap ;  ///< Map holding metainfo about the namespace
  NextInodeProvider mInodeProvider; ///< Provide next free inode
  LRU<IContainerMD::id_t, IContainerMD> mContainerCache;
  // TODO: decide on how to ensure container consistency in case of a crash
  qclient::QSet pCheckConts; ///< Set of container ids to be checked
};

EOSNSNAMESPACE_END

#endif // __EOS_CONTAINER_MD_SVC_HH__
