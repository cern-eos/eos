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
//! @brief Container metadata service based on QuarkDB
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_MD_SVC_HH
#define EOS_NS_CONTAINER_MD_SVC_HH

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include "namespace/ns_quarkdb/persistency/UnifiedInodeProvider.hh"
#include "qclient/structures/QHash.hh"
#include <list>
#include <map>

namespace qclient {
  class QClient;
}

EOSNSNAMESPACE_BEGIN

class QuarkContainerMD;
class MetadataProvider;

//------------------------------------------------------------------------------
//! Container metadata service based on Redis
//------------------------------------------------------------------------------
class QuarkContainerMDSvc : public IContainerMDSvc
{
  friend QuarkContainerMD;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkContainerMDSvc(qclient::QClient *qcl, MetadataFlusher *flusher);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkContainerMDSvc();

  //----------------------------------------------------------------------------
  //! Initialize the container service
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
  //! Asynchronously get the container metadata information for the given ID
  //----------------------------------------------------------------------------
  virtual folly::Future<IContainerMDPtr> getContainerMDFut(
    IContainerMD::id_t id) override;

  //----------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID
  //----------------------------------------------------------------------------
  virtual IContainerMDPtr
  getContainerMD(IContainerMD::id_t id) override
  {
    return getContainerMD(id, 0);
  }

  //----------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID and read lock it
  //----------------------------------------------------------------------------
  virtual MDLocking::ContainerReadLockPtr
  getContainerMDReadLocked(IContainerMD::id_t id) override {
    return MDLocking::readLock(getContainerMD(id,0));
 }

 //----------------------------------------------------------------------------
 //! Get the container metadata information for the given container ID and write lock it
 //----------------------------------------------------------------------------
 virtual MDLocking::ContainerWriteLockPtr
 getContainerMDWriteLocked(IContainerMD::id_t id) override {
   return MDLocking::writeLock(getContainerMD(id,0));
 }

  //------------------------------------------------------------------------
  //! Get the container metadata information for the given ID and clock
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  getContainerMD(IContainerMD::id_t id, uint64_t* clock) override;

  //----------------------------------------------------------------------------
  //! Drop cached ContainerMD - return true if found
  //----------------------------------------------------------------------------
  virtual bool
  dropCachedContainerMD(ContainerIdentifier id) override;

  //----------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer(IContainerMD::id_t id) override;

  //----------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj) override;

  //----------------------------------------------------------------------------
  //! Remove object from the store, write lock the container before calling this!
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
  //! Get the orphans / name conflicts container
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
  //! Set metadata provider
  //----------------------------------------------------------------------------
  void
  setMetadataProvider(MetadataProvider* provider)
  {
    mMetadataProvider = provider;
  }

  //----------------------------------------------------------------------------
  //! Set inode provider
  //----------------------------------------------------------------------------
  void
  setInodeProvider(UnifiedInodeProvider* provider)
  {
    mUnifiedInodeProvider = provider;
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

  //----------------------------------------------------------------------------
  //! Retrieve MD cache statistics.
  //----------------------------------------------------------------------------
  virtual CacheStatistics getCacheStatistics() override;

  //----------------------------------------------------------------------------
  //! Blacklist IDs below the given threshold
  //----------------------------------------------------------------------------
  virtual void blacklistBelow(ContainerIdentifier id) override;

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

  //----------------------------------------------------------------------------
  //! Safety check to make sure there are no container entries in the backend
  //! with ids bigger than the max container id. If there is any problem this
  //! will throw an eos::MDException.
  //----------------------------------------------------------------------------
  void SafetyCheck();

  ListenerList pListeners;              ///< List of listeners to be notified
  IQuotaStats* pQuotaStats;             ///< Quota view
  IFileMDSvc* pFileSvc;                 ///< File metadata service
  qclient::QClient* pQcl;               ///< QClient object
  MetadataFlusher* pFlusher;            ///< Metadata flusher object
  //! Map holding metainfo about the namespace
  qclient::QHash mMetaMap;
  MetadataProvider* mMetadataProvider;  ///< Provider namespace metadata
  UnifiedInodeProvider* mUnifiedInodeProvider; ///< Provide next free inode
  std::atomic<uint64_t> mNumConts;      ///< Total number of containers
  std::string
  mCacheNum;                ///< Temporary workaround to store cache size
};

EOSNSNAMESPACE_END

#endif // __EOS_CONTAINER_MD_SVC_HH__
