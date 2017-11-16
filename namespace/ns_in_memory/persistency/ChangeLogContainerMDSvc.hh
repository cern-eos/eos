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

#ifndef __EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH__
#define __EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH__

#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IChLogContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogFile.hh"
#include "common/Murmur3.hh"
#include "common/hopscotch_map.hh"
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <list>
#include <set>
#include <map>
#include <pthread.h>
#include <limits>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class LockHandler;

//------------------------------------------------------------------------------
//! ChangeLog based container metadata service
//------------------------------------------------------------------------------
class ChangeLogContainerMDSvc:
  public IContainerMDSvc, public IChLogContainerMDSvc
{
  friend class ContainerMDFollower;
  friend class FileMDFollower;
  friend class ConvertContainerMDSvc;

public:

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  ChangeLogContainerMDSvc():
    pFirstFreeId(1), pFollowerThread(0), pSlaveLock(0), pSlaveMode(false),
    pSlaveStarted(false), pSlavePoll(1000), pFollowStart(0), pQuotaStats(0),
    pFileSvc(NULL), pAutoRepair(0), pResSize(1000000), pContainerAccounting(0)
  {
    pChangeLog = new ChangeLogFile();
    pthread_mutex_init(&pFollowStartMutex, 0);
  }

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~ChangeLogContainerMDSvc()
  {
    delete pChangeLog;
  }

  //--------------------------------------------------------------------------
  //! Initizlize the container service
  //--------------------------------------------------------------------------
  virtual void initialize() override;

  //--------------------------------------------------------------------------
  //! Configure the container service
  //--------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config)
  override;

  //--------------------------------------------------------------------------
  //! Finalize the container service
  //--------------------------------------------------------------------------
  virtual void finalize() override;

  //----------------------------------------------------------------------------
  //! Set file metadata service
  //----------------------------------------------------------------------------
  virtual void setFileMDService(IFileMDSvc* file_svc) override
  {
    pFileSvc = file_svc;
  }

  //--------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID
  //--------------------------------------------------------------------------
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

  //--------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //--------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer() override;

  //--------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //--------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj) override;

  //--------------------------------------------------------------------------
  //! Remove object from the store
  //--------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj) override;

  //--------------------------------------------------------------------------
  //! Remove object from the store
  //--------------------------------------------------------------------------
  void removeContainer(IContainerMD::id_t containerId);

  //--------------------------------------------------------------------------
  //! Get number of containers
  //--------------------------------------------------------------------------
  virtual uint64_t getNumContainers() override
  {
    return pIdMap.size();
  }

  //--------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //--------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener) override;

  //--------------------------------------------------------------------------
  //! Prepare for online compacting.
  //!
  //! No external file metadata mutation may occur while the method is
  //! running.
  //!
  //! @param  newLogFileName name for the compacted log file
  //! @return                compacting information that needs to be passed
  //!                        to other functions
  //--------------------------------------------------------------------------
  void* compactPrepare(const std::string& newLogFileName) override;

  //--------------------------------------------------------------------------
  //! Do the compacting.
  //!
  //! This does not access any of the in-memory structures so any external
  //! metadata operations (including mutations) may happen while it is
  //! running.
  //!
  //! @param  compactingData state information returned by compactPrepare
  //--------------------------------------------------------------------------
  void compact(void*& compactingData) override;

  //--------------------------------------------------------------------------
  //! Commit the compacting infomrmation.
  //!
  //! Updates the metadata structures. Needs an exclusive lock on the
  //! namespace. After successfull completion the new compacted
  //! log will be used for all the new data
  //!
  //! @param compactingData state information obtained from CompactPrepare
  //!                       and modified by Compact
  //! @param autorepair     indicates to skip broken records
  //--------------------------------------------------------------------------
  void compactCommit(void* compactingData, bool autorepair = false) override;

  //--------------------------------------------------------------------------
  //! Make a transition from slave to master
  // -----------------------------------------------------------------------
  virtual void slave2Master(std::map<std::string, std::string>& config) override;

  //--------------------------------------------------------------------------
  //! Switch the namespace to read-only mode
  //--------------------------------------------------------------------------
  virtual void makeReadOnly() override;

  //--------------------------------------------------------------------------
  //! Register slave lock
  //--------------------------------------------------------------------------
  void setSlaveLock(LockHandler* slaveLock) override
  {
    pSlaveLock = slaveLock;
  }

  //--------------------------------------------------------------------------
  //! Get slave lock
  //--------------------------------------------------------------------------
  LockHandler* getSlaveLock()
  {
    return pSlaveLock;
  }

  //--------------------------------------------------------------------------
  //! get slave mode
  //--------------------------------------------------------------------------
  bool getSlaveMode()
  {
    return pSlaveMode;
  }

  //--------------------------------------------------------------------------
  //! Start the slave
  //--------------------------------------------------------------------------
  void startSlave() override;

  //--------------------------------------------------------------------------
  //! Stop the slave mode
  //--------------------------------------------------------------------------
  void stopSlave() override;

  //--------------------------------------------------------------------------
  //! Create container in parent
  //--------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> createInParent(const std::string& name,
      IContainerMD* parent) override;

  //--------------------------------------------------------------------------
  //! Get the lost+found container, create if necessary
  //--------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFound();

  //--------------------------------------------------------------------------
  //! Get the orphans container
  //--------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFoundContainer(const std::string& name)
  override;

  //--------------------------------------------------------------------------
  //! Get the change log
  //--------------------------------------------------------------------------
  ChangeLogFile* getChangeLog()
  {
    return pChangeLog;
  }

  //--------------------------------------------------------------------------
  //! Get the following offset
  //--------------------------------------------------------------------------
  uint64_t getFollowOffset() override
  {
    uint64_t lFollowStart;
    pthread_mutex_lock(&pFollowStartMutex);
    lFollowStart = pFollowStart;
    pthread_mutex_unlock(&pFollowStartMutex);
    return lFollowStart;
  }

  //--------------------------------------------------------------------------
  //! Set the following offset
  //--------------------------------------------------------------------------
  void setFollowOffset(uint64_t offset)
  {
    pthread_mutex_lock(&pFollowStartMutex);
    pFollowStart = offset;
    pthread_mutex_unlock(&pFollowStartMutex);
  }

  //--------------------------------------------------------------------------
  //! Get the following poll interval
  //--------------------------------------------------------------------------
  uint64_t getFollowPollInterval() const
  {
    return pSlavePoll;
  }

  //--------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //--------------------------------------------------------------------------
  void setQuotaStats(IQuotaStats* quotaStats) override
  {
    pQuotaStats = quotaStats;
  }

  //------------------------------------------------------------------------
  //! Get id map reservation size
  //------------------------------------------------------------------------
  uint64_t getResSize() const
  {
    return pResSize;
  }

  //--------------------------------------------------------------------------
  //! Get changelog warning messages
  //!
  //! @return vector of warning messages
  //--------------------------------------------------------------------------
  std::vector<std::string> getWarningMessages() override;

  //--------------------------------------------------------------------------
  //! Clear changelog warning messages
  //--------------------------------------------------------------------------
  void clearWarningMessages() override;

  //------------------------------------------------------------------------
  //! Set container accounting
  //------------------------------------------------------------------------
  void setContainerAccounting(IFileMDChangeListener* containerAccounting)
  override
  {
    pContainerAccounting = containerAccounting;
  }

  //------------------------------------------------------------------------
  //! Get first free container id
  //------------------------------------------------------------------------
  IContainerMD::id_t getFirstFreeId() override
  {
    return pFirstFreeId;
  }

  //------------------------------------------------------------------------
  //! Resize container service map
  //------------------------------------------------------------------------
  void resize() override
  {
    return;
  }

private:
  //--------------------------------------------------------------------------
  // Placeholder for the record info
  //--------------------------------------------------------------------------
  struct DataInfo {
    DataInfo(): logOffset(0), ptr((IContainerMD*)0), attached(false) {}
    DataInfo(uint64_t log_offset, std::shared_ptr<IContainerMD> ptr):
      logOffset(log_offset), ptr(ptr), attached(false) {}

    uint64_t logOffset;
    std::shared_ptr<IContainerMD> ptr;
    bool attached;
  };

  typedef tsl::hopscotch_map <
  IFileMD::id_t, DataInfo,
          Murmur3::MurmurHasher<uint64_t>, Murmur3::eqstr > IdMap;
  typedef std::set<IContainerMD::id_t> DeletionSet;
  typedef std::list<IContainerMDChangeListener*> ListenerList;
  typedef std::list<std::shared_ptr<IContainerMD>> ContainerList;

  //--------------------------------------------------------------------------
  // Changelog record scanner
  //--------------------------------------------------------------------------
  class ContainerMDScanner: public ILogRecordScanner
  {
  public:
    ContainerMDScanner(IdMap& idMap, bool slaveMode):
      pIdMap(idMap), pLargestId(0), pSlaveMode(slaveMode)
    {}
    virtual bool processRecord(uint64_t offset, char type,
                               const Buffer& buffer);
    IContainerMD::id_t getLargestId() const
    {
      return pLargestId;
    }
  private:
    IdMap& pIdMap;
    IContainerMD::id_t pLargestId;
    bool pSlaveMode;
  };

  //--------------------------------------------------------------------------
  //! Notify the listeners about the change
  //--------------------------------------------------------------------------
  void notifyListeners(IContainerMD* obj, IContainerMDChangeListener::Action a)
  override;

  //--------------------------------------------------------------------------
  //! Load the container
  //--------------------------------------------------------------------------
  virtual void loadContainer(IdMap::iterator& it);

  //--------------------------------------------------------------------------
  // Recreate the container structure recursively and create the list
  // of orphans and name conflicts
  //--------------------------------------------------------------------------
  virtual void recreateContainer(IdMap::iterator& it,
                                 ContainerList&   orphans,
                                 ContainerList&   nameConflicts);

  //--------------------------------------------------------------------------
  // Attach broken containers to lost+found
  //--------------------------------------------------------------------------
  void attachBroken(IContainerMD* parent, ContainerList& broken);

  //--------------------------------------------------------------------------
  // Data members
  //--------------------------------------------------------------------------
  IContainerMD::id_t pFirstFreeId;
  std::string        pChangeLogPath;
  ChangeLogFile*     pChangeLog;
  IdMap              pIdMap;
  DeletionSet        pFollowerDeletions;
  ListenerList       pListeners;
  pthread_t          pFollowerThread;
  LockHandler*       pSlaveLock;
  bool               pSlaveMode;
  bool               pSlaveStarted;
  int32_t            pSlavePoll;
  pthread_mutex_t    pFollowStartMutex;
  uint64_t           pFollowStart;
  IQuotaStats*       pQuotaStats;
  IFileMDSvc*        pFileSvc;
  bool               pAutoRepair;
  uint64_t           pResSize;
  IFileMDChangeListener* pContainerAccounting;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH__
