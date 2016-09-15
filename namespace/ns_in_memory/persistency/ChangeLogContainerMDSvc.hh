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
#include "namespace/ns_in_memory/persistency/ChangeLogFile.hh"
#include "namespace/ns_in_memory/accounting/QuotaStats.hh"

#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <list>
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
  ChangeLogContainerMDSvc(): pFirstFreeId(0), pSlaveLock(0),
    pSlaveMode(false), pSlaveStarted(false), pSlavePoll(1000),
    pFollowStart(0), pQuotaStats(0), pFileSvc(NULL),
    pAutoRepair(0), pResSize(1000000), pContainerAccounting(0)
  {
    pIdMap.set_deleted_key(0);
    pIdMap.set_empty_key(std::numeric_limits<IContainerMD::id_t>::max());
    pChangeLog = new ChangeLogFile();
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
  virtual void initialize();

  //--------------------------------------------------------------------------
  //! Configure the container service
  //--------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config);

  //--------------------------------------------------------------------------
  //! Finalize the container service
  //--------------------------------------------------------------------------
  virtual void finalize();

  //----------------------------------------------------------------------------
  //! Set file metadata service
  //----------------------------------------------------------------------------
  virtual void setFileMDService(IFileMDSvc* file_svc)
  {
    pFileSvc = file_svc;
  }

  //--------------------------------------------------------------------------
  //! Get the container metadata information for the given container ID
  //--------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> getContainerMD(IContainerMD::id_t id);

  //--------------------------------------------------------------------------
  //! Create new container metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //--------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD> createContainer();

  //--------------------------------------------------------------------------
  //! Update the contaienr metadata in the backing store after the
  //! ContainerMD object has been changed
  //--------------------------------------------------------------------------
  virtual void updateStore(IContainerMD* obj);

  //--------------------------------------------------------------------------
  //! Remove object from the store
  //--------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD* obj);

  //--------------------------------------------------------------------------
  //! Remove object from the store
  //--------------------------------------------------------------------------
  virtual void removeContainer(IContainerMD::id_t containerId);

  //--------------------------------------------------------------------------
  //! Get number of containers
  //--------------------------------------------------------------------------
  virtual uint64_t getNumContainers()
  {
    return pIdMap.size();
  }

  //--------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //--------------------------------------------------------------------------
  virtual void addChangeListener(IContainerMDChangeListener* listener);

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
  void* compactPrepare(const std::string& newLogFileName) const;

  //--------------------------------------------------------------------------
  //! Do the compacting.
  //!
  //! This does not access any of the in-memory structures so any external
  //! metadata operations (including mutations) may happen while it is
  //! running.
  //!
  //! @param  compactingData state information returned by compactPrepare
  //--------------------------------------------------------------------------
  void compact(void*& compactingData);

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
  void compactCommit(void* compactingData, bool autorepair = false);

  //--------------------------------------------------------------------------
  //! Make a transition from slave to master
  // -----------------------------------------------------------------------
  virtual void slave2Master(std::map<std::string, std::string>& config);

  //--------------------------------------------------------------------------
  //! Switch the namespace to read-only mode
  //--------------------------------------------------------------------------
  virtual void makeReadOnly();

  //--------------------------------------------------------------------------
  //! Register slave lock
  //--------------------------------------------------------------------------
  void setSlaveLock(LockHandler* slaveLock)
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
  void startSlave();

  //--------------------------------------------------------------------------
  //! Stop the slave mode
  //--------------------------------------------------------------------------
  void stopSlave();

  //--------------------------------------------------------------------------
  //! Create container in parent
  //--------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> createInParent(const std::string& name,
      IContainerMD* parent);

  //--------------------------------------------------------------------------
  //! Get the lost+found container, create if necessary
  //--------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFound();

  //--------------------------------------------------------------------------
  //! Get the orphans container
  //--------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> getLostFoundContainer(const std::string& name);

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
  uint64_t getFollowOffset() const
  {
    return pFollowStart;
  }

  //--------------------------------------------------------------------------
  //! Set the following offset
  //--------------------------------------------------------------------------
  void setFollowOffset(uint64_t offset)
  {
    pFollowStart = offset;
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
  void setQuotaStats(IQuotaStats* quotaStats)
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
  std::vector<std::string> getWarningMessages();

  //--------------------------------------------------------------------------
  //! Clear changelog warning messages
  //--------------------------------------------------------------------------
  void clearWarningMessages();

  //------------------------------------------------------------------------
  //! Set container accounting
  //------------------------------------------------------------------------
  void setContainerAccounting(IFileMDChangeListener* containerAccounting)
  {
    pContainerAccounting = containerAccounting;
  }

  //------------------------------------------------------------------------
  //! Get first free container id
  //------------------------------------------------------------------------
  IContainerMD::id_t getFirstFreeId() const
  {
    return pFirstFreeId;
  }

  //------------------------------------------------------------------------
  //! Resize container service map
  //------------------------------------------------------------------------
  void resize()
  {
    pIdMap.resize(0);
  }

private:
  //--------------------------------------------------------------------------
  // Placeholder for the record info
  //--------------------------------------------------------------------------
  struct DataInfo {
    DataInfo(): logOffset(0),
      ptr((IContainerMD*)0) {}
    DataInfo(uint64_t logOffset, std::shared_ptr<IContainerMD> ptr)
    {
      this->logOffset = logOffset;
      this->ptr = ptr;
    }
    uint64_t logOffset;
    std::shared_ptr<IContainerMD> ptr;
  };

  typedef google::dense_hash_map<IContainerMD::id_t, DataInfo> IdMap;
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
  void notifyListeners(IContainerMD* obj, IContainerMDChangeListener::Action a);

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
  ListenerList       pListeners;
  pthread_t          pFollowerThread;
  LockHandler*       pSlaveLock;
  bool               pSlaveMode;
  bool               pSlaveStarted;
  int32_t            pSlavePoll;
  uint64_t           pFollowStart;
  IQuotaStats*       pQuotaStats;
  IFileMDSvc*        pFileSvc;
  bool               pAutoRepair;
  uint64_t           pResSize;
  IFileMDChangeListener* pContainerAccounting;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_CHANGE_LOG_CONTAINER_MD_SVC_HH__
