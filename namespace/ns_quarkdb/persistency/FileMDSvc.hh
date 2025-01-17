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
//! @brief File MD service based on quarkdb
//------------------------------------------------------------------------------

#ifndef EOS_NS_FILE_MD_SVC_HH
#define EOS_NS_FILE_MD_SVC_HH

#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include "namespace/ns_quarkdb/persistency/UnifiedInodeProvider.hh"
#include "qclient/structures/QHash.hh"

EOSNSNAMESPACE_BEGIN

class IQuotaStats;
class MetadataFlusher;
class MetadataProvider;

//------------------------------------------------------------------------------
//! FileMDSvc based on Redis
//------------------------------------------------------------------------------
class QuarkFileMDSvc : public IFileMDSvc
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkFileMDSvc(qclient::QClient *qcl, MetadataFlusher *flusher);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkFileMDSvc();

  //----------------------------------------------------------------------------
  //! Initialize the file service
  //----------------------------------------------------------------------------
  virtual void initialize() override;

  //----------------------------------------------------------------------------
  //! Configure the file service
  //!
  //! @param config map holding configuration parameters
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config)
  override;

  //----------------------------------------------------------------------------
  //! Finalize the file service
  //----------------------------------------------------------------------------
  virtual void finalize() override {};

  //----------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID - asynchronous
  //! API.
  //----------------------------------------------------------------------------
  virtual folly::Future<IFileMDPtr> getFileMDFut(IFileMD::id_t id) override;

  //----------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
  //!
  //! @param id file id
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> getFileMD(IFileMD::id_t id) override
  {
    return getFileMD(id, 0);
  }

  //----------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID and clock
  //! value
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> getFileMD(IFileMD::id_t id,
      uint64_t* clock) override;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID and read lock it
  //! throws MD exception in case the file does not exist
  //------------------------------------------------------------------------
  virtual MDLocking::FileReadLockPtr
  getFileMDReadLocked(IFileMD::id_t id) override;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID and write lock it
  //------------------------------------------------------------------------
  virtual MDLocking::FileWriteLockPtr
  getFileMDWriteLocked(IFileMD::id_t id) override;

  //----------------------------------------------------------------------------
  //! Check if a FileMD with a given identifier exists
  //----------------------------------------------------------------------------
  virtual folly::Future<bool> hasFileMD(const eos::FileIdentifier id) override;

  //----------------------------------------------------------------------------
  //! Drop cached FileMD - return true if found
  //----------------------------------------------------------------------------
  virtual bool dropCachedFileMD(FileIdentifier id) override;

  //----------------------------------------------------------------------------
  //! Create new file metadata object with an assigned id
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> createFile(IFileMD::id_t id = 0) override;

  //----------------------------------------------------------------------------
  //! Update the file metadata in the backing store after the FileMD object
  //! has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IFileMD* obj) override;

  //------------------------------------------------------------------------
  //! Remove object from the store, you should write lock the file before calling this
  //------------------------------------------------------------------------
  virtual void removeFile(IFileMD* obj) override;

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  virtual uint64_t getNumFiles() override;

  //----------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //----------------------------------------------------------------------------
  virtual void addChangeListener(IFileMDChangeListener* listener) override;

  //----------------------------------------------------------------------------
  //! Notify the listeners about the change
  //----------------------------------------------------------------------------
  virtual void notifyListeners(IFileMDChangeListener::Event* event) override;

  //----------------------------------------------------------------------------
  //! Set container service
  //!
  //! @param cont_svc container service
  //----------------------------------------------------------------------------
  void setContMDService(IContainerMDSvc* cont_svc) override;

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //!
  //! @param quota_stats object implementing the IQuotaStats interface
  //----------------------------------------------------------------------------
  void setQuotaStats(IQuotaStats* quota_stats) override
  {
    pQuotaStats = quota_stats;
  }

  //----------------------------------------------------------------------------
  //! Visit all the files
  //----------------------------------------------------------------------------
  void visit(IFileVisitor* visitor) override {};

  //----------------------------------------------------------------------------
  //! Get first free file id
  //----------------------------------------------------------------------------
  IFileMD::id_t getFirstFreeId() override;

  //----------------------------------------------------------------------------
  //! Retrieve MD cache statistics.
  //----------------------------------------------------------------------------
  virtual CacheStatistics getCacheStatistics() override;

  //----------------------------------------------------------------------------
  //! Blacklist IDs below the given threshold
  //----------------------------------------------------------------------------
  virtual void blacklistBelow(FileIdentifier id) override;

  //----------------------------------------------------------------------------
  //! Get pointer to metadata provider
  //----------------------------------------------------------------------------
  MetadataProvider* getMetadataProvider();


private:
  typedef std::list<IFileMDChangeListener*> ListenerList;
  //! Interval for backend flush of consistent file ids
  static std::chrono::seconds sFlushInterval;

 //----------------------------------------------------------------------------
  //! Safety check to make sure there are nofile entries in the backend with
  //! ids bigger than the max file id. If there is any problem this will throw
  //! an eos::MDException.
  //----------------------------------------------------------------------------
  void SafetyCheck();

  ListenerList pListeners; ///< List of listeners to notify of changes
  IQuotaStats* pQuotaStats; ///< Quota view
  IContainerMDSvc* pContSvc; ///< Container metadata service
  MetadataFlusher* pFlusher = nullptr; ///< Metadata flusher object
  qclient::QClient* pQcl; ///< QClient object
  qclient::QHash mMetaMap ; ///< Map holding metainfo about the namespace
  std::atomic<uint64_t> mNumFiles; ///< Total number of fileso
  std::unique_ptr<MetadataProvider>
  mMetadataProvider; ///< Provides metadata from backend
  UnifiedInodeProvider mUnifiedInodeProvider; ///< Provides next free inode
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILE_MD_SVC_HH__
