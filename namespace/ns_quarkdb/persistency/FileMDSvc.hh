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
//! @brief File MD service based on redis
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FILE_MD_SVC_HH__
#define __EOS_NS_FILE_MD_SVC_HH__

#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include <condition_variable>
#include <list>
#include <mutex>

EOSNSNAMESPACE_BEGIN

class IQuotaStats;

//------------------------------------------------------------------------------
//! FileMDSvc based on Redis
//------------------------------------------------------------------------------
class FileMDSvc : public IFileMDSvc
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileMDSvc();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileMDSvc() = default;

  //----------------------------------------------------------------------------
  //! Initizlize the file service
  //----------------------------------------------------------------------------
  virtual void initialize();

  //----------------------------------------------------------------------------
  //! Configure the file service
  //!
  //! @param config map holding configuration parameters
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config);

  //----------------------------------------------------------------------------
  //! Finalize the file service
  //----------------------------------------------------------------------------
  virtual void finalize() {};

  //----------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
  //!
  //! @param id file id
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> getFileMD(IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Create new file metadata object with an assigned id
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> createFile();

  //----------------------------------------------------------------------------
  //! Update the file metadata in the backing store after the FileMD object
  //! has been changed
  //----------------------------------------------------------------------------
  virtual void updateStore(IFileMD* obj);

  //----------------------------------------------------------------------------
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeFile(IFileMD* obj);

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  virtual uint64_t getNumFiles();

  //----------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //----------------------------------------------------------------------------
  virtual void addChangeListener(IFileMDChangeListener* listener);

  //----------------------------------------------------------------------------
  //! Notify the listeners about the change
  //----------------------------------------------------------------------------
  virtual void notifyListeners(IFileMDChangeListener::Event* event);

  //----------------------------------------------------------------------------
  //! Set container service
  //!
  //! @param cont_svc container service
  //----------------------------------------------------------------------------
  void setContMDService(IContainerMDSvc* cont_svc);

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //!
  //! @param quota_stats object implementing the IQuotaStats interface
  //----------------------------------------------------------------------------
  void setQuotaStats(IQuotaStats* quota_stats);

  //----------------------------------------------------------------------------
  //! Visit all the files
  //----------------------------------------------------------------------------
  void visit(IFileVisitor* visitor) {};

  //----------------------------------------------------------------------------
  //! Check files that had errors - these are stored in a separate set in the
  //! KV store.
  //!
  //! @return true if all files are consistent, otherwise false
  //----------------------------------------------------------------------------
  bool checkFiles();

  //----------------------------------------------------------------------------
  //! Get first free file id
  //----------------------------------------------------------------------------
  IFileMD::id_t getFirstFreeId() const
  {
    // TODO(esindril): add implementation
    return 0;
  }

private:
  typedef std::list<IFileMDChangeListener*> ListenerList;

  static std::uint64_t sNumFileBuckets; ///< Number of buckets power of 2
  //! Interval for backend flush of consistent file ids
  static std::chrono::seconds sFlushInterval;

  //----------------------------------------------------------------------------
  //! Check file object consistency
  //!
  //! @param fid file id
  //!
  //! @return true if file info is consistent, otherwise false
  //----------------------------------------------------------------------------
  bool checkFile(std::uint64_t fid);

  //----------------------------------------------------------------------------
  //! Attach a broken file to lost+found
  //! TODO: review this
  //----------------------------------------------------------------------------
  void attachBroken(const std::string& parent, IFileMD* file);

  //----------------------------------------------------------------------------
  //! Get file bucket which is computed as the id of the container  modulo the
  //! number of file buckets.
  //!
  //! @param id file id
  //!
  //! @return file bucket key
  //----------------------------------------------------------------------------
  std::string getBucketKey(IContainerMD::id_t id) const;

  //----------------------------------------------------------------------------
  //! Add file to consistency check list to recover it in case of a crash
  //!
  //! @param file IFileMD object
  //----------------------------------------------------------------------------
  void addToDirtySet(IFileMD* file);

  //----------------------------------------------------------------------------
  //! Remove all accumulated objects from the local "dirty" set and mark them
  //! in the backend set accordingly.
  //!
  //! @param id file id
  //! @param force if true then force flush
  //----------------------------------------------------------------------------
  void flushDirtySet(IFileMD::id_t id, bool force = false);

  ListenerList pListeners; ///< List of listeners to notify of changes
  IQuotaStats* pQuotaStats; ///< Quota view
  IContainerMDSvc* pContSvc; ///< Container metadata service
  std::time_t mFlushTimestamp; ///< Timestamp of the last dirty set flush
  uint32_t pBkendPort; ///< Backend instance port
  std::string pBkendHost; ///< Backend intance host
  qclient::QClient* pQcl; ///< QClient object
  qclient::QHash mMetaMap ; ///< Map holding metainfo about the namespace
  qclient::QSet mDirtyFidBackend; ///< Set of "dirty" files
  std::set<std::string> mFlushFidSet; ///< Modified fids which are consistent
  LRU<IFileMD::id_t, IFileMD> mFileCache; ///< Local cache of file objects
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILE_MD_SVC_HH__
