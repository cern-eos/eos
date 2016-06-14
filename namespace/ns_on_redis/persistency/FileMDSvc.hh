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
#include "namespace/ns_on_redis/LRU.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include <condition_variable>
#include <list>
#include <mutex>

//! Forward declarations
namespace redox {
class Redox;
}

EOSNSNAMESPACE_BEGIN

class IQuotaStats;

//------------------------------------------------------------------------------
//! FileMDSvc based on Redis
//------------------------------------------------------------------------------
class FileMDSvc : public IFileMDSvc {
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
  virtual void finalize(){};

  //----------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
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
  //! Remove object from the store
  //----------------------------------------------------------------------------
  virtual void removeFile(IFileMD::id_t fileId);

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
  void visit(IFileVisitor* visitor){};

  //----------------------------------------------------------------------------
  //! Check files that had errors - these are stored in a separate set in the
  //! KV store.
  //!
  //! @return true if all files are consistent, otherwise false
  //----------------------------------------------------------------------------
  bool checkFiles();

  //----------------------------------------------------------------------------
  //! Add file object to consistency check list to recover it in case of a crash
  //!
  //! @param id file object id
  //----------------------------------------------------------------------------
  void addToConsistencyCheck(IFileMD::id_t id);

private:
  typedef std::list<IFileMDChangeListener*> ListenerList;

  static std::uint64_t sNumFileBuckets; ///< Numver of buckets power of 2

  //----------------------------------------------------------------------------
  //! Recheck a file object
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

  ListenerList pListeners;
  IQuotaStats* pQuotaStats;
  IContainerMDSvc* pContSvc;
  redox::Redox* pRedox;
  std::string pRedisHost;
  uint32_t pRedisPort;
  LRU<IFileMD::id_t, IFileMD> mFileCache;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILE_MD_SVC_HH__
