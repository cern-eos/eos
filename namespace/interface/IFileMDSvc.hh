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
// desc:   FileMD interface
//------------------------------------------------------------------------------

#ifndef EOS_NS_I_FILE_MD_SVC_HH
#define EOS_NS_I_FILE_MD_SVC_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/Identifiers.hh"
#include "namespace/interface/Misc.hh"
#include "namespace/MDLocking.hh"
#include <folly/futures/Future.h>
#include <map>
#include <string>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class IContainerMDSvc;
class IQuotaStats;

struct TreeInfos {
  int64_t dsize; //Tree size to update (delta)
  int64_t dtreefiles; //Tree files to update (delta)
  int64_t dtreecontainers; //Tree containers to update (delta)

  TreeInfos():dsize(0),dtreefiles(0),dtreecontainers(0) {}
  TreeInfos(int64_t dtreeSize, int64_t dtreeFiles, int64_t dtreeContainers):dsize(dtreeSize),dtreefiles(dtreeFiles),dtreecontainers(dtreeContainers){}

  TreeInfos operator-() const {
    return {-dsize,-dtreefiles,-dtreecontainers};
  }

  TreeInfos & operator+=(const TreeInfos & treeInfos) {
    dsize += treeInfos.dsize;
    dtreefiles += treeInfos.dtreefiles;
    dtreecontainers += treeInfos.dtreecontainers;
    return *this;
  }
};

//------------------------------------------------------------------------------
//! Interface for a listener that is notified about all of the
//! actions performed in a IFileMDSvc
//------------------------------------------------------------------------------
class IFileMDChangeListener
{
public:
  enum Action {
    Updated = 0,
    Deleted,
    Created,
    LocationAdded,
    LocationUnlinked,
    LocationRemoved,
    SizeChange
  };

  //---------------------------------------------------------------------------
  //! Event sent to the listener
  //----------------------------------------------------------------------------
  struct Event {
    Event(IFileMD* _file, Action _action,
          IFileMD::location_t _location = 0,
          TreeInfos _changed_tree = {0,0,0}):
      file(_file),
      action(_action),
      treeChange(_changed_tree),
      location(_location) {}

    IFileMD*             file;
    Action               action;
    TreeInfos            treeChange;
    IFileMD::location_t  location;

  };

  virtual ~IFileMDChangeListener() {}
  virtual void fileMDChanged(Event* event) = 0;
  virtual void fileMDRead(IFileMD* obj) = 0;
  virtual bool fileMDCheck(IFileMD* obj) = 0;
  virtual void AddTree(IContainerMD* obj , TreeInfos treeInfos) = 0;
  virtual void RemoveTree(IContainerMD* obj , TreeInfos treeInfos) = 0;
};

//------------------------------------------------------------------------------
//! Interface for a file visitor
//------------------------------------------------------------------------------
class IFileVisitor
{
public:
  virtual void visitFile(IFileMD* file) = 0;
};

//----------------------------------------------------------------------------
//! Interface for class responsible for managing the metadata information
//! concerning files. It is responsible for assigning file IDs and managing
//! storage of the metadata. Could be implemented as a change log or db based
//! store or as an interface to memcached or some other caching system or
//! key value store
//----------------------------------------------------------------------------
class IFileMDSvc
{
public:
  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  virtual ~IFileMDSvc() {}

  //------------------------------------------------------------------------
  //! Initialize the file service
  //------------------------------------------------------------------------
  virtual void initialize() = 0;

  //------------------------------------------------------------------------
  //! Configure the file service
  //------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config) = 0;

  //------------------------------------------------------------------------
  //! Finalize the file service
  //------------------------------------------------------------------------
  virtual void finalize() = 0;

  //------------------------------------------------------------------------
  //! Asynchronously get the file metadata information for the given file ID
  //------------------------------------------------------------------------
  virtual folly::Future<IFileMDPtr> getFileMDFut(IFileMD::id_t id) = 0;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> getFileMD(IFileMD::id_t id) = 0;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID and clock
  //! value
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> getFileMD(IFileMD::id_t id,
      uint64_t* clock) = 0;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID and read lock it
  //------------------------------------------------------------------------
  virtual MDLocking::FileReadLockPtr getFileMDReadLocked(IFileMD::id_t id) = 0;

  //------------------------------------------------------------------------
  //! Get the file metadata information for the given file ID and write lock it
  //------------------------------------------------------------------------
  virtual MDLocking::FileWriteLockPtr getFileMDWriteLocked(IFileMD::id_t id) = 0;

  //----------------------------------------------------------------------------
  //! Check if a FileMD with a given identifier exists - no caching
  //----------------------------------------------------------------------------
  virtual folly::Future<bool> hasFileMD(const eos::FileIdentifier id) = 0;

  //----------------------------------------------------------------------------
  //! Drop cached FileMD - return true if found
  //----------------------------------------------------------------------------
  virtual bool
  dropCachedFileMD(FileIdentifier id) = 0;

  //------------------------------------------------------------------------
  //! Create new file metadata object with an assigned id, the user has
  //! to fill all the remaining fields
  //------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> createFile(IFileMD::id_t id) = 0;

  //------------------------------------------------------------------------
  //! Update the file metadata in the backing store after the IFileMD object
  //! has been changed
  //------------------------------------------------------------------------
  virtual void updateStore(IFileMD* obj) = 0;

  //------------------------------------------------------------------------
  //! Remove object from the store
  //------------------------------------------------------------------------
  virtual void removeFile(IFileMD* obj) = 0;

  //------------------------------------------------------------------------
  //! Get number of files
  //------------------------------------------------------------------------
  virtual uint64_t getNumFiles() = 0;

  //------------------------------------------------------------------------
  //! Add file listener that will be notified about all of the changes in
  //! the store
  //------------------------------------------------------------------------
  virtual void addChangeListener(IFileMDChangeListener* listener) = 0;

  //------------------------------------------------------------------------
  //! Notify the listeners about the change
  //------------------------------------------------------------------------
  virtual void notifyListeners(IFileMDChangeListener::Event* event) = 0;

  //----------------------------------------------------------------------------
  //! Set the QuotaStats object for the follower
  //!
  //! @param quota_stats object implementing the IQuotaStats interface
  //----------------------------------------------------------------------------
  virtual void setQuotaStats(IQuotaStats* quota_stats) = 0;

  //----------------------------------------------------------------------------
  //! Set container service
  //----------------------------------------------------------------------------
  virtual void setContMDService(IContainerMDSvc* cont_svc) = 0;

  //----------------------------------------------------------------------------
  //! Visit all the files
  //----------------------------------------------------------------------------
  virtual void visit(IFileVisitor* visitor) = 0;

  //----------------------------------------------------------------------------
  //! Get first free file id
  //----------------------------------------------------------------------------
  virtual IFileMD::id_t getFirstFreeId() = 0;

  //----------------------------------------------------------------------------
  //! Retrieve file metadata cache statistics
  //----------------------------------------------------------------------------
  virtual CacheStatistics getCacheStatistics() = 0;

  //----------------------------------------------------------------------------
  //! Blacklist IDs below the given threshold
  //----------------------------------------------------------------------------
  virtual void blacklistBelow(FileIdentifier id) = 0;

};

EOSNSNAMESPACE_END

#endif // EOS_NS_I_FILE_MD_SVC_HH
