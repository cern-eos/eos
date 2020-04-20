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
//! @author Elvin Sindrilaru <esindril@cern.ch>
//! @brief  Class representing the container interface
//------------------------------------------------------------------------------

#ifndef EOS_NS_ICONTAINER_MD_HH
#define EOS_NS_ICONTAINER_MD_HH

#include "common/SharedMutexWrapper.hh"
#include "namespace/Namespace.hh"
#include "namespace/utils/Buffer.hh"
#include "namespace/utils/LocalityHint.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/Identifiers.hh"
#include "common/Murmur3.hh"
#include <stdint.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <map>
#include <set>
#include <chrono>
#include <sys/time.h>
#include <google/dense_hash_map>
#include <folly/futures/Future.h>
#include <folly/concurrency/ConcurrentHashMap.h>

EOSNSNAMESPACE_BEGIN

//! Forward declarations
class IContainerMDSvc;
class IFileMDSvc;
class IFileMD;
class IContainerMD;
class FileMapIterator;
class ContainerMapIterator;

using IContainerMDPtr = std::shared_ptr<IContainerMD>;
using IFileMDPtr = std::shared_ptr<IFileMD>;

//------------------------------------------------------------------------------
//! Holds either a FileMD or a ContainerMD. Only one of these are ever filled,
//! the other will be nullptr. Both might be nullptr as well.
//------------------------------------------------------------------------------
struct FileOrContainerMD {
  IFileMDPtr file;
  IContainerMDPtr container;
};

//------------------------------------------------------------------------------
//! Class holding the interface to the metadata information concerning a
//! single container
//------------------------------------------------------------------------------
class IContainerMD
{
public:
  //----------------------------------------------------------------------------
  //! Type definitions
  //----------------------------------------------------------------------------
  typedef uint64_t id_t;
  typedef struct timespec ctime_t;
  typedef struct timespec mtime_t;
  typedef struct timespec tmtime_t;
  typedef std::map<std::string, std::string> XAttrMap;

  using ContainerMap = folly::ConcurrentHashMap<std::string, IContainerMD::id_t>;
  using FileMap = folly::ConcurrentHashMap<std::string, IContainerMD::id_t>;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IContainerMD(): mIsDeleted(false) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IContainerMD() {}

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual IContainerMD* clone() const = 0;

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual void InheritChildren(const IContainerMD& other) = 0;

  //----------------------------------------------------------------------------
  //! Add container
  //----------------------------------------------------------------------------
  virtual void addContainer(IContainerMD* container) = 0;

  //----------------------------------------------------------------------------
  //! Remove container
  //----------------------------------------------------------------------------
  virtual void removeContainer(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find sub container, asynchronous API
  //----------------------------------------------------------------------------
  virtual folly::Future<IContainerMDPtr>
  findContainerFut(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find sub container
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IContainerMD>
  findContainer(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  virtual size_t getNumContainers() = 0;

  //----------------------------------------------------------------------------
  //! Add file
  //----------------------------------------------------------------------------
  virtual void addFile(IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Remove file
  //----------------------------------------------------------------------------
  virtual void removeFile(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find file, asynchronous API
  //----------------------------------------------------------------------------
  virtual folly::Future<IFileMDPtr> findFileFut(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find file
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> findFile(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find item
  //----------------------------------------------------------------------------
  virtual folly::Future<FileOrContainerMD> findItem(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  virtual size_t getNumFiles() = 0;

  //----------------------------------------------------------------------------
  //! Get name
  //----------------------------------------------------------------------------
  virtual const std::string& getName() const = 0;

  //----------------------------------------------------------------------------
  //! Set name
  //----------------------------------------------------------------------------
  virtual void setName(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Get container id
  //----------------------------------------------------------------------------
  virtual IContainerMD::id_t getId() const = 0;

  //----------------------------------------------------------------------------
  //! Get container identifier
  //----------------------------------------------------------------------------
  virtual ContainerIdentifier getIdentifier() const = 0;

  //----------------------------------------------------------------------------
  //! Get parent id
  //----------------------------------------------------------------------------
  virtual IContainerMD::id_t getParentId() const = 0;

  //----------------------------------------------------------------------------
  //! Get parent identifier
  //----------------------------------------------------------------------------
  virtual ContainerIdentifier getParentIdentifier() const
  {
    return ContainerIdentifier(getParentId());
  }

  //----------------------------------------------------------------------------
  //! Set parent id
  //----------------------------------------------------------------------------
  virtual void setParentId(IContainerMD::id_t parentId) = 0;

  //----------------------------------------------------------------------------
  //! Get the flags
  //----------------------------------------------------------------------------
  virtual uint16_t getFlags() const = 0;

  //----------------------------------------------------------------------------
  //! Set flags
  //----------------------------------------------------------------------------
  virtual void setFlags(uint16_t flags) = 0;

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  virtual void setMTime(mtime_t mtime) = 0;

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  virtual void setMTimeNow() = 0;

  //----------------------------------------------------------------------------
  //! Trigger an mtime change event
  //----------------------------------------------------------------------------
  virtual void notifyMTimeChange(IContainerMDSvc* containerMDSvc) = 0;

  //----------------------------------------------------------------------------
  //! Get modification time
  //----------------------------------------------------------------------------
  virtual void getMTime(mtime_t& mtime) const  = 0;

  //----------------------------------------------------------------------------
  //! Set propagated modification time (if newer)
  //----------------------------------------------------------------------------
  virtual bool setTMTime(tmtime_t tmtime) = 0;

  //----------------------------------------------------------------------------
  //! Set propagated modification time to now
  //----------------------------------------------------------------------------
  virtual void setTMTimeNow() = 0;

  //----------------------------------------------------------------------------
  //! Get creation time
  //----------------------------------------------------------------------------
  virtual void getTMTime(tmtime_t& tmtime) = 0;

  //----------------------------------------------------------------------------
  //! Get tree size
  //----------------------------------------------------------------------------
  virtual uint64_t getTreeSize() const = 0;

  //----------------------------------------------------------------------------
  //! Set tree size
  //----------------------------------------------------------------------------
  virtual void setTreeSize(uint64_t treesize) = 0;

  //----------------------------------------------------------------------------
  //! Update tree size
  //!
  //! @param delta can be negative or positive
  //----------------------------------------------------------------------------
  virtual uint64_t updateTreeSize(int64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Get creation time
  //----------------------------------------------------------------------------
  virtual void getCTime(ctime_t& ctime) const = 0;

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  virtual void setCTime(ctime_t ctime) = 0;

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  virtual void setCTimeNow() = 0;

  //----------------------------------------------------------------------------
  //! Get uid
  //----------------------------------------------------------------------------
  virtual uid_t getCUid() const = 0;

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  virtual void setCUid(uid_t uid) = 0;

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  virtual gid_t getCGid() const = 0;

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  virtual void setCGid(gid_t gid) = 0;

  //----------------------------------------------------------------------------
  //! Get cloneId
  //----------------------------------------------------------------------------
  virtual time_t getCloneId() const = 0;

  //----------------------------------------------------------------------------
  //! Set cloneId
  //----------------------------------------------------------------------------
  virtual void setCloneId(time_t id) = 0;

  //----------------------------------------------------------------------------
  //! Get cloneFST
  //----------------------------------------------------------------------------
  virtual const std::string getCloneFST() const = 0;

  //----------------------------------------------------------------------------
  //! Set set cloneFST
  //----------------------------------------------------------------------------
  virtual void setCloneFST(const std::string& data) = 0;

  //----------------------------------------------------------------------------
  //! Get mode
  //----------------------------------------------------------------------------
  virtual mode_t getMode() const = 0;

  //----------------------------------------------------------------------------
  //! Set mode
  //----------------------------------------------------------------------------
  virtual void setMode(mode_t mode) = 0;

  //----------------------------------------------------------------------------
  //! Get the attribute
  //----------------------------------------------------------------------------
  virtual std::string getAttribute(const std::string& name) const = 0;

  //----------------------------------------------------------------------------
  //! Add extended attribute
  //----------------------------------------------------------------------------
  virtual void setAttribute(const std::string& name,
                            const std::string& value) = 0;

  //----------------------------------------------------------------------------
  //! Remove attribute
  //----------------------------------------------------------------------------
  virtual void removeAttribute(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Check if the attribute exist
  //----------------------------------------------------------------------------
  virtual bool hasAttribute(const std::string& name) const = 0;

  //----------------------------------------------------------------------------
  //! Return number of attributes
  //----------------------------------------------------------------------------
  virtual size_t numAttributes() const = 0;

  //----------------------------------------------------------------------------
  //! Get map copy of the extended attributes
  //----------------------------------------------------------------------------
  virtual XAttrMap getAttributes() const = 0;

  //----------------------------------------------------------------------------
  //! Check the access permissions
  //!
  //! @return true if all the requested rights are granted, false otherwise
  //----------------------------------------------------------------------------
  virtual bool access(uid_t uid, gid_t gid, int flags = 0) = 0;

  //----------------------------------------------------------------------------
  //! Serialize the object to a buffer
  //----------------------------------------------------------------------------
  virtual void serialize(Buffer& buffer) = 0;

  //----------------------------------------------------------------------------
  //! Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  virtual void deserialize(Buffer& buffer) = 0;

  //----------------------------------------------------------------------------
  //! Get value tracking changes to the metadata object
  //----------------------------------------------------------------------------
  virtual uint64_t getClock() const
  {
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Get env representation of the container object
  //!
  //! @param env string where representation is stored
  //! @param escapeAnd if true escape & with #AND# ...
  //----------------------------------------------------------------------------
  virtual void getEnv(std::string& env, bool escapeAnd = false) = 0;

  //----------------------------------------------------------------------------
  //! Check if object is "deleted" - in the sense that it's not valid anymore
  //----------------------------------------------------------------------------
  virtual bool isDeleted() const
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mIsDeleted;
  }

  //----------------------------------------------------------------------------
  //! Set file as "deleted" - in the sense that it's not valid anymore
  //----------------------------------------------------------------------------
  virtual void setDeleted()
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mIsDeleted = true;
  }

  //----------------------------------------------------------------------------
  //! Get locality hint for this container.
  //----------------------------------------------------------------------------
  virtual std::string getLocalityHint() const
  {
    return LocalityHint::build(ContainerIdentifier(getParentId()), getName());
  }

  std::chrono::steady_clock::time_point getLastPrefetch() const {
    std::shared_lock lock(mLastPrefetchMtx);
    return mLastPrefetch;
  }

  void setLastPrefetch(std::chrono::steady_clock::time_point tp) {
    std::unique_lock lock(mLastPrefetchMtx);
    mLastPrefetch = tp;
  }

private:
  friend class FileMapIterator;
  friend class ContainerMapIterator;

  //----------------------------------------------------------------------------
  //! Make copy constructor and assignment operator private to avoid "slicing"
  //! when dealing with derived classes.
  //----------------------------------------------------------------------------
  IContainerMD(const IContainerMD& other) = delete;
  IContainerMD& operator=(const IContainerMD& other) = delete;

  bool mIsDeleted; ///< Mark if object is still in cache but it was deleted

  std::chrono::steady_clock::time_point mLastPrefetch;
  mutable std::shared_mutex mLastPrefetchMtx;

protected:
  //----------------------------------------------------------------------------
  //! Get iterator to the begining of the subcontainers map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::ContainerMap::const_iterator
  subcontainersBegin() = 0;

  //----------------------------------------------------------------------------
  //! Get iterator to the end of the subcontainers map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::ContainerMap::const_iterator
  subcontainersEnd() = 0;

  //----------------------------------------------------------------------------
  //! Get iterator to the begining of the files map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::FileMap::const_iterator
  filesBegin() = 0;

  //----------------------------------------------------------------------------
  //! Get iterator to the end of the files map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::FileMap::const_iterator
  filesEnd() = 0;

  //----------------------------------------------------------------------------
  //! Get a copy of ContainerMap
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::ContainerMap copyContainerMap() const = 0;

  //----------------------------------------------------------------------------
  //! Get a copy of FileMap
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::FileMap copyFileMap() const = 0;

  mutable std::shared_timed_mutex mMutex;
};

EOSNSNAMESPACE_END

#endif // EOS_NS_ICONTAINER_MD_HH
