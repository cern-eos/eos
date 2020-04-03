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
//! @brief Class representing the container metadata
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_MD_HH
#define EOS_NS_CONTAINER_MD_HH

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "proto/ContainerMd.pb.h"
#include "common/FutureWrapper.hh"
#include <sys/time.h>

#define FRIEND_TEST(test_case_name, test_name)\
friend class test_case_name##_##test_name##_Test

EOSNSNAMESPACE_BEGIN

class IContainerMDSvc;
class IFileMDSvc;

//------------------------------------------------------------------------------
//! Class holding the metadata information concerning a single container
//------------------------------------------------------------------------------
class QuarkContainerMD : public IContainerMD
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkContainerMD(IContainerMD::id_t id, IFileMDSvc* file_svc,
                   IContainerMDSvc* cont_svc);

  //----------------------------------------------------------------------------
  //! Constructor used for testing and dump command
  //----------------------------------------------------------------------------
  QuarkContainerMD(): pContSvc(nullptr), pFileSvc(nullptr), pFlusher(nullptr),
    pQcl(nullptr), mClock(1) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkContainerMD() {};

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  QuarkContainerMD(const QuarkContainerMD& other);

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual QuarkContainerMD* clone() const override;

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  void InheritChildren(const IContainerMD& other) override;

  //----------------------------------------------------------------------------
  //! Set services
  //----------------------------------------------------------------------------
  void setServices(IFileMDSvc* file_svc, IContainerMDSvc* cont_svc);

  //----------------------------------------------------------------------------
  //! Add container
  //----------------------------------------------------------------------------
  void addContainer(IContainerMD* container) override;

  //----------------------------------------------------------------------------
  //! Remove container
  //----------------------------------------------------------------------------
  void removeContainer(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Find subcontainer, asynchronous API
  //----------------------------------------------------------------------------
  folly::Future<IContainerMDPtr> findContainerFut(const std::string& name)
  override;

  //----------------------------------------------------------------------------
  //! Find subcontainer
  //----------------------------------------------------------------------------
  std::shared_ptr<IContainerMD> findContainer(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Find item
  //----------------------------------------------------------------------------
  folly::Future<FileOrContainerMD> findItem(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  size_t getNumContainers() override;

  //----------------------------------------------------------------------------
  //! Add file
  //----------------------------------------------------------------------------
  void addFile(IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Remove file
  //----------------------------------------------------------------------------
  void removeFile(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Find file, asynchronous API.
  //----------------------------------------------------------------------------
  folly::Future<IFileMDPtr> findFileFut(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Find file
  //----------------------------------------------------------------------------
  IFileMDPtr findFile(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  size_t getNumFiles() override;

  //----------------------------------------------------------------------------
  //! Get container id
  //----------------------------------------------------------------------------
  inline IContainerMD::id_t getId() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.id();
  }

  //----------------------------------------------------------------------------
  //! Get container identifier
  //----------------------------------------------------------------------------
  inline ContainerIdentifier getIdentifier() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return ContainerIdentifier(mCont.id());
  }


  //----------------------------------------------------------------------------
  //! Get parent id
  //----------------------------------------------------------------------------
  inline IContainerMD::id_t
  getParentId() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.parent_id();
  }

  //----------------------------------------------------------------------------
  //! Set parent id
  //----------------------------------------------------------------------------
  void
  setParentId(IContainerMD::id_t parentId) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_parent_id(parentId);
  }

  //----------------------------------------------------------------------------
  //! Get the flags
  //----------------------------------------------------------------------------
  inline uint16_t
  getFlags() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.flags();
  }

  //----------------------------------------------------------------------------
  //! Set flags
  //----------------------------------------------------------------------------
  virtual void setFlags(uint16_t flags) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_flags(0x00ff & flags);
  }

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  void setCTime(ctime_t ctime) override;

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  void setCTimeNow() override;

  //----------------------------------------------------------------------------
  //! Get creation time
  //----------------------------------------------------------------------------
  void getCTime(ctime_t& ctime) const override;

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  void setMTime(mtime_t mtime) override;

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  void setMTimeNow() override;

  //----------------------------------------------------------------------------
  //! Get modification time
  //----------------------------------------------------------------------------
  void getMTime(mtime_t& mtime) const override;

  //----------------------------------------------------------------------------
  //! Set propagated modification time (if newer)
  //----------------------------------------------------------------------------
  bool setTMTime(tmtime_t tmtime) override;

  //----------------------------------------------------------------------------
  //! Set propagated modification time to now
  //----------------------------------------------------------------------------
  void setTMTimeNow() override;

  //----------------------------------------------------------------------------
  //! Get propagated modification time
  //----------------------------------------------------------------------------
  void getTMTime(tmtime_t& tmtime) override;

  //----------------------------------------------------------------------------
  //! Trigger an mtime change event
  //----------------------------------------------------------------------------
  void notifyMTimeChange(IContainerMDSvc* containerMDSvc) override;

  //----------------------------------------------------------------------------
  //! Get tree size
  //----------------------------------------------------------------------------
  inline uint64_t
  getTreeSize() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.tree_size();
  }

  //----------------------------------------------------------------------------
  //! Set tree size
  //----------------------------------------------------------------------------
  inline void
  setTreeSize(uint64_t treesize) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_tree_size(treesize);
  }

  //----------------------------------------------------------------------------
  //! Update to tree size
  //----------------------------------------------------------------------------
  uint64_t updateTreeSize(int64_t delta) override;

  //----------------------------------------------------------------------------
  //! Get name
  //----------------------------------------------------------------------------
  inline const std::string&
  getName() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.name();
  }

  //----------------------------------------------------------------------------
  //! Set name
  //----------------------------------------------------------------------------
  void setName(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Get uid
  //----------------------------------------------------------------------------
  inline uid_t
  getCUid() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.uid();
  }

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  inline void
  setCUid(uid_t uid) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_uid(uid);
  }

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  inline gid_t
  getCGid() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.gid();
  }

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  inline void
  setCGid(gid_t gid) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_gid(gid);
  }

  //----------------------------------------------------------------------------
  //! Get cloneId
  //----------------------------------------------------------------------------
  inline time_t
  getCloneId() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.cloneid();
  }

  //----------------------------------------------------------------------------
  //! Set cloneId
  //----------------------------------------------------------------------------
  inline void
  setCloneId(time_t id) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_cloneid(id);
  }

  //----------------------------------------------------------------------------
  //! Get cloneFST
  //----------------------------------------------------------------------------
  inline const std::string
  getCloneFST() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.clonefst();
  }

  //----------------------------------------------------------------------------
  //! Set cloneFST
  //----------------------------------------------------------------------------
  void setCloneFST(const std::string& data) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_clonefst(data);
  }

  //----------------------------------------------------------------------------
  //! Get mode
  //----------------------------------------------------------------------------
  inline mode_t
  getMode() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.mode();
  }

  //----------------------------------------------------------------------------
  //! Set mode
  //----------------------------------------------------------------------------
  inline void
  setMode(mode_t mode) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mCont.set_mode(mode);
  }

  //----------------------------------------------------------------------------
  //! Add extended attribute
  //----------------------------------------------------------------------------
  void
  setAttribute(const std::string& name, const std::string& value) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    (*mCont.mutable_xattrs())[name] = value;
  }

  //----------------------------------------------------------------------------
  //! Remove attribute
  //----------------------------------------------------------------------------
  void removeAttribute(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Check if the attribute exist
  //----------------------------------------------------------------------------
  bool
  hasAttribute(const std::string& name) const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return (mCont.xattrs().find(name) != mCont.xattrs().end());
  }

  //----------------------------------------------------------------------------
  //! Return number of attributes
  //----------------------------------------------------------------------------
  size_t
  numAttributes() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mCont.xattrs().size();
  }

  //----------------------------------------------------------------------------
  // Get the attribute
  //----------------------------------------------------------------------------
  std::string getAttribute(const std::string& name) const override;

  //----------------------------------------------------------------------------
  //! Get map copy of the extended attributes
  //!
  //! @return std::map containing all the extended attributes
  //----------------------------------------------------------------------------
  XAttrMap getAttributes() const override;

  //------------------------------------------------------------------------------
  //! Check the access permissions
  //!
  //! @return true if all the requested rights are granted, false otherwise
  //------------------------------------------------------------------------------
  bool access(uid_t uid, gid_t gid, int flags = 0) override;

  //----------------------------------------------------------------------------
  //! Serialize the object to a buffer
  //----------------------------------------------------------------------------
  void serialize(Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Load children of container
  //----------------------------------------------------------------------------
  void loadChildren();

  //----------------------------------------------------------------------------
  //! Deserialize the class to a buffer and load its children
  //----------------------------------------------------------------------------
  void deserialize(Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Initialize, and inject children
  //----------------------------------------------------------------------------
  void initialize(eos::ns::ContainerMdProto&& proto,
                  IContainerMD::FileMap&& fileMap, IContainerMD::ContainerMap&& containerMap);

  //----------------------------------------------------------------------------
  //! Initialize, without loading children
  //----------------------------------------------------------------------------
  void initializeWithoutChildren(eos::ns::ContainerMdProto&& proto);

  //----------------------------------------------------------------------------
  //! Get value tracking changes to the metadata object
  //----------------------------------------------------------------------------
  virtual uint64_t getClock() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return mClock;
  }

  //----------------------------------------------------------------------------
  //! Get env representation of the container object
  //!
  //! @param env string where representation is stored
  //! @param escapeAnd if true escape & with #AND# ...
  //----------------------------------------------------------------------------
  void getEnv(std::string& env, bool escapeAnd = false) override;

  //----------------------------------------------------------------------------
  //! Get a copy of ContainerMap
  //----------------------------------------------------------------------------
  IContainerMD::ContainerMap copyContainerMap() const override;

  //----------------------------------------------------------------------------
  //! Get a copy of FileMap
  //----------------------------------------------------------------------------
  IContainerMD::FileMap copyFileMap() const override;

private:
  FRIEND_TEST(VariousTests, EtagFormattingContainer);

  //----------------------------------------------------------------------------
  //! Get propagated modification time, no locks
  //----------------------------------------------------------------------------
  void getTMTimeNoLock(tmtime_t& tmtime);

  //----------------------------------------------------------------------------
  //! Get creation time, no locks
  //----------------------------------------------------------------------------
  void getCTimeNoLock(ctime_t& ctime) const;

  //----------------------------------------------------------------------------
  //! Get modification time, no locks
  //----------------------------------------------------------------------------
  void getMTimeNoLock(mtime_t& mtime) const;

  //----------------------------------------------------------------------------
  //! Get iterator to the begining of the subcontainers map
  //----------------------------------------------------------------------------
  eos::IContainerMD::ContainerMap::const_iterator
  subcontainersBegin() override
  {
    // No lock here, only ContainerMapIterator can call us, which locks the mutex.
    return mSubcontainers->cbegin();
  }

  //----------------------------------------------------------------------------
  //! Get iterator to the end of the subcontainers map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::ContainerMap::const_iterator
  subcontainersEnd() override
  {
    // No lock here, only ContainerMapIterator can call us, which locks the mutex.
    return mSubcontainers->cend();
  }

  //----------------------------------------------------------------------------
  //! Get iterator to the begining of the files map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::FileMap::const_iterator
  filesBegin() override
  {
    // No lock here, only FileMapIterator can call us, which locks the mutex.
    return mFiles->cbegin();
  }

  //----------------------------------------------------------------------------
  //! Get iterator to the end of the files map
  //----------------------------------------------------------------------------
  virtual eos::IContainerMD::FileMap::const_iterator
  filesEnd() override
  {
    // No lock here, only FileMapIterator can call us, which locks the mutex.
    return mFiles->cend();
  }

  eos::ns::ContainerMdProto mCont;      ///< Protobuf container representation
  IContainerMDSvc* pContSvc = nullptr;  ///< Container metadata service
  IFileMDSvc* pFileSvc = nullptr;       ///< File metadata service
  MetadataFlusher* pFlusher = nullptr;  ///< Metadata flusher object
  qclient::QClient* pQcl;               ///< QClient object
  std::string pFilesKey;                ///< Map files key
  std::string pDirsKey;                 ///< Map dir key
  uint64_t mClock;                      ///< Value tracking changes

  common::FutureWrapper<ContainerMap> mSubcontainers;
  common::FutureWrapper<FileMap> mFiles;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_CONTAINER_MD_HH__
