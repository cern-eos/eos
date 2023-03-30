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
//! @brief Class representing the file metadata
//------------------------------------------------------------------------------

#ifndef EOS_NS_FILE_MD_HH
#define EOS_NS_FILE_MD_HH

#include "common/SharedMutexWrapper.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "proto/FileMd.pb.h"
#include <cstdint>
#include <sys/time.h>

#define FRIEND_TEST(test_case_name, test_name)\
friend class test_case_name##_##test_name##_Test

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class IFileMDSvc;
class IContainerMD;

//------------------------------------------------------------------------------
//! Class holding the metadata information concerning a single file
//------------------------------------------------------------------------------
class QuarkFileMD : public IFileMD
{
  friend class FileSystemView;

public:
  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  QuarkFileMD();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkFileMD(IFileMD::id_t id, IFileMDSvc* fileMDSvc);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkFileMD() {};

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  QuarkFileMD(const QuarkFileMD& other);

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual QuarkFileMD* clone() const override;

  //----------------------------------------------------------------------------
  //! Assignment operator
  //----------------------------------------------------------------------------
  QuarkFileMD& operator=(const QuarkFileMD& other);

  //----------------------------------------------------------------------------
  //! Get creation time
  //----------------------------------------------------------------------------
  void getCTime(ctime_t& ctime) const override;

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  void setCTime(ctime_t ctime) override;

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  void setCTimeNow() override;

  //----------------------------------------------------------------------------
  //! Get access time
  //----------------------------------------------------------------------------
  void getATime(ctime_t& atime) const override;

  //----------------------------------------------------------------------------
  //! Set access time
  //----------------------------------------------------------------------------
  void setATime(ctime_t atime) override;

  //----------------------------------------------------------------------------
  //! Set access time to now if older than olderthan
  //----------------------------------------------------------------------------
  bool setATimeNow(uint64_t olderthan) override;

  //----------------------------------------------------------------------------
  //! Get modification time
  //----------------------------------------------------------------------------
  void getMTime(ctime_t& mtime) const override;

  //----------------------------------------------------------------------------
  //! Set modification time
  //----------------------------------------------------------------------------
  void setMTime(ctime_t mtime) override;

  //----------------------------------------------------------------------------
  //! Set modification time to now
  //----------------------------------------------------------------------------
  void setMTimeNow() override;

  //----------------------------------------------------------------------------
  //! get sync time
  //----------------------------------------------------------------------------
  void getSyncTime(ctime_t& stime) const override;

  //----------------------------------------------------------------------------
  //! set sync time
  //----------------------------------------------------------------------------
  void setSyncTime(ctime_t stime) override;

  //----------------------------------------------------------------------------
  //! set sync time to now
  //----------------------------------------------------------------------------
  void setSyncTimeNow() override;

  //----------------------------------------------------------------------------
  //! Get file id
  //----------------------------------------------------------------------------
  inline IFileMD::id_t
  getId() const override
  {
    return runReadOp([this]() { return mFile.id(); });
  }

  //----------------------------------------------------------------------------
  //! Get file identifier
  //----------------------------------------------------------------------------
  inline identifier_t
  getIdentifier() const override
  {
    return this->runReadOp([this]() { return identifier_t(mFile.id()); });
  }

  //----------------------------------------------------------------------------
  //! Get size
  //----------------------------------------------------------------------------
  inline uint64_t
  getSize() const override
  {
    return this->runReadOp([this]() { return mFile.size(); });
  }

  //----------------------------------------------------------------------------
  //! Set size - 48 bytes will be used
  //----------------------------------------------------------------------------
  void setSize(uint64_t size) override;

  //----------------------------------------------------------------------------
  //! Get cloneId
  //----------------------------------------------------------------------------
  inline uint64_t
  getCloneId() const override
  {
    return runReadOp([this]() { return mFile.cloneid(); });
  }

  //----------------------------------------------------------------------------
  //! Set cloneId
  //----------------------------------------------------------------------------
  void setCloneId(uint64_t id) override
  {
    return runWriteOp([this, id]() { mFile.set_cloneid(id); });
  }

  //----------------------------------------------------------------------------
  //! Get cloneFST
  //----------------------------------------------------------------------------
  const std::string
  getCloneFST() const override
  {
    return runReadOp([this]() { return mFile.clonefst(); });
  }

  //----------------------------------------------------------------------------
  //! Set cloneFST
  //----------------------------------------------------------------------------
  void setCloneFST(const std::string& data) override
  {
    runWriteOp([this,data](){
      mFile.set_clonefst(data);
    });
  }

  //----------------------------------------------------------------------------
  //! Get parent id
  //----------------------------------------------------------------------------
  inline IContainerMD::id_t
  getContainerId() const override
  {
    return this->runReadOp([this]() { return mFile.cont_id(); });
  }

  //----------------------------------------------------------------------------
  //! Set parent id
  //----------------------------------------------------------------------------
  void
  setContainerId(IContainerMD::id_t containerId) override
  {
    runWriteOp([this, containerId]() { mFile.set_cont_id(containerId); });
  }

  //----------------------------------------------------------------------------
  //! Get checksum
  //----------------------------------------------------------------------------
  inline const Buffer
  getChecksum() const override
  {
    return runReadOp([this]() {
      Buffer buff(mFile.checksum().size());
      buff.putData((void*)mFile.checksum().data(), mFile.checksum().size());
      return buff;
    });
  }

  //----------------------------------------------------------------------------
  //! Set checksum
  //----------------------------------------------------------------------------
  void
  setChecksum(const Buffer& checksum) override
  {
    runWriteOp([this, &checksum]() {
      mFile.set_checksum(checksum.getDataPtr(), checksum.getSize());
    });
  }

  //----------------------------------------------------------------------------
  //! Clear checksum
  //----------------------------------------------------------------------------
  void
  clearChecksum(uint8_t size = 20) override
  {
    runWriteOp([this]() { mFile.clear_checksum(); });
  }

  //----------------------------------------------------------------------------
  //! Set checksum
  //!
  //! @param checksum address of a memory location storing the checksum
  //! @param size     size of the checksum in bytes
  //----------------------------------------------------------------------------
  void
  setChecksum(const void* checksum, uint8_t size) override
  {
    runWriteOp(
        [this, checksum, size]() { mFile.set_checksum(checksum, size); });
  }

  //----------------------------------------------------------------------------
  //! Get name
  //----------------------------------------------------------------------------
  inline const std::string
  getName() const override
  {
    return runReadOp([this]() { return mFile.name(); });
  }

  //----------------------------------------------------------------------------
  //! Set name
  //----------------------------------------------------------------------------
  void setName(const std::string& name) override;

  //----------------------------------------------------------------------------
  //! Add location
  //----------------------------------------------------------------------------
  void addLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Get vector with all the locations
  //----------------------------------------------------------------------------
  inline LocationVector getLocations() const override
  {
    return this->runReadOp([this]() {
      LocationVector locations(mFile.locations().begin(),
                               mFile.locations().end());
      return locations;
    });
  }

  //----------------------------------------------------------------------------
  //! Get location
  //----------------------------------------------------------------------------
  location_t
  getLocation(unsigned int index) override
  {
    return this->runReadOp([this, index]() {
      if (index < (unsigned int)mFile.locations_size()) {
        return mFile.locations(index);
      }
      return (location_t)0;
    });
  }

  //----------------------------------------------------------------------------
  //! Remove location that was previously unlinked
  //----------------------------------------------------------------------------
  void removeLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Remove all locations that were previously unlinked
  //----------------------------------------------------------------------------
  void removeAllLocations() override;

  //----------------------------------------------------------------------------
  //! Clear locations without notifying the listeners
  //----------------------------------------------------------------------------
  void
  clearLocations() override
  {
    this->runWriteOp([this]() { mFile.clear_locations(); });
  }

  //----------------------------------------------------------------------------
  //! Test if location exists, without taking lock
  //----------------------------------------------------------------------------
  bool
  hasLocationNoLock(location_t location)
  {
    for (int i = 0; i < mFile.locations_size(); i++) {
      if (mFile.locations(i) == location) {
        return true;
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Test if location exists
  //----------------------------------------------------------------------------
  bool
  hasLocation(location_t location) override
  {
    return this->runReadOp(
        [this, location]() { return hasLocationNoLock(location); });
  }

  //----------------------------------------------------------------------------
  //! Get number of locations
  //----------------------------------------------------------------------------
  inline size_t
  getNumLocation() const override
  {
    return runReadOp([this]() { return mFile.locations_size(); });
  }

  //----------------------------------------------------------------------------
  //! Get vector with all unlinked locations
  //----------------------------------------------------------------------------
  inline LocationVector getUnlinkedLocations() const override
  {
    return this->runReadOp([this]() {
      LocationVector unlinked_locations(mFile.unlink_locations().begin(),
                                        mFile.unlink_locations().end());
      return unlinked_locations;
    });

  }

  //----------------------------------------------------------------------------
  //! Unlink location
  //----------------------------------------------------------------------------
  void unlinkLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Unlink all locations
  //----------------------------------------------------------------------------
  void unlinkAllLocations() override;

  //----------------------------------------------------------------------------
  //! Clear unlinked locations without notifying the listeners
  //----------------------------------------------------------------------------
  inline void
  clearUnlinkedLocations() override
  {
    this->runWriteOp([this]() { mFile.clear_unlink_locations(); });
  }

  //----------------------------------------------------------------------------
  //! Test the unlinked location
  //----------------------------------------------------------------------------
  bool
  hasUnlinkedLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Get number of unlinked locations
  //----------------------------------------------------------------------------
  inline size_t
  getNumUnlinkedLocation() const override
  {
    return runReadOp([this]() { return mFile.unlink_locations_size(); });
  }

  //----------------------------------------------------------------------------
  //! Get uid
  //----------------------------------------------------------------------------
  inline uid_t
  getCUid() const override
  {
    return runReadOp([this]() { return mFile.uid(); });

  }

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  inline void
  setCUid(uid_t uid) override
  {
    runWriteOp([this, uid]() { mFile.set_uid(uid); });
  }

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  inline gid_t
  getCGid() const override
  {
    return runReadOp([this]() { return mFile.gid(); });
  }

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  inline void
  setCGid(gid_t gid) override
  {
    runWriteOp([this, gid]() { mFile.set_gid(gid); });
  }

  //----------------------------------------------------------------------------
  //! Get layout
  //----------------------------------------------------------------------------
  inline layoutId_t
  getLayoutId() const override
  {
    return runReadOp([this]() { return mFile.layout_id(); });
  }

  //----------------------------------------------------------------------------
  //! Set layout
  //----------------------------------------------------------------------------
  inline void
  setLayoutId(layoutId_t layoutId) override
  {
    runWriteOp([this, layoutId]() { mFile.set_layout_id(layoutId); });
  }

  //----------------------------------------------------------------------------
  //! Get flags
  //----------------------------------------------------------------------------
  inline uint16_t
  getFlags() const override
  {
    return runReadOp([this]() { return mFile.flags(); });
  }

  //----------------------------------------------------------------------------
  //! Get the n-th flag
  //----------------------------------------------------------------------------
  inline bool
  getFlag(uint8_t n) override
  {
    return runReadOp(
        [this, n]() { return (bool)(mFile.flags() & (0x0001 << n)); });
  }

  //----------------------------------------------------------------------------
  //! Set flags
  //----------------------------------------------------------------------------
  inline void
  setFlags(uint16_t flags) override
  {
    return runWriteOp([this, flags]() { mFile.set_flags(flags); });
  }

  //----------------------------------------------------------------------------
  //! Set the n-th flag
  //----------------------------------------------------------------------------
  void
  setFlag(uint8_t n, bool flag) override
  {
    return runWriteOp([this, n, flag]() {
      if (flag) {
        mFile.set_flags(mFile.flags() | (1 << n));
      } else {
        mFile.set_flags(mFile.flags() & (~(1 << n)));
      }
    });
  }

  //----------------------------------------------------------------------------
  //! Env Representation
  //----------------------------------------------------------------------------
  void getEnv(std::string& env, bool escapeAnd = false) override;

  //----------------------------------------------------------------------------
  //! Set the FileMDSvc object
  //----------------------------------------------------------------------------
  inline void
  setFileMDSvc(IFileMDSvc* fileMDSvc) override
  {
    runWriteOp([this, fileMDSvc]() {
      pFileMDSvc = static_cast<QuarkFileMDSvc*>(fileMDSvc);
    });
  }

  //----------------------------------------------------------------------------
  //! Get the FileMDSvc object
  //----------------------------------------------------------------------------
  inline virtual IFileMDSvc*
  getFileMDSvc() override
  {
    return runReadOp([this]() { return pFileMDSvc; });
  }

  //----------------------------------------------------------------------------
  //! Get symbolic link
  //----------------------------------------------------------------------------
  inline std::string
  getLink() const override
  {
    return runReadOp([this]() { return mFile.link_name(); });
  }

  //----------------------------------------------------------------------------
  //! Set symbolic link
  //----------------------------------------------------------------------------
  inline void
  setLink(std::string link_name) override
  {
    runWriteOp([this, link_name]() { mFile.set_link_name(link_name); });
  }

  //----------------------------------------------------------------------------
  //! Check if symbolic link
  //----------------------------------------------------------------------------
  bool
  isLink() const override
  {
    return runReadOp([this]() { return !mFile.link_name().empty(); });
  }

  //----------------------------------------------------------------------------
  //! Add extended attribute
  //----------------------------------------------------------------------------
  void
  setAttribute(const std::string& name, const std::string& value) override
  {
    runWriteOp(
        [this, name, value]() { (*mFile.mutable_xattrs())[name] = value; });
  }

  //----------------------------------------------------------------------------
  //! Remove attribute
  //----------------------------------------------------------------------------
  void
  removeAttribute(const std::string& name) override
  {
    runWriteOp([this, name]() {
      auto it = mFile.xattrs().find(name);

      if (it != mFile.xattrs().end()) {
        mFile.mutable_xattrs()->erase(it->first);
      }
    });
  }

  //----------------------------------------------------------------------------
  //! Remove all attributes
  //----------------------------------------------------------------------------
  void clearAttributes() override
  {
    runWriteOp([this]() { mFile.clear_xattrs(); });
  }

  //----------------------------------------------------------------------------
  //! Check if the attribute exist
  //----------------------------------------------------------------------------
  bool
  hasAttribute(const std::string& name) const override
  {
    return runReadOp([this, name]() {
      return (mFile.xattrs().find(name) != mFile.xattrs().end());
    });
  }

  //----------------------------------------------------------------------------
  //! Return number of attributes
  //----------------------------------------------------------------------------
  inline size_t
  numAttributes() const override
  {
    return runReadOp([this]() { return mFile.xattrs().size(); });
  }

  //----------------------------------------------------------------------------
  //! Get the attribute
  //----------------------------------------------------------------------------
  std::string
  getAttribute(const std::string& name) const override
  {
    return runReadOp([this, &name] {
      auto it = mFile.xattrs().find(name);

      if (it == mFile.xattrs().end()) {
        MDException e(ENOENT);
        e.getMessage() << "Attribute: " << name << " not found";
        throw e;
      }

      return it->second;
    });
  }

  //----------------------------------------------------------------------------
  //! Get map copy of the extended attributes
  //!
  //! @return std::map containing all the extended attributes
  //----------------------------------------------------------------------------
  eos::IFileMD::XAttrMap getAttributes() const override;

  //----------------------------------------------------------------------------
  //! Serialize the object to a buffer
  //----------------------------------------------------------------------------
  void serialize(Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Initialize from protobuf contents
  //----------------------------------------------------------------------------
  void initialize(eos::ns::FileMdProto&& proto);

  //----------------------------------------------------------------------------
  //! Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  void deserialize(const Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Get reference to underlying protobuf object
  //----------------------------------------------------------------------------
  const eos::ns::FileMdProto& getProto() const;

  //----------------------------------------------------------------------------
  //! Get value tracking changes to the metadata object
  //----------------------------------------------------------------------------
  virtual uint64_t getClock() const override
  {
    return runReadOp([this](){
      return mClock;
    });
  };

protected:
  IFileMDSvc* pFileMDSvc;

private:
  FRIEND_TEST(VariousTests, EtagFormatting);

  //----------------------------------------------------------------------------
  //! Get modification time, no locks
  //----------------------------------------------------------------------------
  void getMTimeNoLock(ctime_t& mtime) const;

  //----------------------------------------------------------------------------
  //! Get access time, no locks
  //----------------------------------------------------------------------------
  void getATimeNoLock(ctime_t& atime) const;

  //----------------------------------------------------------------------------
  //! Get modification time, no locks
  //----------------------------------------------------------------------------
  void getSyncTimeNoLock(ctime_t& stime) const;

  //----------------------------------------------------------------------------
  //! Get creation time, no locks
  //----------------------------------------------------------------------------
  void getCTimeNoLock(ctime_t& ctime) const;

  //----------------------------------------------------------------------------
  //! Test the unlinked location, no locks
  //----------------------------------------------------------------------------
  bool hasUnlinkedLocationNoLock(location_t location) const;


  eos::ns::FileMdProto mFile; ///< Protobuf file representation
  uint64_t mClock; ///< Value tracking metadata changes
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILE_MD_HH__
