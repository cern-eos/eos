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

#ifndef __EOS_NS_FILE_MD_HH__
#define __EOS_NS_FILE_MD_HH__

#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "FileMd.pb.h"
#include <stdint.h>
#include <string>
#include <sys/time.h>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class IFileMDSvc;
class IContainerMD;

//------------------------------------------------------------------------------
//! Class holding the metadata information concerning a single file
//------------------------------------------------------------------------------
class FileMD : public IFileMD
{
  friend class FileSystemView;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileMD(id_t id, IFileMDSvc* fileMDSvc);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileMD() {};

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  FileMD(const FileMD& other);

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual FileMD* clone() const override;

  //----------------------------------------------------------------------------
  //! Asignment operator
  //----------------------------------------------------------------------------
  FileMD& operator=(const FileMD& other);

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
  //! Get file id
  //----------------------------------------------------------------------------
  inline id_t
  getId() const override
  {
    return mFile.id();
  }

  //----------------------------------------------------------------------------
  //! Get size
  //----------------------------------------------------------------------------
  inline uint64_t
  getSize() const override
  {
    return mFile.size();
  }

  //----------------------------------------------------------------------------
  //! Set size - 48 bytes will be used
  //----------------------------------------------------------------------------
  void setSize(uint64_t size) override;

  //----------------------------------------------------------------------------
  //! Get parent id
  //----------------------------------------------------------------------------
  inline IContainerMD::id_t
  getContainerId() const override
  {
    return mFile.cont_id();
  }

  //----------------------------------------------------------------------------
  //! Set parent id
  //----------------------------------------------------------------------------
  void
  setContainerId(IContainerMD::id_t containerId) override
  {
    mFile.set_cont_id(containerId);
  }

  //----------------------------------------------------------------------------
  //! Get checksum
  //----------------------------------------------------------------------------
  inline const Buffer
  getChecksum() const override
  {
    Buffer buff(mFile.checksum().size());
    buff.putData((void*)mFile.checksum().data(), mFile.checksum().size());
    return buff;
  }

  //----------------------------------------------------------------------------
  //! Compare checksums
  //!
  //! @param checksum checksum value to compare with
  //! @warning You need to supply enough bytes to compare with the checksum
  //!          stored in the object
  //----------------------------------------------------------------------------
  bool
  checksumMatch(const void* checksum) const override
  {
    return !memcmp(checksum, (void*)mFile.checksum().data(),
                   mFile.checksum().size());
  }

  //----------------------------------------------------------------------------
  //! Set checksum
  //----------------------------------------------------------------------------
  void
  setChecksum(const Buffer& checksum) override
  {
    mFile.set_checksum(checksum.getDataPtr(), checksum.getSize());
  }

  //----------------------------------------------------------------------------
  //! Clear checksum
  //----------------------------------------------------------------------------
  void
  clearChecksum(uint8_t size = 20) override
  {
    mFile.clear_checksum();
  }

  //----------------------------------------------------------------------------
  //! Set checksum
  //!
  //! @param checksum address of a memory location string the checksum
  //! @param size     size of the checksum in bytes
  //----------------------------------------------------------------------------
  void
  setChecksum(const void* checksum, uint8_t size) override
  {
    mFile.set_checksum(checksum, size);
  }

  //----------------------------------------------------------------------------
  //! Get name
  //----------------------------------------------------------------------------
  inline const std::string
  getName() const override
  {
    return mFile.name();
  }

  //----------------------------------------------------------------------------
  //! Set name
  //----------------------------------------------------------------------------
  inline void setName(const std::string& name) override
  {
    mFile.set_name(name);
  }

  //----------------------------------------------------------------------------
  //! Add location
  //----------------------------------------------------------------------------
  void addLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Get vector with all the locations
  //----------------------------------------------------------------------------
  inline LocationVector getLocations() const override
  {
    LocationVector locations(mFile.locations().begin(), mFile.locations().end());
    return locations;
  }

  //----------------------------------------------------------------------------
  //! Get location
  //----------------------------------------------------------------------------
  location_t
  getLocation(unsigned int index) override
  {
    if (index < (unsigned int)mFile.locations_size()) {
      return mFile.locations(index);
    }

    return 0;
  }

  //----------------------------------------------------------------------------
  //! Replace location by index
  //----------------------------------------------------------------------------
  void replaceLocation(unsigned int index, location_t newlocation) override;

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
    mFile.clear_locations();
  }

  //----------------------------------------------------------------------------
  //! Test if location exists
  //----------------------------------------------------------------------------
  bool
  hasLocation(location_t location) override
  {
    for (int i = 0; i < mFile.locations_size(); i++) {
      if (mFile.locations(i) == location) {
        return true;
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Get number of locations
  //----------------------------------------------------------------------------
  inline size_t
  getNumLocation() const override
  {
    return mFile.locations_size();
  }

  //----------------------------------------------------------------------------
  //! Get vector with all unlinked locations
  //----------------------------------------------------------------------------
  inline LocationVector getUnlinkedLocations() const override
  {
    LocationVector unlinked_locations(mFile.unlink_locations().begin(),
                                      mFile.unlink_locations().end());
    return unlinked_locations;
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
    mFile.clear_unlink_locations();
  }

  //----------------------------------------------------------------------------
  //! Test the unlinkedlocation
  //----------------------------------------------------------------------------
  bool
  hasUnlinkedLocation(location_t location) override
  {
    for (int i = 0; i < mFile.unlink_locations_size(); ++i) {
      if (mFile.unlink_locations()[i] == location) {
        return true;
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Get number of unlinked locations
  //----------------------------------------------------------------------------
  inline size_t
  getNumUnlinkedLocation() const override
  {
    return mFile.unlink_locations_size();
  }

  //----------------------------------------------------------------------------
  //! Get uid
  //----------------------------------------------------------------------------
  inline uid_t
  getCUid() const override
  {
    return mFile.uid();
  }

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  inline void
  setCUid(uid_t uid) override
  {
    mFile.set_uid(uid);
  }

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  inline gid_t
  getCGid() const override
  {
    return mFile.gid();
  }

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  inline void
  setCGid(gid_t gid) override
  {
    mFile.set_gid(gid);
  }

  //----------------------------------------------------------------------------
  //! Get layout
  //----------------------------------------------------------------------------
  inline layoutId_t
  getLayoutId() const override
  {
    return mFile.layout_id();
  }

  //----------------------------------------------------------------------------
  //! Set layout
  //----------------------------------------------------------------------------
  inline void
  setLayoutId(layoutId_t layoutId) override
  {
    mFile.set_layout_id(layoutId);
  }

  //----------------------------------------------------------------------------
  //! Get flags
  //----------------------------------------------------------------------------
  inline uint16_t
  getFlags() const override
  {
    return mFile.flags();
  }

  //----------------------------------------------------------------------------
  //! Get the n-th flag
  //----------------------------------------------------------------------------
  inline bool
  getFlag(uint8_t n) override
  {
    return (bool)(mFile.flags() & (0x0001 << n));
  }

  //----------------------------------------------------------------------------
  //! Set flags
  //----------------------------------------------------------------------------
  inline void
  setFlags(uint16_t flags) override
  {
    mFile.set_flags(flags);
  }

  //----------------------------------------------------------------------------
  //! Set the n-th flag
  //----------------------------------------------------------------------------
  void
  setFlag(uint8_t n, bool flag) override
  {
    if (flag) {
      mFile.set_flags(mFile.flags() | (1 << n));
    } else {
      mFile.set_flags(mFile.flags() & (~(1 << n)));
    }
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
    pFileMDSvc = static_cast<FileMDSvc*>(fileMDSvc);
  }

  //----------------------------------------------------------------------------
  //! Get the FileMDSvc object
  //----------------------------------------------------------------------------
  inline virtual IFileMDSvc*
  getFileMDSvc() override
  {
    return pFileMDSvc;
  }

  //----------------------------------------------------------------------------
  //! Get symbolic link
  //----------------------------------------------------------------------------
  inline std::string
  getLink() const override
  {
    return mFile.link_name();
  }

  //----------------------------------------------------------------------------
  //! Set symbolic link
  //----------------------------------------------------------------------------
  inline void
  setLink(std::string link_name) override
  {
    mFile.set_link_name(link_name);
  }

  //----------------------------------------------------------------------------
  //! Check if symbolic link
  //----------------------------------------------------------------------------
  bool
  isLink() const override
  {
    return !mFile.link_name().empty();
  }

  //----------------------------------------------------------------------------
  //! Add extended attribute
  //----------------------------------------------------------------------------
  void
  setAttribute(const std::string& name, const std::string& value) override
  {
    (*mFile.mutable_xattrs())[name] = value;
  }

  //----------------------------------------------------------------------------
  //! Remove attribute
  //----------------------------------------------------------------------------
  void
  removeAttribute(const std::string& name) override
  {
    auto it = mFile.xattrs().find(name);

    if (it != mFile.xattrs().end()) {
      mFile.mutable_xattrs()->erase(it->first);
    }
  }

  //----------------------------------------------------------------------------
  //! Remove all attributes
  //----------------------------------------------------------------------------
  void clearAttributes() override
  {
    mFile.clear_xattrs();
  }

  //----------------------------------------------------------------------------
  //! Check if the attribute exist
  //----------------------------------------------------------------------------
  bool
  hasAttribute(const std::string& name) const override
  {
    return (mFile.xattrs().find(name) != mFile.xattrs().end());
  }

  //----------------------------------------------------------------------------
  //! Return number of attributes
  //----------------------------------------------------------------------------
  inline size_t
  numAttributes() const override
  {
    return mFile.xattrs().size();
  }

  //----------------------------------------------------------------------------
  //! Get the attribute
  //----------------------------------------------------------------------------
  std::string
  getAttribute(const std::string& name) const override
  {
    auto it = mFile.xattrs().find(name);

    if (it == mFile.xattrs().end()) {
      MDException e(ENOENT);
      e.getMessage() << "Attribute: " << name << " not found";
      throw e;
    }

    return it->second;
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
  //! Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  void deserialize(const Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Wait for replies to asynchronous requests
  //!
  //! @return true if all replies successful, otherwise false
  //----------------------------------------------------------------------------
  bool waitAsyncReplies();

  //----------------------------------------------------------------------------
  //! Register asynchronous request which are handled by the AsyncHandler
  //! member object.
  //!
  //! @param aresp pair holding the future and the command that was executed
  //! @param qcl pointer to client object used to send the initial request
  //----------------------------------------------------------------------------
  void Register(qclient::AsyncResponseType aresp, qclient::QClient* qcl);

  //----------------------------------------------------------------------------
  //! Get value tracking changes to the metadata object
  //----------------------------------------------------------------------------
  virtual uint64_t getClock() const override
  {
    return mClock;
  };

protected:
  IFileMDSvc* pFileMDSvc;

private:
  eos::ns::FileMdProto mFile; ///< Protobuf file representation
  qclient::AsyncHandler mAh; ///< Async handler
  uint64_t mClock; ///< Value tracking metadata changes
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILE_MD_HH__
