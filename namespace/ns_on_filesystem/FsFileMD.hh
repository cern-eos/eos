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
//! @author: Elvin Sindrilaru <esindril@cern.ch>
//! @brief: Class representing file metadata saved in a regular file on disk
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FS_FILE_MD_HH__
#define __EOS_NS_FS_FILE_MD_HH__

#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <stdint.h>
#include <cstring>
#include <string>
#include <vector>
#include <sys/time.h>

EOSNSNAMESPACE_BEGIN

//! Forward declration
class IFileMDSvc;
class IContainerMD;

//------------------------------------------------------------------------------
//! Class representing a file-system file object
//------------------------------------------------------------------------------
class FsFileMD: public IFileMD
{
 public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FsFileMD(id_t id, IFileMDSvc* fileMDSvc);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  virtual ~FsFileMD() {};

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual FsFileMD* clone() const;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  FsFileMD(const FileMD& other);

  //----------------------------------------------------------------------------
  //! Asignment operator
  //----------------------------------------------------------------------------
  FsFileMD& operator = (const FileMD& other);

  //----------------------------------------------------------------------------
  //! Get file id
  //----------------------------------------------------------------------------
  id_t getId() const
  {
    return pId;
  }

  //----------------------------------------------------------------------------
  //! Get creation time
  //----------------------------------------------------------------------------
  void getCTime(ctime_t& ctime) const;

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  void setCTime(ctime_t ctime);

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  void setCTimeNow();

  //----------------------------------------------------------------------------
  //! Get modification time
  //----------------------------------------------------------------------------
  void getMTime(ctime_t& mtime) const;

  //----------------------------------------------------------------------------
  //! Set modification time
  //----------------------------------------------------------------------------
  void setMTime(ctime_t mtime);

  //----------------------------------------------------------------------------
  //! Set modification time to now
  //----------------------------------------------------------------------------
  void setMTimeNow();

  //----------------------------------------------------------------------------
  //! Get size
  //----------------------------------------------------------------------------
  uint64_t getSize() const;

  //----------------------------------------------------------------------------
  //! Set size - 48 bytes will be used
  //----------------------------------------------------------------------------
  void setSize(uint64_t size);

  //----------------------------------------------------------------------------
  //! Get tag
  //----------------------------------------------------------------------------
  IContainerMD::id_t getContainerId() const
  {
    return pContainerId;
  }

  //----------------------------------------------------------------------------
  //! Set tag
  //----------------------------------------------------------------------------
  void setContainerId(IContainerMD::id_t containerId)
  {
    pContainerId = containerId;
  }

  //----------------------------------------------------------------------------
  //! Get checksum
  //----------------------------------------------------------------------------
  const Buffer& getChecksum() const;

  //----------------------------------------------------------------------------
  //! Compare checksums
  //! WARNING: you have to supply enough bytes to compare with the checksum
  //! stored in the object!
  //----------------------------------------------------------------------------
  bool checksumMatch(const void* checksum) const;

  //----------------------------------------------------------------------------
  //! Set checksum
  //----------------------------------------------------------------------------
  void setChecksum(const Buffer& checksum);

  //----------------------------------------------------------------------------
  //! Clear checksum
  //----------------------------------------------------------------------------
  void clearChecksum(uint8_t size = 20);

  //----------------------------------------------------------------------------
  //! Set checksum
  //!
  //! @param checksum address of a memory location string the checksum
  //! @param size     size of the checksum in bytes
  //----------------------------------------------------------------------------
  void setChecksum(const void* checksum, uint8_t size);

  //----------------------------------------------------------------------------
  //! Get name
  //----------------------------------------------------------------------------
  const std::string getName() const
  {
    return std::string(pName);
  }

  //----------------------------------------------------------------------------
  //! Set name
  //----------------------------------------------------------------------------
  void setName(const std::string& name)
  {
    pName = name;
  }

  //----------------------------------------------------------------------------
  //! Start iterator for locations
  //----------------------------------------------------------------------------
  LocationVector::const_iterator locationsBegin() const
  {
    return pLocation.begin();
  }

  //----------------------------------------------------------------------------
  //! End iterator for locations
  //----------------------------------------------------------------------------
  LocationVector::const_iterator locationsEnd() const
  {
    return pLocation.end();
  }

  //----------------------------------------------------------------------------
  //! Start iterator for unlinked locations
  //----------------------------------------------------------------------------
  LocationVector::const_iterator unlinkedLocationsBegin() const
  {
    return pUnlinkedLocation.begin();
  }

  //----------------------------------------------------------------------------
  //! End iterator for unlinked locations
  //----------------------------------------------------------------------------
  LocationVector::const_iterator unlinkedLocationsEnd() const
  {
    return pUnlinkedLocation.end();
  }

  //----------------------------------------------------------------------------
  //! Add location
  //----------------------------------------------------------------------------
  void addLocation(location_t location);

  //----------------------------------------------------------------------------
  //! Get vector with all the locations
  //----------------------------------------------------------------------------
  LocationVector getLocations() const;

  //----------------------------------------------------------------------------
  //! Get location
  //----------------------------------------------------------------------------
  location_t getLocation(unsigned int index)
  {
    if (index < pLocation.size())
      return pLocation[index];

    return 0;
  }

  //----------------------------------------------------------------------------
  //! replace location by index
  //----------------------------------------------------------------------------
  void replaceLocation(unsigned int index, location_t newlocation);

  //----------------------------------------------------------------------------
  //! Remove location that was previously unlinked
  //----------------------------------------------------------------------------
  void removeLocation(location_t location);

  //----------------------------------------------------------------------------
  //! Remove all locations that were previously unlinked
  //----------------------------------------------------------------------------
  void removeAllLocations();

  //----------------------------------------------------------------------------
  //! Get vector with all unlinked locations
  //----------------------------------------------------------------------------
  LocationVector getUnlinkedLocations() const;

  //----------------------------------------------------------------------------
  //! Unlink location
  //----------------------------------------------------------------------------
  void unlinkLocation(location_t location);

  //----------------------------------------------------------------------------
  //! Unlink all locations
  //----------------------------------------------------------------------------
  void unlinkAllLocations();

  //----------------------------------------------------------------------------
  //! Clear unlinked locations without notifying the listeners
  //----------------------------------------------------------------------------
  void clearUnlinkedLocations()
  {
    pUnlinkedLocation.clear();
  }

  //----------------------------------------------------------------------------
  //! Test the unlinkedlocation
  //----------------------------------------------------------------------------
  bool hasUnlinkedLocation(location_t location)
  {
    for (unsigned int i = 0; i < pUnlinkedLocation.size(); i++)
    {
      if (pUnlinkedLocation[i] == location)
        return true;
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Get number of unlinked locations
  //----------------------------------------------------------------------------
  size_t getNumUnlinkedLocation() const
  {
    return pUnlinkedLocation.size();
  }

  //----------------------------------------------------------------------------
  //! Clear locations without notifying the listeners
  //----------------------------------------------------------------------------
  void clearLocations()
  {
    pLocation.clear();
  }

  //----------------------------------------------------------------------------
  //! Test the location
  //----------------------------------------------------------------------------
  bool hasLocation(location_t location)
  {
    for (unsigned int i = 0; i < pLocation.size(); i++)
    {
      if (pLocation[i] == location)
        return true;
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Get number of location
  //----------------------------------------------------------------------------
  size_t getNumLocation() const
  {
    return pLocation.size();
  }

  //----------------------------------------------------------------------------
  //! Get uid
  //----------------------------------------------------------------------------
  uid_t getCUid() const
  {
    return pCUid;
  }

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  void setCUid(uid_t uid)
  {
    pCUid = uid;
  }

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  gid_t getCGid() const
  {
    return pCGid;
  }

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  void setCGid(gid_t gid)
  {
    pCGid = gid;
  }

  //----------------------------------------------------------------------------
  //! Get layout
  //----------------------------------------------------------------------------
  layoutId_t getLayoutId() const
  {
    return pLayoutId;
  }

  //----------------------------------------------------------------------------
  //! Set layout
  //----------------------------------------------------------------------------
  void setLayoutId(layoutId_t layoutId)
  {
    pLayoutId = layoutId;
  }

  //----------------------------------------------------------------------------
  //! Get flags
  //----------------------------------------------------------------------------
  uint16_t getFlags() const
  {
    return pFlags;
  }

  //----------------------------------------------------------------------------
  //! Get the n-th flag
  //----------------------------------------------------------------------------
  bool getFlag(uint8_t n)
  {
    return pFlags & (0x0001 << n);
  }

  //----------------------------------------------------------------------------
  //! Set flags
  //----------------------------------------------------------------------------
  void setFlags(uint16_t flags)
  {
    pFlags = flags;
  }

  //----------------------------------------------------------------------------
  //! Set the n-th flag
  //----------------------------------------------------------------------------
  void setFlag(uint8_t n, bool flag)
  {
    if (flag)
      pFlags |= (1 << n);
    else
      pFlags &= !(1 << n);
  }

  //----------------------------------------------------------------------------
  //! Env Representation
  //----------------------------------------------------------------------------
  void getEnv(std::string& env, bool escapeAnd = false);

  //----------------------------------------------------------------------------
  //! Set the FileMDSvc object
  //----------------------------------------------------------------------------
  void setFileMDSvc(IFileMDSvc* fileMDSvc)
  {
    pFileMDSvc = fileMDSvc;
  }

  //----------------------------------------------------------------------------
  //! Get the FileMDSvc object
  //----------------------------------------------------------------------------
  virtual IFileMDSvc* getFileMDSvc()
  {
    return pFileMDSvc;
  }

  //----------------------------------------------------------------------------
  //! Get symbolic link
  //----------------------------------------------------------------------------
  std::string getLink() const
  {
    return pLinkName;
  }

  //----------------------------------------------------------------------------
  //! Set symbolic link
  //----------------------------------------------------------------------------
  void setLink(std::string link_name)
  {
    pLinkName = link_name;
  }

  //----------------------------------------------------------------------------
  //! Check if symbolic link
  //----------------------------------------------------------------------------
  bool isLink() const
  {
    return pLinkName.length() ? true:false;
  }


 protected:
  //----------------------------------------------------------------------------
  //! Data members
  //----------------------------------------------------------------------------
  id_t                pId;
  ctime_t             pCTime;
  ctime_t             pMTime;
  uint64_t            pSize;
  IContainerMD::id_t  pContainerId;
  uid_t               pCUid;
  gid_t               pCGid;
  layoutId_t          pLayoutId;
  uint16_t            pFlags;
  std::string         pName;
  std::string         pLinkName;
  LocationVector      pLocation;
  LocationVector      pUnlinkedLocation;
  Buffer              pChecksum;
  IFileMDSvc*         pFileMDSvc;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FS_FILE_MD_HH__
