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

#include "namespace/Namespace.hh"
#include "namespace/utils/Buffer.hh"
#include "namespace/MDException.hh"
#include <stdint.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <sys/time.h>

EOSNSNAMESPACE_BEGIN

//! Forward declarations
class IContainerMDSvc;
class IFileMDSvc;
class IFileMD;

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

  //----------------------------------------------------------------------------
  //! Desstructor
  //----------------------------------------------------------------------------
  virtual ~IContainerMD() {};

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual IContainerMD* clone() const = 0;

  //----------------------------------------------------------------------------
  //! Add container
  //----------------------------------------------------------------------------
  virtual void addContainer(IContainerMD* container) = 0;

  //----------------------------------------------------------------------------
  //! Remove container
  //----------------------------------------------------------------------------
  virtual void removeContainer(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find sub container
  //----------------------------------------------------------------------------
  virtual IContainerMD* findContainer(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  virtual size_t getNumContainers() const = 0;

  //----------------------------------------------------------------------------
  //! Add file
  //----------------------------------------------------------------------------
  virtual void addFile(IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Remove file
  //----------------------------------------------------------------------------
  virtual void removeFile(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Find file
  //----------------------------------------------------------------------------
  virtual IFileMD* findFile(const std::string& name) = 0;

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  virtual size_t getNumFiles() const = 0;

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
  virtual id_t getId() const = 0;

  //----------------------------------------------------------------------------
  //! Get parent id
  //----------------------------------------------------------------------------
  virtual id_t getParentId() const = 0;

  //----------------------------------------------------------------------------
  //! Set parent id
  //----------------------------------------------------------------------------
  virtual void setParentId(id_t parentId) = 0;

  //----------------------------------------------------------------------------
  //! Get the flags
  //----------------------------------------------------------------------------
  virtual uint16_t& getFlags() = 0;

  //----------------------------------------------------------------------------
  //! Get the flags
  //----------------------------------------------------------------------------
  virtual uint16_t getFlags() const = 0;

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
  virtual void notifyMTimeChange(IContainerMDSvc *containerMDSvc) = 0;

  //----------------------------------------------------------------------------
  //! Get modification time
  //----------------------------------------------------------------------------
  virtual void getMTime(mtime_t &mtime) const  = 0;

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
  virtual void getTMTime(tmtime_t &tmtime) const = 0;

  //----------------------------------------------------------------------------
  //! Get tree size
  //----------------------------------------------------------------------------
  virtual uint64_t getTreeSize() const = 0;

  //----------------------------------------------------------------------------
  //! Set tree size
  //----------------------------------------------------------------------------
  virtual void setTreeSize(uint64_t treesize) = 0;

  //----------------------------------------------------------------------------
  //! Add to tree size
  //----------------------------------------------------------------------------
  virtual uint64_t addTreeSize(uint64_t addsize) = 0;

  //----------------------------------------------------------------------------
  //! Remove from tree size
  //----------------------------------------------------------------------------
  virtual uint64_t removeTreeSize(uint64_t removesize) = 0;

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
  //! Get mode
  //----------------------------------------------------------------------------
  virtual mode_t getMode() const = 0;

  //----------------------------------------------------------------------------
  //! Set mode
  //----------------------------------------------------------------------------
  virtual void setMode(mode_t mode) = 0;

  //----------------------------------------------------------------------------
  //! Get ACL Id
  //----------------------------------------------------------------------------
  virtual uint16_t getACLId() const = 0;

  //----------------------------------------------------------------------------
  //! Set ACL Id
  //----------------------------------------------------------------------------
  virtual void setACLId(uint16_t ACLId) = 0;

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
  //! Get attribute begin iterator
  //----------------------------------------------------------------------------
  virtual XAttrMap::iterator attributesBegin() = 0;

  //----------------------------------------------------------------------------
  //! Get the attribute end iterator
  //----------------------------------------------------------------------------
  virtual XAttrMap::iterator attributesEnd() = 0;

  //----------------------------------------------------------------------------
  //! Check the access permissions
  //!
  //! @return true if all the requested rights are granted, false otherwise
  //----------------------------------------------------------------------------
  virtual bool access(uid_t uid, gid_t gid, int flags = 0) = 0;

  //----------------------------------------------------------------------------
  //! Clean up the entire contents for the container. Delete files and
  //! containers recurssively
  //!
  //! @param cmd_svc container metadata service
  //! @param fmd_svc file metadata service
  //!
  //----------------------------------------------------------------------------
  virtual void cleanUp(IContainerMDSvc* cmd_svc, IFileMDSvc* fmd_svc) = 0;

  //----------------------------------------------------------------------------
  //! Get pointer to first subcontainer. *MUST* be used in conjunction with
  //! nextContainer to iterate over the list of subcontainers.
  //!
  //! @return pointer to first subcontainer or 0 if no subcontainers
  //----------------------------------------------------------------------------
  virtual IContainerMD* beginSubContainer() = 0;

  //----------------------------------------------------------------------------
  //! Get pointer to the next subcontainer object. *MUST* be used in conjunction
  //! with beginContainers to iterate over the list of subcontainers.
  //!
  //! @return pointer to next subcontainer or 0 if no subcontainers
  //----------------------------------------------------------------------------
  virtual IContainerMD* nextSubContainer() = 0;

  //----------------------------------------------------------------------------
  //! Get pointer to first file in the container. *MUST* be used in conjunction
  //! with nextFile to iterate over the list of files.
  //!
  //! @return pointer to the first file or 0 if no files
  //----------------------------------------------------------------------------
  virtual IFileMD* beginFile() = 0;

  //----------------------------------------------------------------------------
  //! Get pointer to the next file object. *MUST* be used in conjunction
  //! with beginFiles to iterate over the list of files.
  //!
  //! @return pointer to next file or 0 if no files
  //----------------------------------------------------------------------------
  virtual IFileMD* nextFile() = 0;
};

EOSNSNAMESPACE_END

#endif // EOS_NS_ICONTAINER_MD_HH
