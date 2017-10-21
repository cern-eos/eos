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
#include <memory>
#include <string>
#include <map>
#include <set>
#include <sys/time.h>
#include <google/dense_hash_map>

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
  typedef google::dense_hash_map< std::string, id_t > ContainerMap;
  typedef google::dense_hash_map< std::string, id_t > FileMap;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IContainerMD()
  {
    mSubcontainers.set_deleted_key("");
    mFiles.set_deleted_key("");
    mSubcontainers.set_empty_key("##_EMPTY_##");
    mFiles.set_empty_key("##_EMPTY_##");
  };

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IContainerMD() {};

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual IContainerMD* clone() const = 0;

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual void InheritChildren(const IContainerMD& other)
  {
    return;
  }

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
  //! Find file
  //----------------------------------------------------------------------------
  virtual std::shared_ptr<IFileMD> findFile(const std::string& name) = 0;

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
  //! Clean up the entire contents for the container. Delete files and
  //! containers recurssively
  //----------------------------------------------------------------------------
  virtual void cleanUp() = 0;

  //----------------------------------------------------------------------------
  //! Get set of subcontainer names contained in the current object
  //!
  //! @return set of subcontainer names
  //----------------------------------------------------------------------------
  virtual std::set<std::string> getNameContainers() const = 0;

  //----------------------------------------------------------------------------
  //! Get iterator to the begining of the subcontainers map
  //----------------------------------------------------------------------------
  inline eos::IContainerMD::ContainerMap::const_iterator
  subcontainersBegin() const
  {
    return mSubcontainers.begin();
  }

  //----------------------------------------------------------------------------
  //! Get iterator to the end of the subcontainers map
  //----------------------------------------------------------------------------
  inline eos::IContainerMD::ContainerMap::const_iterator
  subcontainersEnd() const
  {
    return mSubcontainers.end();
  }

  //----------------------------------------------------------------------------
  //! Get iterator to the begining of the files map
  //----------------------------------------------------------------------------
  inline eos::IContainerMD::FileMap::const_iterator
  filesBegin() const
  {
    return mFiles.begin();
  }

  //----------------------------------------------------------------------------
  //! Get iterator to the end of the files map
  //----------------------------------------------------------------------------
  inline eos::IContainerMD::FileMap::const_iterator
  filesEnd() const
  {
    return mFiles.end();
  }

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
  };

protected:
  ContainerMap mSubcontainers; //! Directory name to id map
  FileMap mFiles; ///< File name to id map

private:

  //----------------------------------------------------------------------------
  //! Make copy constructor and assignment operator private to avoid "slicing"
  //! when dealing with derived classes.
  //----------------------------------------------------------------------------
  IContainerMD(const IContainerMD& other);

  IContainerMD& operator=(const IContainerMD& other);
};

EOSNSNAMESPACE_END

#endif // EOS_NS_ICONTAINER_MD_HH
