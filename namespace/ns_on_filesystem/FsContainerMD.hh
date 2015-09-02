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
//! @brief  Class representing the FS container object
//------------------------------------------------------------------------------

#ifndef __EOS_NS_FS_CONTAINER_MD_HH__
#define __EOS_NS_FS_CONTAINER_MD_HH__

#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class representing a file-system container object
//------------------------------------------------------------------------------
class FsContainerMD: public IContainerMD
{
 public:
  //----------------------------------------------------------------------------
  //! Type definitions
  //----------------------------------------------------------------------------
  typedef google::dense_hash_map<std::string, IContainerMD*> ContainerMap;
  typedef google::dense_hash_map<std::string, IFileMD*>      FileMap;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param path full path on the file-system for this container
  //----------------------------------------------------------------------------
  FsContainerMD(const std::string& path);

  //----------------------------------------------------------------------------
  //! Desstructor
  //----------------------------------------------------------------------------
  virtual ~FsContainerMD();

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  FsContainerMD(const ContainerMD& other);

  //----------------------------------------------------------------------------
  //! Assignment operator
  //----------------------------------------------------------------------------
  FsContainerMD& operator= (const ContainerMD& other);

  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  IContainerMD* clone() const;

  //----------------------------------------------------------------------------
  //! Add container
  //----------------------------------------------------------------------------
  void addContainer(IContainerMD* container);

  //----------------------------------------------------------------------------
  //! Remove container
  //----------------------------------------------------------------------------
  void removeContainer(const std::string& name);

  //----------------------------------------------------------------------------
  //! Find sub container
  //----------------------------------------------------------------------------
  IContainerMD* findContainer(const std::string& name);

  //----------------------------------------------------------------------------
  //! Get number of containers
  //----------------------------------------------------------------------------
  size_t getNumContainers() const;

  //----------------------------------------------------------------------------
  //! Add file
  //----------------------------------------------------------------------------
  void addFile(IFileMD* file);

  //----------------------------------------------------------------------------
  //! Remove file
  //----------------------------------------------------------------------------
  void removeFile(const std::string& name);

  //----------------------------------------------------------------------------
  //! Find file
  //----------------------------------------------------------------------------
  IFileMD* findFile(const std::string& name);

  //----------------------------------------------------------------------------
  //! Get number of files
  //----------------------------------------------------------------------------
  size_t getNumFiles() const;

  //----------------------------------------------------------------------------
  //! Get full path to container
  //!
  //! @return container full path
  //----------------------------------------------------------------------------
  const std::string& getName() const;

  //----------------------------------------------------------------------------
  //! Set container path
  //!
  //! @param rel_path relative path with respect to the namespace mountpoint
  //----------------------------------------------------------------------------
  void setName(const std::string& rel_path);

  //----------------------------------------------------------------------------
  //! Get container id
  //----------------------------------------------------------------------------
  id_t getId() const;

  //----------------------------------------------------------------------------
  //! Get parent id
  //----------------------------------------------------------------------------
  id_t getParentId() const;

  //----------------------------------------------------------------------------
  //! Set parent id
  //----------------------------------------------------------------------------
  void setParentId(id_t parentId);

  //----------------------------------------------------------------------------
  //! Get the flags
  //----------------------------------------------------------------------------
  uint16_t& getFlags();

  //----------------------------------------------------------------------------
  //! Get the flags
  //----------------------------------------------------------------------------
  uint16_t getFlags() const;

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
  //! Get uid
  //----------------------------------------------------------------------------
  uid_t getCUid() const;

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  void setCUid(uid_t uid);

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  gid_t getCGid() const;

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  void setCGid(gid_t gid);

  //----------------------------------------------------------------------------
  //! Get mode
  //----------------------------------------------------------------------------
  mode_t getMode() const;

  //----------------------------------------------------------------------------
  //! Set mode
  //----------------------------------------------------------------------------
  void setMode(mode_t mode);

  //----------------------------------------------------------------------------
  //! Get ACL Id
  //----------------------------------------------------------------------------
  uint16_t getACLId() const;

  //----------------------------------------------------------------------------
  //! Set ACL Id
  //----------------------------------------------------------------------------
  void setACLId(uint16_t ACLId);

  //----------------------------------------------------------------------------
  //! Get the attribute
  //----------------------------------------------------------------------------
  std::string getAttribute(const std::string& name) const;

  //----------------------------------------------------------------------------
  //! Add extended attribute
  //----------------------------------------------------------------------------
  void setAttribute(const std::string& name,
                    const std::string& value);

  //----------------------------------------------------------------------------
  //! Remove attribute
  //----------------------------------------------------------------------------
  void removeAttribute(const std::string& name);

  //----------------------------------------------------------------------------
  //! Check if the attribute exist
  //----------------------------------------------------------------------------
  bool hasAttribute(const std::string& name) const;

  //----------------------------------------------------------------------------
  //! Return number of attributes
  //----------------------------------------------------------------------------
  size_t numAttributes() const;

  //----------------------------------------------------------------------------
  //! Get attribute begin iterator
  //----------------------------------------------------------------------------
  XAttrMap::iterator attributesBegin();

  //----------------------------------------------------------------------------
  //! Get the attribute end iterator
  //----------------------------------------------------------------------------
  XAttrMap::iterator attributesEnd();

  //----------------------------------------------------------------------------
  //! Check the access permissions
  //!
  //! @return true if all the requested rights are granted, false otherwise
  //----------------------------------------------------------------------------
  bool access(uid_t uid, gid_t gid, int flags = 0);

  //----------------------------------------------------------------------------
  //! Clean up the entire contents for the container. Delete files and
  //! containers recurssively
  //!
  //! @param cmd_svc container metadata service
  //! @param fmd_svc file metadata service
  //!
  //----------------------------------------------------------------------------
  void cleanUp(IContainerMDSvc* cmd_svc, IFileMDSvc* fmd_svc);

  //----------------------------------------------------------------------------
  //! Get pointer to first subcontainer. *MUST* be used in conjunction with
  //! nextContainer to iterate over the list of subcontainers.
  //!
  //! @return pointer to first subcontainer or 0 if no subcontainers
  //----------------------------------------------------------------------------
  IContainerMD* beginSubContainer();

  //----------------------------------------------------------------------------
  //! Get pointer to the next subcontainer object. *MUST* be used in conjunction
  //! with beginContainers to iterate over the list of subcontainers.
  //!
  //! @return pointer to next subcontainer or 0 if no subcontainers
  //----------------------------------------------------------------------------
  IContainerMD* nextSubContainer();

  //----------------------------------------------------------------------------
  //! Get pointer to first file in the container. *MUST* be used in conjunction
  //! with nextFile to iterate over the list of files.
  //!
  //! @return pointer to the first file or 0 if no files
  //----------------------------------------------------------------------------
  IFileMD* beginFile();

  //----------------------------------------------------------------------------
  //! Get pointer to the next file object. *MUST* be used in conjunction
  //! with beginFiles to iterate over the list of files.
  //!
  //! @return pointer to next file or 0 if no files
  //----------------------------------------------------------------------------
  IFileMD* nextFile();

 protected:

  std::string  mFullPath;
  FileMap      mFiles;
  ContainerMap mSubContainers;
  time_t       mMtime;
  time_t       mCtime;
  struct stat  mInfo;

 private:

  //----------------------------------------------------------------------------
  //! Get all entries from current container
  //----------------------------------------------------------------------------
  void getEntries();
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FS_CONTAINER_MD_HH__
