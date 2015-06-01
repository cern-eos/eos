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

#ifndef EOS_NS_FSCONTAINER_MD_HH
#define EOS_NS_FSCONTAINER_MD_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class representing a file-system container object
//------------------------------------------------------------------------------
class FsContainerMD: public IContainerMD
{
   public:
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    FsContainerMD();
  
    //--------------------------------------------------------------------------
    //! Desstructor
    //--------------------------------------------------------------------------
    virtual ~FsContainerMD();

    //--------------------------------------------------------------------------
    //! Virtual copy constructor
    //--------------------------------------------------------------------------
    IContainerMD* clone() const = 0;

    //--------------------------------------------------------------------------
    //! Add container
    //--------------------------------------------------------------------------
    void addContainer(IContainerMD* container) = 0;

    //--------------------------------------------------------------------------
    //! Remove container
    //--------------------------------------------------------------------------
    void removeContainer(const std::string& name) = 0;

    //--------------------------------------------------------------------------
    //! Find sub container
    //--------------------------------------------------------------------------
    IContainerMD* findContainer(const std::string& name) = 0;

    //--------------------------------------------------------------------------
    //! Get number of containers
    //--------------------------------------------------------------------------
    size_t getNumContainers() const = 0;

    //--------------------------------------------------------------------------
    //! Add file
    //--------------------------------------------------------------------------
    void addFile(IFileMD* file) = 0;

    //--------------------------------------------------------------------------
    //! Remove file
    //--------------------------------------------------------------------------
    void removeFile(const std::string& name) = 0;

    //--------------------------------------------------------------------------
    //! Find file
    //--------------------------------------------------------------------------
    IFileMD* findFile(const std::string& name) = 0;

    //--------------------------------------------------------------------------
    //! Get number of files
    //--------------------------------------------------------------------------
    size_t getNumFiles() const = 0;

    //--------------------------------------------------------------------------
    //! Get name
    //--------------------------------------------------------------------------
    const std::string& getName() const = 0;

    //--------------------------------------------------------------------------
    //! Set name
    //--------------------------------------------------------------------------
    void setName(const std::string& name) = 0;

    //--------------------------------------------------------------------------
    //! Get container id
    //--------------------------------------------------------------------------
    id_t getId() const = 0;

    //--------------------------------------------------------------------------
    //! Get parent id
    //--------------------------------------------------------------------------
    id_t getParentId() const = 0;

    //--------------------------------------------------------------------------
    //! Set parent id
    //--------------------------------------------------------------------------
    void setParentId(id_t parentId) = 0;

    //--------------------------------------------------------------------------
    //! Get the flags
    //--------------------------------------------------------------------------
    uint16_t& getFlags() = 0;

    //--------------------------------------------------------------------------
    //! Get the flags
    //--------------------------------------------------------------------------
    uint16_t getFlags() const = 0;

    //--------------------------------------------------------------------------
    //! Get creation time
    //--------------------------------------------------------------------------
    void getCTime(ctime_t& ctime) const = 0;

    //--------------------------------------------------------------------------
    //! Set creation time
    //--------------------------------------------------------------------------
    void setCTime(ctime_t ctime) = 0;

    //--------------------------------------------------------------------------
    //! Set creation time to now
    //--------------------------------------------------------------------------
    void setCTimeNow() = 0;


    //--------------------------------------------------------------------------
    //! Get uid
    //--------------------------------------------------------------------------
    uid_t getCUid() const = 0;

    //--------------------------------------------------------------------------
    //! Set uid
    //--------------------------------------------------------------------------
    void setCUid(uid_t uid) = 0;

    //--------------------------------------------------------------------------
    //! Get gid
    //--------------------------------------------------------------------------
    gid_t getCGid() const = 0;

    //--------------------------------------------------------------------------
    //! Set gid
    //--------------------------------------------------------------------------
    void setCGid(gid_t gid) = 0;

    //--------------------------------------------------------------------------
    //! Get mode
    //--------------------------------------------------------------------------
    mode_t getMode() const = 0;

    //--------------------------------------------------------------------------
    //! Set mode
    //--------------------------------------------------------------------------
    void setMode(mode_t mode) = 0;

    //--------------------------------------------------------------------------
    //! Get ACL Id
    //--------------------------------------------------------------------------
    uint16_t getACLId() const = 0;

    //--------------------------------------------------------------------------
    //! Set ACL Id
    //--------------------------------------------------------------------------
    void setACLId(uint16_t ACLId) = 0;

    //--------------------------------------------------------------------------
    //! Get the attribute
    //--------------------------------------------------------------------------
    std::string getAttribute(const std::string& name) const = 0;

    //--------------------------------------------------------------------------
    //! Add extended attribute
    //--------------------------------------------------------------------------
    void setAttribute(const std::string& name,
                      const std::string& value) = 0;

    //--------------------------------------------------------------------------
    //! Remove attribute
    //--------------------------------------------------------------------------
    void removeAttribute(const std::string& name) = 0;

    //--------------------------------------------------------------------------
    //! Check if the attribute exist
    //--------------------------------------------------------------------------
    bool hasAttribute(const std::string& name) const = 0;

    //--------------------------------------------------------------------------
    //! Return number of attributes
    //--------------------------------------------------------------------------
    size_t numAttributes() const = 0;

    //--------------------------------------------------------------------------
    //! Get attribute begin iterator
    //--------------------------------------------------------------------------
    XAttrMap::iterator attributesBegin() = 0;

    //--------------------------------------------------------------------------
    //! Get the attribute end iterator
    //--------------------------------------------------------------------------
    XAttrMap::iterator attributesEnd() = 0;

    //--------------------------------------------------------------------------
    //! Check the access permissions
    //!
    //! @return true if all the requested rights are granted, false otherwise
    //--------------------------------------------------------------------------
    bool access(uid_t uid, gid_t gid, int flags = 0) = 0;

    //--------------------------------------------------------------------------
    //! Clean up the entire contents for the container. Delete files and
    //! containers recurssively
    //!
    //! @param cmd_svc container metadata service
    //! @param fmd_svc file metadata service
    //!
    //--------------------------------------------------------------------------
    void cleanUp(IContainerMDSvc* cmd_svc, IFileMDSvc* fmd_svc) = 0;

    //--------------------------------------------------------------------------
    //! Serialize the object to a buffer
    //--------------------------------------------------------------------------
    void serialize(Buffer& buffer) = 0;

    //--------------------------------------------------------------------------
    //! Deserialize the class to a buffer
    //--------------------------------------------------------------------------
    void deserialize(Buffer& buffer) = 0;

    //--------------------------------------------------------------------------
    //! Get pointer to first subcontainer. *MUST* be used in conjunction with
    //! nextContainer to iterate over the list of subcontainers.
    //!
    //! @return pointer to first subcontainer or 0 if no subcontainers
    //--------------------------------------------------------------------------
    IContainerMD* beginSubContainer() = 0;

    //--------------------------------------------------------------------------
    //! Get pointer to the next subcontainer object. *MUST* be used in conjunction
    //! with beginContainers to iterate over the list of subcontainers.
    //!
    //! @return pointer to next subcontainer or 0 if no subcontainers
    //--------------------------------------------------------------------------
    IContainerMD* nextSubContainer() = 0;

    //--------------------------------------------------------------------------
    //! Get pointer to first file in the container. *MUST* be used in conjunction
    //! with nextFile to iterate over the list of files.
    //!
    //! @return pointer to the first file or 0 if no files
    //--------------------------------------------------------------------------
    IFileMD* beginFile() = 0;

    //--------------------------------------------------------------------------
    //! Get pointer to the next file object. *MUST* be used in conjunction
    //! with beginFiles to iterate over the list of files.
    //!
    //! @return pointer to next file or 0 if no files
    //--------------------------------------------------------------------------
    IFileMD* nextFile() = 0;
};

EOSNSNAMESPACE_END

#endif // EOS_NS_FSCONTAINER_MD_HH
