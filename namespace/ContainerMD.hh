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
// desc:   Class representing the container metadata
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_MD_HH
#define EOS_NS_CONTAINER_MD_HH

#include <stdint.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <google/sparse_hash_map>
#include <google/dense_hash_map>
#include <map>
#include <sys/time.h>

#include "namespace/IContainerMD.hh"
#include "namespace/persistency/Buffer.hh"

namespace eos
{

//----------------------------------------------------------------------------
//! Class holding the metadata information concerning a single container
//----------------------------------------------------------------------------
class ContainerMD: public IContainerMD
{
  public:
    //------------------------------------------------------------------------
    // Type definitions
    //------------------------------------------------------------------------
    typedef google::dense_hash_map<std::string, IContainerMD*> ContainerMap;
    typedef google::dense_hash_map<std::string, IFileMD*>      FileMap;

    //------------------------------------------------------------------------
    //! Constructor
    //------------------------------------------------------------------------
    ContainerMD(id_t id);

    //------------------------------------------------------------------------
    //! Copy constructor
    //------------------------------------------------------------------------
    ContainerMD(const ContainerMD& other);

    //------------------------------------------------------------------------
    //! Assignment operator
    //------------------------------------------------------------------------
    ContainerMD& operator= (const ContainerMD& other);

    //------------------------------------------------------------------------
    //! Copy constructor from IContainerMD
    //------------------------------------------------------------------------
    ContainerMD(const IContainerMD* other);

    //------------------------------------------------------------------------
    //! Assignment operator from IContainerMD
    //------------------------------------------------------------------------
    ContainerMD& operator = (const IContainerMD* other);

    //------------------------------------------------------------------------
    //! Add container
    //------------------------------------------------------------------------
    virtual void addContainer(IContainerMD* container);

    //------------------------------------------------------------------------
    //! Remove container
    //------------------------------------------------------------------------
    virtual void removeContainer(const std::string& name);

    //------------------------------------------------------------------------
    //! Find sub container
    //------------------------------------------------------------------------
    virtual IContainerMD* findContainer(const std::string& name);

    //------------------------------------------------------------------------
    //! Get number of containers
    //------------------------------------------------------------------------
    size_t getNumContainers() const
    {
      return pSubContainers.size();
    }

    //------------------------------------------------------------------------
    //! Get the start iterator to the container list
    //------------------------------------------------------------------------
    ContainerMap::iterator containersBegin()
    {
      return pSubContainers.begin();
    }

    //------------------------------------------------------------------------
    //! Get the end iterator of the contaienr list
    //------------------------------------------------------------------------
    ContainerMap::iterator containersEnd()
    {
      return pSubContainers.end();
    }

    //------------------------------------------------------------------------
    //! Add file
    //------------------------------------------------------------------------
    virtual void addFile(IFileMD* file);

    //------------------------------------------------------------------------
    //! Remove file
    //------------------------------------------------------------------------
    virtual void removeFile(const std::string& name);

    //------------------------------------------------------------------------
    //! Find file
    //------------------------------------------------------------------------
    virtual IFileMD* findFile(const std::string& name);

    //------------------------------------------------------------------------
    //! Get number of files
    //------------------------------------------------------------------------
    virtual size_t getNumFiles() const
    {
      return pFiles.size();
    }

    //------------------------------------------------------------------------
    //! Get the start iterator to the file list
    //------------------------------------------------------------------------
    FileMap::iterator filesBegin()
    {
      return pFiles.begin();
    }

    //------------------------------------------------------------------------
    //! Get the end iterator of the contaienr list
    //------------------------------------------------------------------------
    FileMap::iterator filesEnd()
    {
      return pFiles.end();
    }

    //------------------------------------------------------------------------
    //! Get container id
    //------------------------------------------------------------------------
    id_t getId() const
    {
      return pId;
    }

    //------------------------------------------------------------------------
    //! Get parent id
    //------------------------------------------------------------------------
    id_t getParentId() const
    {
      return pParentId;
    }

    //------------------------------------------------------------------------
    //! Set parent id
    //------------------------------------------------------------------------
    void setParentId(id_t parentId)
    {
      pParentId = parentId;
    }

    //------------------------------------------------------------------------
    //! Get the flags
    //------------------------------------------------------------------------
    uint16_t& getFlags()
    {
      return pFlags;
    }

    //------------------------------------------------------------------------
    //! Get the flags
    //------------------------------------------------------------------------
    uint16_t getFlags() const
    {
      return pFlags;
    }

    //------------------------------------------------------------------------
    //! Set creation time
    //------------------------------------------------------------------------
    void setCTime(ctime_t ctime)
    {
      pCTime.tv_sec = ctime.tv_sec;
      pCTime.tv_nsec = ctime.tv_nsec;
    }

    //------------------------------------------------------------------------
    //! Set creation time to now
    //------------------------------------------------------------------------
    void setCTimeNow()
    {
#ifdef __APPLE__
      struct timeval tv;
      gettimeofday(&tv, 0);
      pCTime.tv_sec = tv.tv_sec;
      pCTime.tv_nsec = tv.tv_usec * 1000;
#else
      clock_gettime(CLOCK_REALTIME, &pCTime);
#endif
    }

    //------------------------------------------------------------------------
    //! Get creation time
    //------------------------------------------------------------------------
    void getCTime(ctime_t& ctime) const
    {
      ctime.tv_sec = pCTime.tv_sec;
      ctime.tv_nsec = pCTime.tv_nsec;
    }

    //------------------------------------------------------------------------
    //! Get name
    //------------------------------------------------------------------------
    const std::string& getName() const
    {
      return pName;
    }

    //------------------------------------------------------------------------
    //! Set name
    //------------------------------------------------------------------------
    void setName(const std::string& name)
    {
      pName = name;
    }

    //------------------------------------------------------------------------
    //! Get uid
    //------------------------------------------------------------------------
    uid_t getCUid() const
    {
      return pCUid;
    }

    //------------------------------------------------------------------------
    //! Set uid
    //------------------------------------------------------------------------
    void setCUid(uid_t uid)
    {
      pCUid = uid;
    }

    //------------------------------------------------------------------------
    //! Get gid
    //------------------------------------------------------------------------
    gid_t getCGid() const
    {
      return pCGid;
    }

    //------------------------------------------------------------------------
    //! Set gid
    //------------------------------------------------------------------------
    void setCGid(gid_t gid)
    {
      pCGid = gid;
    }

    //------------------------------------------------------------------------
    //! Get mode
    //------------------------------------------------------------------------
    mode_t getMode() const
    {
      return pMode;
    }

    //------------------------------------------------------------------------
    //! Set mode
    //------------------------------------------------------------------------
    void setMode(mode_t mode)
    {
      pMode = mode;
    }

    //------------------------------------------------------------------------
    //! Get ACL Id
    //------------------------------------------------------------------------
    uint16_t getACLId() const
    {
      return pACLId;
    }

    //------------------------------------------------------------------------
    //! Set ACL Id
    //------------------------------------------------------------------------
    void setACLId(uint16_t ACLId)
    {
      pACLId = ACLId;
    }

    //------------------------------------------------------------------------
    //! Add extended attribute
    //------------------------------------------------------------------------
    void setAttribute(const std::string& name, const std::string& value)
    {
      pXAttrs[name] = value;
    }

    //------------------------------------------------------------------------
    //! Remove attribute
    //------------------------------------------------------------------------
    void removeAttribute(const std::string& name)
    {
      XAttrMap::iterator it = pXAttrs.find(name);

      if (it != pXAttrs.end())
        pXAttrs.erase(it);
    }

    //------------------------------------------------------------------------
    //! Check if the attribute exist
    //------------------------------------------------------------------------
    bool hasAttribute(const std::string& name) const
    {
      return pXAttrs.find(name) != pXAttrs.end();
    }

    //------------------------------------------------------------------------
    //! Return number of attributes
    //------------------------------------------------------------------------
    size_t numAttributes() const
    {
      return pXAttrs.size();
    }

    //------------------------------------------------------------------------
    // Get the attribute
    //------------------------------------------------------------------------
    std::string getAttribute(const std::string& name) const
    throw(MDException)
    {
      XAttrMap::const_iterator it = pXAttrs.find(name);

      if (it == pXAttrs.end())
      {
        MDException e(ENOENT);
        e.getMessage() << "Attribute: " << name << " not found";
        throw e;
      }

      return it->second;
    }

    //------------------------------------------------------------------------
    //! Get attribute begin iterator
    //------------------------------------------------------------------------
    XAttrMap::iterator attributesBegin()
    {
      return pXAttrs.begin();
    }

    //------------------------------------------------------------------------
    //! Get the attribute end iterator
    //------------------------------------------------------------------------
    XAttrMap::iterator attributesEnd()
    {
      return pXAttrs.end();
    }

    //--------------------------------------------------------------------------
    //! Check the access permissions
    //!
    //! @return true if all the requested rights are granted, false otherwise
    //--------------------------------------------------------------------------
    virtual bool access(uid_t uid, gid_t gid, int flags = 0);

    //------------------------------------------------------------------------
    //! Clean up the entire contents for the container. Delete files and
    //! containers recurssively
    //!
    //! @param cont_svc container metadata service
    //! @param file_svc file metadata service
    //!
    //------------------------------------------------------------------------
    virtual void cleanUp(IContainerMDSvc* cont_svc, IFileMDSvc* file_svc);

    //------------------------------------------------------------------------
    //! Serialize the object to a buffer
    //------------------------------------------------------------------------
    virtual void serialize(Buffer& buffer) throw(MDException);

    //------------------------------------------------------------------------
    //! Deserialize the class to a buffer
    //------------------------------------------------------------------------
    virtual void deserialize(Buffer& buffer) throw(MDException);

  protected:
    id_t         pId;
    id_t         pParentId;
    uint16_t     pFlags;
    ctime_t      pCTime;
    std::string  pName;
    uid_t        pCUid;
    gid_t        pCGid;
    mode_t       pMode;
    uint16_t     pACLId;
    XAttrMap     pXAttrs;
    ContainerMap pSubContainers;
    FileMap      pFiles;
};
}

#endif // EOS_NS_CONTAINER_MD_HH
