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

#include "namespace/ns_in_memory/ContainerMD.hh"
#include "namespace/ns_in_memory/FileMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <sys/stat.h>

namespace eos
{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(id_t id):
  IContainerMD(),
  pId(id),
  pParentId(0),
  pFlags(0),
  pName(""),
  pCUid(0),
  pCGid(0),
  pMode(040755),
  pACLId(0)
{
  pCTime.tv_sec = 0;
  pCTime.tv_nsec = 0;
  pSubContainers.set_deleted_key("");
  pFiles.set_deleted_key("");
  pSubContainers.set_empty_key("##_EMPTY_##");
  pFiles.set_empty_key("##_EMPTY_##");
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
ContainerMD*
ContainerMD::clone() const
{
  return new ContainerMD(*this);
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(const ContainerMD& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Asignment operator
//------------------------------------------------------------------------------
ContainerMD& ContainerMD::operator= (const ContainerMD& other)
{
  pId       = other.pId;
  pParentId = other.pParentId;
  pFlags    = other.pFlags;
  pCTime    = other.pCTime;
  pName     = other.pName;
  pCUid     = other.pCUid;
  pCGid     = other.pCGid;
  pMode     = other.pMode;
  pACLId    = other.pACLId;
  pXAttrs   = other.pXAttrs;
  pFlags    = other.pFlags;
  return *this;
}

//------------------------------------------------------------------------------
// Find sub container
//------------------------------------------------------------------------------
IContainerMD*
ContainerMD::findContainer(const std::string& name)
{
  ContainerMap::iterator it = pSubContainers.find(name);

  if (it == pSubContainers.end())
    return 0;

  return it->second;
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
ContainerMD::removeContainer(const std::string& name)
{
  pSubContainers.erase(name);
}

//------------------------------------------------------------------------------
// Add container
//------------------------------------------------------------------------------
void
ContainerMD::addContainer(IContainerMD* container)
{
  container->setParentId(pId);
  pSubContainers[container->getName()] = container;
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
IFileMD*
ContainerMD::findFile(const std::string& name)
{
  FileMap::iterator it = pFiles.find(name);

  if (it == pFiles.end())
    return 0;

  return it->second;
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
ContainerMD::addFile(IFileMD* file)
{
  file->setContainerId(pId);
  pFiles[file->getName()] = file;
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
void
ContainerMD::removeFile(const std::string& name)
{
  pFiles.erase(name);
}

//------------------------------------------------------------------------
// Clean up the entire contents for the container. Delete files and
// containers recurssively
//------------------------------------------------------------------------
void
ContainerMD::cleanUp(IContainerMDSvc* cont_svc, IFileMDSvc* file_svc)
{
  for (auto itf = pFiles.begin(); itf != pFiles.end(); ++itf)
    file_svc->removeFile(itf->second);

  for (auto itc = pSubContainers.begin(); itc != pSubContainers.end(); ++itc)
  {
    (void) itc->second->cleanUp(cont_svc, file_svc);
    cont_svc->removeContainer(itc->second);
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::serialize(Buffer& buffer) throw(MDException)
{
  buffer.putData(&pId,       sizeof(pId));
  buffer.putData(&pParentId, sizeof(pParentId));
  buffer.putData(&pFlags,    sizeof(pFlags));
  buffer.putData(&pCTime,    sizeof(pCTime));
  buffer.putData(&pCUid,     sizeof(pCUid));
  buffer.putData(&pCGid,     sizeof(pCGid));
  buffer.putData(&pMode,     sizeof(pMode));
  buffer.putData(&pACLId,    sizeof(pACLId));
  uint16_t len = pName.length() + 1;
  buffer.putData(&len,          2);
  buffer.putData(pName.c_str(), len);
  len = pXAttrs.size();
  buffer.putData(&len, sizeof(len));
  XAttrMap::iterator it;

  for (it = pXAttrs.begin(); it != pXAttrs.end(); ++it)
  {
    uint16_t strLen = it->first.length() + 1;
    buffer.putData(&strLen, sizeof(strLen));
    buffer.putData(it->first.c_str(), strLen);
    strLen = it->second.length() + 1;
    buffer.putData(&strLen, sizeof(strLen));
    buffer.putData(it->second.c_str(), strLen);
  }
}

//------------------------------------------------------------------------------
// Deserialize the class to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::deserialize(Buffer& buffer) throw(MDException)
{
  uint16_t offset = 0;
  offset = buffer.grabData(offset, &pId,       sizeof(pId));
  offset = buffer.grabData(offset, &pParentId, sizeof(pParentId));
  offset = buffer.grabData(offset, &pFlags,    sizeof(pFlags));
  offset = buffer.grabData(offset, &pCTime,    sizeof(pCTime));
  offset = buffer.grabData(offset, &pCUid,     sizeof(pCUid));
  offset = buffer.grabData(offset, &pCGid,     sizeof(pCGid));
  offset = buffer.grabData(offset, &pMode,     sizeof(pMode));
  offset = buffer.grabData(offset, &pACLId,    sizeof(pACLId));
  uint16_t len;
  offset = buffer.grabData(offset, &len, 2);
  char strBuffer[len];
  offset = buffer.grabData(offset, strBuffer, len);
  pName = strBuffer;
  uint16_t len1 = 0;
  uint16_t len2 = 0;
  len = 0;
  offset = buffer.grabData(offset, &len, sizeof(len));

  for (uint16_t i = 0; i < len; ++i)
  {
    offset = buffer.grabData(offset, &len1, sizeof(len1));
    char strBuffer1[len1];
    offset = buffer.grabData(offset, strBuffer1, len1);
    offset = buffer.grabData(offset, &len2, sizeof(len2));
    char strBuffer2[len2];
    offset = buffer.grabData(offset, strBuffer2, len2);
    pXAttrs.insert(std::make_pair <char*, char*>(strBuffer1, strBuffer2));
  }
}

//------------------------------------------------------------------------------
// Access checking helpers
//------------------------------------------------------------------------------
#define CANREAD  0x01
#define CANWRITE 0x02
#define CANENTER 0x04

static char convertModetUser(mode_t mode)
{
  char perms = 0;
  if (mode & S_IRUSR) perms |= CANREAD;
  if (mode & S_IWUSR) perms |= CANWRITE;
  if (mode & S_IXUSR) perms |= CANENTER;
  return perms;
}

static char convertModetGroup(mode_t mode)
{
  char perms = 0;
  if (mode & S_IRGRP) perms |= CANREAD;
  if (mode & S_IWGRP) perms |= CANWRITE;
  if (mode & S_IXGRP) perms |= CANENTER;
  return perms;
}

static char convertModetOther(mode_t mode)
{
  char perms = 0;
  if (mode & S_IROTH) perms |= CANREAD;
  if (mode & S_IWOTH) perms |= CANWRITE;
  if (mode & S_IXOTH) perms |= CANENTER;
  return perms;
}

static bool checkPerms(char actual, char requested)
{
  for (int i = 0; i < 3; ++i)
    if (requested & (1 << i))
      if (!(actual & (1 << i)))
        return false;

  return true;
}

//------------------------------------------------------------------------------
// Check the access permissions
//------------------------------------------------------------------------------
bool
ContainerMD::access(uid_t uid, gid_t gid, int flags)
{
  // root can do everything
  if (uid == 0)
    return true;

  // daemon can read everything
  if ((uid == 2) && (!(flags & W_OK)))
    return true;

  // Convert the flags
  char convFlags = 0;
  if (flags & R_OK) convFlags |= CANREAD;
  if (flags & W_OK) convFlags |= CANWRITE;
  if (flags & X_OK) convFlags |= CANENTER;

  // Check the perms
  if (uid == pCUid)
  {
    char user = convertModetUser(pMode);
    return checkPerms(user, convFlags);
  }

  if (gid == pCGid)
  {
    char group = convertModetGroup(pMode);
    return checkPerms(group, convFlags);
  }

  char other = convertModetOther(pMode);
  return checkPerms(other, convFlags);
}

//------------------------------------------------------------------------------
// Get pointer to first subcontainer. Must be used in conjunction with
// nextContainer to iterate over the list of subcontainers.
//------------------------------------------------------------------------------
IContainerMD*
ContainerMD::beginSubContainer()
{
  if (pSubContainers.empty())
  {
    pIterContainer = pSubContainers.end();
    return (IContainerMD*)(0);
  }
  else
  {
    pIterContainer = pSubContainers.begin();
    return pIterContainer->second;
  }
}

//------------------------------------------------------------------------------
// Get pointer to the next subcontainer object. Must be used in conjunction
// with beginContainers to iterate over the list of subcontainers.
//------------------------------------------------------------------------------
IContainerMD*
ContainerMD::nextSubContainer()
{
  if (pIterContainer == pSubContainers.end())
    return (IContainerMD*)(0);

  ++pIterContainer;

  if (pIterContainer == pSubContainers.end())
    return (IContainerMD*)(0);
  else
    return pIterContainer->second;
}

//------------------------------------------------------------------------------
// Get pointer to first file in the container. Must be used in conjunction
// with nextFile to iterate over the list of files.
//------------------------------------------------------------------------------
IFileMD*
ContainerMD::beginFile()
{
  if (pFiles.empty())
  {
    pIterFile = pFiles.end();
    return (IFileMD*)(0);
  }
  else
  {
    pIterFile = pFiles.begin();
    return pIterFile->second;
  }
}

//------------------------------------------------------------------------------
// Get pointer to the next file object. Must be used in conjunction
// with beginFiles to iterate over the list of files.
//------------------------------------------------------------------------------
IFileMD*
ContainerMD::nextFile()
{
  if (pIterFile == pFiles.end())
    return (IFileMD*)(0);

  ++pIterFile;

  if (pIterFile == pFiles.end())
    return (IFileMD*)(0);
  else
    return pIterFile->second;
}
}
