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
#include "namespace/PermissionHandler.hh"
#include <sys/stat.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(ContainerMD::id_t id, IFileMDSvc* file_svc,
                         IContainerMDSvc* cont_svc):
  IContainerMD(), pId(id), pParentId(0), pFlags(0), pName(""), pCUid(0),
  pCGid(0), pMode(040755), pACLId(0), pFileSvc(file_svc),
  pContSvc(cont_svc)
{
  mSubcontainers.set_deleted_key("");
  mFiles.set_deleted_key("");
  mSubcontainers.set_empty_key("##_EMPTY_##");
  mFiles.set_empty_key("##_EMPTY_##");

  pCTime.tv_sec = 0;
  pCTime.tv_nsec = 0;
  pMTime.tv_sec = 0;
  pMTime.tv_nsec = 0;
  pTMTime.tv_sec = 0;
  pTMTime.tv_nsec = 0;
  setTreeSize(0);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ContainerMD::~ContainerMD()
{
  try {
    mFiles.clear();
    mSubcontainers.clear();
  } catch (const std::length_error& e) {}
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
// Assignment operator
//------------------------------------------------------------------------------
ContainerMD& ContainerMD::operator= (const ContainerMD& other)
{
  pId       = other.pId;
  pParentId = other.pParentId;
  pFlags    = other.pFlags;
  pCTime    = other.pCTime;
  pMTime    = other.pMTime;
  pTMTime   = other.pTMTime;
  pName     = other.pName;
  pCUid     = other.pCUid;
  pCGid     = other.pCGid;
  pMode     = other.pMode;
  pACLId    = other.pACLId;
  pXAttrs   = other.pXAttrs;
  pFlags    = other.pFlags;
  pFileSvc  = other.pFileSvc;
  pContSvc  = other.pContSvc;
#if __GNUC_PREREQ(4,8)
  this->pTMTime_atomic = other.pTMTime_atomic;
#endif
  pTreeSize = 0;
  // Note: mFiles, mSubcontainers, pTreeSize are not copied here
  return *this;
}

//------------------------------------------------------------------------------
// Children inheritance
//------------------------------------------------------------------------------
void
ContainerMD::InheritChildren(const IContainerMD& other)
{
  const ContainerMD& otherContainer = dynamic_cast<const ContainerMD&>(other);
  mFiles = otherContainer.mFiles;
  mSubcontainers = otherContainer.mSubcontainers;
  setTreeSize(otherContainer.getTreeSize());
}

//------------------------------------------------------------------------------
// Find sub container, asynchronous API
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
ContainerMD::findContainerFut(const std::string& name)
{
  return folly::makeFuture<IContainerMDPtr>(this->findContainer(name));
}

//------------------------------------------------------------------------------
// Find sub container
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMD::findContainer(const std::string& name)
{
  ContainerMap::iterator it = mSubcontainers.find(name);

  if (it == mSubcontainers.end()) {
    return std::shared_ptr<IContainerMD>((IContainerMD*)0);
  }

  return pContSvc->getContainerMD(it->second);
}

//------------------------------------------------------------------------------
// Find item
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
ContainerMD::findItem(const std::string &name)
{
  FileOrContainerMD retval;

  // First, check if a file with such name exists.
  retval.file = findFile(name);

  if(retval.file) {
    // Yep, we're done.
    return retval;
  }

  retval.container = findContainer(name);
  return retval;
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
ContainerMD::removeContainer(const std::string& name)
{
  mSubcontainers.erase(name);
  mSubcontainers.resize(0);
}

//------------------------------------------------------------------------------
// Add container
//------------------------------------------------------------------------------
void
ContainerMD::addContainer(IContainerMD* container)
{
  container->setParentId(pId);
  mSubcontainers[container->getName()] = container->getId();
}

//------------------------------------------------------------------------------
// Find file, asynchronous API.
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr>
ContainerMD::findFileFut(const std::string& name)
{
  return this->findFile(name);
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
ContainerMD::findFile(const std::string& name)
{
  eos::IContainerMD::FileMap::iterator it = mFiles.find(name);

  if (it == mFiles.end()) {
    return nullptr;
  }

  return pFileSvc->getFileMD(it->second);
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
ContainerMD::addFile(IFileMD* file)
{
  file->setContainerId(pId);
  mFiles[file->getName()] = file->getId();
  IFileMDChangeListener::Event e(file, IFileMDChangeListener::SizeChange,
                                 0, file->getSize());
  file->getFileMDSvc()->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
void
ContainerMD::removeFile(const std::string& name)
{
  if (mFiles.count(name)) {
    std::shared_ptr<IFileMD> file = pFileSvc->getFileMD(mFiles[name]);
    IFileMDChangeListener::Event e(file.get(), IFileMDChangeListener::SizeChange,
                                   0, -file->getSize());
    file->getFileMDSvc()->notifyListeners(&e);
    mFiles.erase(name);
    mFiles.resize(0);
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::serialize(Buffer& buffer)
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
  len = pXAttrs.size() + 2;
  buffer.putData(&len, sizeof(len));
  XAttrMap::const_iterator it;

  for (it = pXAttrs.begin(); it != pXAttrs.end(); ++it) {
    uint16_t strLen = it->first.length() + 1;
    buffer.putData(&strLen, sizeof(strLen));
    buffer.putData(it->first.c_str(), strLen);
    strLen = it->second.length() + 1;
    buffer.putData(&strLen, sizeof(strLen));
    buffer.putData(it->second.c_str(), strLen);
  }

  // Store mtime as ext. attributes
  std::string k1 = "sys.mtime.s";
  std::string k2 = "sys.mtime.ns";
  uint16_t l1 = k1.length() + 1;
  uint16_t l2 = k2.length() + 1;
  uint16_t l3;
  char stime[64];
  snprintf(stime, sizeof(stime), "%llu", (unsigned long long)pMTime.tv_sec);
  l3 = strlen(stime) + 1;
  // key
  buffer.putData(&l1, sizeof(l1));
  buffer.putData(k1.c_str(), l1);
  // value
  buffer.putData(&l3, sizeof(l3));
  buffer.putData(stime, l3);
  snprintf(stime, sizeof(stime), "%llu", (unsigned long long)pMTime.tv_nsec);
  l3 = strlen(stime) + 1;
  // key
  buffer.putData(&l2, sizeof(l2));
  buffer.putData(k2.c_str(), l2);
  // value
  buffer.putData(&l3, sizeof(l3));
  buffer.putData(stime, l3);
}

//------------------------------------------------------------------------------
// Deserialize the class to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::deserialize(Buffer& buffer)
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
  pMTime.tv_sec = pCTime.tv_sec;
  pMTime.tv_nsec = pCTime.tv_nsec;
  uint16_t len1 = 0;
  uint16_t len2 = 0;
  len = 0;
  offset = buffer.grabData(offset, &len, sizeof(len));

  for (uint16_t i = 0; i < len; ++i) {
    offset = buffer.grabData(offset, &len1, sizeof(len1));
    char strBuffer1[len1];
    offset = buffer.grabData(offset, strBuffer1, len1);
    offset = buffer.grabData(offset, &len2, sizeof(len2));
    char strBuffer2[len2];
    offset = buffer.grabData(offset, strBuffer2, len2);
    std::string key = strBuffer1;

    if (key == "sys.mtime.s") {
      // Stored modification time in s
      pMTime.tv_sec = strtoull(strBuffer2, 0, 10);
    } else {
      if (key == "sys.mtime.ns") {
        // Stored modification time in ns
        pMTime.tv_nsec = strtoull(strBuffer2, 0, 10);
      } else {
        pXAttrs.insert(std::make_pair <char*, char*>(strBuffer1, strBuffer2));
      }
    }
  }
}

//------------------------------------------------------------------------------
// Check the access permissions
//------------------------------------------------------------------------------
bool
ContainerMD::access(uid_t uid, gid_t gid, int flags)
{
  // root can do everything
  if (uid == 0) {
    return true;
  }

  // daemon can read everything
  if ((uid == DAEMONUID) && (!(flags & W_OK))) {
    return true;
  }

  // Filter out based on sys.mask
  mode_t filteredMode = PermissionHandler::filterWithSysMask(pXAttrs, pMode);

  // Convert the flags
  char convFlags = PermissionHandler::convertRequested(flags);

  // Check the perms
  if (uid == pCUid) {
    char user = PermissionHandler::convertModetUser(filteredMode);
    return PermissionHandler::checkPerms(user, convFlags);
  }

  if (gid == pCGid) {
    char group = PermissionHandler::convertModetGroup(filteredMode);
    return PermissionHandler::checkPerms(group, convFlags);
  }

  char other = PermissionHandler::convertModetOther(filteredMode);
  return PermissionHandler::checkPerms(other, convFlags);
}

//------------------------------------------------------------------------------
// Set modification time
//------------------------------------------------------------------------------
void
ContainerMD::setMTime(mtime_t mtime)
{
  pMTime.tv_sec = mtime.tv_sec;
  pMTime.tv_nsec = mtime.tv_nsec;
}

//------------------------------------------------------------------------------
// Set creation time to now
//------------------------------------------------------------------------------
void ContainerMD::setMTimeNow()
{
#ifdef __APPLE__
  struct timeval tv;
  gettimeofday(&tv, 0);
  pMTime.tv_sec = tv.tv_sec;
  pMTime.tv_nsec = tv.tv_usec * 1000;
#else
  clock_gettime(CLOCK_REALTIME, &pMTime);
#endif
}

//------------------------------------------------------------------------------
// Trigger an mtime change event
//------------------------------------------------------------------------------
void ContainerMD::notifyMTimeChange(IContainerMDSvc* containerMDSvc)
{
  containerMDSvc->notifyListeners(this , IContainerMDChangeListener::MTimeChange);
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
ContainerMD::getAttributes() const
{
  return pXAttrs;
}

EOSNSNAMESPACE_END
