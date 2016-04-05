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

#include "namespace/ns_on_redis/Constants.hh"
#include "namespace/ns_on_redis/ContainerMD.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"
#include <sys/stat.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(id_t id, IFileMDSvc* file_svc,
			 IContainerMDSvc* cont_svc):
  IContainerMD(), pId(id), pParentId(0), pFlags(0), pName(""), pCUid(0), pCGid(0),
  pMode(040755), pACLId(0), pTreeSize(0), pContSvc(cont_svc), pFileSvc(file_svc)
{
  pCTime.tv_sec = 0;
  pCTime.tv_nsec = 0;
  pMTime.tv_sec = 0;
  pMTime.tv_nsec = 0;
  pTMTime.tv_sec = 0;
  pTMTime.tv_nsec = 0;
  pFilesKey = std::to_string(id) + constants::sMapFilesSuffix;
  pDirsKey = std::to_string(id) + constants::sMapDirsSuffix;
  // TODO: review this
  //pRedox = RedisClient::getInstance();
  pRedox = static_cast<ContainerMDSvc*>(cont_svc)->pRedox;
}

//------------------------------------------------------------------------------
// Find subcontainer
//------------------------------------------------------------------------------
std::unique_ptr<IContainerMD>
ContainerMD::findContainer(const std::string& name)
{
  try
  {
    IFileMD::id_t cid = std::stoull(pRedox->hget(pDirsKey, name));
    return pContSvc->getContainerMD(cid);
  }
  catch (std::exception& e)
  {
    return std::unique_ptr<IContainerMD>(nullptr);
  }
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
ContainerMD::removeContainer(const std::string& name)
{
  try
  {
    pRedox->hdel(pDirsKey, name);
  }
  catch (std::runtime_error& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container " << name << " not found";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Add container
//------------------------------------------------------------------------------
void
ContainerMD::addContainer(IContainerMD* container)
{
  container->setParentId(pId);

  try
  {
    pRedox->hset(pDirsKey, container->getName(), container->getId());
  }
  catch (std::runtime_error& e)
  {
    MDException e(EINVAL);
    e.getMessage() << "Failed to add subcontainer #" << container->getId();
    throw e;
  }
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
std::unique_ptr<IFileMD>
ContainerMD::findFile(const std::string& name)
{
  try
  {
    std::string fid = pRedox->hget(pFilesKey, name);
    return pFileSvc->getFileMD(std::stoull(fid));
  }
  catch (std::exception& e)
  {
    return std::unique_ptr<IFileMD>(nullptr);
  }
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
ContainerMD::addFile(IFileMD* file)
{
  file->setContainerId(pId);

  try
  {
    pRedox->hset(pFilesKey, file->getName(), file->getId());
  }
  catch (std::runtime_error& e)
  {
    MDException e(EINVAL);
    e.getMessage() << "Failed to add file #" << file->getId();
    throw e;
  }

  IFileMDChangeListener::Event e(file, IFileMDChangeListener::SizeChange,
				 0, 0, file->getSize());
  pFileSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
void
ContainerMD::removeFile(const std::string& name)
{
  std::unique_ptr<IFileMD> file;

  try
  {
    IFileMD::id_t fid = std::stoull(pRedox->hget(pFilesKey, name));
    file = pFileSvc->getFileMD(fid);
  }
  catch (std::exception& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Unknown file " << name << " in container " << pName;
    throw e;
  }

  try
  {
    // Remove from the list of files in current container
    pRedox->hdel(pFilesKey, name);
  }
  catch (std::runtime_error& e)
  {
    // It was already deleted - don't do anything
  }

  IFileMDChangeListener::Event e(file.get(), IFileMDChangeListener::SizeChange,
				 0, 0, -file->getSize());
  pFileSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
size_t
ContainerMD::getNumFiles()
{
  try
  {
    return pRedox->hlen(pFilesKey);
  }
  catch (std::runtime_error& e)
  {
    return 0;
  }
}

//----------------------------------------------------------------------------
// Get number of containers
//----------------------------------------------------------------------------
size_t
ContainerMD::getNumContainers()
{
  try
  {
    return pRedox->hlen(pDirsKey);
  }
  catch (std::runtime_error& e)
  {
    return 0;
  }
}

//------------------------------------------------------------------------
// Clean up the entire contents for the container. Delete files and
// containers recurssively
//------------------------------------------------------------------------
void
ContainerMD::cleanUp(IContainerMDSvc* cont_svc, IFileMDSvc* file_svc)
{
  // Remove all files
  auto vect_fids = pRedox->hvals(pFilesKey);

  for (auto itf = vect_fids.begin(); itf != vect_fids.end(); ++itf)
    file_svc->removeFile(std::stoull(*itf));

  if (!pRedox->del(pFilesKey))
  {
    MDException e(EINVAL);
    e.getMessage() << "Failed to remove files in container " << pName;
    throw e;
  }

  // Remove all subcontainers
  auto vect_cids = pRedox->hvals(pDirsKey);

  for (auto itc = vect_cids.begin(); itc != vect_cids.end(); ++itc)
  {
    std::unique_ptr<IContainerMD> cont =
      pContSvc->getContainerMD(std::stoull(*itc));
    cont->cleanUp(cont_svc, file_svc);
    pContSvc->removeContainer(cont.get());
  }

  if (!pRedox->del(pDirsKey))
  {
    MDException e(EINVAL);
    e.getMessage() << "Failed to remove subcontainers in container " << pName;
    throw e;
  }
}

//------------------------------------------------------------------------------
// Get set of file names contained in the current object
//------------------------------------------------------------------------------
std::set<std::string>
ContainerMD::getNameFiles() const
{
  std::vector<std::string> vect_files = pRedox->hvals(pFilesKey);
  std::set<std::string> set_files(vect_files.begin(), vect_files.end());
  return set_files;
}


//----------------------------------------------------------------------------
// Get set of subcontainer names contained in the current object
//----------------------------------------------------------------------------
std::set<std::string>
ContainerMD::getNameContainers() const
{
  std::vector<std::string> vect_subcont = pRedox->hvals(pDirsKey);
  std::set<std::string> set_subcont(vect_subcont.begin(), vect_subcont.end());
  return set_subcont;
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
  {
    if (requested & (1 << i))
      if (!(actual & (1 << i)))
	return false;
  }

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
// Set name
//------------------------------------------------------------------------------
void
ContainerMD::setName(const std::string& name)
{
  // Check that there is no clash with other subcontainers having the same name
  if (pParentId)
  {
    std::unique_ptr<eos::IContainerMD> parent = pContSvc->getContainerMD(pParentId);

    if (parent->findContainer(name))
    {
      eos::MDException e(EINVAL);
      e.getMessage() << "Container with name \"" << name << "\" already exists";
      throw e;
    }
  }

  pName = name;
}

//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void ContainerMD::setCTime(ctime_t ctime)
{
  pCTime.tv_sec = ctime.tv_sec;
  pCTime.tv_nsec = ctime.tv_nsec;
}

//------------------------------------------------------------------------------
// Set creation time to now
//------------------------------------------------------------------------------
void ContainerMD::setCTimeNow()
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

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void ContainerMD::getCTime(ctime_t& ctime) const
{
  ctime.tv_sec = pCTime.tv_sec;
  ctime.tv_nsec = pCTime.tv_nsec;
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
// Get modification time
//------------------------------------------------------------------------------
void ContainerMD::getMTime(mtime_t& mtime) const
{
  mtime.tv_sec = pMTime.tv_sec;
  mtime.tv_nsec = pMTime.tv_nsec;
}

//------------------------------------------------------------------------------
// Set propagated modification time (if newer)
//------------------------------------------------------------------------------
bool ContainerMD::setTMTime(tmtime_t tmtime)
{
  if ((tmtime.tv_sec > pMTime.tv_sec) ||
      ((tmtime.tv_sec == pMTime.tv_sec) &&
       (tmtime.tv_nsec > pMTime.tv_nsec)))
  {
    pTMTime.tv_sec = tmtime.tv_sec;
    pTMTime.tv_nsec = tmtime.tv_nsec;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Set propagated modification time to now
//------------------------------------------------------------------------------
void ContainerMD::setTMTimeNow()
{
  tmtime_t tmtime;
#ifdef __APPLE__
  struct timeval tv;
  gettimeofday(&tv, 0);
  tmtime..tv_sec = tv.tv_sec;
  tmtime.tv_nsec = tv.tv_usec * 1000;
#else
  clock_gettime(CLOCK_REALTIME, &tmtime);
#endif
  setTMTime(tmtime);
  return;
}

//------------------------------------------------------------------------------
// Get propagated modification time
//------------------------------------------------------------------------------
void ContainerMD::getTMTime(tmtime_t& tmtime) const
{
  tmtime.tv_sec = pTMTime.tv_sec;
  tmtime.tv_nsec = pTMTime.tv_nsec;
}

//------------------------------------------------------------------------------
// Trigger an mtime change event
//------------------------------------------------------------------------------
void ContainerMD::notifyMTimeChange(IContainerMDSvc* containerMDSvc)
{
  containerMDSvc->notifyListeners(this, IContainerMDChangeListener::MTimeChange);
}

//------------------------------------------------------------------------------
// Add to tree size
//------------------------------------------------------------------------------
uint64_t ContainerMD::addTreeSize(uint64_t addsize)
{
  pTreeSize += addsize;
  return pTreeSize;
}

//------------------------------------------------------------------------------
// Remove from tree size
//------------------------------------------------------------------------------
uint64_t ContainerMD::removeTreeSize(uint64_t removesize)
{
  pTreeSize += removesize;
  return pTreeSize;
}

//------------------------------------------------------------------------------
// Get the attribute
//------------------------------------------------------------------------------
std::string ContainerMD::getAttribute(const std::string& name) const
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

//------------------------------------------------------------------------------
// Remove attribute
//------------------------------------------------------------------------------
void ContainerMD::removeAttribute(const std::string& name)
{
  XAttrMap::iterator it = pXAttrs.find(name);

  if (it != pXAttrs.end())
    pXAttrs.erase(it);
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::serialize(std::string& buffer)
{
  buffer.append(reinterpret_cast<const char*>(&pId),       sizeof(pId));
  buffer.append(reinterpret_cast<const char*>(&pParentId), sizeof(pParentId));
  buffer.append(reinterpret_cast<const char*>(&pFlags),    sizeof(pFlags));
  buffer.append(reinterpret_cast<const char*>(&pCTime),    sizeof(pCTime));
  buffer.append(reinterpret_cast<const char*>(&pCUid),     sizeof(pCUid));
  buffer.append(reinterpret_cast<const char*>(&pCGid),     sizeof(pCGid));
  buffer.append(reinterpret_cast<const char*>(&pMode),     sizeof(pMode));
  buffer.append(reinterpret_cast<const char*>(&pACLId),    sizeof(pACLId));
  uint16_t len = pName.length() + 1;
  buffer.append(reinterpret_cast<const char*>(&len), 2);
  buffer.append(pName.c_str(), len);
  len = pXAttrs.size() + 2;
  buffer.append(reinterpret_cast<const char*>(&len), sizeof(len));

  for (auto it = pXAttrs.begin(); it != pXAttrs.end(); ++it)
  {
    uint16_t strLen = it->first.length() + 1;
    buffer.append(reinterpret_cast<const char*>(&strLen), sizeof(strLen));
    buffer.append(it->first.c_str(), strLen);
    strLen = it->second.length() + 1;
    buffer.append(reinterpret_cast<const char*>(&strLen), sizeof(strLen));
    buffer.append(it->second.c_str(), strLen);
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
  buffer.append(reinterpret_cast<const char*>(&l1), sizeof(l1));
  buffer.append(k1.c_str(), l1);
  // value
  buffer.append(reinterpret_cast<const char*>(&l3), sizeof(l3));
  buffer.append(stime, l3);
  snprintf(stime, sizeof(stime), "%llu", (unsigned long long)pMTime.tv_nsec);
  l3 = strlen(stime) + 1;
  // key
  buffer.append(reinterpret_cast<const char*>(&l2), sizeof(l2));
  buffer.append(k2.c_str(), l2);
  // value
  buffer.append(reinterpret_cast<const char*>(&l3), sizeof(l3));
  buffer.append(stime, l3);
}

//------------------------------------------------------------------------------
// Deserialize the class from buffer
//------------------------------------------------------------------------------
void
ContainerMD::deserialize(const std::string& buffer)
{
  uint16_t offset = 0;
  offset = Buffer::grabData(buffer, offset, &pId, sizeof(pId));
  offset = Buffer::grabData(buffer, offset, &pParentId, sizeof(pParentId));
  offset = Buffer::grabData(buffer, offset, &pFlags, sizeof(pFlags));
  offset = Buffer::grabData(buffer, offset, &pCTime, sizeof(pCTime));
  offset = Buffer::grabData(buffer, offset, &pCUid, sizeof(pCUid));
  offset = Buffer::grabData(buffer, offset, &pCGid, sizeof(pCGid));
  offset = Buffer::grabData(buffer, offset, &pMode, sizeof(pMode));
  offset = Buffer::grabData(buffer, offset, &pACLId, sizeof(pACLId));
  uint16_t len;
  offset = Buffer::grabData(buffer, offset, &len, sizeof(len));
  char strBuffer[len];
  offset = Buffer::grabData(buffer, offset, strBuffer, len);
  pName = strBuffer;
  uint16_t len1 = 0;
  uint16_t len2 = 0;
  len = 0;
  offset = Buffer::grabData(buffer, offset, &len, sizeof(len));

  for (uint16_t i = 0; i < len; ++i)
  {
    offset = Buffer::grabData(buffer, offset, &len1, sizeof(len1));
    char strBuffer1[len1];
    offset = Buffer::grabData(buffer, offset, strBuffer1, len1);
    offset = Buffer::grabData(buffer, offset, &len2, sizeof(len2));
    char strBuffer2[len2];
    offset = Buffer::grabData(buffer, offset, strBuffer2, len2);
    std::string key = strBuffer1;

    if (key == "sys.mtime.s")
    {
      // Stored modification time in s
      pMTime.tv_sec = strtoull(strBuffer2, 0, 10);
    }
    else
    {
      if (key == "sys.mtime.ns")
      {
	// Stored modification time in ns
	pMTime.tv_nsec = strtoull(strBuffer2, 0, 10);
      }
      else
      {
	pXAttrs.insert(std::make_pair<char*, char*>(strBuffer1, strBuffer2));
      }
    }
  }

  // Rebuild the file and subcontainer keys
  pFilesKey = std::to_string(pId) + constants::sMapFilesSuffix;
  pDirsKey = std::to_string(pId) + constants::sMapDirsSuffix;
}

EOSNSNAMESPACE_END
