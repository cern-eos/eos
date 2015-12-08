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

#include "namespace/ns_on_redis/Constants.hh"
#include "namespace/ns_on_redis/ContainerMD.hh"
#include "namespace/ns_on_redis/FileMD.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <sys/stat.h>

EOSNSNAMESPACE_BEGIN

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
  pACLId(0),
  pTreeSize(0)
{
  pCTime.tv_sec = 0;
  pCTime.tv_nsec = 0;
  pMTime.tv_sec = 0;
  pMTime.tv_nsec = 0;
  pTMTime.tv_sec = 0;
  pTMTime.tv_nsec = 0;
  pFilesKey = std::to_string(id) + constants::sMapFilesSuffix;
  pDirsKey = std::to_string(id) + constants::sMapDirsSuffix;
}

//------------------------------------------------------------------------------
// Desstructor
//------------------------------------------------------------------------------
ContainerMD::~ContainerMD()
{
  // empty
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
  pMTime    = other.pMTime;
  pTMTime   = other.pTMTime;
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
  try
  {
    IFileMD::id_t cid = std::stoull(pRedox->hget(pDirsKey, name));
    return pContSvc->getContainerMD(cid);
  }
  catch (std::runtime_error& e)
  {
    return nullptr;
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
IFileMD*
ContainerMD::findFile(const std::string& name)
{
  try
  {
    std::string fid = pRedox->hget(pFilesKey, name);
    return pFileSvc->getFileMD(std::stoull(fid));
  }
  catch (std::runtime_error& e)
  {
    return nullptr;
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
				 0, 0, file->getSize() );
  pFileSvc->notifyListeners( &e );
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
    file.reset(pFileSvc->getFileMD(fid));
  }
  catch (std::runtime_error& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Unknow file=" << name << " in container=" << pName;
    throw e;
  }

  IFileMDChangeListener::Event e(file.get(),IFileMDChangeListener::SizeChange,
				 0, 0, -file->getSize());
  pFileSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
size_t
ContainerMD::getNumFiles()
{
  return pRedox->hlen(pFilesKey);
}

//----------------------------------------------------------------------------
// Get number of containers
//----------------------------------------------------------------------------
size_t
ContainerMD::getNumContainers()
{
  return pRedox->hlen(pDirsKey);
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
    std::unique_ptr<IContainerMD> cont {pContSvc->getContainerMD(std::stoull(*itc))};
    cont->cleanUp(cont_svc, file_svc);
  }

  if (pRedox->del(pDirsKey))
  {
    MDException e(EINVAL);
    e.getMessage() << "Failed to remove subcontainers in container " << pName;
    throw e;
  }
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
  XAttrMap::iterator it;

  for (it = pXAttrs.begin(); it != pXAttrs.end(); ++it)
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
  offset = Buffer::grabData(buffer, offset, sizeof(pId), &pId);
  offset = Buffer::grabData(buffer, offset, sizeof(pParentId), &pParentId);
  offset = Buffer::grabData(buffer, offset, sizeof(pFlags), &pFlags);
  offset = Buffer::grabData(buffer, offset, sizeof(pCTime), &pCTime);
  offset = Buffer::grabData(buffer, offset, sizeof(pCUid), &pCUid);
  offset = Buffer::grabData(buffer, offset, sizeof(pCGid), &pCGid);
  offset = Buffer::grabData(buffer, offset, sizeof(pMode), &pMode);
  offset = Buffer::grabData(buffer, offset, sizeof(pACLId), &pACLId);
  uint16_t len;
  offset = Buffer::grabData(buffer, offset, sizeof(len), &len);
  char strBuffer[len];
  offset = Buffer::grabData(buffer, offset, len, strBuffer);
  pName = strBuffer;
  uint16_t len1 = 0;
  uint16_t len2 = 0;
  len = 0;
  offset = Buffer::grabData(buffer, offset, sizeof(len), &len);

  for (uint16_t i = 0; i < len; ++i)
  {
    offset = Buffer::grabData(buffer, offset, sizeof(len1), &len1);
    char strBuffer1[len1];
    offset = Buffer::grabData(buffer, offset, len1, strBuffer1);
    offset = Buffer::grabData(buffer, offset, sizeof(len2), &len2);
    char strBuffer2[len2];
    offset = Buffer::grabData(buffer, offset, len2, strBuffer2);
    std::string key = strBuffer1;

    if (key=="sys.mtime.s")
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

  pFilesKey = std::to_string(pId) + constants::sMapFilesSuffix;
  pDirsKey = std::to_string(pId) + constants::sMapDirsSuffix;
}

//------------------------------------------------------------------------------
// Get pointer to first subcontainer. Must be used in conjunction with
// nextContainer to iterate over the list of subcontainers.
//------------------------------------------------------------------------------
std::unique_ptr<IContainerMD>
ContainerMD::beginSubContainer()
{
  // TODO: review this to be more efficient in case there are many subcont
  // i.e. use the hscan function and do the same also for files
  pSubCont = pRedox->hvals(pDirsKey);

  if (pSubCont.empty())
  {
    pIterSubCont = pSubCont.end();
    return std::unique_ptr<IContainerMD>{nullptr};
  }
  else
  {
    pIterSubCont = pSubCont.begin();
    return std::unique_ptr<IContainerMD>
      {pContSvc->getContainerMD(std::stoull(*pIterSubCont))};
  }
}

//------------------------------------------------------------------------------
// Get pointer to the next subcontainer object. Must be used in conjunction
// with beginContainers to iterate over the list of subcontainers.
//------------------------------------------------------------------------------
std::unique_ptr<IContainerMD>
ContainerMD::nextSubContainer()
{
  if (pIterSubCont == pSubCont.end())
    return nullptr;

  ++pIterSubCont;

  if (pIterSubCont == pSubCont.end())
  {
    return std::unique_ptr<IContainerMD>{nullptr};
  }
  else
  {
    return std::unique_ptr<IContainerMD>
      {pContSvc->getContainerMD(std::stoull(*pIterSubCont))};
  }
}

//------------------------------------------------------------------------------
// Get pointer to first file in the container. Must be used in conjunction
// with nextFile to iterate over the list of files.
//------------------------------------------------------------------------------
std::unique_ptr<IFileMD>
ContainerMD::beginFile()
{
  pFiles = pRedox->hvals(pFilesKey);

  if (pFiles.empty())
  {
    pIterFile = pFiles.end();
    return std::unique_ptr<IFileMD>(nullptr);
  }
  else
  {
    pIterFile = pFiles.begin();
    return std::unique_ptr<IFileMD>{
      pFileSvc->getFileMD(std::stoull(*pIterFile))};
  }
}

//------------------------------------------------------------------------------
// Get pointer to the next file object. Must be used in conjunction
// with beginFiles to iterate over the list of files.
//------------------------------------------------------------------------------
std::unique_ptr<IFileMD>
ContainerMD::nextFile()
{
  if (pIterFile == pFiles.end())
    return std::unique_ptr<IFileMD>(nullptr);

  ++pIterFile;

  if (pIterFile == pFiles.end())
  {
    return std::unique_ptr<IFileMD>{nullptr};
  }
  else
  {
    return std::unique_ptr<IFileMD>{
      pFileSvc->getFileMD(std::stoull(*pIterFile))};
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
void ContainerMD::notifyMTimeChange(IContainerMDSvc *containerMDSvc)
{
  containerMDSvc->notifyListeners(this , IContainerMDChangeListener::MTimeChange);
}

EOSNSNAMESPACE_END
