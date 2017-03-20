/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/utils/StringConvertion.hh"
#include <sys/stat.h>
#include <algorithm>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(id_t id, IFileMDSvc* file_svc,
                         IContainerMDSvc* cont_svc)
  : IContainerMD(), pId(id), pParentId(0), pFlags(0), pCTime{0}, pName(""),
    pCUid(0), pCGid(0), pMode(040755), pACLId(0), pMTime{0}, pTMTime{0},
    pTreeSize(0), pContSvc(cont_svc), pFileSvc(file_svc),
    pFilesKey(stringify(id) + constants::sMapFilesSuffix),
    pDirsKey(stringify(id) + constants::sMapDirsSuffix),
    mDirsMap(), mFilesMap()
{
  ContainerMDSvc* impl_cont_svc = dynamic_cast<ContainerMDSvc*>(cont_svc);

  if (!impl_cont_svc) {
    MDException e(EFAULT);
    e.getMessage() << "ContainerMDSvc dynamic cast failed";
    throw e;
  }

  pQcl = impl_cont_svc->pQcl;
  pFilesMap = qclient::QHash(*pQcl, pFilesKey);
  pDirsMap = qclient::QHash(*pQcl, pDirsKey);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ContainerMD::~ContainerMD()
{
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
  pTreeSize = other.pTreeSize;
  pContSvc  = other.pContSvc;
  pFileSvc  = other.pFileSvc;
  // Note: pFiles and pSubContainers are not copied here
  pQcl      = other.pQcl;
  return *this;
}

//------------------------------------------------------------------------------
// Find subcontainer
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMD::findContainer(const std::string& name)
{
  auto iter = mDirsMap.find(name);

  if (iter == mDirsMap.end()) {
    return std::shared_ptr<IContainerMD>(nullptr);
  }

  std::shared_ptr<IContainerMD> cont(nullptr);

  try {
    cont = pContSvc->getContainerMD(iter->second);
  } catch (MDException& ex) {
    cont.reset();
  }

  // Curate the list of subcontainers in case entry is not found
  if (cont == nullptr) {
    mDirsMap.erase(iter);

    try {
      (void) pDirsMap.hdel(name);
    } catch (std::runtime_error& qdb_err) {
      MDException e(ECOMM);
      e.getMessage() << __FUNCTION__ << " " << qdb_err.what();
      throw e;
    }
  }

  return cont;
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
ContainerMD::removeContainer(const std::string& name)
{
  auto it = mDirsMap.find(name);

  if (it == mDirsMap.end()) {
    MDException e(ENOENT);
    e.getMessage() << "Container " << name << " not found";
    throw e;
  }

  mDirsMap.erase(it);

  // Do async call to KV backend
  try {
    (void) pDirsMap.hdel(name);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << __FUNCTION__ << " " << qdb_err.what();
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
  auto ret = mDirsMap.insert(std::pair<std::string, IContainerMD::id_t>(
                               container->getName(), container->getId()));

  if (!ret.second) {
    MDException e(EINVAL);
    e.getMessage() << "Failed to add subcontainer #" << container->getId();
    throw e;
  }

  // Add to new container to KV backend
  try {
    if (!pDirsMap.hset(container->getName(), container->getId())) {
      MDException e(EINVAL);
      e.getMessage() << "Failed to add subcontainer #" << container->getId();
      throw e;
    }
  } catch (std::runtime_error& qdb_err) {
    MDException e(ECOMM);
    e.getMessage() << __FUNCTION__ << " " << qdb_err.what();
    throw e;
  }
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
ContainerMD::findFile(const std::string& name)
{
  auto iter = mFilesMap.find(name);

  if (iter == mFilesMap.end()) {
    return std::shared_ptr<IFileMD>(nullptr);
  }

  std::shared_ptr<IFileMD> file(nullptr);

  try {
    file = pFileSvc->getFileMD(iter->second);
  } catch (MDException& e) {
    file.reset();
  }

  // Curate the list of files in case file entry is not found
  if (file == nullptr) {
    mFilesMap.erase(iter);

    try {
      (void) pFilesMap.hdel(stringify(iter->second));
    } catch (std::runtime_error& qdb_err) {
      MDException e(ECOMM);
      e.getMessage() << __FUNCTION__ << " " << qdb_err.what();
      throw e;
    }
  }

  return file;
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
ContainerMD::addFile(IFileMD* file)
{
  file->setContainerId(pId);
  auto ret = mFilesMap.insert(
               std::pair<std::string, IFileMD::id_t>(file->getName(), file->getId()));

  if (!ret.second) {
    MDException e(EINVAL);
    e.getMessage() << "Error, file #" << file->getId() << " already exists";
    throw e;
  }

  try {
    if (!pFilesMap.hset(file->getName(), file->getId())) {
      MDException e(EINVAL);
      e.getMessage() << "File #" << file->getId() << " already exists";
      throw e;
    }
  } catch (std::runtime_error& qdb_err) {
    MDException e(EINVAL);
    e.getMessage() << __FUNCTION__ << " " << qdb_err.what();
    throw e;
  }

  if (file->getSize() != 0u) {
    IFileMDChangeListener::Event e(file, IFileMDChangeListener::SizeChange, 0,
                                   0, file->getSize());
    pFileSvc->notifyListeners(&e);
  }
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
void
ContainerMD::removeFile(const std::string& name)
{
  IFileMD::id_t id;
  auto iter = mFilesMap.find(name);

  if (iter == mFilesMap.end()) {
    MDException e(ENOENT);
    e.getMessage() << "Unknown file " << name << " in container " << pName;
    throw e;
  } else {
    id = iter->second;
    mFilesMap.erase(iter);
  }

  // Do async call to KV backend
  try {
    (void) pFilesMap.hdel(name);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << __FUNCTION__ << " " << qdb_err.what();
    throw e;
  }

  try {
    std::shared_ptr<IFileMD> file = pFileSvc->getFileMD(id);
    IFileMDChangeListener::Event e(
      file.get(), IFileMDChangeListener::SizeChange, 0, 0, -file->getSize());
    pFileSvc->notifyListeners(&e);
  } catch (MDException& e) {
    // File already removed
  }
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
size_t
ContainerMD::getNumFiles()
{
  return mFilesMap.size();
}

//----------------------------------------------------------------------------
// Get number of containers
//----------------------------------------------------------------------------
size_t
ContainerMD::getNumContainers()
{
  return mDirsMap.size();
}

//------------------------------------------------------------------------
// Clean up the entire contents for the container. Delete files and
// containers recurssively
//------------------------------------------------------------------------
void
ContainerMD::cleanUp()
{
  std::shared_ptr<IFileMD> file;

  for (auto && elem : mFilesMap) {
    file = pFileSvc->getFileMD(elem.second);
    pFileSvc->removeFile(file.get());
  }

  file.reset();
  mFilesMap.clear();
  qclient::AsyncHandler ah;
  ah.Register(pQcl->del_async(pFilesKey), pQcl);

  // Remove all subcontainers
  for (auto && elem : mDirsMap) {
    std::shared_ptr<IContainerMD> cont = pContSvc->getContainerMD(elem.second);
    cont->cleanUp();
    pContSvc->removeContainer(cont.get());
  }

  mDirsMap.clear();
  ah.Register(pQcl->del_async(pDirsKey), pQcl);

  if (!ah.Wait()) {
    auto resp = ah.GetResponses();
    int err_conn = std::count_if(resp.begin(), resp.end(),
    [](long long int elem) {
      return (elem == -ECOMM);
    });

    if (err_conn) {
      MDException e(ECOMM);
      e.getMessage() << __FUNCTION__ << "Container " << pName
                     << " error contacting KV-store";
      throw e;
    }
  }
}

//------------------------------------------------------------------------------
// Get set of file names contained in the current object
//------------------------------------------------------------------------------
std::set<std::string>
ContainerMD::getNameFiles() const
{
  std::set<std::string> set_files;

  for (auto && elem : mFilesMap) {
    set_files.insert(elem.first);
  }

  return set_files;
}

//----------------------------------------------------------------------------
// Get set of subcontainer names contained in the current object
//----------------------------------------------------------------------------
std::set<std::string>
ContainerMD::getNameContainers() const
{
  std::set<std::string> set_dirs;

  for (auto && elem : mDirsMap) {
    set_dirs.insert(elem.first);
  }

  return set_dirs;
}

//------------------------------------------------------------------------------
// Access checking helpers
//------------------------------------------------------------------------------
#define CANREAD 0x01
#define CANWRITE 0x02
#define CANENTER 0x04

static char
convertModetUser(mode_t mode)
{
  char perms = 0;

  if ((mode & S_IRUSR) != 0u) {
    perms |= CANREAD;
  }

  if ((mode & S_IWUSR) != 0u) {
    perms |= CANWRITE;
  }

  if ((mode & S_IXUSR) != 0u) {
    perms |= CANENTER;
  }

  return perms;
}

static char
convertModetGroup(mode_t mode)
{
  char perms = 0;

  if ((mode & S_IRGRP) != 0u) {
    perms |= CANREAD;
  }

  if ((mode & S_IWGRP) != 0u) {
    perms |= CANWRITE;
  }

  if ((mode & S_IXGRP) != 0u) {
    perms |= CANENTER;
  }

  return perms;
}

static char
convertModetOther(mode_t mode)
{
  char perms = 0;

  if ((mode & S_IROTH) != 0u) {
    perms |= CANREAD;
  }

  if ((mode & S_IWOTH) != 0u) {
    perms |= CANWRITE;
  }

  if ((mode & S_IXOTH) != 0u) {
    perms |= CANENTER;
  }

  return perms;
}

static bool
checkPerms(char actual, char requested)
{
  for (int i = 0; i < 3; ++i) {
    if ((requested & (1 << i)) != 0) {
      if ((actual & (1 << i)) == 0) {
        return false;
      }
    }
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
  if (uid == 0) {
    return true;
  }

  // daemon can read everything
  if ((uid == 2) && ((flags & W_OK) == 0)) {
    return true;
  }

  // Convert the flags
  char convFlags = 0;

  if ((flags & R_OK) != 0) {
    convFlags |= CANREAD;
  }

  if ((flags & W_OK) != 0) {
    convFlags |= CANWRITE;
  }

  if ((flags & X_OK) != 0) {
    convFlags |= CANENTER;
  }

  // Check the perms
  if (uid == pCUid) {
    char user = convertModetUser(pMode);
    return checkPerms(user, convFlags);
  }

  if (gid == pCGid) {
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
  if (pParentId != 0u) {
    std::shared_ptr<eos::IContainerMD> parent =
      pContSvc->getContainerMD(pParentId);

    if (parent->findContainer(name)) {
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
void
ContainerMD::setCTime(ctime_t ctime)
{
  pCTime.tv_sec = ctime.tv_sec;
  pCTime.tv_nsec = ctime.tv_nsec;
}

//------------------------------------------------------------------------------
// Set creation time to now
//------------------------------------------------------------------------------
void
ContainerMD::setCTimeNow()
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
void
ContainerMD::getCTime(ctime_t& ctime) const
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
void
ContainerMD::setMTimeNow()
{
#ifdef __APPLE__
  struct timeval tv = {0};
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
void
ContainerMD::getMTime(mtime_t& mtime) const
{
  mtime.tv_sec = pMTime.tv_sec;
  mtime.tv_nsec = pMTime.tv_nsec;
}

//------------------------------------------------------------------------------
// Set propagated modification time (if newer)
//------------------------------------------------------------------------------
bool
ContainerMD::setTMTime(tmtime_t tmtime)
{
  if ((tmtime.tv_sec > pMTime.tv_sec) ||
      ((tmtime.tv_sec == pMTime.tv_sec) && (tmtime.tv_nsec > pMTime.tv_nsec))) {
    pTMTime.tv_sec = tmtime.tv_sec;
    pTMTime.tv_nsec = tmtime.tv_nsec;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Set propagated modification time to now
//------------------------------------------------------------------------------
void
ContainerMD::setTMTimeNow()
{
  tmtime_t tmtime = {0};
#ifdef __APPLE__
  struct timeval tv;
  gettimeofday(&tv, 0);
  tmtime..tv_sec = tv.tv_sec;
  tmtime.tv_nsec = tv.tv_usec * 1000;
#else
  clock_gettime(CLOCK_REALTIME, &tmtime);
#endif
  setTMTime(tmtime);
}

//------------------------------------------------------------------------------
// Get propagated modification time
//------------------------------------------------------------------------------
void
ContainerMD::getTMTime(tmtime_t& tmtime) const
{
  tmtime.tv_sec = pTMTime.tv_sec;
  tmtime.tv_nsec = pTMTime.tv_nsec;
}

//------------------------------------------------------------------------------
// Trigger an mtime change event
//------------------------------------------------------------------------------
void
ContainerMD::notifyMTimeChange(IContainerMDSvc* containerMDSvc)
{
  containerMDSvc->notifyListeners(this,
                                  IContainerMDChangeListener::MTimeChange);
}

//------------------------------------------------------------------------------
// Add to tree size
//------------------------------------------------------------------------------
uint64_t
ContainerMD::addTreeSize(uint64_t addsize)
{
  pTreeSize += addsize;
  return pTreeSize;
}

//------------------------------------------------------------------------------
// Remove from tree size
//------------------------------------------------------------------------------
uint64_t
ContainerMD::removeTreeSize(uint64_t removesize)
{
  pTreeSize += removesize;
  return pTreeSize;
}

//------------------------------------------------------------------------------
// Get the attribute
//------------------------------------------------------------------------------
std::string
ContainerMD::getAttribute(const std::string& name) const
{
  auto const it = pXAttrs.find(name);

  if (it == pXAttrs.end()) {
    MDException e(ENOENT);
    e.getMessage() << "Attribute: " << name << " not found";
    throw e;
  }

  return it->second;
}

//------------------------------------------------------------------------------
// Remove attribute
//------------------------------------------------------------------------------
void
ContainerMD::removeAttribute(const std::string& name)
{
  auto it = pXAttrs.find(name);

  if (it != pXAttrs.end()) {
    pXAttrs.erase(it);
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::serialize(Buffer& buffer)
{
  // Wait for any ongoing async requests and throw error if smth failed
  // if (!waitAsyncReplies()) {
  //   MDException e(EFAULT);
  //   e.getMessage() << "Container #" << pId << " has failed async replies";
  //   throw e;
  // }
  buffer.putData(&pId, sizeof(pId));
  buffer.putData(&pParentId, sizeof(pParentId));
  buffer.putData(&pFlags, sizeof(pFlags));
  buffer.putData(&pCTime, sizeof(pCTime));
  buffer.putData(&pCUid, sizeof(pCUid));
  buffer.putData(&pCGid, sizeof(pCGid));
  buffer.putData(&pMode, sizeof(pMode));
  buffer.putData(&pACLId, sizeof(pACLId));
  uint16_t len = pName.length() + 1;
  buffer.putData(&len, 2);
  buffer.putData(pName.c_str(), len);
  len = pXAttrs.size() + 2;
  buffer.putData(&len, sizeof(len));
  XAttrMap::iterator it;

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
  snprintf(static_cast<char*>(stime), sizeof(stime), "%llu",
           static_cast<unsigned long long>(pMTime.tv_sec));
  l3 = strlen(static_cast<char*>(stime)) + 1;
  // key
  buffer.putData(&l1, sizeof(l1));
  buffer.putData(k1.c_str(), l1);
  // value
  buffer.putData(&l3, sizeof(l3));
  buffer.putData(static_cast<char*>(stime), l3);
  snprintf(static_cast<char*>(stime), sizeof(stime), "%llu",
           static_cast<unsigned long long>(pMTime.tv_nsec));
  l3 = strlen(static_cast<char*>(stime)) + 1;
  // key
  buffer.putData(&l2, sizeof(l2));
  buffer.putData(k2.c_str(), l2);
  // value
  buffer.putData(&l3, sizeof(l3));
  buffer.putData(static_cast<char*>(stime), l3);
}

//------------------------------------------------------------------------------
// Deserialize the class to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::deserialize(Buffer& buffer)
{
  uint16_t offset = 0;
  offset = buffer.grabData(offset, &pId, sizeof(pId));
  offset = buffer.grabData(offset, &pParentId, sizeof(pParentId));
  offset = buffer.grabData(offset, &pFlags, sizeof(pFlags));
  offset = buffer.grabData(offset, &pCTime, sizeof(pCTime));
  offset = buffer.grabData(offset, &pCUid, sizeof(pCUid));
  offset = buffer.grabData(offset, &pCGid, sizeof(pCGid));
  offset = buffer.grabData(offset, &pMode, sizeof(pMode));
  offset = buffer.grabData(offset, &pACLId, sizeof(pACLId));
  uint16_t len;
  offset = buffer.grabData(offset, &len, 2);
  char strBuffer[len];
  offset = buffer.grabData(offset, static_cast<char*>(strBuffer), len);
  pName = static_cast<char*>(strBuffer);
  pMTime.tv_sec = pCTime.tv_sec;
  pMTime.tv_nsec = pCTime.tv_nsec;
  uint16_t len1 = 0;
  uint16_t len2 = 0;
  len = 0;
  offset = buffer.grabData(offset, &len, sizeof(len));

  for (uint16_t i = 0; i < len; ++i) {
    offset = buffer.grabData(offset, &len1, sizeof(len1));
    char strBuffer1[len1];
    offset = buffer.grabData(offset, static_cast<char*>(strBuffer1), len1);
    offset = buffer.grabData(offset, &len2, sizeof(len2));
    char strBuffer2[len2];
    offset = buffer.grabData(offset, static_cast<char*>(strBuffer2), len2);
    std::string key = static_cast<char*>(strBuffer1);

    if (key == "sys.mtime.s") {
      // Stored modification time in s
      pMTime.tv_sec = strtoull(static_cast<char*>(strBuffer2), nullptr, 10);
    } else {
      if (key == "sys.mtime.ns") {
        // Stored modification time in ns
        pMTime.tv_nsec = strtoull(static_cast<char*>(strBuffer2), nullptr, 10);
      } else {
        pXAttrs.insert(std::pair<char*, char*>(static_cast<char*>(strBuffer1),
                                               static_cast<char*>(strBuffer2)));
      }
    }
  }

  // Rebuild the file and subcontainer keys
  std::string files_key = stringify(pId) + constants::sMapFilesSuffix;
  pFilesMap.setKey(files_key);
  std::string dirs_key = stringify(pId) + constants::sMapDirsSuffix;
  pDirsMap.setKey(dirs_key);

  // Grab the files and subcontainers
  try {
    std::string cursor = "0";
    std::pair<std::string, std::unordered_map<std::string, std::string>> reply;

    do {
      reply = pFilesMap.hscan(cursor);
      cursor = reply.first;

      for (auto && elem : reply.second) {
        mFilesMap.emplace(elem.first, std::stoull(elem.second));
      }
    } while (cursor != "0");

    // Get the subcontainers
    cursor = "0";

    do {
      reply = pDirsMap.hscan(cursor);
      cursor = reply.first;

      for (auto && elem : reply.second) {
        mDirsMap.emplace(elem.first, std::stoull(elem.second));
      }
    } while (cursor != "0");
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << pId << "failed to get subentries";
    throw e;
  }
}

EOSNSNAMESPACE_END
