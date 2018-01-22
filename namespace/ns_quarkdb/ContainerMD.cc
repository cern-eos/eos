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
#include "namespace/utils/DataHelper.hh"
#include "namespace/utils/StringConvertion.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include <sys/stat.h>
#include <algorithm>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(id_t id, IFileMDSvc* file_svc,
                         IContainerMDSvc* cont_svc)
  : IContainerMD(), pContSvc(cont_svc), pFileSvc(file_svc),
    pFilesKey(stringify(id) + constants::sMapFilesSuffix),
    pDirsKey(stringify(id) + constants::sMapDirsSuffix), mClock(1)
{
  mCont.set_id(id);
  mCont.set_mode(040755);
  ContainerMDSvc* impl_cont_svc = dynamic_cast<ContainerMDSvc*>(cont_svc);

  if (!impl_cont_svc) {
    MDException e(EFAULT);
    e.getMessage() << __FUNCTION__ << " ContainerMDSvc dynamic cast failed";
    throw e;
  }

  pQcl = impl_cont_svc->pQcl;
  pFlusher = impl_cont_svc->pFlusher;
  pFilesMap = qclient::QHash(*pQcl, pFilesKey);
  pDirsMap = qclient::QHash(*pQcl, pDirsKey);
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
  mCont    = other.mCont;
  pContSvc = other.pContSvc;
  pFileSvc = other.pFileSvc;
  pQcl     = other.pQcl;
  mClock   = other.mClock;
  pFlusher = other.pFlusher;
  pDirsKey = other.pDirsKey;
  pFilesKey = other.pFilesKey;
  pFilesMap = qclient::QHash(*pQcl, pFilesKey);
  pDirsMap = qclient::QHash(*pQcl, pDirsKey);
  return *this;
}

//------------------------------------------------------------------------------
// Find subcontainer
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMD::findContainer(const std::string& name)
{
  auto iter = mSubcontainers.find(name);

  if (iter == mSubcontainers.end()) {
    return nullptr;
  }

  std::shared_ptr<IContainerMD> cont;

  try {
    cont = pContSvc->getContainerMD(iter->second);
  } catch (const MDException& ex) {
    cont = nullptr;
  }

  // Curate the list of subcontainers in case entry is not found
  if (cont == nullptr) {
    pFlusher->hdel(pDirsKey, name);
    mSubcontainers.erase(iter);
  }

  return cont;
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
ContainerMD::removeContainer(const std::string& name)
{
  auto it = mSubcontainers.find(name);

  if (it == mSubcontainers.end()) {
    MDException e(ENOENT);
    e.getMessage()  << __FUNCTION__ << " Container " << name << " not found";
    throw e;
  }

  mSubcontainers.erase(it);
  mSubcontainers.resize(0);
  // Delete container also from KV backend
  pFlusher->hdel(pDirsKey, name);
}

//------------------------------------------------------------------------------
// Add container
//------------------------------------------------------------------------------
void
ContainerMD::addContainer(IContainerMD* container)
{
  container->setParentId(mCont.id());
  auto ret = mSubcontainers.insert(std::make_pair(container->getName(),
                                   container->getId()));

  // @todo (esindril): Here we (should ?!) follow the behaviour of the namespace
  // in memory and don't do any extra checks but this can lead to multiple
  // accounting of this file in the quota view since the listeners are notified
  // every time we call this ....
  if (!ret.second) {
    eos::MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__ << " Container with name \""
                    << container->getName() << "\" already exists";
    throw e;
  }

  // Add to new container to KV backend
  pFlusher->hset(pDirsKey, container->getName(), stringify(container->getId()));
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
ContainerMD::findFile(const std::string& name)
{
  auto iter = mFiles.find(name);

  if (iter == mFiles.end()) {
    return nullptr;
  }

  std::shared_ptr<IFileMD> file;

  try {
    file = pFileSvc->getFileMD(iter->second);
  } catch (MDException& e) {
    file = nullptr;
  }

  // Curate the list of files in case file entry is not found
  if (file == nullptr) {
    pFlusher->hdel(pFilesKey, name);
    mFiles.erase(iter);
  }

  return file;
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
ContainerMD::addFile(IFileMD* file)
{
  file->setContainerId(mCont.id());
  (void)mFiles.insert(std::make_pair(file->getName(), file->getId()));
  // @todo (esindril): Here we follow the behaviour of the namespace in memory
  // and don't do any extra checks but this can lead to multiple accounting
  // of this file in the quota view since the listeners are notified every
  // time we call this ....
  // if (!ret.second) {
  //   MDException e(EINVAL);
  //   e.getMessage() << "Error, file #" << file->getId() << " already exists";
  //   throw e;
  // }
  pFlusher->hset(pFilesKey, file->getName(), std::to_string(file->getId()));

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
  auto iter = mFiles.find(name);

  if (iter != mFiles.end()) {
    IFileMD::id_t id = iter->second;
    mFiles.erase(iter);
    mFiles.resize(0);
    // Do async call to KV backend
    pFlusher->hdel(pFilesKey, name);

    try {
      std::shared_ptr<IFileMD> file = pFileSvc->getFileMD(id);
      // NOTE: This is an ugly hack. The file object has no reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      IFileMDChangeListener::Event
      e(file.get(), IFileMDChangeListener::SizeChange, mCont.id(),
        0, -file->getSize());
      pFileSvc->notifyListeners(&e);
    } catch (MDException& e) {
      // File already removed
    }
  }
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
size_t
ContainerMD::getNumFiles()
{
  return mFiles.size();
}

//----------------------------------------------------------------------------
// Get number of containers
//----------------------------------------------------------------------------
size_t
ContainerMD::getNumContainers()
{
  return mSubcontainers.size();
}

//------------------------------------------------------------------------
// Clean up the entire contents for the container. Delete files and
// containers recursively
//------------------------------------------------------------------------
void
ContainerMD::cleanUp()
{
  for (const auto& elem : mFiles) {
    auto file = pFileSvc->getFileMD(elem.second);
    pFileSvc->removeFile(file.get());
  }

  mFiles.clear();

  // Remove all subcontainers
  for (const auto& elem : mSubcontainers) {
    auto cont = pContSvc->getContainerMD(elem.second);
    cont->cleanUp();
    pContSvc->removeContainer(cont.get());
  }

  mSubcontainers.clear();
  // Delete files and subcontainers map from the KV backend
  pFlusher->del(pFilesKey);
  pFlusher->del(pDirsKey);
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
  if (uid == mCont.uid()) {
    char user = convertModetUser(mCont.mode());
    return checkPerms(user, convFlags);
  }

  if (gid == mCont.gid()) {
    char group = convertModetGroup(mCont.mode());
    return checkPerms(group, convFlags);
  }

  char other = convertModetOther(mCont.mode());
  return checkPerms(other, convFlags);
}

//------------------------------------------------------------------------------
// Set name
//------------------------------------------------------------------------------
void
ContainerMD::setName(const std::string& name)
{
  // // Check that there is no clash with other subcontainers having the same name
  // if (mCont.parent_id() != 0u) {
  //   auto parent = pContSvc->getContainerMD(mCont.parent_id());
  //   if (parent->findContainer(name)) {
  //     eos::MDException e(EINVAL);
  //     e.getMessage() << "Container with name \"" << name << "\" already exists";
  //     throw e;
  //   }
  // }
  mCont.set_name(name);
}

//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void
ContainerMD::setCTime(ctime_t ctime)
{
  mCont.set_ctime(&ctime, sizeof(ctime));
}

//------------------------------------------------------------------------------
// Set creation time to now
//------------------------------------------------------------------------------
void
ContainerMD::setCTimeNow()
{
  struct timespec tnow;
#ifdef __APPLE__
  struct timeval tv;
  gettimeofday(&tv, 0);
  tnow.tv_sec = tv.tv_sec;
  tnow.tv_nsec = tv.tv_usec * 1000;
#else
  clock_gettime(CLOCK_REALTIME, &tnow);
#endif
  mCont.set_ctime(&tnow, sizeof(tnow));
}

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void
ContainerMD::getCTime(ctime_t& ctime) const
{
  (void) memcpy(&ctime, mCont.ctime().data(), sizeof(ctime));
}

//------------------------------------------------------------------------------
// Set modification time
//------------------------------------------------------------------------------
void
ContainerMD::setMTime(mtime_t mtime)
{
  mCont.set_mtime(&mtime, sizeof(mtime));
}

//------------------------------------------------------------------------------
// Set creation time to now
//------------------------------------------------------------------------------
void
ContainerMD::setMTimeNow()
{
  struct timespec tnow;
#ifdef __APPLE__
  struct timeval tv = {0};
  gettimeofday(&tv, 0);
  tnow.tv_sec = tv.tv_sec;
  tnow.tv_nsec = tv.tv_usec * 1000;
#else
  clock_gettime(CLOCK_REALTIME, &tnow);
#endif
  mCont.set_mtime(&tnow, sizeof(tnow));
}

//------------------------------------------------------------------------------
// Get modification time
//------------------------------------------------------------------------------
void
ContainerMD::getMTime(mtime_t& mtime) const
{
  (void) memcpy(&mtime, mCont.mtime().data(), sizeof(mtime));
}

//------------------------------------------------------------------------------
// Set propagated modification time (if newer)
//------------------------------------------------------------------------------
bool
ContainerMD::setTMTime(tmtime_t tmtime)
{
  tmtime_t tmt;
  getTMTime(tmt);

  if (((tmt.tv_sec == 0) && (tmt.tv_nsec == 0)) ||
      (tmtime.tv_sec > tmt.tv_sec) ||
      ((tmtime.tv_sec == tmt.tv_sec) &&
       (tmtime.tv_nsec > tmt.tv_nsec))) {
    mCont.set_stime(&tmtime, sizeof(tmtime));
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
ContainerMD::getTMTime(tmtime_t& tmtime)
{
  (void) memcpy(&tmtime, mCont.stime().data(), sizeof(tmtime));
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
// Update tree size
//------------------------------------------------------------------------------
uint64_t
ContainerMD::updateTreeSize(int64_t delta)
{
  uint64_t sz = mCont.tree_size();

  // Avoid usigned underflow
  if ((delta < 0) && (std::llabs(delta) > sz)) {
    sz = 0;
  } else {
    sz += delta;
  }

  mCont.set_tree_size(sz);
  return sz;
}

//------------------------------------------------------------------------------
// Get the attribute
//------------------------------------------------------------------------------
std::string
ContainerMD::getAttribute(const std::string& name) const
{
  auto it = mCont.xattrs().find(name);

  if (it == mCont.xattrs().end()) {
    MDException e(ENOENT);
    e.getMessage()  << __FUNCTION__  << " Attribute: " << name << " not found";
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
  auto it = mCont.xattrs().find(name);

  if (it != mCont.xattrs().end()) {
    mCont.mutable_xattrs()->erase(it->first);
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ContainerMD::serialize(Buffer& buffer)
{
  // Align the buffer to 4 bytes to efficiently compute the checksum
  ++mClock;
  size_t obj_size = mCont.ByteSizeLong();
  uint32_t align_size = (obj_size + 3) >> 2 << 2;
  size_t sz = sizeof(align_size);
  size_t msg_size = align_size + 2 * sz;
  buffer.setSize(msg_size);
  // Write the checksum value, size of the raw protobuf object and then the
  // actual protobuf object serialized
  const char* ptr = buffer.getDataPtr() + 2 * sz;
  google::protobuf::io::ArrayOutputStream aos((void*)ptr, align_size);

  if (!mCont.SerializeToZeroCopyStream(&aos)) {
    MDException ex(EIO);
    ex.getMessage() << "Failed while serializing buffer";
    throw ex;
  }

  // Compute the CRC32C checkusm
  uint32_t cksum = DataHelper::computeCRC32C((void*)ptr, align_size);
  cksum = DataHelper::finalizeCRC32C(cksum);
  // Point to the beginning to fill in the checksum and size of useful data
  ptr = buffer.getDataPtr();
  (void) memcpy((void*)ptr, &cksum, sz);
  ptr += sz;
  (void) memcpy((void*)ptr, &obj_size, sz);
}

//------------------------------------------------------------------------------
// Deserialize from buffer
//------------------------------------------------------------------------------
void
ContainerMD::deserialize(Buffer& buffer)
{
  uint32_t cksum_expected = 0;
  uint32_t obj_size = 0;
  size_t sz = sizeof(cksum_expected);
  const char* ptr = buffer.getDataPtr();
  (void) memcpy(&cksum_expected, ptr, sz);
  ptr += sz;
  (void) memcpy(&obj_size, ptr, sz);
  size_t msg_size = buffer.getSize();
  uint32_t align_size = msg_size - 2 * sz;
  ptr += sz; // now pointing to the serialized object
  uint32_t cksum_computed = DataHelper::computeCRC32C((void*)ptr, align_size);
  cksum_computed = DataHelper::finalizeCRC32C(cksum_computed);

  if (cksum_expected != cksum_computed) {
    MDException ex(EIO);
    ex.getMessage() << "FileMD object checksum missmatch";
    throw ex;
  }

  google::protobuf::io::ArrayInputStream ais(ptr, obj_size);

  if (!mCont.ParseFromZeroCopyStream(&ais)) {
    MDException ex(EIO);
    ex.getMessage() << "Failed while deserializing buffer";
    throw ex;
  }

  // Rebuild the file and subcontainer keys
  pFilesKey = stringify(mCont.id()) + constants::sMapFilesSuffix;
  pFilesMap.setKey(pFilesKey);
  pDirsKey = stringify(mCont.id()) + constants::sMapDirsSuffix;
  pDirsMap.setKey(pDirsKey);

  // Grab the files and subcontainers
  constexpr int64_t count = 2000000;

  if (pQcl) {
    try {
      std::string cursor = "0";
      std::pair<std::string, std::unordered_map<std::string, std::string>> reply;

      do {
        reply = pFilesMap.hscan(cursor, count);
        cursor = reply.first;

        for (const auto& elem : reply.second) {
          mFiles.insert(std::make_pair(elem.first, std::stoull(elem.second)));
        }
      } while (cursor != "0");

      // Get the subcontainers
      cursor = "0";

      do {
        reply = pDirsMap.hscan(cursor, count);
        cursor = reply.first;

        for (const auto& elem : reply.second) {
          mSubcontainers.insert(std::make_pair(elem.first, std::stoull(elem.second)));
        }
      } while (cursor != "0");
    } catch (std::runtime_error& qdb_err) {
      MDException e(ENOENT);
      e.getMessage()  << __FUNCTION__  << " Container #" << mCont.id()
                      << " failed to get subentries";
      throw e;
    }
  }
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
ContainerMD::getAttributes() const
{
  XAttrMap xattrs;

  for (const auto& elem : mCont.xattrs()) {
    xattrs.insert(elem);
  }

  return xattrs;
}

//------------------------------------------------------------------------------
// Get env representation of the container object
//------------------------------------------------------------------------------
void
ContainerMD::getEnv(std::string& env, bool escapeAnd)
{
  env = "";
  std::ostringstream oss;
  std::string saveName = mCont.name();

  if (escapeAnd) {
    if (!saveName.empty()) {
      std::string from = "&";
      std::string to = "#AND#";
      size_t start_pos = 0;

      while ((start_pos = saveName.find(from, start_pos)) !=
             std::string::npos) {
        saveName.replace(start_pos, from.length(), to);
        start_pos += to.length();
      }
    }
  }

  ctime_t ctime;
  ctime_t mtime;
  ctime_t stime;
  (void) getCTime(ctime);
  (void) getMTime(mtime);
  (void) getTMTime(stime);
  oss << "name=" << saveName
      << "&id=" << mCont.id()
      << "&uid=" << mCont.uid() << "&gid=" << mCont.gid()
      << "&parentid=" << mCont.parent_id()
      << "&mode=" << std::oct << mCont.mode() << std::dec
      << "&flags=" << std::oct << mCont.flags() << std::dec
      << "&treesize=" << mCont.tree_size()
      << "&ctime=" << ctime.tv_sec << "&ctime_ns=" << ctime.tv_nsec
      << "&mtime=" << mtime.tv_sec << "&mtime_ns=" << mtime.tv_nsec
      << "&stime=" << stime.tv_sec << "&stime_ns=" << stime.tv_nsec;

  for (const auto& elem : mCont.xattrs()) {
    oss << "&" << elem.first << "=" << elem.second;
  }

  env += oss.str();
}

EOSNSNAMESPACE_END
