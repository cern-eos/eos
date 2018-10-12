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
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/PermissionHandler.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "common/Assert.hh"
#include "common/StacktraceHere.hh"
#include "common/Logging.hh"
#include <sys/stat.h>
#include <algorithm>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMD::ContainerMD(ContainerMD::id_t id, IFileMDSvc* file_svc,
                         IContainerMDSvc* cont_svc)
  : IContainerMD(),
    pFilesKey(stringify(id) + constants::sMapFilesSuffix),
    pDirsKey(stringify(id) + constants::sMapDirsSuffix), mClock(1)
{
  mSubcontainers->set_deleted_key("");
  mFiles->set_deleted_key("");
  mSubcontainers->set_empty_key("##_EMPTY_##");
  mFiles->set_empty_key("##_EMPTY_##");
  mCont.set_id(id);
  mCont.set_mode(040755);

  if (!cont_svc && !file_svc) {
    // "Standalone" ContainerMD, without associated container service.
    // Don't call functions which might modify metadata..
    // This is a hack, it would be probably cleaner to remove the services
    // from this class altogether.
    return;
  }

  setServices(file_svc, cont_svc);
}

//------------------------------------------------------------------------------
// Set namespace services
//------------------------------------------------------------------------------
void ContainerMD::setServices(IFileMDSvc* file_svc, IContainerMDSvc* cont_svc)
{
  eos_assert(pFileSvc == nullptr && pContSvc == nullptr);
  eos_assert(file_svc != nullptr && cont_svc != nullptr);
  pFileSvc = file_svc;
  pContSvc = cont_svc;
  ContainerMDSvc* impl_cont_svc = dynamic_cast<ContainerMDSvc*>(cont_svc);

  if (!impl_cont_svc) {
    MDException e(EFAULT);
    e.getMessage() << __FUNCTION__ << " ContainerMDSvc dynamic cast failed";
    throw e;
  }

  pQcl = impl_cont_svc->pQcl;
  pFlusher = impl_cont_svc->pFlusher;
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
  return *this;
}

//------------------------------------------------------------------------------
//! Turn a ContainerMDPtr into FileOrContainerMD.
//------------------------------------------------------------------------------
static FileOrContainerMD wrapContainerMD(IContainerMDPtr ptr) {
  return FileOrContainerMD {nullptr, ptr};
}

//------------------------------------------------------------------------------
//! Turn a FileMDPtr into FileOrContainerMD.
//------------------------------------------------------------------------------
static FileOrContainerMD wrapFileMD(IFileMDPtr ptr) {
  return FileOrContainerMD {ptr, nullptr};
}

//------------------------------------------------------------------------------
//! Extract FileMDPtr out of FileOrContainerMD.
//------------------------------------------------------------------------------
static IFileMDPtr extractFileMD(FileOrContainerMD ptr) {
  return ptr.file;
}

//------------------------------------------------------------------------------
//! Extract ContainerMDPtr out of FileOrContainerMD.
//------------------------------------------------------------------------------
static IContainerMDPtr extractContainerMD(FileOrContainerMD ptr) {
  return ptr.container;
}

//------------------------------------------------------------------------------
//! Find item
//------------------------------------------------------------------------------
folly::Future<FileOrContainerMD>
ContainerMD::findItem(const std::string& name)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // We're looking for "name". Look inside subcontainer map to check if there's
  // a container with such name.
  //----------------------------------------------------------------------------
  auto iter = mSubcontainers->find(name);
  if(iter != mSubcontainers->end()) {
    //--------------------------------------------------------------------------
    // We have a hit, this is a ContainerMD. Retrieve result asynchronously
    // from container service.
    //--------------------------------------------------------------------------
    ContainerIdentifier target(iter->second);
    lock.unlock();

    folly::Future<FileOrContainerMD> fut = pContSvc->getContainerMDFut(target.getUnderlyingUInt64())
    .then(wrapContainerMD)
    .onError([this, name](const folly::exception_wrapper &e) {
      //------------------------------------------------------------------------
      // Should not happen...
      //------------------------------------------------------------------------
      eos_static_crit("Exception occurred while looking up container with name %s in subcontainer with id %llu: %s", name.c_str(), getId(), e.what());
      return FileOrContainerMD {};
    });

    return fut;
  }

  //----------------------------------------------------------------------------
  // This is not a ContainerMD.. maybe it's a FileMD?
  //----------------------------------------------------------------------------
  auto iter2 = mFiles->find(name);
  if (iter2 != mFiles->end()) {
    //--------------------------------------------------------------------------
    // We have a hit, this is a FileMD. Retrieve result asynchronously
    // from file service.
    //--------------------------------------------------------------------------
    FileIdentifier target(iter2->second);
    lock.unlock();

    folly::Future<FileOrContainerMD> fut = pFileSvc->getFileMDFut(target.getUnderlyingUInt64())
    .then(wrapFileMD)
    .onError([this, name](const folly::exception_wrapper &e) {
      //------------------------------------------------------------------------
      // Should not happen...
      //------------------------------------------------------------------------
      eos_static_crit("Exception occurred while looking up file with name %s in subcontainer with id %llu: %s", name.c_str(), getId(), e.what());
      return FileOrContainerMD {};
    });

    return fut;
  }

  //----------------------------------------------------------------------------
  // Nope, "name" doesn't exist in this container.
  //----------------------------------------------------------------------------
  return FileOrContainerMD {};
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
ContainerMD::removeContainer(const std::string& name)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  auto it = mSubcontainers->find(name);

  if (it == mSubcontainers->end()) {
    MDException e(ENOENT);
    e.getMessage()  << __FUNCTION__ << " Container " << name << " not found";
    throw e;
  }

  mSubcontainers->erase(it);
  mSubcontainers->resize(0);
  // Delete container also from KV backend
  pFlusher->hdel(pDirsKey, name);
}

//------------------------------------------------------------------------------
// Add container
//------------------------------------------------------------------------------
void
ContainerMD::addContainer(IContainerMD* container)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (container->getName().empty()) {
    eos_static_crit(eos::common::getStacktrace().c_str());
    throw_mdexception(EINVAL,
                      "Attempted to add container with empty name! ID: " << container->getId() <<
                      ", target container ID: " << mCont.id());
  }

  container->setParentId(mCont.id());
  auto ret = mSubcontainers->insert(std::make_pair(container->getName(),
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
// Find file, asynchronous API
//------------------------------------------------------------------------------
folly::Future<IFileMDPtr>
ContainerMD::findFileFut(const std::string& name)
{
  return this->findItem(name).then(extractFileMD);
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
ContainerMD::findFile(const std::string& name)
{
  return this->findItem(name).get().file;
}

//------------------------------------------------------------------------------
// Find subcontainer, asynchronous API
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
ContainerMD::findContainerFut(const std::string& name)
{
  return this->findItem(name).then(extractContainerMD);
}

//------------------------------------------------------------------------------
// Find subcontainer
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMD::findContainer(const std::string& name)
{
  return this->findItem(name).get().container;
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
ContainerMD::addFile(IFileMD* file)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (file->getName().empty()) {
    eos_static_crit(eos::common::getStacktrace().c_str());
    throw_mdexception(EINVAL,
                      "Attempted to add file with empty filename! ID: " << file->getId() <<
                      ", target container ID: " << mCont.id());
  }

  auto containerConflict = mSubcontainers->find(file->getName());
  if(containerConflict != mSubcontainers->end()) {
    eos_static_crit(eos::common::getStacktrace().c_str());
    throw_mdexception(EEXIST, "Attempted to add file with name " << file->getName() <<
                      " while a subcontainer exists already there.");
  }

  auto fileConflict = mFiles->find(file->getName());
  if(fileConflict != mFiles->end() && fileConflict->second != file->getId()) {
    eos_static_crit(eos::common::getStacktrace().c_str());
    throw_mdexception(EEXIST, "Attempted to add file with name " << file->getName() <<
                      " while a different file exists already there.");
  }

  file->setContainerId(mCont.id());
  (void)mFiles->insert(std::make_pair(file->getName(), file->getId()));
  pFlusher->hset(pFilesKey, file->getName(), std::to_string(file->getId()));
  lock.unlock();

  if (file->getSize() != 0u) {
    IFileMDChangeListener::Event e(file, IFileMDChangeListener::SizeChange, 0,
                                   file->getSize());
    pFileSvc->notifyListeners(&e);
  }
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
void
ContainerMD::removeFile(const std::string& name)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  auto iter = mFiles->find(name);

  if (iter != mFiles->end()) {
    IFileMD::id_t id = iter->second;
    mFiles->erase(iter);
    mFiles->resize(0);
    pFlusher->hdel(pFilesKey, name);

    try {
      lock.unlock();
      std::shared_ptr<IFileMD> file = pFileSvc->getFileMD(id);
      // NOTE: This is an ugly hack. The file object has no reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      IFileMDChangeListener::Event
      e(file.get(), IFileMDChangeListener::SizeChange, mCont.id(), -file->getSize());
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
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mFiles->size();
}

//----------------------------------------------------------------------------
// Get number of containers
//----------------------------------------------------------------------------
size_t
ContainerMD::getNumContainers()
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mSubcontainers->size();
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

  // Filter out based on sys.mask
  mode_t filteredMode = PermissionHandler::filterWithSysMask(mCont.xattrs(),
                        mCont.mode());
  // Convert the flags
  char convFlags = PermissionHandler::convertRequested(flags);
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  // Check the perms
  if (uid == mCont.uid()) {
    char user = PermissionHandler::convertModetUser(filteredMode);
    return PermissionHandler::checkPerms(user, convFlags);
  }

  if (gid == mCont.gid()) {
    char group = PermissionHandler::convertModetGroup(filteredMode);
    return PermissionHandler::checkPerms(group, convFlags);
  }

  char other = PermissionHandler::convertModetOther(filteredMode);
  return PermissionHandler::checkPerms(other, convFlags);
}

//------------------------------------------------------------------------------
// Set name
//------------------------------------------------------------------------------
void
ContainerMD::setName(const std::string& name)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  if(mCont.id() != 1 && name.find('/') != std::string::npos) {
    eos_static_crit("Detected slashes in container name: %s", eos::common::getStacktrace().c_str());
    throw_mdexception(EINVAL, "Bug, detected slashes in container name: " << name);
  }

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
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
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
  setCTime(tnow);
}

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void
ContainerMD::getCTime(ctime_t& ctime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getCTimeNoLock(ctime);
}

//------------------------------------------------------------------------------
// Get creation time, no locks
//------------------------------------------------------------------------------
void
ContainerMD::getCTimeNoLock(ctime_t& ctime) const
{
  (void) memcpy(&ctime, mCont.ctime().data(), sizeof(ctime));
}

//------------------------------------------------------------------------------
// Set modification time
//------------------------------------------------------------------------------
void
ContainerMD::setMTime(mtime_t mtime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
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
  setMTime(tnow);
}

//------------------------------------------------------------------------------
// Get modification time
//------------------------------------------------------------------------------
void
ContainerMD::getMTime(mtime_t& mtime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getMTimeNoLock(mtime);
}

//------------------------------------------------------------------------------
// Get modification time, no lock
//------------------------------------------------------------------------------
void
ContainerMD::getMTimeNoLock(mtime_t& mtime) const
{
  (void) memcpy(&mtime, mCont.mtime().data(), sizeof(mtime));
}

//------------------------------------------------------------------------------
// Set propagated modification time (if newer)
//------------------------------------------------------------------------------
bool
ContainerMD::setTMTime(tmtime_t tmtime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  tmtime_t tmt;
  getTMTimeNoLock(tmt);

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
// Get propagated modification time, no locks
//------------------------------------------------------------------------------
void
ContainerMD::getTMTimeNoLock(tmtime_t& tmtime)
{
  (void) memcpy(&tmtime, mCont.stime().data(), sizeof(tmtime));
}

//------------------------------------------------------------------------------
// Get propagated modification time
//------------------------------------------------------------------------------
void
ContainerMD::getTMTime(tmtime_t& tmtime)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getTMTimeNoLock(tmtime);
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
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
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
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
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
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
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
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
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
// Load children
//------------------------------------------------------------------------------
void
ContainerMD::loadChildren()
{
  // Requires lock be taken outside.
  // Rebuild the file and subcontainer keys
  pFilesKey = stringify(mCont.id()) + constants::sMapFilesSuffix;
  pDirsKey = stringify(mCont.id()) + constants::sMapDirsSuffix;

  if (pQcl) {
    mFiles = MetadataFetcher::getFilesInContainer(*pQcl,
             ContainerIdentifier(mCont.id()));
    mSubcontainers = MetadataFetcher::getSubContainers(*pQcl,
                     ContainerIdentifier(mCont.id()));
  } else {
    // I think this case only happens inside some tests.. remove eventually?
    mFiles->clear();
    mSubcontainers->clear();
  }
}

//------------------------------------------------------------------------------
// Deserialize from buffer
//------------------------------------------------------------------------------
void
ContainerMD::deserialize(Buffer& buffer)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  Serialization::deserializeContainer(buffer, mCont);
  loadChildren();
}

//------------------------------------------------------------------------------
// Initialize, inject children
//------------------------------------------------------------------------------
void
ContainerMD::initialize(eos::ns::ContainerMdProto&& proto,
                        IContainerMD::FileMap&& fileMap, IContainerMD::ContainerMap&& containerMap)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mCont = std::move(proto);
  mFiles.get() = std::move(fileMap);
  mSubcontainers.get() = std::move(containerMap);
  // Rebuild the file and subcontainer keys
  pFilesKey = stringify(mCont.id()) + constants::sMapFilesSuffix;
  pDirsKey = stringify(mCont.id()) + constants::sMapDirsSuffix;
}

//------------------------------------------------------------------------------
// Initialize from a ContainerMdProto object, without loading children maps.
//------------------------------------------------------------------------------
void
ContainerMD::initializeWithoutChildren(eos::ns::ContainerMdProto&& proto)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mCont = std::move(proto);
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
ContainerMD::getAttributes() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
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
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  env.clear();
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
  (void) getCTimeNoLock(ctime);
  (void) getMTimeNoLock(mtime);
  (void) getTMTimeNoLock(stime);
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
