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

#include <sstream>
#include "common/StacktraceHere.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/utils/DataHelper.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Empty constructor
//------------------------------------------------------------------------------
FileMD::FileMD()
{
  pFileMDSvc = nullptr;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMD::FileMD(IFileMD::id_t id, IFileMDSvc* fileMDSvc):
  pFileMDSvc(fileMDSvc), mClock(1)
{
  mFile.set_id(id);
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
FileMD*
FileMD::clone() const
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  return new FileMD(*this);
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
FileMD::FileMD(const FileMD& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Asignment operator
//------------------------------------------------------------------------------
FileMD&
FileMD::operator = (const FileMD& other)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile = other.mFile;
  mClock = other.mClock;
  pFileMDSvc   = 0;
  return *this;
}

//------------------------------------------------------------------------------
// Set name
//------------------------------------------------------------------------------
void FileMD::setName(const std::string& name)
{
  if(name.find('/') != std::string::npos) {
    eos_static_crit("Detected slashes in filename: %s", eos::common::getStacktrace().c_str());
    throw_mdexception(EINVAL, "Bug, detected slashes in file name: " << name);
  }

  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_name(name);
}

//------------------------------------------------------------------------------
// Add location
//------------------------------------------------------------------------------
void
FileMD::addLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (hasLocationNoLock(location)) {
    return;
  }

  mFile.add_locations(location);
  lock.unlock();
  IFileMDChangeListener::Event e(this, IFileMDChangeListener::LocationAdded,
                                 location);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Remove location that was previously unlinked
//------------------------------------------------------------------------------
void
FileMD::removeLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  for (auto it = mFile.mutable_unlink_locations()->cbegin();
       it != mFile.mutable_unlink_locations()->cend(); ++it) {
    if (*it == location) {
      it = mFile.mutable_unlink_locations()->erase(it);
      lock.unlock();
      IFileMDChangeListener::Event
      e(this, IFileMDChangeListener::LocationRemoved, location);
      pFileMDSvc->notifyListeners(&e);
      return;
    }
  }
}

//------------------------------------------------------------------------------
// Remove all locations that were previously unlinked
//------------------------------------------------------------------------------
void
FileMD::removeAllLocations()
{
  while (true) {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    auto it = mFile.unlink_locations().cbegin();

    if (it == mFile.unlink_locations().cend()) {
      return;
    }

    location_t location = *it;
    lock.unlock();
    removeLocation(location);
  }
}

//------------------------------------------------------------------------------
// Unlink location
//------------------------------------------------------------------------------
void
FileMD::unlinkLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  for (auto it = mFile.mutable_locations()->cbegin();
       it != mFile.mutable_locations()->cend(); ++it) {
    if (*it == location) {
      mFile.add_unlink_locations(*it);
      it = mFile.mutable_locations()->erase(it);
      lock.unlock();
      IFileMDChangeListener::Event
      e(this, IFileMDChangeListener::LocationUnlinked, location);
      pFileMDSvc->notifyListeners(&e);
      return;
    }
  }
}

//------------------------------------------------------------------------------
// Unlink all locations
//------------------------------------------------------------------------------
void
FileMD::unlinkAllLocations()
{
  while (true) {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    auto it = mFile.locations().cbegin();

    if (it == mFile.locations().cend()) {
      return;
    }

    location_t location = *it;
    lock.unlock();
    unlinkLocation(location);
  }
}

//------------------------------------------------------------------------
//  Env Representation
//------------------------------------------------------------------------
void
FileMD::getEnv(std::string& env, bool escapeAnd)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  env = "";
  std::ostringstream oss;
  std::string saveName = mFile.name();

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
  (void) getCTimeNoLock(ctime);
  (void) getMTimeNoLock(mtime);
  oss << "name=" << saveName << "&id=" << mFile.id()
      << "&ctime=" << ctime.tv_sec << "&ctime_ns=" << ctime.tv_nsec
      << "&mtime=" << mtime.tv_sec << "&mtime_ns=" << mtime.tv_nsec
      << "&size=" << mFile.size() << "&cid=" << mFile.cont_id()
      << "&uid=" << mFile.uid() << "&gid=" << mFile.gid()
      << "&lid=" << mFile.layout_id() << "&flags=" << mFile.flags()
      << "&link=" << mFile.link_name();
  env += oss.str();
  env += "&location=";
  char locs[16];

  for (const auto& elem : mFile.locations()) {
    snprintf(static_cast<char*>(locs), sizeof(locs), "%u", elem);
    env += static_cast<char*>(locs);
    env += ",";
  }

  for (const auto& elem : mFile.unlink_locations()) {
    snprintf(static_cast<char*>(locs), sizeof(locs), "!%u", elem);
    env += static_cast<char*>(locs);
    env += ",";
  }

  env += "&checksum=";
  uint8_t size = mFile.checksum().size();

  for (uint8_t i = 0; i < size; i++) {
    char hx[3];
    hx[0] = 0;
    snprintf(static_cast<char*>(hx), sizeof(hx), "%02x",
             *(unsigned char*)(mFile.checksum().data() + i));
    env += static_cast<char*>(hx);
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a std::string buffer
//------------------------------------------------------------------------------
void
FileMD::serialize(eos::Buffer& buffer)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  if (pFileMDSvc == nullptr) {
    MDException ex(ENOTSUP);
    ex.getMessage() << "This was supposed to be a read only copy!";
    throw ex;
  }

  // Increase clock to mark that metadata file has suffered updates
  ++mClock;
  // Align the buffer to 4 bytes to efficiently compute the checksum
  size_t obj_size = mFile.ByteSizeLong();
  uint32_t align_size = (obj_size + 3) >> 2 << 2;
  size_t sz = sizeof(align_size);
  size_t msg_size = align_size + 2 * sz;
  buffer.setSize(msg_size);
  // Write the checksum value, size of the raw protobuf object and then the
  // actual protobuf object serialized
  const char* ptr = buffer.getDataPtr() + 2 * sz;
  google::protobuf::io::ArrayOutputStream aos((void*)ptr, align_size);

  if (!mFile.SerializeToZeroCopyStream(&aos)) {
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
// Initialize from protobuf contents
//------------------------------------------------------------------------------
void
FileMD::initialize(eos::ns::FileMdProto&& proto)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile = std::move(proto);
}

//------------------------------------------------------------------------------
// Deserialize from buffer
//------------------------------------------------------------------------------
void
FileMD::deserialize(const eos::Buffer& buffer)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  Serialization::deserializeFile(buffer, mFile);
}

//------------------------------------------------------------------------------
// Set size - 48 bytes will be used
//------------------------------------------------------------------------------
void
FileMD::setSize(uint64_t size)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  int64_t sizeChange = (size & 0x0000ffffffffffff) - mFile.size();
  mFile.set_size(size & 0x0000ffffffffffff);
  lock.unlock();
  IFileMDChangeListener::Event e(this, IFileMDChangeListener::SizeChange, 0,
                                 sizeChange);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Get creation time, no lock
//------------------------------------------------------------------------------
void
FileMD::getCTimeNoLock(ctime_t& ctime) const
{
  (void) memcpy(&ctime, mFile.ctime().data(), sizeof(ctime_t));
}

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void
FileMD::getCTime(ctime_t& ctime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getCTimeNoLock(ctime);
}

//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void
FileMD::setCTime(ctime_t ctime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_ctime(&ctime, sizeof(ctime));
}

//----------------------------------------------------------------------------
// Set creation time to now
//----------------------------------------------------------------------------
void
FileMD::setCTimeNow()
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
// Get modification time, no locks
//------------------------------------------------------------------------------
void
FileMD::getMTimeNoLock(ctime_t& mtime) const
{
  (void) memcpy(&mtime, mFile.mtime().data(), sizeof(ctime_t));
}

//------------------------------------------------------------------------------
// Get modification time
//------------------------------------------------------------------------------
void
FileMD::getMTime(ctime_t& mtime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getMTimeNoLock(mtime);
}

//------------------------------------------------------------------------------
// Set modification time
//------------------------------------------------------------------------------
void
FileMD::setMTime(ctime_t mtime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_mtime(&mtime, sizeof(mtime));
}

//------------------------------------------------------------------------------
// Set modification time to now
//------------------------------------------------------------------------------
void
FileMD::setMTimeNow()
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
  setMTime(tnow);
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
FileMD::getAttributes() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  std::map<std::string, std::string> xattrs;

  for (const auto& elem : mFile.xattrs()) {
    xattrs.insert(elem);
  }

  return xattrs;
}

bool
FileMD::hasUnlinkedLocation(IFileMD::location_t location)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  for (int i = 0; i < mFile.unlink_locations_size(); ++i) {
    if (mFile.unlink_locations()[i] == location) {
      return true;
    }
  }

  return false;
}

EOSNSNAMESPACE_END
