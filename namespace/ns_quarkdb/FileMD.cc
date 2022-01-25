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
#include <chrono>
#include "common/Logging.hh"
#include "common/StacktraceHere.hh"
#include "common/StringConversion.hh"
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
QuarkFileMD::QuarkFileMD()
{
  pFileMDSvc = nullptr;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkFileMD::QuarkFileMD(IFileMD::id_t id, IFileMDSvc* fileMDSvc):
  pFileMDSvc(fileMDSvc)
{
  mFile.set_id(id);
  mClock = std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
QuarkFileMD*
QuarkFileMD::clone() const
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  return new QuarkFileMD(*this);
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
QuarkFileMD::QuarkFileMD(const QuarkFileMD& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Assignment operator
//------------------------------------------------------------------------------
QuarkFileMD&
QuarkFileMD::operator = (const QuarkFileMD& other)
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
void QuarkFileMD::setName(const std::string& name)
{
  if (name.find('/') != std::string::npos) {
    eos_static_crit("Detected slashes in filename: %s",
                    eos::common::getStacktrace().c_str());
    throw_mdexception(EINVAL, "Bug, detected slashes in file name: " << name);
  }

  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_name(name);
}

//------------------------------------------------------------------------------
// Add location
//------------------------------------------------------------------------------
void
QuarkFileMD::addLocation(location_t location)
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
QuarkFileMD::removeLocation(location_t location)
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
QuarkFileMD::removeAllLocations()
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
QuarkFileMD::unlinkLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  for (auto it = mFile.mutable_locations()->cbegin();
       it != mFile.mutable_locations()->cend(); ++it) {
    if (*it == location) {
      // If location is already unlink, skip adding it
      if (!hasUnlinkedLocationNoLock(location)) {
        mFile.add_unlink_locations(*it);
      }

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
QuarkFileMD::unlinkAllLocations()
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
QuarkFileMD::getEnv(std::string& env, bool escapeAnd)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  env = "";
  std::ostringstream oss;
  std::string saveName = mFile.name();

  if (escapeAnd) {
    if (!saveName.empty()) {
      saveName = eos::common::StringConversion::SealXrdPath(saveName);
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
QuarkFileMD::serialize(eos::Buffer& buffer)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  // Increase clock to mark that metadata file has suffered updates
  mClock = std::chrono::high_resolution_clock::now().time_since_epoch().count();
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

  // Compute the CRC32C checksum
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
QuarkFileMD::initialize(eos::ns::FileMdProto&& proto)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile = std::move(proto);
}

//------------------------------------------------------------------------------
// Deserialize from buffer
//------------------------------------------------------------------------------
void
QuarkFileMD::deserialize(const eos::Buffer& buffer)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  Serialization::deserializeFile(buffer, mFile);
}

//----------------------------------------------------------------------------
// Get reference to underlying protobuf object
//----------------------------------------------------------------------------
const eos::ns::FileMdProto&
QuarkFileMD::getProto() const
{
  return mFile;
}

//------------------------------------------------------------------------------
// Set size - 48 bytes will be used
//------------------------------------------------------------------------------
void
QuarkFileMD::setSize(uint64_t size)
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
QuarkFileMD::getCTimeNoLock(ctime_t& ctime) const
{
  (void) memcpy(&ctime, mFile.ctime().data(), sizeof(ctime_t));
}

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void
QuarkFileMD::getCTime(ctime_t& ctime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getCTimeNoLock(ctime);
}

//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void
QuarkFileMD::setCTime(ctime_t ctime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_ctime(&ctime, sizeof(ctime));
}

//----------------------------------------------------------------------------
// Set creation time to now
//----------------------------------------------------------------------------
void
QuarkFileMD::setCTimeNow()
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
QuarkFileMD::getMTimeNoLock(ctime_t& mtime) const
{
  (void) memcpy(&mtime, mFile.mtime().data(), sizeof(ctime_t));
}

//------------------------------------------------------------------------------
// Get modification time
//------------------------------------------------------------------------------
void
QuarkFileMD::getMTime(ctime_t& mtime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getMTimeNoLock(mtime);
}

//------------------------------------------------------------------------------
// Set modification time
//------------------------------------------------------------------------------
void
QuarkFileMD::setMTime(ctime_t mtime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_mtime(&mtime, sizeof(mtime));
}

//------------------------------------------------------------------------------
// Set modification time to now
//------------------------------------------------------------------------------
void
QuarkFileMD::setMTimeNow()
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
  struct timespec default_ts = {0, 0};
  setSyncTime(default_ts);
}

/* SyncTime: whenever the file is changed, SyncTime becomes MTime. It is only
 * when mtime is set explicitely that the two diverge. Hence. the logic
 * here is that if SyncTime is 0, use MTime. And reset SyncTime to
 * zero when MTime is set to now (thus logically setting them both).
 */

//------------------------------------------------------------------------------
// Get sync time, no lock
//------------------------------------------------------------------------------
void
QuarkFileMD::getSyncTimeNoLock(ctime_t& stime) const
{
  (void) memcpy(&stime, mFile.stime().data(), sizeof(stime));

  if (stime.tv_sec == 0) {  /* fall back to mtime if default */
    (void) memcpy(&stime, mFile.mtime().data(), sizeof(stime));
  }
}

//------------------------------------------------------------------------------
// Get sync time
//------------------------------------------------------------------------------
void
QuarkFileMD::getSyncTime(ctime_t& stime) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  getSyncTimeNoLock(stime);
}

//------------------------------------------------------------------------------
// Set sync time
//------------------------------------------------------------------------------
void
QuarkFileMD::setSyncTime(ctime_t stime)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mFile.set_stime(&stime, sizeof(stime));
}

//------------------------------------------------------------------------------
// Set sync time to now
//------------------------------------------------------------------------------
void
QuarkFileMD::setSyncTimeNow()
{
  struct timespec tnow;
  clock_gettime(CLOCK_REALTIME, &tnow);
  setSyncTime(tnow);
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
QuarkFileMD::getAttributes() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  std::map<std::string, std::string> xattrs;

  for (const auto& elem : mFile.xattrs()) {
    xattrs.insert(elem);
  }

  return xattrs;
}

//------------------------------------------------------------------------------
// Test the unlinked location
//------------------------------------------------------------------------------
bool QuarkFileMD::hasUnlinkedLocation(IFileMD::location_t location)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return hasUnlinkedLocationNoLock(location);
}

//------------------------------------------------------------------------------
// Test the unlinked location, no locks
//------------------------------------------------------------------------------
bool QuarkFileMD::hasUnlinkedLocationNoLock(location_t location) const
{
  for (int i = 0; i < mFile.unlink_locations_size(); ++i) {
    if (mFile.unlink_locations()[i] == location) {
      return true;
    }
  }

  return false;
}


EOSNSNAMESPACE_END
