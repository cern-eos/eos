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
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMD::FileMD(id_t id, IFileMDSvc* fileMDSvc):
  pFileMDSvc(fileMDSvc), mAh()
{
  mFile.set_id(id);
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
FileMD*
FileMD::clone() const
{
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
  mFile = other.mFile;
  pFileMDSvc   = 0;
  return *this;
}

//------------------------------------------------------------------------------
// Add location
//------------------------------------------------------------------------------
void
FileMD::addLocation(location_t location)
{
  if (hasLocation(location)) {
    return;
  }

  mFile.add_locations(location);
  IFileMDChangeListener::Event e(this, IFileMDChangeListener::LocationAdded,
                                 location);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Replace location by index
//------------------------------------------------------------------------------
void
FileMD::replaceLocation(unsigned int index, location_t newlocation)
{
  location_t oldLocation = mFile.locations(index);
  mFile.set_locations(index, newlocation);
  IFileMDChangeListener::Event e(this, IFileMDChangeListener::LocationReplaced,
                                 newlocation, oldLocation);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Remove location that was previously unlinked
//------------------------------------------------------------------------------
void
FileMD::removeLocation(location_t location)
{
  for (auto it = mFile.unlink_locations().begin();
       it != mFile.unlink_locations().end(); ++it) {
    if (*it == location) {
      mFile.mutable_unlink_locations()->erase(it);
      IFileMDChangeListener::Event e(this, IFileMDChangeListener::LocationRemoved,
                                     location);
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
  auto it = mFile.unlink_locations().rbegin();

  while (it != mFile.unlink_locations().rend()) {
    IFileMDChangeListener::Event e(this, IFileMDChangeListener::LocationRemoved,
                                   *it);
    pFileMDSvc->notifyListeners(&e);
    ++it;
  }

  mFile.clear_locations();
}

//------------------------------------------------------------------------------
// Unlink location
//------------------------------------------------------------------------------
void
FileMD::unlinkLocation(location_t location)
{
  for (auto it = mFile.locations().begin();
       it != mFile.locations().end(); ++it) {
    if (*it == location) {
      mFile.add_unlink_locations(*it);
      mFile.mutable_locations()->erase(it);
      IFileMDChangeListener::Event e(
        this, IFileMDChangeListener::LocationUnlinked, location);
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
  auto it = mFile.locations().rbegin();

  while (it != mFile.locations().rend()) {
    mFile.add_unlink_locations(*it);
    IFileMDChangeListener::Event e(
      this, IFileMDChangeListener::LocationUnlinked, *it);
    pFileMDSvc->notifyListeners(&e);
    ++it;
  }

  mFile.clear_locations();
}

//------------------------------------------------------------------------
//  Env Representation
//------------------------------------------------------------------------
void
FileMD::getEnv(std::string& env, bool escapeAnd)
{
  env = "";
  std::ostringstream o;
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
  (void) getCTime(ctime);
  (void) getMTime(mtime);
  o << "name=" << saveName << "&id=" << mFile.id()
    << "&ctime=" << ctime.tv_sec << "&ctime_ns=" << ctime.tv_nsec
    << "&mtime=" << mtime.tv_sec << "&mtime_ns=" << mtime.tv_nsec
    << "&size=" << mFile.size() << "&cid=" << mFile.cont_id()
    << "&uid=" << mFile.uid() << "&gid=" << mFile.gid()
    << "&lid=" << mFile.layout_id();
  env += o.str();
  env += "&location=";
  char locs[16];

  for (auto && elem : mFile.locations()) {
    snprintf(static_cast<char*>(locs), sizeof(locs), "%u", elem);
    env += static_cast<char*>(locs);
    env += ",";
  }

  for (auto && elem : mFile.unlink_locations()) {
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
  if (pFileMDSvc == nullptr) {
    MDException ex(ENOTSUP);
    ex.getMessage() << "This was supposed to be a read only copy!";
    throw ex;
  }

  // TODO(esindril): add checksum for the FileMD serialized object
  size_t msg_size = mFile.ByteSizeLong();
  buffer.setSize(msg_size);
  google::protobuf::io::ArrayOutputStream aos(buffer.getDataPtr(), msg_size);

  if (!mFile.SerializeToZeroCopyStream(&aos)) {
    MDException ex(EIO);
    ex.getMessage() << "Failed while serializing buffer";
    throw ex;
  }

  if (!waitAsyncReplies()) {
    MDException ex(EIO);
    ex.getMessage() << "KV-store asynchronous reuqests failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// Deserialize the class to a buffer
//------------------------------------------------------------------------------
void
FileMD::deserialize(const eos::Buffer& buffer)
{
  google::protobuf::io::ArrayInputStream ais(buffer.getDataPtr(),
      buffer.getSize());

  if (!mFile.ParseFromZeroCopyStream(&ais)) {
    MDException ex(EIO);
    ex.getMessage() << "Failed while deserializing buffer";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// Set size - 48 bytes will be used
//------------------------------------------------------------------------------
void
FileMD::setSize(uint64_t size)
{
  int64_t sizeChange = (size & 0x0000ffffffffffff) - mFile.size();
  mFile.set_size(size & 0x0000ffffffffffff);
  IFileMDChangeListener::Event e(this, IFileMDChangeListener::SizeChange, 0, 0,
                                 sizeChange);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void
FileMD::getCTime(ctime_t& ctime) const
{
  (void) memcpy(&ctime, mFile.ctime().data(), sizeof(ctime_t));
}

//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void
FileMD::setCTime(ctime_t ctime)
{
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
  mFile.set_ctime(&tnow, sizeof(tnow));
}

//------------------------------------------------------------------------------
// Get modification time
//------------------------------------------------------------------------------
void
FileMD::getMTime(ctime_t& mtime) const
{
  (void) memcpy(&mtime, mFile.mtime().data(), sizeof(time_t));
}

//------------------------------------------------------------------------------
// Set modification time
//------------------------------------------------------------------------------
void
FileMD::setMTime(ctime_t mtime)
{
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
  mFile.set_mtime(&tnow, sizeof(tnow));
}

//------------------------------------------------------------------------------
// Wait for replies to asynchronous requests
//------------------------------------------------------------------------------
bool
FileMD::waitAsyncReplies()
{
  bool ret = mAh.Wait();

  if (!ret) {
    std::ostringstream oss;
    auto resp = mAh.GetResponses();

    for (auto && elem : resp) {
      oss << elem << " ";
    }

    oss << std::endl;
    fprintf(stderr, "Async responses: %s\n", oss.str().c_str());
  }

  return ret;
}

//------------------------------------------------------------------------------
// Register asynchronous request
//------------------------------------------------------------------------------
void
FileMD::Register(qclient::AsyncResponseType aresp, qclient::QClient* qcl)
{
  mAh.Register(std::move(aresp), qcl);
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
FileMD::getAttributes() const
{
  std::map<std::string, std::string> xattrs;

  for (const auto& elem : mFile.xattrs()) {
    xattrs.insert(elem);
  }

  return xattrs;
}

EOSNSNAMESPACE_END
