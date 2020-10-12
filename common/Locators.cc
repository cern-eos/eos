//------------------------------------------------------------------------------
// File: Locators.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/Namespace.hh"
#include "common/Locators.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "common/InstanceName.hh"
#include "common/Assert.hh"
#include "common/FileSystem.hh"
#include "common/ParseUtils.hh"
#include "common/StringUtils.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemLocator::FileSystemLocator(const std::string& _host, int _port,
                                     const std::string& _storagepath) : host(_host), port(_port),
  storagepath(_storagepath)
{
  storageType = FileSystemLocator::parseStorageType(_storagepath);
}

//------------------------------------------------------------------------------
// Try to parse a "queuepath"
//------------------------------------------------------------------------------
bool FileSystemLocator::fromQueuePath(const std::string& queuepath,
                                      FileSystemLocator& out)
{
  std::string queue = queuepath;

  if (!startsWith(queue, "/eos/")) {
    return false;
  }

  queue.erase(0, 5);
  //----------------------------------------------------------------------------
  // Chop /eos/, extract host+port
  //----------------------------------------------------------------------------
  size_t slashLocation = queue.find("/");

  if (slashLocation == std::string::npos) {
    return false;
  }

  std::string hostPort = std::string(queue.begin(),
                                     queue.begin() + slashLocation);
  queue.erase(0, slashLocation);
  //----------------------------------------------------------------------------
  // Separate host from port
  //----------------------------------------------------------------------------
  size_t separator = hostPort.find(":");

  if (separator == std::string::npos) {
    return false;
  }

  out.host = std::string(hostPort.begin(), hostPort.begin() + separator);
  hostPort.erase(0, separator + 1);
  int64_t port;

  if (!ParseInt64(hostPort, port)) {
    return false;
  }

  out.port = port;

  //----------------------------------------------------------------------------
  // Chop "/fst/", extract local path
  //----------------------------------------------------------------------------
  if (!startsWith(queue, "/fst")) {
    return false;
  }

  queue.erase(0, 4);
  out.storagepath = queue;

  if (out.storagepath.size() < 2) {
    // Empty, or "/"? Reject
    return false;
  }

  out.storageType = FileSystemLocator::parseStorageType(out.storagepath);

  if (out.storageType == StorageType::Unknown) {
    return false;
  }

  return true;
}

//----------------------------------------------------------------------------
//! Parse storage type from storage path string
//----------------------------------------------------------------------------
FileSystemLocator::StorageType FileSystemLocator::parseStorageType(
  const std::string& storagepath)
{
  if (storagepath.find("/") == 0) {
    return StorageType::Local;
  } else if (storagepath.find("root://") == 0) {
    return StorageType ::Xrd;
  } else if (storagepath.find("s3://") == 0) {
    return StorageType::S3;
  } else if (storagepath.find("dav://") == 0) {
    return StorageType::WebDav;
  } else if (storagepath.find("http://") == 0) {
    return StorageType::HTTP;
  } else if (storagepath.find("https://") == 0) {
    return StorageType::HTTPS;
  } else {
    return StorageType::Unknown;
  }
}

//------------------------------------------------------------------------------
// Get host
//------------------------------------------------------------------------------
std::string FileSystemLocator::getHost() const
{
  return host;
}

//------------------------------------------------------------------------------
// Get hostport, concatenated together as "host:port"
//------------------------------------------------------------------------------
std::string FileSystemLocator::getHostPort() const
{
  return SSTR(host << ":" << port);
}

//------------------------------------------------------------------------------
// Get queuepath
//------------------------------------------------------------------------------
std::string FileSystemLocator::getQueuePath() const
{
  return SSTR("/eos/" << host << ":" << port << "/fst" << storagepath);
}

//------------------------------------------------------------------------------
// Get "FST queue", ie /eos/example.com:3002/fst
//------------------------------------------------------------------------------
std::string FileSystemLocator::getFSTQueue() const
{
  return SSTR("/eos/" << host << ":" << port << "/fst");
}

//------------------------------------------------------------------------------
// Get port
//------------------------------------------------------------------------------
int FileSystemLocator::getPort() const
{
  return port;
}

//------------------------------------------------------------------------------
// Get storage path
//------------------------------------------------------------------------------
std::string FileSystemLocator::getStoragePath() const
{
  return storagepath;
}

//----------------------------------------------------------------------------
// Get storage type
//----------------------------------------------------------------------------
FileSystemLocator::StorageType FileSystemLocator::getStorageType() const
{
  return storageType;
}

//----------------------------------------------------------------------------
// Check whether filesystem is local or remote
//----------------------------------------------------------------------------
bool FileSystemLocator::isLocal() const
{
  return storageType == StorageType::Local;
}

//------------------------------------------------------------------------------
// Empty constructor
//------------------------------------------------------------------------------
SharedHashLocator::SharedHashLocator()
{
  mInitialized = false;
}

//------------------------------------------------------------------------------
// Constructor: Pass the EOS instance name, BaseView type, and name.
//
// Once we drop the MQ entirely, the instance name can be removed.
//------------------------------------------------------------------------------
SharedHashLocator::SharedHashLocator(const std::string& instanceName, Type type,
                                     const std::string& name)
  : mInitialized(true), mInstanceName(instanceName), mType(type), mName(name)
{
  switch (mType) {
  case Type::kSpace: {
    mMqSharedHashPath = SSTR("/config/" << instanceName << "/space/" << name);
    mBroadcastQueue = "/eos/*/mgm";
    break;
  }

  case Type::kGroup: {
    mMqSharedHashPath = SSTR("/config/" << instanceName << "/group/" << name);
    mBroadcastQueue = "/eos/*/mgm";
    break;
  }

  case Type::kNode: {
    std::string hostPort = eos::common::StringConversion::GetHostPortFromQueue(
                             name.c_str()).c_str();
    mMqSharedHashPath = SSTR("/config/" << instanceName << "/node/" << hostPort);
    mBroadcastQueue = SSTR("/eos/" << hostPort << "/fst");
    break;
  }

  case Type::kGlobalConfigHash: {
    mMqSharedHashPath = SSTR("/config/" << instanceName << "/mgm/");
    mBroadcastQueue = "/eos/*/mgm";
    break;
  }

  default: {
    eos_assert("should never reach here");
  }
  }
}

//------------------------------------------------------------------------------
//! Constructor: Same as above, but auto-discover instance name.
//------------------------------------------------------------------------------
SharedHashLocator::SharedHashLocator(Type type, const std::string& name)
  : SharedHashLocator(InstanceName::get(), type, name) {}

//------------------------------------------------------------------------------
// Constructor: Special case for FileSystems, as they work a bit differently
// than the rest.
//------------------------------------------------------------------------------
SharedHashLocator::SharedHashLocator(const FileSystemLocator& fsLocator,
                                     bool bc2mgm):
  mInitialized(true), mType(Type::kFilesystem)
{
  mMqSharedHashPath = fsLocator.getQueuePath();
  mBroadcastQueue = fsLocator.getFSTQueue();

  if (bc2mgm) {
    mBroadcastQueue = "/eos/*/mgm";
  }
}

//------------------------------------------------------------------------------
//! Convenience "Constructors": Make locator for space, group, node
//------------------------------------------------------------------------------
SharedHashLocator SharedHashLocator::makeForSpace(const std::string& name)
{
  return SharedHashLocator(Type::kSpace, name);
}

SharedHashLocator SharedHashLocator::makeForGroup(const std::string& name)
{
  return SharedHashLocator(Type::kGroup, name);
}

SharedHashLocator SharedHashLocator::makeForNode(const std::string& name)
{
  return SharedHashLocator(Type::kNode, name);
}

SharedHashLocator SharedHashLocator::makeForGlobalHash()
{
  return SharedHashLocator(Type::kGlobalConfigHash, "");
}

//------------------------------------------------------------------------------
// Get "config queue" for shared hash
//------------------------------------------------------------------------------
std::string SharedHashLocator::getConfigQueue() const
{
  return mMqSharedHashPath;
}

//------------------------------------------------------------------------------
// Get "broadcast queue" for shared hash
//------------------------------------------------------------------------------
std::string SharedHashLocator::getBroadcastQueue() const
{
  return mBroadcastQueue;
}

//----------------------------------------------------------------------------
// Check if this object is actually pointing to something
//----------------------------------------------------------------------------
bool SharedHashLocator::empty() const
{
  return !mInitialized;
}

//------------------------------------------------------------------------------
// Produce SharedHashLocator by parsing config queue
//------------------------------------------------------------------------------
bool SharedHashLocator::fromConfigQueue(const std::string& configQueue,
                                        SharedHashLocator& out)
{
  std::vector<std::string> parts =
    common::StringTokenizer::split<std::vector<std::string>>(configQueue, '/');
  std::reverse(parts.begin(), parts.end());

  if (parts.empty() || parts.back() != "config") {
    return false;
  }

  parts.pop_back();

  if (parts.empty()) {
    return false;
  }

  SharedHashLocator::Type type;
  std::string instanceName = parts.back();
  parts.pop_back();

  if (parts.empty()) {
    return false;
  } else if (parts.back() == "node") {
    type = Type::kNode;
  } else if (parts.back() == "space") {
    type = Type::kSpace;
  } else if (parts.back() == "group") {
    type = Type::kGroup;
  } else if (parts.back() == "mgm" && parts.size() == 1u) {
    type = Type::kGlobalConfigHash;
    out = SharedHashLocator(instanceName, type, "");
    return true;
  } else {
    return false;
  }

  parts.pop_back();

  if (parts.empty()) {
    return false;
  }

  std::string name = parts.back();
  parts.pop_back();

  if (!parts.empty()) {
    return false;
  }

  if (instanceName.empty() || name.empty()) {
    return false;
  }

  out = SharedHashLocator(instanceName, type, name);
  return true;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TransferQueueLocator::TransferQueueLocator(const FileSystemLocator &fsLocator, const std::string &tag)
: mLocator(fsLocator), mTag(tag) {}

//------------------------------------------------------------------------------
// Constructor: Queue tied to an FST
//------------------------------------------------------------------------------
TransferQueueLocator::TransferQueueLocator(const std::string &fstQueue, const std::string &tag)
: mFstQueue(fstQueue), mTag(tag) {}

//------------------------------------------------------------------------------
// Get "queue"
//------------------------------------------------------------------------------
std::string TransferQueueLocator::getQueue() const {
  if(!mFstQueue.empty()) {
    return mFstQueue;
  }
  else {
    return mLocator.getFSTQueue();
  }
}

//------------------------------------------------------------------------------
// Get "queuepath"
//------------------------------------------------------------------------------
std::string TransferQueueLocator::getQueuePath() const {
  if(!mFstQueue.empty()) {
    return SSTR(mFstQueue << "/gw/txqueue/" << mTag);
  }
  else {
    return SSTR(mLocator.getQueuePath() << "/txqueue/" << mTag);
  }
}

//------------------------------------------------------------------------------
// Get QDB key for this queue
//------------------------------------------------------------------------------
std::string TransferQueueLocator::getQDBKey() const {
  if(!mFstQueue.empty()) {
    std::vector<std::string> parts;
    parts = eos::common::StringTokenizer::split<std::vector<std::string>>(mFstQueue, '/');
    return SSTR("txqueue-fst||" << parts[1] << "||" << mTag);
  }
  else {
    return SSTR("txqueue-filesystem||" << mLocator.getHostPort() << "||" << mLocator.getStoragePath() << "||" << mTag);
  }
}

EOSCOMMONNAMESPACE_END
