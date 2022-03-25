//------------------------------------------------------------------------------
// File: FileSystem.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "common/Namespace.hh"
#include "common/FileSystem.hh"
#include "common/Logging.hh"
#include "common/TransferQueue.hh"
#include "common/StringUtils.hh"
#include "common/ParseUtils.hh"
#include "common/Assert.hh"
#include "common/Constants.hh"
#include "common/ParseUtils.hh"
#include "common/StringConversion.hh"
#include "mq/MessagingRealm.hh"
#include <list>

EOSCOMMONNAMESPACE_BEGIN;

//------------------------------------------------------------------------------
// Contains a batch of key-value updates on the attributes of a filesystem.
//------------------------------------------------------------------------------
FileSystemUpdateBatch::FileSystemUpdateBatch() {}

//------------------------------------------------------------------------------
// Set filesystem ID - durable.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setId(fsid_t fsid)
{
  setLongLongDurable("id", fsid);
}

//----------------------------------------------------------------------------
// Set the draining status - local.
//----------------------------------------------------------------------------
void FileSystemUpdateBatch::setDrainStatusLocal(DrainStatus status)
{
  setStringLocal("local.drain", FileSystem::GetDrainStatusAsString(status));
}

//------------------------------------------------------------------------------
// Set durable string.
//
// All observers of this filesystem are guaranteed to receive the update
// eventually, and all updates are guaranteed to be applied in the same
// order for all observers.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setStringDurable(const std::string& key,
    const std::string& value)
{
  mBatch.SetDurable(key, value);
}

//------------------------------------------------------------------------------
// Set transient string. Depending on network instabilities,
// process restarts, or the phase of the moon, some or all observers of this
// filesystem may or may not receive the update.
//
// Transient updates may be applied out of order, and if multiple subscribers
// try to modify the same value, it's possible that observers will not all
// converge on a single consistent value.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setStringTransient(const std::string& key,
    const std::string& value)
{
  mBatch.SetTransient(key, value);
}

//------------------------------------------------------------------------------
// Set local string. This node, and only this node will store the value,
// and only until process restart.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setStringLocal(const std::string& key,
    const std::string& value)
{
  mBatch.SetLocal(key, value);
}

//------------------------------------------------------------------------------
// Set durable int64_t - serialize as string automatically.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setLongLongDurable(const std::string& key,
    int64_t value)
{
  return setStringDurable(key, std::to_string(value));
}

//------------------------------------------------------------------------------
// Set transient int64_t - serialize as string automatically.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setLongLongTransient(const std::string& key,
    int64_t value)
{
  return setStringTransient(key, std::to_string(value));
}

//------------------------------------------------------------------------------
// Set local int64_t - serialize as string automatically.
//------------------------------------------------------------------------------
void FileSystemUpdateBatch::setLongLongLocal(const std::string& key,
    int64_t value)
{
  return setStringLocal(key, std::to_string(value));
}

//------------------------------------------------------------------------------
// Get durable updates map
//------------------------------------------------------------------------------
const mq::SharedHashWrapper::Batch&
FileSystemUpdateBatch::getBatch() const
{
  return mBatch;
}

//------------------------------------------------------------------------------
// Empty constructor
//------------------------------------------------------------------------------
FstLocator::FstLocator() {}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FstLocator::FstLocator(const std::string& host, int port)
  : mHost(host), mPort(port) {}

//------------------------------------------------------------------------------
// Get host
//------------------------------------------------------------------------------
std::string FstLocator::getHost() const
{
  return mHost;
}

//------------------------------------------------------------------------------
// Get port
//------------------------------------------------------------------------------
int FstLocator::getPort() const
{
  return mPort;
}

//------------------------------------------------------------------------------
// Get fst queuepath
//------------------------------------------------------------------------------
std::string FstLocator::getQueuePath() const
{
  return SSTR("/eos/" << mHost << ":" << mPort << "/fst");
}

//------------------------------------------------------------------------------
// Get host:port
//------------------------------------------------------------------------------
std::string FstLocator::getHostPort() const
{
  return SSTR(mHost << ":" << mPort);
}

//------------------------------------------------------------------------------
// Try to parse from queuepath
//------------------------------------------------------------------------------
bool FstLocator::fromQueuePath(const std::string& queuepath, FstLocator& out)
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

  out.mHost = std::string(hostPort.begin(), hostPort.begin() + separator);
  hostPort.erase(0, separator + 1);
  int64_t port;

  if (!ParseInt64(hostPort, port)) {
    return false;
  }

  out.mPort = port;

  //----------------------------------------------------------------------------
  // Chop "/fst/"
  //----------------------------------------------------------------------------
  if (queue != "/fst") {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
GroupLocator::GroupLocator() {}

//------------------------------------------------------------------------------
// Get group (space.index)
//------------------------------------------------------------------------------
std::string GroupLocator::getGroup() const
{
  return mGroup;
}

//------------------------------------------------------------------------------
// Get space
//------------------------------------------------------------------------------
std::string GroupLocator::getSpace() const
{
  return mSpace;
}

//------------------------------------------------------------------------------
// Get index
//------------------------------------------------------------------------------
int GroupLocator::getIndex() const
{
  return mIndex;
}

//------------------------------------------------------------------------------
// Parse full group (space.index)
//
// NOTE: In case parsing fails, out will still be filled
// with "description.0" to match legacy behaviour.
//------------------------------------------------------------------------------
bool GroupLocator::parseGroup(const std::string& description,
                              GroupLocator& out)
{
  size_t dot = description.find(".");

  if (dot == std::string::npos) {
    out.mGroup = description;
    out.mSpace = description;
    out.mIndex = 0;

    if (description != eos::common::EOS_SPARE_GROUP) {
      eos_static_notice("Unable to parse group: %s, assuming index is zero",
                        description.c_str());
      return false;
    }

    return true;
  }

  out.mGroup = description;
  out.mSpace = std::string(description.c_str(), dot);
  std::string index = std::string(description.begin() + dot + 1,
                                  description.end());
  int64_t idx;

  if (!ParseInt64(index, idx)) {
    eos_static_crit("Could not parse integer index in group: %s",
                    description.c_str());
    out.mIndex = 0;
    return false;
  }

  out.mIndex = idx;
  return true;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemCoreParams::FileSystemCoreParams(uint32_t id,
    const FileSystemLocator& fsLocator, const GroupLocator& grpLocator,
    const std::string& uuid, ConfigStatus cfg)
  : mFsId(id), mLocator(fsLocator), mGroup(grpLocator), mUuid(uuid),
    mConfigStatus(cfg) {}

//------------------------------------------------------------------------------
// Get locator
//------------------------------------------------------------------------------
const FileSystemLocator& FileSystemCoreParams::getLocator() const
{
  return mLocator;
}

//------------------------------------------------------------------------------
//! Get group locator
//------------------------------------------------------------------------------
const GroupLocator& FileSystemCoreParams::getGroupLocator() const
{
  return mGroup;
}

//------------------------------------------------------------------------------
//! Get id
//------------------------------------------------------------------------------
uint32_t FileSystemCoreParams::getId() const
{
  return mFsId;
}

//------------------------------------------------------------------------------
//! Get uuid
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getUuid() const
{
  return mUuid;
}

//------------------------------------------------------------------------------
// Get current ConfigStatus
//------------------------------------------------------------------------------
ConfigStatus FileSystemCoreParams::getConfigStatus() const
{
  return mConfigStatus;
}

//------------------------------------------------------------------------------
// Get queuepath
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getQueuePath() const
{
  return mLocator.getQueuePath();
}

//------------------------------------------------------------------------------
// Get host
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getHost() const
{
  return mLocator.getHost();
}

//------------------------------------------------------------------------------
// Get hostport
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getHostPort() const
{
  return mLocator.getHostPort();
}

//------------------------------------------------------------------------------
// Get FST queue
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getFSTQueue() const
{
  return mLocator.getFSTQueue();
}

//------------------------------------------------------------------------------
// Get group (space.index)
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getGroup() const
{
  return mGroup.getGroup();
}

//------------------------------------------------------------------------------
// Get space
//------------------------------------------------------------------------------
std::string FileSystemCoreParams::getSpace() const
{
  return mGroup.getSpace();
}

//------------------------------------------------------------------------------
//! Default constructor for fs_snapshot_t
//------------------------------------------------------------------------------
FileSystem::fs_snapshot_t::fs_snapshot_t()
{
  mId = 0;
  mQueue = "";
  mQueuePath = "";
  mGroup = "";
  mPath = "";
  mUuid = "";
  mHost = "";
  mHostPort = "";
  mProxyGroup = "";
  mS3Credentials = "";
  mFileStickyProxyDepth = -1;
  mPort = 0;
  mErrMsg = "";
  mGeoTag = "";
  mPublishTimestamp = 0;
  mStatus = BootStatus::kDown;
  mConfigStatus = ConfigStatus::kOff;
  mDrainStatus = DrainStatus::kNoDrain;
  mHeadRoom = 0;
  mErrCode = 0;
  mBootSentTime = 0;
  mBootDoneTime = 0;
  mDiskUtilization = 0;
  mNetEthRateMiB = 0;
  mNetInRateMiB = 0;
  mNetOutRateMiB = 0;
  mDiskWriteRateMb = 0;
  mDiskReadRateMb = 0;
  mDiskType = 0;
  mDiskBsize = 0;
  mDiskBlocks = 0;
  mDiskBfree = 0;
  mDiskBused = 0;
  mDiskBavail = 0;
  mDiskFiles = 0;
  mDiskFfree = 0;
  mDiskFused = 0;
  mFiles = 0;
  mDiskNameLen = 0;
  mDiskRopen = 0;
  mDiskWopen = 0;
  mMaxDiskRopen = 0;
  mMaxDiskWopen = 0;
  mScanIoRate = 0;
  mScanEntryInterval = 0;
  mScanDiskInterval = 0;
  mScanNsInterval = 0;
  mScanNsRate = 0;
  mFsckRefreshInterval = 0;
  mBalThresh = 0.0;
}

//------------------------------------------------------------------------------
// "Absorb" all information contained within coreParams into this object.
// Fields which are not present in coreParams (ie mNetInRateMiB) remain
// unchanged.
//------------------------------------------------------------------------------
void FileSystem::fs_snapshot_t::fillFromCoreParams(const FileSystemCoreParams&
    coreParams)
{
  mId = coreParams.getId();
  mQueue = coreParams.getFSTQueue();
  mQueuePath = coreParams.getQueuePath();
  mGroup = coreParams.getGroup();
  mPath = coreParams.getLocator().getStoragePath();
  mUuid = coreParams.getUuid();
  mHost = coreParams.getHost();
  mHostPort = coreParams.getHostPort();
  mPort = coreParams.getLocator().getPort();
  mConfigStatus = coreParams.getConfigStatus();
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystem::FileSystem(const FileSystemLocator& locator,
                       mq::MessagingRealm* realm, bool bc2mgm)
  : mLocator(locator), mHashLocator(locator, bc2mgm)
{
  mRealm = realm;
  mInternalBootStatus = BootStatus::kDown;
  cActive = ActiveStatus::kOffline;
  cStatus = BootStatus::kDown;
  cConfigStatus = ConfigStatus::kOff;
  cActiveTime = 0;
  cStatusTime = 0;
  cConfigTime = 0;
  std::string broadcast = mHashLocator.getBroadcastQueue();

  if (realm->getSom()) {
    mq::SharedHashWrapper::Batch updateBatch;
    updateBatch.SetDurable("queue", mLocator.getFSTQueue());
    updateBatch.SetDurable("queuepath", mLocator.getQueuePath());
    updateBatch.SetDurable("path", mLocator.getStoragePath());
    updateBatch.SetDurable("hostport", locator.getHostPort());
    updateBatch.SetDurable("host", locator.getHost());
    updateBatch.SetDurable("port", std::to_string(locator.getPort()));
    updateBatch.SetLocal("local.drain", "nodrain");

    if (!bc2mgm) {
      updateBatch.SetDurable("configstatus", "down");
    }

    mq::SharedHashWrapper(mRealm, mHashLocator).set(updateBatch);
    mBalanceQueue = new TransferQueue(TransferQueueLocator(mLocator, "balanceq"),
                                      realm, bc2mgm);
    mExternQueue = new TransferQueue(TransferQueueLocator(mLocator, "externq"),
                                     realm, bc2mgm);
  } else {
    mBalanceQueue = 0;
    mExternQueue = 0;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileSystem::~FileSystem()
{
  if (mBalanceQueue) {
    delete mBalanceQueue;
  }

  if (mExternQueue) {
    delete mExternQueue;
  }
}

//------------------------------------------------------------------------------
// Delete shared hash object corresponding to this file system and also
// broadcast the message. This should be called only when an explicit removal
// of the file system is request though "fs rm".
//------------------------------------------------------------------------------
void FileSystem::DeleteSharedHash()
{
  mq::SharedHashWrapper::deleteHash(mRealm, mHashLocator);
}

//------------------------------------------------------------------------------
// Get underlying hash locator
//------------------------------------------------------------------------------
SharedHashLocator FileSystem::getHashLocator() const
{
  return mHashLocator;
}

//------------------------------------------------------------------------------
// Return the given status as a string
//------------------------------------------------------------------------------
const char*
FileSystem::GetStatusAsString(BootStatus status)
{
  if (status == BootStatus::kDown) {
    return "down";
  }

  if (status == BootStatus::kOpsError) {
    return "opserror";
  }

  if (status == BootStatus::kBootFailure) {
    return "bootfailure";
  }

  if (status == BootStatus::kBootSent) {
    return "bootsent";
  }

  if (status == BootStatus::kBooting) {
    return "booting";
  }

  if (status == BootStatus::kBooted) {
    return "booted";
  }

  return "unknown";
}

//------------------------------------------------------------------------------
// Return given drain status as a string
//------------------------------------------------------------------------------
const char*
FileSystem::GetDrainStatusAsString(DrainStatus status)
{
  if (status == DrainStatus::kNoDrain) {
    return "nodrain";
  }

  if (status == DrainStatus::kDrainPrepare) {
    return "prepare";
  }

  if (status == DrainStatus::kDrainWait) {
    return "waiting";
  }

  if (status == DrainStatus::kDraining) {
    return "draining";
  }

  if (status == DrainStatus::kDrained) {
    return "drained";
  }

  if (status == DrainStatus::kDrainStalling) {
    return "stalling";
  }

  if (status == DrainStatus::kDrainExpired) {
    return "expired";
  }

  if (status == DrainStatus::kDrainFailed) {
    return "failed";
  }

  return "unknown";
}

//------------------------------------------------------------------------------
// Return given configuration status as a string
//------------------------------------------------------------------------------
const char*
FileSystem::GetConfigStatusAsString(ConfigStatus status)
{
  switch (status) {
  case ConfigStatus::kUnknown: {
    return "unknown";
  }

  case ConfigStatus::kOff: {
    return "off";
  }

  case ConfigStatus::kEmpty: {
    return "empty";
  }

  case ConfigStatus::kDrainDead: {
    return "draindead";
  }

  case ConfigStatus::kGroupDrain: {
    return "groupdrain";
  }

  case ConfigStatus::kDrain: {
    return "drain";
  }

  case ConfigStatus::kRO: {
    return "ro";
  }

  case ConfigStatus::kWO: {
    return "wo";
  }

  case ConfigStatus::kRW: {
    return "rw";
  }

  default: {
    return "unknown";
  }
  }
}

//------------------------------------------------------------------------------
// Get the status from a string representation
//------------------------------------------------------------------------------
BootStatus
FileSystem::GetStatusFromString(const char* ss)
{
  if (!ss) {
    return BootStatus::kDown;
  }

  if (!strcmp(ss, "down")) {
    return BootStatus::kDown;
  }

  if (!strcmp(ss, "opserror")) {
    return BootStatus::kOpsError;
  }

  if (!strcmp(ss, "bootfailure")) {
    return BootStatus::kBootFailure;
  }

  if (!strcmp(ss, "bootsent")) {
    return BootStatus::kBootSent;
  }

  if (!strcmp(ss, "booting")) {
    return BootStatus::kBooting;
  }

  if (!strcmp(ss, "booted")) {
    return BootStatus::kBooted;
  }

  return BootStatus::kDown;
}


//------------------------------------------------------------------------------
// Return configuration status from a string representation
//------------------------------------------------------------------------------
ConfigStatus
FileSystem::GetConfigStatusFromString(const char* ss)
{
  if (!ss) {
    return ConfigStatus::kOff;
  }

  if (!strcmp(ss, "unknown")) {
    return ConfigStatus::kUnknown;
  }

  if (!strcmp(ss, "off")) {
    return ConfigStatus::kOff;
  }

  if (!strcmp(ss, "empty")) {
    return ConfigStatus::kEmpty;
  }

  if (!strcmp(ss, "draindead")) {
    return ConfigStatus::kDrainDead;
  }

  if (!strcmp(ss, "drain")) {
    return ConfigStatus::kDrain;
  }

  if (!strcmp(ss, "ro")) {
    return ConfigStatus::kRO;
  }

  if (!strcmp(ss, "wo")) {
    return ConfigStatus::kWO;
  }

  if (!strcmp(ss, "rw")) {
    return ConfigStatus::kRW;
  }

  if (!strcmp(ss, "down")) {
    return ConfigStatus::kOff;
  }

  return ConfigStatus::kUnknown;
}

//------------------------------------------------------------------------------
// Return drains status from string representation
//------------------------------------------------------------------------------
DrainStatus
FileSystem::GetDrainStatusFromString(const char* ss)
{
  if (!ss) {
    return DrainStatus::kNoDrain;
  }

  if (!strcmp(ss, "nodrain")) {
    return DrainStatus::kNoDrain;
  }

  if (!strcmp(ss, "prepare")) {
    return DrainStatus::kDrainPrepare;
  }

  if (!strcmp(ss, "wait")) {
    return DrainStatus::kDrainWait;
  }

  if (!strcmp(ss, "draining")) {
    return DrainStatus::kDraining;
  }

  if (!strcmp(ss, "stalling")) {
    return DrainStatus::kDrainStalling;
  }

  if (!strcmp(ss, "drained")) {
    return DrainStatus::kDrained;
  }

  if (!strcmp(ss, "expired")) {
    return DrainStatus::kDrainExpired;
  }

  if (!strcmp(ss, "failed")) {
    return DrainStatus::kDrainFailed;
  }

  return DrainStatus::kNoDrain;
}

//------------------------------------------------------------------------------
// Return active status from a string representation
//------------------------------------------------------------------------------
ActiveStatus
FileSystem::GetActiveStatusFromString(const char* ss)
{
  if (!ss) {
    return ActiveStatus::kOffline;
  }

  if (!strcmp(ss, "online")) {
    return ActiveStatus::kOnline;
  }

  if (!strcmp(ss, "offline")) {
    return ActiveStatus::kOffline;
  }

  if (!strcmp(ss, "overload")) {
    return ActiveStatus::kOverload;
  }

  return ActiveStatus::kOffline;
}

//------------------------------------------------------------------------------
// Return register request string
//------------------------------------------------------------------------------
const char*
FileSystem::GetRegisterRequestString()
{
  return "mgm.cmd=register";
}

//------------------------------------------------------------------------------
// Apply the given batch of updates
//------------------------------------------------------------------------------
bool FileSystem::applyBatch(const FileSystemUpdateBatch& batch)
{
  return mq::SharedHashWrapper(mRealm, mHashLocator).set(batch.getBatch());
}

//------------------------------------------------------------------------------
// Apply the given core parameters
//------------------------------------------------------------------------------
bool FileSystem::applyCoreParams(const FileSystemCoreParams& params)
{
  FileSystemUpdateBatch batch;
  batch.setStringDurable("uuid", params.getUuid());
  batch.setStringDurable("schedgroup", params.getGroupLocator().getGroup());
  batch.setStringDurable("configstatus",
                         GetConfigStatusAsString(params.getConfigStatus()));
  batch.setId(params.getId());
  return applyBatch(batch);
}

//------------------------------------------------------------------------------
// Set a local long long
//------------------------------------------------------------------------------
bool FileSystem::setLongLongLocal(const std::string& key, int64_t value)
{
  common::FileSystemUpdateBatch batch;
  batch.setLongLongLocal(key, value);
  return this->applyBatch(batch);
}

//------------------------------------------------------------------------------
// Set a key-value pair in a filesystem and evt. broadcast it.
//------------------------------------------------------------------------------
bool FileSystem::SetString(const char* key, const char* str, bool broadcast)
{
  return mq::SharedHashWrapper(mRealm, mHashLocator).set(key, str, broadcast);
}

//------------------------------------------------------------------------------
// Remove a key from a filesystem and evt. broadcast it.
//------------------------------------------------------------------------------
bool FileSystem::RemoveKey(const char* key, bool broadcast)
{
  return mq::SharedHashWrapper(mRealm, mHashLocator).del(key, broadcast);
}

//------------------------------------------------------------------------------
// Get all keys in a vector of strings.
//------------------------------------------------------------------------------
bool FileSystem::GetKeys(std::vector<std::string>& keys)
{
  return mq::SharedHashWrapper(mRealm, mHashLocator).getKeys(keys);
}

//------------------------------------------------------------------------------
// Get the string value by key
//------------------------------------------------------------------------------
std::string FileSystem::GetString(const char* key)
{
  std::string skey = key;

  if (skey == "<n>") {
    return "1";
  }

  return mq::SharedHashWrapper(mRealm, mHashLocator).get(key);
}

//------------------------------------------------------------------------------
// Get a long long value by key
//------------------------------------------------------------------------------
long long FileSystem::GetLongLong(const char* key)
{
  return ParseLongLong(GetString(key));
}

//------------------------------------------------------------------------------
// Get a double value by key
//------------------------------------------------------------------------------
double FileSystem::GetDouble(const char* key)
{
  return ParseDouble(GetString(key));
}

//--------------------------------------------------------------------------
//! Get used bytes
//--------------------------------------------------------------------------
uint64_t
FileSystem::GetUsedbytes()
{
  return GetLongLong("stat.statfs.usedbytes");
}

//--------------------------------------------------------------------------
//! Get used bytes space name
//--------------------------------------------------------------------------
std::string
FileSystem::GetSpace()
{
  return getCoreParams().getSpace();
}

//------------------------------------------------------------------------------
// Serializes hash contents as follows 'key1=val1 key2=val2 ... keyn=valn'
// but return only keys that don't start with filter_prefix.
//------------------------------------------------------------------------------
std::string
FileSystem::SerializeWithFilter(const std::map<std::string, std::string>&
                                contents,
                                std::list<std::string> filter_prefixes)
{
  using eos::common::StringConversion;
  std::string key;
  std::string val;
  std::ostringstream oss;
  bool filter_out;
  filter_prefixes.push_back("drainstatus");

  for (auto it = contents.begin(); it != contents.end(); it++) {
    key = it->first.c_str();
    filter_out = false;

    for (const auto& prefix : filter_prefixes) {
      if (prefix.length() && (key.find(prefix) == 0)) {
        filter_out = true;
        break;
      }
    }

    if (filter_out) {
      eos_static_debug("msg=\"filter out\" key=\"%s\"", key.c_str());
    } else {
      val = it->second;

      if ((val[0] == '"') && (val[val.length() - 1] == '"')) {
        std::string to_encode = val.substr(1, val.length() - 2);
        std::string encoded = StringConversion::curl_default_escaped(to_encode);

        if (!encoded.empty()) {
          val = '"';
          val += encoded;
          val += '"';
        }
      }

      oss << key << "=" << val.c_str() << " ";
    }
  }

  return oss.str();
}

//------------------------------------------------------------------------------
// Print contents onto a table: Originally implemented in XrdMqSharedHash.
//
// Format contents of the hash map to be displayed using the table object.
//
// @param table_mq_header table header
// @param talbe_md_data table data
// @param format format has to be provided as a chain separated by "|" of
//        the following tags
// "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>:tag=<tag>:condition=<key>=<val>"
// -> to print a key of the attached children
// "sep=<seperator>" -> to put a seperator
// "header=1" -> to put a header with description on top - this must be the
//               first format tag.
// "indent=<n>" -> indent the output
// The formats are:
// 's' : print as string
// 'S' : print as short string (truncated after .)
// 'l' : print as long long
// 'f' : print as double
// 'o' : print as <key>=<val>
// '-' : left align the printout
// '+' : convert numbers into k,M,G,T,P ranges
// The unit is appended to every number:
// e.g. 1500 with unit=B would end up as '1.5 kB'
// "tag=<tag>" -> use <tag> instead of the variable name to print the header
// @param filter to filter out hash content
//------------------------------------------------------------------------------
static void printOntoTable(mq::SharedHashWrapper& hash,
                           TableHeader& table_mq_header,
                           TableData& table_mq_data, std::string format, const std::string& filter)
{
  using eos::common::StringConversion;
  std::vector<std::string> formattoken;
  StringConversion::Tokenize(format, formattoken, "|");
  table_mq_data.emplace_back();

  for (unsigned int i = 0; i < formattoken.size(); ++i) {
    std::vector<std::string> tagtoken;
    std::map<std::string, std::string> formattags;
    StringConversion::Tokenize(formattoken[i], tagtoken, ":");

    for (unsigned int j = 0; j < tagtoken.size(); ++j) {
      std::vector<std::string> keyval;
      StringConversion::Tokenize(tagtoken[j], keyval, "=");

      if (keyval.size() >= 2) {
        formattags[keyval[0]] = keyval[1];
      }
    }

    if (formattags.count("format")) {
      unsigned int width = atoi(formattags["width"].c_str());
      std::string format = formattags["format"];
      std::string unit = formattags["unit"];

      // Normal member printout
      if (formattags.count("key")) {
        if (format.find("s") != std::string::npos) {
          table_mq_data.back().push_back(
            TableCell(hash.get(formattags["key"]), format));
        }

        if (format.find("S") != std::string::npos) {
          std::string shortstring = hash.get(formattags["key"].c_str());
          const size_t pos = shortstring.find(".");

          if (pos != std::string::npos) {
            shortstring.erase(pos);
          }

          table_mq_data.back().push_back(TableCell(shortstring, format));
        }

        if ((format.find("l")) != std::string::npos) {
          table_mq_data.back().push_back(
            TableCell(hash.getLongLong(formattags["key"].c_str()), format, unit));
        }

        if ((format.find("f")) != std::string::npos) {
          table_mq_data.back().push_back(
            TableCell(hash.getDouble(formattags["key"].c_str()), format, unit));
        }

        XrdOucString name = formattags["key"].c_str();

        if (format.find("o") == std::string::npos) {  //only for table output
          name.replace("stat.", "");
          name.replace("stat.statfs.", "");
          name.replace("local.", "");

          if (formattags.count("tag")) {
            name = formattags["tag"].c_str();
          }
        }

        table_mq_header.push_back(std::make_tuple(name.c_str(), width, format));
      }

      if (formattags.count("compute")) {
        if (formattags["compute"] == "usage") {
          // compute the percentage usage
          long long used_bytes = hash.getLongLong("stat.statfs.usedbytes");
          long long capacity = hash.getLongLong("stat.statfs.capacity");
          long long headroom = hash.getLongLong("headroom");
          double usage = 0;

          if (capacity) {
            usage = 100.0 * (used_bytes + headroom) / (capacity);

            if (usage > 100.0) {
              usage = 100.0;
            }
          }

          table_mq_data.back().push_back(
            TableCell(usage, format, unit));
          table_mq_header.push_back(std::make_tuple("usage", width, format));
        }
      }
    }
  }

  //we check for filters
  bool toRemove = false;

  if (filter.find("d") != string::npos) {
    std::string drain = hash.get("local.drain");

    // @note there is a bug when initializing local.drain on an fs which is not
    // propagated to the shared hash therefore, we need to also exclude the
    // the empty drain status from the list of active drainings
    if (drain.empty() || (drain == "nodrain")) {
      toRemove = true;
    }
  }

  if (filter.find("e") != string::npos) {
    int err = (int) hash.getLongLong("stat.errc");

    if (err == 0) {
      toRemove = true;
    }
  }

  if (toRemove) {
    table_mq_data.pop_back();
  }
}

//------------------------------------------------------------------------------
// Store a configuration key-val pair.
// Internally, these keys are not prefixed with 'stat.'
//------------------------------------------------------------------------------
void
FileSystem::CreateConfig(std::string& key, std::string& val)
{
  key = mLocator.getQueuePath();
  val.clear();
  std::map<std::string, std::string> contents;
  mq::SharedHashWrapper(mRealm, mHashLocator).getContents(contents);
  val = SerializeWithFilter(contents, {"stat.", "local."});
}

//------------------------------------------------------------------------------
// Retrieve FileSystem's core parameters
//------------------------------------------------------------------------------
FileSystemCoreParams FileSystem::getCoreParams()
{
  mq::SharedHashWrapper hash(mRealm, mHashLocator);
  std::string id;

  if (!hash.get("id", id) || id.empty()) {
    return FileSystemCoreParams(0, FileSystemLocator(), GroupLocator(), "",
                                ConfigStatus::kOff);
  }

  GroupLocator groupLocator;
  GroupLocator::parseGroup(hash.get("schedgroup"), groupLocator);
  std::string uuid = hash.get("uuid");
  ConfigStatus cfg = GetConfigStatusFromString(hash.get("configstatus").c_str());
  return FileSystemCoreParams(atoi(id.c_str()), mLocator, groupLocator, uuid,
                              cfg);
}

//------------------------------------------------------------------------------
// Snapshots all variables of a filesystem into a snapshot struct
//------------------------------------------------------------------------------
bool
FileSystem::SnapShotFileSystem(FileSystem::fs_snapshot_t& fs, bool dolock)
{
  mq::SharedHashWrapper hash(mRealm, mHashLocator, dolock, false);
  std::string tmp;

  if (!hash.get("id", tmp)) {
    fs = {};
    return false;
  }

  fs.mId = hash.getLongLong("id");
  fs.mQueue = mLocator.getFSTQueue();
  fs.mQueuePath = mLocator.getQueuePath();
  fs.mGroup = hash.get("schedgroup");
  fs.mUuid = hash.get("uuid");
  fs.mHost = mLocator.getHost();
  fs.mHostPort = mLocator.getHostPort();
  fs.mProxyGroup = hash.get("proxygroup");
  fs.mS3Credentials = hash.get("s3credentials");
  fs.mFileStickyProxyDepth = -1;

  if (hash.get("filestickyproxydepth").size()) {
    fs.mFileStickyProxyDepth = hash.getLongLong("filestickyproxydepth");
  }

  fs.mPort = mLocator.getPort();
  GroupLocator groupLocator;
  GroupLocator::parseGroup(fs.mGroup, groupLocator);
  fs.mSpace = groupLocator.getSpace();
  fs.mGroupIndex = groupLocator.getIndex();
  fs.mPath = mLocator.getStoragePath();
  fs.mErrMsg = hash.get("stat.errmsg");
  fs.mGeoTag = hash.get("stat.geotag");
  fs.mForceGeoTag.clear();

  if (hash.get("forcegeotag").size()) {
    std::string forceGeoTag = hash.get("forcegeotag");

    if (forceGeoTag != "<none>") {
      fs.mGeoTag = forceGeoTag;
      fs.mForceGeoTag = forceGeoTag;
    }
  }

  fs.mPublishTimestamp = (size_t) hash.getLongLong("stat.publishtimestamp");
  fs.mStatus = GetStatusFromString(hash.get("stat.boot").c_str());
  fs.mConfigStatus = GetConfigStatusFromString(hash.get("configstatus").c_str());
  fs.mDrainStatus = GetDrainStatusFromString(hash.get("local.drain").c_str());
  fs.mActiveStatus = GetActiveStatusFromString(hash.get("stat.active").c_str());
  //headroom can be configured as KMGTP so the string should be properly converted
  fs.mHeadRoom = StringConversion::GetSizeFromString(hash.get("headroom"));
  fs.mErrCode = (unsigned int) hash.getLongLong("stat.errc");
  fs.mBootSentTime = (time_t) hash.getLongLong("bootsenttime");
  fs.mBootDoneTime = (time_t) hash.getLongLong("stat.bootdonetime");
  fs.mDiskUtilization = hash.getDouble("stat.disk.load");
  fs.mNetEthRateMiB = hash.getDouble("stat.net.ethratemib");
  fs.mNetInRateMiB = hash.getDouble("stat.net.inratemib");
  fs.mNetOutRateMiB = hash.getDouble("stat.net.outratemib");
  fs.mDiskWriteRateMb = hash.getDouble("stat.disk.writeratemb");
  fs.mDiskReadRateMb = hash.getDouble("stat.disk.readratemb");
  fs.mDiskType = (long) hash.getLongLong("stat.statfs.type");
  fs.mDiskFreeBytes = hash.getLongLong("stat.statfs.freebytes");
  fs.mDiskCapacity = hash.getLongLong("stat.statfs.capacity");
  fs.mDiskBsize = (long) hash.getLongLong("stat.statfs.bsize");
  fs.mDiskBlocks = (long) hash.getLongLong("stat.statfs.blocks");
  fs.mDiskBfree = (long) hash.getLongLong("stat.statfs.bfree");
  fs.mDiskBused = (long) hash.getLongLong("stat.statfs.bused");
  fs.mDiskBavail = (long) hash.getLongLong("stat.statfs.bavail");
  fs.mDiskFiles = (long) hash.getLongLong("stat.statfs.files");
  fs.mDiskFfree = (long) hash.getLongLong("stat.statfs.ffree");
  fs.mDiskFused = (long) hash.getLongLong("stat.statfs.fused");
  fs.mDiskFilled = (double) hash.getDouble("stat.statfs.filled");
  fs.mNominalFilled = (double) hash.getDouble("stat.nominal.filled");
  fs.mFiles = (long) hash.getLongLong("stat.usedfiles");
  fs.mDiskNameLen = (long) hash.getLongLong("stat.statfs.namelen");
  fs.mDiskRopen = (long) hash.getLongLong("stat.ropen");
  fs.mDiskWopen = (long) hash.getLongLong("stat.wopen");
  fs.mMaxDiskRopen = (long) hash.getLongLong("max.ropen");
  fs.mMaxDiskWopen = (long) hash.getLongLong("max.wopen");
  fs.mScanIoRate = (long) hash.getLongLong(eos::common::SCAN_IO_RATE_NAME);
  fs.mScanEntryInterval = (long)hash.getLongLong
                          (eos::common::SCAN_ENTRY_INTERVAL_NAME);
  fs.mScanDiskInterval = (long)hash.getLongLong(
                           eos::common::SCAN_DISK_INTERVAL_NAME);
  fs.mScanNsInterval = (long)hash.getLongLong(eos::common::SCAN_NS_INTERVAL_NAME);
  fs.mScanNsRate = (long)hash.getLongLong(eos::common::SCAN_NS_RATE_NAME);
  fs.mFsckRefreshInterval = (long) hash.getLongLong(
                              eos::common::FSCK_REFRESH_INTERVAL_NAME);
  fs.mGracePeriod = (time_t) hash.getLongLong("graceperiod");
  fs.mDrainPeriod = (time_t) hash.getLongLong("drainperiod");
  fs.mBalThresh   = hash.getDouble("stat.balance.threshold");
  return true;
}

//----------------------------------------------------------------------------
// Return the configuration status (via cache)
//----------------------------------------------------------------------------
ConfigStatus
FileSystem::GetConfigStatus(bool cached)
{
  XrdSysMutexHelper lock(cConfigLock);

  if (cached) {
    time_t now = time(NULL);

    if (now - cConfigTime) {
      cConfigTime = now;
    } else {
      return cConfigStatus;
    }
  }

  cConfigStatus = GetConfigStatusFromString(GetString("configstatus").c_str());
  return cConfigStatus;
}

//----------------------------------------------------------------------------
// Return the filesystem status (via a cache)
//----------------------------------------------------------------------------
BootStatus
FileSystem::GetStatus(bool cached)
{
  XrdSysMutexHelper lock(cStatusLock);

  if (cached) {
    time_t now = time(NULL);

    if (now - cStatusTime) {
      cStatusTime = now;
    } else {
      return cStatus;
    }
  }

  cStatus = GetStatusFromString(GetString("stat.boot").c_str());
  return cStatus;
}

//----------------------------------------------------------------------------
// Function printing the file system info to the table
//----------------------------------------------------------------------------
void
FileSystem::Print(TableHeader& table_mq_header, TableData& table_mq_data,
                  std::string listformat, const std::string& filter)
{
  mq::SharedHashWrapper hash(mRealm, mHashLocator);
  printOntoTable(hash, table_mq_header, table_mq_data, listformat, filter);
}

//----------------------------------------------------------------------------
// Get the activation status via a cache.
// This can be used with a small cache which 1s expiration time to avoid too
// many lookup's in tight loops.
//----------------------------------------------------------------------------
ActiveStatus
FileSystem::GetActiveStatus(bool cached)
{
  XrdSysMutexHelper lock(cActiveLock);

  if (cached) {
    time_t now = time(NULL);

    if (now - cActiveTime) {
      cActiveTime = now;
    } else {
      return cActive;
    }
  }

  std::string active = GetString("stat.active");

  if (active == "online") {
    cActive = ActiveStatus::kOnline;
    return ActiveStatus::kOnline;
  } else if (active == "offline") {
    cActive = ActiveStatus::kOffline;
    return ActiveStatus::kOffline;
  } else if (active == "overload") {
    cActive = ActiveStatus::kOverload;
    return ActiveStatus::kOverload;
  } else {
    cActive = ActiveStatus::kUndefined;
    return ActiveStatus::kUndefined;
  }
}

//------------------------------------------------------------------------------
// Convert input to file system id
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
FileSystem::ConvertToFsid(const std::string& value)
{
  eos::common::FileSystem::fsid_t fsid = 0ul;

  try {
    size_t pos = 0;
    fsid = std::stoul(value, &pos);

    if (pos != value.length()) {
      throw std::runtime_error("failed fsid conversion");
    }
  } catch (...) {
    fsid = 0ul;
  }

  return fsid;
}

EOSCOMMONNAMESPACE_END;
