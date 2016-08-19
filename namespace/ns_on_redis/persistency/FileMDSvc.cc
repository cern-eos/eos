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

#include "namespace/ns_on_redis/persistency/FileMDSvc.hh"
#include "namespace/ns_on_redis/Constants.hh"
#include "namespace/ns_on_redis/FileMD.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include "namespace/ns_on_redis/accounting/QuotaStats.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"
#include "namespace/utils/StringConvertion.hh"

EOSNSNAMESPACE_BEGIN

std::uint64_t FileMDSvc::sNumFileBuckets = 1024 * 1024;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMDSvc::FileMDSvc()
  : pQuotaStats(nullptr), pContSvc(nullptr), pRedox(nullptr), mMetaMap(),
    mSetCheckFiles(), pRedisHost(""), pRedisPort(0), mFileCache(10e6) {}

//------------------------------------------------------------------------------
// Configure the file service
//------------------------------------------------------------------------------
void
FileMDSvc::configure(const std::map<std::string, std::string>& config)
{
  const std::string key_host = "redis_host";
  const std::string key_port = "redis_port";

  if (config.find(key_host) != config.end()) {
    pRedisHost = config.at(key_host);
  }

  if (config.find(key_port) != config.end()) {
    pRedisPort = std::stoul(config.at(key_port));
  }
}

//------------------------------------------------------------------------------
// Initialize the file service
//------------------------------------------------------------------------------
void
FileMDSvc::initialize()
{
  if (pContSvc == nullptr) {
    MDException e(EINVAL);
    e.getMessage() << "FileMDSvc: container service not set";
    throw e;
  }

  pRedox = RedisClient::getInstance(pRedisHost, pRedisPort);
  mMetaMap.setKey(constants::sMapMetaInfoKey);
  mMetaMap.setClient(*pRedox);
  mSetCheckFiles.setKey(constants::sSetCheckFiles);
  mSetCheckFiles.setClient(*pRedox);
}

//------------------------------------------------------------------------------
// Get the file metadata information for the given file ID
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
FileMDSvc::getFileMD(IFileMD::id_t id)
{
  // Check first in cache
  std::shared_ptr<IFileMD> file = mFileCache.get(id);

  if (file != nullptr) {
    return file;
  }

  // If not in cache, then get info from KV store
  std::string blob;

  try {
    std::string sid = stringify(id);
    redox::RedoxHash bucket_map(*pRedox, getBucketKey(id));
    blob = bucket_map.hget(sid);
  } catch (std::runtime_error& redis_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << id << " not found";
    throw e;
  }

  if (blob.empty()) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << id << " not found";
    throw e;
  }

  file = std::make_shared<FileMD>(0, this);
  eos::Buffer ebuff;
  ebuff.putData(blob.c_str(), blob.length());
  dynamic_cast<FileMD*>(file.get())->deserialize(ebuff);
  return mFileCache.put(file->getId(), file);
}

//------------------------------------------------------------------------------
// Create new file metadata object
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
FileMDSvc::createFile()
{
  try {
    // Get first available file id
    uint64_t free_id = mMetaMap.hincrby(constants::sFirstFreeFid, 1);
    std::shared_ptr<IFileMD> file{new FileMD(free_id, this)};
    file = mFileCache.put(free_id, file);
    IFileMDChangeListener::Event e(file.get(), IFileMDChangeListener::Created);
    notifyListeners(&e);
    return file;
  } catch (std::runtime_error& e) {
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// Update backend store and notify all the listeners
//------------------------------------------------------------------------------
void
FileMDSvc::updateStore(IFileMD* obj)
{
  eos::Buffer ebuff;

  if (!dynamic_cast<FileMD*>(obj)->serialize(ebuff)) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << obj->getId() << " serialization failed";
    throw e;
  }

  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());

  try {
    std::string sid = stringify(obj->getId());
    redox::RedoxHash bucket_map(*pRedox, getBucketKey(obj->getId()));
    bucket_map.hset(sid, buffer);
  } catch (std::runtime_error& redis_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << obj->getId() << " failed to contact backend";
    throw e;
  }

  if (!dynamic_cast<FileMD*>(obj)->IsConsistent()) {
    mSetCheckFiles.srem(obj->getId());
    dynamic_cast<FileMD*>(obj)->SetConsistent(true);
  }
}

//------------------------------------------------------------------------------
// Remove object from the store
//------------------------------------------------------------------------------
void
FileMDSvc::removeFile(IFileMD* obj)
{
  IFileMD::id_t file_id = obj->getId();

  try {
    std::string sid = stringify(file_id);
    redox::RedoxHash bucket_map(*pRedox, getBucketKey(file_id));
    bucket_map.hdel(sid);
  } catch (std::runtime_error& redis_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << file_id << " not found. ";
    e.getMessage() << "The object was not created in this store!";
    throw e;
  }

  // Remove file from the set of files to chec
  mSetCheckFiles.srem(file_id);
  IFileMDChangeListener::Event e(file_id, IFileMDChangeListener::Deleted);
  notifyListeners(&e);
  // TODO (esindril): Wait for any async notification from the views before
  // deleting the file object
  mFileCache.remove(file_id);
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
uint64_t
FileMDSvc::getNumFiles()
{
  std::atomic<std::uint32_t> num_requests{0};
  std::atomic<std::uint64_t> num_files{0};
  std::string bucket_key("");
  std::mutex mutex;
  std::condition_variable cond_var;
  auto callback_len = [&num_files, &num_requests,
  &cond_var](redox::Command<long long int>& c) {
    if (c.ok()) {
      num_files += c.reply();
    }

    if (--num_requests == 0u) {
      cond_var.notify_one();
    }
  };
  auto wrapper_cb = [&num_requests, &callback_len]() -> decltype(callback_len) {
    num_requests++;
    return callback_len;
  };

  for (std::uint64_t i = 0; i < sNumFileBuckets; ++i) {
    bucket_key = stringify(i);
    bucket_key += constants::sFileKeySuffix;
    redox::RedoxHash bucket_map(*pRedox, bucket_key);

    try {
      bucket_map.hlen(wrapper_cb());
    } catch (std::runtime_error& redis_err) {
      // no change
    }
  }

  {
    // Wait for all responses
    std::unique_lock<std::mutex> lock(mutex);

    while (num_requests != 0u) {
      cond_var.wait(lock);
    }
  }

  return num_files;
}

//------------------------------------------------------------------------------
// Attach a broken file to lost+found
//------------------------------------------------------------------------------
void
FileMDSvc::attachBroken(const std::string& parent, IFileMD* file)
{
  std::ostringstream s1, s2;
  std::shared_ptr<IContainerMD> parentCont =
    pContSvc->getLostFoundContainer(parent);
  s1 << file->getContainerId();
  std::shared_ptr<IContainerMD> cont = parentCont->findContainer(s1.str());

  if (!cont) {
    cont = pContSvc->createInParent(s1.str(), parentCont.get());
  }

  s2 << file->getName() << "." << file->getId();
  file->setName(s2.str());
  cont->addFile(file);
}

//------------------------------------------------------------------------------
// Add file listener
//------------------------------------------------------------------------------
void
FileMDSvc::addChangeListener(IFileMDChangeListener* listener)
{
  pListeners.push_back(listener);
}

//------------------------------------------------------------------------------
// Notify the listeners about the change
//------------------------------------------------------------------------------
void
FileMDSvc::notifyListeners(IFileMDChangeListener::Event* event)
{
  for (auto && elem : pListeners) {
    elem->fileMDChanged(event);
  }
}

//------------------------------------------------------------------------------
// Set container service
//------------------------------------------------------------------------------
void
FileMDSvc::setContMDService(IContainerMDSvc* cont_svc)
{
  pContSvc = dynamic_cast<eos::ContainerMDSvc*>(cont_svc);
}

//------------------------------------------------------------------------------
// Set the QuotaStats object for the follower
//------------------------------------------------------------------------------
void
FileMDSvc::setQuotaStats(IQuotaStats* quota_stats)
{
  pQuotaStats = quota_stats;
}

//------------------------------------------------------------------------------
// Check files that have errors
//------------------------------------------------------------------------------
bool
FileMDSvc::checkFiles()
{
  bool is_ok = true;
  int64_t cursor = 0;
  std::pair<int64_t, std::vector<std::string>> reply;
  std::vector<std::string> to_drop;

  do {
    reply = mSetCheckFiles.sscan(cursor);
    cursor = reply.first;

    for (auto && elem : reply.second) {
      if (checkFile(std::stoull(elem))) {
        to_drop.emplace_back(elem);
      } else {
        is_ok = false;
      }
    }
  } while (cursor != 0);

  if (!to_drop.empty()) {
    try {
      if (mSetCheckFiles.srem(to_drop) != (long long int) to_drop.size()) {
        fprintf(stderr, "Failed to drop files that have been fixed\n");
      }
    } catch (std::runtime_error& e) {
      fprintf(stderr, "Failed to drop files that have been fixed\n");
    }
  }

  return is_ok;
}

//------------------------------------------------------------------------------
// Recheck individual file - the information stored in the filemd map is the
// one reliabe and to be enforced.
//------------------------------------------------------------------------------
bool
FileMDSvc::checkFile(std::uint64_t fid)
{
  bool is_ok = true;
  std::shared_ptr<IFileMD> file = getFileMD(fid);

  for (auto && elem : pListeners) {
    if (!elem->fileMDCheck(file.get())) {
      is_ok = false;
      break;
    }
  }

  return is_ok;
}

//------------------------------------------------------------------------------
// Add file object to consistency check list
//------------------------------------------------------------------------------
void
FileMDSvc::addToConsistencyCheck(IFileMD::id_t id)
{
  try {
    mSetCheckFiles.sadd(id);
  } catch (std::runtime_error& redis_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << id
                   << " failed to insert into the set of files to be checked "
                   << "- got an exception";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Get file bucket
//------------------------------------------------------------------------------
std::string
FileMDSvc::getBucketKey(IContainerMD::id_t id) const
{
  if (id >= sNumFileBuckets) {
    id = id & (sNumFileBuckets - 1);
  }

  std::string bucket_key = stringify(id);
  bucket_key += constants::sFileKeySuffix;
  return bucket_key;
}

EOSNSNAMESPACE_END
