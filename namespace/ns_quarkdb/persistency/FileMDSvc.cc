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

#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/utils/StringConvertion.hh"
#include <numeric>

EOSNSNAMESPACE_BEGIN

std::uint64_t FileMDSvc::sNumFileBuckets(1024 * 1024);
std::chrono::seconds FileMDSvc::sFlushInterval(5);

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMDSvc::FileMDSvc()
  : pQuotaStats(nullptr), pContSvc(nullptr), mFlushTimestamp(std::time(nullptr)),
    pBkendPort(0), pBkendHost(""), pQcl(nullptr), mMetaMap(),
    mDirtyFidBackend(), mFlushFidSet(), mFileCache(10e6) {}

//------------------------------------------------------------------------------
// Configure the file service
//------------------------------------------------------------------------------
void
FileMDSvc::configure(const std::map<std::string, std::string>& config)
{
  const std::string key_host = "qdb_host";
  const std::string key_port = "qdb_port";

  if (config.find(key_host) != config.end()) {
    pBkendHost = config.at(key_host);
  }

  if (config.find(key_port) != config.end()) {
    pBkendPort = std::stoul(config.at(key_port));
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

  pQcl = BackendClient::getInstance(pBkendHost, pBkendPort);
  mMetaMap.setKey(constants::sMapMetaInfoKey);
  mMetaMap.setClient(*pQcl);
  mDirtyFidBackend.setKey(constants::sSetCheckFiles);
  mDirtyFidBackend.setClient(*pQcl);
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
    qclient::QHash bucket_map(*pQcl, getBucketKey(id));
    blob = bucket_map.hget(sid);
  } catch (std::runtime_error& qdb_err) {
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
  file.get()->deserialize(ebuff);
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
  obj->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());

  try {
    std::string sid = stringify(obj->getId());
    qclient::QHash bucket_map(*pQcl, getBucketKey(obj->getId()));
    bucket_map.hset(sid, buffer);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << obj->getId() << " failed to contact backend";
    throw e;
  }

  // Flush fids in bunches to avoid too many round trips to the backend
  flushDirtySet(obj->getId());
}

//------------------------------------------------------------------------------
// Remove object from the store
//------------------------------------------------------------------------------
void
FileMDSvc::removeFile(IFileMD* obj)
{
  try {
    std::string sid = stringify(obj->getId());
    qclient::QHash bucket_map(*pQcl, getBucketKey(obj->getId()));
    bucket_map.hdel(sid);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << obj->getId() << " not found. ";
    e.getMessage() << "The object was not created in this store!";
    throw e;
  }

  IFileMDChangeListener::Event e(obj, IFileMDChangeListener::Deleted);
  notifyListeners(&e);
  // Wait for any async notification before deleting the object
  (void) dynamic_cast<FileMD*>(obj)->waitAsyncReplies();
  mFileCache.remove(obj->getId());
  flushDirtySet(obj->getId(), true);
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
uint64_t
FileMDSvc::getNumFiles()
{
  std::atomic<std::uint32_t> num_requests(0);
  std::atomic<std::uint64_t> num_files(0);
  std::string bucket_key("");
  qclient::AsyncHandler ah;

  for (std::uint64_t i = 0; i < sNumFileBuckets; ++i) {
    bucket_key = stringify(i);
    bucket_key += constants::sFileKeySuffix;
    qclient::QHash bucket_map(*pQcl, bucket_key);
    ah.Register(bucket_map.hlen_async(), qclient::OpType::HLEN);
  }

  // Wait for all responses and sum up the results
  (void) ah.Wait();
  std::vector<long long int> resp = ah.GetResponses();
  num_files = std::accumulate(resp.begin(), resp.end(), (long long int)0);
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
  // Mark file as inconsistent so we can recover it in case of a crash
  addToDirtySet(event->file);

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
// Check file object consistency
//------------------------------------------------------------------------------
bool
FileMDSvc::checkFiles()
{
  bool is_ok = true;
  std::string cursor {"0"};
  std::pair<std::string, std::vector<std::string>> reply;
  std::vector<std::string> to_drop;

  do {
    reply = mDirtyFidBackend.sscan(cursor);
    cursor = reply.first;

    for (auto && elem : reply.second) {
      if (checkFile(std::stoull(elem))) {
        to_drop.emplace_back(elem);
      } else {
        is_ok = false;
      }
    }
  } while (cursor != "0");

  if (!to_drop.empty()) {
    try {
      if (mDirtyFidBackend.srem(to_drop) != (long long int) to_drop.size()) {
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
// one reliable and to be enforced.
//------------------------------------------------------------------------------
bool
FileMDSvc::checkFile(std::uint64_t fid)
{
  try {
    std::shared_ptr<IFileMD> file = getFileMD(fid);

    for (auto && elem : pListeners) {
      if (!elem->fileMDCheck(file.get())) {
        return false;
      }
    }
  } catch (MDException& e) {
    fprintf(stderr, "[%s] Fid: %lu not found.\n", __FUNCTION__, fid);
    return false;
  }

  return true;
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

//------------------------------------------------------------------------------
// Add file object to consistency check list
//------------------------------------------------------------------------------
void
FileMDSvc::addToDirtySet(IFileMD* file)
{
  // Remove from the set of fid to be flushed and update the backend only if
  // wasn't in the set - optimize the number of RTT to backend.
  IFileMD::id_t fid = file->getId();

  if (mFlushFidSet.erase(stringify(fid)) == 0) {
    try {
      mDirtyFidBackend.sadd(fid);  // add to backend set
    } catch (std::runtime_error& qdb_err) {
      MDException e(ENOENT);
      e.getMessage() << "File #" << fid
                     << " failed to insert into the set of files to be checked "
                     << "- got an exception";
      throw e;
    }
  }
}

//------------------------------------------------------------------------------
// Remove all accumulated file ids from the local "dirty" set and mark them in
// the backend set accordingly.
//------------------------------------------------------------------------------
void
FileMDSvc::flushDirtySet(IFileMD::id_t id, bool force)
{
  (void) mFlushFidSet.insert(stringify(id));
  std::time_t now = std::time(nullptr);
  std::chrono::seconds duration(now - mFlushTimestamp);

  if (force || (duration >= sFlushInterval)) {
    mFlushTimestamp = now;
    std::vector<std::string> to_del(mFlushFidSet.begin(), mFlushFidSet.end());
    mFlushFidSet.clear();

    try {
      mDirtyFidBackend.srem(to_del);
    } catch (std::runtime_error& qdb_err) {
      MDException e(ENOENT);
      e.getMessage() << "Failed to clear set of dirty files - backend error";
      throw e;
    }
  }
}

//------------------------------------------------------------------------------
// Get first free file id
//------------------------------------------------------------------------------
IFileMD::id_t
FileMDSvc::getFirstFreeId()
{
  id_t id = 0;
  std::string sval = mMetaMap.hget(constants::sFirstFreeFid);

  if (!sval.empty()) {
    id = std::stoull(sval);
  }

  return id;
}

EOSNSNAMESPACE_END
