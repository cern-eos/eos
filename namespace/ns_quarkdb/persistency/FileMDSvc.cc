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
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/utils/StringConvertion.hh"
#include <numeric>

EOSNSNAMESPACE_BEGIN

std::uint64_t FileMDSvc::sNumFileBuckets(1024 * 1024);
std::chrono::seconds FileMDSvc::sFlushInterval(5);

//----------------------------------------------------------------------------
//! Override number of buckets
//----------------------------------------------------------------------------
void FileMDSvc::OverrideNumberOfBuckets(uint64_t buckets)
{
  sNumFileBuckets = buckets;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMDSvc::FileMDSvc()
  : pQuotaStats(nullptr), pContSvc(nullptr), pFlusher(nullptr), pQcl(nullptr),
    mMetaMap(), mFileCache(10e8), mNumFiles(0ull) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileMDSvc::~FileMDSvc()
{
  if (pFlusher) {
    pFlusher->synchronize();
  }
}

//------------------------------------------------------------------------------
// Configure the file service
//------------------------------------------------------------------------------
void
FileMDSvc::configure(const std::map<std::string, std::string>& config)
{
  std::string qdb_cluster;
  std::string qdb_flusher_id;
  const std::string key_cluster = "qdb_cluster";
  const std::string key_flusher = "qdb_flusher_md";
  const std::string cache_size = "file_cache_size";

  if ((config.find(key_cluster) != config.end()) &&
      (config.find(key_flusher) != config.end())) {
    qdb_cluster = config.at(key_cluster);
    qdb_flusher_id = config.at(key_flusher);
    qclient::Members qdb_members;

    if (!qdb_members.parse(qdb_cluster)) {
      eos::MDException e(EINVAL);
      e.getMessage() << __FUNCTION__ << " Failed to parse qdbcluster members: "
                     << qdb_cluster;
      throw e;
    }

    pQcl = BackendClient::getInstance(qdb_members);
    mMetaMap.setKey(constants::sMapMetaInfoKey);
    mMetaMap.setClient(*pQcl);
    mInodeProvider.configure(mMetaMap, constants::sLastUsedFid);
    pFlusher = MetadataFlusherFactory::getInstance(qdb_flusher_id, qdb_members);
  }

  if (config.find(cache_size) != config.end()) {
    mFileCache.set_max_size(std::stoull(config.at(cache_size)));
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
    e.getMessage()  << __FUNCTION__ << " FileMDSvc: container service not set";
    throw e;
  }

  if ((pQcl == nullptr) || (pFlusher == nullptr)) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__ << " No qclient/flusher initialized for "
                    << "the container metadata service";
    throw e;
  }

  SafetyCheck();
  ComputeNumberOfFiles();
}

//------------------------------------------------------------------------------
// Safety check to make sure there are no file entries in the backend with
// ids bigger than the max file id.
//------------------------------------------------------------------------------
void
FileMDSvc::SafetyCheck()
{
  std::string blob;
  id_t free_id = getFirstFreeId();
  std::list<uint64_t> offsets  = {1, 10, 50, 100, 501, 1001, 11000, 50000,
                                  100000, 150199, 200001, 1000002, 2000123
                                 };

  for (auto incr : offsets) {
    id_t check_id = free_id + incr;

    try {
      std::string sid = stringify(check_id);
      qclient::QHash bucket_map(*pQcl, getBucketKey(check_id));
      blob = bucket_map.hget(sid);
    } catch (std::runtime_error& qdb_err) {
      // Fine, we didn't find the file
      continue;
    }

    if (!blob.empty()) {
      MDException e(EEXIST);
      e.getMessage() << __FUNCTION__ << " FATAL: Risk of data loss, found "
                     << "file with id bigger max file id";
      throw e;
    }
  }
}

//------------------------------------------------------------------------------
// Get the file metadata information for the given file id
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
FileMDSvc::getFileMD(IFileMD::id_t id, uint64_t* clock)
{
  auto file = mFileCache.get(id);

  if (file != nullptr) {
    if (file->isDeleted()) {
      MDException e(ENOENT);
      e.getMessage() << __FUNCTION__ << " File #" << id << " not found";
      throw e;
    } else {
      if (clock) {
        *clock = file->getClock();
      }

      return file;
    }
  }

  // If not in cache, then get info from KV store
  std::shared_ptr<FileMD> retval = std::make_shared<FileMD>(0, this);
  retval->initialize(MetadataFetcher::getFileFromId(*pQcl, id).get());
  return mFileCache.put(retval->getId(), retval);
}

//------------------------------------------------------------------------------
// Create new file metadata object
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
FileMDSvc::createFile()
{
  uint64_t free_id = mInodeProvider.reserve();
  std::shared_ptr<IFileMD> file{new FileMD(free_id, this)};
  file = mFileCache.put(free_id, file);
  IFileMDChangeListener::Event e(file.get(), IFileMDChangeListener::Created);
  notifyListeners(&e);
  ++mNumFiles;
  return file;
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
  std::string sid = stringify(obj->getId());
  pFlusher->hset(getBucketKey(obj->getId()), sid, buffer);
}

//------------------------------------------------------------------------------
// Remove object from the store
//------------------------------------------------------------------------------
void
FileMDSvc::removeFile(IFileMD* obj)
{
  std::string sid = stringify(obj->getId());
  pFlusher->hdel(getBucketKey(obj->getId()), sid);
  IFileMDChangeListener::Event e(obj, IFileMDChangeListener::Deleted);
  notifyListeners(&e);
  obj->setDeleted();

  if (mNumFiles) {
    --mNumFiles;
  }
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
uint64_t
FileMDSvc::getNumFiles()
{
  return mNumFiles.load();
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
  for (const auto& elem : pListeners) {
    elem->fileMDChanged(event);
  }
}

//------------------------------------------------------------------------------
// Set container service
//------------------------------------------------------------------------------
void
FileMDSvc::setContMDService(IContainerMDSvc* cont_svc)
{
  ContainerMDSvc* impl_cont_svc = dynamic_cast<eos::ContainerMDSvc*>(cont_svc);

  if (!impl_cont_svc) {
    MDException e(EFAULT);
    e.getMessage() << __FUNCTION__ << " ContainerMDSvc dynamic cast failed";
    throw e;
  }

  pContSvc = impl_cont_svc;
}

//------------------------------------------------------------------------------
// Get file bucket
//------------------------------------------------------------------------------
std::string
FileMDSvc::getBucketKey(IContainerMD::id_t id)
{
  id = id & (sNumFileBuckets - 1);
  std::string bucket_key = stringify(id);
  bucket_key += constants::sFileKeySuffix;
  return bucket_key;
}

//------------------------------------------------------------------------------
// Get first free file id
//------------------------------------------------------------------------------
IFileMD::id_t
FileMDSvc::getFirstFreeId()
{
  return mInodeProvider.getFirstFreeId();
}

//----------------------------------------------------------------------------
//! Compute the number of files from the backend
//----------------------------------------------------------------------------
void
FileMDSvc::ComputeNumberOfFiles()
{
  std::string bucket_key("");
  qclient::AsyncHandler ah;

  for (uint64_t i = 0ull; i < sNumFileBuckets; ++i) {
    bucket_key = stringify(i);
    bucket_key += constants::sFileKeySuffix;
    qclient::QHash bucket_map(*pQcl, bucket_key);
    bucket_map.hlen_async(&ah);
  }

  // Wait for all responses and sum up the results
  (void) ah.Wait();
  std::list<long long int> resp = ah.GetResponses();
  mNumFiles.store(std::accumulate(resp.begin(), resp.end(), 0ull));
}

EOSNSNAMESPACE_END
