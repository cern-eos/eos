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

#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/utils/StringConvertion.hh"
#include <memory>
#include <numeric>

EOSNSNAMESPACE_BEGIN

std::uint64_t ContainerMDSvc::sNumContBuckets = 128 * 1024;

//------------------------------------------------------------------------------
// Get container bucket
//------------------------------------------------------------------------------
std::string
ContainerMDSvc::getBucketKey(IContainerMD::id_t id)
{
  id = id & (sNumContBuckets - 1);
  std::string bucket_key = stringify(id);
  bucket_key += constants::sContKeySuffix;
  return bucket_key;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMDSvc::ContainerMDSvc()
  : pQuotaStats(nullptr), pFileSvc(nullptr), pQcl(nullptr), pFlusher(nullptr),
    mMetaMap(), mContainerCache(10e7) {}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void
ContainerMDSvc::configure(const std::map<std::string, std::string>& config)
{
  std::string qdb_cluster;
  std::string qdb_flusher_id;
  const std::string key_cluster = "qdb_cluster";
  const std::string key_flusher = "qdb_flusher_md";
  const std::string cache_size = "dir_cache_size";

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
    mMetaMap.hset("EOS-NS-FORMAT-VERSION", "1");
    mInodeProvider.configure(mMetaMap, constants::sLastUsedCid);
    pFlusher = MetadataFlusherFactory::getInstance(qdb_flusher_id, qdb_members);
  }

  if (config.find(cache_size) != config.end()) {
    mContainerCache.set_max_size(std::stoull(config.at(cache_size)));
  }
}

//------------------------------------------------------------------------------
// Initizlize the container service
//------------------------------------------------------------------------------
void
ContainerMDSvc::initialize()
{
  if (pFileSvc == nullptr) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__  << " No file metadata service set for "
                    << "the container metadata service";
    throw e;
  }

  if ((pQcl == nullptr) || (pFlusher == nullptr)) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__ << " No qclient/flusher initialized for "
                    << "the container metadata service";
    throw e;
  }

  SafetyCheck();
}

//------------------------------------------------------------------------------
// Safety check to make sure there are no container entries in the backed
// with ids bigger than the max container id.
//------------------------------------------------------------------------------
void
ContainerMDSvc::SafetyCheck()
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
      // Fine, we didn't find the container
      continue;
    }

    if (!blob.empty()) {
      MDException e(EEXIST);
      e.getMessage()  << __FUNCTION__ << " FATAL: Risk of data loss, found "
                      << "container with id bigger than max container id";
      throw e;
    }
  }
}

//----------------------------------------------------------------------------
// Get the container metadata information
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::getContainerMD(IContainerMD::id_t id, uint64_t* clock)
{
  // Check first in cache
  std::shared_ptr<IContainerMD> cont = mContainerCache.get(id);

  if (cont != nullptr) {
    if (cont->isDeleted()) {
      MDException e(ENOENT);
      e.getMessage()  << __FUNCTION__ << " Container #" << id << " not found";
      throw e;
    } else {
      if (clock) {
        *clock = cont->getClock();
      }

      return cont;
    }
  }

  // If not in cache, then get it from the KV backend
  std::string blob;

  try {
    std::string sid = stringify(id);
    qclient::QHash bucket_map(*pQcl, getBucketKey(id));
    blob = bucket_map.hget(sid);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << id << " not found";
    throw e;
  }

  if (blob.empty()) {
    MDException e(ENOENT);
    e.getMessage()  << __FUNCTION__ << " Container #" << id << " not found";
    throw e;
  }

  cont.reset(new ContainerMD(0, pFileSvc, static_cast<IContainerMDSvc*>(this)));
  eos::Buffer ebuff;
  ebuff.putData(blob.c_str(), blob.length());
  cont->deserialize(ebuff);

  if (clock) {
    *clock = cont->getClock();
  }

  return mContainerCache.put(cont->getId(), cont);
}

//----------------------------------------------------------------------------
// Create a new container metadata object
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::createContainer()
{
  uint64_t free_id = mInodeProvider.reserve();
  std::shared_ptr<IContainerMD> cont
  (new ContainerMD(free_id, pFileSvc, static_cast<IContainerMDSvc*>(this)));
  return mContainerCache.put(cont->getId(), cont);
}

//----------------------------------------------------------------------------
// Update backend store and notify listeners
//----------------------------------------------------------------------------
void
ContainerMDSvc::updateStore(IContainerMD* obj)
{
  eos::Buffer ebuff;
  obj->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());
  std::string sid = stringify(obj->getId());
  pFlusher->hset(getBucketKey(obj->getId()), sid, buffer);
}

//----------------------------------------------------------------------------
// Remove object from the store assuming it's already empty
//----------------------------------------------------------------------------
void
ContainerMDSvc::removeContainer(IContainerMD* obj)
{
  // Protection in case the container is not empty
  if ((obj->getNumFiles() != 0) || (obj->getNumContainers() != 0)) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__ << " Failed to remove container #"
                    << obj->getId() << " since it's not empty";
    throw e;
  }

  std::string sid = stringify(obj->getId());
  pFlusher->hdel(getBucketKey(obj->getId()), stringify(obj->getId()));

  // If this was the root container i.e. id=1 then drop also the meta map
  if (obj->getId() == 1) {
    pFlusher->del(constants::sMapMetaInfoKey);
  }

  obj->setDeleted();
}

//------------------------------------------------------------------------------
// Add change listener
//------------------------------------------------------------------------------
void
ContainerMDSvc::addChangeListener(IContainerMDChangeListener* listener)
{
  pListeners.push_back(listener);
}

//------------------------------------------------------------------------------
// Create container in parent
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::createInParent(const std::string& name, IContainerMD* parent)
{
  std::shared_ptr<IContainerMD> container = createContainer();
  container->setName(name);
  parent->addContainer(container.get());
  updateStore(container.get());
  return container;
}

//----------------------------------------------------------------------------
// Get the lost+found container, create if necessary
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::getLostFound()
{
  // Get root
  std::shared_ptr<IContainerMD> root;

  try {
    root = getContainerMD(1);
  } catch (MDException& e) {
    root = createContainer();
    root->setParentId(root->getId());
    updateStore(root.get());
  }

  // Get or create lost+found if necessary
  std::shared_ptr<IContainerMD> lostFound = root->findContainer("lost+found");

  if (lostFound == nullptr) {
    lostFound = createInParent("lost+found", root.get());
  }

  return lostFound;
}

//----------------------------------------------------------------------------
// Get the orphans / name conflicts container
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::getLostFoundContainer(const std::string& name)
{
  std::shared_ptr<IContainerMD> lostFound = getLostFound();

  if (name.empty()) {
    return lostFound;
  }

  std::shared_ptr<IContainerMD> cont = lostFound->findContainer(name);

  if (cont == nullptr) {
    cont = createInParent(name, lostFound.get());
  }

  return cont;
}

//------------------------------------------------------------------------------
// Get number of containers which is sum(hlen(hash_i)) for i=0,128k
//------------------------------------------------------------------------------
uint64_t
ContainerMDSvc::getNumContainers()
{
  std::uint64_t num_conts = 0;
  std::string bucket_key("");
  qclient::AsyncHandler ah;

  for (std::uint64_t i = 0; i < sNumContBuckets; ++i) {
    bucket_key = stringify(i);
    bucket_key += constants::sContKeySuffix;
    qclient::QHash bucket_map(*pQcl, bucket_key);
    bucket_map.hlen_async(&ah);
  }

  (void) ah.Wait();
  auto resp = ah.GetResponses();
  num_conts = std::accumulate(resp.begin(), resp.end(), 0ull);
  return num_conts;
}

//------------------------------------------------------------------------------
// Notify the listeners about the change
//------------------------------------------------------------------------------
void
ContainerMDSvc::notifyListeners(IContainerMD* obj,
                                IContainerMDChangeListener::Action a)
{
  for (const auto& elem : pListeners) {
    elem->containerMDChanged(obj, a);
  }
}

//------------------------------------------------------------------------------
// Get first free container id
//------------------------------------------------------------------------------
IContainerMD::id_t
ContainerMDSvc::getFirstFreeId()
{
  return mInodeProvider.getFirstFreeId();
}

EOSNSNAMESPACE_END
