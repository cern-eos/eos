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
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/persistency/MetadataProvider.hh"
#include "namespace/utils/StringConvertion.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "common/Assert.hh"
#include "common/Logging.hh"
#include <memory>
#include <numeric>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMDSvc::ContainerMDSvc()
  : pQuotaStats(nullptr), pFileSvc(nullptr), pQcl(nullptr), pFlusher(nullptr),
    mMetaMap(), mNumConts(0ull) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ContainerMDSvc::~ContainerMDSvc()
{
  if (pFlusher) {
    pFlusher->synchronize();
  }
}

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

  if ((config.find(key_cluster) != config.end()) &&
      (config.find(key_flusher) != config.end())) {
    qdb_cluster = config.at(key_cluster);
    qdb_flusher_id = config.at(key_flusher);
    QdbContactDetails contactDetails;

    if (!contactDetails.members.parse(qdb_cluster)) {
      eos::MDException e(EINVAL);
      e.getMessage() << __FUNCTION__ << " Failed to parse qdbcluster members: "
                     << qdb_cluster;
      throw e;
    }

    pQcl = BackendClient::getInstance(contactDetails.members);
    mMetaMap.setKey(constants::sMapMetaInfoKey);
    mMetaMap.setClient(*pQcl);
    mMetaMap.hset("EOS-NS-FORMAT-VERSION", "1");
    mInodeProvider.configure(mMetaMap, constants::sLastUsedCid);
    pFlusher = MetadataFlusherFactory::getInstance(qdb_flusher_id, contactDetails.members);
  }

  if (config.find(constants::sMaxNumCacheDirs) != config.end()) {
    mCacheNum = config.at(constants::sMaxNumCacheDirs);

    if (mMetadataProvider) {
      mMetadataProvider->setContainerMDCacheNum(std::stoull(mCacheNum));
    }
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

  if (mMetadataProvider == nullptr) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__  << " No metadata provider set for "
                    << "the container metadata service";
    throw e;
  }

  if ((pQcl == nullptr) || (pFlusher == nullptr)) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__ << " No qclient/flusher initialized for "
                    << "the container metadata service";
    throw e;
  }

  if (!mCacheNum.empty()) {
    mMetadataProvider->setContainerMDCacheNum(std::stoull(mCacheNum));
  }

  SafetyCheck();
  ComputeNumberOfContainers();
}

//------------------------------------------------------------------------------
// Safety check to make sure there are no container entries in the backed
// with ids bigger than the max container id.
//------------------------------------------------------------------------------
void
ContainerMDSvc::SafetyCheck()
{
  std::string blob;
  IContainerMD::id_t free_id = getFirstFreeId();
  std::list<uint64_t> offsets  = {1, 10, 50, 100, 501, 1001, 11000, 50000,
                                  100000, 150199, 200001, 1000002, 2000123
                                 };
  std::vector<folly::Future<eos::ns::ContainerMdProto>> futs;

  for (auto incr : offsets) {
    IContainerMD::id_t check_id = free_id + incr;
    futs.emplace_back(MetadataFetcher::getContainerFromId(*pQcl,
                      ContainerIdentifier(check_id)));
  }

  for (size_t i = 0; i < futs.size(); i++) {
    try {
      futs[i].get();
    } catch (eos::MDException& qdb_err) {
      // All is good, we didn't find any container, as expected
      continue;
    }

    // Uh-oh, this is bad.
    MDException e(EEXIST);
    e.getMessage()  << __FUNCTION__ << " FATAL: Risk of data loss, found "
                    << "container with id bigger than max container id";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Asynchronously get the container metadata information for the given ID
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
ContainerMDSvc::getContainerMDFut(IContainerMD::id_t id)
{
  //----------------------------------------------------------------------------
  // Short-circuit for container zero, avoid a pointless roundtrip by failing
  // immediatelly. Happens for files which have been unlinked, but not removed
  // yet.
  //----------------------------------------------------------------------------
  if (id == 0) {
    return folly::makeFuture<IContainerMDPtr>(make_mdexception(ENOENT,
           "Container #0 not found"));
  }

  return mMetadataProvider->retrieveContainerMD(ContainerIdentifier(id));
}

//------------------------------------------------------------------------------
// Get the container metadata information
//------------------------------------------------------------------------------
IContainerMDPtr
ContainerMDSvc::getContainerMD(IContainerMD::id_t id, uint64_t* clock)
{
  IContainerMDPtr container = getContainerMDFut(id).get();

  if (container && clock) {
    *clock = container->getClock();
  }

  return container;
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
  ++mNumConts;
  mMetadataProvider->insertContainerMD(cont->getIdentifier(), cont);
  return cont;
}

//----------------------------------------------------------------------------
// Update backend store and notify listeners
//----------------------------------------------------------------------------
void
ContainerMDSvc::updateStore(IContainerMD* obj)
{
  pFlusher->execute(RequestBuilder::writeContainerProto(obj));
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
  pFlusher->execute(RequestBuilder::deleteContainerProto(ContainerIdentifier(
                      obj->getId())));

  // If this was the root container i.e. id=1 then drop also the meta map
  if (obj->getId() == 1) {
    pFlusher->del(constants::sMapMetaInfoKey);
  }

  obj->setDeleted();

  if (mNumConts) {
    --mNumConts;
  }
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
  ++mNumConts;
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
  return mNumConts.load();
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

//------------------------------------------------------------------------------
// Compute the number of containers from the backend
//------------------------------------------------------------------------------
void
ContainerMDSvc::ComputeNumberOfContainers()
{
  std::string bucket_key("");
  qclient::AsyncHandler ah;

  for (std::uint64_t i = 0; i < RequestBuilder::sNumContBuckets; ++i) {
    bucket_key = stringify(i);
    bucket_key += constants::sContKeySuffix;
    qclient::QHash bucket_map(*pQcl, bucket_key);
    bucket_map.hlen_async(&ah);
  }

  (void) ah.Wait();
  auto resp = ah.GetResponses();
  mNumConts.store(std::accumulate(resp.begin(), resp.end(), 0ull));
  mNumConts += pQcl->execute(
                 RequestBuilder::getNumberOfContainers()).get()->integer;
}

//------------------------------------------------------------------------------
//! Retrieve MD cache statistics.
//------------------------------------------------------------------------------
CacheStatistics
ContainerMDSvc::getCacheStatistics()
{
  return mMetadataProvider->getContainerMDCacheStats();
}

EOSNSNAMESPACE_END
