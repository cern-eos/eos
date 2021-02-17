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
#include "namespace/ns_quarkdb/persistency/MetadataProvider.hh"
#include "namespace/utils/StringConvertion.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/ConfigurationParser.hh"
#include "common/Assert.hh"
#include "common/Logging.hh"
#include "common/StacktraceHere.hh"
#include <memory>
#include <numeric>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkContainerMDSvc::QuarkContainerMDSvc(qclient::QClient* qcl,
    MetadataFlusher* flusher)
  : pQuotaStats(nullptr), pFileSvc(nullptr), pQcl(qcl), pFlusher(flusher),
    mMetaMap(), mMetadataProvider(nullptr), mNumConts(0ull) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuarkContainerMDSvc::~QuarkContainerMDSvc()
{
  if (pFlusher) {
    pFlusher->synchronize();
  }
}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void
QuarkContainerMDSvc::configure(const std::map<std::string, std::string>& config)
{
  mMetaMap.setKey(constants::sMapMetaInfoKey);
  mMetaMap.setClient(*pQcl);
  mMetaMap.hset("EOS-NS-FORMAT-VERSION", "1");

  if (config.find(constants::sMaxNumCacheDirs) != config.end()) {
    mCacheNum = config.at(constants::sMaxNumCacheDirs);

    if (mMetadataProvider) {
      mMetadataProvider->setContainerMDCacheNum(std::stoull(mCacheNum));
    }
  }
}

//------------------------------------------------------------------------------
// Initialize the container service
//------------------------------------------------------------------------------
void
QuarkContainerMDSvc::initialize()
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

  if (mUnifiedInodeProvider == nullptr) {
    MDException e(EINVAL);
    e.getMessage()  << __FUNCTION__  << " No inode provider set for "
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
  mNumConts.store(pQcl->execute(RequestBuilder::getNumberOfContainers())
                  .get()->integer);
}

//------------------------------------------------------------------------------
// Safety check to make sure there are no container entries in the backed
// with ids bigger than the max container id.
//------------------------------------------------------------------------------
void
QuarkContainerMDSvc::SafetyCheck()
{
  std::string blob;
  IContainerMD::id_t free_id = getFirstFreeId();
  std::vector<uint64_t> offsets  = {1, 10, 50, 100, 501, 1001, 11000, 50000,
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
      std::move(futs[i]).get();
    } catch (eos::MDException& qdb_err) {
      // All is good, we didn't find any container, as expected
      continue;
    }

    // Uh-oh, this is bad.
    MDException e(EEXIST);
    e.getMessage()  << __FUNCTION__ << " FATAL: Risk of data loss, found "
                    << "container (" << free_id + offsets[i] <<
                    ") with id bigger than max container id (" << free_id << ")";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Asynchronously get the container metadata information for the given ID
//------------------------------------------------------------------------------
folly::Future<IContainerMDPtr>
QuarkContainerMDSvc::getContainerMDFut(IContainerMD::id_t id)
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
QuarkContainerMDSvc::getContainerMD(IContainerMD::id_t id, uint64_t* clock)
{
  IContainerMDPtr container = getContainerMDFut(id).get();

  if (container && clock) {
    *clock = container->getClock();
  }

  return container;
}

//------------------------------------------------------------------------------
// Drop cached ContainerMD - return true if found
//------------------------------------------------------------------------------
bool
QuarkContainerMDSvc::dropCachedContainerMD(ContainerIdentifier id)
{
  return mMetadataProvider->dropCachedContainerID(id);
}

//------------------------------------------------------------------------------
// Create a new container metadata object
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
QuarkContainerMDSvc::createContainer(IContainerMD::id_t id)
{
  uint64_t free_id;

  if (id > 0) {
    mUnifiedInodeProvider->blacklistContainerId(id);
    free_id = id;
  } else {
    free_id = mUnifiedInodeProvider->reserveContainerId();
  }

  std::shared_ptr<IContainerMD> cont
  (new QuarkContainerMD(free_id, pFileSvc, static_cast<IContainerMDSvc*>(this)));
  ++mNumConts;
  mMetadataProvider->insertContainerMD(cont->getIdentifier(), cont);
  return cont;
}

//----------------------------------------------------------------------------
// Update backend store and notify listeners
//----------------------------------------------------------------------------
void
QuarkContainerMDSvc::updateStore(IContainerMD* obj)
{
  if (obj->getName() == "") {
    eos_static_crit("updateContainerStore called on container with empty "
                    "name; id=%llu, parent=%llu, trace=%s", obj->getId(),
                    obj->getParentId(), common::getStacktrace().c_str());
    // eventually throw, once we understand how this happens
  }

  pFlusher->execute(RequestBuilder::writeContainerProto(obj));
}

//----------------------------------------------------------------------------
// Remove object from the store assuming it's already empty
//----------------------------------------------------------------------------
void
QuarkContainerMDSvc::removeContainer(IContainerMD* obj)
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
QuarkContainerMDSvc::addChangeListener(IContainerMDChangeListener* listener)
{
  pListeners.push_back(listener);
}

//------------------------------------------------------------------------------
// Create container in parent
//------------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
QuarkContainerMDSvc::createInParent(const std::string& name,
                                    IContainerMD* parent)
{
  std::shared_ptr<IContainerMD> container = createContainer(0);
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
QuarkContainerMDSvc::getLostFound()
{
  // Get root
  std::shared_ptr<IContainerMD> root;

  try {
    root = getContainerMD(1);
  } catch (MDException& e) {
    root = createContainer(0);
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
QuarkContainerMDSvc::getLostFoundContainer(const std::string& name)
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
QuarkContainerMDSvc::getNumContainers()
{
  return mNumConts.load();
}

//------------------------------------------------------------------------------
// Notify the listeners about the change
//------------------------------------------------------------------------------
void
QuarkContainerMDSvc::notifyListeners(IContainerMD* obj,
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
QuarkContainerMDSvc::getFirstFreeId()
{
  return mUnifiedInodeProvider->getFirstFreeContainerId();
}

//------------------------------------------------------------------------------
//! Retrieve MD cache statistics.
//------------------------------------------------------------------------------
CacheStatistics
QuarkContainerMDSvc::getCacheStatistics()
{
  return mMetadataProvider->getContainerMDCacheStats();
}

//------------------------------------------------------------------------------
// Blacklist IDs below the given threshold
//------------------------------------------------------------------------------
void
QuarkContainerMDSvc::blacklistBelow(ContainerIdentifier id)
{
  mUnifiedInodeProvider->blacklistContainerId(id.getUnderlyingUInt64());
}

EOSNSNAMESPACE_END
