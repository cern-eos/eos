/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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

#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_on_redis/FileMD.hh"
#include "namespace/ns_on_redis/ContainerMD.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"
#include "namespace/utils/StringConvertion.hh"
#include <memory>

EOSNSNAMESPACE_BEGIN

std::uint64_t ContainerMDSvc::sNumContBuckets = 128 * 1024;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMDSvc::ContainerMDSvc():
  pQuotaStats(nullptr), pFileSvc(nullptr), pRedox(nullptr), pRedisHost(""),
  pRedisPort(0), mContainerCache(static_cast<uint64_t>(10e6))
{}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void ContainerMDSvc::configure(std::map<std::string, std::string>& config)
{
  std::string key_host = "redis_host";
  std::string key_port = "redis_port";

  if (config.find(key_host) != config.end())
    pRedisHost = config[key_host];

  if (config.find(key_port) != config.end())
    pRedisPort = std::stoul(config[key_port]);
}

//------------------------------------------------------------------------------
// Initizlize the container service
//------------------------------------------------------------------------------
void ContainerMDSvc::initialize()
{
  pRedox = RedisClient::getInstance(pRedisHost, pRedisPort);

  if (!pFileSvc)
  {
    MDException e(EINVAL);
    e.getMessage() << "No file metadata service set for the container "
		   << "metadata service";
    throw e;
  }
}

//----------------------------------------------------------------------------
// Get the container metadata information
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::getContainerMD(IContainerMD::id_t id)
{
  // Check first in cache
  std::shared_ptr<IContainerMD> cont = mContainerCache.get(id);

  if (cont)
    return cont;

  // If not in cache, then get it from the KV store
  std::string blob;

  try
  {
    std::string sid = stringify(id);
    std::string bucket_key = getBucketKey(id);
    blob = pRedox->hget(bucket_key, sid);
  }
  catch (std::runtime_error& redis_err)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << id << " not found";
    throw e;
  }

  if (blob.empty())
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << id << " not found";
    throw e;
  }

  cont = std::make_shared<ContainerMD>(0, pFileSvc,
				       static_cast<IContainerMDSvc*>(this));
  eos::Buffer ebuff;
  ebuff.putData(blob.c_str(), blob.length());
  static_cast<ContainerMD*>(cont.get())->deserialize(ebuff);
  return mContainerCache.put(cont->getId(), cont);
}

//----------------------------------------------------------------------------
// Create a new container metadata object
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD> ContainerMDSvc::createContainer()
{
  try
  {
    // Get the first free container id
    // TODO: grab 100 ids which are put back in a list is we shut down and
    // haven't used all or them. When a new mgm starts he first cheks this
    // list and grabs any available ids.
    uint64_t free_id = pRedox->hincrby(constants::sMapMetaInfoKey,
				       constants::sFirstFreeCid, 1);
    std::shared_ptr<IContainerMD> cont {
      new ContainerMD(free_id, pFileSvc, static_cast<IContainerMDSvc*>(this))};

    return mContainerCache.put(cont->getId(), cont);
  }
  catch (std::runtime_error& redis_err)
  {
    MDException e(ENOENT);
    e.getMessage() << "Failed to create new container" << std::endl;
    throw e;
  }
}

//----------------------------------------------------------------------------
// Update backend store and notify listeners
//----------------------------------------------------------------------------
void ContainerMDSvc::updateStore(IContainerMD* obj)
{
  eos::Buffer ebuff;
  dynamic_cast<ContainerMD*>(obj)->serialize(ebuff);
  std::string buffer(ebuff.getDataPtr(), ebuff.getSize());

  try
  {
    std::string sid = stringify(obj->getId());
    std::string bucket_key = getBucketKey(obj->getId());
    pRedox->hset(bucket_key, sid, buffer);
  }
  catch (std::runtime_error& redis_err)
  {
    MDException e(ENOENT);
    e.getMessage() << "File #" << obj->getId() << " failed to contact backend";
    throw e;
  }

  notifyListeners(obj, IContainerMDChangeListener::Updated);
}

//----------------------------------------------------------------------------
// Remove object from the store assuming it's already empty
//----------------------------------------------------------------------------
void ContainerMDSvc::removeContainer(IContainerMD* obj)
{
  // Protection in case the container is not empty i.e check that it doesn't
  // hold any files or subcontainers
  if ((obj->getNumFiles() != 0) || (obj->getNumContainers() != 0))
  {
    MDException e(EINVAL);
    e.getMessage() << "Failed to remove container #" << obj->getId()
		   << " since it's not empty";
    throw e;
  }

  try {
    std::string sid = stringify(obj->getId());
    std::string bucket_key = getBucketKey(obj->getId());
    pRedox->hdel(bucket_key, sid);

    // Drop the container from the parent's set of unlinked subcontainers
    pRedox->srem(constants::sSetCheckConts, sid);
  }
  catch (std::runtime_error& redis_err)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << obj->getId() << " not found. "
		   << "The object was not created in this store!";
    throw e;
  }

  // If this was the root container i.e. id=1 then drop the meta map
  if (obj->getId() == 1)
  {
    (void) pRedox->del(constants::sMapMetaInfoKey);
  }

  notifyListeners(obj, IContainerMDChangeListener::Deleted);
  mContainerCache.remove(obj->getId());
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
std::shared_ptr<IContainerMD> ContainerMDSvc::getLostFound()
{
  // Get root
  std::shared_ptr<IContainerMD> root;

  try
  {
    root = getContainerMD(1);
  }
  catch (MDException& e)
  {
    root = createContainer();
    root->setParentId(root->getId());
    updateStore(root.get());
  }

  // Get or create lost+found if necessary
  std::shared_ptr<IContainerMD> lostFound = root->findContainer("lost+found");

  if (!lostFound)
    lostFound = createInParent("lost+found", root.get());

  return lostFound;
}

//----------------------------------------------------------------------------
// Get the orphans container
//----------------------------------------------------------------------------
std::shared_ptr<IContainerMD>
ContainerMDSvc::getLostFoundContainer(const std::string& name)
{
  std::shared_ptr<IContainerMD> lostFound = getLostFound();

  if (name.empty())
    return lostFound;

  std::shared_ptr<IContainerMD> cont = lostFound->findContainer(name);

  if (!cont)
    cont = createInParent(name, lostFound.get());

  return cont;
}

//------------------------------------------------------------------------------
// Get number of containers which is sum(hlen(hash_i)) for i=0,128k
//------------------------------------------------------------------------------
uint64_t ContainerMDSvc::getNumContainers()
{
  std::atomic<std::uint32_t> num_requests{0};
  std::atomic<std::uint64_t> num_conts{0};
  std::string bucket_key("");
  std::mutex mutex;
  std::condition_variable cond_var;
  auto callback_len = [&num_conts, &num_requests, &cond_var]
    (redox::Command<long long int>& c) {
    if (c.ok()) {
      num_conts += c.reply();
    }

    if (!--num_requests)
      cond_var.notify_one();
  };

  auto wrapper_cb = [&num_requests, &callback_len]() -> decltype(callback_len) {
    num_requests++;
    return callback_len;
  };

  for (std::uint64_t i = 0; i < sNumContBuckets; ++i)
  {
    bucket_key = stringify(i);
    bucket_key += constants::sContKeySuffix;

    try {
      pRedox->hlen(bucket_key, wrapper_cb());
    }
    catch (std::runtime_error& redis_err) {
      // no change
    }
  }

  // Wait for all responses
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (num_requests)
      cond_var.wait(lock);
  }

  return num_conts;
}

//------------------------------------------------------------------------------
// Notify the listeners about the change
//------------------------------------------------------------------------------
void ContainerMDSvc::notifyListeners(IContainerMD* obj,
				     IContainerMDChangeListener::Action a)
{
  for (auto it = pListeners.begin(); it != pListeners.end(); ++it)
    (*it)->containerMDChanged(obj, a);
}

//------------------------------------------------------------------------------
// Get container bucket
//------------------------------------------------------------------------------
std::string
ContainerMDSvc::getBucketKey(IContainerMD::id_t id) const
{
  if (id >= sNumContBuckets)
    id = id & (sNumContBuckets - 1);

  std::string bucket_key = stringify(id);
  bucket_key += constants::sContKeySuffix;
  return bucket_key;
}

EOSNSNAMESPACE_END
