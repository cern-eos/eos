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

#include "common/ShellCmd.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_on_redis/FileMD.hh"
#include "namespace/ns_on_redis/ContainerMD.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"
#include <memory>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Initizlize the container service
//------------------------------------------------------------------------------
void ContainerMDSvc::initialize()
{
  pRedox = RedisClient::getInstance();
}

//----------------------------------------------------------------------------
// Configure the container service
//----------------------------------------------------------------------------
void ContainerMDSvc::configure(std::map<std::string, std::string>& config)
{
  // empty
}

//----------------------------------------------------------------------------
// Finalize the container service
//----------------------------------------------------------------------------
void ContainerMDSvc::finalize()
{
  // empty
}

//----------------------------------------------------------------------------
// Get the container metadata information
//----------------------------------------------------------------------------
IContainerMD* ContainerMDSvc::getContainerMD(IContainerMD::id_t id)
{
  std::string blob;
  std::string key = std::to_string(id) + constants::sContKeySuffix;

  try
  {
    blob = pRedox->hget(key, "data");
  }
  catch (std::runtime_error& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << id << " not found";
    throw e;
  }

  ContainerMD* cont {new ContainerMD(0)};
  cont->deserialize(blob);
  return cont;
}

//----------------------------------------------------------------------------
// Create a new container metadata object
//----------------------------------------------------------------------------
IContainerMD* ContainerMDSvc::createContainer()
{
  // Get the first free container id
  uint64_t free_id = pRedox->hincrby(constants::sMapMetaInfoKey,
				     constants::sFieldLastCid, 1);
  IContainerMD* cont = new ContainerMD(free_id);
  return cont;
}

//----------------------------------------------------------------------------
// Update backend store and notify listeners
//----------------------------------------------------------------------------
void ContainerMDSvc::updateStore(IContainerMD* obj)
{
  std::string buffer;
  dynamic_cast<ContainerMD*>(obj)->serialize(buffer);
  std::string key = std::to_string(obj->getId()) + constants::sContKeySuffix;
  pRedox->hset(key, "data", buffer);
  notifyListeners(obj, IContainerMDChangeListener::Updated);
}

//----------------------------------------------------------------------------
// Remove object from the store assuming it's already empty
//----------------------------------------------------------------------------
void ContainerMDSvc::removeContainer(IContainerMD* obj)
{
  std::string key = std::to_string(obj->getId()) + constants::sContKeySuffix;

  try
  {
    pRedox->hdel(key, "data");
  }
  catch (std::runtime_error& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << obj->getId() << " not found. ";
    e.getMessage() << "The object was not created in this store!";
    throw e;
  }

  notifyListeners(obj, IContainerMDChangeListener::Deleted);
}

//----------------------------------------------------------------------------
// Add change listener
//----------------------------------------------------------------------------
void
ContainerMDSvc::addChangeListener(IContainerMDChangeListener* listener)
{
  pListeners.push_back(listener);
}

//------------------------------------------------------------------------
// Create container in parent
//------------------------------------------------------------------------
IContainerMD* ContainerMDSvc::createInParent(const std::string& name,
					     IContainerMD* parent)
{
  IContainerMD* container = createContainer();
  container->setName(name);
  parent->addContainer(container);
  updateStore(container);
  return container;
}

//----------------------------------------------------------------------------
// Get the lost+found container, create if necessary
//----------------------------------------------------------------------------
IContainerMD* ContainerMDSvc::getLostFound()
{
  // Get root
  IContainerMD* root = 0;

  try
  {
    root = getContainerMD(1);
  }
  catch (MDException& e)
  {
    root = createContainer();
    root->setParentId(root->getId());
    updateStore(root);
  }

  // Get or create lost+found if necessary
  IContainerMD* lostFound = root->findContainer("lost+found");

  if (lostFound)
  {
    delete root;
    return lostFound;
  }

  lostFound = createInParent("lost+found", root);
  delete root;
  return lostFound;
}

//----------------------------------------------------------------------------
// Get the orphans container
//----------------------------------------------------------------------------
IContainerMD* ContainerMDSvc::getLostFoundContainer(const std::string& name)
{
  IContainerMD* lostFound = getLostFound();

  if (name.empty())
    return lostFound;

  IContainerMD* cont = lostFound->findContainer(name);

  if (cont)
    return cont;

  return createInParent(name, lostFound);
}

EOSNSNAMESPACE_END
