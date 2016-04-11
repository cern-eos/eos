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
#include "namespace/ns_on_ramcloud/FileMD.hh"
#include "namespace/ns_on_ramcloud/ContainerMD.hh"
#include "namespace/ns_on_ramcloud/RamCloudClient.hh"
#include "namespace/ns_on_ramcloud/persistency/ContainerMDSvc.hh"
#include <memory>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ContainerMDSvc::ContainerMDSvc():
  pQuotaStats(nullptr), pFileSvc(nullptr), pDirsTableName("eos_containers"),
  pDirsTableId(), pMetaTableId()
{}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void ContainerMDSvc::configure(std::map<std::string, std::string>& config)
{
}

//------------------------------------------------------------------------------
// Initizlize the container service
//------------------------------------------------------------------------------
void ContainerMDSvc::initialize()
{
  if (!pFileSvc)
  {
    MDException e(EINVAL);
    e.getMessage() << "No file metadata service set for the container "
		   << "metadata service";
    throw e;
  }

  RAMCloud::RamCloud* client = getRamCloudClient();
  pDirsTableId = client->createTable(pDirsTableName.c_str());
  pMetaTableId = client->createTable(constants::sMetaTableName.c_str());
}

//----------------------------------------------------------------------------
// Get the container metadata information
//----------------------------------------------------------------------------
std::unique_ptr<IContainerMD>
ContainerMDSvc::getContainerMD(IContainerMD::id_t id)
{
  eos::Buffer blob;

  try
  {
    RAMCloud::Buffer bval;
    std::string key = std::to_string(id) + constants::sContKeySuffix;
    RAMCloud::RamCloud* client = getRamCloudClient();
    client->read(pDirsTableId, static_cast<const void*>(key.c_str()),
		 key.length(), &bval);
    blob.putData(bval.getOffset<char>(0), bval.size());
  }
  catch (RAMCloud::ClientException& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << id << " not found";
    throw e;
  }

  std::unique_ptr<IContainerMD> cont {new ContainerMD(0, pFileSvc,
				      static_cast<IContainerMDSvc*>(this))};
  static_cast<ContainerMD*>(cont.get())->deserialize(blob);
  fprintf(stderr, "Container name: %s, id=%lu, requested_id=%lu\n",
	  cont->getName().c_str(), cont->getId(), id);
  return cont;
}

//----------------------------------------------------------------------------
// Create a new container metadata object
//----------------------------------------------------------------------------
std::unique_ptr<IContainerMD> ContainerMDSvc::createContainer()
{
  // Get the first free container id
  try {
    RAMCloud::RamCloud* client = getRamCloudClient();
    uint64_t free_id = client->incrementInt64(pMetaTableId,
      static_cast<const void*>(constants::sFirstFreeCid.c_str()),
      constants::sFirstFreeCid.length(), 1);
    std::unique_ptr<IContainerMD> cont {new ContainerMD(free_id, pFileSvc,
      static_cast<IContainerMDSvc*>(this))};
    // Increase total number of containers
    (void) client->incrementInt64(pMetaTableId,
      static_cast<const void*>(constants::sNumConts.c_str()),
      constants::sNumConts.length(), 1);
    return cont;
  }
  catch (RAMCloud::ClientException& e) {
    MDException e(ENOENT);
    e.getMessage() << "Unable to create container";
    throw e;
  }
}

//----------------------------------------------------------------------------
// Update backend store and notify listeners
//----------------------------------------------------------------------------
void ContainerMDSvc::updateStore(IContainerMD* obj)
{
  eos::Buffer buffer;
  dynamic_cast<ContainerMD*>(obj)->serialize(buffer);
  std::string key = std::to_string(obj->getId()) + constants::sContKeySuffix;
  RAMCloud::RamCloud* client = getRamCloudClient();
  client->write(pDirsTableId, static_cast<const void*>(key.c_str()),
		key.length(), static_cast<const void*>(buffer.getDataPtr()),
		buffer.getSize());
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

  std::string key = std::to_string(obj->getId()) + constants::sContKeySuffix;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try
  {
    client->remove(pDirsTableId, static_cast<const void*>(key.c_str()),
		   key.length());
  }
  catch (RAMCloud::ClientException& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << obj->getId() << " not found. ";
    e.getMessage() << "The object was not created in this store!";
    throw e;
  }

  // If this was the root container i.e. id=1 then drop the meta map
  if (obj->getId() == 1)
  {
    client->dropTable(constants::sMetaTableName.c_str());
  }
  else
  {
    // Decrease total number of containers
    (void) client->incrementInt64(pMetaTableId,
      static_cast<const void*>(constants::sNumConts.c_str()),
      constants::sNumConts.length(), -1);
  }

  notifyListeners(obj, IContainerMDChangeListener::Deleted);
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
std::unique_ptr<IContainerMD>
ContainerMDSvc::createInParent(const std::string& name, IContainerMD* parent)
{
  std::unique_ptr<IContainerMD> container = createContainer();
  container->setName(name);
  parent->addContainer(container.get());
  updateStore(container.get());
  return container;
}

//----------------------------------------------------------------------------
// Get the lost+found container, create if necessary
//----------------------------------------------------------------------------
std::unique_ptr<IContainerMD> ContainerMDSvc::getLostFound()
{
  // Get root
  std::unique_ptr<IContainerMD> root;

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
  std::unique_ptr<IContainerMD> lostFound = root->findContainer("lost+found");

  if (lostFound)
  {
    return lostFound;
  }

  lostFound = createInParent("lost+found", root.get());
  return lostFound;
}

//----------------------------------------------------------------------------
// Get the orphans container
//----------------------------------------------------------------------------
std::unique_ptr<IContainerMD>
ContainerMDSvc::getLostFoundContainer(const std::string& name)
{
  std::unique_ptr<IContainerMD> lostFound = getLostFound();

  if (name.empty())
    return lostFound;

  std::unique_ptr<IContainerMD> cont = lostFound->findContainer(name);

  if (cont)
    return cont;

  return createInParent(name, lostFound.get());
}

//------------------------------------------------------------------------------
// Get number of containers
//------------------------------------------------------------------------------
uint64_t ContainerMDSvc::getNumContainers()
{
  uint64_t num_conts = 0;

  try
  {
    RAMCloud::Buffer bval;
    RAMCloud::RamCloud* client = getRamCloudClient();
    client->read(pMetaTableId, static_cast<const void*>(constants::sNumConts.c_str()),
		 constants::sNumConts.length(), &bval);
    num_conts = static_cast<uint64_t>(*bval.getOffset<int64_t>(0));
  }
  catch (std::exception& e) { }

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

EOSNSNAMESPACE_END
