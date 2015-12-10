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

#include "namespace/ns_on_redis/Constants.hh"
#include "namespace/ns_on_redis/FileMD.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include "namespace/ns_on_redis/persistency/FileMDSvc.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Initizlize the file service
//------------------------------------------------------------------------------
void FileMDSvc::initialize()
{
  if (!pContSvc)
  {
    MDException e(EINVAL);
    e.getMessage() << "FileMDSvc: container service not set";
    throw e;
  }

  pRedox = RedisClient::getInstance();
}

//------------------------------------------------------------------------------
// Configure the file service
//------------------------------------------------------------------------------
void FileMDSvc::configure(
  std::map<std::string, std::string>& config)
{
  // empty
}

//------------------------------------------------------------------------------
// Finalize the file service
//------------------------------------------------------------------------------
void FileMDSvc::finalize()
{
  // empty
}

//------------------------------------------------------------------------------
// Get the file metadata information for the given file ID
//------------------------------------------------------------------------------
IFileMD*
FileMDSvc::getFileMD(IFileMD::id_t id)
{
  std::string blob;
  std::string key = std::to_string(id) + constants::sFileKeySuffix;

  try
  {
    blob = pRedox->hget(key, "data");
  }
  catch (std::runtime_error& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "File #" << id << " not found";
    throw e;
  }

  IFileMD* file {new FileMD(0, this)};
  static_cast<FileMD*>(file)->deserialize(blob);
  file->setFileMDSvc(this);
  return file;
}

//------------------------------------------------------------------------------
// Create new file metadata object
//------------------------------------------------------------------------------
IFileMD* FileMDSvc::createFile()
{
  // Get first available file id
  uint64_t free_id = pRedox->hincrby(constants::sMapMetaInfoKey,
				     constants::sFirstFreeFid, 1);
  // Increase total number of files
  (void) pRedox->hincrby(constants::sMapMetaInfoKey, constants::sNumFiles, 1);
  IFileMD* file {new FileMD(free_id, this)};
  IFileMDChangeListener::Event e(file, IFileMDChangeListener::Created);
  notifyListeners(&e);
  return file;
}

//------------------------------------------------------------------------------
// Update backend store and notify all the listeners
//------------------------------------------------------------------------------
void FileMDSvc::updateStore(IFileMD* obj)
{
  // Store the file in the changelog and notify the listener
  std::string buffer;
  dynamic_cast<FileMD*>(obj)->serialize(buffer);
  std::string key = std::to_string(obj->getId()) + constants::sFileKeySuffix;
  pRedox->hset(key, "data", buffer);
  IFileMDChangeListener::Event e(obj, IFileMDChangeListener::Updated);
  notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Remove object from the store
//------------------------------------------------------------------------------
void FileMDSvc::removeFile(IFileMD* obj)
{
  removeFile(obj->getId());
}

//------------------------------------------------------------------------------
// Remove object from the store
//------------------------------------------------------------------------------
void FileMDSvc::removeFile(FileMD::id_t fileId)
{
  std::string key = std::to_string(fileId) + constants::sFileKeySuffix;

  try
  {
    pRedox->hdel(key, "data");
  }
  catch (std::runtime_error& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "File #" << fileId << " not found. ";
    e.getMessage() << "The object was not created in this store!";
    throw e;
  }

  // Notify the listeners
  IFileMDChangeListener::Event e(fileId, IFileMDChangeListener::Deleted);
  notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Add file listener
//------------------------------------------------------------------------------
void FileMDSvc::addChangeListener(IFileMDChangeListener* listener)
{
  pListeners.push_back(listener);
}

//------------------------------------------------------------------------------
// Set container service
//------------------------------------------------------------------------------
void
FileMDSvc::setContainerService(IContainerMDSvc* cont_svc)
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
// Attach a broken file to lost+found
//------------------------------------------------------------------------------
void FileMDSvc::attachBroken(const std::string& parent,
			     IFileMD* file)
{
  std::ostringstream s1, s2;
  IContainerMD* parentCont = pContSvc->getLostFoundContainer(parent);
  s1 << file->getContainerId();
  IContainerMD* cont = parentCont->findContainer(s1.str());

  if (!cont)
    cont = pContSvc->createInParent(s1.str(), parentCont);

  s2 << file->getName() << "." << file->getId();
  file->setName(s2.str());
  cont->addFile(file);
}

//------------------------------------------------------------------------------
// Get number of files
//------------------------------------------------------------------------------
uint64_t FileMDSvc::getNumFiles()
{
  return std::stoull(pRedox->hget(constants::sMapMetaInfoKey,
				  constants::sNumFiles));
}

EOSNSNAMESPACE_END
