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

#include "namespace/ns_on_ramcloud/Constants.hh"
#include "namespace/ns_on_ramcloud/FileMD.hh"
#include "namespace/ns_on_ramcloud/RamCloudClient.hh"
#include "namespace/ns_on_ramcloud/persistency/FileMDSvc.hh"
#include "namespace/ns_on_ramcloud/accounting/QuotaStats.hh"
#include "namespace/ns_on_ramcloud/persistency/ContainerMDSvc.hh"
#include "namespace/ns_on_ramcloud/RamCloudClient.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMDSvc::FileMDSvc():
  pFilesTableName("eos_files"), pFilesTableId(), pMetaTableId()
{}

//------------------------------------------------------------------------------
// Configure the file service
//------------------------------------------------------------------------------
void FileMDSvc::configure(std::map<std::string, std::string>& config)
{
}

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

  RAMCloud::RamCloud* client = getRamCloudClient();
  pFilesTableId = client->createTable(pFilesTableName.c_str());
  pMetaTableId = client->createTable(constants::sMetaTableName.c_str());
}

//------------------------------------------------------------------------------
// Get the file metadata information for the given file ID
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
FileMDSvc::getFileMD(IFileMD::id_t id)
{
  eos::Buffer blob;

  try
  {
    RAMCloud::Buffer bval;
    RAMCloud::RamCloud* client = getRamCloudClient();
    std::string key = std::to_string(id) + constants::sFileKeySuffix;
    client->read(pFilesTableId, static_cast<const void*>(key.c_str()),
		 key.length(), &bval);

    blob.putData(bval.getOffset<char>(0), bval.size());
  }
  catch (RAMCloud::ClientException& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "File #" << id << " not found";
    throw e;
  }

  std::shared_ptr<IFileMD> file {new FileMD(0, this)};
  static_cast<FileMD*>(file.get())->deserialize(blob);
  return file;
}

//------------------------------------------------------------------------------
// Create new file metadata object
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD> FileMDSvc::createFile()
{
  // Get first available file id
  RAMCloud::RamCloud* client = getRamCloudClient();
  uint64_t free_id = client->incrementInt64(pMetaTableId,
    static_cast<const void*>(constants::sFirstFreeFid.c_str()),
    constants::sFirstFreeFid.length(), 1);

  // Increase total number of files
  (void) client->incrementInt64(pMetaTableId,
     static_cast<const void*>(constants::sNumFiles.c_str()),
     constants::sNumFiles.length(), 1);

  std::shared_ptr<IFileMD> file {new FileMD(free_id, this)};
  IFileMDChangeListener::Event e(file.get(), IFileMDChangeListener::Created);
  notifyListeners(&e);
  return file;
}

//------------------------------------------------------------------------------
// Update backend store and notify all the listeners
//------------------------------------------------------------------------------
void FileMDSvc::updateStore(IFileMD* obj)
{
  eos::Buffer buffer;
  dynamic_cast<FileMD*>(obj)->serialize(buffer);
  std::string key = std::to_string(obj->getId()) + constants::sFileKeySuffix;
  RAMCloud::RamCloud* client = getRamCloudClient();
  client->write(pFilesTableId, static_cast<const void*>(key.c_str()),
		key.length(), static_cast<const void*>(buffer.getDataPtr()),
		buffer.getSize());
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
  RAMCloud::RamCloud* client = getRamCloudClient();
  std::string key = std::to_string(fileId) + constants::sFileKeySuffix;

  try
  {
    client->remove(pFilesTableId, static_cast<const void*>(key.c_str()),
		   key.length());
  }
  catch (RAMCloud::ClientException& e)
  {
    MDException e(ENOENT);
    e.getMessage() << "File #" << fileId << " not found. ";
    e.getMessage() << "The object was not created in this store!";
    throw e;
  }

  // Decrease total number of files
  (void) client->incrementInt64(pMetaTableId,
     static_cast<const void*>(constants::sNumFiles.c_str()),
     constants::sNumFiles.length(), -1);

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
// Notify the listeners about the change
//------------------------------------------------------------------------------
void FileMDSvc::notifyListeners(IFileMDChangeListener::Event* event)
{
  for (auto it = pListeners.begin(); it != pListeners.end(); ++it)
    (*it)->fileMDChanged(event);
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
// Get number of files
//------------------------------------------------------------------------------
uint64_t FileMDSvc::getNumFiles()
{
  uint64_t num_files = 0;

  try
  {
    RAMCloud::Buffer bval;
    RAMCloud::RamCloud* client = getRamCloudClient();
    client->read(pMetaTableId, static_cast<const void*>(constants::sNumFiles.c_str()),
      constants::sNumFiles.length(), &bval);
    num_files = static_cast<uint64_t>(*bval.getOffset<int64_t>(0));
  }
  catch (std::exception& e) {}

  return num_files;
}

//------------------------------------------------------------------------------
// Attach a broken file to lost+found
//------------------------------------------------------------------------------
void FileMDSvc::attachBroken(const std::string& parent, IFileMD* file)
{
  std::ostringstream s1, s2;
  std::shared_ptr<IContainerMD> parentCont = pContSvc->getLostFoundContainer(parent);
  s1 << file->getContainerId();
  std::shared_ptr<IContainerMD> cont = parentCont->findContainer(s1.str());

  if (!cont)
    cont = pContSvc->createInParent(s1.str(), parentCont.get());

  s2 << file->getName() << "." << file->getId();
  file->setName(s2.str());
  cont->addFile(file);
}

EOSNSNAMESPACE_END
