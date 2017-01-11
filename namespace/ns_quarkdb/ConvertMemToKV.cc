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

#include "namespace/ns_quarkdb/ConvertMemToKV.hh"
#include "common/LayoutId.hh"
#include "namespace/Constants.hh"
#include "namespace/ns_in_memory/FileMD.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogConstants.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/utils/StringConvertion.hh"

// Static global variables
static qclient::QClient* sQcl;

EOSNSNAMESPACE_BEGIN

std::uint64_t ConvertContainerMDSvc::sNumContBuckets = 128 * 1024;
std::uint64_t ConvertFileMDSvc::sNumFileBuckets = 1024 * 1024;

//------------------------------------------------------------------------------
//           ************* ConvertContainerMD Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertContainerMD::ConvertContainerMD(id_t id, IFileMDSvc* file_svc,
                                       IContainerMDSvc* cont_svc)
  : eos::ContainerMD(id, file_svc, cont_svc)
{
  pFilesKey = stringify(id) + constants::sMapFilesSuffix;
  pDirsKey = stringify(id) + constants::sMapDirsSuffix;
  pFilesMap = qclient::QHash(*sQcl, pFilesKey);
  pDirsMap = qclient::QHash(*sQcl, pDirsKey);
}

//------------------------------------------------------------------------------
// Update internal state
//------------------------------------------------------------------------------
void
ConvertContainerMD::updateInternal()
{
  pFilesKey = stringify(pId) + constants::sMapFilesSuffix;
  pDirsKey = stringify(pId) + constants::sMapDirsSuffix;
  pFilesMap.setKey(pFilesKey);
  pDirsMap.setKey(pDirsKey);
}

//------------------------------------------------------------------------------
// Add container to the KV store
//------------------------------------------------------------------------------
void
ConvertContainerMD::addContainer(eos::IContainerMD* container)
{
  try {
    pDirsMap.hset(container->getName(), container->getId());
  } catch (std::runtime_error& qdb_err) {
    MDException e(EINVAL);
    e.getMessage() << "Failed to add subcontainer #" << container->getId()
                   << " or KV-backend connection error";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Add file to container in the KV store
//------------------------------------------------------------------------------
void
ConvertContainerMD::addFile(eos::IFileMD* file)
{
  try {
    pFilesMap.hset(file->getName(), file->getId());
  } catch (std::runtime_error& qdb_err) {
    MDException e(EINVAL);
    e.getMessage() << "File #" << file->getId() << " already exists or"
                   << " KV-backend conntection error";
    throw e;
  }
}

//------------------------------------------------------------------------------
//         ************* ConvertContainerMDSvc Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Convert the containers
//------------------------------------------------------------------------------
void
ConvertContainerMDSvc::recreateContainer(IdMap::iterator& it,
    ContainerList& orphans,
    ContainerList& nameConflicts)
{
  eos::Buffer ebuff;
  pChangeLog->readRecord(it->second.logOffset, ebuff);
  std::shared_ptr<IContainerMD> container =
    std::make_shared<ConvertContainerMD>(IContainerMD::id_t(0), pFileSvc,
                                         this);
  dynamic_cast<ConvertContainerMD*>(container.get())->deserialize(ebuff);
  dynamic_cast<ConvertContainerMD*>(container.get())->updateInternal();
  it->second.ptr = container;

  // For non-root containers recreate the parent
  if (container->getId() != container->getParentId()) {
    IdMap::iterator parentIt = pIdMap.find(container->getParentId());

    if (parentIt == pIdMap.end()) {
      orphans.push_back(container);
      return;
    }

    if (!(parentIt->second.ptr)) {
      recreateContainer(parentIt, orphans, nameConflicts);
    }

    std::shared_ptr<IContainerMD> parent = parentIt->second.ptr;
    std::shared_ptr<IContainerMD> child = parent->findContainer(
                                            container->getName());

    if (!child) {
      parent->addContainer(container.get());
      exportToQuotaView(container.get());
    } else {
      nameConflicts.push_back(child);
      parent->addContainer(container.get());
    }
  }

  // Add container to the KV store
  try {
    std::string buffer(ebuff.getDataPtr(), ebuff.getSize());
    std::string sid = stringify(container->getId());
    qclient::QHash bucket_map(*sQcl, getBucketKey(container->getId()));
    bucket_map.hset(sid, buffer);
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "Container #" << container->getId()
                   << " failed to contact backend";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Export container info to the quota view
//------------------------------------------------------------------------------
void
ConvertContainerMDSvc::exportToQuotaView(IContainerMD* cont)
{
  if ((cont->getFlags() & QUOTA_NODE_FLAG) != 0) {
    qclient::QSet set_quotaids(*sQcl, quota::sSetQuotaIds);
    set_quotaids.sadd(cont->getId());
  }
}

//------------------------------------------------------------------------------
// Get container bucket
//------------------------------------------------------------------------------
std::string
ConvertContainerMDSvc::getBucketKey(IContainerMD::id_t id) const
{
  if (id >= sNumContBuckets) {
    id = id & (sNumContBuckets - 1);
  }

  std::string bucket_key = stringify(id);
  bucket_key += constants::sContKeySuffix;
  return bucket_key;
}

//------------------------------------------------------------------------------
//         ************* ConvertFileMDSvc Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get file bucket
//------------------------------------------------------------------------------
std::string
ConvertFileMDSvc::getBucketKey(IContainerMD::id_t id) const
{
  if (id >= sNumFileBuckets) {
    id = id & (sNumFileBuckets - 1);
  }

  std::string bucket_key = stringify(id);
  bucket_key += constants::sFileKeySuffix;
  return bucket_key;
}

//------------------------------------------------------------------------------
// Initialize the file service
//------------------------------------------------------------------------------
void
ConvertFileMDSvc::initialize()
{
  pIdMap.resize(pResSize);

  if (pContSvc == nullptr) {
    MDException e(EINVAL);
    e.getMessage() << "ConvertFileMDSvc: container service not set";
    throw e;
  }

  int logOpenFlags = ChangeLogFile::Create | ChangeLogFile::Append;
  // Rescan the change log if needed
  pChangeLog->open(pChangeLogPath, logOpenFlags, FILE_LOG_MAGIC);
  pFollowStart = pChangeLog->getFirstOffset();
  FileMDScanner scanner(pIdMap, pSlaveMode);
  pFollowStart = pChangeLog->scanAllRecords(&scanner);
  pFirstFreeId = scanner.getLargestId() + 1;

  // Recreate the files
  for (auto && elem : pIdMap) {
    // Unpack the serialized buffers
    std::shared_ptr<IFileMD> file = std::make_shared<FileMD>(0, this);
    dynamic_cast<FileMD*>(file.get())->deserialize(*elem.second.buffer);

    // Attach to the hierarchy
    if (file->getContainerId() == 0) {
      continue;
    }

    // Add file to the KV store
    try {
      std::string buffer(elem.second.buffer->getDataPtr(),
                         elem.second.buffer->getSize());
      std::string sid = stringify(file->getId());
      qclient::QHash bucket_map(*sQcl, getBucketKey(file->getId()));
      bucket_map.hset(sid, buffer);
    } catch (std::runtime_error& qdb_err) {
      MDException e(ENOENT);
      e.getMessage() << "File #" << file->getId() << " failed to contact backend";
      throw e;
    }

    // Free the memory used by the buffer
    delete elem.second.buffer;
    elem.second.buffer = nullptr;
    std::shared_ptr<IContainerMD> cont;

    try {
      cont = pContSvc->getContainerMD(file->getContainerId());
    } catch (MDException& e) {
      cont = nullptr;
    }

    if (!cont) {
      attachBroken("orphans", file.get());
      continue;
    }

    if (cont->findFile(file->getName())) {
      attachBroken("name_conflicts", file.get());
      continue;
    } else {
      cont->addFile(file.get());
      // Populate the FilsSystemView and QuotaView
      exportToFsView(file.get());
      exportToQuotaView(file.get());
    }
  }
}

//------------------------------------------------------------------------------
// Export file info to the file-system view
//------------------------------------------------------------------------------
void
ConvertFileMDSvc::exportToFsView(IFileMD* file)
{
  IFileMD::LocationVector loc_vect = file->getLocations();
  std::string key, val;
  qclient::QSet fs_set(*sQcl, "");

  for (const auto& elem : loc_vect) {
    // Store fsid if it doesn't exist
    key = fsview::sSetFsIds;
    val = stringify(elem);
    fs_set.setKey(key);

    if (!fs_set.sismember(val)) {
      fs_set.sadd(val);
    }

    // Add file to corresponding fs file set
    key = val + fsview::sFilesSuffix;
    fs_set.setKey(key);
    fs_set.sadd(stringify(file->getId()));
  }

  IFileMD::LocationVector unlink_vect = file->getUnlinkedLocations();

  for (const auto& elem : unlink_vect) {
    key = stringify(elem) + fsview::sUnlinkedSuffix;
    fs_set.setKey(key);
    fs_set.sadd(stringify(file->getId()));
  }

  fs_set.setKey(fsview::sNoReplicaPrefix);

  if ((file->getNumLocation() == 0) && (file->getNumUnlinkedLocation() == 0)) {
    fs_set.sadd(stringify(file->getId()));
  }
}

//------------------------------------------------------------------------------
// Export file info to the quota view
//------------------------------------------------------------------------------
void
ConvertFileMDSvc::exportToQuotaView(IFileMD* file)
{
  // Search for a quota node
  std::shared_ptr<IContainerMD> current =
    pContSvc->getContainerMD(file->getContainerId());

  while ((current->getId() != 1) &&
         ((current->getFlags() & QUOTA_NODE_FLAG) == 0)) {
    current = pContSvc->getContainerMD(current->getParentId());
  }

  if ((current->getFlags() & QUOTA_NODE_FLAG) == 0) {
    return;
  }

  // Add current file to the hmap contained in the current quota node
  std::string quota_uid_key =
    stringify(current->getId()) + quota::sQuotaUidsSuffix;
  std::string quota_gid_key =
    stringify(current->getId()) + quota::sQuotaGidsSuffix;
  const std::string suid = stringify(file->getCUid());
  const std::string sgid = stringify(file->getCGid());
  // Compute physical size
  eos::IFileMD::layoutId_t lid = file->getLayoutId();
  const int64_t size =
    file->getSize() * eos::common::LayoutId::GetSizeFactor(lid);
  qclient::QHash quota_map(*sQcl, quota_uid_key);
  std::string field = suid + quota::sPhysicalSpaceTag;
  (void)quota_map.hincrby(field, size);
  field = suid + quota::sSpaceTag;
  (void)quota_map.hincrby(field, file->getSize());
  field = suid + quota::sFilesTag;
  (void)quota_map.hincrby(field, 1);
  quota_map.setKey(quota_gid_key);
  field = sgid + quota::sPhysicalSpaceTag;
  (void)quota_map.hincrby(field, size);
  field = sgid + quota::sSpaceTag;
  (void)quota_map.hincrby(field, file->getSize());
  field = sgid + quota::sFilesTag;
  (void)quota_map.hincrby(field, 1);
}

EOSNSNAMESPACE_END

//------------------------------------------------------------------------------
// Print usage information
//------------------------------------------------------------------------------
void
usage()
{
  std::cerr << "Usage:                                            " << std::endl
            << "  ./convert_mem_to_kv <file_chlog> <dir_chlog> <bknd_host> "
            << "<bknd_port>" << std::endl
            << "    file_chlog - file changelog                   " << std::endl
            << "    dir_chlog  - directory changelog              " << std::endl
            << "    bknd_host  - Backend host destination         " << std::endl
            << "    bknd_port  - Backend port destination         "
            << std::endl;
}

//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int
main(int argc, char* argv[])
{
  std::cout << "First line in main()" << std::endl;

  if (argc != 5) {
    usage();
    return 1;
  }

  std::string file_chlog(argv[1]);
  std::string dir_chlog(argv[2]);
  std::string bknd_host(argv[3]);
  std::uint32_t bknd_port(std::stoull(argv[4]));
  sQcl = eos::BackendClient::getInstance(bknd_host, bknd_port);
  // Check file and directory changelog files
  int ret;
  struct stat info = {0};
  std::list<std::string> lst_files{file_chlog, dir_chlog};

  for (auto& fn : lst_files) {
    ret = stat(fn.c_str(), &info);

    if (ret != 0) {
      std::cerr << "Unable to access file: " << fn << std::endl;
      return EIO;
    }
  }

  std::unique_ptr<eos::IFileMDSvc> file_svc(new eos::ConvertFileMDSvc());
  std::unique_ptr<eos::IContainerMDSvc> cont_svc(
    new eos::ConvertContainerMDSvc());
  std::map<std::string, std::string> config_cont{{"changelog_path", dir_chlog},
    {"slave_mode", "false"}};
  std::map<std::string, std::string> config_file{{"changelog_path", file_chlog},
    {"slave_mode", "false"}};
  // Initialize the container meta-data service
  cont_svc->setFileMDService(file_svc.get());
  cont_svc->configure(config_cont);
  cont_svc->initialize();
  // Initialize the file meta-data service
  file_svc->setContMDService(cont_svc.get());
  file_svc->configure(config_file);
  file_svc->initialize();
  // TODO(esindril): save the first free file and container id in the meta_hmap
  return 0;
}
