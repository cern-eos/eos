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
#include "namespace/utils/DataHelper.hh"
#include "ContainerMd.pb.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

// Static global variable
static std::string sBkndHost;
static std::uint32_t sBkndPort;
static long long int sAsyncBatch = 128 * 256 - 1;
static qclient::QClient* sQcl;
static qclient::AsyncHandler sAh;

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
  // Populate the protobuf object which is used later or in the serialization
  // step
  mCont.set_id(pId);
  mCont.set_parent_id(pParentId);
  mCont.set_uid(pCUid);
  mCont.set_gid(pCGid);
  // This must be updated when adding files
  //mCont.set_tree_size(pTreeSize);
  mCont.set_mode(pMode);
  mCont.set_flags(pFlags);
  mCont.set_acl_id(pACLId);
  mCont.set_name(pName);
  mCont.set_ctime(&pCTime, sizeof(pCTime));
  mCont.set_mtime(&pCTime, sizeof(pCTime));
  mCont.set_stime(&pCTime, sizeof(pCTime));
  mCont.clear_xattrs();

  for (auto& xattr : pXAttrs) {
    (*mCont.mutable_xattrs())[xattr.first] = xattr.second;
  }
}

//------------------------------------------------------------------------------
// Add container to the KV store
//------------------------------------------------------------------------------
void
ConvertContainerMD::addContainer(eos::IContainerMD* container)
{
  try {
    sAh.Register(pDirsMap.hset_async(container->getName(), container->getId()),
                 pDirsMap.getClient());
  } catch (std::runtime_error& qdb_err) {
    MDException e(EINVAL);
    e.getMessage() << "Failed to add subcontainer #" << container->getId()
                   << " or KV-backend connection error";
    throw e;
  }

  pSubContainers[container->getName()] = container->getId();
}

//------------------------------------------------------------------------------
// Add file to container in the KV store
//------------------------------------------------------------------------------
void
ConvertContainerMD::addFile(eos::IFileMD* file)
{
  try {
    sAh.Register(pFilesMap.hset_async(file->getName(), file->getId()),
                 pFilesMap.getClient());
  } catch (std::runtime_error& qdb_err) {
    MDException e(EINVAL);
    e.getMessage() << "File #" << file->getId() << " already exists or"
                   << " KV-backend conntection error";
    throw e;
  }

  std::lock_guard<std::mutex> scope_lock(mMutexFiles);
  pFiles[file->getName()] = file->getId();
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
std::shared_ptr<IFileMD>
ConvertContainerMD::findFile(const std::string& name)
{
  std::lock_guard<std::mutex> scop_lock(mMutexFiles);
  return eos::ContainerMD::findFile(name);
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ConvertContainerMD::serialize(std::string& buffer)
{
  // Align the buffer to 4 bytes to efficiently compute the checksum
  size_t obj_size = mCont.ByteSizeLong();
  uint32_t align_size = (obj_size + 3) >> 2 << 2;
  size_t sz = sizeof(align_size);
  size_t msg_size = align_size + 2 * sz;
  buffer.resize(msg_size);
  // Write the checksum value, size of the raw protobuf object and then the
  // actual protobuf object serialized
  const char* ptr = buffer.c_str() + 2 * sz;
  google::protobuf::io::ArrayOutputStream aos((void*)ptr, align_size);

  if (!mCont.SerializeToZeroCopyStream(&aos)) {
    MDException ex(EIO);
    ex.getMessage() << "Failed while serializing buffer";
    throw ex;
  }

  // Compute the CRC32C checksum
  uint32_t cksum = DataHelper::computeCRC32C((void*)ptr, align_size);
  cksum = DataHelper::finalizeCRC32C(cksum);
  // Point to the beginning to fill in the checksum and size of useful data
  ptr = buffer.c_str();
  (void) memcpy((void*)ptr, &cksum, sz);
  ptr += sz;
  (void) memcpy((void*)ptr, &obj_size, sz);
}


//------------------------------------------------------------------------------
//         ************* ConvertContainerMDSvc Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertContainerMDSvc::ConvertContainerMDSvc():
  ChangeLogContainerMDSvc(), mFirstFreeId(0), mConvQView(nullptr)
{}


//------------------------------------------------------------------------------
// Load the container
//------------------------------------------------------------------------------
void ConvertContainerMDSvc::loadContainer(IdMap::iterator& it)
{
  Buffer buffer;
  pChangeLog->readRecord(it->second.logOffset, buffer);
  std::shared_ptr<IContainerMD> container =
    std::make_shared<ConvertContainerMD>(IContainerMD::id_t(0), pFileSvc, this);
  container->deserialize(buffer);
  it->second.ptr = container;
}


//------------------------------------------------------------------------------
// Convert the containers
//------------------------------------------------------------------------------
void
ConvertContainerMDSvc::recreateContainer(IdMap::iterator& it,
    ContainerList& orphans,
    ContainerList& nameConflicts)
{
  std::shared_ptr<IContainerMD> container = it->second.ptr;
  ConvertContainerMD* tmp_cmd =
    dynamic_cast<ConvertContainerMD*>(container.get());

  if (tmp_cmd) {
    tmp_cmd->updateInternal();
  } else {
    std::cerr << __FUNCTION__ << "Error: failed dynamic cast" << std::endl;
    exit(1);
  }

  it->second.attached = true;

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

      if ((container->getFlags() & QUOTA_NODE_FLAG) != 0) {
        mConvQView->addQuotaNode(container->getId());
      }
    } else {
      nameConflicts.push_back(child);
      parent->addContainer(container.get());
    }
  } else {
    // This is not the root container but doesn't have a parrent - add it to
    // the list of orphans
    if (container->getId()) {
      orphans.push_back(container);
      return;
    }
  }
}

//------------------------------------------------------------------------------
// Commit all the container info to the backend.
//------------------------------------------------------------------------------
void
ConvertContainerMDSvc::CommitToBackend()
{
  std::shared_ptr<IContainerMD> container;
  static std::uint64_t total = getNumContainers();
  static std::int64_t count = 0;

  for (auto it = pIdMap.begin(); it != pIdMap.end(); ++it) {
    container = it->second.ptr;

    // Skip unattached
    if (!it->second.attached) {
      std::cerr << __FUNCTION__ << " Skipping unattached container id "
                << it->first << std::endl;
      continue;
    }

    // Add container to the KV store
    try {
      if (getFirstFreeId() <= container->getId()) {
        mFirstFreeId = container->getId() + 1;
      }

      ++count;
      std::string buffer;
      (static_cast<eos::ConvertContainerMD*>(container.get()))->serialize(buffer);
      std::string sid = stringify(container->getId());
      qclient::QHash bucket_map(*sQcl, getBucketKey(container->getId()));
      sAh.Register(bucket_map.hset_async(sid, buffer), bucket_map.getClient());

      if ((count & sAsyncBatch) == 0) {
        if (!sAh.WaitForAtLeast(sAsyncBatch)) {
          std::cerr << __FUNCTION__ << " Got error response from the backend"
                    << std::endl;
          exit(1);
        }

        std::cout << "Processed " << count << "/" << total << " directories "
                  << std::endl;
      }
    } catch (std::runtime_error& qdb_err) {
      MDException e(ENOENT);
      e.getMessage() << "Container #" << container->getId()
                     << " failed to contact backend";
      throw e;
    }
  }
}


//------------------------------------------------------------------------------
// Set quota view object reference
//------------------------------------------------------------------------------
void
ConvertContainerMDSvc::setQuotaView(ConvertQuotaView* qview)
{
  mConvQView = qview;
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
// Get first free container id
//------------------------------------------------------------------------------
IContainerMD::id_t
ConvertContainerMDSvc::getFirstFreeId()
{
  return mFirstFreeId;
}

//------------------------------------------------------------------------------
//         ************* ConvertFileMDSvc Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertFileMDSvc::ConvertFileMDSvc():
  ChangeLogFileMDSvc(), mFirstFreeId(0), mConvQView(nullptr),
  mConvFsView(nullptr), mCount(0), mSyncTimeAcc(nullptr), mContAcc(nullptr)
{}

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
  pChangeLog->open(pChangeLogPath, logOpenFlags, FILE_LOG_MAGIC);
  pFollowStart = pChangeLog->getFirstOffset();
  FileMDScanner scanner(pIdMap, pSlaveMode);
  pFollowStart = pChangeLog->scanAllRecords(&scanner);
  std::uint64_t total = pIdMap.size();
  std::time_t start = std::time(nullptr);

  // Recreate the files
  for (auto && elem : pIdMap) {
    ++mCount;

    if ((mCount & sAsyncBatch) == 0) {
      if (!sAh.WaitForAtLeast(sAsyncBatch)) {
        std::cerr << __FUNCTION__ << " Got error response from the backend"
                  << std::endl;
        exit(1);
      }

      // Get the rate
      auto now = std::time(nullptr);

      if (now != (std::time_t)(-1)) {
        std::chrono::seconds duration(now - start);

        if (duration.count()) {
          double rate = (double)mCount / duration.count();
          std::cout << "Processed " << mCount << "/" << total << " files at "
                    << rate << " Hz" << std::endl;
        }
      }
    }

    // Unpack the serialized buffers
    std::shared_ptr<IFileMD> file = std::make_shared<FileMD>(0, this);

    if (eos::FileMD* tmp_fmd = dynamic_cast<FileMD*>(file.get())) {
      tmp_fmd->deserialize(*elem.second.buffer);
    }

    // Attach to the hierarchy
    if (file->getContainerId() == 0) {
      continue;
    }

    {
      // Update the first free id
      std::lock_guard<std::mutex> scope_lock(mMutexFreeId);

      if (getFirstFreeId() <= file->getId()) {
        mFirstFreeId = file->getId() + 1;
      }
    }

    // Add file to the KV store
    try {
      std::string buffer(elem.second.buffer->getDataPtr(),
                         elem.second.buffer->getSize());
      std::string sid = stringify(file->getId());
      qclient::QHash bucket_map(*sQcl, getBucketKey(file->getId()));
      sAh.Register(bucket_map.hset_async(sid, buffer), bucket_map.getClient());
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
      // Populate the FileSystemView and QuotaView
      mConvQView->addQuotaInfo(file.get());
      mConvFsView->addFileInfo(file.get());

      // Propagate mtime and size up the tree
      if (mSyncTimeAcc && mContAcc) {
        mSyncTimeAcc->QueueForUpdate(file->getContainerId());
        mContAcc->QueueForUpdate(file->getContainerId(), file->getSize(),
                                 eos::ContainerAccounting::OpType::FILE);

        // Update every 1M files
        if (mCount % 1000000 == 0) {
          mSyncTimeAcc->PropagateUpdates();
          mContAcc->PropagateUpdates();
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Get first free container id
//------------------------------------------------------------------------------
IFileMD::id_t
ConvertFileMDSvc::getFirstFreeId()
{
  return mFirstFreeId;
}

//------------------------------------------------------------------------------
// Set quota view object reference
//------------------------------------------------------------------------------
void
ConvertFileMDSvc::setViews(ConvertQuotaView* qview, ConvertFsView* fsview)
{
  mConvQView = qview;
  mConvFsView = fsview;
}

//------------------------------------------------------------------------------
//         ************* ConvertQuotaView Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Add quota node for a specific container
//------------------------------------------------------------------------------
void
ConvertQuotaView::addQuotaNode(IContainerMD::id_t id)
{
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);
  mSetQuotaIds.insert(stringify(id));
}

//------------------------------------------------------------------------------
// Add quota info for a specific file object
//------------------------------------------------------------------------------
void
ConvertQuotaView::addQuotaInfo(IFileMD* file)
{
  // Search for a quota node
  std::shared_ptr<IContainerMD> current =
    mContSvc->getContainerMD(file->getContainerId());

  while ((current->getId() != 1) &&
         ((current->getFlags() & QUOTA_NODE_FLAG) == 0)) {
    current = mContSvc->getContainerMD(current->getParentId());
  }

  if ((current->getFlags() & QUOTA_NODE_FLAG) == 0) {
    return;
  }

  // Compute physical size
  std::string sid = stringify(current->getId());
  eos::IFileMD::layoutId_t lid = file->getLayoutId();
  const int64_t size = file->getSize() *
                       eos::common::LayoutId::GetSizeFactor(lid);
  // Add current file to the the quota map
  const std::string suid = stringify(file->getCUid()) + ":uid";
  const std::string sgid = stringify(file->getCGid()) + ":gid";
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);
  auto it_map = mQuotaMap.find(sid);

  if (it_map == mQuotaMap.end()) {
    auto pair = mQuotaMap.emplace(sid, std::make_pair(QuotaNodeMapT(),
                                  QuotaNodeMapT()));
    it_map = pair.first;
  }

  QuotaNodeMapT& map_uid = it_map->second.first;
  QuotaNodeMapT& map_gid = it_map->second.second;
  eos::IQuotaNode::UsageInfo& user = map_uid[suid];
  eos::IQuotaNode::UsageInfo& group = map_gid[sgid];
  user.physicalSpace += size;
  group.physicalSpace += size;
  user.space += file->getSize();
  group.space += file->getSize();
  user.files++;
  group.files++;
}

//------------------------------------------------------------------------------
// Export container info to the quota view
//------------------------------------------------------------------------------
void
ConvertQuotaView::commitToBackend()
{
  qclient::QSet set_quotaids(*sQcl, quota::sSetQuotaIds);
  eos::common::RWMutexReadLock rd_lock(mRWMutex);

  // Export the set of quota nodes
  // TODO (esindril): add sadd with multiple entries
  for (auto& elem : mSetQuotaIds) {
    sAh.Register(set_quotaids.sadd_async(elem), set_quotaids.getClient());
  }

  mSetQuotaIds.clear();
  // Export the actual quota information
  std::string uid_key, gid_key;

  for (auto it = mQuotaMap.begin(); it != mQuotaMap.end(); ++it) {
    uid_key = it->first + quota::sQuotaUidsSuffix;
    gid_key = it->first + quota::sQuotaGidsSuffix;
    QuotaNodeMapT& uid_map = it->second.first;
    QuotaNodeMapT& gid_map = it->second.second;
    qclient::QHash quota_map(*sQcl, uid_key);

    for (auto& elem : uid_map) {
      eos::IQuotaNode::UsageInfo& info = elem.second;
      std::string field = elem.first + quota::sPhysicalSpaceTag;
      sAh.Register(quota_map.hset_async(field, info.physicalSpace),
                   quota_map.getClient());
      field = elem.first + quota::sSpaceTag;
      sAh.Register(quota_map.hset_async(field, info.space), quota_map.getClient());
      field = elem.first + quota::sFilesTag;
      sAh.Register(quota_map.hset_async(field, info.files), quota_map.getClient());
    }

    quota_map.setKey(gid_key);

    for (auto& elem : gid_map) {
      eos::IQuotaNode::UsageInfo& info = elem.second;
      std::string field = elem.first + quota::sPhysicalSpaceTag;
      sAh.Register(quota_map.hset_async(field, info.physicalSpace),
                   quota_map.getClient());
      field = elem.first + quota::sSpaceTag;
      sAh.Register(quota_map.hset_async(field, info.space),
                   quota_map.getClient());
      field = elem.first + quota::sFilesTag;
      sAh.Register(quota_map.hset_async(field, info.files), quota_map.getClient());
    }
  }

  if (!sAh.Wait()) {
    std::cerr << __FUNCTION__ << " Got error response from the backend "
              << "while exporting the quota view" << std::endl;
    exit(1);
  } else {
    std::cout << "Quota view successfully commited" << std::endl;
  }
}

//------------------------------------------------------------------------------
//         ************* ConvertFsView Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Add file info to the file system view
//------------------------------------------------------------------------------
void
ConvertFsView::addFileInfo(IFileMD* file)
{
  IFileMD::LocationVector loc_vect = file->getLocations();
  std::string key, val;
  std::string selem;
  std::lock_guard<std::mutex> scope_lock(mMutex);

  for (const auto& elem : loc_vect) {
    // Store fsid if it doesn't exist
    selem = stringify(elem);
    // First is the set of replica file ids
    mFsView[selem].first.insert(stringify(file->getId()));
  }

  IFileMD::LocationVector unlink_vect = file->getUnlinkedLocations();

  for (const auto& elem : unlink_vect) {
    selem = stringify(elem);
    // Second is the set of unlinked file ids
    mFsView[selem].second.insert(stringify(file->getId()));
  }

  if ((file->getNumLocation() == 0) && (file->getNumUnlinkedLocation() == 0)) {
    mFileNoReplica.insert(stringify(file->getId()));
  }
}

//------------------------------------------------------------------------------
// Commit all of the fs view information to the backend
//------------------------------------------------------------------------------
void
ConvertFsView::commitToBackend()
{
  std::string key, val;
  qclient::QSet fs_set(*sQcl, "");
  std::list<std::string> lst_elem;

  for (const auto& fs_elem : mFsView) {
    key = fsview::sSetFsIds;
    val = stringify(fs_elem.first);
    fs_set.setKey(key);
    sAh.Register(fs_set.sadd_async(val), fs_set.getClient());
    // Add file to corresponding fs file set
    key = val + fsview::sFilesSuffix;
    fs_set.setKey(key);

    if (fs_elem.second.first.size()) {
      lst_elem.clear();
      lst_elem.assign(fs_elem.second.first.begin(), fs_elem.second.first.end());

      if (fs_set.sadd(lst_elem) != (long long int)lst_elem.size()) {
        std::cerr << "Error whlie doing bulk sadd operations!" << std::endl;
        exit(1);
      }
    }

    key = val + fsview::sUnlinkedSuffix;
    fs_set.setKey(key);

    if (fs_elem.second.second.size()) {
      lst_elem.clear();
      lst_elem.assign(fs_elem.second.second.begin(), fs_elem.second.second.end());

      if (fs_set.sadd(lst_elem) != (long long int)lst_elem.size()) {
        std::cerr << "Error whlie doing bulk sadd operations!" << std::endl;
        exit(1);
      }
    }
  }

  fs_set.setKey(fsview::sNoReplicaPrefix);
  lst_elem.clear();
  lst_elem.assign(mFileNoReplica.begin(), mFileNoReplica.end());

  if (fs_set.sadd(lst_elem) != (long long int)lst_elem.size()) {
    std::cerr << "Error whlie doing bulk sadd operations!" << std::endl;
    exit(1);
  }

  // Wait for all in-flight async requests
  if (!sAh.Wait()) {
    std::cerr << __FUNCTION__ << " Got error response from the backend"
              << std::endl;
    exit(1);
  } else {
    std::cout << "FileSystem view successfully commited" << std::endl;
  }
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
  if (argc != 5) {
    usage();
    return 1;
  }

  try {
    std::string file_chlog(argv[1]);
    std::string dir_chlog(argv[2]);
    sBkndHost = argv[3];
    sBkndPort = std::stoull(argv[4]);
    sQcl = eos::BackendClient::getInstance(sBkndHost, sBkndPort);
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
    std::cout << "Initialize the container meta-data service" << std::endl;
    cont_svc->setFileMDService(file_svc.get());
    cont_svc->configure(config_cont);
    // Create the quota view object
    std::unique_ptr<eos::ConvertQuotaView> quota_view
    (new eos::ConvertQuotaView(sQcl, cont_svc.get(), file_svc.get()));
    std::unique_ptr<eos::ConvertFsView> fs_view(new eos::ConvertFsView());
    eos::ConvertContainerMDSvc* conv_cont_svc =
      dynamic_cast<eos::ConvertContainerMDSvc*>(cont_svc.get());
    eos::ConvertFileMDSvc* conv_file_svc =
      dynamic_cast<eos::ConvertFileMDSvc*>(file_svc.get());

    if (!conv_cont_svc || !conv_file_svc) {
      std::cerr << "Not convert meta-data service type" << std::endl;
      exit(-1);
    }

    conv_cont_svc->setQuotaView(quota_view.get());
    conv_file_svc->setViews(quota_view.get(), fs_view.get());
    std::time_t cont_start = std::time(nullptr);
    cont_svc->initialize();
    std::chrono::seconds cont_duration {std::time(nullptr) - cont_start};
    std::cout << "Container init: " << cont_duration.count() << " seconds" <<
              std::endl;
    // Initialize the file meta-data service
    std::cout << "Initialize the file meta-data service" << std::endl;
    file_svc->setContMDService(cont_svc.get());
    // Create views for sync time and tree size propagation
    eos::common::RWMutex dummy_ns_mutex;
    eos::IContainerMDChangeListener* sync_view =
      new eos::SyncTimeAccounting(conv_cont_svc, &dummy_ns_mutex, 0);
    eos::IFileMDChangeListener* cont_acc =
      new eos::ContainerAccounting(conv_cont_svc, &dummy_ns_mutex, 0);
    conv_file_svc->setSyncTimeAcc(sync_view);
    conv_file_svc->setContainerAcc(cont_acc);
    conv_file_svc->configure(config_file);
    std::time_t file_start = std::time(nullptr);
    file_svc->initialize();

    // Wait for all in-flight async requests
    if (!sAh.Wait()) {
      std::cerr << __FUNCTION__ << " Got error response from the backend"
                << std::endl;
      exit(1);
    }

    std::chrono::seconds file_duration {std::time(nullptr) - file_start};
    std::cout << "File init: " << file_duration.count() << " seconds" << std::endl;
    std::cout << "Commit quota and file system view ..." << std::endl;
    std::time_t views_start = std::time(nullptr);
    quota_view->commitToBackend();
    fs_view->commitToBackend();
    std::chrono::seconds views_duration {std::time(nullptr) - views_start};
    std::cout << "Quota+FsView init: " << views_duration.count() << " seconds" <<
              std::endl;
    // Commit the directory information to the backend
    std::cout << "Commit container info to backend: " << std::endl;

    try {
      conv_cont_svc->CommitToBackend();
    } catch (eos::MDException& e) {
      std::cerr << "Exception thrown: " << e.what() << std::endl;
      return 1;
    }

    // Save the first free file and container id in the meta_hmap - actually it is
    // the last id since we get the first free id by doing a hincrby operation
    qclient::QHash meta_map {*sQcl, eos::constants::sMapMetaInfoKey};
    meta_map.hset(eos::constants::sFirstFreeFid, file_svc->getFirstFreeId() - 1);
    meta_map.hset(eos::constants::sFirstFreeCid, cont_svc->getFirstFreeId() - 1);
  } catch (std::runtime_error& e) {
    std::cerr << "Exception thrown: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
