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

#include "namespace/ns_quarkdb/tools/ConvertMemToKV.hh"
#include "common/LayoutId.hh"
#include "common/Parallel.hh"
#include "namespace/Constants.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogConstants.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/utils/StringConvertion.hh"
#include "namespace/utils/DataHelper.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "qclient/structures/QHash.hh"

// Static global variable
static std::string sBkndHost;
static std::int32_t sBkndPort;
static long long int sAsyncBatch = 1023;
static qclient::QClient* sQcl;
static qclient::AsyncHandler sAh;
static size_t sThreads = 1;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//           ************* ConvertFileMD Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertFileMD::ConvertFileMD(IFileMD::id_t id, IFileMDSvc* fileMDSvc):
  eos::FileMD(id, fileMDSvc)
{}

//------------------------------------------------------------------------------
// Update internal state
//------------------------------------------------------------------------------
void
ConvertFileMD::updateInternal()
{
  mFile.set_id(pId);
  mFile.set_cont_id(pContainerId);
  mFile.set_uid(pCUid);
  mFile.set_gid(pCGid);
  mFile.set_size(pSize);
  mFile.set_layout_id(pLayoutId);
  mFile.set_flags(pFlags);
  mFile.set_name(pName);
  mFile.set_link_name(pLinkName);
  mFile.set_ctime(&pCTime, sizeof(pCTime));
  mFile.set_mtime(&pMTime, sizeof(pMTime));
  mFile.set_checksum(pChecksum.getDataPtr(), pChecksum.getSize());

  for (const auto& loc : pLocation) {
    mFile.add_locations(loc);
  }

  for (const auto& unlinked : pUnlinkedLocation) {
    mFile.add_unlink_locations(unlinked);
  }

  for (const auto& xattr : pXAttrs) {
    (*mFile.mutable_xattrs())[xattr.first] = xattr.second;
  }
}

//------------------------------------------------------------------------------
// Serialize the object to an std::string buffer
//------------------------------------------------------------------------------
void
ConvertFileMD::serializeToStr(std::string& buffer)
{
  // Align the buffer to 4 bytes to efficiently compute the checksum
  size_t obj_size = mFile.ByteSizeLong();
  uint32_t align_size = (obj_size + 3) >> 2 << 2;
  size_t sz = sizeof(align_size);
  size_t msg_size = align_size + 2 * sz;
  buffer.resize(msg_size);
  // Write the checksum value, size of the raw protobuf object and then the
  // actual protobuf object serialized
  const char* ptr = buffer.c_str() + 2 * sz;
  google::protobuf::io::ArrayOutputStream aos((void*)ptr, align_size);

  if (!mFile.SerializeToZeroCopyStream(&aos)) {
    MDException ex(EIO);
    ex.getMessage() << "Failed while serializing buffer";
    throw ex;
  }

  // @todo (esindril): Could use compression on the entries
  // Compute the CRC32C checkusm
  uint32_t cksum = DataHelper::computeCRC32C((void*)ptr, align_size);
  cksum = DataHelper::finalizeCRC32C(cksum);
  // Point to the beginning to fill in the checksum and size of useful data
  ptr = buffer.c_str();
  (void) memcpy((void*)ptr, &cksum, sz);
  ptr += sz;
  (void) memcpy((void*)ptr, &obj_size, sz);
}

//------------------------------------------------------------------------------
//           ************* ConvertContainerMD Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertContainerMD::ConvertContainerMD(IContainerMD::id_t id,
                                       IFileMDSvc* file_svc,
                                       IContainerMDSvc* cont_svc)
  : eos::ContainerMD(id, file_svc, cont_svc)
{
  pFilesKey = stringify(id) + constants::sMapFilesSuffix;
  pDirsKey = stringify(id) + constants::sMapDirsSuffix;
}

//------------------------------------------------------------------------------
// Update internal state
//------------------------------------------------------------------------------
void
ConvertContainerMD::updateInternal()
{
  pFilesKey = stringify(pId) + constants::sMapFilesSuffix;
  pDirsKey = stringify(pId) + constants::sMapDirsSuffix;
  mCont.set_tree_size(pTreeSize);
  mCont.set_id(pId);
  mCont.set_parent_id(pParentId);
  mCont.set_uid(pCUid);
  mCont.set_gid(pCGid);
  // Remove S_ISGID which was used as a flag to enable/disable attribute
  // inheritance - attributes are now inherited by default
  pMode ^= S_ISGID;
  mCont.set_mode(pMode);
  mCont.set_flags(pFlags);
  mCont.set_name(pName);
  mCont.set_ctime(&pCTime, sizeof(pCTime));
  mCont.set_mtime(&pCTime, sizeof(pCTime));
  mCont.set_stime(&pCTime, sizeof(pCTime));
  mCont.clear_xattrs();

  for (auto& xattr : pXAttrs) {
    // Convert acls to numeric values
    if ((xattr.first == "sys.acl") || (xattr.first == "user.acl")) {
      convertAclToNumeric(xattr.second);
    }

    (*mCont.mutable_xattrs())[xattr.first] = xattr.second;
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void
ConvertContainerMD::serializeToStr(std::string& buffer)
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
// Commit map of subcontainer to the backend
//------------------------------------------------------------------------------
void
ConvertContainerMD::commitSubcontainers(qclient::AsyncHandler& ah,
                                        qclient::QClient& qclient)
{
  uint32_t num_batches = 0u;
  uint32_t max_batches = 10u;
  uint32_t max_per_batch = 100u;
  uint32_t count = 0u;
  std::vector<std::string> cmd;
  cmd.reserve(max_per_batch * 2 + 2);
  cmd.push_back("HMSET");
  cmd.push_back(pDirsKey);

  for (auto it = mSubcontainers.begin(); it != mSubcontainers.end(); ++it) {
    ++count;
    cmd.push_back(it->first);
    cmd.push_back(stringify(it->second));

    if (count == max_per_batch) {
      ah.Register(&qclient, cmd);
      cmd.clear();
      cmd.reserve(max_per_batch * 2 + 2);
      cmd.push_back("HMSET");
      cmd.push_back(pDirsKey);
      count = 0u;
      ++num_batches;
    }

    if (num_batches == max_batches) {
      num_batches = 0u;

      if (!ah.Wait()) {
        std::cerr << __FUNCTION__ << " Got error response from the backend"
                  << std::endl;
        std::abort();
      }
    }
  }

  if (cmd.size() > 2) {
    ah.Register(&qclient, cmd);
  }
}

//------------------------------------------------------------------------------
// Commit map of files to the backend
//------------------------------------------------------------------------------
void
ConvertContainerMD::commitFiles(qclient::AsyncHandler& ah,
                                qclient::QClient& qclient)
{
  uint32_t num_batches = 0u;
  uint32_t max_batches = 10u;
  uint32_t max_per_batch = 100u;
  uint32_t count = 0u;
  std::vector<std::string> cmd;
  cmd.reserve(max_per_batch * 2 + 2);
  cmd.push_back("HMSET");
  cmd.push_back(pFilesKey);

  for (auto it = mFiles.begin(); it != mFiles.end(); ++it) {
    ++count;
    cmd.push_back(it->first);
    cmd.push_back(stringify(it->second));

    if (count == max_per_batch) {
      ah.Register(&qclient, cmd);
      cmd.clear();
      cmd.reserve(max_per_batch * 2 + 2);
      cmd.push_back("HMSET");
      cmd.push_back(pFilesKey);
      count = 0;
      ++num_batches;
    }

    if (num_batches == max_batches) {
      num_batches = 0u;

      if (!ah.Wait()) {
        std::cerr << __FUNCTION__ << " Got error response from the backend"
                  << std::endl;
        std::abort();
      }
    }
  }

  if (cmd.size() > 2) {
    ah.Register(&qclient, cmd);
  }
}

//------------------------------------------------------------------------------
// Convert ACL to numeric representation of uid/gid(s). This code was taken
// from ~/mgm/Acl.hh/cc.
//------------------------------------------------------------------------------
void
ConvertContainerMD::convertAclToNumeric(std::string& acl_val)
{
  if (acl_val.empty()) {
    return;
  }

  bool is_uid, is_gid;
  std::string sid;
  std::ostringstream oss;
  using eos::common::StringConversion;
  std::vector<std::string> rules;
  StringConversion::Tokenize(acl_val, rules, ",");

  if (!rules.size() && acl_val.length()) {
    rules.push_back(acl_val);
  }

  for (auto& rule : rules) {
    is_uid = is_gid = false;
    std::vector<std::string> tokens;
    StringConversion::Tokenize(rule, tokens, ":");
    eos_static_debug("rule=%s, tokens.size=%i", rule.c_str(), tokens.size());

    if (tokens.size() != 3) {
      oss << rule << ',';
      continue;
    }

    is_uid = (tokens[0] == "u");
    is_gid = (tokens[0] == "g");

    if (!is_uid && !is_gid) {
      oss << rule << ',';
      continue;
    }

    sid = tokens[1];
    bool needs_conversion = false;
    // Convert to numeric representation
    needs_conversion =
      (std::find_if(sid.begin(), sid.end(),
    [](const char& c) {
      return std::isalpha(c);
    }) != sid.end());

    if (needs_conversion) {
      int errc = 0;
      std::uint32_t numeric_id {0};
      std::string string_id {""};

      if (is_uid) {
        numeric_id = eos::common::Mapping::UserNameToUid(sid, errc);
        string_id = std::to_string(numeric_id);
      } else {
        numeric_id = eos::common::Mapping::GroupNameToGid(sid, errc);
        string_id = std::to_string(numeric_id);
      }

      if (errc) {
        oss.str("");
        oss << "failed to convert id: \"" << sid << "\" to numeric format";
        // Print error message and fall-back to uid daemon (2)
        eos_static_err(oss.str().c_str());
        string_id = "2";
      }

      oss << tokens[0] << ':' << string_id << ':' << tokens[2] << ',';
    } else {
      oss << rule << ',';
    }
  }

  acl_val = oss.str();

  if (*acl_val.rbegin() == ',') {
    acl_val.pop_back();
  }
}

//------------------------------------------------------------------------------
//         ************* ConvertContainerMDSvc Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertContainerMDSvc::ConvertContainerMDSvc():
  ChangeLogContainerMDSvc(), mConvQView(nullptr)
{
  int num_mutexes = sThreads;

  while (num_mutexes > 0) {
    --num_mutexes;
    mMutexPool.push_back(new std::mutex());
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ConvertContainerMDSvc::~ConvertContainerMDSvc()
{
  for (auto& elem : mMutexPool) {
    delete elem;
  }

  mMutexPool.clear();
}

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
  it.value().ptr = container;
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
  it.value().attached = true;

  // For non-root containers recreate the parent
  if (container->getId() != container->getParentId()) {
    IdMap::iterator parentIt = pIdMap.find(container->getParentId());

    if (parentIt == pIdMap.end()) {
      orphans.push_back(container);
      return;
    }

    if (parentIt->second.ptr == nullptr) {
      recreateContainer(parentIt, orphans, nameConflicts);
    }

    std::shared_ptr<IContainerMD> parent = parentIt->second.ptr;
    std::shared_ptr<IContainerMD> child =
      parent->findContainer(container->getName());

    if (child == nullptr) {
      parent->addContainer(container.get());
    } else {
      nameConflicts.push_back(container);
    }
  } else {
    // Non-root container without parent - add to the list of orphans
    if (container->getId() != 1) {
      orphans.push_back(container);
      return;
    }
  }
}

//------------------------------------------------------------------------------
// Commit all the container info to the backend
//------------------------------------------------------------------------------
void
ConvertContainerMDSvc::commitToBackend()
{
  std::uint64_t total = pIdMap.size();
  int nthreads = sThreads;
  int chunk = total / nthreads;
  int last_chunk = chunk + total - (chunk * nthreads);
  // Parallel loop
  eos::common::Parallel::For(0, nthreads, [&](int i) {
    std::int64_t count = 0;
    qclient::AsyncHandler ah;
    eos::ConvertContainerMD* conv_cont = nullptr;
    std::shared_ptr<IContainerMD> container;
    IdMap::iterator it = pIdMap.begin();
    std::advance(it, i * chunk);
    qclient::Options opts;
    opts.transparentRedirects = true;
    opts.retryStrategy = qclient::RetryStrategy::NoRetries();
    qclient::QClient qclient(qclient::Members(sBkndHost, sBkndPort),
                             std::move(opts));
    int max_elem = (i == (nthreads - 1) ? last_chunk : chunk);

    for (int n = 0; n < max_elem; ++n) {
      ++count;
      container = it->second.ptr;
      conv_cont = dynamic_cast<eos::ConvertContainerMD*>(container.get());
      ++it;

      if (conv_cont == nullptr) {
        std::cerr << "Skipping null container id: " << it->first << std::endl;
        continue;
      } else {
        conv_cont->updateInternal();
      }

      // Add container md to the KV store
      try {
        std::string buffer;
        conv_cont->serializeToStr(buffer);
        ah.Register(&qclient,
                    RequestBuilder::writeContainerProto(container->getIdentifier(),
                        container->getLocalityHint(),
                        buffer));

        // Commit subcontainers and files only if not empty otherwise the hmset
        // command will fail
        if (conv_cont->getNumContainers()) {
          conv_cont->commitSubcontainers(ah, qclient);
        }

        if (conv_cont->getNumFiles()) {
          conv_cont->commitFiles(ah, qclient);
        }

        if ((count & sAsyncBatch) == 0) {
          if (!ah.Wait()) {
            std::cerr << __FUNCTION__ << " Got error response from the backend"
                      << std::endl;
            std::terminate();
          }

          std::cout << "Processed " << count << "/" << total << " directories "
                    << std::endl;
        }
      } catch (const std::runtime_error& qdb_err) {
        MDException e(ENOENT);
        e.getMessage() << "Container #" << container->getId()
                       << " failed to contact backend";
        throw e;
      }
    }

    // Wait for any other replies
    if (!ah.Wait()) {
      std::cerr << __FUNCTION__
                << "ERROR: Failed to commit to backend" << std::endl;
      std::terminate();
    }
  });
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
// Get mutex corresponding to container id
//------------------------------------------------------------------------------
std::mutex*
ConvertContainerMDSvc::GetContMutex(IContainerMD::id_t id)
{
  int indx = id % mMutexPool.size();
  return mMutexPool[indx];
}


//------------------------------------------------------------------------------
//         ************* ConvertFileMDSvc Class ************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConvertFileMDSvc::ConvertFileMDSvc():
  ChangeLogFileMDSvc(), mFirstFreeId(0), mConvQView(nullptr),
  mConvFsView(nullptr), mSyncTimeAcc(nullptr), mContAcc(nullptr)
{}

//------------------------------------------------------------------------------
// Initialize the file service
//------------------------------------------------------------------------------
void
ConvertFileMDSvc::initialize()
{
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
  int nthreads = sThreads;
  int chunk = total / nthreads;
  int last_chunk = chunk + pIdMap.size() - (chunk * nthreads);
  auto start = std::time(nullptr);
  std::mutex mutex_lost_found;
  mFirstFreeId = scanner.getLargestId() + 1;
  // Recreate the files
  eos::common::Parallel::For(0, nthreads, [&](int i) noexcept {
    std::int64_t count = 0;
    IdMap::iterator it = pIdMap.begin();
    std::advance(it, i * chunk);
    qclient::AsyncHandler ah;
    qclient::Options opts;
    opts.transparentRedirects = true;
    opts.retryStrategy = qclient::RetryStrategy::NoRetries();
    qclient::QClient qclient(qclient::Members(sBkndHost, sBkndPort),
                             std::move(opts));
    int max_elem = ((i == (nthreads - 1) ? last_chunk : chunk));

    for (int n = 0; n < max_elem; ++n) {
      ++count;

      if ((count & sAsyncBatch) == 0) {
        if (!ah.Wait()) {
          std::cerr << __FUNCTION__ << " Got error response from the backend"
                    << std::endl;
          std::terminate();
        }

        std::cout << "Tid: " << std::this_thread::get_id() << " processed "
                  << count << "/" << max_elem << " files " << std::endl;
      }

      // Unpack the serialized buffers
      std::shared_ptr<IFileMD> file = std::make_shared<ConvertFileMD>(0, this);

      if (file) {
        file->deserialize(*(it->second.buffer));
        delete it->second.buffer;
        it.value().buffer = nullptr;
      } else {
        std::cerr << "Failed to allocate memory for FileMD" << std::endl;
        std::terminate();
      }

      ++it;
      std::shared_ptr<IContainerMD> cont = nullptr;

      try {
        cont = pContSvc->getContainerMD(file->getContainerId());
      } catch (const MDException& e) {
        cont = nullptr;
      }

      if (!cont || (file->getContainerId() == 0)) {
        std::lock_guard<std::mutex> lock(mutex_lost_found);
        attachBroken("orphans", file.get());
        addFileToQdb((ConvertFileMD*)file.get(), ah, qclient);
        continue;
      }

      // Get mutex for current container
      std::mutex* mtx = static_cast<ConvertContainerMDSvc*>(pContSvc)
                        ->GetContMutex(cont->getId());
      mtx->lock();

      if (((ConvertContainerMD*)(cont.get()))->findFileName(file->getName()) ||
          file->getName().empty()) {
        mtx->unlock();
        std::lock_guard<std::mutex> lock(mutex_lost_found);
        attachBroken("name_conflicts", file.get());
        addFileToQdb((ConvertFileMD*)file.get(), ah, qclient);
      } else {
        cont->addFile(file.get());
        mtx->unlock();
        addFileToQdb((ConvertFileMD*)file.get(), ah, qclient);
        // Populate the FileSystemView and QuotaView
        mConvQView->addQuotaInfo(file.get());
        mConvFsView->addFileInfo(file.get());

        // Propagate mtime and size up the tree
        if (mSyncTimeAcc && mContAcc) {
          mSyncTimeAcc->QueueForUpdate(file->getContainerId());
          mContAcc->QueueForUpdate(file->getContainerId(), file->getSize());

          // Update every 100k files from thread 0 only
          if ((count % 100000 == 0) && (i == 0)) {
            mSyncTimeAcc->PropagateUpdates();
            mContAcc->PropagateUpdates();
          }
        }
      }
    }

    // wait for any other replies
    if (!ah.Wait()) {
      std::cerr << "ERROR: Failed to commit to backend" << std::endl;
      std::terminate();
    }
  });
  // Propagate any remaining updates
  mSyncTimeAcc->PropagateUpdates();
  mContAcc->PropagateUpdates();
  // Get the rate
  auto finish = std::time(nullptr);

  if (finish != (std::time_t)(-1)) {
    std::chrono::seconds duration(finish - start);

    if (duration.count()) {
      double rate = (double)total / duration.count();
      std::cout << "Processed files at " << rate << " Hz" << std::endl;
    }
  }

  pIdMap.clear();
}

//------------------------------------------------------------------------------
// Add file object to KV store
//------------------------------------------------------------------------------
void
ConvertFileMDSvc::addFileToQdb(ConvertFileMD* file, qclient::AsyncHandler& ah,
                               qclient::QClient& qclient)
const
{
  try {
    std::string buffer;
    // @todo (esindril): Could use compression when storing the entries
    file->updateInternal();
    file->serializeToStr(buffer);
    ah.Register(&qclient, RequestBuilder::writeFileProto(file->getIdentifier(),
                file->getLocalityHint(),
                buffer));
  } catch (std::runtime_error& qdb_err) {
    MDException e(ENOENT);
    e.getMessage() << "File #" << file->getId() << " failed to contact backend";
    throw e;
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
// Add quota info for a specific file object
//------------------------------------------------------------------------------
void
ConvertQuotaView::addQuotaInfo(IFileMD* file)
{
  // Search for a quota node
  std::shared_ptr<IContainerMD> current;

  try {
    current = mContSvc->getContainerMD(file->getContainerId());
  } catch (const eos::MDException& e) {
    std::cerr << __FUNCTION__ << e.what() << std::endl;
    return;
  }

  while ((current->getId() != 1) &&
         ((current->getFlags() & QUOTA_NODE_FLAG) == 0)) {
    if (current->getParentId() == 0ull) {
      std::cerr << __FUNCTION__ << "Cotainer id:" << current->getId()
                << " has a 0 parent id - skip";
      return;
    } else {
      current = mContSvc->getContainerMD(current->getParentId());
    }
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
  const std::string suid = stringify(file->getCUid());
  const std::string sgid = stringify(file->getCGid());
  std::lock_guard<std::mutex> lock(mMutex);
  auto it_map = mQuotaMap.find(sid);

  if (it_map == mQuotaMap.end()) {
    auto pair = mQuotaMap.emplace(sid, std::make_pair(QuotaNodeMapT(),
                                  QuotaNodeMapT()));
    it_map = pair.first;
  }

  QuotaNodeMapT& map_uid = it_map->second.first;
  QuotaNodeMapT& map_gid = it_map->second.second;
  eos::QuotaNodeCore::UsageInfo& user = map_uid[suid];
  eos::QuotaNodeCore::UsageInfo& group = map_gid[sgid];
  user.physicalSpace += size;
  group.physicalSpace += size;
  user.space += file->getSize();
  group.space += file->getSize();
  user.files++;
  group.files++;
}

//------------------------------------------------------------------------------
// Commit quota view information to the backend
//------------------------------------------------------------------------------
void
ConvertQuotaView::commitToBackend()
{
  qclient::AsyncHandler ah;
  qclient::Options opts;
  opts.transparentRedirects = true;
  opts.retryStrategy = qclient::RetryStrategy::NoRetries();
  qclient::QClient qcl(sBkndHost, sBkndPort, std::move(opts));
  std::string uid_key, gid_key;
  std::uint64_t count = 0u;
  std::uint64_t max_count = 100;

  for (auto it = mQuotaMap.begin(); it != mQuotaMap.end(); ++it) {
    ++count;
    uid_key = KeyQuotaUidMap(it->first);
    gid_key = KeyQuotaGidMap(it->first);
    QuotaNodeMapT& uid_map = it->second.first;
    QuotaNodeMapT& gid_map = it->second.second;
    qclient::QHash quota_map(*sQcl, uid_key);

    for (auto& elem : uid_map) {
      eos::QuotaNodeCore::UsageInfo& info = elem.second;
      std::string field = elem.first + quota::sPhysicalSize;
      quota_map.hset_async(field, std::to_string(info.physicalSpace), &ah);
      field = elem.first + quota::sLogicalSize;
      quota_map.hset_async(field, std::to_string(info.space), &ah);
      field = elem.first + quota::sNumFiles;
      quota_map.hset_async(field, std::to_string(info.files), &ah);
    }

    quota_map.setKey(gid_key);

    for (auto& elem : gid_map) {
      eos::QuotaNodeCore::UsageInfo& info = elem.second;
      std::string field = elem.first + quota::sPhysicalSize;
      quota_map.hset_async(field, std::to_string(info.physicalSpace), &ah);
      field = elem.first + quota::sLogicalSize;
      quota_map.hset_async(field, std::to_string(info.space), &ah);
      field = elem.first + quota::sNumFiles;
      quota_map.hset_async(field, std::to_string(info.files), &ah);
    }

    if (count >= max_count) {
      count = 0u;

      if (!ah.Wait()) {
        std::cerr << __FUNCTION__ << " Got error response from the backend "
                  << "while exporting the quota view" << std::endl;
        std::terminate();
      }
    }
  }

  if (!ah.Wait()) {
    std::cerr << __FUNCTION__ << " Got error response from the backend "
              << "while exporting the quota view" << std::endl;
    std::terminate();
  } else {
    std::cout << "Quota view successfully committed" << std::endl;
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
  std::string selem;
  IFileMD::LocationVector loc_vect = file->getLocations();
  std::string sid = stringify(file->getId());
  std::lock_guard<std::mutex> scope_lock(mMutex);

  for (const auto& elem : loc_vect) {
    // Store fsid if it doesn't exist
    selem = stringify(elem);
    // First is the set of replica file ids
    mFsView[selem].first.push_back(sid);
  }

  IFileMD::LocationVector unlink_vect = file->getUnlinkedLocations();

  for (const auto& elem : unlink_vect) {
    selem = stringify(elem);
    // Second is the set of unlinked file ids
    mFsView[selem].second.push_back(sid);
  }

  if ((file->getNumLocation() == 0) && (file->getNumUnlinkedLocation() == 0)) {
    mFileNoReplica.push_back(sid);
  }
}

//------------------------------------------------------------------------------
// Commit all of the fs view information to the backend
//------------------------------------------------------------------------------
void
ConvertFsView::commitToBackend()
{
  uint64_t total = mFsView.size();
  int nthreads = sThreads;
  int chunk = total / nthreads;
  int last_chunk = chunk + total - (chunk * nthreads);
  uint32_t max_batches = 20;
  int max_sadd_size = 1000;
  // Parallel loop
  eos::common::Parallel::For(0, nthreads, [&](int i) {
    std::uint64_t count = 0u;
    std::string key, val;
    qclient::AsyncHandler ah;
    qclient::Options opts;
    opts.transparentRedirects = true;
    opts.retryStrategy = qclient::RetryStrategy::NoRetries();
    qclient::QClient qclient(sBkndHost, sBkndPort, std::move(opts));
    qclient::QSet fs_set(qclient, "");
    int max_elem = (i == (nthreads - 1) ? last_chunk : chunk);
    auto it = mFsView.begin();
    std::advance(it, i * chunk);

    for (int n = 0; n < max_elem; ++n) {
      ++count;
      // Add file to corresponding fs file set
      key = eos::RequestBuilder::keyFilesystemFiles(std::stoi(it->first));
      fs_set.setKey(key);

      if (it->second.first.size()) {
        uint32_t num_batches = 0u;
        uint64_t pos = 0u;
        uint64_t total = it->second.first.size();
        auto begin = it->second.first.begin();

        while (pos < total) {
          uint64_t step = (pos + max_sadd_size >= total) ? (total - pos) : max_sadd_size;
          auto end = begin;
          std::advance(end, step);
          fs_set.sadd_async(begin, end, &ah);
          begin = end;
          pos += step;
          ++num_batches;

          if (num_batches == max_batches) {
            num_batches = 0u;

            if (!ah.Wait()) {
              std::cerr << __FUNCTION__ << " Got error response from the backend"
                        << std::endl;
              std::terminate();
            }
          }
        }
      }

      key = eos::RequestBuilder::keyFilesystemUnlinked(std::stoi(it->first));
      fs_set.setKey(key);

      if (it->second.second.size()) {
        uint32_t num_batches = 0u;
        uint64_t pos = 0u;
        uint64_t total = it->second.second.size();
        auto begin = it->second.second.begin();

        while (pos < total) {
          uint64_t step = (pos + max_sadd_size >= total) ? (total - pos) : max_sadd_size;
          auto end = begin;
          std::advance(end, step);
          fs_set.sadd_async(begin, end, &ah);
          begin = end;
          pos += step;
          ++num_batches;

          if (num_batches == max_batches) {
            num_batches = 0u;

            if (!ah.Wait()) {
              std::cerr << __FUNCTION__ << " Got error response from the backend"
                        << std::endl;
              std::terminate();
            }
          }
        }
      }

      if ((count & max_batches) == 0) {
        if (!ah.Wait()) {
          std::cerr << __FUNCTION__ << " Got error response from the backend"
                    << std::endl;
          std::terminate();
        }
      }

      ++it;
    }

    // Only the first thread will commit this
    if (i == 0) {
      fs_set.setKey(fsview::sNoReplicaPrefix);
      size_t num_batches = 0u;
      size_t pos = 0u;
      size_t total = mFileNoReplica.size();
      auto begin = mFileNoReplica.begin();

      while (pos < total) {
        uint64_t step = (pos + max_sadd_size >= total) ? (total - pos) : max_sadd_size;
        auto end = begin;
        std::advance(end, step);
        fs_set.sadd_async(begin, end, &ah);
        begin = end;
        pos += step;
        ++num_batches;

        if (num_batches == max_batches) {
          num_batches = 0u;

          if (!ah.Wait()) {
            std::cerr << __FUNCTION__ << " Got error response from the backend"
                      << std::endl;
            std::terminate();
          }
        }
      }
    }

    // Wait for all in-flight async requests
    if (!ah.Wait()) {
      std::cerr << __FUNCTION__ << " Got error response from the backend"
                << std::endl;
      std::terminate();
    }
  });
}

EOSNSNAMESPACE_END
//------------------------------------------------------------------------------
// Print usage information
//------------------------------------------------------------------------------
void
usage()
{
  std::cerr << "Usage:                                            " << std::endl
            << "  ./eos-ns-convert <file_chlog> <dir_chlog> <bknd_host> "
            << "<bknd_port>" << std::endl
            << "    file_chlog - file changelog                   " << std::endl
            << "    dir_chlog  - directory changelog              " << std::endl
            << "    bknd_host  - Backend host destination         " << std::endl
            << "    bknd_port  - Backend port destination         " << std::endl
            << std::endl;
}
//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int
main(int argc, char* argv[])
{
  sThreads = std::thread::hardware_concurrency();
  const char* conversionThreads = getenv("CONVERSION_THREADS");

  if (conversionThreads) {
    sThreads = atoi(conversionThreads);
  }

  std::cerr << "Using " << sThreads << " parallel threads for conversion" <<
            std::endl;

  if (argc != 5) {
    usage();
    return 1;
  }

  // Disable CRC32 computation for entries since we know they should be fine
  // as we've just compacted the changelogs.
  if (setenv("EOS_NS_BOOT_NOCRC32", "1", 1)) {
    std::cerr << "ERROR: failed to set EOS_NS_BOOT_NOCRC32 env variable"
              << std::endl;
    return 1;
  }

  if (setenv("EOS_NS_CONVERT_NOCRC32", "1", 1)) {
    std::cerr << "ERROR: failed to set EOS_NS_CONVERT_NOCRC32 env variable"
              << std::endl;
    return 1;
  }

  if (setenv("EOS_NS_BOOT_PARALLEL", "1", 1)) {
    std::cerr << "ERROR: failed to set EOS_NS_BOOT_PARALLEL env variable"
              << std::endl;
    return 1;
  }

  try {
    std::string file_chlog(argv[1]);
    std::string dir_chlog(argv[2]);
    sBkndHost = argv[3];
    sBkndPort = std::stoull(argv[4]);
    sQcl = eos::BackendClient::getInstance
           (eos::QdbContactDetails(qclient::Members(sBkndHost, sBkndPort), ""));
    // Check file and directory changelog files
    struct stat info = {0};
    std::list<std::string> lst_files {file_chlog, dir_chlog};

    for (auto& fn : lst_files) {
      int ret = stat(fn.c_str(), &info);

      if (ret != 0) {
        std::cerr << "Unable to access file: " << fn << std::endl;
        return EIO;
      }
    }

    std::time_t start = std::time(nullptr);
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
    // Create the view objects
    std::unique_ptr<eos::ConvertQuotaView> quota_view{
      new eos::ConvertQuotaView(cont_svc.get())};
    std::unique_ptr<eos::ConvertFsView> fs_view{
      new eos::ConvertFsView()};
    eos::ConvertContainerMDSvc* conv_cont_svc =
      dynamic_cast<eos::ConvertContainerMDSvc*>(cont_svc.get());
    eos::ConvertFileMDSvc* conv_file_svc =
      dynamic_cast<eos::ConvertFileMDSvc*>(file_svc.get());

    if (!conv_cont_svc || !conv_file_svc) {
      std::cerr << "Failed dynamic cast to convert meta-data type" << std::endl;
      std::terminate();
    }

    conv_cont_svc->setQuotaView(quota_view.get());
    conv_file_svc->setViews(quota_view.get(), fs_view.get());
    std::time_t cont_start = std::time(nullptr);
    cont_svc->initialize();
    std::chrono::seconds cont_duration {std::time(nullptr) - cont_start};

    if (cont_duration.count()) {
      double rate = (double)cont_svc->getNumContainers() / cont_duration.count();
      std::cout << "Container init: " << cont_svc->getNumContainers()
                << " containers in " << cont_duration.count() << " seconds at ~"
                << rate << " Hz" << std::endl;
    }

    // Initialize the file meta-data service
    std::cout << "Initialize the file meta-data service" << std::endl;
    file_svc->setContMDService(cont_svc.get());
    // Create views for sync time and tree size propagation
    eos::common::RWMutex dummy_ns_mutex;
    eos::IContainerMDChangeListener* sync_view =
      new eos::QuarkSyncTimeAccounting(conv_cont_svc, &dummy_ns_mutex, 0);
    eos::IFileMDChangeListener* cont_acc =
      new eos::QuarkContainerAccounting(conv_cont_svc, &dummy_ns_mutex, 0);
    conv_file_svc->setSyncTimeAcc(sync_view);
    conv_file_svc->setContainerAcc(cont_acc);
    conv_file_svc->configure(config_file);
    std::time_t file_start = std::time(nullptr);
    file_svc->initialize();
    std::chrono::seconds file_duration {std::time(nullptr) - file_start};
    std::cout << "File init: " << file_duration.count() << " seconds" << std::endl;
    std::cout << "Commit quota and file system view ..." << std::endl;
    std::time_t views_start = std::time(nullptr);
    // Commit the quota view information
    quota_view->commitToBackend();
    std::time_t quota_end = std::time(nullptr);
    std::chrono::seconds quota_duration {quota_end - views_start};
    std::cout << "Quota init: " << quota_duration.count() << " seconds"
              << std::endl;
    // Commit the file system view information
    fs_view->commitToBackend();
    std::time_t fsview_end = std::time(nullptr);
    std::chrono::seconds fsview_duration {fsview_end - quota_end};
    std::cout << "FsView init: " << fsview_duration.count() << " seconds"
              << std::endl;
    // Commit the directory information to the backend
    std::cout << "Commit container info to backend: " << std::endl;

    try {
      conv_cont_svc->commitToBackend();
    } catch (eos::MDException& e) {
      std::cerr << "Exception thrown: " << e.what() << std::endl;
      return 1;
    }

    std::chrono::seconds cont_commit{std::time(nullptr) - fsview_end};
    std::cout << "Container init: " << cont_svc->getNumContainers()
              << " containers in " << cont_commit.count() << " seconds"
              << std::endl;
    // Save the last used file and container id in the meta_hmap
    qclient::QHash meta_map {*sQcl, eos::constants::sMapMetaInfoKey};
    meta_map.hset(eos::constants::sLastUsedFid,
                  std::to_string(file_svc->getFirstFreeId() - 1));
    meta_map.hset(eos::constants::sLastUsedCid,
                  std::to_string(cont_svc->getFirstFreeId() - 1));
    // QuarkDB bulkload finalization (triggers manual compaction in rocksdb)
    std::time_t finalizeStart = std::time(nullptr);
    sQcl->exec("quarkdb_bulkload_finalize").get();
    std::time_t finalizeEnd = std::time(nullptr);
    std::cout << "QuarkDB bulkload finalization: " << finalizeEnd - finalizeStart <<
              " seconds" << std::endl;
    std::chrono::seconds full_duration {std::time(nullptr) - start};
    std::cout << "Conversion duration: " << full_duration.count() << std::endl;
  } catch (const std::runtime_error& e) {
    std::cerr << "Exception thrown: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
