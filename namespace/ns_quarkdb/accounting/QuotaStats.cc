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

//------------------------------------------------------------------------------
// @author Elvin-Alin Sindrilaru <esindril@cern.ch>
// @brief User quota accounting
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "qclient/QScanner.hh"
#include "common/StringTokenizer.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// *** Class QuotaNode implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuotaNode::QuotaNode(IQuotaStats* quota_stats, IContainerMD::id_t node_id)
  : IQuotaNode(quota_stats, node_id)
{
  std::string snode_id = std::to_string(node_id);
  pQcl = static_cast<QuotaStats*>(quota_stats)->pQcl;
  pFlusher = static_cast<QuotaStats*>(quota_stats)->pFlusher;
  pQuotaUidKey = QuotaStats::KeyQuotaUidMap(snode_id);
  pUidMap = qclient::QHash(*pQcl, pQuotaUidKey);
  pQuotaGidKey = QuotaStats::KeyQuotaGidMap(snode_id);
  pGidMap = qclient::QHash(*pQcl, pQuotaGidKey);
}

//------------------------------------------------------------------------------
// Account a new file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void
QuotaNode::addFile(const IFileMD* file)
{
  const std::string suid = std::to_string(file->getCUid());
  const std::string sgid = std::to_string(file->getCGid());
  const int64_t size = pQuotaStats->getPhysicalSize(file);
  std::string field = suid + quota::sPhysicalSize;
  pFlusher->hincrby(pQuotaUidKey, field, size);
  field = sgid + quota::sPhysicalSize;
  pFlusher->hincrby(pQuotaGidKey, field, size);
  field = suid + quota::sLogicalSize;
  pFlusher->hincrby(pQuotaUidKey, field, file->getSize());
  field = sgid + quota::sLogicalSize;
  pFlusher->hincrby(pQuotaGidKey, field, file->getSize());
  field = suid + quota::sNumFiles;
  pFlusher->hincrby(pQuotaUidKey, field, 1);
  field = sgid + quota::sNumFiles;
  pFlusher->hincrby(pQuotaGidKey, field, 1);
  // Update the cached information
  UsageInfo& user  = pUserUsage[file->getCUid()];
  UsageInfo& group = pGroupUsage[file->getCGid()];
  user.physicalSpace  += size;
  group.physicalSpace += size;
  user.space   += file->getSize();
  group.space  += file->getSize();
  ++user.files;
  ++group.files;
}

//------------------------------------------------------------------------------
// Remove a file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void
QuotaNode::removeFile(const IFileMD* file)
{
  const std::string suid = std::to_string(file->getCUid());
  const std::string sgid = std::to_string(file->getCGid());
  int64_t size = pQuotaStats->getPhysicalSize(file);
  std::string field = suid + quota::sPhysicalSize;
  pFlusher->hincrby(pQuotaUidKey, field, -size);
  field = sgid + quota::sPhysicalSize;
  pFlusher->hincrby(pQuotaGidKey, field, -size);
  field = suid + quota::sLogicalSize;
  pFlusher->hincrby(pQuotaUidKey, field, -file->getSize());
  field = sgid + quota::sLogicalSize;
  pFlusher->hincrby(pQuotaGidKey, field, -file->getSize());
  field = suid + quota::sNumFiles;
  pFlusher->hincrby(pQuotaUidKey, field, -1);
  field = sgid + quota::sNumFiles;
  pFlusher->hincrby(pQuotaGidKey, field, -1);
  // Update the cached information
  UsageInfo& user  = pUserUsage[file->getCUid()];
  UsageInfo& group = pGroupUsage[file->getCGid()];
  user.physicalSpace  -= size;
  group.physicalSpace -= size;
  user.space   -= file->getSize();
  group.space  -= file->getSize();
  --user.files;
  --group.files;
}

//------------------------------------------------------------------------------
// Meld in another quota node
//------------------------------------------------------------------------------
void
QuotaNode::meld(const IQuotaNode* node)
{
  const QuotaNode* impl_node = dynamic_cast<const QuotaNode*>(node);
  std::string field;
  qclient::QHash hmap(*pQcl,
                      QuotaStats::KeyQuotaUidMap(std::to_string(impl_node->getId())));
  // elems is a list of keys and values
  std::vector<std::string> elems = hmap.hgetall();

  for (auto it = elems.begin(); it != elems.end(); ++it) {
    field = *it;
    ++it;
    pFlusher->hincrby(pQuotaUidKey, field, std::stoll(*it));
  }

  hmap.setKey(QuotaStats::KeyQuotaGidMap(std::to_string(impl_node->getId())));
  elems = hmap.hgetall();

  for (auto it = elems.begin(); it != elems.end(); ++it) {
    field = *it;
    ++it;
    pFlusher->hincrby(pQuotaGidKey, field, std::stoll(*it));
  }

  // Update the cached information
  for (auto it1 = impl_node->pUserUsage.begin();
       it1 != impl_node->pUserUsage.end(); ++it1) {
    pUserUsage[it1->first] += it1->second;
  }

  for (auto it2 = impl_node->pGroupUsage.begin();
       it2 != impl_node->pGroupUsage.end(); ++it2) {
    pGroupUsage[it2->first] += it2->second;
  }
}

//------------------------------------------------------------------------------
// Update with information from the backend
//------------------------------------------------------------------------------
void QuotaNode::updateFromBackend()
{
  std::string cursor = "0";
  long long count = 300;
  std::pair<std::string, std::unordered_map<std::string, std::string>> reply;

  do {
    reply = pUidMap.hscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      size_t pos = elem.first.find(':');
      uint64_t uid = std::stoull(elem.first.substr(0, pos));
      std::string type = elem.first.substr(pos + 1);
      auto it_uid = pUserUsage.find(uid);

      if (it_uid == pUserUsage.end()) {
        auto pair = pUserUsage.emplace(uid, UsageInfo());
        it_uid = pair.first;
      }

      UsageInfo& uinfo = it_uid->second;

      if (type == "logical_size") {
        uinfo.space = std::stoull(elem.second);
      } else if (type == "physical_size") {
        uinfo.physicalSpace = std::stoull(elem.second);
      } else if (type == "files") {
        uinfo.files = std::stoull(elem.second);
      }
    }
  } while (cursor != "0");

  cursor = "0";

  do {
    reply = pGidMap.hscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      size_t pos = elem.first.find(':');
      uint64_t uid = std::stoull(elem.first.substr(0, pos));
      std::string type = elem.first.substr(pos + 1);
      auto it_gid = pGroupUsage.find(uid);

      if (it_gid == pGroupUsage.end()) {
        auto pair = pGroupUsage.emplace(uid, UsageInfo());
        it_gid = pair.first;
      }

      UsageInfo& ginfo = it_gid->second;

      if (type == "logical_size") {
        ginfo.space = std::stoull(elem.second);
      } else if (type == "physical_size") {
        ginfo.physicalSpace = std::stoull(elem.second);
      } else if (type == "files") {
        ginfo.files = std::stoull(elem.second);
      }
    }
  } while (cursor != "0");
}

//------------------------------------------------------------------------------
// *** Class QuotaStats implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuotaStats::QuotaStats():
  pQcl(nullptr), pFlusher(nullptr) {}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuotaStats::~QuotaStats()
{
  if (pFlusher) {
    pFlusher->synchronize();
  }

  for (auto && elem : pNodeMap) {
    delete elem.second;
  }

  pNodeMap.clear();
}

//------------------------------------------------------------------------------
// Configure the quota service
//------------------------------------------------------------------------------
void
QuotaStats::configure(const std::map<std::string, std::string>& config)
{
  std::string qdb_cluster;
  std::string qdb_flusher_id;
  const std::string key_cluster = "qdb_cluster";
  const std::string key_flusher = "qdb_flusher_quota";

  if ((config.find(key_cluster) != config.end()) &&
      (config.find(key_flusher) != config.end())) {
    qdb_cluster = config.at(key_cluster);
    qdb_flusher_id = config.at(key_flusher);
  } else {
    eos::MDException e(EINVAL);
    e.getMessage() << __FUNCTION__  << " No qdb_cluster or qdb_flusher_quota "
                   << "configuration info provided";
    throw e;
  }

  qclient::Members qdb_members;

  if (!qdb_members.parse(qdb_cluster)) {
    eos::MDException e(EINVAL);
    e.getMessage() << __FUNCTION__
                   << " Failed to parse qdbcluster members";
    throw e;
  }

  pQcl = BackendClient::getInstance(qdb_members);
  pFlusher = MetadataFlusherFactory::getInstance(qdb_flusher_id, qdb_members);
}

//------------------------------------------------------------------------------
// Get a quota node associated to the container id
//------------------------------------------------------------------------------
IQuotaNode*
QuotaStats::getQuotaNode(IContainerMD::id_t node_id)
{
  if (pNodeMap.count(node_id) != 0u) {
    return pNodeMap[node_id];
  }

  std::string snode_id = std::to_string(node_id);

  if ((pQcl->exists(KeyQuotaUidMap(snode_id)) == 1) ||
      (pQcl->exists(KeyQuotaGidMap(snode_id)) == 1)) {
    QuotaNode* ptr = new QuotaNode(this, node_id);
    ptr->updateFromBackend();
    pNodeMap[node_id] = static_cast<IQuotaNode*>(ptr);
    return ptr;
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Register a new quota node
//------------------------------------------------------------------------------
IQuotaNode*
QuotaStats::registerNewNode(IContainerMD::id_t node_id)
{
  std::string snode_id = std::to_string(node_id);

  if (pNodeMap.count(node_id) ||
      (pQcl->exists(KeyQuotaUidMap(snode_id)) == 1) ||
      (pQcl->exists(KeyQuotaGidMap(snode_id)) == 1)) {
    MDException e;
    e.getMessage() << "Quota node already exist: " << snode_id;
    throw e;
  }

  IQuotaNode* ptr = new QuotaNode(this, node_id);
  pNodeMap[node_id] = ptr;
  return ptr;
}

//------------------------------------------------------------------------------
// Remove quota node
//------------------------------------------------------------------------------
void
QuotaStats::removeNode(IContainerMD::id_t node_id)
{
  if (pNodeMap.count(node_id) != 0u) {
    pNodeMap.erase(node_id);
  }

  std::string snode_id = std::to_string(node_id);
  pFlusher->del(KeyQuotaUidMap(snode_id));
  pFlusher->del(KeyQuotaGidMap(snode_id));
}

//------------------------------------------------------------------------------
// Get the set of all quota node ids. The quota node id corresponds to the
// container id.
//------------------------------------------------------------------------------
std::unordered_set<IContainerMD::id_t>
QuotaStats::getAllIds()
{
  std::unordered_set<IContainerMD::id_t> quota_ids;
  qclient::QScanner quota_set(*pQcl, quota::sPrefix + "*:*");
  std::vector<std::string> results;

  while (quota_set.next(results)) {
    for (const std::string& rep : results) {
      // Extract quota node id
      IContainerMD::id_t id = 0;

      if (ParseQuotaId(rep, id)) {
        quota_ids.insert(id);
      }
    }
  }

  return quota_ids;
}

//------------------------------------------------------------------------------
// Get quota node uid map key
//------------------------------------------------------------------------------
std::string
QuotaStats::KeyQuotaUidMap(const std::string& sid)
{
  return quota::sPrefix + sid + ":" + quota::sUidsSuffix;
}

//------------------------------------------------------------------------------
// Get quota node gid map key
//------------------------------------------------------------------------------
std::string
QuotaStats::KeyQuotaGidMap(const std::string& sid)
{
  return quota::sPrefix + sid + ":" + quota::sGidsSuffix;
}

//------------------------------------------------------------------------------
// Parse quota id from string
//------------------------------------------------------------------------------
bool
QuotaStats::ParseQuotaId(const std::string& input, IContainerMD::id_t& id)
{
  std::vector<std::string> parts =
    eos::common::StringTokenizer::split< std::vector<std::string> >(input, ':');

  if (parts.size() != 3) {
    return false;
  }

  if (parts[0] + ":" != quota::sPrefix) {
    return false;
  }

  if ((parts[2] != quota::sUidsSuffix) && (parts[2] != quota::sGidsSuffix)) {
    return false;
  }

  id = std::stoull(parts[1]);
  return true;
}

EOSNSNAMESPACE_END
