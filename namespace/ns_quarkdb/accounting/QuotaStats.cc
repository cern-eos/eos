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
  pQcl = static_cast<QuotaStats*>(quota_stats)->pQcl.get();
  pFlusher = static_cast<QuotaStats*>(quota_stats)->pFlusher;
  pQuotaUidKey = quota::sPrefix + snode_id + quota::sUidsSuffix;
  pUidMap = qclient::QHash(*pQcl, pQuotaUidKey);
  pQuotaGidKey = quota::sPrefix + snode_id + quota::sGidsSuffix;
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
  pFlusher->hincrby(pQuotaUidKey, field, 1);
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
  pFlusher->hincrby(pQuotaUidKey, field, -1);
}

//------------------------------------------------------------------------------
// Meld in another quota node
//------------------------------------------------------------------------------
void
QuotaNode::meld(const IQuotaNode* node)
{
  const QuotaNode* impl_node = dynamic_cast<const QuotaNode*>(node);

  if (!impl_node) {
    throw std::runtime_error("QuotaNode dynamic cast failed");
  }

  std::string field;
  qclient::QHash hmap(*pQcl, impl_node->getUidKey());
  std::vector<std::string> elems = hmap.hgetall();

  for (auto it = elems.begin(); it != elems.end(); ++it) {
    field = *it;
    ++it;
    (void)pUidMap.hincrby(field, *it);
  }

  hmap.setKey(impl_node->getGidKey());
  elems = hmap.hgetall();

  for (auto it = elems.begin(); it != elems.end(); ++it) {
    field = *it;
    ++it;
    (void)pGidMap.hincrby(field, *it);
  }
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getUsedSpaceByUser(uid_t uid)
{
  try {
    std::string field = std::to_string(uid) + quota::sLogicalSize;
    std::string val = pUidMap.hget(field);
    return (val.empty() ? 0 : std::stoull(val));
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getUsedSpaceByGroup(gid_t gid)
{
  try {
    std::string field = std::to_string(gid) + quota::sLogicalSize;
    std::string val = pGidMap.hget(field);
    return (val.empty() ? 0 : std::stoull(val));
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getPhysicalSpaceByUser(uid_t uid)
{
  try {
    std::string field = std::to_string(uid) + quota::sPhysicalSize;
    std::string val = pUidMap.hget(field);
    return (val.empty() ? 0 : std::stoull(val));
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getPhysicalSpaceByGroup(gid_t gid)
{
  try {
    std::string field = std::to_string(gid) + quota::sPhysicalSize;
    std::string val = pGidMap.hget(field);
    return (val.empty() ? 0 : std::stoull(val));
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getNumFilesByUser(uid_t uid)
{
  try {
    std::string field = std::to_string(uid) + quota::sNumFiles;
    std::string val = pUidMap.hget(field);
    return (val.empty() ? 0 : std::stoull(val));
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//----------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//----------------------------------------------------------------------------
uint64_t
QuotaNode::getNumFilesByGroup(gid_t gid)
{
  try {
    std::string field = std::to_string(gid) + quota::sNumFiles;
    std::string val = pGidMap.hget(field);
    return (val.empty() ? 0 : std::stoull(val));
  } catch (std::runtime_error& e) {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get the set of uids for which information is stored in the current quota
// node.
//------------------------------------------------------------------------------
std::unordered_set<uint64_t>
QuotaNode::getUids()
{
  std::string suid;
  std::vector<std::string> keys = pUidMap.hkeys();
  std::unordered_set<uint64_t> uids;
  uids.reserve(keys.size() / 3);

  // The keys have the following format: uid1:logical_size, uid1:physical_size,
  // uid1:files ... uidn:files.
  for (auto && elem : keys) {
    suid = elem.substr(0, elem.find(':'));
    uids.insert(std::stoull(suid));
  }

  return uids;
}

//----------------------------------------------------------------------------
// Get the set of gids for which information is stored in the current quota
// node.
//----------------------------------------------------------------------------
std::unordered_set<uint64_t>
QuotaNode::getGids()
{
  std::string sgid;
  std::vector<std::string> keys = pGidMap.hkeys();
  std::unordered_set<uint64_t> gids;
  gids.reserve(keys.size() / 3);

  for (auto && elem : keys) {
    sgid = elem.substr(0, elem.find(':'));
    gids.insert(std::stoull(elem));
  }

  return gids;
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
    delete pFlusher;
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
  const std::string key_cluster = "qdb_cluster";

  if (config.find(key_cluster) != config.end()) {
    qdb_cluster = config.at(key_cluster);
  } else {
    eos::MDException e(EINVAL);
    e.getMessage() << __FUNCTION__
                   << " No qdbcluster configuration info provided";
    throw e;
  }

  qclient::Members qdb_members;

  if (!qdb_members.parse(qdb_cluster)) {
    eos::MDException e(EINVAL);
    e.getMessage() << __FUNCTION__
                   << " Failed to parse qdbcluster members";
    throw e;
  }

  pQcl.reset(new qclient::QClient(qdb_members, true, false));
  pFlusher = MetadataFlusherFactory::getInstance("quota", qdb_members);
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

  IQuotaNode* ptr = new QuotaNode(this, node_id);
  pNodeMap[node_id] = ptr;
  return ptr;
}

//------------------------------------------------------------------------------
// Register a new quota node
//------------------------------------------------------------------------------
IQuotaNode*
QuotaStats::registerNewNode(IContainerMD::id_t node_id)
{
  if (pNodeMap.count(node_id)) {
    MDException e;
    e.getMessage() << "Quota node already exist: " << node_id;
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
  qclient::QScanner quota_set(*(pQcl.get()), quota::sPrefix + "*:*");
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
QuotaStats:: KeyQuotaUidMap(const std::string& sid)
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
