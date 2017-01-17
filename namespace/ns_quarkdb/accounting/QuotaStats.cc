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

EOSNSNAMESPACE_BEGIN

const std::string QuotaStats::sSetQuotaIds = "quota_set_ids";
const std::string QuotaStats::sQuotaUidsSuffix = ":quota_hmap_uid";
const std::string QuotaStats::sQuotaGidsSuffix = ":quota_hmap_gid";

const std::string QuotaNode::sSpaceTag = ":space";
const std::string QuotaNode::sPhysicalSpaceTag = ":physical_space";
const std::string QuotaNode::sFilesTag = ":files";

//------------------------------------------------------------------------------
// *** Class QuotaNode implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuotaNode::QuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t node_id)
  : IQuotaNode(quotaStats),
    pQcl(dynamic_cast<QuotaStats*>(quotaStats)->pQcl)
{
  pQuotaUidKey = std::to_string(node_id) + QuotaStats::sQuotaUidsSuffix;
  pUidMap = qclient::QHash(*pQcl, pQuotaUidKey);
  pQuotaGidKey = std::to_string(node_id) + QuotaStats::sQuotaGidsSuffix;
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
  std::string field = suid + sPhysicalSpaceTag;
  pAh.Register(pUidMap.hincrby_async(field, size), pUidMap.getClient());
  field = sgid + sPhysicalSpaceTag;
  pAh.Register(pGidMap.hincrby_async(field, size), pGidMap.getClient());
  field = suid + sSpaceTag;
  pAh.Register(pUidMap.hincrby_async(field, file->getSize()),
               pUidMap.getClient());
  field = sgid + sSpaceTag;
  pAh.Register(pGidMap.hincrby_async(field, file->getSize()),
               pGidMap.getClient());
  field = suid + sFilesTag;
  pAh.Register(pUidMap.hincrby_async(field, 1), pUidMap.getClient());
  field = sgid + sFilesTag;
  pAh.Register(pGidMap.hincrby_async(field, 1), pGidMap.getClient());

  if (!pAh.Wait()) {
    std::vector<long long int> resp = pAh.GetResponses();

    for (auto& r : resp) {
      if (r == -ECOMM) {
        // Communication error
        MDException e;
        e.getMessage() << "Failed to connect to backend while updating quota";
        throw e;
      } else if (r == -1) {
        // Unexpected reply type
        MDException e;
        e.getMessage() << "Unexpected reply type while updating quota";
        throw e;
      }
    }
  }
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
  std::string field = suid + sPhysicalSpaceTag;
  pAh.Register(pUidMap.hincrby_async(field, -size), pUidMap.getClient());
  field = sgid + sPhysicalSpaceTag;
  pAh.Register(pGidMap.hincrby_async(field, -size), pGidMap.getClient());
  field = suid + sSpaceTag;
  size = static_cast<int64_t>(file->getSize());
  pAh.Register(pUidMap.hincrby_async(field, -size), pUidMap.getClient());
  field = sgid + sSpaceTag;
  pAh.Register(pGidMap.hincrby_async(field, -size), pGidMap.getClient());
  field = suid + sFilesTag;
  pAh.Register(pUidMap.hincrby_async(field, -1), pUidMap.getClient());
  field = sgid + sFilesTag;
  pAh.Register(pGidMap.hincrby_async(field, -1), pGidMap.getClient());

  if (!pAh.Wait()) {
    std::vector<long long int> resp = pAh.GetResponses();

    for (auto& r : resp) {
      if (r == -ECOMM) {
        // Communication error
        MDException e;
        e.getMessage() << "Failed to connect to backend while updating quota";
        throw e;
      } else if (r == -1) {
        // Unexpected reply type
        MDException e;
        e.getMessage() << "Unexpected reply type while updating quota";
        throw e;
      }
    }
  }
}

//------------------------------------------------------------------------------
// Meld in another quota node
//------------------------------------------------------------------------------
void
QuotaNode::meld(const IQuotaNode* node)
{
  std::string field;
  qclient::QHash hmap(*pQcl, dynamic_cast<const QuotaNode*>(node)->getUidKey());
  std::vector<std::string> elems = hmap.hgetall();

  for (auto it = elems.begin(); it != elems.end(); ++it) {
    field = *it;
    ++it;
    (void)pUidMap.hincrby(field, *it);
  }

  hmap.setKey(dynamic_cast<const QuotaNode*>(node)->getGidKey());
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
    std::string field = std::to_string(uid) + sSpaceTag;
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
    std::string field = std::to_string(gid) + sSpaceTag;
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
    std::string field = std::to_string(uid) + sPhysicalSpaceTag;
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
    std::string field = std::to_string(gid) + sPhysicalSpaceTag;
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
    std::string field = std::to_string(uid) + sFilesTag;
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
    std::string field = std::to_string(gid) + sFilesTag;
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
std::vector<uint64_t>
QuotaNode::getUids()
{
  std::string suid;
  std::vector<std::string> keys = pUidMap.hkeys();
  std::vector<uint64_t> uids;
  uids.resize(keys.size() / 3);

  // The keys have to following format: uid1:space, uid1:physical_space,
  // uid1:files ... uidn:files.
  for (auto && elem : keys) {
    suid = elem.substr(0, elem.find(':'));
    uids.push_back(std::stoul(suid));
  }

  return uids;
}

//----------------------------------------------------------------------------
// Get the set of gids for which information is stored in the current quota
// node.
//----------------------------------------------------------------------------
std::vector<uint64_t>
QuotaNode::getGids()
{
  std::string sgid;
  std::vector<std::string> keys = pGidMap.hkeys();
  std::vector<uint64_t> gids;
  gids.resize(keys.size() / 3);

  // The keys have to following format: gid1:space, gid1:physical_space,
  // gid1:files ... gidn:files.
  for (auto && elem : keys) {
    sgid = elem.substr(0, elem.find(':'));
    gids.push_back(std::stoul(elem));
  }

  return gids;
}

//------------------------------------------------------------------------------
// *** Class QuotaStats implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuotaStats::QuotaStats(const std::map<std::string, std::string>& config)
{
  const std::string key_host = "qdb_host";
  const std::string key_port = "qdb_port";
  std::string host{""};
  uint32_t port{0};

  if (config.find(key_host) != config.end()) {
    host = config.find(key_host)->second;
  }

  if (config.find(key_port) != config.end()) {
    port = std::stoul(config.find(key_port)->second);
  }

  pQcl = BackendClient::getInstance(host, port);
  pIdsSet.setClient(*pQcl);
  pIdsSet.setKey(sSetQuotaIds);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuotaStats::~QuotaStats()
{
  pQcl = nullptr;

  for (auto && elem : pNodeMap) {
    delete elem.second;
  }

  pNodeMap.clear();
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

  if (!pIdsSet.sismember(node_id)) {
    return nullptr;
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
  if (pIdsSet.sismember(node_id)) {
    MDException e;
    e.getMessage() << "Quota node already exist: " << node_id;
    throw e;
  }

  if (!pIdsSet.sadd(node_id)) {
    MDException e;
    e.getMessage() << "Failed to register new quota node: " << node_id;
    throw e;
  }

  IQuotaNode* ptr{new QuotaNode(this, node_id)};
  pNodeMap[node_id] = ptr;
  return ptr;
}

//------------------------------------------------------------------------------
// Remove quota node
//------------------------------------------------------------------------------
void
QuotaStats::removeNode(IContainerMD::id_t node_id)
{
  std::string snode_id = std::to_string(node_id);

  if (pNodeMap.count(node_id) != 0u) {
    pNodeMap.erase(node_id);
  }

  try {
    if (!pIdsSet.srem(snode_id)) {
      MDException e;
      e.getMessage() << "Quota node " << node_id << " does not exist in set";
      throw e;
    }

    // Delete the hmaps associated with the current node
    std::string key = snode_id + sQuotaUidsSuffix;
    (void) pQcl->del(key);
    key = snode_id + sQuotaGidsSuffix;
    (void) pQcl->del(key);
  } catch (std::runtime_error& qdb_err) {
    MDException e;
    e.getMessage() << "Remove quota node " << node_id << " failed - "
                   << qdb_err.what();
    throw e;
  }
}

//------------------------------------------------------------------------------
// Get the set of all quota node ids. The quota node id corresponds to the
// container id.
//------------------------------------------------------------------------------
std::set<std::string>
QuotaStats::getAllIds()
{
  return pIdsSet.smembers();
}

EOSNSNAMESPACE_END
