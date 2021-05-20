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
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/ConfigurationParser.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "qclient/structures/QScanner.hh"
#include "qclient/structures/QHash.hh"
#include "common/StringTokenizer.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// *** Class QuotaNode implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkQuotaNode::QuarkQuotaNode(IQuotaStats* quota_stats,
                               IContainerMD::id_t node_id)
  : IQuotaNode(quota_stats, node_id)
{
  std::string snode_id = std::to_string(node_id);
  pQcl = static_cast<QuarkQuotaStats*>(quota_stats)->pQcl;
  pFlusher = static_cast<QuarkQuotaStats*>(quota_stats)->pFlusher;
  pQuotaUidKey = QuarkQuotaStats::KeyQuotaUidMap(snode_id);
  pQuotaGidKey = QuarkQuotaStats::KeyQuotaGidMap(snode_id);
}

//------------------------------------------------------------------------------
// Account a new file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void
QuarkQuotaNode::addFile(const IFileMD* file)
{
  const std::string suid = std::to_string(file->getCUid());
  const std::string sgid = std::to_string(file->getCGid());
  const int64_t size = pQuotaStats->getPhysicalSize(file);
  const std::string physicalSize = std::to_string(size);
  const std::string logicalSize = std::to_string(file->getSize());
  pFlusher->exec("HINCRBYMULTI",
                 pQuotaUidKey, suid + quota::sPhysicalSize, physicalSize,
                 pQuotaGidKey, sgid + quota::sPhysicalSize, physicalSize,
                 pQuotaUidKey, suid + quota::sLogicalSize,  logicalSize,
                 pQuotaGidKey, sgid + quota::sLogicalSize,  logicalSize,
                 pQuotaUidKey, suid + quota::sNumFiles,     "1",
                 pQuotaGidKey, sgid + quota::sNumFiles,     "1"
                );
  // Update the cached information
  pCore.addFile(
    file->getCUid(),
    file->getCGid(),
    file->getSize(),
    size
  );
}

//------------------------------------------------------------------------------
// Remove a file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void
QuarkQuotaNode::removeFile(const IFileMD* file)
{
  const std::string suid = std::to_string(file->getCUid());
  const std::string sgid = std::to_string(file->getCGid());
  const int64_t size = pQuotaStats->getPhysicalSize(file);
  const int64_t logicalSizeInt = file->getSize();
  const std::string minusPhysicalSize = std::to_string(-size);
  const std::string minusLogicalSize = std::to_string(-logicalSizeInt);
  pFlusher->exec("HINCRBYMULTI",
                 pQuotaUidKey, suid + quota::sPhysicalSize, minusPhysicalSize,
                 pQuotaGidKey, sgid + quota::sPhysicalSize, minusPhysicalSize,
                 pQuotaUidKey, suid + quota::sLogicalSize,  minusLogicalSize,
                 pQuotaGidKey, sgid + quota::sLogicalSize,  minusLogicalSize,
                 pQuotaUidKey, suid + quota::sNumFiles,     "-1",
                 pQuotaGidKey, sgid + quota::sNumFiles,     "-1"
                );
  // Update the cached information
  pCore.removeFile(
    file->getCUid(),
    file->getCGid(),
    file->getSize(),
    size
  );
}

//------------------------------------------------------------------------------
// Meld in another quota node
//------------------------------------------------------------------------------
void
QuarkQuotaNode::meld(const IQuotaNode* node)
{
  const QuarkQuotaNode* impl_node = static_cast<const QuarkQuotaNode*>(node);
  // Meld in the uid map info
  qclient::QHash hmap(*pQcl,
                      QuarkQuotaStats::KeyQuotaUidMap(std::to_string(impl_node->getId())));
  std::pair<std::string, std::map<std::string, std::string>> reply;
  std::string cursor = "0";
  constexpr int64_t count = 2000000;

  do {
    reply = hmap.hscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      pFlusher->hincrby(pQuotaUidKey, elem.first, std::stoll(elem.second));
    }
  } while (cursor != "0");

  // Meld in the gid map info
  hmap.setKey(QuarkQuotaStats::KeyQuotaGidMap(std::to_string(
                impl_node->getId())));
  cursor = "0";

  do {
    reply = hmap.hscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      pFlusher->hincrby(pQuotaGidKey, elem.first, std::stoll(elem.second));
    }
  } while (cursor != "0");

  // Update the cached information
  pCore.meld(node->getCore());
}

//------------------------------------------------------------------------------
// Update with information from the backend
//------------------------------------------------------------------------------
void QuarkQuotaNode::updateFromBackend()
{
  std::string cursor = "0";
  constexpr int64_t count = 2000000;
  std::pair<std::string, std::map<std::string, std::string>> reply;
  qclient::QHash uid_map(*pQcl, pQuotaUidKey);
  qclient::QHash gid_map(*pQcl, pQuotaGidKey);
  std::set<std::string> to_delete;

  do {
    reply = uid_map.hscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      size_t pos = elem.first.find(':');
      uint64_t uid = std::stoull(elem.first.substr(0, pos));
      std::string type = elem.first.substr(pos + 1);
      auto it_uid = pCore.mUserInfo.find(uid);

      if (it_uid == pCore.mUserInfo.end()) {
        auto pair = pCore.mUserInfo.emplace(uid, QuotaNodeCore::UsageInfo());
        it_uid = pair.first;
      }

      QuotaNodeCore::UsageInfo& uinfo = it_uid->second;

      if (type == "logical_size") {
        uinfo.space = std::stoull(elem.second);
      } else if (type == "physical_size") {
        uinfo.physicalSpace = std::stoull(elem.second);
      } else if (type == "files") {
        uinfo.files = std::stoull(elem.second);
      }

      // If nothing is used we can drop the entry from the map
      if ((uinfo.space == 0ull) && (uinfo.physicalSpace == 0ull) &&
          (uinfo.files == 0ull)) {
        to_delete.insert(elem.first);
        pCore.mUserInfo.erase(it_uid);
      }
    }
  } while (cursor != "0");

  for (const auto& key_del : to_delete) {
    uid_map.hdel(key_del);
  }

  to_delete.clear();
  cursor = "0";

  do {
    reply = gid_map.hscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      size_t pos = elem.first.find(':');
      uint64_t uid = std::stoull(elem.first.substr(0, pos));
      std::string type = elem.first.substr(pos + 1);
      auto it_gid = pCore.mGroupInfo.find(uid);

      if (it_gid == pCore.mGroupInfo.end()) {
        auto pair = pCore.mGroupInfo.emplace(uid, QuotaNodeCore::UsageInfo());
        it_gid = pair.first;
      }

      QuotaNodeCore::UsageInfo& ginfo = it_gid->second;

      if (type == "logical_size") {
        ginfo.space = std::stoull(elem.second);
      } else if (type == "physical_size") {
        ginfo.physicalSpace = std::stoull(elem.second);
      } else if (type == "files") {
        ginfo.files = std::stoull(elem.second);
      }

      // If nothing is used we can drop the entry from the map
      if ((ginfo.space == 0ull) && (ginfo.physicalSpace == 0ull) &&
          (ginfo.files == 0ull)) {
        to_delete.insert(elem.first);
        pCore.mGroupInfo.erase(it_gid);
      }
    }
  } while (cursor != "0");

  for (const auto& key_del : to_delete) {
    gid_map.hdel(key_del);
  }
}

//------------------------------------------------------------------------------
// Replace underlying QuotaNodeCore object.
//------------------------------------------------------------------------------
void
QuarkQuotaNode::replaceCore(const QuotaNodeCore& updated)
{
  pCore = updated;
  pFlusher->exec("DEL", pQuotaUidKey);
  pFlusher->exec("DEL", pQuotaGidKey);

  for (auto it = pCore.mUserInfo.begin(); it != pCore.mUserInfo.end(); it++) {
    std::string suid = std::to_string(it->first);
    pFlusher->exec("HSET", pQuotaUidKey,
                   suid + quota::sPhysicalSize,
                   std::to_string(it->second.physicalSpace)
                  );
    pFlusher->exec("HSET", pQuotaUidKey,
                   suid + quota::sLogicalSize,
                   std::to_string(it->second.space)
                  );
    pFlusher->exec("HSET", pQuotaUidKey,
                   suid + quota::sNumFiles,
                   std::to_string(it->second.files)
                  );
  }

  for (auto it = pCore.mGroupInfo.begin(); it != pCore.mGroupInfo.end(); it++) {
    std::string sgid = std::to_string(it->first);
    pFlusher->exec("HSET", pQuotaGidKey,
                   sgid + quota::sPhysicalSize,
                   std::to_string(it->second.physicalSpace)
                  );
    pFlusher->exec("HSET", pQuotaGidKey,
                   sgid + quota::sLogicalSize,
                   std::to_string(it->second.space)
                  );
    pFlusher->exec("HSET", pQuotaGidKey,
                   sgid + quota::sNumFiles,
                   std::to_string(it->second.files)
                  );
  }
}


//------------------------------------------------------------------------------
// Update underlying QuotaNodeCore object.
//------------------------------------------------------------------------------
void
QuarkQuotaNode::updateCore(const QuotaNodeCore& updated)
{
  // replace all existing entries from updated and flush them
  pCore << updated;

  for (auto it = updated.mUserInfo.begin(); it != updated.mUserInfo.end(); it++) {
    std::string suid = std::to_string(it->first);
    pFlusher->exec("HSET", pQuotaUidKey,
                   suid + quota::sPhysicalSize,
                   std::to_string(it->second.physicalSpace)
                  );
    pFlusher->exec("HSET", pQuotaUidKey,
                   suid + quota::sLogicalSize,
                   std::to_string(it->second.space)
                  );
    pFlusher->exec("HSET", pQuotaUidKey,
                   suid + quota::sNumFiles,
                   std::to_string(it->second.files)
                  );
  }

  for (auto it = updated.mGroupInfo.begin(); it != updated.mGroupInfo.end(); it++) {
    std::string sgid = std::to_string(it->first);
    pFlusher->exec("HSET", pQuotaGidKey,
                   sgid + quota::sPhysicalSize,
                   std::to_string(it->second.physicalSpace)
                  );
    pFlusher->exec("HSET", pQuotaGidKey,
                   sgid + quota::sLogicalSize,
                   std::to_string(it->second.space)
                  );
    pFlusher->exec("HSET", pQuotaGidKey,
                   sgid + quota::sNumFiles,
                   std::to_string(it->second.files)
                  );
  }
}

//------------------------------------------------------------------------------
// *** Class QuotaStats implementaion ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkQuotaStats::QuarkQuotaStats(qclient::QClient* qcl,
                                 MetadataFlusher* flusher):
  pQcl(qcl), pFlusher(flusher) {}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuarkQuotaStats::~QuarkQuotaStats()
{
  pNodeMap.clear();
}

//------------------------------------------------------------------------------
// Configure the quota service
//------------------------------------------------------------------------------
void
QuarkQuotaStats::configure(const std::map<std::string, std::string>& config)
{
  // Nothing to do, dependencies are passed through the constructor
}

//------------------------------------------------------------------------------
// Get a quota node associated to the container id
//------------------------------------------------------------------------------
IQuotaNode*
QuarkQuotaStats::getQuotaNode(IContainerMD::id_t node_id)
{
  auto it = pNodeMap.find(node_id);

  if (it != pNodeMap.end()) {
    return it->second.get();
  }

  std::string snode_id = std::to_string(node_id);

  if ((pQcl->exists(KeyQuotaUidMap(snode_id)) == 1) ||
      (pQcl->exists(KeyQuotaGidMap(snode_id)) == 1)) {
    QuarkQuotaNode* ptr = new QuarkQuotaNode(this, node_id);
    ptr->updateFromBackend();
    pNodeMap[node_id].reset(ptr);
    return ptr;
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Register a new quota node
//------------------------------------------------------------------------------
IQuotaNode*
QuarkQuotaStats::registerNewNode(IContainerMD::id_t node_id)
{
  std::string snode_id = std::to_string(node_id);

  if (pNodeMap.count(node_id) ||
      (pQcl->exists(KeyQuotaUidMap(snode_id)) == 1) ||
      (pQcl->exists(KeyQuotaGidMap(snode_id)) == 1)) {
    MDException e;
    e.getMessage() << "Quota node already exist: " << snode_id;
    throw e;
  }

  IQuotaNode* ptr = new QuarkQuotaNode(this, node_id);
  pNodeMap[node_id].reset(ptr);
  return ptr;
}

//------------------------------------------------------------------------------
// Remove quota node
//------------------------------------------------------------------------------
void
QuarkQuotaStats::removeNode(IContainerMD::id_t node_id)
{
  auto it = pNodeMap.find(node_id);

  if (it != pNodeMap.end()) {
    pNodeMap.erase(it);
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
QuarkQuotaStats::getAllIds()
{
  std::unordered_set<IContainerMD::id_t> quota_ids;
  qclient::QScanner quota_set(*pQcl, quota::sPrefix + "*:*");

  for (; quota_set.valid(); quota_set.next()) {
    // Extract quota node id
    IContainerMD::id_t id = 0;

    if (ParseQuotaId(quota_set.getValue(), id)) {
      quota_ids.insert(id);
    }
  }

  return quota_ids;
}

//------------------------------------------------------------------------------
// Get quota node uid map key
//------------------------------------------------------------------------------
std::string
QuarkQuotaStats::KeyQuotaUidMap(const std::string& sid)
{
  return quota::sPrefix + sid + ":" + quota::sUidsSuffix;
}

//------------------------------------------------------------------------------
// Get quota node gid map key
//------------------------------------------------------------------------------
std::string
QuarkQuotaStats::KeyQuotaGidMap(const std::string& sid)
{
  return quota::sPrefix + sid + ":" + quota::sGidsSuffix;
}

//------------------------------------------------------------------------------
// Parse quota id from string
//------------------------------------------------------------------------------
bool
QuarkQuotaStats::ParseQuotaId(const std::string& input, IContainerMD::id_t& id)
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
