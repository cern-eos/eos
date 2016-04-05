/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   User quota accounting
//------------------------------------------------------------------------------

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_on_ramcloud/RamCloudClient.hh"
#include "namespace/ns_on_ramcloud/accounting/QuotaStats.hh"

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
QuotaNode::QuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t node_id):
  IQuotaNode(quotaStats)
{
  pQuotaUidKey = std::to_string(node_id) + QuotaStats::sQuotaUidsSuffix;
  pQuotaGidKey = std::to_string(node_id) + QuotaStats::sQuotaGidsSuffix;
  RAMCloud::RamCloud* client = getRamCloudClient();

  // Create the necessary tables for this node
  try {
    (void) client->getTableId(pQuotaUidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    client->createTable(pQuotaUidKey.c_str());
  }

  try {
    (void) client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    client->createTable(pQuotaGidKey.c_str());
  }
}

//------------------------------------------------------------------------------
// Account a new file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void QuotaNode::addFile(const IFileMD* file)
{
  RAMCloud::RamCloud* client = getRamCloudClient();
  const std::string suid = std::to_string(file->getCUid());
  const std::string sgid = std::to_string(file->getCGid());
  const int64_t size = pQuotaStats->getPhysicalSize(file);
  uint64_t table_uid, table_gid;

  try {
    table_uid = client->getTableId(pQuotaUidKey.c_str());
    table_gid = client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaUidKey.c_str());
    table_gid = client->createTable(pQuotaGidKey.c_str());
  }

  // TODO: all these should be done asynchronously
  try {
    std::string field = suid + sPhysicalSpaceTag;
    client->incrementInt64(table_uid, static_cast<const void*>(field.c_str()),
			   field.length(), size);
    field = sgid + sPhysicalSpaceTag;
    client->incrementInt64(table_gid, static_cast<const void*>(field.c_str()),
			   field.length(), size);
    field = suid + sSpaceTag;
    client->incrementInt64(table_uid, static_cast<const void*>(field.c_str()),
			   field.length(), file->getSize());
    field = sgid + sSpaceTag;
    client->incrementInt64(table_gid, static_cast<const void*>(field.c_str()),
			   field.length(), file->getSize());
    field = suid + sFilesTag;
    client->incrementInt64(table_uid, static_cast<const void*>(field.c_str()),
			   field.length(), 1);
    field = sgid + sFilesTag;
    client->incrementInt64(table_gid, static_cast<const void*>(field.c_str()),
			   field.length(), 1);
  }
  catch (RAMCloud::ClientException&e) {
    // TODO: take some action in case it fails
  }
}

//------------------------------------------------------------------------------
// Remove a file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void QuotaNode::removeFile(const IFileMD* file)
{
  RAMCloud::RamCloud* client = getRamCloudClient();
  const std::string suid = std::to_string(file->getCUid());
  const std::string sgid = std::to_string(file->getCGid());
  int64_t size = pQuotaStats->getPhysicalSize(file);
  uint64_t table_uid, table_gid;

  try {
    table_uid = client->getTableId(pQuotaUidKey.c_str());
    table_gid = client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaUidKey.c_str());
    table_gid = client->createTable(pQuotaGidKey.c_str());
  }

  try {
    std::string field = suid + sPhysicalSpaceTag;
    client->incrementInt64(table_uid, field.c_str(), field.length(), -size);
    field = sgid + sPhysicalSpaceTag;
    client->incrementInt64(table_gid, field.c_str(), field.length(), -size);
    field = suid + sSpaceTag;
    size = static_cast<int64_t>(file->getSize());
    client->incrementInt64(table_uid, field.c_str(), field.length(), -size);
    field = sgid + sSpaceTag;
    client->incrementInt64(table_gid, field.c_str(), field.length(), -size);
    field = suid + sFilesTag;
    client->incrementInt64(table_uid, field.c_str(), field.length(), -1);
    field = sgid + sFilesTag;
    client->incrementInt64(table_gid, field.c_str(), field.length(), -1);
  }
  catch (RAMCloud::ClientException& e) {
    // TODO: take some action in case it fails
  }
}

//------------------------------------------------------------------------------
// Meld in another quota node
//------------------------------------------------------------------------------
void QuotaNode::meld(const IQuotaNode* node)
{
  RAMCloud::RamCloud* client = getRamCloudClient();
  uint64_t table_uid, table_gid;

  try {
    table_uid = client->getTableId(pQuotaUidKey.c_str());
    table_gid = client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaUidKey.c_str());
    table_gid = client->createTable(pQuotaGidKey.c_str());
  }

  try {
    int64_t value;
    uint32_t key_len = 0, data_len = 0;
    const void* key_buff = 0, *data_buff = 0;
    // Merge all the uid values
    uint64_t table_id = client->getTableId(node->getUidKey().c_str());
    RAMCloud::TableEnumerator iter1(*client, table_id, false);

    while (iter1.hasNext())
    {
      iter1.nextKeyAndData(&key_len, &key_buff, &data_len, &data_buff);
      value = *reinterpret_cast<const int64_t*>(data_buff);
      client->incrementInt64(table_uid, key_buff, key_len, value);
    }

    // Merge all the gid values
    table_id = client->getTableId(node->getGidKey().c_str());
    RAMCloud::TableEnumerator iter2(*client, table_id, false);

    while (iter2.hasNext())
    {
      iter2.nextKeyAndData(&key_len, &key_buff, &data_len, &data_buff);
      value = *reinterpret_cast<const int64_t*>(data_buff);
      client->incrementInt64(table_gid, key_buff, key_len, value);
    }
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    // nothing to merge
  }
  catch (RAMCloud::ClientException& e) {
    // TODO: take some action in case it fails
  }
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getUsedSpaceByUser(uid_t uid)
{
  uint64_t val;
  uint64_t table_uid;
  std::string field = std::to_string(uid) + sSpaceTag;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_uid = client->getTableId(pQuotaUidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaUidKey.c_str());
  }

  try {
    RAMCloud::Buffer bval;
    client->read(table_uid, field.c_str(), field.length(), &bval);
    val = (uint64_t)*bval.getOffset<int64_t>(0);
  }
  catch (RAMCloud::ClientException& e) {
    val = 0UL;
  }

  return val;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getUsedSpaceByGroup(gid_t gid)
{
  uint64_t val;
  uint64_t table_uid;
  std::string field = std::to_string(gid) + sSpaceTag;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_uid = client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaGidKey.c_str());
  }

  try {
    RAMCloud::Buffer bval;
    client->read(table_uid, field.c_str(), field.length(), &bval);
    val = (uint64_t)*bval.getOffset<int64_t>(0);
  }
  catch (RAMCloud::ClientException& e) {
    val = 0UL;
  }

  return val;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getPhysicalSpaceByUser(uid_t uid)
{
  uint64_t val;
  uint64_t table_uid;
  std::string field = std::to_string(uid) + sPhysicalSpaceTag;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_uid = client->getTableId(pQuotaUidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaUidKey.c_str());
  }

  try {
    RAMCloud::Buffer bval;
    client->read(table_uid, field.c_str(), field.length(), &bval);
    val = (uint64_t)*bval.getOffset<int64_t>(0);
  }
  catch (RAMCloud::ClientException& e) {
    val = 0UL;
  }

  return val;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getPhysicalSpaceByGroup(gid_t gid)
{
  uint64_t val;
  uint64_t table_uid;
  std::string field = std::to_string(gid) + sPhysicalSpaceTag;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_uid = client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaGidKey.c_str());
  }

  try {
    RAMCloud::Buffer bval;
    client->read(table_uid, field.c_str(), field.length(), &bval);
    val = (uint64_t)*bval.getOffset<int64_t>(0);
  }
  catch (RAMCloud::ClientException& e) {
    val = 0UL;
  }

  return val;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t
QuotaNode::getNumFilesByUser(uid_t uid)
{
  uint64_t val;
  uint64_t table_uid;
  std::string field = std::to_string(uid) + sFilesTag;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_uid = client->getTableId(pQuotaUidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaUidKey.c_str());
  }

  try {
    RAMCloud::Buffer bval;
    client->read(table_uid, field.c_str(), field.length(), &bval);
    val = (uint64_t)*bval.getOffset<int64_t>(0);
  }
  catch (RAMCloud::ClientException& e) {
    val = 0UL;
  }

  return val;
}

//----------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//----------------------------------------------------------------------------
uint64_t
QuotaNode::getNumFilesByGroup(gid_t gid)
{
  uint64_t val;
  uint64_t table_uid;
  std::string field = std::to_string(gid) + sFilesTag;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_uid = client->getTableId(pQuotaGidKey.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_uid = client->createTable(pQuotaGidKey.c_str());
  }

  try {
    RAMCloud::Buffer bval;
    client->read(table_uid, field.c_str(), field.length(), &bval);
    val = (uint64_t)*bval.getOffset<int64_t>(0);
  }
  catch (RAMCloud::ClientException& e) {
    val = 0UL;
  }

  return val;
}

//------------------------------------------------------------------------------
// Get the set of uids for which information is stored in the current quota
// node.
//------------------------------------------------------------------------------
std::vector<unsigned long>
QuotaNode::getUids()
{
  std::string suid;
  std::vector<unsigned long> uids;
  RAMCloud::RamCloud* client = getRamCloudClient();
  uint64_t table_id;

  try {
    std::string key;
    table_id = client->getTableId(pQuotaUidKey.c_str());
    RAMCloud::TableEnumerator iter(*client, table_id, true);
    uint32_t size = 0;
    const void* buffer = 0;

    // The keys have to following format: uid1:space, uid1:physical_space,
    // uid1:files ... uidn:files.
    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      RAMCloud::Object object(buffer, size);
      key = static_cast<const char*>(object.getKey());
      suid = key.substr(0, key.find(':'));
      uids.push_back(std::stoul(suid));
    }
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return uids;
  }

  return uids;
}

//----------------------------------------------------------------------------
// Get the set of gids for which information is stored in the current quota
// node.
//----------------------------------------------------------------------------
std::vector<unsigned long>
QuotaNode::getGids()
{
  std::string sgid;
  std::vector<unsigned long> gids;
  RAMCloud::RamCloud* client = getRamCloudClient();
  uint64_t table_id;

  try {
    std::string key;
    table_id = client->getTableId(pQuotaGidKey.c_str());
    RAMCloud::TableEnumerator iter(*client, table_id, true);
    uint32_t size = 0;
    const void* buffer = 0;

    // The keys have to following format: gid1:space, gid1:physical_space,
    // gid1:files ... gidn:files.
    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      RAMCloud::Object object(buffer, size);
      key = static_cast<const char*>(object.getKey());
      sgid = key.substr(0, key.find(':'));
      gids.push_back(std::stoul(sgid));
    }
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return gids;
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
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuotaStats::~QuotaStats()
{
  for (auto it = pNodeMap.begin(); it != pNodeMap.end(); ++it)
    delete it->second;

  pNodeMap.clear();
}

//------------------------------------------------------------------------------
// Get a quota node associated to the container id
//------------------------------------------------------------------------------
IQuotaNode* QuotaStats::getQuotaNode(IContainerMD::id_t node_id)
{
  if (pNodeMap.count(node_id))
    return pNodeMap[node_id];

  uint64_t table_id;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    RAMCloud::Buffer bval;
    std::string key = std::to_string(node_id);
    table_id = client->getTableId(sSetQuotaIds.c_str());
    client->read(table_id, key.c_str(), key.length(), &bval);
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    return nullptr;
  }
  catch (RAMCloud::ClientException& e) {
    return nullptr;
  }

  IQuotaNode* ptr {new QuotaNode(this, node_id)};
  pNodeMap[node_id] = ptr;
  return ptr;
}

//------------------------------------------------------------------------------
// Register a new quota node
//------------------------------------------------------------------------------
IQuotaNode* QuotaStats::registerNewNode(IContainerMD::id_t node_id)
{
  uint64_t table_id;
  std::string snode_id = std::to_string(node_id);
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_id = client->getTableId(sSetQuotaIds.c_str());
  }
  catch (RAMCloud::TableDoesntExistException& e) {
    table_id = client->createTable(sSetQuotaIds.c_str());
  }

  RAMCloud::Buffer bval;

  try {
    client->read(table_id, snode_id.c_str(), snode_id.length(), &bval);
  }
  catch (RAMCloud::ClientException& e) {}

  if (bval.size())
  {
    MDException e;
    e.getMessage() << "Quota node already exist: " << node_id;
    throw e;
  }

  try {
    client->write(table_id, snode_id.c_str(), snode_id.length(), nullptr);
  }
  catch (RAMCloud::ClientException& e) {
    MDException e;
    e.getMessage() << "Failed to register new quota node: " << node_id;
    throw e;
  }

  IQuotaNode* ptr {new QuotaNode(this, node_id)};
  pNodeMap[node_id] = ptr;
  return ptr;
}

//------------------------------------------------------------------------------
// Remove quota node
//------------------------------------------------------------------------------
void QuotaStats::removeNode(IContainerMD::id_t node_id)
{
  uint64_t table_id;
  std::string snode_id = std::to_string(node_id);
  RAMCloud::RamCloud* client = getRamCloudClient();

  if (pNodeMap.count(node_id)) {
    pNodeMap.erase(node_id);
  }

  try {
    table_id = client->getTableId(sSetQuotaIds.c_str());
    client->remove(table_id, snode_id.c_str(), snode_id.length());
  }
  catch (RAMCloud::TableDoesntExistException& e) {}
  catch (RAMCloud::ClientException& e) {
    MDException e;
    e.getMessage() << "Quota node " << node_id << " does not exist in set";
    throw e;
  }

  // Delete the hmaps associated with the current node
  try {
    std::string key = snode_id + sQuotaUidsSuffix;
    client->dropTable(key.c_str());
    key = snode_id + sQuotaGidsSuffix;
    client->dropTable(key.c_str());
  }
  catch (RAMCloud::ClientException& e) {}
}

//------------------------------------------------------------------------------
// Get the set of all quota node ids. The quota node id corresponds to the
// container id.
//------------------------------------------------------------------------------
std::set<std::string>
QuotaStats::getAllIds()
{
  uint64_t table_id;
  std::set<std::string> uids;
  RAMCloud::RamCloud* client = getRamCloudClient();

  try {
    table_id = client->getTableId(sSetQuotaIds.c_str());
    RAMCloud::TableEnumerator iter(*client, table_id, false);
    uint32_t key_len = 0, data_len = 0;
    const void* key_buff = 0, *data_buff = 0;

    while (iter.hasNext())
    {
      iter.nextKeyAndData(&key_len, &key_buff, &data_len, &data_buff);
      uids.emplace(static_cast<const char*>(data_buff), data_len);
    }
  }
  catch (RAMCloud::TableDoesntExistException& e){}

  return uids;
}

EOSNSNAMESPACE_END
