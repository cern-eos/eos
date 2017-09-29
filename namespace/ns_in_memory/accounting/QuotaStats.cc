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
#include "namespace/ns_in_memory/accounting/QuotaStats.hh"

namespace eos
{
//----------------------------------------------------------------------------
// Account a new file, adjust the size using the size mapping function
//----------------------------------------------------------------------------
void QuotaNode::addFile(const IFileMD* file)
{
  uint64_t size = pQuotaStats->getPhysicalSize(file);
  UsageInfo& user  = pUserUsage[file->getCUid()];
  UsageInfo& group = pGroupUsage[file->getCGid()];
  user.physicalSpace  += size;
  group.physicalSpace += size;
  user.space   += file->getSize();
  group.space  += file->getSize();
  user.files++;
  group.files++;
}

//----------------------------------------------------------------------------
// Remove a file, adjust the size using the size mapping function
//----------------------------------------------------------------------------
void QuotaNode::removeFile(const IFileMD* file)
{
  uint64_t size = pQuotaStats->getPhysicalSize(file);
  UsageInfo& user  = pUserUsage[file->getCUid()];
  UsageInfo& group = pGroupUsage[file->getCGid()];
  user.physicalSpace  -= size;
  group.physicalSpace -= size;
  user.space   -= file->getSize();
  group.space  -= file->getSize();
  user.files--;
  group.files--;
}

//----------------------------------------------------------------------------
// Meld in another quota node
//----------------------------------------------------------------------------
void QuotaNode::meld(const IQuotaNode* node)
{
  const QuotaNode* qnode = static_cast<const QuotaNode*>(node);

  for (auto it1 = qnode->pUserUsage.begin(); it1 != qnode->pUserUsage.end();
       ++it1) {
    pUserUsage[it1->first] += it1->second;
  }

  for (auto it2 = qnode->pGroupUsage.begin(); it2 != qnode->pGroupUsage.end();
       ++it2) {
    pGroupUsage[it2->first] += it2->second;
  }
}

//----------------------------------------------------------------------------
// Get the set of uids for which information is stored in the current quota
// node.
//----------------------------------------------------------------------------
std::vector<unsigned long>
QuotaNode::getUids()
{
  std::vector<unsigned long> uids;

  for (auto it = pUserUsage.begin(); it != pUserUsage.end(); ++it) {
    uids.push_back(it->first);
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
  std::vector<unsigned long> gids;

  for (auto it = pGroupUsage.begin(); it != pGroupUsage.end(); ++it) {
    gids.push_back(it->first);
  }

  return gids;
}

//----------------------------------------------------------------------------
// Get a quota node associated to the container id
//----------------------------------------------------------------------------
IQuotaNode* QuotaStats::getQuotaNode(IContainerMD::id_t nodeId)
{
  NodeMap::iterator it = pNodeMap.find(nodeId);

  if (it == pNodeMap.end()) {
    return 0;
  }

  return it->second;
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
QuotaStats::~QuotaStats()
{
  for (auto it = pNodeMap.begin(); it != pNodeMap.end(); ++it) {
    delete it->second;
  }
}

//------------------------------------------------------------------------------
// Get the set of all quota node ids. The quota node id corresponds to the
// container id.
//------------------------------------------------------------------------------
std::set<std::string>
QuotaStats::getAllIds()
{
  char buff[48];
  std::set<std::string> set_ids;

  for (auto it = pNodeMap.begin(); it != pNodeMap.end(); ++it) {
    snprintf(buff, 48, "%lu", it->first);
    (void)set_ids.insert(std::string(buff));
  }

  return set_ids;
}

//----------------------------------------------------------------------------
// Register a new quota node
//----------------------------------------------------------------------------
IQuotaNode* QuotaStats::registerNewNode(IContainerMD::id_t nodeId)
{
  NodeMap::iterator it = pNodeMap.find(nodeId);

  if (it != pNodeMap.end()) {
    MDException e;
    e.getMessage() << "Quota node already exist: " << nodeId;
    throw e;
  }

  QuotaNode* node = new QuotaNode(this , nodeId);
  pNodeMap[nodeId] = node;
  return node;
}

//----------------------------------------------------------------------------
// Remove quota node
//----------------------------------------------------------------------------
void QuotaStats::removeNode(IContainerMD::id_t nodeId)
{
  NodeMap::iterator it = pNodeMap.find(nodeId);

  if (it == pNodeMap.end()) {
    MDException e;
    e.getMessage() << "Quota node does not exist: " << nodeId;
    throw e;
  }

  delete it->second;
  pNodeMap.erase(it);
}
}
