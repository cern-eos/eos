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
//------------------------------------------------------------------------------
// Account a new file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void QuotaNode::addFile(const IFileMD* file)
{
  pCore.addFile(
    file->getCUid(),
    file->getCGid(),
    file->getSize(),
    pQuotaStats->getPhysicalSize(file)
  );
}

//------------------------------------------------------------------------------
// Remove a file, adjust the size using the size mapping function
//------------------------------------------------------------------------------
void QuotaNode::removeFile(const IFileMD* file)
{
  pCore.removeFile(
    file->getCUid(),
    file->getCGid(),
    file->getSize(),
    pQuotaStats->getPhysicalSize(file)
  );
}

//----------------------------------------------------------------------------
// Meld in another quota node
//----------------------------------------------------------------------------
void QuotaNode::meld(const IQuotaNode* node)
{
  pCore.meld(node->getCore());
}

//------------------------------------------------------------------------------
// Replace underlying QuotaNodeCore object.
//------------------------------------------------------------------------------
void QuotaNode::replaceCore(const QuotaNodeCore &updated)
{
  pCore = updated;
}


//------------------------------------------------------------------------------
// Update underlying QuotaNodeCore object.
//------------------------------------------------------------------------------
void QuotaNode::updateCore(const QuotaNodeCore &updated)
{
  pCore << updated;
}

//------------------------------------------------------------------------------
// Get a quota node associated to the container id
//------------------------------------------------------------------------------
IQuotaNode* QuotaStats::getQuotaNode(IContainerMD::id_t nodeId)
{
  NodeMap::iterator it = pNodeMap.find(nodeId);

  if (it == pNodeMap.end()) {
    return 0;
  }

  return it->second;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
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
std::unordered_set<IContainerMD::id_t>
QuotaStats::getAllIds()
{
  std::unordered_set<IContainerMD::id_t> set_ids;

  for (auto it = pNodeMap.begin(); it != pNodeMap.end(); ++it) {
    (void)set_ids.insert(it->first);
  }

  return set_ids;
}

//------------------------------------------------------------------------------
// Register a new quota node
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
// Remove quota node
//------------------------------------------------------------------------------
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
