//------------------------------------------------------------------------------
// @file SchedulingSlowTree.cc
// @author Geoffray Adde - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#define DEFINE_TREECOMMON_MACRO
#include "mgm/geotree/SchedulingSlowTree.hh"

#include <iomanip>
#include <sstream>
#include <iterator>
#include <iostream>
#include <sstream>
#include <cassert>
#include <vector>
#include <map>
#include <stdlib.h>

using namespace std;

EOSMGMNAMESPACE_BEGIN

ostream& SlowTreeNode::display(ostream &os) const
{
  os<<pNodeInfo.geotag;
  return os;
}

ostream& SlowTreeNode::recursiveDisplay(ostream &os, bool useColors, const string &prefix) const
{
  std::string consoleEscapeCode,consoleReset;
  if(useColors)
  {
    bool isReadable = (pNodeState.mStatus & Readable);
    bool isDisabled = (pNodeState.mStatus & Disabled);

    bool isWritable = (pNodeState.mStatus & Writable);
    bool isAvailable = (pNodeState.mStatus & Available);
    bool isDraining = (pNodeState.mStatus & Draining);
    bool isFs = pChildren.empty();
    consoleEscapeCode = "\033[";
    consoleReset = "\033[0m";

    if(isDisabled) // DISABLED
    consoleEscapeCode = consoleEscapeCode + ("2;39;49m");
    else
    {
      if(isFs && isDraining)
      consoleEscapeCode = consoleEscapeCode + ("1;33;");
      else
      consoleEscapeCode = consoleEscapeCode + ("1;39;");

      if( !isAvailable
          || (isFs && (!(isReadable || isWritable))) ) // UNAVAILABLE OR NOIO
      consoleEscapeCode = consoleEscapeCode + ("41");
      else if(isFs)
      {
        if( isReadable && ! isWritable ) // RO case
        consoleEscapeCode = consoleEscapeCode + "44";
        else if( !isReadable && isWritable )// WO case
        consoleEscapeCode = consoleEscapeCode + "43";
        else
        consoleEscapeCode = consoleEscapeCode + "49";
      }
      else
      {
        consoleEscapeCode = consoleEscapeCode + "49";
      }
      consoleEscapeCode = consoleEscapeCode + "m";
    }
  }

  stringstream ss;
  ss<<prefix;
  os << right << setw(8) << setfill('-') <<consoleEscapeCode<< *this;
  ss << right << setw(7) << setfill(' ') << "";
  if(pChildren.empty())
  {
    os << "@" << pNodeInfo.host << " ["<< pLeavesCount<<","<<pNodeCount<<","<<fsStatusToStr(pNodeState.mStatus)<< "]"<< consoleReset <<endl;
  }
  else
  {
    os << " ["<< pLeavesCount<<","<<pNodeCount<< "]"<< consoleReset << endl;
    for(tNodeMap::const_iterator it=pChildren.begin();it!=pChildren.end();it++)
    {
      std::string color;
      if(useColors)
      {
        if((it->second->pNodeState.mStatus & Disabled))
        color= "\033[2;39;49m";
        else
        color= "\033[1;39;49m";
      }

      if( it!=pChildren.end() && ++tNodeMap::const_iterator(it)==pChildren.end())
      { // final branch
        os << ss.str() << color << "`--";
        it->second->recursiveDisplay(os,useColors,ss.str()+="   ");
        os << ss.str() << endl;
      }
      else
      { // intermediate branch
        os << ss.str() << color << "|--";
        it->second->recursiveDisplay(os,useColors,ss.str()+="|  ");
      }
    }
  }
  return os;
}

ostream& SlowTree::display(ostream &os, bool useColors) const
{
  return pRootNode.recursiveDisplay(os, useColors);
}

SlowTreeNode* SlowTree::insert( const TreeNodeInfo *info, const TreeNodeStateFloat *state)
{
  SlowTreeNode* startFrom=&pRootNode;
  ostringstream oss;
  oss<<info->geotag<<"::"<<info->fsId;
  std::string fullgeotag;
  SlowTreeNode*result = insert(
      info,
      state,
      fullgeotag,
      oss.str(),
      startFrom,
      NULL
  );
  return result;
}

SlowTreeNode* SlowTree::insert( const TreeNodeInfo *info, const TreeNodeStateFloat *state, std::string &fullGeotag, const std::string &partialGeotag, SlowTreeNode* startFrom, SlowTreeNode* startedConstructingAt)
{
  if(partialGeotag.empty()) return NULL;

  // find the first :: separator
  size_t sepPos;
  for(sepPos=0; sepPos<partialGeotag.length()-1;sepPos++)
  if(partialGeotag[sepPos]==':' && partialGeotag[sepPos+1]==':')
  break;
  if(sepPos==partialGeotag.length()-1) sepPos=partialGeotag.length();

  string geoTagAtom=partialGeotag.substr(0,sepPos);
  if(!fullGeotag.empty()) fullGeotag += "::";
  fullGeotag += geoTagAtom;

  bool newbranch = ! startFrom->pChildren.count(geoTagAtom);

  if( newbranch )
  {
    startFrom->pChildren[geoTagAtom] = new SlowTreeNode;
    startFrom->pChildren[geoTagAtom]->pFather = startFrom;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.geotag=geoTagAtom;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.fullGeotag=fullGeotag;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.fsId=0;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.nodeType=TreeNodeInfo::intermediate;
    pNodeCount++; // add one node to the counter
    if(!startedConstructingAt) startedConstructingAt = startFrom->pChildren[geoTagAtom];
  }

  startFrom = startFrom->pChildren[geoTagAtom];

  if(sepPos==partialGeotag.length())
  { // update the attributes
    startFrom->pNodeInfo.host=info->host;
    startFrom->pNodeInfo.fsId=info->fsId;
    startFrom->pNodeInfo.nodeType=TreeNodeInfo::fs;
    startFrom->pNodeState=*state;
    if(newbranch)
    { // update the pLeavesCount
      for(SlowTreeNode* node = startFrom; node!=NULL; node = node->pFather)
      {
        node->pLeavesCount++;
      }
    }
    else
    assert(false);

    if(startedConstructingAt)
    { // update the node count if needed
      size_t nconstr = 0;
      size_t metStarted = false;
      for(SlowTreeNode *it = startFrom; it!=NULL; it = it->pFather )
      {
        if(!metStarted) nconstr++;
        if(it == startedConstructingAt ) metStarted = true;
        it->pNodeCount += nconstr;
      }
    }

    __EOSMGM_TREECOMMON_DBG2__ eos_static_debug("inserted fsid=%lu   geotag=%s   fullgeotag=%s",startFrom->pNodeInfo.fsId,startFrom->pNodeInfo.geotag.c_str(),startFrom->pNodeInfo.fullGeotag.c_str());
    return startFrom;

  }
  else
  return insert(
      info,
      state,
      fullGeotag,
      partialGeotag.substr(sepPos+2,partialGeotag.length()-sepPos-2),
      startFrom,
      startedConstructingAt
  );
}

bool SlowTree::remove( const TreeNodeInfo *info)
{
  ostringstream oss;
  if(info->geotag.empty())
  return false; // should not be used with empty fullgeotag

  std::string fullgeotag;
  if(info->fsId)
  {
    ostringstream oss;
    oss<<info->geotag<<"::"<<info->fsId;
    fullgeotag = oss.str();
  }
  else
  fullgeotag = info->geotag;

  size_t pos=0;
  size_t ppos=0;
  string geoTagAtom;
  SlowTreeNode *node = &pRootNode;

  while((pos=fullgeotag.find("::",pos+1)) != string::npos)
  {
    pos+=2; // skip the "::"
    geoTagAtom = fullgeotag.substr(ppos,pos-ppos-2);// take "::" into account
    if(!node->pChildren.count(geoTagAtom))
    return false;// no matching leaf found!
    node = node->pChildren[geoTagAtom];
    ppos = pos;
  }
  geoTagAtom = fullgeotag.substr(ppos,string::npos);
  if(!node->pChildren.count(geoTagAtom))
  return false; // no matching leaf found!
  node = node->pChildren[geoTagAtom];

  // arrived to the end of the string so, we can delete the matched branch.
  // simplify the tree by erasing the biggest empty branch
  while( (node->pFather!=NULL) && (node->pFather!=&pRootNode) && (node->pFather->pChildren.size()==1) )
  node = node->pFather;
  node->pFather->pChildren.erase(node->pNodeInfo.geotag);

  size_t lcount = node->pLeavesCount;
  size_t ncount = node->pNodeCount;

  for(SlowTreeNode *it = node; it!=NULL; it=it->pFather )
  { // update the recursive leaves count in the tree
    it->pLeavesCount -= lcount;
    it->pNodeCount -= ncount;
  }

  pNodeCount -= ncount;
  delete node;

  return true;
}

SlowTreeNode* SlowTree::moveToNewGeoTag(SlowTreeNode* node, const std::string newGeoTag)
{
  if(node->pChildren.size())
  return NULL; // this function can only move a leaf. Moving a branch would involve running throuh the branch and get all the leaves.
  TreeNodeInfo info = node->pNodeInfo;
  TreeNodeStateFloat state = node->pNodeState;

  info.geotag = node->pNodeInfo.fullGeotag.substr(0,node->pNodeInfo.fullGeotag.rfind("::"));
  if(!remove(&info))
  return NULL;
  info.geotag = newGeoTag;
  return insert(&info,&state);
}

bool SlowTree::buildFastStrcturesSched(
    FastPlacementTree *fpt, FastROAccessTree *froat, FastRWAccessTree *frwat,
    FastBalancingPlacementTree *fbpt, FastBalancingAccessTree *fbat,
    FastDrainingPlacementTree *fdpt, FastDrainingAccessTree *fdat,
    FastTreeInfo *fastinfo, Fs2TreeIdxMap *fs2idx, GeoTag2NodeIdxMap *geo2node) const
{

  // check that the FastTree are large enough
  if(froat->getMaxNodeCount()<getNodeCount() || frwat->getMaxNodeCount()<getNodeCount()
      || fpt->getMaxNodeCount()<getNodeCount()
      || fdat->getMaxNodeCount()<getNodeCount() || fdpt->getMaxNodeCount()<getNodeCount()
      || fbat->getMaxNodeCount()<getNodeCount() || fbpt->getMaxNodeCount()<getNodeCount())
  return false;

  if(geo2node->getMaxNodeCount()<getNodeCount())
  {
    if(geo2node->getMaxNodeCount()==0)
    geo2node->selfAllocate(getNodeCount());
    else
    assert(false);
    //return false;
  }

  __EOSMGM_TREECOMMON_DBG1__
  if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    stringstream ss;
    ss << (*this);
    eos_static_debug("SLOWTREE IS %s",ss.str().c_str());
  }

  // update the SlowwTree before converting it
  ((SlowTree*)this)->pRootNode.update();

  // create the node vector layout
  vector<vector<const SlowTreeNode*> >nodesByDepth;// [depth][branchIdxAtThisDepth]
  map<const SlowTreeNode*,int> nodes2idxChildren;
  map<const SlowTreeNode*,int> nodes2idxGeoTag;
  nodesByDepth.resize(nodesByDepth.size()+1);
  nodesByDepth.back().push_back(&pRootNode);
  size_t count=0;
  nodes2idxChildren[&pRootNode]=count++;
  bool godeeper=(bool)pRootNode.pChildren.size();
  while(godeeper)
  {
    // create a new level
    nodesByDepth.resize(nodesByDepth.size()+1);
    // iterate through the nodes of the last level
    for(vector<const SlowTreeNode*>::const_iterator it=(nodesByDepth.end()-2)->begin();it!=(nodesByDepth.end()-2)->end();it++)
    {
      godeeper = false;
      // iterate through the children of each of those nodes
      for(SlowTreeNode::tNodeMap::const_iterator cit=(*it)->pChildren.begin();cit!=(*it)->pChildren.end();cit++)
      {
        nodesByDepth.back().push_back(cit->second);
        nodes2idxChildren[cit->second]=count++;
        if(!godeeper && !(*cit).second->pChildren.empty() ) godeeper=true;
      }
    }
  }

  // copy the vector layout of the node to the FastTree
  size_t nodecount=0;
  size_t linkcount=0;
  std::map<unsigned long,tFastTreeIdx> fs2idxMap;
  fastinfo->clear();
  fastinfo->resize(pNodeCount);
  // it's not necessary to clear the fs2idx map because a given fs should appear only in one placement group
  bool firstnode=true;
  for(vector<vector<const SlowTreeNode*> >::const_iterator dit=nodesByDepth.begin();dit!=nodesByDepth.end();dit++)
  {
    for(vector<const SlowTreeNode*>::const_iterator it=dit->begin();it!=dit->end();it++)
    {
      // write the content of the node
      if(
          !(*it)->writeFastTreeNodeTemplate<PlacementPriorityRandWeightEvaluator, PlacementPriorityComparator,eos::common::FileSystem::fsid_t>(fpt->pNodes+nodecount) )
      {
        assert(false);
        return false;
      }

      // update the links
      // father first
      if(firstnode)
      fpt->pNodes[nodecount].treeData.fatherIdx = 0;
      else
      fpt->pNodes[nodecount].treeData.fatherIdx = (tFastTreeIdx)nodes2idxChildren[(*it)->pFather];
      // then children
      tFastTreeIdx nchildren = 0;
      fpt->pNodes[nodecount].treeData.firstBranchIdx = linkcount;
      {
        for(auto cit=(*it)->pChildren.begin();cit!=(*it)->pChildren.end();cit++)
        {
          ( fpt->pBranches[linkcount].sonIdx = (tFastTreeIdx)nodes2idxChildren[cit->second] );
          linkcount++; nchildren++;
        }
      }

      fpt->pNodes[nodecount].treeData.childrenCount = nchildren;
      // fill in the default TreeNodePlacement
      fpt->pNodes[nodecount].fileData.freeSlotsCount = (*it)->pLeavesCount;// replica placed so, all slot are available to place a new one
      fpt->pNodes[nodecount].fileData.takenSlotsCount = 0;

      // fill in the FastTreeInfo
      (*fastinfo)[nodecount] = (*it)->pNodeInfo;
      // fill in tFs2TreeIdxMap
      if((*it)->pNodeInfo.nodeType==TreeNodeInfo::fs)
      fs2idxMap[(*it)->pNodeInfo.fsId] = nodecount;
      // iterate the node
      nodecount++;
    }
    firstnode=false;
  }
  // finish the placment tree
  fpt->pNodeCount = pNodeCount;
  fpt->updateTree();

  //finish the RO access tree
  if( fpt->copyToFastTree(froat))
  { assert(false); return false;}
  for(tFastTreeIdx i=0;i<froat->pNodeCount;i++)
  froat->pNodes[i].fileData.freeSlotsCount = 0;
  froat->pNodeCount = pNodeCount;
  froat->updateTree();
  //finish the RW access tree
  if( fpt->copyToFastTree(frwat))
  { assert(false); return false;}
  for(tFastTreeIdx i=0;i<frwat->pNodeCount;i++)
  frwat->pNodes[i].fileData.freeSlotsCount = 0;
  frwat->pNodeCount = pNodeCount;
  frwat->updateTree();
  // copy them to the other trees (balancing and draining)
  if( fpt->copyToFastTree(fbpt) )
  { assert(false); return false;}
  fbpt->updateTree();
  if( fpt->copyToFastTree(fdpt) )
  { assert(false); return false;}
  fdpt->updateTree();
  if( froat->copyToFastTree(fbat) )
  { assert(false); return false;}
  fbat->updateTree();
  if( froat->copyToFastTree(fdat) )
  { assert(false); return false;}
  fdat->updateTree();

  // some sanity checks
  __EOSMGM_TREECOMMON_CHK1__ if(
      nodecount != pNodeCount ||
      linkcount != pNodeCount-1 ||
      count != pNodeCount
  )
  {
    assert(false);
    return false;
  }

  // create the node vector layout
  nodesByDepth.clear();// [depth][branchIdxAtThisDepth]
  nodesByDepth.resize(nodesByDepth.size()+1);
  nodesByDepth.back().push_back(&pRootNode);
  count=0;
  nodes2idxGeoTag[&pRootNode]=count;
  geo2node->pNodes[count].fastTreeIndex=0;
  strncpy(geo2node->pNodes[count].tag,pRootNode.pNodeInfo.geotag.c_str(),GeoTag2NodeIdxMap::gMaxTagSize);
  count++;
  godeeper=(bool)pRootNode.pChildren.size();
  while(godeeper)
  {
    // create a new level
    nodesByDepth.resize(nodesByDepth.size()+1);
    // iterate through the nodes of the last level
    for(vector<const SlowTreeNode*>::const_iterator it=(nodesByDepth.end()-2)->begin();it!=(nodesByDepth.end()-2)->end();it++)
    {
      godeeper = false;
      // iterate through the children of each of those nodes
      for(auto cit=(*it)->pChildren.begin();cit!=(*it)->pChildren.end();cit++)
      {
        nodesByDepth.back().push_back(cit->second);
        nodes2idxGeoTag[cit->second]=count;
        geo2node->pNodes[count].fastTreeIndex=nodes2idxChildren[cit->second];
        strncpy(geo2node->pNodes[count].tag,cit->second->pNodeInfo.geotag.c_str(),GeoTag2NodeIdxMap::gMaxTagSize);
        count++;
        if(!godeeper && !cit->second->pChildren.empty() ) godeeper=true;
      }
    }
  }

  nodecount=0;
  for(vector<vector<const SlowTreeNode*> >::const_iterator dit=nodesByDepth.begin();dit!=nodesByDepth.end();dit++)
  {
    for(vector<const SlowTreeNode*>::const_iterator it=dit->begin();it!=dit->end();it++)
    {
      geo2node->pNodes[nodecount].branchCount = (tFastTreeIdx)(*it)->pChildren.size();
      geo2node->pNodes[nodecount].firstBranch = geo2node->pNodes[nodecount].branchCount?
      nodes2idxGeoTag[(*it)->pChildren.begin()->second]:
      0;
      nodecount++;
    }
  }
  geo2node->pSize = nodecount;

  // some sanity checks
  __EOSMGM_TREECOMMON_CHK1__ if(
      nodecount != pNodeCount ||
      linkcount != pNodeCount-1 ||
      count != pNodeCount
  )
  {
    eos_static_alert("Unable to generate the fast tree because of a failed sanity check.");
    return false;
  }

  // fill in the outsourced data
  if(fs2idx->pMaxSize == 0)
  fs2idx->selfAllocate((tFastTreeIdx)fs2idxMap.size());
  if(fs2idx->pMaxSize<fs2idxMap.size())
  {
    eos_static_crit("could not generate the fast tree because the fs2idx is too small");
    return false;
  }
  count = 0;
  for(std::map<unsigned long,tFastTreeIdx>::const_iterator it=fs2idxMap.begin();it!=fs2idxMap.end();it++)
  {
    fs2idx->pFsIds[count] = it->first;
    fs2idx->pNodeIdxs[count++] = it->second;
  }
  fs2idx->pSize = fs2idxMap.size();

  froat->pFs2Idx = frwat->pFs2Idx = fpt->pFs2Idx = fdat->pFs2Idx = fdpt->pFs2Idx = fbat->pFs2Idx = fbpt->pFs2Idx = fs2idx;
  froat->pTreeInfo = frwat->pTreeInfo = fpt->pTreeInfo = fdat->pTreeInfo = fdpt->pTreeInfo = fbat->pTreeInfo = fbpt->pTreeInfo = fastinfo;

  __EOSMGM_TREECOMMON_CHK2__
  fpt->checkConsistency(0,true);
  __EOSMGM_TREECOMMON_CHK2__
  fbpt->checkConsistency(0,true);
  __EOSMGM_TREECOMMON_CHK2__
  fdpt->checkConsistency(0,true);
  __EOSMGM_TREECOMMON_CHK2__
  froat->checkConsistency(0,true);
  __EOSMGM_TREECOMMON_CHK2__
  frwat->checkConsistency(0,true);
  __EOSMGM_TREECOMMON_CHK2__
  fbat->checkConsistency(0,true);
  __EOSMGM_TREECOMMON_CHK2__
  fdat->checkConsistency(0,true);

  __EOSMGM_TREECOMMON_DBG1__ if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    stringstream ss;
    ss << (*fpt);
    eos_static_debug("FASTTREE IS %s",ss.str().c_str());
  }

  fpt->checkConsistency(0,true);

  return true;
}

bool SlowTree::buildFastStrcturesGW(
    FastGatewayAccessTree *fgat, Host2TreeIdxMap *host2idx, FastTreeInfo *fastinfo, GeoTag2NodeIdxMap *geo2node) const
{

  // check that the FastTree are large enough
  if(fgat->getMaxNodeCount()<getNodeCount())
  return false;

  if(geo2node->getMaxNodeCount()<getNodeCount())
  {
    if(geo2node->getMaxNodeCount()==0)
    geo2node->selfAllocate(getNodeCount());
    else
    assert(false);
    //return false;
  }

  __EOSMGM_TREECOMMON_DBG1__
  if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    stringstream ss;
    ss << (*this);
    eos_static_debug("SLOWTREE IS %s",ss.str().c_str());
  }

  // update the SlowwTree before converting it
  ((SlowTree*)this)->pRootNode.update();

  // create the node vector layout
  vector<vector<const SlowTreeNode*> >nodesByDepth;// [depth][branchIdxAtThisDepth]
  map<const SlowTreeNode*,int> nodes2idxChildren;
  map<const SlowTreeNode*,int> nodes2idxGeoTag;
  nodesByDepth.resize(nodesByDepth.size()+1);
  nodesByDepth.back().push_back(&pRootNode);
  size_t count=0;
  nodes2idxChildren[&pRootNode]=count++;
  bool godeeper=(bool)pRootNode.pChildren.size();
  while(godeeper)
  {
    // create a new level
    nodesByDepth.resize(nodesByDepth.size()+1);
    // iterate through the nodes of the last level
    for(vector<const SlowTreeNode*>::const_iterator it=(nodesByDepth.end()-2)->begin();it!=(nodesByDepth.end()-2)->end();it++)
    {
      godeeper = false;
      // iterate through the children of each of those nodes
      for(SlowTreeNode::tNodeMap::const_iterator cit=(*it)->pChildren.begin();cit!=(*it)->pChildren.end();cit++)
      {
        nodesByDepth.back().push_back(cit->second);
        nodes2idxChildren[cit->second]=count++;
        if(!godeeper && !(*cit).second->pChildren.empty() ) godeeper=true;
      }
    }
  }

  // copy the vector layout of the node to the FastTree
  size_t nodecount=0;
  size_t linkcount=0;
  //std::map<unsigned long,tFastTreeIdx> fs2idxMap;
  std::map<std::string,tFastTreeIdx> host2idxMap;
  fastinfo->clear();
  fastinfo->resize(pNodeCount);
  // it's not necessary to clear the fs2idx map because a given fs should appear only in one placement group
  bool firstnode=true;
  for(vector<vector<const SlowTreeNode*> >::const_iterator dit=nodesByDepth.begin();dit!=nodesByDepth.end();dit++)
  {
    for(vector<const SlowTreeNode*>::const_iterator it=dit->begin();it!=dit->end();it++)
    {
      // write the content of the node
      if(
          !(*it)->writeFastTreeNodeTemplate<GatewayPriorityRandWeightEvaluator, GatewayPriorityComparator,char*>(fgat->pNodes+nodecount) )
      {
        assert(false);
        return false;
      }

      // update the links
      // father first
      if(firstnode)
        fgat->pNodes[nodecount].treeData.fatherIdx = 0;
      else
        fgat->pNodes[nodecount].treeData.fatherIdx = (tFastTreeIdx)nodes2idxChildren[(*it)->pFather];
      // then children
      tFastTreeIdx nchildren = 0;
      fgat->pNodes[nodecount].treeData.firstBranchIdx = linkcount;
      {
        for(auto cit=(*it)->pChildren.begin();cit!=(*it)->pChildren.end();cit++)
        {
          ( fgat->pBranches[linkcount].sonIdx = (tFastTreeIdx)nodes2idxChildren[cit->second] );
          linkcount++; nchildren++;
        }
      }

      fgat->pNodes[nodecount].treeData.childrenCount = nchildren;
      // fill in the default TreeNodePlacement
      fgat->pNodes[nodecount].fileData.freeSlotsCount = (*it)->pLeavesCount;// replica placed so, all slot are available to place a new one
      fgat->pNodes[nodecount].fileData.takenSlotsCount = 0;

      // fill in the FastTreeInfo
      (*fastinfo)[nodecount] = (*it)->pNodeInfo;
      // fill in tFs2TreeIdxMap
      if((*it)->pNodeInfo.nodeType==TreeNodeInfo::fs)
      host2idxMap[(*it)->pNodeInfo.host] = nodecount;
      // iterate the node
      nodecount++;
    }
    firstnode=false;
  }
  // finish the gateway tree
  fgat->updateTree();
  fgat->pNodeCount = pNodeCount;

  // some sanity checks
  __EOSMGM_TREECOMMON_CHK1__ if(
      nodecount != pNodeCount ||
      linkcount != pNodeCount-1 ||
      count != pNodeCount
  )
  {
    assert(false);
    return false;
  }

  // create the node vector layout
  nodesByDepth.clear();// [depth][branchIdxAtThisDepth]
  nodesByDepth.resize(nodesByDepth.size()+1);
  nodesByDepth.back().push_back(&pRootNode);
  count=0;
  nodes2idxGeoTag[&pRootNode]=count;
  geo2node->pNodes[count].fastTreeIndex=0;
  strncpy(geo2node->pNodes[count].tag,pRootNode.pNodeInfo.geotag.c_str(),GeoTag2NodeIdxMap::gMaxTagSize);
  count++;
  godeeper=(bool)pRootNode.pChildren.size();
  while(godeeper)
  {
    // create a new level
    nodesByDepth.resize(nodesByDepth.size()+1);
    // iterate through the nodes of the last level
    for(vector<const SlowTreeNode*>::const_iterator it=(nodesByDepth.end()-2)->begin();it!=(nodesByDepth.end()-2)->end();it++)
    {
      godeeper = false;
      // iterate through the children of each of those nodes
      for(auto cit=(*it)->pChildren.begin();cit!=(*it)->pChildren.end();cit++)
      {
        nodesByDepth.back().push_back(cit->second);
        nodes2idxGeoTag[cit->second]=count;
        geo2node->pNodes[count].fastTreeIndex=nodes2idxChildren[cit->second];
        strncpy(geo2node->pNodes[count].tag,cit->second->pNodeInfo.geotag.c_str(),GeoTag2NodeIdxMap::gMaxTagSize);
        count++;
        if(!godeeper && !cit->second->pChildren.empty() ) godeeper=true;
      }
    }
  }

  nodecount=0;
  for(vector<vector<const SlowTreeNode*> >::const_iterator dit=nodesByDepth.begin();dit!=nodesByDepth.end();dit++)
  {
    for(vector<const SlowTreeNode*>::const_iterator it=dit->begin();it!=dit->end();it++)
    {
      geo2node->pNodes[nodecount].branchCount = (tFastTreeIdx)(*it)->pChildren.size();
      geo2node->pNodes[nodecount].firstBranch = geo2node->pNodes[nodecount].branchCount?
      nodes2idxGeoTag[(*it)->pChildren.begin()->second]:
      0;
      nodecount++;
    }
  }
  geo2node->pSize = nodecount;

  // some sanity checks
  __EOSMGM_TREECOMMON_CHK1__ if(
      nodecount != pNodeCount ||
      linkcount != pNodeCount-1 ||
      count != pNodeCount
  )
  {
    eos_static_alert("Unable to generate the fast tree because of a failed sanity check.");
    return false;
  }

  // fill in the outsourced data
  if(host2idx->pMaxSize == 0)
  host2idx->selfAllocate((tFastTreeIdx)host2idxMap.size());
  if(host2idx->pMaxSize<host2idxMap.size())
  {
    eos_static_crit("could not generate the fast tree because the fs2idx is too small");
    return false;
  }
  count = 0;
  for(auto it=host2idxMap.begin();it!=host2idxMap.end();it++)
  {
    strncpy(host2idx->pBuffer+count*host2idx->pStrLen,it->first.c_str(),host2idx->pStrLen);
    //host2idx->pBuffer[count] = it->first;
    host2idx->pNodeIdxs[count++] = it->second;
  }
  host2idx->pSize = host2idxMap.size();

  fgat->pFs2Idx = host2idx;
  fgat->pTreeInfo = fastinfo;

  __EOSMGM_TREECOMMON_CHK2__
  fgat->checkConsistency(2,true);

  __EOSMGM_TREECOMMON_DBG1__ if(eos::common::Logging::gLogMask & LOG_MASK(LOG_DEBUG))
  {
    stringstream ss;
    ss << (*fgat);
    eos_static_debug("FASTTREE IS %s",ss.str().c_str());
  }

  fgat->checkConsistency(0,true);

  return true;
}

EOSMGMNAMESPACE_END
