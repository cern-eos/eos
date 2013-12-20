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

using namespace std;

EOSMGMNAMESPACE_BEGIN

ostream& SlowTreeNode::display(ostream &os) const{
  os<<pNodeInfo.mGeotag;
  return os;
}

ostream& SlowTreeNode::recursiveDisplay(ostream &os, const string &prefix) const{
  stringstream ss;
  ss<<prefix;
  os << right << setw(8) << setfill('-') << *this;
  ss << right << setw(7) << setfill(' ') << "";
  if(pChildren.empty())
    os << "@" << pNodeInfo.mHost << endl;
  else
  {
    os << endl;
    for(tNodeMap::const_iterator it=pChildren.begin();it!=pChildren.end();it++)
    //for(tGeoTagOrderedBranchesSet::const_iterator it=pGeoTagOrderedChildren.begin();it!=pGeoTagOrderedChildren.end();it++)
    {
      if( it!=pChildren.end() && ++tNodeMap::const_iterator(it)==pChildren.end())
      //if( it!=pGeoTagOrderedChildren.end() && ++tGeoTagOrderedBranchesSet::const_iterator(it)==pGeoTagOrderedChildren.end())
      { // final branch
        os << ss.str() << "`--";
        it->second->recursiveDisplay(os,ss.str()+="   ");
        //(*it)->recursiveDisplay(os,ss.str()+="   ");
      }
      else
      { // intermediate branch
        os << ss.str() << "|--";
        it->second->recursiveDisplay(os,ss.str()+="|  ");
        //(*it)->recursiveDisplay(os,ss.str()+="|  ");
      }
    }
  }
  return os;
}

ostream& SlowTree::display(ostream &os) const{
  return pRootNode.recursiveDisplay(os);
}

SlowTreeNode* SlowTree::insert( const TreeNodeInfo *info, const TreeNodeStateFloat *state)
{
  SlowTreeNode* startFrom=&pRootNode;
  ostringstream oss;
  oss<<info->mGeotag<<"::"<<info->mFsId;
  std::string fullgeotag;
  SlowTreeNode*result = insert(
      info,
      state,
      fullgeotag,
      oss.str(),
      startFrom
  );
  return result;
}

SlowTreeNode* SlowTree::insert( const TreeNodeInfo *info, const TreeNodeStateFloat *state, std::string &fullgeotag, const std::string &partialgeotag, SlowTreeNode* startFrom)
{
  if(partialgeotag.empty()) return NULL;
  // find the first :: separator
  size_t sepPos;
  for(sepPos=0; sepPos<partialgeotag.length()-1;sepPos++)
    if(partialgeotag[sepPos]==':' && partialgeotag[sepPos+1]==':')
      break;
  if(sepPos==partialgeotag.length()-1) sepPos=partialgeotag.length();

  string geoTagAtom=partialgeotag.substr(0,sepPos);
  if(!fullgeotag.empty()) fullgeotag += "::";
  fullgeotag += geoTagAtom;

  if( ! startFrom->pChildren.count(geoTagAtom) ) {
    startFrom->pChildren[geoTagAtom] = new SlowTreeNode;
    startFrom->pChildren[geoTagAtom]->pFather = startFrom;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.mGeotag=geoTagAtom;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.mFullGeotag=fullgeotag;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.mFsId=0;
    startFrom->pChildren[geoTagAtom]->pNodeInfo.mNodeType=TreeNodeInfo::intermediate;
    pNodeCount++; // add one node to the counter
  }

  startFrom = startFrom->pChildren[geoTagAtom];

  if(sepPos==partialgeotag.length()) { // update the attributes
    startFrom->pNodeInfo.mHost=info->mHost;
    startFrom->pNodeInfo.mFsId=info->mFsId;
    startFrom->pNodeInfo.mNodeType=TreeNodeInfo::fs;
    startFrom->pNodeState=*state;
    return startFrom;
  }
  else
    return insert(
        info,
        state,
        fullgeotag,
        partialgeotag.substr(sepPos+2,partialgeotag.length()-sepPos-2),
        startFrom
    );
}

bool SlowTree::buildFastStrctures(FastPlacementTree *fpt, FastAccessTree *fat,
    FastTreeInfo *fastinfo, Fs2TreeIdxMap *fs2idx,
    GeoTag2NodeIdxMap *geo2node) const {

  // check that the FastTree are large enough
  if(fat->getMaxNodeCount()<getNodeCount() && fpt->getMaxNodeCount()<getNodeCount())
    return false;
  if(geo2node->getMaxNodeCount()<getNodeCount()) {
    if(geo2node->getMaxNodeCount()==0)
      geo2node->selfAllocate(getNodeCount());
  }
  else
        return false;

  // update the SlowwTree before converting it
  ((SlowTree*)this)->pRootNode.update();

  // create the node vector layout
  vector<vector<const SlowTreeNode*> >nodesByDepth; // [depth][branchIdxAtThisDepth]
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
      //for(SlowTreeNode::tPlctPriorityOrderedBranchSet::const_iterator cit=(*it)->pAccessPriorityOrderedChildren.begin();cit!=(*it)->pAccessPriorityOrderedChildren.end();cit++)
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
          !(*it)->WriteFastTreeNodeTemplate<AccessPriorityRandWeightEvaluator, AccessPriorityComparator>(fat->pNodes+nodecount) ||
          !(*it)->WriteFastTreeNodeTemplate<PlacementPriorityRandWeightEvaluator, PlacementPriorityComparator>(fpt->pNodes+nodecount) )
        return false;

      // update the links
        // father first
      if(firstnode)
        fpt->pNodes[nodecount].mTreeData.mFatherIdx = fat->pNodes[nodecount].mTreeData.mFatherIdx = 0;
      else
        fpt->pNodes[nodecount].mTreeData.mFatherIdx = fat->pNodes[nodecount].mTreeData.mFatherIdx = (tFastTreeIdx)nodes2idxChildren[(*it)->pFather];
        // then children
      tFastTreeIdx nchildren = 0;
      fpt->pNodes[nodecount].mTreeData.mFirstBranchIdx = fat->pNodes[nodecount].mTreeData.mFirstBranchIdx = linkcount;
      {
        SlowTreeNode::tPlctPriorityOrderedBranchSet::const_iterator cit2=(*it)->pPlctPriorityOrderedChildren.begin();
        for(SlowTreeNode::tAccessPriorityOrderedBranchSet::const_iterator cit=(*it)->pAccessPriorityOrderedChildren.begin();cit!=(*it)->pAccessPriorityOrderedChildren.end();cit++)
        {
          assert(cit2!=(*it)->pPlctPriorityOrderedChildren.end());
          ( fat->pBranches[linkcount].mSonIdx = (tFastTreeIdx)nodes2idxChildren[*cit] );
          ( fpt->pBranches[linkcount].mSonIdx = (tFastTreeIdx)nodes2idxChildren[*cit2] );
          linkcount++; nchildren++; cit2++;

        }
      }
      //std::cout<< "nchildren = "<<nchildren<<std::endl;
      fpt->pNodes[nodecount].mTreeData.mNbChildren = fat->pNodes[nodecount].mTreeData.mNbChildren = nchildren;
      // fill in the default TreeNodePlacement
      ////////////////////static_cast<TreeNodeSlots&>(fat->pNodes[nodecount].mFileData) = (*it)->pNodePlacement;
      fat->pNodes[nodecount].mFileData.mNbFreeSlots  = 0; // no replica placed so, no free slot for access yet
      fpt->pNodes[nodecount].mFileData.mNbFreeSlots  = (*it)->pLeavesCount; // replica placed so, all slot are available to place a new one
      fpt->pNodes[nodecount].mFileData.mNbTakenSlots = fat->pNodes[nodecount].mFileData.mNbTakenSlots = 0;
      // update mLastHighestPriorityIdx
      if(nchildren) {
        {
          SlowTreeNode::TreeNodeAccessGreater tnacomp;
          const SlowTreeNode::tAccessPriorityOrderedBranchSet::const_iterator bit=(*it)->pAccessPriorityOrderedChildren.begin();
          fat->pNodes[nodecount].mFileData.mLastHighestPriorityOffset=0;
          for(SlowTreeNode::tAccessPriorityOrderedBranchSet::const_iterator cit=bit;cit != ((*it)->pAccessPriorityOrderedChildren.end());cit++)
          {
            if( !tnacomp(*cit,*bit) ) break;
            fat->pNodes[nodecount].mFileData.mLastHighestPriorityOffset++;
          }
          fat->pNodes[nodecount].mFileData.mLastHighestPriorityOffset--;
        }
        {
          SlowTreeNode::TreeNodePlacementGreater tnpcomp;
          const SlowTreeNode::tPlctPriorityOrderedBranchSet::const_iterator bit=(*it)->pPlctPriorityOrderedChildren.begin();
          fpt->pNodes[nodecount].mFileData.mLastHighestPriorityOffset=0;
          for(SlowTreeNode::tPlctPriorityOrderedBranchSet::const_iterator cit=bit;cit != ((*it)->pPlctPriorityOrderedChildren.end());cit++)
          {
            if( !tnpcomp(*cit,*bit) ) break;
            fpt->pNodes[nodecount].mFileData.mLastHighestPriorityOffset++;
          }
          fpt->pNodes[nodecount].mFileData.mLastHighestPriorityOffset--;
        }
      }
      // fill in the FastTreeInfo
      (*fastinfo)[nodecount] = (*it)->pNodeInfo;
      // fill in tFs2TreeIdxMap
      if((*it)->pNodeInfo.mNodeType==TreeNodeInfo::fs)
        fs2idxMap[(*it)->pNodeInfo.mFsId] = nodecount;
      // iterate the node
      nodecount++;
    }
    firstnode=false;
  }

  // some sanity checks
  CHK1 if(
      nodecount != pNodeCount   ||
      linkcount != pNodeCount-1 ||
      count != pNodeCount
      ) {
    assert(false);
    return false;
  }

  // create the node vector layout
   nodesByDepth.clear(); // [depth][branchIdxAtThisDepth]
   // map<const SlowTreeNode*,int> nodes2idx; // keep this information
   nodesByDepth.resize(nodesByDepth.size()+1);
   nodesByDepth.back().push_back(&pRootNode);
   count=0;
   nodes2idxGeoTag[&pRootNode]=count;
   geo2node->pNodes[count].mFastTreeIndex=0;
   strncpy(geo2node->pNodes[count].tag,pRootNode.pNodeInfo.mGeotag.c_str(),GeoTag2NodeIdxMap::gMaxTagSize);
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
       for(SlowTreeNode::tGeoTagOrderedBranchesSet::const_iterator cit=(*it)->pGeoTagOrderedChildren.begin();cit!=(*it)->pGeoTagOrderedChildren.end();cit++)
       {
         nodesByDepth.back().push_back(*cit);
         nodes2idxGeoTag[*cit]=count;
         geo2node->pNodes[count].mFastTreeIndex=nodes2idxChildren[*cit];
         strncpy(geo2node->pNodes[count].tag,(*cit)->pNodeInfo.mGeotag.c_str(),GeoTag2NodeIdxMap::gMaxTagSize);
         count++;
         if(!godeeper && !(*cit)->pChildren.empty() ) godeeper=true;
       }
     }
   }

   nodecount=0;
   for(vector<vector<const SlowTreeNode*> >::const_iterator dit=nodesByDepth.begin();dit!=nodesByDepth.end();dit++)
   {
     for(vector<const SlowTreeNode*>::const_iterator it=dit->begin();it!=dit->end();it++)
     {
         geo2node->pNodes[nodecount].mFirstBranch = nodes2idxGeoTag[*(*it)->pGeoTagOrderedChildren.begin()];
         geo2node->pNodes[nodecount].mNbBranch    = (tFastTreeIdx)(*it)->pChildren.size();
         nodecount++;
     }
   }

  // some sanity checks
  CHK1 if(
      nodecount != pNodeCount   ||
      linkcount != pNodeCount-1 ||
      count != pNodeCount
      ) {
    assert(false);
    return false;
  }

  // fill in the outsourced data
  if(fs2idx->pMaxSize == 0)
    fs2idx->selfAllocate((tFastTreeIdx)fs2idxMap.size());
  if(fs2idx->pMaxSize<fs2idxMap.size())
    assert(false); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  count = 0;
  for(std::map<unsigned long,tFastTreeIdx>::const_iterator it=fs2idxMap.begin();it!=fs2idxMap.end();it++) {
    fs2idx->pFsIds[count]       = it->first;
    fs2idx->pNodeIdxs[count++]  = it->second;
  }
  fs2idx->pSize = fs2idxMap.size();

  fat->pFs2Idx = fs2idx;
  fat->pTreeInfo = fastinfo;

  fpt->pFs2Idx = fs2idx;
  fpt->pTreeInfo = fastinfo;

  return true;
}

EOSMGMNAMESPACE_END
