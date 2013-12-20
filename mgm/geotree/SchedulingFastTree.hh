//------------------------------------------------------------------------------
// @file SchedulingFastTree.hh
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

#ifndef __EOSMGM_FASTTREE__H__
#define __EOSMGM_FASTTREE__H__

#include "mgm/geotree/SchedulingTreeCommon.hh"
#include <cstddef>
#include <ostream>
#include <string>
#include <vector>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <iomanip>
#define DEFINE_TREECOMMON_MACRO

#pragma pack(push,1)

/*----------------------------------------------------------------------------*/
/**
 * @file SchedulingFastTree.hh
 *
 * @brief Class representing the geotag-based tree structure of a scheduling group
 *
 * There are two representations of this tree structure:
 * - the first one defined is SchedulingSlowTree.hh
 *   is flexible and the tree can be shaped easily
 *   on the other hand, it's big and possibly scattered in the memory, so its
 *   access speed might be low
 * - the second one is a set a compact and fast structures (defined in the current file)
 *   these structure ares compact and contiguous in memory which makes them fast
 *   the shape of the underlying tree cannot be changed once they are constructed
 * Typically, a tree is constructed using the first representation (also referred as "slow").
 * Then, a representation of the second kind (also referred as "fast") is created from the
 * previous. It's then used to issue all the file scheduling operations at a high throughput (MHz)
 *
 */

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class mapping a geotag to the closest node in a FastTree
 *        This closest node is described by its index in the FastTree
 *
 */
/*----------------------------------------------------------------------------*/
class GeoTag2NodeIdxMap : public SchedTreeBase
{
  friend class SlowTree;
  friend class SlowTreeNode;

  static const size_t gMaxTagSize = 9; // 8+1
  struct Node
  {
    char tag[gMaxTagSize];
    tFastTreeIdx mFastTreeIndex;
    tFastTreeIdx mFirstBranch;
    tFastTreeIdx mNbBranch;
  };

  bool pSelfAllocated;
  tFastTreeIdx pMaxSize;
  tFastTreeIdx pSize;
  Node* pNodes;

  // !!! numbering in GeoTag order
  void
  search(const char *tag, tFastTreeIdx &startFrom) const
  {
    if (*tag == 0)
      return;
    int cmp;
    unsigned char k = 0;
    while (tag[k + 1] != 0 && !(tag[k + 1] == ':' && tag[k] == ':') && k < gMaxTagSize)
      k++;
    unsigned char strl;
    bool godeeper = false;

    if (tag[k] == ':' && tag[k + 1] == ':')
    {
      strl = k;
      godeeper = true;
    }
    else
      strl = (unsigned char) (((size_t) (k + 1)) < gMaxTagSize) ? (k + 1) : gMaxTagSize;

    // dichotomy search on the label
    tFastTreeIdx left = pNodes[startFrom].mFirstBranch;
    tFastTreeIdx right = pNodes[startFrom].mFirstBranch + pNodes[startFrom].mNbBranch - 1;
    char *lefts = pNodes[left].tag;
    char *rights = pNodes[right].tag;

    // narrow down the interval
    while (right - left > 1)
    {
      tFastTreeIdx mid = (left + right) / 2;
      char *mids = pNodes[mid].tag;
      cmp = strncmp(mids, tag, strl);
      if (cmp < 0)
      {
        left = mid;
        lefts = pNodes[mid].tag;
      }
      else if (!cmp)
      {
        //startFrom = pNodes[mid].mFirstBranch;
        startFrom = mid;
        goto next;
      }
      else
      {
        right = mid;
        rights = pNodes[mid].tag;
      }
    }

    // check the final interval
    if (!strncmp(lefts, tag, strl))
    {
      //startFrom = pNodes[left].mFirstBranch;
      startFrom = left;
      goto next;
    }
    else if (!strncmp(rights, tag, strl))
    {
      //startFrom = pNodes[right].mFirstBranch;
      startFrom = right;
      goto next;
    }
    else
      return;

    next: if (godeeper)
      search(tag + k + 2, startFrom);
    return;
  }

public:
  GeoTag2NodeIdxMap()
  {
    pMaxSize = 0;
    pSize = 0;
    pNodes = 0;
    pSelfAllocated = false;
  }

  ~ GeoTag2NodeIdxMap()
  {
    if (pSelfAllocated)
      selfUnallocate();
  }

  bool
  selfAllocate(tFastTreeIdx size)
  {
    pSelfAllocated = true;
    pMaxSize = size;
    pNodes = new Node[size];
    return true;
  }
  bool
  selfUnallocate()
  {
    delete[] pNodes;
    return true;
  }

  bool
  allocate(void *buffer, size_t bufsize, tFastTreeIdx size)
  {
    size_t memsize = sizeof(Node) * size;
    if (bufsize < memsize)
      return false;
    pMaxSize = size;
    pSelfAllocated = false;
    pNodes = (Node*) buffer;
    return true;
  }

  inline tFastTreeIdx
  getMaxNodeCount() const
  {
    return pMaxSize;
  }

  inline tFastTreeIdx
  getClosestFastTreeNode(const char *tag) const
  {
    tFastTreeIdx node = 0;
    search(tag, node);
    return pNodes[node].mFastTreeIndex;
  }
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class mapping an FsId to its position in a FastTree
 *        This position is described by the index of the corresponding node
 *        in the FastTree. The FsId is templated.
 *
 */
/*----------------------------------------------------------------------------*/
template<typename T>
class FsId2NodeIdxMap : public SchedTreeBase
{
  friend class SlowTree;
  friend class SlowTreeNode;

  // size
  tFastTreeIdx pMaxSize;
  tFastTreeIdx pSize;
  bool pSelfAllocated;

  // data
  T *pFsIds;
  tFastTreeIdx *pNodeIdxs;

public:
  // creation, allocation, destruction
  FsId2NodeIdxMap()
  {
    pMaxSize = 0;
    pSelfAllocated = false;
  }

  ~ FsId2NodeIdxMap()
  {
    if (pSelfAllocated)
      selfUnallocate();
  }

  bool
  selfAllocate(tFastTreeIdx size)
  {
    pSelfAllocated = true;
    pMaxSize = size;
    pFsIds = (T*) new char[(sizeof(T) + sizeof(tFastTreeIdx)) * size];
    pNodeIdxs = (tFastTreeIdx*) (pFsIds + size);
    return true;
  }
  bool
  selfUnallocate()
  {
    delete[] pFsIds;
    return true;
  }

  bool
  allocate(void *buffer, size_t bufsize, tFastTreeIdx size)
  {
    size_t memsize = (sizeof(T) + sizeof(tFastTreeIdx)) * size;
    if (bufsize < memsize)
      return false;
    pMaxSize = size;
    pSelfAllocated = false;
    pFsIds = (T*) buffer;
    pNodeIdxs = (tFastTreeIdx*) (pFsIds + size);
    return true;
  }

  bool
  get(const T &fsid, const tFastTreeIdx * &idx) const
  {
    tFastTreeIdx left = 0;
    tFastTreeIdx right = pSize - 1;

    if (!pSize || fsid > pFsIds[right] || fsid < pFsIds[left])
      return false;

    if (fsid == pFsIds[right])
    {
      idx = &pNodeIdxs[right];
      return true;
    }

    while (right - left > 1)
    {
      tFastTreeIdx mid = (left + right) / 2;
      if (fsid < pFsIds[mid])
        right = mid;
      else
        left = mid;
    }

    if (fsid == pFsIds[left])
    {
      idx = &pNodeIdxs[left];
      return true;
    }

    return false;
  }

  class const_iterator
  {
    friend class FsId2NodeIdxMap;
    T* pFsIdPtr;
    tFastTreeIdx* pNodeIdxPtr;
    const_iterator(T* const & fsidptr, tFastTreeIdx* const & nodeidxptr) :
      pFsIdPtr(fsidptr), pNodeIdxPtr(nodeidxptr)
    {
    }
  public:
    const_iterator()
    {
    }
    const_iterator&
    operator =(const const_iterator &it)
    {
      pFsIdPtr = it.pFsIdPtr;
      pNodeIdxPtr = it.pNodeIdxPtr;
      return *this;
    }
    const_iterator&
    operator ++()
    {
      pFsIdPtr++;
      pNodeIdxPtr++;
      return *this;
    }
    const_iterator
    operator ++(int unused)
    {
      pFsIdPtr++;
      pNodeIdxPtr++;
      return const_iterator(pFsIdPtr - 1, pNodeIdxPtr - 1);
    }
    bool
    operator ==(const const_iterator &it) const
    {
      return pNodeIdxPtr == it.pNodeIdxPtr && pFsIdPtr == it.pFsIdPtr;
    }
    bool
    operator !=(const const_iterator &it) const
    {
      return pNodeIdxPtr != it.pNodeIdxPtr || pFsIdPtr != it.pFsIdPtr;
    }
    std::pair<T, tFastTreeIdx>
    operator *() const
    {
      return std::make_pair(*pFsIdPtr, *pNodeIdxPtr);
    }
  };

  inline const_iterator
  begin() const
  {
    return const_iterator(pFsIds, pNodeIdxs);
  }
  inline const_iterator
  end() const
  {
    return const_iterator(pFsIds + pSize, pNodeIdxs + pSize);
  }

};

/*----------------------------------------------------------------------------*/
/**
 * @brief Implementation of FsId2NodeIdxMap with the default FsId type.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FsId2NodeIdxMap<unsigned long> Fs2TreeIdxMap;

std::ostream&
operator <<(std::ostream &os, const Fs2TreeIdxMap &info);
inline std::ostream&
operator <<(std::ostream &os, const Fs2TreeIdxMap &info)
{
  // inline because of GCC MAP BUG : end iterator is corrupted when moved to c file
  for (Fs2TreeIdxMap::const_iterator it = info.begin(); it != info.end(); it++)
  {
    os << std::setfill(' ') << "fs=" << std::setw(20) << (*it).first << " -> " << "idx=" << (int) (*it).second << std::endl;
  }
  return os;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for file placement.
 *
 */
/*----------------------------------------------------------------------------*/
class PlacementPriorityComparator
{
public:
  PlacementPriorityComparator()
  {
  }
  inline signed char
  operator()(const SchedTreeBase::TreeNodeState<unsigned char>* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
      const SchedTreeBase::TreeNodeState<unsigned char>* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
  {
    return SchedTreeBase::ComparePlct<unsigned char>(lefts, leftp, rights, rightp);
  }
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative weights of branches in the tree
 *        having the same priority. This weight is used for file placement
 *        by random sampling in all the branches having the same priority.
 *
 */
/*----------------------------------------------------------------------------*/
class PlacementPriorityRandWeightEvaluator
{
public:
  PlacementPriorityRandWeightEvaluator()
  {
  }
  inline unsigned char
  operator()(const SchedTreeBase::TreeNodeState<unsigned char> &state, const SchedTreeBase::TreeNodeSlots &plct) const
  {
    return state.mDlScore;
  }
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for file access.
 *
 */
/*----------------------------------------------------------------------------*/
class AccessPriorityComparator
{
public:
  AccessPriorityComparator()
  {
  }
  inline signed char
  operator()(const SchedTreeBase::TreeNodeState<unsigned char>* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
      const SchedTreeBase::TreeNodeState<unsigned char>* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
  {
    return SchedTreeBase::CompareAccess<unsigned char>(lefts, leftp, rights, rightp);
  }
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative weights of branches in the tree
 *        having the same priority. This weight is used for file access
 *        by random sampling in all the branches having the same priority.
 *
 */
/*----------------------------------------------------------------------------*/
class AccessPriorityRandWeightEvaluator
{
public:
  AccessPriorityRandWeightEvaluator()
  {
  }
  inline unsigned char
  operator()(const SchedTreeBase::TreeNodeState<unsigned char> &state, const SchedTreeBase::TreeNodeSlots &plct) const
  {
    return plct.mNbFreeSlots;
  }
};

template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting> class InverseFastTreeLookUpTable;

/*----------------------------------------------------------------------------*/
/**
 * @brief This is the generic fast tree class.
 *
 *        Every leaf in the tree hold information about free and taken slots.
 *        The main purpose of this class is to find a free slot and update this
 *        information very quickly.
 *        The way to do this is consistent at any depth in the tree:
 *        - Find the highest priority branch(es).
 *        - Among these highest priority branches, select one by weighted
 *          random sampling
 *        The class has two template arguments allowing to specify
 *        - the relative priority of branches
 *        - the weighting of these branches in the random sampling
 *        This very is then used to implement FastAcessTree and FastPlacementTree
 *        just by changing these templates arguments.
 *        The speed is achieved using a compact memory layout.
 *        Nodes of the tree (and the data they contain) are all aligned as a vector
 *        at the beginning of the class.
 *        After this vector, there is a second vector containing the branches.
 *        A branch is just a node number. There is as many branches as nodes (-1 actually).
 *        Each node contains the index of the first child branch in the branch vector
 *        and the number of branches it owns.
 *        For each node, its branches in the branch vector are kept in a
 *        decreasing priority order.
 *        Note that the compactness of the memory layout depends directly on
 *        the size of the typedef tFastTreeIdx. it also dictates the maximum
 *        number of nodes in the tree.
 *
 */
/*----------------------------------------------------------------------------*/
template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting>
class FastTree : public SchedTreeBase
{
  friend class SlowTree;
  friend class SlowTreeNode;

  struct Branch
  {
    tFastTreeIdx mSonIdx;
  };

  typedef FastTree<FsDataMemberForRand, FsAndFileDataComparerForBranchSorting> tSelf;
  typedef TreeNodeStateChar FsData;

  struct FileData : public TreeNodeSlots
  {
    tFastTreeIdx mLastHighestPriorityOffset;
  };

  struct TreeStructure
  {
    tFastTreeIdx mFatherIdx;
    tFastTreeIdx mFirstBranchIdx;
    tFastTreeIdx mNbChildren;
  };

  struct FastTreeNode
  {
    TreeStructure mTreeData;
    FsData mFsData;
    FileData mFileData;
  };

  // the layout of a FastTree in memory is just as follow
  // 1xFastTreeNode then n1*Branch then 1*FastTreeNode then n2*Branch then ... then 1*FastBranch then np*Branch
  // note that there is exactly p-1 branches overall because every node but the root node has exactly one branch as a father
  // for this FastTree with p branches, the memory size is p*sizeof(FastTreeNode)+p*sizeof(Branch)
  //          for 2 locations 1 building/location 1 room/building 30 racks overall 100 fs overall (135 nodes) 135*(9+2) -> 1485 bytes
  bool pSelfAllocated;
  tFastTreeIdx pMaxNodeCount;
  FastTreeNode *pNodes;
  Branch *pBranches;

  // outsourced data
  Fs2TreeIdxMap* pFs2Idx;
  FastTreeInfo * pTreeInfo;

  inline bool
  FTLower(const FastTree::FsData* const &lefts, const FastTree::FileData* const &leftp, const FastTree::FsData* const &rights,
      const FastTree::FileData* const &rightp) const
  {
    //return pBranchComp(lefts, static_cast<const TreeNodePlacement * const &>(leftp), rights, static_cast<const TreeNodePlacement * const &>(rightp));
    return pBranchComp(lefts, static_cast<const TreeNodeSlots * const &>(leftp), rights, static_cast<const TreeNodeSlots * const &>(rightp)) > 0;
  }

  inline bool
  FTLowerNode(const tFastTreeIdx&left, const tFastTreeIdx&right) const
  {
    return FTLower(&pNodes[left].mFsData, &pNodes[left].mFileData, &pNodes[right].mFsData, &pNodes[right].mFileData);
  }

  inline bool
  FTLowerBranch(const tFastTreeIdx&left, const tFastTreeIdx&right) const
  {
    return FTLower(&pNodes[pBranches[left].mSonIdx].mFsData, &pNodes[pBranches[left].mSonIdx].mFileData, &pNodes[pBranches[right].mSonIdx].mFsData,
        &pNodes[pBranches[right].mSonIdx].mFileData);
  }

  inline bool
  FTEqual(const FastTree::FsData* const &lefts, const FastTree::FileData* const &leftp, const FastTree::FsData* const &rights,
      const FastTree::FileData* const &rightp) const
  {
    return pBranchComp(lefts, static_cast<const TreeNodeSlots * const &>(leftp), rights, static_cast<const TreeNodeSlots * const &>(rightp)) == 0;
  }

  inline bool
  FTEqualNode(const tFastTreeIdx&left, const tFastTreeIdx&right) const
  {
    return FTEqual(&pNodes[left].mFsData, &pNodes[left].mFileData, &pNodes[right].mFsData, &pNodes[right].mFileData);
  }

  inline bool
  FTEqualBranch(const tFastTreeIdx&left, const tFastTreeIdx&right) const
  {
    return FTEqual(&pNodes[pBranches[left].mSonIdx].mFsData, &pNodes[pBranches[left].mSonIdx].mFileData, &pNodes[pBranches[right].mSonIdx].mFsData,
        &pNodes[pBranches[right].mSonIdx].mFileData);
  }

  std::ostream&
  recursiveDisplay(std::ostream &os, const FastTreeInfo &info, const std::string &prefix, tFastTreeIdx node) const;

  inline tFastTreeIdx
  getRandomBranch(const tFastTreeIdx &node) const
  {
    const tFastTreeIdx &nBranches = pNodes[node].mFileData.mLastHighestPriorityOffset + 1;
    DBG3
    std::cout << "getRandomBranch at " << (*pTreeInfo)[node] << " choose among " << (int) nBranches << std::endl;
    int weightSum = 0;

    for (tFastTreeIdx i = pNodes[node].mTreeData.mFirstBranchIdx; i < pNodes[node].mTreeData.mFirstBranchIdx + nBranches; i++)
    {
      const FastTreeNode &node = pNodes[pBranches[i].mSonIdx];
      weightSum += randVar(node.mFsData, node.mFileData);
    }

    int r = rand();
    r = r % (weightSum);
    tFastTreeIdx i = 0;

    for (weightSum = 0, i = pNodes[node].mTreeData.mFirstBranchIdx; i < pNodes[node].mTreeData.mFirstBranchIdx + nBranches; i++)
    {
      const FastTreeNode &node = pNodes[pBranches[i].mSonIdx];
      weightSum += randVar(node.mFsData, node.mFileData);
      if (weightSum > r)
        break;
    }

    CHK1
    assert(i <= pNodes[node].mTreeData.mFirstBranchIdx + pNodes[node].mFileData.mLastHighestPriorityOffset);

    //assert(pNodes[pBranches[i].mSonIdx].mFileData.mNbFreeSlots);
    return pBranches[i].mSonIdx;
  }

  inline tFastTreeIdx
  findNewRank(tFastTreeIdx left, tFastTreeIdx right, const tFastTreeIdx &modified) const
  {
    DBG3
    std::cout << "findNewRank: " << (int) left << " " << (int) right << " " << (int) modified << std::endl;
    //		std::cout << "findNewRank: "<<(int)left<< " "<<(int)right << " "<<(int)modified<<std::endl;
    //		std::cout << "findNewRank: "<<(int)pBranches[left].mSonIdx<< " "<<(int)pBranches[right].mSonIdx << " "<<(int)modified<<std::endl;
    //		//std::cout << "findNewRank: "<<pNodes[pBranches[left].mSonIdx]<< " "<<pNodes[pBranches[right].mSonIdx] << " "<<(int)modified<<std::endl;
    if (right == left)
      return right;
    bool firstiter = true;
    //if(modified==left && !FTLower(pBranches[left].mSonIdx,pBranches[modified].mSonIdx)) return left;
    while (true)
    {
      //	                std::cout << "left= "<<(int)left << " right= "<<(int)right<<std::endl;
      if (!firstiter)
        assert(!FTLowerBranch(modified,right) && !FTLowerBranch(left,modified));
      if (!firstiter && right - left == 1)
      {
        assert(!FTLowerBranch(modified,right) && !FTLowerBranch(right-1,modified));
        return right;
      }
      if (left == modified)
        left = left + 1;
      //			std::cout << "a" << std::endl;
      if (right == modified)
        right = right - 1;

      //			std::cout << "b" << std::endl;
      if (!FTLowerNode(pBranches[modified].mSonIdx, pBranches[left].mSonIdx))
        return left;
      //			std::cout << "c" << std::endl;
      if (!FTLowerNode(pBranches[right].mSonIdx, pBranches[modified].mSonIdx))
        return right + 1; // which might not exist show that it should be at the end
      //			std::cout << "d" << std::endl;
      tFastTreeIdx mid = (left + right) / 2;
      if (mid == modified)
      { // mid point should NOT be the middle point
        if (mid + 1 > right)
          mid--;
        else
          mid++;
      }

      if (!FTLowerNode(pBranches[modified].mSonIdx, pBranches[mid].mSonIdx))
        right = mid;
      else
        left = mid;
      firstiter = false;
    }
  }

  inline void
  fixBranchSorting(const tFastTreeIdx &node, const tFastTreeIdx &modifiedBranchIdx)
  {
    CHK1
    assert(
        modifiedBranchIdx >= pNodes[node].mTreeData.mFirstBranchIdx && modifiedBranchIdx < pNodes[node].mTreeData.mFirstBranchIdx+pNodes[node].mTreeData.mNbChildren);

    const Branch &modifiedBranch = pBranches[modifiedBranchIdx];
    const tFastTreeIdx &firstBranchIdx = pNodes[node].mTreeData.mFirstBranchIdx;
    const tFastTreeIdx &nbChildren = pNodes[node].mTreeData.mNbChildren;
    tFastTreeIdx &lastHPOffset = pNodes[node].mFileData.mLastHighestPriorityOffset;
    CHK3
    checkConsistency(0, false);

    if (nbChildren < 2)
      return;
    // if it's already ordered, nothing to do.
    //std::cout << (int)firstBranchIdx <<"\t"<<(int)modifiedBranchIdx<<"\t"<<(int)firstBranchIdx+nbChildren-1<<std::endl;
    //std::cout << !FTLower(pNodes[modifiedBranchIdx]., modifiedBranchIdx+1) << "\t" << !FTLower(modifiedBranchIdx-1, modifiedBranchIdx) << std::endl;
    if ((modifiedBranchIdx == firstBranchIdx && !FTLowerBranch(modifiedBranchIdx, modifiedBranchIdx + 1))
        || (modifiedBranchIdx == firstBranchIdx + nbChildren - 1 && !FTLowerBranch(modifiedBranchIdx - 1, modifiedBranchIdx))
        || (!FTLowerBranch(modifiedBranchIdx, modifiedBranchIdx + 1) && !FTLowerBranch(modifiedBranchIdx - 1, modifiedBranchIdx)))
      goto update_and_return;

    {
      tFastTreeIdx newrank = findNewRank(firstBranchIdx, firstBranchIdx + nbChildren - 1, modifiedBranchIdx);
      DBG3
      std::cout << "findNewRank returned " << (int) newrank << std::endl;
      //std::cout << "findNewRank returned "<<(int)newrank <<std::endl;
      //std::cout << "path 3" << std::endl;
      // in any other case, memory move is involved inside the branches array

      // keep a copy of the branch
      Branch modbr = modifiedBranch;
      // move the appropriate range of branches
      if (modifiedBranchIdx < newrank)
      {
        memmove(&pBranches[modifiedBranchIdx], &pBranches[modifiedBranchIdx + 1], (newrank - modifiedBranchIdx) * sizeof(Branch));
        pBranches[newrank - 1] = modbr;
      }
      else if (modifiedBranchIdx > newrank)
      {
        memmove(&pBranches[newrank + 1], &pBranches[newrank], (modifiedBranchIdx - newrank) * sizeof(Branch));
        pBranches[newrank] = modbr;
      }
      // insert the modified branch
    }
    update_and_return: lastHPOffset = 0;
    //std::cout<< "lastHPOffset before update " << (int)lastHPOffset << std::endl;
    while (lastHPOffset < nbChildren - 1
        && !FTLower(&pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].mSonIdx].mFsData,
            &pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].mSonIdx].mFileData, &pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFsData,
            &pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFileData))
    {

      //			assert(
      //					(!FTLower(&pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFsData,
      //							&pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFileData,
      //							&pNodes[pBranches[firstBranchIdx + lastHPOffset+1].mSonIdx].mFsData,
      //							&pNodes[pBranches[firstBranchIdx + lastHPOffset+1].mSonIdx].mFileData) )
      //							==
      //									FTEqual(&pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFsData,
      //											&pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFileData,
      //											&pNodes[pBranches[firstBranchIdx + lastHPOffset+1].mSonIdx].mFsData,
      //											&pNodes[pBranches[firstBranchIdx + lastHPOffset+1].mSonIdx].mFileData)
      //			);
      lastHPOffset++;
    }

    //std::cout<< "lastHPOffset after update " << (int)lastHPOffset << std::endl;
    CHK3
    checkConsistency(0, true);
    return;
  }

  inline void
  fixBranchSortingHP(const tFastTreeIdx &node, const tFastTreeIdx &modifiedBranchIdx)
  {
    // this is an optimized version where the updated branch gets a lower or equal priority
    // this version is typically called after finding a free slot (which is supposed to be HP by definition)
    // all the branches between
    // pBranches[pNodes[node].mTreeData.mFirstBranchIdx].mSonIdx;
    // pBranches[pNodes[node].mTreeData.mFirstBranchIdx+pNodes[node].mFileData.mLastHighestPriorityIdx].mSonIdx;
    // have the same priority
    // modified branch modifiedBranch should be among those

    //      CHK1
    //        assert(
    //            modifiedBranchIdx >= pNodes[node].mTreeData.mFirstBranchIdx && modifiedBranchIdx <= pNodes[node].mTreeData.mFirstBranchIdx+pNodes[node].mFileData.mLastHighestPriorityOffset);

    const Branch &modifiedBranch = pBranches[modifiedBranchIdx];
    const FsData &modifiedFsData = pNodes[modifiedBranch.mSonIdx].mFsData;
    const FileData &modifiedFileData = pNodes[modifiedBranch.mSonIdx].mFileData;
    const tFastTreeIdx &firstBranchIdx = pNodes[node].mTreeData.mFirstBranchIdx;
    const tFastTreeIdx &nbChildren = pNodes[node].mTreeData.mNbChildren;
    tFastTreeIdx &lastHPOffset = pNodes[node].mFileData.mLastHighestPriorityOffset;
    const bool modifiedIsInHp = modifiedBranchIdx <= firstBranchIdx + lastHPOffset;

    //      if (!modifiedIsInHp)
    //        std::cout << "modifiedIsInHp = " << modifiedIsInHp << std::endl;

    CHK3
    checkConsistency(0, false);

    // this function should not be called in that case
    if (!nbChildren)
      return;

    if (modifiedBranchIdx == firstBranchIdx + nbChildren - 1) // nothing to do, the sorting is already the worst rated
    {
      //std::cout << "Path 0" <<std::endl; std::cout.flush();
      goto update_and_return;
    }
    // if all the branches have the lowest priority level, the selected branches just go to the end
    if (lastHPOffset == pNodes[node].mTreeData.mNbChildren - 1)
    {
      std::swap(pBranches[modifiedBranchIdx], pBranches[firstBranchIdx + lastHPOffset]);
      //std::cout << "Path 1" <<std::endl; std::cout.flush();
      goto update_and_return;
    }

    // if the modified branch still have a highest or equal priority than the next priority level a swap is enough
    else if ((modifiedBranchIdx <= firstBranchIdx + lastHPOffset) && // this one should ALWAYS be TRUE for a placement
        !FTLower(&modifiedFsData, &(modifiedFileData), &pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].mSonIdx].mFsData,
            &pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].mSonIdx].mFileData))
    {
      std::swap(pBranches[modifiedBranchIdx], pBranches[firstBranchIdx + lastHPOffset]);
      //std::cout << "Path 2" <<std::endl; std::cout.flush();
      goto update_and_return;
    }
    else
    {
      if(!modifiedIsInHp) return fixBranchSorting(node, modifiedBranchIdx);

      // in any other case, memory move is involved inside the branches array
      // find the first branch of which priority is lower than the modified branch
      tFastTreeIdx insertionIdx;
      for (insertionIdx = firstBranchIdx + lastHPOffset + 1;
          insertionIdx < firstBranchIdx + nbChildren
          && FTLower(&modifiedFsData, &modifiedFileData, &pNodes[pBranches[insertionIdx].mSonIdx].mFsData,
              &pNodes[pBranches[insertionIdx].mSonIdx].mFileData); insertionIdx++)
      {
      }
      //std::cout<< "insertion = " << (int)insertionIdx << "\tmodified = " << (int)modifiedBranchIdx <<std::endl;
      //std::cout<< (int)FTLowerBranch(insertionIdx,modifiedBranchIdx) << "\t" << (int)FTLowerBranch(modifiedBranchIdx,insertionIdx) <<std::endl;
      // keep a copy of the branch
      Branch modbr = modifiedBranch;
      // move the appropriate range of branches
      memmove(&pBranches[modifiedBranchIdx], &pBranches[modifiedBranchIdx + 1], (insertionIdx - modifiedBranchIdx) * sizeof(Branch));
      // insert the modified branch
      pBranches[insertionIdx - 1] = modbr;
      //std::cout << "Path 3" <<std::endl; std::cout.flush();
    }

    update_and_return: if (modifiedIsInHp && lastHPOffset > 0)
    {
      // there is more than one branch having the highest priority, just decrement
      // on more replica in a branch should mean a lower priority and so, leaving the HPset
      lastHPOffset--;
      //std::cout << "Path A HP" <<std::endl; std::cout.flush();
    }
    else
    { // the modified node is the last one with the maximum priority
      lastHPOffset = 0;
      while (lastHPOffset < nbChildren - 1
          && !FTLower(&pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].mSonIdx].mFsData,
              &pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].mSonIdx].mFileData, &pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFsData,
              &pNodes[pBranches[firstBranchIdx + lastHPOffset].mSonIdx].mFileData))
        lastHPOffset++;
      //std::cout << "Path B HP" <<std::endl; std::cout.flush();
    }

    CHK3
    checkConsistency(0, true);
    return;
  }

  public:
  unsigned char
  getMaxNodeCount() const
  {
    return pMaxNodeCount;
  }

  inline bool
  findFreeSlotsMultiple(std::vector<tFastTreeIdx>&idxs, tFastTreeIdx nReplicas, tFastTreeIdx startFrom = 0, bool allowUpRoot = false)
  {
    //      tFastTreeIdx freeFromCurrentStart = pNodes[startFrom].mFileData.mNbFreeSlots;
    //
    //      // check if there is enough freeSlots to do the job
    //      if ( nReplicas > freeFromCurrentStart &&
    //          (!allowUpRoot || nReplicas > pNodes[0].mFileData.mNbFreeSlots) )
    //        return false;
    //
    //       if (freeFromCurrentStart)
    //       {
    //         if (!pNodes[startFrom].mTreeData.mNbChildren)
    //         {
    //           // we are arrived
    //           newReplica = startFrom;
    //           // update the file replica info in the tree
    //           if (decrFreeSlot)
    //             decrementFreeSlot(newReplica, true);
    //           //if(decrFreeSlot) decrementFreeSlot(newReplica, false);
    //           return true;
    //         }
    //         else
    //         {
    //           if(pNodes[startFrom].mFileData.mLastHighestPriorityOffset>0)
    //           {
    //             ///////////////////////////getRandomBranch(startFrom);
    //           }
    //           else { // straight way to the freeslot
    //             idxs.push_back(0);
    //             findFreeSlot(pBranches[pNodes[startFrom].mTreeData.mFirstBranchIdx],false,idxs.back(),false);
    //           }
    //
    //           if(!findFreeSlotsMultiple(getRandomBranch(startFrom), allowUpRoot, newReplica, decrFreeSlot))
    //             return false;
    //
    //         }
    //       }
    //
    //       else
    //       {
    //         // no free slot then, try higher if allowed and not already at the root
    //         if (allowUpRoot && startFrom)
    //         {
    //           return findFreeSlot(pNodes[startFrom].mTreeData.mFatherIdx, allowUpRoot, newReplica, decrFreeSlot);
    //         }
    //         else
    //           assert(false);
    //         //return false;
    //       }

    return true;
  }

  inline tFastTreeIdx
  findFreeSlotsAll(tFastTreeIdx *idxs, tFastTreeIdx sizeIdxs, tFastTreeIdx startFrom = 0, bool allowUpRoot = false, const int &filterStatus = All, tFastTreeIdx *upRootLevelsCount=NULL, tFastTreeIdx *upRootLevelsIdxs=NULL, tFastTreeIdx *upRootLevels=NULL) const
  {
    //std::cout<< "findFreeSlotsAll " << filterStatus << std::endl;
    tFastTreeIdx sizeIdxsBak = sizeIdxs;
    if(upRootLevelsIdxs) *upRootLevelsCount = 0;
    if (_findFreeSlotsAll(idxs, sizeIdxs, startFrom, allowUpRoot, startFrom, filterStatus, upRootLevelsCount, upRootLevelsIdxs, upRootLevels, 0)) {
      if(upRootLevelsIdxs) {
        for(int k=0;k<*upRootLevelsCount;k++)  upRootLevelsIdxs[k] = sizeIdxsBak -upRootLevelsIdxs[k] ;

//        std::cout << "*upRootLevelsCount = " << (int)*upRootLevelsCount << std::endl;
//        for(int k=0;k<*upRootLevelsCount;k++)  std::cout << (int)upRootLevelsIdxs[k] << "\t";
//        std::cout<< std::endl;
//        for(int k=0;k<*upRootLevelsCount;k++)  std::cout << (int)upRootLevels[k] << "\t";
//        std::cout<< std::endl;
      }
      return sizeIdxsBak - sizeIdxs;
    }
    else
      return 0;
  }

  inline bool
  _findFreeSlotsAll(tFastTreeIdx *&idxs, tFastTreeIdx &sizeIdxs, tFastTreeIdx startFrom, bool allowUpRoot , tFastTreeIdx callerNode, const int &filterStatus,
      tFastTreeIdx *upRootLevelsCount, tFastTreeIdx *upRootLevelsIdxs, tFastTreeIdx *upRootLevels, tFastTreeIdx currentUpRootLevel) const
  {
    //std::cout<< "_findFreeSlotsAll " << filterStatus << std::endl;
    if (!pNodes[startFrom].mTreeData.mNbChildren)
    {
      //std::cout << pNodes[startFrom].mFsData.mStatus << "\t" << filterStatus << "\t" << All <<std::endl;
      if (pNodes[startFrom].mFileData.mNbFreeSlots && (filterStatus == All || (pNodes[startFrom].mFsData.mStatus & filterStatus)) )
      {
        if (sizeIdxs)
        {
          if(upRootLevelsIdxs) {
            if( *upRootLevelsCount == 0 ) {
              upRootLevels[0] = currentUpRootLevel;
              upRootLevelsIdxs[0] = sizeIdxs;
              (*upRootLevelsCount)++;
            }
            else if(upRootLevels[*upRootLevelsCount-1] < currentUpRootLevel) {
              upRootLevels[*upRootLevelsCount] = currentUpRootLevel;
              upRootLevelsIdxs[*upRootLevelsCount] = sizeIdxs;
              (*upRootLevelsCount)++;
            }
          }
          *idxs = startFrom;
          idxs++;
          sizeIdxs--;
        }
        else
        {
          // no enough space to write all the replicas
          // it should not happen when called from findFreeSlotsAll because it4s checked there
          assert(false);
          return false;
        }
      }
    }
    //else
    //{
      for (tFastTreeIdx bidx = pNodes[startFrom].mTreeData.mFirstBranchIdx;
          bidx < pNodes[startFrom].mTreeData.mFirstBranchIdx + pNodes[startFrom].mTreeData.mNbChildren; bidx++)
      {
        if ( pBranches[bidx].mSonIdx == callerNode ||
            !pNodes[pBranches[bidx].mSonIdx].mFileData.mNbFreeSlots ||
            !(filterStatus == All || (pNodes[startFrom].mFsData.mStatus & filterStatus)) )
          continue;
        if (!_findFreeSlotsAll(idxs, sizeIdxs, pBranches[bidx].mSonIdx, false, startFrom, filterStatus, upRootLevelsCount, upRootLevelsIdxs, upRootLevels, currentUpRootLevel))
        {
          // something is wrong. It should no happen
          // free slots are supposed to be there but none are found!
          assert(false);
          return false;
        }
      }

      //std::cout << allowUpRoot << " && " << (int)startFrom <<std::endl;
      if(allowUpRoot && startFrom) {
        if(upRootLevelsIdxs) {
          currentUpRootLevel++;
        }
        _findFreeSlotsAll(idxs, sizeIdxs, pNodes[startFrom].mTreeData.mFatherIdx, true, startFrom, filterStatus, upRootLevelsCount, upRootLevelsIdxs, upRootLevels, currentUpRootLevel);
      }
    //}
    return true;
  }

  inline void
  checkConsistency(tFastTreeIdx node, bool checkOrder = true, bool recursive = true, std::map<tFastTreeIdx, tFastTreeIdx> *map = 0)
  {
    bool del = false;
    if (map == 0)
    {
      map = new std::map<tFastTreeIdx, tFastTreeIdx>;
      del = true;
    }
    if (recursive)
    {
      for (tFastTreeIdx bidx = pNodes[node].mTreeData.mFirstBranchIdx; bidx < pNodes[node].mTreeData.mFirstBranchIdx + pNodes[node].mTreeData.mNbChildren;
          bidx++)
        checkConsistency(pBranches[bidx].mSonIdx, checkOrder, true, map);
    }

    assert(
        pNodes[node].mTreeData.mNbChildren == 0 || ( pNodes[node].mFileData.mLastHighestPriorityOffset >= 0 && pNodes[node].mFileData.mLastHighestPriorityOffset < pNodes[node].mTreeData.mNbChildren ));
    // check that every node is referred at most once in a branch
    for (tFastTreeIdx bidx = pNodes[node].mTreeData.mFirstBranchIdx; bidx < pNodes[node].mTreeData.mFirstBranchIdx + pNodes[node].mTreeData.mNbChildren;
        bidx++)
    {
      // check that this node is not already referred
      assert(!map->count(pBranches[bidx].mSonIdx));
      (*map)[pBranches[bidx].mSonIdx] = node; // set the father in the map
    }

    // check the order is respected in the branches
    if (checkOrder)
    {
      bool checkedHpOfs = false;
      tFastTreeIdx lastHpOfs = 0;
      for (tFastTreeIdx bidx = pNodes[node].mTreeData.mFirstBranchIdx; bidx < pNodes[node].mTreeData.mFirstBranchIdx + pNodes[node].mTreeData.mNbChildren - 1;
          bidx++)
      {
        assert(
            !FTLower( &pNodes[pBranches[bidx].mSonIdx].mFsData, &pNodes[pBranches[bidx].mSonIdx].mFileData, &pNodes[pBranches[bidx+1].mSonIdx].mFsData, &pNodes[pBranches[bidx+1].mSonIdx].mFileData ));
        if (!checkedHpOfs
            && !FTEqual(&pNodes[pBranches[bidx].mSonIdx].mFsData, &pNodes[pBranches[bidx].mSonIdx].mFileData, &pNodes[pBranches[bidx + 1].mSonIdx].mFsData,
                &pNodes[pBranches[bidx + 1].mSonIdx].mFileData))
        {
          assert(lastHpOfs == pNodes[node].mFileData.mLastHighestPriorityOffset);
          checkedHpOfs = true;
        }
        lastHpOfs++;
      }
      if (!checkedHpOfs && lastHpOfs)
      {
        assert(pNodes[node].mTreeData.mNbChildren-1 == pNodes[node].mFileData.mLastHighestPriorityOffset);
      }
    }
    if (del)
      delete map;
  }

  std::ostream&
  recursiveDisplay(std::ostream &os, const std::string &prefix = "") const
  {
    return recursiveDisplay(os, prefix, 0);
  }

  protected:
  FsDataMemberForRand randVar;
  FsAndFileDataComparerForBranchSorting pBranchComp;
  public:
  FastTree() :
    randVar(), pBranchComp()
  {
    pMaxNodeCount = 0;
    pSelfAllocated = false;
  }

  ~FastTree()
  {
    if (pSelfAllocated)
      selfUnallocate();
  }

  bool
  selfAllocate(tFastTreeIdx size)
  {
    pMaxNodeCount = size;
    size_t memsize = (sizeof(FastTreeNode) + sizeof(Branch)) * size;
    DBG2
    std::cout << "self allocation size = " << memsize << std::endl;
    pNodes = (FastTreeNode*) new char[memsize];
    pBranches = (Branch*) (pNodes + size);
    pSelfAllocated = true;
    return true;
  }
  bool
  selfUnallocate()
  {
    delete[] pNodes;
    return true;
  }

  bool
  allocate(void *buffer, size_t bufsize, tFastTreeIdx size)
  {
    size_t memsize = (sizeof(FastTreeNode) + sizeof(Branch)) * size;
    if (bufsize < memsize)
      return false;
    pMaxNodeCount = size;
    pNodes = (FastTreeNode*) buffer;
    pBranches = (Branch*) (pNodes + size);
    pSelfAllocated = false;
    return true;
  }

  size_t
  copyToBuffer(char* buffer, size_t bufsize) const
  {
    size_t memsize = (sizeof(FastTreeNode) + sizeof(Branch)) * pMaxNodeCount + sizeof(FastTree);
    if (bufsize < memsize)
      return memsize;
    // copy all the data members
    tSelf *destFastTree = (tSelf *) (buffer);
    // adjust the value of some of them
    (*destFastTree) = *this;
    destFastTree->pSelfAllocated = false;
    destFastTree->pNodes = (FastTreeNode *) (buffer += sizeof(tSelf));
    destFastTree->pBranches = (Branch *) (buffer += sizeof(FastTreeNode) * pMaxNodeCount);
    memcpy(destFastTree->pNodes, pNodes, (sizeof(FastTreeNode) + sizeof(Branch)) * pMaxNodeCount); // pNodes and pBranches copied at once
    return 0;
  }

  std::ostream&
  recursiveDisplay(std::ostream &os, const std::string &prefix, tFastTreeIdx node) const
  {
    std::stringstream ss;
    ss << prefix;
    os << std::right << std::setw(8) << std::setfill('-');
    tFastTreeIdx &nbChildren = pNodes[node].mTreeData.mNbChildren;
    if ((*pTreeInfo)[node].mNodeType == TreeNodeInfo::intermediate)
      os << (*pTreeInfo)[node].mGeotag;
    else if ((*pTreeInfo)[node].mNodeType == TreeNodeInfo::fs)
      os << (*pTreeInfo)[node].mFsId;
    os << "/( free:" << (int) pNodes[node].mFileData.mNbFreeSlots << "|repl:" << (int) pNodes[node].mFileData.mNbTakenSlots << "|pidx:"
        << (int) pNodes[node].mFileData.mLastHighestPriorityOffset << ")";
    ss << std::right << std::setw(7) << std::setfill(' ') << "";

    if (!nbChildren)
      os << "@" << (*pTreeInfo)[node].mHost << std::endl;
    else
    {
      os << std::endl;
      tFastTreeIdx &firstBranchIdx = pNodes[node].mTreeData.mFirstBranchIdx;
      for (tFastTreeIdx branchIdx = firstBranchIdx; branchIdx < firstBranchIdx + nbChildren; branchIdx++)
      {
        tFastTreeIdx childIdx = pBranches[branchIdx].mSonIdx;
        bool lastChild = (branchIdx == firstBranchIdx + nbChildren - 1);
        if (lastChild)
        { // final branch
          os << ss.str() << "`--";
          recursiveDisplay(os, ss.str() += "   ", childIdx);
        }
        else
        { // intermediate branch
          os << ss.str() << "|--";
          recursiveDisplay(os, ss.str() += "|  ", childIdx);
        }
      }
    }
    return os;
  }

  void
  decrementFreeSlot(tFastTreeIdx node, bool useHpSpeedUp = false)
  {
    // first update the node information
    CHK1
    assert(pNodes[node].mFileData.mNbFreeSlots>0);
    CHK2
    checkConsistency(0);
    pNodes[node].mFileData.mNbFreeSlots--;
    pNodes[node].mFileData.mNbTakenSlots++;
    //checkConsistency(0,false);

    // if there is a father node, update its branches
    if (node)
    {
      tFastTreeIdx father = pNodes[node].mTreeData.mFatherIdx;
      tFastTreeIdx firstBranchIndex = pNodes[father].mTreeData.mFirstBranchIdx;
      tFastTreeIdx nbBranches = pNodes[father].mTreeData.mNbChildren;
      tFastTreeIdx matchBranchIdx;
      // first locate the branch (it should be in the first positions if it's a placement)
      for (matchBranchIdx = firstBranchIndex; matchBranchIdx < firstBranchIndex + nbBranches && pBranches[matchBranchIdx].mSonIdx != node; matchBranchIdx++)
      {
      }
      CHK1
      assert(pBranches[matchBranchIdx].mSonIdx==node);
      // the branches are supposed to be ordered before the update
      if (useHpSpeedUp)
        fixBranchSortingHP(father, matchBranchIdx); // optimized for
      else
        fixBranchSorting(father, matchBranchIdx);
      // finally iterate upper in the tree
      decrementFreeSlot(father, useHpSpeedUp);
    }
  }

  void
  incrementFreeSlot(tFastTreeIdx node, bool useHpSpeedUp = false)
  {
    // first update the node information
    //      CHK1
    //        assert(pNodes[node].mFileData.mNbFreeSlots>0);
    CHK2
    checkConsistency(0);
    pNodes[node].mFileData.mNbFreeSlots++;

    //pNodes[node].mFileData.mNbFreeSlots--;
    //pNodes[node].mFileData.mNbTakenSlots++;
    //checkConsistency(0,false);

    // if there is a father node, update its branches
    if (node)
    {
      tFastTreeIdx father = pNodes[node].mTreeData.mFatherIdx;
      tFastTreeIdx firstBranchIndex = pNodes[father].mTreeData.mFirstBranchIdx;
      tFastTreeIdx nbBranches = pNodes[father].mTreeData.mNbChildren;
      tFastTreeIdx matchBranchIdx;
      // first locate the branch (it should be in the first positions if it's a placement)
      for (matchBranchIdx = firstBranchIndex; matchBranchIdx < firstBranchIndex + nbBranches && pBranches[matchBranchIdx].mSonIdx != node; matchBranchIdx++)
      {
      }
      CHK1
      assert(pBranches[matchBranchIdx].mSonIdx==node);
      // the branches are supposed to be ordered before the update
      if (useHpSpeedUp)
        fixBranchSortingHP(father, matchBranchIdx); // optimized for
      else
        fixBranchSorting(father, matchBranchIdx);
      // finally iterate upper in the tree
      incrementFreeSlot(father, useHpSpeedUp);
    }
  }

  bool
  findFreeSlot(tFastTreeIdx& newReplica, tFastTreeIdx startFrom=0, bool allowUpRoot=false, bool decrFreeSlot = true)
  {
    //
    if (pNodes[startFrom].mFileData.mNbFreeSlots)
    {
      if (!pNodes[startFrom].mTreeData.mNbChildren)
      {
        // we are arrived
        newReplica = startFrom;
        // update the file replica info in the tree
        if (decrFreeSlot)
          decrementFreeSlot(newReplica, true);
        //if(decrFreeSlot) decrementFreeSlot(newReplica, false);
        return true;
      }
      else
      {
        if(pNodes[startFrom].mFileData.mLastHighestPriorityOffset) {
          return findFreeSlot(newReplica, getRandomBranch(startFrom), false, decrFreeSlot);
        }
        else {
          return findFreeSlot(newReplica, pBranches[pNodes[startFrom].mTreeData.mFirstBranchIdx].mSonIdx, false,decrFreeSlot);
        }
      }
    }
    else
    {
      // no free slot then, try higher if allowed and not already at the root
      if (allowUpRoot && startFrom)
      {
        // we won't go through the cirrent branch again because it has no free slot. Else, we wouldn't go uproot
        return findFreeSlot(newReplica, pNodes[startFrom].mTreeData.mFatherIdx, allowUpRoot, decrFreeSlot);
      }
      else
        assert(false);
      //return false;
    }
  }


  bool GenerateInvertedLookup(InverseFastTreeLookUpTable<FsDataMemberForRand,FsAndFileDataComparerForBranchSorting> *iftlut, int EntriesFilter, int WeightedNodesFilter) const {
    // TODO make it generic to two status status a entry status and a value status
    // TODO allocate or self allocate

    // iterate through the fs's and check if they are Balancing
    std::vector< std::vector<tFastTreeIdx> > balancingLeaves(pMaxNodeCount);
    balancingLeaves.resize(0);

    // get all the balancers
    tFastTreeIdx *allBalancersIdxs = new tFastTreeIdx[pMaxNodeCount];
    tFastTreeIdx nAllBalancers = findFreeSlotsAll(allBalancersIdxs,pMaxNodeCount,0,false,EntriesFilter);
    std::sort(allBalancersIdxs,allBalancersIdxs+nAllBalancers);
    DBG3 std::cout << "AllBalancersIdx = ";
    DBG3 for(int k=0;k<nAllBalancers;k++) std::cout<<(int)allBalancersIdxs[k]<<"\t";
    DBG3 std::cout<<std::endl;

    tFastTreeIdx *allBalancingsIdxs = new tFastTreeIdx[pMaxNodeCount];
    tFastTreeIdx nAllBalancings = findFreeSlotsAll(allBalancingsIdxs,pMaxNodeCount,0,false,WeightedNodesFilter);
    std::sort(allBalancingsIdxs,allBalancingsIdxs+nAllBalancings);
    DBG3 std::cout << "AllBalancingsIdx = ";
    DBG3 for(int k=0;k<nAllBalancings;k++) std::cout<<(int)allBalancingsIdxs[k]<<"\t";
    DBG3 std::cout<<std::endl;

    if(!nAllBalancers || !nAllBalancings) {
      delete[] allBalancersIdxs;
      delete[] allBalancingsIdxs;
      return false;
    }

    tFastTreeIdx *balancersSourceCounts = new tFastTreeIdx[pMaxNodeCount]; // this array is sparse but no map involved
    std::fill_n(balancersSourceCounts,pMaxNodeCount,0);
    tFastTreeIdx *balancersIdxs = new tFastTreeIdx[pMaxNodeCount];
    tFastTreeIdx *upRootLevels = new tFastTreeIdx[pMaxNodeCount];
    tFastTreeIdx *upRootLevelsIdxs = new tFastTreeIdx[pMaxNodeCount];
    tFastTreeIdx upRootLevelsCount;
    tFastTreeIdx nBalancings=0, nBalancers=0;
    size_t nWNodes = 0;
    for(const tFastTreeIdx *bingIt = allBalancingsIdxs; bingIt != allBalancingsIdxs+nAllBalancings; bingIt++)
    {
      nBalancers = findFreeSlotsAll(balancersIdxs,pMaxNodeCount,*bingIt,true,EntriesFilter,&upRootLevelsCount,upRootLevelsIdxs, upRootLevels);
      balancingLeaves.push_back(std::vector<tFastTreeIdx>());
      balancingLeaves.back().insert(balancingLeaves.back().begin(),balancersIdxs,balancersIdxs+(upRootLevelsCount>1?upRootLevelsIdxs[1]:nBalancers) );
      nBalancings++;

      tFastTreeIdx count = 0;
      for(std::vector<tFastTreeIdx>::iterator berIt = balancingLeaves.back().begin(); berIt != balancingLeaves.back().end(); berIt++)
      {
        balancersSourceCounts[*berIt]++; // one more source for this balancer
        nWNodes++;
        count++;
      }
    }

    ////// for each balancing, find the closest balancers and then

    // two possible policies
    // 1- stop to the first available level of balancers of each balancing
    //    It guarantees not to mess up too much the geolocation and every balancing has at least a balancer
    //    Some box may be drained really faster than others

    // now we invert this table
    DBG3 std::cout << " nAllBalancers = " << (int)nAllBalancers << "\tnWNodes = "<< (int)nWNodes <<std::endl;
    iftlut->selfAllocate(nBalancers,nWNodes);
    // prepare the inversetree structure
    tFastTreeIdx *treeIdx2InverseTreeIdx = new tFastTreeIdx[pMaxNodeCount]; // this array is sparse but no map involved
    size_t WNIndex=0;
    for(tFastTreeIdx i=0;i<nAllBalancers;i++){
      iftlut->pEntryNodes[i].mIdx = allBalancersIdxs[i];
      treeIdx2InverseTreeIdx[allBalancersIdxs[i]] = i;
      iftlut->pEntryNodes[i].mFirstWNode = WNIndex;
      iftlut->pEntryNodes[i].mNbWNodes = balancersSourceCounts[allBalancersIdxs[i]];
      WNIndex += balancersSourceCounts[allBalancersIdxs[i]];
    }
    iftlut->pEntryCount = nAllBalancers;
    iftlut->pWNodeCount = nWNodes;

    // copy the data into the structure
    std::fill_n(balancersSourceCounts,pMaxNodeCount,0);
    const tFastTreeIdx *allBalIt = allBalancingsIdxs;
    for(std::vector< std::vector<tFastTreeIdx> >::iterator bingIt = balancingLeaves.begin();
        bingIt != balancingLeaves.end(); bingIt++) {
      for(std::vector<tFastTreeIdx>::iterator berIt = bingIt->begin(); berIt != bingIt->end(); berIt++)
      {
        tFastTreeIdx i = treeIdx2InverseTreeIdx[*berIt];
        iftlut->pWeightedNodes[ iftlut->pEntryNodes[i].mFirstWNode + balancersSourceCounts[*berIt] ].mIdx = *allBalIt;
        iftlut->pWeightedNodes[ iftlut->pEntryNodes[i].mFirstWNode + balancersSourceCounts[*berIt] ].mWeight = 1.0/bingIt->size();
        balancersSourceCounts[*berIt]++; // one more source for this balancer
      }
      allBalIt++;
    }

    // 2- you fill the balancing with balancers until all the balancers are included
    //    and until all the balancing are matched with the same number of balancers
    //    it guarantees that all balancers are used at their best and that all balancing are equally treated
    // [NOT IMPLEMENTED]

    // allocate the IFTLUT
    delete[] allBalancersIdxs;
    delete[] allBalancingsIdxs;
    delete[] balancersSourceCounts;
    delete[] treeIdx2InverseTreeIdx;
    delete[] balancersIdxs;
    delete[] upRootLevels;
    delete[] upRootLevelsIdxs;

    return true;
  }

  bool GenerateBalancingInvertedLookup(InverseFastTreeLookUpTable<FsDataMemberForRand,FsAndFileDataComparerForBranchSorting> *iftlut) const {
    return GenerateInvertedLookup(iftlut,SchedTreeBase::Balancer,SchedTreeBase::Balancing);
  }

  bool GenerateDrainingInvertedLookup(InverseFastTreeLookUpTable<FsDataMemberForRand,FsAndFileDataComparerForBranchSorting> *iftlut) const {
    return GenerateInvertedLookup(iftlut,SchedTreeBase::Drainer,SchedTreeBase::Draining);
  }

};


/*----------------------------------------------------------------------------*/
/**
 * @brief Class to do an inverted lookup on a placement Tree.
 *
 *        The FastPlacementTree is meant to find a destination fs for some data.
 *        That's how data are place in eos.
 *        Draining and balancing are processed differently.
 *        To an available destination fs, a source fs should be matched.
 *        Given a destination fs, it can give a source fs randomly sampled
 *        such that it's among the closest source in the tree.
 *        This class contains two vectors of equal size.
 *        - the first one contains the destination fs
 *        - the second one contains all the matching sources fs and their weights.
 *        All the complexity of these class lays in its construction which is
 *        delegated to class FastTree.
 *
 */
/*----------------------------------------------------------------------------*/
template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting>
class InverseFastTreeLookUpTable : public SchedTreeBase
{
public:
  typedef struct {
    tFastTreeIdx mIdx;
    half mWeight;
  } tWeightedNode;

public:
  typedef struct {
    tFastTreeIdx mIdx;
    size_t mFirstWNode;
    tFastTreeIdx mNbWNodes;
  } tEntryNode;

protected:
  friend class FastTree<FsDataMemberForRand,FsAndFileDataComparerForBranchSorting>;
  bool pSelfAllocated;
  tFastTreeIdx pMaxEntryCount;
  size_t pMaxWNodeCount;
  tFastTreeIdx pEntryCount;
  size_t pWNodeCount;
  tEntryNode *pEntryNodes;
  tWeightedNode *pWeightedNodes;

  bool
    findEntry(const tFastTreeIdx &entry, tFastTreeIdx &entryIdx) const
    {
      tFastTreeIdx left = 0;
      tFastTreeIdx right = pEntryCount - 1;

      if (!pEntryCount || entry > pEntryNodes[right].mIdx || entry < pEntryNodes[left].mIdx)
        return false;

      if (entry == pEntryNodes[right].mIdx)
      {
        entryIdx = right;
        return true;
      }

      while (right - left > 1)
      {
        tFastTreeIdx mid = (left + right) / 2;
        if (entry < pEntryNodes[mid].mIdx)
          right = mid;
        else
          left = mid;
      }

      if (entry == pEntryNodes[left].mIdx)
      {
        entryIdx = left;
        return true;
      }

      return false;
    }

  bool getRandomLookup (tFastTreeIdx entryIdx, tFastTreeIdx &randomInverse) const {
    const tFastTreeIdx &nValues = pEntryNodes[entryIdx].mNbWNodes;
    if(!nValues) return false;
     DBG3
     std::cout << "getRandomLookup at chooses among " << (int) nValues << std::endl;
     float weightSum;
     weightSum = 0;

     for (size_t i = pEntryNodes[entryIdx].mFirstWNode; i < pEntryNodes[entryIdx].mFirstWNode + nValues; i++)
       weightSum += pWeightedNodes[i].mWeight;

     int r = rand();
     float hr = ((float(r) / RAND_MAX) * weightSum );

     size_t i = 0;
     for (weightSum = 0, i = pEntryNodes[entryIdx].mFirstWNode; i < pEntryNodes[entryIdx].mFirstWNode + nValues; i++)
     {
       weightSum += pWeightedNodes[i].mWeight;
       if (weightSum > hr)
         break;
     }

     randomInverse = pWeightedNodes[i].mIdx;

     return true;
  }

public:
  InverseFastTreeLookUpTable() :
    pSelfAllocated(false), pMaxEntryCount(0), pMaxWNodeCount(0)
  {
  }

  ~InverseFastTreeLookUpTable()
  {
    if (pSelfAllocated)
      selfUnallocate();
  }

  bool getRandomInverseLookup( tFastTreeIdx entry, tFastTreeIdx &randomInverse) const {
    tFastTreeIdx entryIdx;
    if(!findEntry(entry,entryIdx))
      return false;

    //std::cout << (int)entry << " found at idx " << (int)entryIdx <<std::endl;

    if(!getRandomLookup(entryIdx,randomInverse))
      return false;

    return true;
  }

  bool getFullInverseLookup( tFastTreeIdx entry, const tWeightedNode *&fullInverse, tFastTreeIdx *fullInverseSize) const {
    tFastTreeIdx entryIdx;
    if(!findEntry(entry,entryIdx))
      return false;

    fullInverse = &pWeightedNodes[pEntryNodes[entryIdx].mFirstWNode];
    *fullInverseSize = pEntryNodes[entryIdx].mNbWNodes;

    return true;
  }

  bool
  selfAllocate(tFastTreeIdx nEntry, size_t nWNodes)
  {
    pMaxEntryCount = nEntry;
    pMaxWNodeCount = nWNodes;
    pSelfAllocated = true;
    size_t memsize = sizeof(tEntryNode)*nEntry + sizeof(tWeightedNode)*nWNodes;
    DBG2
    std::cout << "self allocation size = " << memsize << std::endl;
    pEntryNodes    = (tEntryNode*) new char[memsize];
    pWeightedNodes = (tWeightedNode*) (pEntryNodes + nEntry);
    return true;
  }
  bool
  selfUnallocate()
  {
    delete[] pEntryNodes;
    return true;
  }

  bool
  allocate(void *buffer, size_t bufsize, tFastTreeIdx nEntry, size_t nWNodes)
  {
    size_t memsize = sizeof(tEntryNode)*nEntry + sizeof(tWeightedNode)*nWNodes;
    if (bufsize < memsize)
      return false;
    pMaxEntryCount = nEntry;
    pMaxWNodeCount = nWNodes;
    pSelfAllocated = false;
    pEntryNodes = (tEntryNode*) buffer;
    pWeightedNodes = (tWeightedNode*) (pEntryNodes + nEntry);
    return true;
  }

  std::ostream & display(std::ostream &os) const {
    //std::cout<< pEntryCount <<std::endl;
    for(tFastTreeIdx i=0;i<pEntryCount;i++) {
      os << "["<< (int)pEntryNodes[i].mIdx<< "] => ";
      for(size_t j=0;j<pEntryNodes[i].mNbWNodes;j++){
        const tWeightedNode &wn = pWeightedNodes[pEntryNodes[i].mFirstWNode+j];
        os << "[" << (int)wn.mIdx<< "](" << wn.mWeight<< ")" <<"\t";
      }
      os << std::endl;
    }
    return os;
  }

};

template<typename T1, typename T2>
inline std::ostream&
operator <<(std::ostream &os, const InverseFastTreeLookUpTable<T1, T2> &ift)
{
  return ift.display(os);
}

#pragma pack(pop)

/*----------------------------------------------------------------------------*/
/**
 * @brief FastTree instantiation for replica placement.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<PlacementPriorityRandWeightEvaluator, PlacementPriorityComparator> FastPlacementTree;

/**
 * @brief FastTree instantiation for replica placement.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<AccessPriorityRandWeightEvaluator, AccessPriorityComparator> FastAccessTree;

/**
 * @brief InverseFastTreeLookUpTable instantiation for Balancing.
 *
 */
/*----------------------------------------------------------------------------*/
typedef InverseFastTreeLookUpTable<PlacementPriorityRandWeightEvaluator, PlacementPriorityComparator> BalancingInverseFastTree;

/**
 * @brief InverseFastTreeLookUpTable instantiation for Draining.
 *
 */
/*----------------------------------------------------------------------------*/
typedef InverseFastTreeLookUpTable<PlacementPriorityRandWeightEvaluator, PlacementPriorityComparator> DrainingInverseFastTree;


template<typename T1, typename T2>
inline std::ostream&
operator <<(std::ostream &os, const FastTree<T1, T2> &tree)
{
  return tree.recursiveDisplay(os);
}

EOSMGMNAMESPACE_END

#endif /* __EOSMGM_FASTTREE__H__ */
