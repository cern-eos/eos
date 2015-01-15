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
#include <algorithm>
#include <limits>
#define __EOSMGM_FASTTREE__H__

#define DEFINE_TREECOMMON_MACRO

#if __EOSMGM_TREECOMMON__PACK__STRUCTURE__==1
#pragma pack(push,1)
#endif

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
		tFastTreeIdx fastTreeIndex;
		tFastTreeIdx firstBranch;
		tFastTreeIdx branchCount;
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
		tFastTreeIdx left = pNodes[startFrom].firstBranch;
		tFastTreeIdx right = pNodes[startFrom].firstBranch + pNodes[startFrom].branchCount - 1;
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
	void print(char *buf){
		for(int i=0;i<pSize;i++)
			buf+=sprintf(buf,"%s %d %d %d\n",(const char*)&pNodes[i].tag[0],(int)pNodes[i].fastTreeIndex,(int)pNodes[i].firstBranch,(int)pNodes[i].branchCount);
	}

	GeoTag2NodeIdxMap()
	{
		pMaxSize = 0;
		pSize = 0;
		pNodes = 0;
		pSelfAllocated = false;
	}

	~GeoTag2NodeIdxMap()
	{
		if (pSelfAllocated)
		selfUnallocate();
	}

	tFastTreeIdx copyToGeoTag2NodeIdxMap(GeoTag2NodeIdxMap* dest) const
	{
		if (dest->pMaxSize < pSize)
		return pSize;
		dest->pSize = pSize;
		// copy the data
		memcpy(dest->pNodes, pNodes, sizeof(struct Node)*pSize);
		return 0;
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
	allocate(void *buffer, size_t bufSize, tFastTreeIdx size)
	{
		size_t memsize = sizeof(Node) * size;
		if (bufSize < memsize)
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
		return pNodes[node].fastTreeIndex;
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
	FsId2NodeIdxMap(): pMaxSize(0), pSize(0),pSelfAllocated(false)
	{
	}

	~ FsId2NodeIdxMap()
	{
		if (pSelfAllocated)
		selfUnallocate();
	}

	tFastTreeIdx copyToFsId2NodeIdxMap(FsId2NodeIdxMap* dest) const
	{
		if (dest->pMaxSize < pSize)
		return pSize;
		dest->pSize = pSize;
		// copy the data
		memcpy(dest->pFsIds, pFsIds, sizeof(T)*pSize);
		memcpy(dest->pNodeIdxs, pNodeIdxs, sizeof(tFastTreeIdx)*pSize);
		return 0;
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
	allocate(void *buffer, size_t bufSize, tFastTreeIdx size)
	{
		size_t memsize = (sizeof(T) + sizeof(tFastTreeIdx)) * size;
		if (bufSize < memsize)
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
		const_iterator(T* const & fsidPtr, tFastTreeIdx* const & nodeIdxPtr) :
		pFsIdPtr(fsidPtr), pNodeIdxPtr(nodeIdxPtr)
		{
		}
	public:
		const_iterator() :
			pFsIdPtr(NULL),pNodeIdxPtr(NULL)
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
typedef FsId2NodeIdxMap<eos::common::FileSystem::fsid_t> Fs2TreeIdxMap;

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
	char saturationThresh;
	char spreadingFillRatioCap,fillRatioCompTol;
	PlacementPriorityComparator() : saturationThresh(0),spreadingFillRatioCap(0),fillRatioCompTol(0)
	{
	}
	inline signed char
	operator()(const SchedTreeBase::TreeNodeStateChar* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
			const SchedTreeBase::TreeNodeStateChar* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
	{
		return SchedTreeBase::comparePlct<char>(lefts, leftp, rights, rightp,spreadingFillRatioCap,fillRatioCompTol);
	}

	inline bool isValidSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		const int16_t mask = SchedTreeBase::Available|SchedTreeBase::Writable;
		return (mask==(s->mStatus&mask)) && (p->freeSlotsCount>0);
	}

	inline bool isSaturatedSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		return s->dlScore < saturationThresh;
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
	operator()(const SchedTreeBase::TreeNodeStateChar &state, const SchedTreeBase::TreeNodeSlots &plct) const
	{
		//return state.dlScore;
		return plct.maxDlScore;
	}
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for file placement in draining.
 *
 */
/*----------------------------------------------------------------------------*/
class DrainingPlacementPriorityComparator
{
public:
	char saturationThresh;
	char spreadingFillRatioCap,fillRatioCompTol;
	DrainingPlacementPriorityComparator() : saturationThresh(0),spreadingFillRatioCap(0),fillRatioCompTol(0)
	{
	}
	inline signed char
	operator()(const SchedTreeBase::TreeNodeStateChar* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
			const SchedTreeBase::TreeNodeStateChar* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
	{
		return SchedTreeBase::compareDrnPlct<char>(lefts, leftp, rights, rightp,spreadingFillRatioCap,fillRatioCompTol);
	}

	inline bool isValidSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		const int16_t mask = SchedTreeBase::Available|SchedTreeBase::Writable|SchedTreeBase::Drainer;
		return (mask==(s->mStatus&mask)) && (p->freeSlotsCount>0);
	}

	inline bool isSaturatedSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		return s->dlScore < saturationThresh;
	}
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative weights of branches in the tree
 *        having the same priority. This weight is used for file placement
 *        in draining by random sampling in all the branches having
 *        the same priority. It's the same as the general file placement case
 *
 */
/*----------------------------------------------------------------------------*/
typedef PlacementPriorityRandWeightEvaluator DrainingPlacementPriorityRandWeightEvaluator;

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for file placement in balancing.
 *
 */
/*----------------------------------------------------------------------------*/
class BalancingPlacementPriorityComparator
{
public:
	char saturationThresh;
	char spreadingFillRatioCap,fillRatioCompTol;
	BalancingPlacementPriorityComparator() : saturationThresh(0),spreadingFillRatioCap(0),fillRatioCompTol(0)
	{
	}
	inline signed char
	operator()(const SchedTreeBase::TreeNodeStateChar* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
			const SchedTreeBase::TreeNodeStateChar* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
	{
		return SchedTreeBase::compareBlcPlct<char>(lefts, leftp, rights, rightp,spreadingFillRatioCap,fillRatioCompTol);
	}

	inline bool isValidSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		const int16_t mask = SchedTreeBase::Available|SchedTreeBase::Writable|SchedTreeBase::Balancer;
		return (mask==(s->mStatus&mask)) && (p->freeSlotsCount>0);
	}

	inline bool isSaturatedSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		return s->dlScore < saturationThresh;
	}
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative weights of branches in the tree
 *        having the same priority. This weight is used for file placement
 *        in balancing by random sampling in all the branches having
 *        the same priority. It's the same as the general file placement case
 *
 */
/*----------------------------------------------------------------------------*/
typedef PlacementPriorityRandWeightEvaluator BalancingPlacementPriorityRandWeightEvaluator;

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for Read-Only file access.
 *
 */
/*----------------------------------------------------------------------------*/
class ROAccessPriorityComparator
{
public:
	SchedTreeBase::tFastTreeIdx saturationThresh;
	ROAccessPriorityComparator() : saturationThresh(0)
	{
	}
	inline signed char
	operator()(const SchedTreeBase::TreeNodeStateChar* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
			const SchedTreeBase::TreeNodeStateChar* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
	{
		return SchedTreeBase::compareAccess<char>(lefts, leftp, rights, rightp);
	}

	inline bool isValidSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		const int16_t mask = SchedTreeBase::Available|SchedTreeBase::Readable;
		return (mask==(s->mStatus&mask)) && (p->freeSlotsCount>0);
	}

	inline bool isSaturatedSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		return s->ulScore < saturationThresh;
	}
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for Read-Write file access.
 *
 */
/*----------------------------------------------------------------------------*/
class RWAccessPriorityComparator
{
public:
	SchedTreeBase::tFastTreeIdx saturationThresh;
	RWAccessPriorityComparator() : saturationThresh(0)
	{
	}
	inline signed char
	operator()(const SchedTreeBase::TreeNodeStateChar* const &lefts, const SchedTreeBase::TreeNodeSlots* const &leftp,
			const SchedTreeBase::TreeNodeStateChar* const &rights, const SchedTreeBase::TreeNodeSlots* const &rightp) const
	{
		return SchedTreeBase::compareAccess<char>(lefts, leftp, rights, rightp);
	}

	inline bool isValidSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		const int16_t mask = SchedTreeBase::Available|SchedTreeBase::Readable|SchedTreeBase::Writable;
		return (mask==(s->mStatus&mask)) && (p->freeSlotsCount>0);
	}

	inline bool isSaturatedSlot(const SchedTreeBase::TreeNodeStateChar* const &s, const SchedTreeBase::TreeNodeSlots* const &p) const
	{
		return s->ulScore < saturationThresh || s->dlScore < saturationThresh;
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
	operator()(const SchedTreeBase::TreeNodeStateChar &state, const SchedTreeBase::TreeNodeSlots &plct) const
	{
		return plct.maxUlScore;
	}
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for file access in draining.
 *        It's the same as the general file access case
 *
 */
/*----------------------------------------------------------------------------*/
typedef ROAccessPriorityComparator DrainingAccessPriorityComparator;

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative weights of branches in the tree
 *        having the same priority. This weight is used for file access in
 *        draining. It's the same as the general file access case
 *
 */
/*----------------------------------------------------------------------------*/
typedef AccessPriorityRandWeightEvaluator DrainingAccessPriorityRandWeightEvaluator;

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative priorities of branches in
 *        the fast tree for file access in balancing.
 *        It's the same as the general file access case
 *
 */
/*----------------------------------------------------------------------------*/
typedef ROAccessPriorityComparator BalancingAccessPriorityComparator;

/*----------------------------------------------------------------------------*/
/**
 * @brief Functor Class to define relative weights of branches in the tree
 *        having the same priority. This weight is used for file access in
 *        balancing. It's the same as the general file access case
 *
 */
/*----------------------------------------------------------------------------*/
typedef AccessPriorityRandWeightEvaluator BalancingAccessPriorityRandWeightEvaluator;

template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting> struct FastTreeBranchComparator;
template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting> struct FastTreeBranchComparatorInv;

struct FastTreeBranch
{
	SchedTreeBase::tFastTreeIdx sonIdx;
};

template<typename T1,typename T2> class FastTree;
template<typename T1,typename T2,typename T3, typename T4> size_t
copyFastTree(FastTree<T1,T2>* dest,const FastTree<T3,T4>* src);

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
public:
	template<typename T1,typename T2,typename T3, typename T4> friend size_t
	copyFastTree(FastTree<T1,T2>* dest,const FastTree<T3,T4>* src);
	friend class SlowTree;
	friend class SlowTreeNode;
	friend class GeoTreeEngine;
	friend struct TreeEntryMap;
	friend struct FastStructures;
	friend struct FsComparator;

	typedef FastTreeBranch Branch;

	typedef FastTree<FsDataMemberForRand, FsAndFileDataComparerForBranchSorting> tSelf;
	typedef TreeNodeStateChar FsData;

	struct FileData : public TreeNodeSlots
	{
		tFastTreeIdx lastHighestPriorityOffset;
	};

	struct TreeStructure
	{
		tFastTreeIdx fatherIdx;
		tFastTreeIdx firstBranchIdx;
		tFastTreeIdx childrenCount;
	};

	struct FastTreeNode
	{
		TreeStructure treeData;
		FsData fsData;
		FileData fileData;
	};
protected:
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
		return pBranchComp(lefts, static_cast<const TreeNodeSlots * const &>(leftp), rights, static_cast<const TreeNodeSlots * const &>(rightp)) > 0;
	}

	inline bool
	FTGreater(const FastTree::FsData* const &lefts, const FastTree::FileData* const &leftp, const FastTree::FsData* const &rights,
			const FastTree::FileData* const &rightp) const
	{
		return pBranchComp(lefts, static_cast<const TreeNodeSlots * const &>(leftp), rights, static_cast<const TreeNodeSlots * const &>(rightp)) < 0;
	}

public:
	inline bool
	FTLowerNode(const tFastTreeIdx&left, const tFastTreeIdx&right) const
	{
		return FTLower(&pNodes[left].fsData, &pNodes[left].fileData, &pNodes[right].fsData, &pNodes[right].fileData);
	}

	inline bool
	FTGreaterNode(const tFastTreeIdx&left, const tFastTreeIdx&right) const
	{
		return FTGreater(&pNodes[left].fsData, &pNodes[left].fileData, &pNodes[right].fsData, &pNodes[right].fileData);
	}

	inline bool
	isValidSlotNode(tFastTreeIdx node) const
	{
		return pBranchComp.isValidSlot(&pNodes[node].fsData, &pNodes[node].fileData);
	}

	inline bool
	isSaturatedSlotNode(tFastTreeIdx node) const
	{
		return pBranchComp.isSaturatedSlot(&pNodes[node].fsData, &pNodes[node].fileData);
	}

protected:
	inline bool
	FTLowerBranch(const tFastTreeIdx&left, const tFastTreeIdx&right) const
	{
		return FTLower(&pNodes[pBranches[left].sonIdx].fsData, &pNodes[pBranches[left].sonIdx].fileData, &pNodes[pBranches[right].sonIdx].fsData,
				&pNodes[pBranches[right].sonIdx].fileData);
	}

	inline bool
	isValidSlotBranch(tFastTreeIdx branch) const
	{
		return pBranchComp.isValidSlot(&pNodes[pBranches[branch].sonIdx].fsData, &pNodes[pBranches[branch].sonIdx].fileData);
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
		return FTEqual(&pNodes[left].fsData, &pNodes[left].fileData, &pNodes[right].fsData, &pNodes[right].fileData);
	}

	inline bool
	FTEqualBranch(const tFastTreeIdx&left, const tFastTreeIdx&right) const
	{
		return FTEqual(&pNodes[pBranches[left].sonIdx].fsData, &pNodes[pBranches[left].sonIdx].fileData, &pNodes[pBranches[right].sonIdx].fsData,
				&pNodes[pBranches[right].sonIdx].fileData);
	}

	std::ostream&
	recursiveDisplay(std::ostream &os, const FastTreeInfo &info, const std::string &prefix, tFastTreeIdx node) const;

	inline tFastTreeIdx
	getRandomBranch(const tFastTreeIdx &node, bool* visited=NULL) const
	{
		const tFastTreeIdx &nBranches = pNodes[node].fileData.lastHighestPriorityOffset + 1;
		__EOSMGM_TREECOMMON_DBG3__ if(eos::common::Logging::gLogMask & LOG_DEBUG)
		{
			stringstream ss;
			ss << "getRandomBranch at " << (*pTreeInfo)[node] << " choose among " << (int) nBranches << std::endl;
			eos_static_debug(ss.str().c_str());
		}
		int weightSum = 0;

		for (tFastTreeIdx i = pNodes[node].treeData.firstBranchIdx; i < pNodes[node].treeData.firstBranchIdx + nBranches; i++)
		{
			const FastTreeNode &node = pNodes[pBranches[i].sonIdx];
			weightSum += max(pRandVar(node.fsData, node.fileData),(unsigned char)0);
		}
		if(weightSum)
		{
		int r = rand();
		r = r % (weightSum);
		tFastTreeIdx i = 0;

		for (weightSum = 0, i = pNodes[node].treeData.firstBranchIdx; i < pNodes[node].treeData.firstBranchIdx + nBranches; i++)
		{
			const FastTreeNode &node = pNodes[pBranches[i].sonIdx];
			weightSum += pRandVar(node.fsData, node.fileData);
			if (weightSum > r)
			break;
		}

		__EOSMGM_TREECOMMON_CHK1__
		assert(i <= pNodes[node].treeData.firstBranchIdx + pNodes[node].fileData.lastHighestPriorityOffset);

		return pBranches[i].sonIdx;
		}
		else
		{
			// in this case all weights are 0 -> uniform probability
			return pBranches[pNodes[node].treeData.firstBranchIdx+rand()%nBranches].sonIdx;
		}
	}

	inline bool
	getRandomBranchGeneric(const tFastTreeIdx &brchBegIdx, const tFastTreeIdx &brchEndIdx, tFastTreeIdx * const & output ,bool* visitedNode) const
	{
		if(brchBegIdx>=brchEndIdx)
		return false;

		__EOSMGM_TREECOMMON_DBG3__ if(eos::common::Logging::gLogMask & LOG_DEBUG)
		{
			stringstream ss;
			ss << "getRandomBranchGeneric from Branch " << (int)brchBegIdx << " to branch " << (int)brchEndIdx << std::endl;
			eos_static_debug(ss.str().c_str());
		}
		int weightSum = 0;

		for (tFastTreeIdx i = brchBegIdx; i < brchEndIdx; i++)
		{
			tFastTreeIdx nodeIdx = pBranches[i].sonIdx;
			if(!visitedNode[nodeIdx])
			{
				const FastTreeNode &node = pNodes[pBranches[i].sonIdx];
				weightSum += pRandVar(node.fsData, node.fileData);
			}
		}

		if(weightSum==0)
		return false;

		int r = rand();
		r = r % (weightSum);
		tFastTreeIdx i = 0;

		for (weightSum = 0, i = brchBegIdx; i < brchEndIdx; i++)
		{
			tFastTreeIdx nodeIdx = pBranches[i].sonIdx;
			if(!visitedNode[nodeIdx])
			{
				const FastTreeNode &node = pNodes[pBranches[i].sonIdx];
				weightSum += pRandVar(node.fsData, node.fileData);
				if (weightSum > r)
				break;
			}
		}

		__EOSMGM_TREECOMMON_CHK1__
		assert(i <brchEndIdx);

		*output = pBranches[i].sonIdx;

		return true;
	}

	inline tFastTreeIdx
	findNewRank(tFastTreeIdx left, tFastTreeIdx right, const tFastTreeIdx &modified) const
	{
		__EOSMGM_TREECOMMON_DBG3__ eos_static_debug( "findNewRank: %d %d %d\n",(int) left ,(int) right ,(int) modified);
		if (right == left)
		return right;
		bool firstiter = true;

		while (true)
		{
			if (!firstiter)
			assert(!FTLowerBranch(modified,right) && !FTLowerBranch(left,modified));
			if (!firstiter && right - left == 1)
			{
				assert(!FTLowerBranch(modified,right) && !FTLowerBranch(right-1,modified));
				return right;
			}
			if (left == modified)
			left = left + 1;
			if (right == modified)
			right = right - 1;

			if (!FTLowerNode(pBranches[modified].sonIdx, pBranches[left].sonIdx))
			return left;

			if (!FTLowerNode(pBranches[right].sonIdx, pBranches[modified].sonIdx))
			return right + 1; // which might not exist show that it should be at the end

			tFastTreeIdx mid = (left + right) / 2;
			if (mid == modified)
			{ // mid point should NOT be the middle point
				if (mid + 1 > right)
				mid--;
				else
				mid++;
			}

			if (!FTLowerNode(pBranches[modified].sonIdx, pBranches[mid].sonIdx))
			right = mid;
			else
			left = mid;
			firstiter = false;
		}
	}

	inline void
	fixBranchSorting(const tFastTreeIdx &node, const tFastTreeIdx &modifiedBranchIdx)
	{
		__EOSMGM_TREECOMMON_CHK1__
		assert(
				modifiedBranchIdx >= pNodes[node].treeData.firstBranchIdx && modifiedBranchIdx < pNodes[node].treeData.firstBranchIdx+pNodes[node].treeData.childrenCount);

		const Branch &modifiedBranch = pBranches[modifiedBranchIdx];
		const tFastTreeIdx &firstBranchIdx = pNodes[node].treeData.firstBranchIdx;
		const tFastTreeIdx &nbChildren = pNodes[node].treeData.childrenCount;
		tFastTreeIdx &lastHPOffset = pNodes[node].fileData.lastHighestPriorityOffset;
		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(0, false);

		if (nbChildren < 2)
		return;
		// if it's already ordered, nothing to do.
		if ((modifiedBranchIdx == firstBranchIdx && !FTLowerBranch(modifiedBranchIdx, modifiedBranchIdx + 1))
				|| (modifiedBranchIdx == firstBranchIdx + nbChildren - 1 && !FTLowerBranch(modifiedBranchIdx - 1, modifiedBranchIdx))
				|| (!FTLowerBranch(modifiedBranchIdx, modifiedBranchIdx + 1) && !FTLowerBranch(modifiedBranchIdx - 1, modifiedBranchIdx)))
		goto update_and_return;

		{
			tFastTreeIdx newrank = findNewRank(firstBranchIdx, firstBranchIdx + nbChildren - 1, modifiedBranchIdx);
			__EOSMGM_TREECOMMON_DBG3__ eos_static_debug("findNewRank returned %d\n" , (int) newrank );

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
		while (lastHPOffset < nbChildren - 1
				&& !FTLowerBranch(firstBranchIdx + lastHPOffset + 1,
						firstBranchIdx + lastHPOffset) )
		{
			lastHPOffset++;
		}

		__EOSMGM_TREECOMMON_CHK3__
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

		const Branch &modifiedBranch = pBranches[modifiedBranchIdx];
		const FsData &modifiedFsData = pNodes[modifiedBranch.sonIdx].fsData;
		const FileData &modifiedFileData = pNodes[modifiedBranch.sonIdx].fileData;
		const tFastTreeIdx &firstBranchIdx = pNodes[node].treeData.firstBranchIdx;
		const tFastTreeIdx &nbChildren = pNodes[node].treeData.childrenCount;
		tFastTreeIdx &lastHPOffset = pNodes[node].fileData.lastHighestPriorityOffset;
		const bool modifiedIsInHp = modifiedBranchIdx <= firstBranchIdx + lastHPOffset;

		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(0, false);

		// this function should not be called in that case
		if (!nbChildren)
		return;

		if (modifiedBranchIdx == firstBranchIdx + nbChildren - 1)// nothing to do, the sorting is already the worst rated
		{
			goto update_and_return;
		}
		// if all the branches have the lowest priority level, the selected branches just go to the end
		if (lastHPOffset == pNodes[node].treeData.childrenCount - 1)
		{
			std::swap(pBranches[modifiedBranchIdx], pBranches[firstBranchIdx + lastHPOffset]);
			goto update_and_return;
		}

		// if the modified branch still have a highest or equal priority than the next priority level a swap is enough
		else if ((modifiedBranchIdx <= firstBranchIdx + lastHPOffset) &&// this one should ALWAYS be TRUE for a placement
				!FTLower(&modifiedFsData, &(modifiedFileData), &pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].sonIdx].fsData,
						&pNodes[pBranches[firstBranchIdx + lastHPOffset + 1].sonIdx].fileData))
		{
			std::swap(pBranches[modifiedBranchIdx], pBranches[firstBranchIdx + lastHPOffset]);
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
					&& FTLower(&modifiedFsData, &modifiedFileData, &pNodes[pBranches[insertionIdx].sonIdx].fsData,
							&pNodes[pBranches[insertionIdx].sonIdx].fileData); insertionIdx++)
			{
			}
			// keep a copy of the branch
			Branch modbr = modifiedBranch;
			// move the appropriate range of branches
			memmove(&pBranches[modifiedBranchIdx], &pBranches[modifiedBranchIdx + 1], (insertionIdx - modifiedBranchIdx) * sizeof(Branch));
			// insert the modified branch
			pBranches[insertionIdx - 1] = modbr;
		}

		update_and_return: if (modifiedIsInHp && lastHPOffset > 0)
		{
			// there is more than one branch having the highest priority, just decrement if the priority got lower
			// the modified branch is at the end now and has been swapped with the modifiedBranch
			if(FTLowerBranch(firstBranchIdx+lastHPOffset,firstBranchIdx))
			{
				lastHPOffset--;
			}
		}
		else
		{ // the modified node is the last one with the maximum priority
			lastHPOffset = 0;
			while (lastHPOffset < nbChildren - 1
					&& !FTLowerBranch(firstBranchIdx + lastHPOffset + 1, firstBranchIdx + lastHPOffset))
			lastHPOffset++;
		}

		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(0, true);
		return;
	}

public:
	inline void
	sortBranchesAtNode(const tFastTreeIdx &node, bool recursive=false)
	{
		FastTreeBranchComparator<FsDataMemberForRand,FsAndFileDataComparerForBranchSorting> comparator(this);
		FastTreeBranchComparatorInv<FsDataMemberForRand,FsAndFileDataComparerForBranchSorting> comparator2(this);

		const tFastTreeIdx &firstBranchIdx = pNodes[node].treeData.firstBranchIdx;
		const tFastTreeIdx &nbChildren = pNodes[node].treeData.childrenCount;
		tFastTreeIdx &lastHPOffset = pNodes[node].fileData.lastHighestPriorityOffset;

		if(recursive)
		for(SchedTreeBase::tFastTreeIdx b=firstBranchIdx;b<firstBranchIdx+nbChildren;b++)
		sortBranchesAtNode(pBranches[b].sonIdx,true);

		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(node, false);

		if(nbChildren<2)
		return;

		std::sort(pBranches+firstBranchIdx,pBranches+firstBranchIdx+nbChildren,comparator);
		// other possible types of sorting algorithm
		//insertionSort(pBranches+firstBranchIdx,pBranches+firstBranchIdx+nbChildren,comparator2);
		//bubbleSort(pBranches+firstBranchIdx,pBranches+firstBranchIdx+nbChildren,comparator2);
		switch(nbChildren)
		{
			case 2:
			if(FTLowerBranch(firstBranchIdx+1,firstBranchIdx))
			lastHPOffset = 0;
			else
			lastHPOffset = 1;
			break;
			default:
			Branch *ub = std::upper_bound(pBranches+firstBranchIdx+1,pBranches+firstBranchIdx+nbChildren,pBranches[firstBranchIdx],comparator);
			lastHPOffset = (
					ub
					- (pBranches+firstBranchIdx+1));
			break;
		}
		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(node, true);

		return;
	}

	inline void
	sortAllBranches()
	{
		sortBranchesAtNode(0,true);
	}

	inline void
	updateBranch(const tFastTreeIdx &node)
	{
		const tFastTreeIdx &firstBranchIdx = pNodes[node].treeData.firstBranchIdx;
		const tFastTreeIdx &nbChildren = pNodes[node].treeData.childrenCount;

		if(pNodes[node].treeData.childrenCount)
		{
			sortBranchesAtNode(node,false);
			pNodes[node].fsData.aggregate(
					&pNodes[ firstBranchIdx+1 ].fsData , // this index firstBranchIdx+1 is a HACK to get the index of the first node (in memory) of a branch of the current node
					&pNodes[ firstBranchIdx+1 + nbChildren ].fsData,
					sizeof(FastTreeNode)
			);
			pNodes[node].fileData.aggregate(
					&pNodes[ firstBranchIdx+1 ].fileData ,
					&pNodes[ firstBranchIdx+1 + nbChildren].fileData,
					&pNodes[ firstBranchIdx+1 ].fsData ,// this index firstBranchIdx+1 is a HACK to get the index of the first node (in memory) of a branch of the current node
					&pNodes[ firstBranchIdx+1 + nbChildren].fsData,
					sizeof(FastTreeNode),
					sizeof(FastTreeNode)
			);

		}
		else
		{
			// there is a free slot but if it's not valid make it null
			if(!isValidSlotNode(node)) pNodes[node].fileData.freeSlotsCount = 0;
			pNodes[node].fileData.maxUlScore = pNodes[node].fsData.ulScore;
			pNodes[node].fileData.maxDlScore = pNodes[node].fsData.dlScore;
			pNodes[node].fileData.avgUlScore = pNodes[node].fsData.ulScore;
			pNodes[node].fileData.avgDlScore = pNodes[node].fsData.dlScore;
		}
		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(0, true);

		if(pNodes[node].treeData.fatherIdx!=node)
		updateBranch(pNodes[node].treeData.fatherIdx);
	}

	inline void
	updateTree(const tFastTreeIdx &node=0, bool noFreeSlotIfInvalid=true, bool oneFreeSlotIfValid=false)
	{

		const tFastTreeIdx &firstBranchIdx = pNodes[node].treeData.firstBranchIdx;
		const tFastTreeIdx &nbChildren = pNodes[node].treeData.childrenCount;

		for(SchedTreeBase::tFastTreeIdx b=firstBranchIdx;b<firstBranchIdx+nbChildren;b++)
		updateTree(pBranches[b].sonIdx,noFreeSlotIfInvalid,oneFreeSlotIfValid);

		if(nbChildren<2)
		pNodes[node].fileData.lastHighestPriorityOffset = 0;

		if(nbChildren)
		{
			sortBranchesAtNode(node,false);

			pNodes[node].fsData.aggregate(
					&pNodes[ firstBranchIdx+1 ].fsData , // this index firstBranchIdx+1 is a HACK to get the index of the first node (in memory) of a branch of the current node
					&pNodes[ firstBranchIdx+1 + nbChildren].fsData,
					sizeof(FastTreeNode)
			);
			pNodes[node].fileData.aggregate(
					&pNodes[ firstBranchIdx+1 ].fileData ,
					&pNodes[ firstBranchIdx+1 + nbChildren].fileData,
					&pNodes[ firstBranchIdx+1 ].fsData ,// this index firstBranchIdx+1 is a HACK to get the index of the first node (in memory) of a branch of the current node
					&pNodes[ firstBranchIdx+1 + nbChildren].fsData,
					sizeof(FastTreeNode),
					sizeof(FastTreeNode)
			);
		}
		else
		{
			// there is a free slot but if it's not valid make it null
			unsigned char freeSlots = 1;
			std::swap(pNodes[node].fileData.freeSlotsCount,freeSlots);
			if(isValidSlotNode(node))
			{
				if(oneFreeSlotIfValid)
				pNodes[node].fileData.freeSlotsCount = 1;
				else
				std::swap(pNodes[node].fileData.freeSlotsCount,freeSlots);
			}
			else
			{
				if(noFreeSlotIfInvalid)
				pNodes[node].fileData.freeSlotsCount = 0;
				else
				std::swap(pNodes[node].fileData.freeSlotsCount,freeSlots);
			}
		}

		pNodes[node].fileData.maxUlScore = pNodes[node].fsData.ulScore;
		pNodes[node].fileData.maxDlScore = pNodes[node].fsData.dlScore;
		pNodes[node].fileData.avgUlScore = pNodes[node].fsData.ulScore;
		pNodes[node].fileData.avgDlScore = pNodes[node].fsData.dlScore;

		__EOSMGM_TREECOMMON_CHK3__
		checkConsistency(node, true);
	}

	unsigned char
	getMaxNodeCount() const
	{
		return pMaxNodeCount;
	}

	inline bool
	findFreeSlotsMultiple(std::vector<tFastTreeIdx>&idxs, tFastTreeIdx nReplicas, tFastTreeIdx startFrom = 0, bool allowUpRoot = false)
	{
		// NOT IMPLEMENTED
		eos_static_crit("NOT IMPLEMENTED");
		return false;
	}

	inline tFastTreeIdx
	findFreeSlotsAll(tFastTreeIdx *idxs, tFastTreeIdx sizeIdxs, tFastTreeIdx startFrom = 0, bool allowUpRoot = false, const int &maskStatus = None, tFastTreeIdx *upRootLevelsCount=NULL, tFastTreeIdx *upRootLevelsIdxs=NULL, tFastTreeIdx *upRootLevels=NULL) const
	{
		tFastTreeIdx sizeIdxsBak = sizeIdxs;
		if(upRootLevelsIdxs) *upRootLevelsCount = 0;
		if (_findFreeSlotsAll(idxs, sizeIdxs, startFrom, allowUpRoot, startFrom, maskStatus, upRootLevelsCount, upRootLevelsIdxs, upRootLevels, 0))
		{
			if(upRootLevelsIdxs)
			{
				for(int k=0;k<*upRootLevelsCount;k++) upRootLevelsIdxs[k] = sizeIdxsBak -upRootLevelsIdxs[k];
			}
			return sizeIdxsBak - sizeIdxs;
		}
		else
		return 0;
	}

	inline bool
	_findFreeSlotsAll(tFastTreeIdx *&idxs, tFastTreeIdx &sizeIdxs, tFastTreeIdx startFrom, bool allowUpRoot , tFastTreeIdx callerNode, const int &statusMask,
			tFastTreeIdx *upRootLevelsCount, tFastTreeIdx *upRootLevelsIdxs, tFastTreeIdx *upRootLevels, tFastTreeIdx currentUpRootLevel) const
	{
		if (!pNodes[startFrom].treeData.childrenCount)
		{
			if (pNodes[startFrom].fileData.freeSlotsCount && ((pNodes[startFrom].fsData.mStatus & statusMask) == statusMask) )
			{
				if (sizeIdxs)
				{
					if(isValidSlotNode(startFrom))
					{
						// if the slot is free, it should be a valid one, see the explanation in findFreeSlot
						if(upRootLevelsIdxs)
						{
							if( *upRootLevelsCount == 0 )
							{
								upRootLevels[0] = currentUpRootLevel;
								upRootLevelsIdxs[0] = sizeIdxs;
								(*upRootLevelsCount)++;
							}
							else if(upRootLevels[*upRootLevelsCount-1] < currentUpRootLevel)
							{
								upRootLevels[*upRootLevelsCount] = currentUpRootLevel;
								upRootLevelsIdxs[*upRootLevelsCount] = sizeIdxs;
								(*upRootLevelsCount)++;
							}
						}
						*idxs = startFrom;
						idxs++;
						sizeIdxs--;
					}
				}
				else
				{
					// no enough space to write all the replicas
					// it should not happen when called from findFreeSlotsAll because it's checked there
					return false;
				}
			}
		}
		for (tFastTreeIdx bidx = pNodes[startFrom].treeData.firstBranchIdx;
				bidx < pNodes[startFrom].treeData.firstBranchIdx + pNodes[startFrom].treeData.childrenCount; bidx++)
		{
			if ( pBranches[bidx].sonIdx == callerNode ||
					!pNodes[pBranches[bidx].sonIdx].fileData.freeSlotsCount ||
					!((pNodes[startFrom].fsData.mStatus & statusMask) == statusMask) )
			continue;
			if (!_findFreeSlotsAll(idxs, sizeIdxs, pBranches[bidx].sonIdx, false, startFrom, statusMask, upRootLevelsCount, upRootLevelsIdxs, upRootLevels, currentUpRootLevel))
			{
				// something is wrong. It should not happen
				// free slots are supposed to be there but none are found!
				eos_static_crit("Inconsistency in FastGeoTree");
				return false;
			}
		}

		if(allowUpRoot && startFrom)
		{
			if(upRootLevelsIdxs)
			{
				currentUpRootLevel++;
			}
			_findFreeSlotsAll(idxs, sizeIdxs, pNodes[startFrom].treeData.fatherIdx, true, startFrom, statusMask, upRootLevelsCount, upRootLevelsIdxs, upRootLevels, currentUpRootLevel);
		}

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
			for (tFastTreeIdx bidx = pNodes[node].treeData.firstBranchIdx; bidx < pNodes[node].treeData.firstBranchIdx + pNodes[node].treeData.childrenCount;
					bidx++)
			checkConsistency(pBranches[bidx].sonIdx, checkOrder, true, map);
		}

		assert(
				pNodes[node].treeData.childrenCount == 0 || ( pNodes[node].fileData.lastHighestPriorityOffset >= 0 && pNodes[node].fileData.lastHighestPriorityOffset < pNodes[node].treeData.childrenCount ));

		// check that every node is referred at most once in a branch
		for (tFastTreeIdx bidx = pNodes[node].treeData.firstBranchIdx; bidx < pNodes[node].treeData.firstBranchIdx + pNodes[node].treeData.childrenCount;
				bidx++)
		{
			// check that this node is not already referred
			assert(!map->count(pBranches[bidx].sonIdx));
			(*map)[pBranches[bidx].sonIdx] = node;// set the father in the map
		}

		// check the order is respected in the branches
		if (checkOrder)
		{
			bool checkedHpOfs = false;
			tFastTreeIdx lastHpOfs = 0;
			for (tFastTreeIdx bidx = pNodes[node].treeData.firstBranchIdx; bidx < pNodes[node].treeData.firstBranchIdx + pNodes[node].treeData.childrenCount - 1;
					bidx++)
			{
				assert(
						//!FTLower( &pNodes[pBranches[bidx].mSonIdx].mFsData, &pNodes[pBranches[bidx].mSonIdx].mFileData, &pNodes[pBranches[bidx+1].mSonIdx].mFsData, &pNodes[pBranches[bidx+1].mSonIdx].mFileData )
						!FTLowerBranch( bidx, bidx+1)
				);
				if (!checkedHpOfs
						&& !FTEqual(&pNodes[pBranches[bidx].sonIdx].fsData, &pNodes[pBranches[bidx].sonIdx].fileData, &pNodes[pBranches[bidx + 1].sonIdx].fsData,
								&pNodes[pBranches[bidx + 1].sonIdx].fileData))
				{
					assert(lastHpOfs == pNodes[node].fileData.lastHighestPriorityOffset);
					checkedHpOfs = true;
				}
				lastHpOfs++;
			}
			if (!checkedHpOfs && lastHpOfs)
			{
				assert(pNodes[node].treeData.childrenCount-1 == pNodes[node].fileData.lastHighestPriorityOffset);
			}
		}
		if (del)
		delete map;
	}

	std::ostream&
	recursiveDisplay(std::ostream &os, const std::string &prefix = "") const
	{
		if(!pNodes[0].treeData.childrenCount)
		return os; // NOTHING TO DISPLAY
		return recursiveDisplay(os, prefix, 0);
	}

protected:
	FsDataMemberForRand pRandVar;
	FsAndFileDataComparerForBranchSorting pBranchComp;
public:
	FastTree() :
	pRandVar(), pBranchComp()
	{
		pMaxNodeCount = 0;
		pSelfAllocated = false;
	}

	~FastTree()
	{
		if (pSelfAllocated)
		selfUnallocate();
	}

	void setSaturationThreshold( const char &thresh)
	{
		pBranchComp.saturationThresh = thresh;
	}
	void setSpreadingFillRatioCap(const char &cap) {
		pBranchComp.spreadingFillRatioCap = cap;
	}
	void setFillRatioCompTol(const char &tol) {
		pBranchComp.fillRatioCompTol = tol;
	}
	bool
	selfAllocate(tFastTreeIdx size)
	{
		pMaxNodeCount = size;
		size_t memsize = (sizeof(FastTreeNode) + sizeof(Branch)) * size;
		__EOSMGM_TREECOMMON_DBG2__ eos_static_debug("self allocation size = %lu\n",memsize);
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

	tSelf & operator = (const tSelf &model)
	{
		(*static_cast<SchedTreeBase*>(this)) = *static_cast<const SchedTreeBase*>(&model);
		this->pFs2Idx = model.pFs2Idx;
		this->pMaxNodeCount = model.pMaxNodeCount;
		this->pSelfAllocated = model.pSelfAllocated;
		this->pTreeInfo = model.pTreeInfo;
		this->pBranchComp = model.pBranchComp;
		return *this;
	}

	size_t
	copyToBuffer(char* buffer, size_t bufSize) const
	{
		size_t memsize = (sizeof(FastTreeNode) + sizeof(Branch)) * pMaxNodeCount + sizeof(FastTree);
		if (bufSize < memsize)
		return memsize;
		// copy all the data members
		tSelf *destFastTree = (tSelf *) (buffer);
		// adjust the value of some of them
		(*destFastTree) = *this;
		destFastTree->pNodes = (FastTreeNode *) (buffer += sizeof(tSelf));
		destFastTree->pBranches = (Branch *) (buffer += sizeof(FastTreeNode) * pMaxNodeCount);
		memcpy(destFastTree->pNodes, pNodes, (sizeof(FastTreeNode) + sizeof(Branch)) * pMaxNodeCount);// pNodes and pBranches copied at once
		return 0;
	}


	template<typename T1,typename T2> size_t
	copyToFastTree(FastTree<T1,T2>* dest) const
	{
		return copyFastTree(dest,this);
	}

	std::ostream&
	recursiveDisplay(std::ostream &os, const std::string &prefix, tFastTreeIdx node) const
	{
		std::stringstream ss;
		ss << prefix;
		os << std::right << std::setw(8) << std::setfill('-');
		tFastTreeIdx &nbChildren = pNodes[node].treeData.childrenCount;
		if ((*pTreeInfo)[node].nodeType == TreeNodeInfo::intermediate)
		os << (*pTreeInfo)[node].geotag;
		else if ((*pTreeInfo)[node].nodeType == TreeNodeInfo::fs)
		os << std::dec << (unsigned int)(*pTreeInfo)[node].fsId;
		os << "/( free:" << (int) pNodes[node].fileData.freeSlotsCount << "|repl:" << (int) pNodes[node].fileData.takenSlotsCount
				<< "|pidx:"<< (int) pNodes[node].fileData.lastHighestPriorityOffset<< "|status:"
				<< std::hex << pNodes[node].fsData.mStatus << std::dec
				<< "|ulSc:"<< (int) pNodes[node].fsData.ulScore
				<< "|dlSc:"<< (int) pNodes[node].fsData.dlScore
				<< "|filR:"<< (int) pNodes[node].fsData.fillRatio
				<< "|totS:"<<  pNodes[node].fsData.totalSpace<< ")";
		ss << std::right << std::setw(7) << std::setfill(' ') << "";

		if (!nbChildren)
		os << "@" << (*pTreeInfo)[node].host << std::endl;
		else
		{
			os << std::endl;
			tFastTreeIdx &firstBranchIdx = pNodes[node].treeData.firstBranchIdx;
			for (tFastTreeIdx branchIdx = firstBranchIdx; branchIdx < firstBranchIdx + nbChildren; branchIdx++)
			{
				tFastTreeIdx childIdx = pBranches[branchIdx].sonIdx;
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
		__EOSMGM_TREECOMMON_CHK1__
		assert(pNodes[node].fileData.freeSlotsCount>0);
		__EOSMGM_TREECOMMON_CHK2__
		checkConsistency(0);
		pNodes[node].fileData.freeSlotsCount--;
		pNodes[node].fileData.takenSlotsCount++;
		//checkConsistency(0,false);

		// if there is a father node, update its branches
		if (node)
		{
			tFastTreeIdx father = pNodes[node].treeData.fatherIdx;
			tFastTreeIdx firstBranchIndex = pNodes[father].treeData.firstBranchIdx;
			tFastTreeIdx nbBranches = pNodes[father].treeData.childrenCount;
			tFastTreeIdx matchBranchIdx;
			// first locate the branch (it should be in the first positions if it's a placement)
			for (matchBranchIdx = firstBranchIndex; matchBranchIdx < firstBranchIndex + nbBranches && pBranches[matchBranchIdx].sonIdx != node; matchBranchIdx++)
			{
			}
			__EOSMGM_TREECOMMON_CHK1__
			assert(pBranches[matchBranchIdx].sonIdx==node);
			// the branches are supposed to be ordered before the update
			if (useHpSpeedUp)
			fixBranchSortingHP(father, matchBranchIdx);// optimized for
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
		__EOSMGM_TREECOMMON_CHK2__
		checkConsistency(0);
		pNodes[node].fileData.freeSlotsCount++;

		// if there is a father node, update its branches
		if (node)
		{
			tFastTreeIdx father = pNodes[node].treeData.fatherIdx;
			tFastTreeIdx firstBranchIndex = pNodes[father].treeData.firstBranchIdx;
			tFastTreeIdx nbBranches = pNodes[father].treeData.childrenCount;
			tFastTreeIdx matchBranchIdx;
			// first locate the branch (it should be in the first positions if it's a placement)
			for (matchBranchIdx = firstBranchIndex; matchBranchIdx < firstBranchIndex + nbBranches && pBranches[matchBranchIdx].sonIdx != node; matchBranchIdx++)
			{
			}
			__EOSMGM_TREECOMMON_CHK1__
			assert(pBranches[matchBranchIdx].sonIdx==node);
			// the branches are supposed to be ordered before the update
			if (useHpSpeedUp)
			fixBranchSortingHP(father, matchBranchIdx);// optimized for
			else
			fixBranchSorting(father, matchBranchIdx);
			// finally iterate upper in the tree
			incrementFreeSlot(father, useHpSpeedUp);
		}
	}

	bool
	findFreeSlotFirstHit(tFastTreeIdx& newReplica, tFastTreeIdx startFrom=0, bool allowUpRoot=false, bool decrFreeSlot = true)
	{
		if (pNodes[startFrom].fileData.freeSlotsCount)
		{
			if (!pNodes[startFrom].treeData.childrenCount)
			{
				if(isValidSlotNode(startFrom))
				{
					// we are arrived
					newReplica = startFrom;
					// update the file replica info in the tree
					if (decrFreeSlot) {
					decrementFreeSlot(newReplica, true);
					}
					return true;
				}
				else
				{ // if the current one is not valid, all the other leaves sharing the same father are not (because they are ordered)
					// this also implies that all the available slots should satisfy this valid slot condition because we could be stuck in a
					// situation where some free slots are valid and some other are not and it's impossible to know when going through the tree
					assert(false);
					return false;
				}
			}
			else
			{
				if(pNodes[startFrom].fileData.lastHighestPriorityOffset)
				{
					return findFreeSlotFirstHit(newReplica, getRandomBranch(startFrom), false, decrFreeSlot);
				}
				else
				{
					return findFreeSlotFirstHit(newReplica, pBranches[pNodes[startFrom].treeData.firstBranchIdx].sonIdx, false,decrFreeSlot);
				}
			}
		}
		else
		{
			// no free slot then, try higher if allowed and not already at the root
			if (allowUpRoot && startFrom)
			{
				// we won't go through the current branch again because it has no free slot. Else, we wouldn't go uproot
				return findFreeSlotFirstHit(newReplica, pNodes[startFrom].treeData.fatherIdx, allowUpRoot, decrFreeSlot);
			}
			else
			return false;
		}
	}

	bool
	findFreeSlotSkipSaturated(tFastTreeIdx& newReplica, tFastTreeIdx startFrom, bool allowUpRoot, bool decrFreeSlot, bool *visited=NULL)
	{
		// initial call to allocate the visited array in the stack
		if(!visited)
		{
			// initialize children as non visited
			// visited children (over allocated but it allows a static allocation)
			bool localvisited[(256)^sizeof(tFastTreeIdx)];
			for(size_t t=0; t<(256^sizeof(tFastTreeIdx)); t++ ) localvisited[t] = false;

			SchedTreeBase::tFastTreeIdx fatherIdx = startFrom;
			if(!allowUpRoot){
				//make the current branch the root
				swap(fatherIdx,pNodes[startFrom].treeData.fatherIdx);
		}

			bool ret = findFreeSlotSkipSaturated(newReplica, startFrom, true, decrFreeSlot, localvisited);

			if(!allowUpRoot){
				// put back the original father
				swap(fatherIdx,pNodes[startFrom].treeData.fatherIdx);
			}

			return ret;
		}

		if(!visited[startFrom] && (pNodes[startFrom].fileData.freeSlotsCount) ){
			// it's a leaf
			if (!pNodes[startFrom].treeData.childrenCount)
			{
				if(isValidSlotNode(startFrom) && !isSaturatedSlotNode(startFrom))
				{
					eos_static_debug("node %d is valid and unsaturated", (int)startFrom );
					// we are arrived
					newReplica = startFrom;
					// update the file replica info in the tree
					if (decrFreeSlot) {
					decrementFreeSlot(newReplica, true);
					}
					// we found something, we stop here
					return true;
				}
				else
				{
					eos_static_debug("node %d is NOT (valid and unsaturated) status=%x, dlScore=%d, freeslot=%d, isvalid=%d, issaturated=%d", (int)startFrom,
							(int)pNodes[startFrom].fsData.mStatus,(int)pNodes[startFrom].fsData.dlScore,(int)pNodes[startFrom].fileData.freeSlotsCount
							,(int)isValidSlotNode(startFrom),(int)isSaturatedSlotNode(startFrom));
					// there is nothing we can use here either not valid either saturated
					goto go_back;
				}
			}
			// it's a branch
			else
			{
				tFastTreeIdx priorityLevel, begBrIdx, endBrIdx;
				priorityLevel = 0;
				endBrIdx = begBrIdx = pNodes[startFrom].treeData.firstBranchIdx;
				const tFastTreeIdx endIdx = endBrIdx + pNodes[startFrom].treeData.childrenCount;
				// visit each level of priority
				while(endBrIdx<endIdx)
				{
					// if the first node is this level of priority doesn't have any slot available
					// and we reached that point. It means that the whole subranch doesn't have any available slot
					// we return false
					if(!pNodes[pBranches[begBrIdx].sonIdx].fileData.freeSlotsCount)
					{
						goto go_back;
					}

					// endBrIdx
					if(priorityLevel)
					{
						while(endBrIdx<endIdx &&
								!FTLowerBranch(endBrIdx,begBrIdx))
						endBrIdx++;
					}
					else
					{
						endBrIdx += pNodes[startFrom].fileData.lastHighestPriorityOffset+1;
					}

					// visit the current level of priority
					// as long as there is still some branch to try in this priority level, do it
					if(endBrIdx==begBrIdx+1)
					{
						if(findFreeSlotSkipSaturated(newReplica, pBranches[begBrIdx].sonIdx, false, decrFreeSlot,visited))
						return true;
					}
					else
					{
						tFastTreeIdx nodeIdxToVisit = 0;
						// try until no branch is selectable
						while(getRandomBranchGeneric(begBrIdx,endBrIdx,&nodeIdxToVisit,visited))
						{
							// if only one branch, no need to call getRandomBranch
							if(findFreeSlotSkipSaturated(newReplica, nodeIdxToVisit, false, decrFreeSlot,visited))
							return true;
						}
					}
					// move to the next priority level
					priorityLevel++;
					begBrIdx = endBrIdx;
				}
				// no slot available in any priority level -> nothing is available
				goto go_back;
			}
		}
		go_back:
		// if the node is already visited all the subbranches are visited too
		// go upstream
		if(allowUpRoot && startFrom!=pNodes[startFrom].treeData.fatherIdx)
			{
			visited[startFrom] = true;
				return findFreeSlotSkipSaturated(newReplica, pNodes[startFrom].treeData.fatherIdx, allowUpRoot, decrFreeSlot,visited);
			}
		else // we are back to the root (the node father of himself), no luck
		{
			visited[startFrom] = true;
			return false;
		}
	}

	inline bool
	findFreeSlot(tFastTreeIdx& newReplica, tFastTreeIdx startFrom=0, bool allowUpRoot=false, bool decrFreeSlot = true, bool skipSaturated=false)
	{
		if(skipSaturated)
		{
			return findFreeSlotSkipSaturated(newReplica,startFrom,allowUpRoot,decrFreeSlot);
		}
		else
		{
			return findFreeSlotFirstHit(newReplica,startFrom,allowUpRoot,decrFreeSlot);
		}
	}

};

template<typename T1,typename T2,typename T3, typename T4> inline size_t
copyFastTree(FastTree<T1,T2>* dest,const FastTree<T3,T4>* src)
{
	if (dest->pMaxNodeCount < src->pMaxNodeCount)
	return src->pMaxNodeCount;
	// copy some members
	dest->pFs2Idx = src->pFs2Idx;
	dest->pTreeInfo = src->pTreeInfo;
	// copy the nodes and the branches
	memcpy(dest->pNodes, src->pNodes, (sizeof(typename FastTree<T1,T2>::FastTreeNode)) * src->pMaxNodeCount);
	memcpy(dest->pBranches, src->pBranches, (sizeof(typename FastTree<T1,T2>::Branch)) * src->pMaxNodeCount);
	return 0;
}

template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting> struct FastTreeBranchComparator
{
	typedef FastTree<FsDataMemberForRand, FsAndFileDataComparerForBranchSorting> tFastTree;
	typedef FastTreeBranch tBranch;
	const tFastTree *fTree;
	FastTreeBranchComparator(const tFastTree *ftree) : fTree(ftree)
	{};
	bool operator() (tBranch left, tBranch right) const
	{
		return fTree->FTGreaterNode(left.sonIdx,right.sonIdx);
	};
};

template<typename FsDataMemberForRand, typename FsAndFileDataComparerForBranchSorting> struct FastTreeBranchComparatorInv
{
	typedef FastTree<FsDataMemberForRand, FsAndFileDataComparerForBranchSorting> tFastTree;
	typedef FastTreeBranch tBranch;
	const tFastTree *fTree;
	FastTreeBranchComparatorInv(const tFastTree *ftree) : fTree(ftree)
	{};
	bool operator() (tBranch left, tBranch right) const
	{
		return fTree->FTLowerNode(left.sonIdx,right.sonIdx);
	};
};

#if __EOSMGM_TREECOMMON__PACK__STRUCTURE__==1
#pragma pack(pop)
#endif

/*----------------------------------------------------------------------------*/
/**
 * @brief FastTree instantiation for replica placement.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<PlacementPriorityRandWeightEvaluator, PlacementPriorityComparator> FastPlacementTree;

/**
 * @brief FastTree instantiation for replica Read-Only access.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<AccessPriorityRandWeightEvaluator, ROAccessPriorityComparator> FastROAccessTree;

/**
 * @brief FastTree instantiation for replica Read-Write access.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<AccessPriorityRandWeightEvaluator, RWAccessPriorityComparator> FastRWAccessTree;

/*----------------------------------------------------------------------------*/
/**
 * @brief FastTree instantiation for draining replica placement.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<DrainingPlacementPriorityRandWeightEvaluator, DrainingPlacementPriorityComparator> FastDrainingPlacementTree;

/**
 * @brief FastTree instantiation for draining replica access.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<DrainingAccessPriorityRandWeightEvaluator, DrainingAccessPriorityComparator> FastDrainingAccessTree;

/*----------------------------------------------------------------------------*/
/**
 * @brief FastTree instantiation for balancing replica placement.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<BalancingPlacementPriorityRandWeightEvaluator, BalancingPlacementPriorityComparator> FastBalancingPlacementTree;

/**
 * @brief FastTree instantiation for balancing replica access.
 *
 */
/*----------------------------------------------------------------------------*/
typedef FastTree<BalancingAccessPriorityRandWeightEvaluator, BalancingAccessPriorityComparator> FastBalancingAccessTree;

template<typename T1, typename T2>
inline std::ostream&
operator <<(std::ostream &os, const FastTree<T1, T2> &tree)
{
	return tree.recursiveDisplay(os);
}

template<typename T1, typename T2>
void __attribute__ ((used)) __attribute__ ((noinline))
debugDisplay(const FastTree<T1, T2> &tree)
{
	tree.recursiveDisplay(std::cout);
}

EOSMGMNAMESPACE_END

#endif /* __EOSMGM_FASTTREE__H__ */
