//------------------------------------------------------------------------------
// @file SchedulingSlowTree.hh
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

#ifndef __EOSMGM_SLOWTREE__H__
#define __EOSMGM_SLOWTREE__H__

#include <map>
#include <ostream>
#include <set>
#include <vector>
#define DEFINE_TREECOMMON_MACRO
#include "mgm/geotree/SchedulingFastTree.hh"
#include "mgm/geotree/SchedulingTreeCommon.hh"

/*----------------------------------------------------------------------------*/
/**
 * @file SchedulingSlowTree.hh
 *
 * @brief Class representing the geotag-based tree structure of a scheduling group
 *
 * There are two representations of this tree structure:
 * - the first one defined in the current file
 *   is flexible and the tree can be shaped easily
 *   on the other hand, it's big and possibly scattered in the memory, so its
 *   access speed might be low
 * - the second one is a set a compact and fast structures (defined in SchedulingFastTree.hh)
 *   these structure ares compact and contiguous in memory which makes them fast
 *   the shape of the underlying tree cannot be changed once they are constructed
 * Typically, a tree is constructed using the first representation (also referred as "slow").
 * Then, a representation of the second kind (also referred as "fast") is created from the
 * previous. It's then used to issue all the file scheduling operations at a high throughput (MHz)
 *
 */

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class SlowTree;

/*----------------------------------------------------------------------------*/
/**
 * @brief Class representing a node of the Class SlowTree
 *
 */
/*----------------------------------------------------------------------------*/
class SlowTreeNode : public SchedTreeBase
{
	friend class SlowTree;
	friend class GeoTreeEngine;
	friend struct TreeEntryMap;
	friend struct FastStructures;

	// tree parents
	SlowTreeNode *pFather;
	typedef std::map<std::string,SlowTreeNode*> tNodeMap;
	int pLeavesCount;
	int pNodeCount;
	// branches are accessed by their geotag. Convenient for insertion;
	tNodeMap pChildren;
	// info
	TreeNodeInfo pNodeInfo;

	// attributes
	TreeNodeStateFloat pNodeState;

protected:
	void destroy()
	{
		for(tNodeMap::iterator it=pChildren.begin(); it!=pChildren.end(); it++)
		delete it->second;
	}

	// update the aggregated data in the nodes and and the ordered set of the branches
	void update()
	{
		if(!pChildren.empty())
		{
			// first update the branches
			pLeavesCount = 0;
			for(tNodeMap::const_iterator it=pChildren.begin();it!=pChildren.end();it++)
			{
				// update this branch
				it->second->update();
				pLeavesCount += it->second->pLeavesCount;
			}
		}
		else
		pLeavesCount = 1;
	}
public:
	SlowTreeNode() : pFather(0) , pLeavesCount(0), pNodeCount(0)
	{}
	~SlowTreeNode()
	{
		destroy();
	}

	template<typename T1,typename T2> inline bool writeFastTreeNodeTemplate (struct FastTree<T1,T2>::FastTreeNode *ftn) const
	{
		pNodeState.writeCompactVersion(&ftn->fsData);
		return true;
	}

	std::ostream& display(std::ostream &os) const;
	std::ostream& recursiveDisplay(std::ostream &os, bool useColors=false, const std::string &prefix="") const;
};

inline std::ostream& operator << (std::ostream &os, const SlowTreeNode &treenode)
{
	return treenode.display(os);
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Class representing a scheduling group.
 *        This class is an helper to construct faster and fixed shape structures.
 *
 */
/*----------------------------------------------------------------------------*/
class SlowTree : public SchedTreeBase
{
	// tree parents
	SlowTreeNode pRootNode;

	// size
	size_t pNodeCount;

	void Init()
	{
		pNodeCount=1; // because of pRootNode
		pRootNode.pNodeInfo.nodeType=TreeNodeInfo::intermediate;
		pRootNode.pFather=NULL;
		pRootNode.pNodeCount = 1;
		pDebugLevel=0;
	}

	SlowTreeNode* insert( const TreeNodeInfo *info, const TreeNodeStateFloat *state, std::string &fullgeotag, const std::string &partialgeotag, SlowTreeNode* startfrom, SlowTreeNode* startedConstructingAt);

public:
	SlowTree(const std::string &groupId)
	{
		Init();
		pRootNode.pNodeInfo.geotag=groupId;
	}
	void setName(const std::string &groupId)
	{
		pRootNode.pNodeInfo.geotag=groupId;
	}
	SlowTree()
	{	Init();}
	~SlowTree()
	{}
	void emitDebugInfo( const size_t debugLevel) const
	{}
	SlowTreeNode* insert( const TreeNodeInfo *info, const TreeNodeStateFloat *state);
	bool remove( const TreeNodeInfo *info);
	SlowTreeNode* moveToNewGeoTag(SlowTreeNode* node, const std::string newGeoTag);
	size_t getNodeCount() const
	{	return pNodeCount;}
	std::ostream& display(std::ostream &os, bool useColors=false) const;

	bool buildFastStrctures(
			FastPlacementTree *fpt, FastROAccessTree *froat, FastRWAccessTree *frwat,
			FastBalancingPlacementTree *fbpt, FastBalancingAccessTree *fbat,
			FastDrainingPlacementTree *fdpt, FastDrainingAccessTree *fdat,
			FastTreeInfo *fastinfo, Fs2TreeIdxMap *fs2idx, GeoTag2NodeIdxMap *geo2node) const;

	FastPlacementTree* allocateAndBuildFastTreeTemplate(FastPlacementTree *fasttree, FastTreeInfo *fastinfo, Fs2TreeIdxMap *fs2idx, GeoTag2NodeIdxMap *geo2node) const;
};

inline std::ostream& operator << (std::ostream &os, const SlowTree &tree)
{
	return tree.display(os);
}

EOSMGMNAMESPACE_END

#endif /* __EOSMGM_SLOWTREE__H__ */
