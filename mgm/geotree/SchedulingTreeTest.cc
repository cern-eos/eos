//------------------------------------------------------------------------------
// @file SchedulingTreeTest.hh
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

#include "mgm/geotree/SchedulingSlowTree.hh"
#include "common/Logging.hh"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <set>
#include <vector>
#include <map>
#include <algorithm>
#include <fstream>
#include <cmath>
#include <functional>

using namespace std;
using namespace eos::mgm;

#define RUN_FUNCTIONAL_TEST 1
#define RUN_BURNIN_TEST 1
size_t CheckLevel = 1;
size_t DebugLevel = 1;
const int bufferSize = 16384;
const size_t groupSize = 100;
const size_t nFsPerBox = 26;

size_t nAvailableFsPlct;
size_t nAvailableFsDrnPlct;
size_t nAvailableFsBlcPlct;
size_t nAvailableFsROAccess;
size_t nAvailableFsRWAccess;
size_t nUnavailFs;
size_t nDisabledFs;

// trim from start
static inline std::string &ltrim(std::string &s)
{
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
	return s;
}

// trim from end
static inline std::string &rtrim(std::string &s)
{
	s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
	return s;
}

// trim from both ends
static inline std::string &trim(std::string &s)
{
	return ltrim(rtrim(s));
}

int PopulateSchedGroupFromFile(const string &fileName, size_t groupSize, size_t nFsPerBox, vector<set<pair<string, string> > > &schedGroups)
{
	// open file
	ifstream ifs;
	ifs.open(fileName.c_str());
	if (!ifs.is_open())
	{
		cerr << "cannot open file " << fileName << endl;
		return 1;
	}

	// read file
	string host, geotag;
	map<string, string> items;
	while (!ifs.eof() && !ifs.fail() && !ifs.bad())
	{
		host.clear();
		geotag.clear();
		getline(ifs, host, ':');
		getline(ifs, geotag);
		if (host.empty())
			break;
		items[trim(host)] = trim(geotag);
	}
	size_t nHosts = items.size();
	cout << "read " << nHosts << " items in file " << fileName << endl;

	// check schedgroups size
	size_t nGroups = (nFsPerBox * nHosts) / groupSize;
	if (!nGroups)
	{
		cerr << " group size is too large for the number of hosts and fs per host" << endl;
		return 1;
	}
	if (groupSize > nHosts)
	{
		cerr << " group size is larger than the number of hosts " << endl;
		return 1;
	}

	// build schedgroups
	size_t grSize = groupSize;
	set<pair<string, string> > *currentGroup = 0;
	for (size_t fs = 0; fs < nFsPerBox; fs++)
	{
		for (map<string, string>::const_iterator it = items.begin(); it != items.end(); it++)
		{
			if (grSize == groupSize)
			{ // create a new group if necessary
				if (schedGroups.size() == nGroups)
					goto end;
				schedGroups.resize(schedGroups.size() + 1);
				currentGroup = &schedGroups[schedGroups.size() - 1];
				grSize = 0;
			}
			ostringstream oss;
			oss << it->second;
			currentGroup->insert(make_pair(it->first, oss.str()));
			grSize++;
		}
	}
	end: return 0;
}

inline size_t treeDepthSimilarity(const std::string & left, const std::string &right)
{
	if (left.empty() || right.empty())
		return 0;
	size_t depth = 0;
	for (size_t k = 0; k < min(left.size(), right.size()) - 1; k++)
	{
		if (left[k] != right[k])
			break;
		if (left[k] == ':' && left[k + 1] == ':')
			depth++;
	}
	return depth;
}

template<typename T1, typename T2, typename T3, typename T4> void functionalTestFastTree(FastTree<T1, T2> *fptree, FastTree<T3, T4> *fatree,
		GeoTag2NodeIdxMap *geomap, SchedTreeBase::FastTreeInfo *treeinfo, size_t nMaxReplicas)
{
	// do verification regarding the placement, the access and the geolocation
	for (size_t loop = 0; loop < 1000; loop++)
	{
		// select a random number of replicas
		size_t nreplica = 1 + rand() % (nMaxReplicas);

		// copy a blank copy of the FastTree
		char buffer[bufferSize];
		assert(fptree->copyToBuffer(buffer, bufferSize) == 0);
		FastPlacementTree *ftree = (FastPlacementTree*) buffer;
		char buffer2[bufferSize];
		assert(fatree->copyToBuffer(buffer2, bufferSize) == 0);
		FastROAccessTree *ftree2 = (FastROAccessTree*) buffer2;

		// place the replicas
		set<SchedTreeBase::tFastTreeIdx> repIdxs;
		SchedTreeBase::tFastTreeIdx repIdx;
		for (size_t k = 0; k < nreplica; k++)
		{
			assert(ftree->findFreeSlot(repIdx));
			repIdxs.insert(repIdx);
		}
		// repopulate the access tree with the placed replicas
		for (set<SchedTreeBase::tFastTreeIdx>::const_iterator it = repIdxs.begin(); it != repIdxs.end(); it++)
		{
			ftree2->incrementFreeSlot(*it);
		}

		// ========= PLACEMENT/ACCESS ROUNDTRIP TEST =========
		// get all the replicas
		SchedTreeBase::tFastTreeIdx allreplicas[255], nr;
		nr = 255;
		nr = ftree2->findFreeSlotsAll(allreplicas, nr);
		assert(nr);
		// check that all the replicas are there
		set<SchedTreeBase::tFastTreeIdx> allreplicasset, symdif;
		allreplicasset.insert(allreplicas, allreplicas + nr);
		set<SchedTreeBase::tFastTreeIdx>::iterator symdifbeg = symdif.begin();
		set_symmetric_difference(repIdxs.begin(), repIdxs.end(), allreplicasset.begin(), allreplicasset.end(), inserter(symdif, symdifbeg));
		assert(symdif.empty());
		for (size_t k = 0; k < nreplica; k++)
		{
			// check that the closest node is the node itself
			SchedTreeBase::tFastTreeIdx closest = geomap->getClosestFastTreeNode((*treeinfo)[k].fullGeotag.c_str());
			assert(closest == k);
			// request an access
			assert(ftree2->findFreeSlot(repIdx, closest, true, true));
			// check that the replica node is among the placed replica
			assert(repIdxs.count(repIdx));
			// check that it's the nearest one (i.e) the deepest tree similarity
			size_t simRep = treeDepthSimilarity((*treeinfo)[k].fullGeotag, (*treeinfo)[closest].fullGeotag);
			for (set<SchedTreeBase::tFastTreeIdx>::const_iterator it = repIdxs.begin(); it != repIdxs.end(); it++)
				assert(treeDepthSimilarity((*treeinfo)[k].fullGeotag, (*treeinfo)[*it].fullGeotag) <= simRep);
		}
	}
}

template
void __attribute__ ((used)) __attribute__ ((noinline))
eos::mgm::debugDisplay(const FastPlacementTree &tree);

template
void __attribute__ ((used)) __attribute__ ((noinline))
eos::mgm::debugDisplay(const FastROAccessTree &tree);

void debugDisplayPlct(const FastPlacementTree &tree)
{
	debugDisplay(tree);
}
void debugDisplayAccs(const FastROAccessTree &tree)
{
	debugDisplay(tree);
}

int main()
{
	eos::common::Logging::Init();

	eos::common::Logging::SetUnit("SchedulingTreeTest");
	eos::common::Logging::SetLogPriority(LOG_INFO);
	srand(0);

	string geoTagFileNameStr = __FILE__;
	geoTagFileNameStr += ".testfile";
	const char *geoTagFileName = geoTagFileNameStr.c_str();

	vector<set<pair<string, string> > > schedGroups;
	SchedTreeBase::gSettings.checkLevel = CheckLevel;
	SchedTreeBase::gSettings.debugLevel = DebugLevel;

	PopulateSchedGroupFromFile(geoTagFileName, groupSize, nFsPerBox, schedGroups);
	vector<SlowTree> trees(schedGroups.size());
	vector<FastPlacementTree> fptrees(schedGroups.size());
	vector<FastBalancingPlacementTree> fbptrees(schedGroups.size());
	vector<FastDrainingPlacementTree> fdptrees(schedGroups.size());
	vector<FastROAccessTree> froatrees(schedGroups.size());
	vector<FastRWAccessTree> frwatrees(schedGroups.size());
	vector<FastBalancingAccessTree> fbatrees(schedGroups.size());
	vector<FastDrainingAccessTree> fdatrees(schedGroups.size());
	vector<SchedTreeBase::FastTreeInfo> ftinfos(schedGroups.size());
	vector<Fs2TreeIdxMap> ftmaps(schedGroups.size());
	vector<GeoTag2NodeIdxMap> geomaps(schedGroups.size());
	vector<set<eos::common::FileSystem::fsid_t> > drainerfs(schedGroups.size()), drainingfs(schedGroups.size());
	vector<set<eos::common::FileSystem::fsid_t> > balancerfs(schedGroups.size()), balancingfs(schedGroups.size());
	vector<map<eos::common::FileSystem::fsid_t, SchedTreeBase::tFastTreeIdx> > maxDningToDnerSimil(schedGroups.size()), maxBcingToBcerSimil(
			schedGroups.size());

	size_t idx = 0;
	for (vector<set<pair<string, string> > >::const_iterator sgit = schedGroups.begin(); sgit != schedGroups.end(); sgit++)
	{
		ostringstream oss;
		oss << idx;
		trees[idx].setName(oss.str());
		// insert the branches in the tree
		nAvailableFsPlct = 0;
		nAvailableFsDrnPlct = 0;
		nAvailableFsBlcPlct = 0;
		nAvailableFsROAccess = 0;
		nAvailableFsRWAccess = 0;
		nUnavailFs = 0;
		nDisabledFs = 0;
		for (set<pair<string, string> >::const_iterator it = sgit->begin(); it != sgit->end(); it++)
		{
			SchedTreeBase::TreeNodeInfo info;
			info.geotag = it->second;
			info.host = it->first;
			info.fsId = (eos::common::FileSystem::fsid_t) rand();
			//std::cout<<info.mGeotag.c_str()<<"\t"<<info.mFsId<<std::endl;
			SchedTreeBase::TreeNodeStateFloat state;
			state.dlScore = 1.0;
			state.ulScore = 1.0;
			state.mStatus = SchedTreeBase::Available | SchedTreeBase::Writable | SchedTreeBase::Readable;
			state.fillRatio = 0.5;
			state.totalSpace = 2e12;

			int r = rand();
			if (r < RAND_MAX / 64) // make 1/64 th unavailable
			{
				state.mStatus = (SchedTreeBase::tStatus) (state.mStatus & ~SchedTreeBase::Available);
				nUnavailFs++;
			}
			else if (! (r%16) ) // make 1/16 th disabled
			{
                                state.mStatus = (SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Disabled);
                                nDisabledFs++;
			}
			else
			{
				nAvailableFsPlct++;
				nAvailableFsROAccess++;
				nAvailableFsRWAccess++;

				if (r < RAND_MAX / 32)
				{
					state.mStatus = (SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Draining);
					state.mStatus = (SchedTreeBase::tStatus) (state.mStatus & ~SchedTreeBase::Writable);
					state.mStatus = (SchedTreeBase::tStatus) (state.mStatus & ~SchedTreeBase::Readable);
					nAvailableFsPlct--;
					nAvailableFsROAccess--;
					nAvailableFsRWAccess--;
					drainingfs[idx].insert(info.fsId);
					maxDningToDnerSimil[idx][info.fsId] = 0;
				}
				else
				{
					if (r > RAND_MAX / 4)
					{
						state.mStatus = (SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Drainer);
						drainerfs[idx].insert(info.fsId);
						nAvailableFsDrnPlct++;
					}
					r = rand();
					if (r < RAND_MAX / 8)
					{
						if (state.mStatus & SchedTreeBase::Drainer)
							nAvailableFsDrnPlct--;
						state.mStatus = (SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Balancing);
						state.mStatus = (SchedTreeBase::tStatus) (state.mStatus & ~SchedTreeBase::Writable);
						nAvailableFsPlct--;
						nAvailableFsRWAccess--;
						balancingfs[idx].insert(info.fsId);
						maxBcingToBcerSimil[idx][info.fsId] = 0;
					}
					if (r > 7 * (RAND_MAX / 8))
					{
						state.mStatus = (SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Balancer);
						balancerfs[idx].insert(info.fsId);
						nAvailableFsBlcPlct++;
					}
				}
			}
			//std::cout<< "insert =>" << it->first <<"\t"<< it->second<<"::"<<info.mFsId <<std::endl;
			assert(trees[idx].insert(&info,&state)!=NULL);
			assert(trees[idx].remove(&info)); // erase and rewrite to just to give a try to this feature
			trees[idx].insert(&info, &state);
		}

		cout << "group " << std::setw(3) << idx << "\tnAvailableFsROAccess = " << std::setw(3) << nAvailableFsROAccess
				<< "\tnAvailableFsRWAccess = " << std::setw(3) << nAvailableFsRWAccess << "\tnAvailableFsPlct = " << std::setw(3) << nAvailableFsPlct
				<< "\tnAvailableFsBlcPlct = " << std::setw(3) << nAvailableFsBlcPlct << "\tnAvailableFsDrnPlct = " << std::setw(3)
				<< nAvailableFsDrnPlct << "\tnUnavailFs = " << std::setw(3) << nUnavailFs<< "\tnDisabledFs = " << std::setw(3) << nDisabledFs <<std::endl;

		// allocate the memory for the content of the FastTree
		fptrees[idx].selfAllocate(trees[idx].getNodeCount());
		fbptrees[idx].selfAllocate(trees[idx].getNodeCount());
		fdptrees[idx].selfAllocate(trees[idx].getNodeCount());
		froatrees[idx].selfAllocate(trees[idx].getNodeCount());
		frwatrees[idx].selfAllocate(trees[idx].getNodeCount());
		fbatrees[idx].selfAllocate(trees[idx].getNodeCount());
		fdatrees[idx].selfAllocate(trees[idx].getNodeCount());

		// build the FastTree
		//std::cout<<trees[0]<<std::endl;
		assert(
				trees[idx].buildFastStrctures(&fptrees[idx], &froatrees[idx], &frwatrees[idx], &fbptrees[idx], &fbatrees[idx], &fdptrees[idx],
						&fdatrees[idx], &ftinfos[idx], &ftmaps[idx], &geomaps[idx]));

		// check the consistency of the FastTree
		fptrees[idx].checkConsistency(0);
		// check the consistency of the FastTree
		froatrees[idx].checkConsistency(0);

		// fill the maxSimilarty maps
		for (auto dningit = maxDningToDnerSimil[idx].begin(); dningit != maxDningToDnerSimil[idx].end(); dningit++)
		{
			size_t maxSim = 0;
			const SchedTreeBase::tFastTreeIdx *dningIdx;
			assert(ftmaps[idx].get(dningit->first, dningIdx));
			for (auto dnerit = drainerfs[idx].begin(); dnerit != drainerfs[idx].end(); dnerit++)
			{
				const SchedTreeBase::tFastTreeIdx *dnerIdx;
				assert(ftmaps[idx].get(*dnerit, dnerIdx));
				size_t sim = treeDepthSimilarity(ftinfos[idx][*dnerIdx].fullGeotag, ftinfos[idx][*dningIdx].fullGeotag);
				maxSim = max(maxSim, sim);
			}
			maxDningToDnerSimil[idx][dningit->first] = maxSim;
		}
		for (auto bcingit = maxBcingToBcerSimil[idx].begin(); bcingit != maxBcingToBcerSimil[idx].end(); bcingit++)
		{
			size_t maxSim = 0;
			const SchedTreeBase::tFastTreeIdx *bcingIdx;
			assert(ftmaps[idx].get(bcingit->first, bcingIdx));
			for (auto bcerit = balancerfs[idx].begin(); bcerit != balancerfs[idx].end(); bcerit++)
			{
				const SchedTreeBase::tFastTreeIdx *bcerIdx;
				assert(ftmaps[idx].get(*bcerit, bcerIdx));
				size_t sim = treeDepthSimilarity(ftinfos[idx][*bcerIdx].fullGeotag, ftinfos[idx][*bcingIdx].fullGeotag);
				maxSim = max(maxSim, sim);
			}
			maxBcingToBcerSimil[idx][bcingit->first] = maxSim;
		}

#if RUN_FUNCTIONAL_TEST==1

		// functional testing
		functionalTestFastTree(&fptrees[idx], &froatrees[idx], &geomaps[idx], &ftinfos[idx], nAvailableFsPlct);
		functionalTestFastTree(&fptrees[idx], &frwatrees[idx], &geomaps[idx], &ftinfos[idx], nAvailableFsPlct);
		functionalTestFastTree(&fbptrees[idx], &fbatrees[idx], &geomaps[idx], &ftinfos[idx], nAvailableFsBlcPlct);
		functionalTestFastTree(&fdptrees[idx], &fdatrees[idx], &geomaps[idx], &ftinfos[idx], nAvailableFsDrnPlct);

		// illustrate some display facility
		if (idx == schedGroups.size() - 1)
		{
			// display the SlowTree
		        cout << "====== Illustrating the display of a SlowTree ======" << std::endl;
                        cout << trees[idx] << endl;
                        cout << "====================================================" << std::endl << std::endl;
                        cout << "====== Illustrating the color display of a SlowTree ======" << std::endl;
                        trees[idx].display(cout,true);
                        cout << endl;
			cout << "====================================================" << std::endl << std::endl;
			// display the FastTree
                        cout << "====== Illustrating the display of a Placement FastTree ======" << std::endl;
                        cout << fptrees[idx] << endl;
                        cout << "==============================================================" << std::endl << std::endl;
                        cout << "====== Illustrating the color display of a Placement FastTree ======" << std::endl;
                        fptrees[idx].recursiveDisplay(cout,true);
                        cout<< endl;
                        cout << "==============================================================" << std::endl << std::endl;
			// display the FastTree
                        cout << "====== Illustrating the display of an Access FastTree ======" << std::endl;
                        cout << froatrees[idx] << endl;
                        cout << "============================================================" << std::endl << std::endl;
                        cout << "====== Illustrating the color display of an Access FastTree ======" << std::endl;
                        froatrees[idx].recursiveDisplay(cout,true);
                        cout<< endl;
                        cout << "============================================================" << std::endl << std::endl;
			// display the info array related to the fast tree
                        cout << "====== Illustrating the display of a Tree Nodes Information Table ======" << std::endl;
                        cout << ftinfos[idx];
                        cout << "=========================================================================" << std::endl << std::endl;
			// display the mapping from fs to FastTree nodes
			cout << "====== Illustrating the display of a Fs2TreeIdxMap ======" << std::endl;
			cout << ftmaps[idx];
			cout << "=========================================================" << std::endl << std::endl;
		}
#endif // FUNCTIONAL_TEST
		idx++;
	}

#if RUN_BURNIN_TEST==1
	debugDisplay(fptrees[0]);
	const size_t nbIter = 10000;
	//const size_t nbIter = 100;
	//const size_t nbIter = 1;
	vector<SchedTreeBase::tFastTreeIdx> replicaIdxs(3 * schedGroups.size());

	clock_t begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(fptrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		FastPlacementTree *ftree = (FastPlacementTree*) buffer;
		SchedTreeBase::tFastTreeIdx repId;
		for (int k = 0; k < 3; k++)
		{
			//ftree->placeNewReplica(0,false,repId);
			ftree->findFreeSlot(repId);
			replicaIdxs[3 * (i % schedGroups.size()) + k] = repId;
		}
	}
	clock_t elapsed = clock() - begin;
	cout << "REPLICA PLACEMENT SPEED TEST" << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << 3 * schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " placements/sec " << endl;
	cout << "----------------------------" << endl << endl;

	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(fptrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		char buffer2[bufferSize];
		assert(froatrees[i % schedGroups.size()].copyToBuffer(buffer2, bufferSize) == 0);
	}
	elapsed = clock() - begin;
	cout << "FAST TREE COPY ONLY SPEED TEST" << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << 6 * schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " copies/sec " << endl;
	cout << "----------------------" << endl << endl;

	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(fptrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		FastPlacementTree *ftree = (FastPlacementTree*) buffer;
		char buffer2[bufferSize];
		assert(froatrees[i % schedGroups.size()].copyToBuffer(buffer2, bufferSize) == 0);
		FastROAccessTree *ftree2 = (FastROAccessTree*) buffer2;
		// update the tree
		for (int k = 0; k < 3; k++)
		{
			ftree->decrementFreeSlot(replicaIdxs[3 * (i % schedGroups.size()) + k]);
			ftree2->incrementFreeSlot(replicaIdxs[3 * (i % schedGroups.size()) + k]);
		}
	}
	elapsed = clock() - begin;
	cout << "TREE REPOPULATING SPEED TEST" << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << 6 * schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " repop/sec " << endl;
	cout << "----------------------" << endl << endl;

	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(froatrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		FastROAccessTree *ftree = (FastROAccessTree*) buffer;
		// pick a random geolocation which makes sense in the tree
		const string &clientGeoString = ftinfos[i % schedGroups.size()][rand() % ftinfos[i % schedGroups.size()].size()].fullGeotag;
		SchedTreeBase::tFastTreeIdx closestNode = geomaps[i % schedGroups.size()].getClosestFastTreeNode(clientGeoString.c_str());
		// update the tree
		for (int k = 0; k < 3; k++)
		{
			ftree->incrementFreeSlot(replicaIdxs[3 * (i % schedGroups.size()) + k]);
		}
		// access the replicas
		SchedTreeBase::tFastTreeIdx repId;
		//ftree->findFreeSlot(closestNode,true,repId,false);
		ftree->findFreeSlot(repId, closestNode, true, true);
	}
	elapsed = clock() - begin;
	cout << "FILE ACCESS 1 REP SPEED TEST" << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << 3 * schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " access/sec " << endl;
	cout << "----------------------" << endl << endl;

	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(froatrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		FastROAccessTree *ftree = (FastROAccessTree*) buffer;
		// update the tree
		for (int k = 0; k < 3; k++)
		{
			ftree->incrementFreeSlot(replicaIdxs[3 * (i % schedGroups.size()) + k]);
		}    // access the replicas
		SchedTreeBase::tFastTreeIdx repIdxs[3];
		ftree->findFreeSlotsAll(repIdxs, 3);
	}
	elapsed = clock() - begin;
	cout << "FILE ACCESS ALL REP SPEED TEST" << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << 3 * schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " access/sec " << endl;
	cout << "----------------------" << endl << endl;

	vector<vector<SchedTreeBase::tFastTreeIdx> > drainers(schedGroups.size()), balancers(schedGroups.size());
	SchedTreeBase::tFastTreeIdx fsize;
	for (size_t idx = 0; idx < schedGroups.size(); idx++)
	{
		drainers[idx].resize(128);
		fsize = fptrees[idx].findFreeSlotsAll(&drainers[idx][0], drainers[idx].size(), 0, false, SchedTreeBase::Drainer);
		assert(fsize);
		drainers[idx].resize(fsize);
		balancers[idx].resize(128);
		fsize = fptrees[idx].findFreeSlotsAll(&balancers[idx][0], balancers[idx].size(), 0, false, SchedTreeBase::Balancer);
		assert(fsize);
		drainers[idx].resize(fsize);
	}

	// first get the idx range of the fs's
	std::vector<SchedTreeBase::tFastTreeIdx> fsIdxBegV(schedGroups.size()),fsIdxEndV(schedGroups.size());
        for (size_t i = 0; i < schedGroups.size (); i++)
        {
          fsIdxBegV[i] = 0;
          while (ftinfos[i][fsIdxBegV[i]].nodeType == SchedTreeBase::TreeNodeInfo::intermediate)
            fsIdxBegV[i]++;
          fsIdxEndV[i] = ftinfos[i].size ();
        }
	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(fptrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		FastPlacementTree *ftree = (FastPlacementTree*) buffer;
		// select a random file system
		SchedTreeBase::tFastTreeIdx rfs = fsIdxBegV[i % schedGroups.size()] + rand() % (fsIdxEndV[i % schedGroups.size()] - fsIdxBegV[i % schedGroups.size()]);
		ftree->updateBranch(rfs);
	}
	elapsed = clock() - begin;
	cout << "UPDATE FAST TREE TEST (ONE BRANCH) " << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " updates/sec " << endl;
	cout << "----------------------" << endl << endl;

	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		char buffer[bufferSize];
		assert(fptrees[i % schedGroups.size()].copyToBuffer(buffer, bufferSize) == 0);
		FastPlacementTree *ftree = (FastPlacementTree*) buffer;
		ftree->updateTree();
	}
	elapsed = clock() - begin;
	cout << "UPDATE FAST TREE TEST (FULL TREE) " << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " updates/sec " << endl;
	cout << "----------------------" << endl << endl;

	begin = clock();
	for (size_t i = 0; i < schedGroups.size() * nbIter; i++)
	{
		int j = i % schedGroups.size();
		assert(
				trees[j].buildFastStrctures(&fptrees[j], &froatrees[j], &frwatrees[j], &fbptrees[j], &fbatrees[j], &fdptrees[j], &fdatrees[j],
						&ftinfos[j], &ftmaps[j], &geomaps[j]));
	}
	elapsed = clock() - begin;
	cout << "FAST STRUCTURES BUILDING TEST" << endl;
	cout << "elapsed time : " << float(elapsed) / CLOCKS_PER_SEC << " sec." << endl;
	cout << "speed        : " << schedGroups.size() * nbIter / (float(elapsed) / CLOCKS_PER_SEC) << " builds/sec " << endl;
	cout << "----------------------" << endl << endl;

#endif // BURNIN_TEST
	return 0;
}
