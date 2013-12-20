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
#include <algorithm>
#include <functional>
using namespace std;
using namespace eos::mgm;

#define RUN_FUNCTIONAL_TEST 1
#define RUN_BURNIN_TEST 1
size_t CheckLevel = 1;
size_t DebugLevel = 0;
const int bufferSize = 4096;

// trim from start
static inline std::string &ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
  return s;
}

// trim from end
static inline std::string &rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
  return s;
}

// trim from both ends
static inline std::string &trim(std::string &s) {
  return ltrim(rtrim(s));
}

int PopulateSchedGroupFromFile(const string &fileName, size_t groupSize, size_t nFsPerBox, vector<set<pair<string,string> > > &schedGroups)
{
  // open file
  ifstream ifs;
  ifs.open(fileName.c_str());
  if(!ifs.is_open()) {
    cerr << "cannot open file " << fileName <<endl;
    return 1;
  }

  // read file
  string host,geotag;
  map<string,string> items;
  while(!ifs.eof() && !ifs.fail() && !ifs.bad()){
    host.clear(); geotag.clear();
    getline(ifs,host,':');
    getline(ifs,geotag);
    if(host.empty()) break;
    items[trim(host)]=trim(geotag);
  }
  size_t nHosts = items.size();
  cout << "read " << nHosts << " items in file "<< fileName << endl;

  // check schedgroups size
  size_t nGroups = (nFsPerBox*nHosts)/groupSize;
  if(! nGroups ) {
    cerr << " group size is too large for the number of hosts and fs per host" << endl;
    return 1;
  }
  if( groupSize > nHosts ) {
    cerr << " group size is larger than the number of hosts " << endl;
    return 1;
  }

  // build schedgroups
  size_t grSize=groupSize;
  set<pair<string,string> > *currentGroup=0;
  for(size_t fs=0; fs<nFsPerBox; fs++ ) {
    for(map<string,string>::const_iterator it=items.begin();it!=items.end();it++){
      if(grSize==groupSize) { // create a new group if necessary
        if(schedGroups.size()==nGroups)
          goto end;
        schedGroups.resize(schedGroups.size()+1);
        currentGroup = & schedGroups[schedGroups.size()-1];
        grSize=0;
      }
      ostringstream oss;
      oss<<it->second;
      //oss<<"::"<<it->first;
      //oss<<"::"<<setw((size_t)round(log10(nFsPerBox)))<<fs;
      currentGroup->insert(make_pair(it->first,oss.str()));
      grSize++;
    }
  }
  end:
  return 0;
}

inline size_t treeDepthSimilarity( const std::string & left, const std::string &right) {
  if(left.empty() || right.empty()) return 0;
  size_t depth = 0;
  for(size_t k = 0; k<min(left.size(),right.size())-1;k++) {
    if(left[k]!=right[k]) break;
    if(left[k]==':' && left[k+1]==':') depth++;
  }
  return depth;
}


int
main()
{
  string geoTagFileNameStr = __FILE__;
  geoTagFileNameStr += ".testfile";
  const char *geoTagFileName = geoTagFileNameStr.c_str();

  const size_t groupSize = 100;
  const size_t nFsPerBox = 26;
  vector<set<pair<string,string> > > schedGroups;
  SchedTreeBase::gSettings.mCheckLevel = CheckLevel;
  SchedTreeBase::gSettings.mDebugLevel = DebugLevel;

  PopulateSchedGroupFromFile(geoTagFileName,groupSize,nFsPerBox,schedGroups);
  vector<SlowTree> trees(schedGroups.size());
  vector<FastPlacementTree> ftrees(schedGroups.size());
  vector<SchedTreeBase::FastTreeInfo> ftinfos(schedGroups.size());
  vector<Fs2TreeIdxMap> ftmaps(schedGroups.size());
  vector<GeoTag2NodeIdxMap> geomaps(schedGroups.size());
  vector<FastAccessTree> ftrees2(schedGroups.size());
  vector<BalancingInverseFastTree> biftrees(schedGroups.size());
  vector<DrainingInverseFastTree> diftrees(schedGroups.size());
  vector<set<unsigned long> > drainerfs(schedGroups.size()),drainingfs(schedGroups.size());
  vector<set<unsigned long> > balancerfs(schedGroups.size()),balancingfs(schedGroups.size());
  vector<map<unsigned long,SchedTreeBase::tFastTreeIdx> > maxDningToDnerSimil(schedGroups.size()),maxBcingToBcerSimil(schedGroups.size());

  size_t idx=0;
  for(vector<set<pair<string,string> > >::const_iterator sgit=schedGroups.begin();sgit!=schedGroups.end();sgit++)
  {
    ostringstream oss;
    oss<<idx;
    trees[idx].setName(oss.str());
    // insert the branches in the tree
    for(set<pair<string,string> >::const_iterator it=sgit->begin();it!=sgit->end();it++)
    {
      SchedTreeBase::TreeNodeInfo info;
      info.mGeotag=it->second;
      info.mHost=it->first;
      info.mFsId=rand();
      SchedTreeBase::TreeNodeStateFloat state;
      state.mDlScore=1.0;
      state.mUlScore=1.0;
      state.mStatus=SchedTreeBase::None;
      state.mFillRatio=0.5;
      state.mTotalSpace=2e12;
      state.mStatus = SchedTreeBase::None;
      int r = rand();
      if(r < RAND_MAX/16 )  {
        state.mStatus =(SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Draining);
        drainingfs[idx].insert(info.mFsId);
        maxDningToDnerSimil[idx][info.mFsId]=0;
      }
      else
      {
        if(r>RAND_MAX/4) {
          state.mStatus =(SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Drainer);
          drainerfs[idx].insert(info.mFsId);
        }
        r = rand();
        if(r < RAND_MAX/8 )  {
          state.mStatus =(SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Balancing);
          balancingfs[idx].insert(info.mFsId);
          maxBcingToBcerSimil[idx][info.mFsId]=0;
        }
        if(r > 7*(RAND_MAX/8) ) {
          state.mStatus =(SchedTreeBase::tStatus) (state.mStatus | SchedTreeBase::Balancer);
          balancerfs[idx].insert(info.mFsId);
        }
      }
      trees[idx].insert(&info,&state);
    }
    // allocate the memory for the content of the FastTree
    ftrees[idx].selfAllocate(trees[idx].getNodeCount());
    ftrees2[idx].selfAllocate(trees[idx].getNodeCount());
    // build the FastTree
    assert( trees[idx].buildFastStrctures(&ftrees[idx],&ftrees2[idx],&ftinfos[idx],&ftmaps[idx],&geomaps[idx]) );
    ftrees[idx].GenerateBalancingInvertedLookup(&biftrees[idx]);
    ftrees[idx].GenerateDrainingInvertedLookup(&diftrees[idx]);
    // check the consistency of the FastTree
    ftrees[idx].checkConsistency(0);
    // check the consistency of the FastTree
    ftrees2[idx].checkConsistency(0);

    // fill the maxSimilarty maps
    for(auto dningit = maxDningToDnerSimil[idx].begin(); dningit != maxDningToDnerSimil[idx].end(); dningit++){
      size_t maxSim = 0;
      const SchedTreeBase::tFastTreeIdx *dningIdx;
      assert(ftmaps[idx].get(dningit->first,dningIdx) );
      for(auto dnerit=drainerfs[idx].begin();dnerit!=drainerfs[idx].end();dnerit++){
        const SchedTreeBase::tFastTreeIdx *dnerIdx;
        assert(ftmaps[idx].get(*dnerit,dnerIdx));
        size_t sim = treeDepthSimilarity(ftinfos[idx][*dnerIdx].mFullGeotag,ftinfos[idx][*dningIdx].mFullGeotag);
        maxSim = max(maxSim,sim);
      }
      maxDningToDnerSimil[idx][dningit->first] = maxSim;
    }
    for(auto bcingit = maxBcingToBcerSimil[idx].begin(); bcingit != maxBcingToBcerSimil[idx].end(); bcingit++){
      size_t maxSim = 0;
      const SchedTreeBase::tFastTreeIdx *bcingIdx;
      assert(ftmaps[idx].get(bcingit->first,bcingIdx) );
      for(auto bcerit=balancerfs[idx].begin();bcerit!=balancerfs[idx].end();bcerit++){
        const SchedTreeBase::tFastTreeIdx *bcerIdx;
        assert(ftmaps[idx].get(*bcerit,bcerIdx));
        size_t sim = treeDepthSimilarity(ftinfos[idx][*bcerIdx].mFullGeotag,ftinfos[idx][*bcingIdx].mFullGeotag);
        maxSim = max(maxSim,sim);
      }
      maxBcingToBcerSimil[idx][bcingit->first] = maxSim;
    }

#if RUN_FUNCTIONAL_TEST==1
    // do verification regarding the placement, the access and the geolocation
    for(size_t loop=0;loop<1000;loop++)
    {
      // select a random number of replicas
      size_t nreplica = 1 + rand()%(groupSize);

      // copy a blank copy of the FastTree
      char buffer[bufferSize];
      assert( ftrees[idx].copyToBuffer(buffer,bufferSize) == 0 );
      FastPlacementTree *ftree = (FastPlacementTree*)buffer;
      char buffer2[bufferSize];
      assert( ftrees2[idx].copyToBuffer(buffer2,bufferSize) == 0 );
      FastAccessTree *ftree2 = (FastAccessTree*)buffer2;

      // place the replicas
      set<SchedTreeBase::tFastTreeIdx> repIdxs;
      SchedTreeBase::tFastTreeIdx repIdx;
      for(size_t k=0;k<nreplica;k++) {
        ftree->findFreeSlot(repIdx);
        repIdxs.insert(repIdx);
      }
      // repopulate the access tree with the placed replicas
      for(set<SchedTreeBase::tFastTreeIdx>::const_iterator it=repIdxs.begin();it!=repIdxs.end();it++) {
        ftree2->incrementFreeSlot(*it);
      }

      // ========= PLACEMENT/ACCESS ROUNDTRIP TEST =========
      // get all the replicas
      SchedTreeBase::tFastTreeIdx allreplicas[255],nr;
      nr = 255;
      nr = ftree2->findFreeSlotsAll(allreplicas,nr);
      assert( nr );
      // check that all the replicas are there
      set<SchedTreeBase::tFastTreeIdx> allreplicasset,symdif;
      allreplicasset.insert(allreplicas,allreplicas+nr);
      set<SchedTreeBase::tFastTreeIdx>::iterator symdifbeg = symdif.begin();
      set_symmetric_difference(repIdxs.begin(),repIdxs.end(),allreplicasset.begin(),allreplicasset.end(),inserter(symdif,symdifbeg));
      assert(symdif.empty());
      for(size_t k=0;k<nreplica;k++)
      {
        // check that the closest node is the node itself
        SchedTreeBase::tFastTreeIdx closest = geomaps[idx].getClosestFastTreeNode(ftinfos[idx][k].mFullGeotag.c_str());
        assert( closest == k );
        // request an access
        assert( ftree2->findFreeSlot(repIdx, closest,true, true ) );
        // check that the replica node is among the placed replica
        assert( repIdxs.count(repIdx) );
        // check that it's the nearest one (i.e) the deepest tree similarity
        size_t simRep = treeDepthSimilarity(ftinfos[idx][k].mFullGeotag,ftinfos[idx][closest].mFullGeotag);
        for(set<SchedTreeBase::tFastTreeIdx>::const_iterator it = repIdxs.begin(); it!=repIdxs.end(); it++)
          assert( treeDepthSimilarity(ftinfos[idx][k].mFullGeotag,ftinfos[idx][*it].mFullGeotag)<=simRep );
      }
    }

    // ========= DRAINERS/DRAINING TEST =========
    //
    {
      SchedTreeBase::tFastTreeIdx drainers[255],drainings[255],nders,ndings;
      vector<unsigned long> derfs,dingfs;
      set<unsigned long> symdif;
      // check that we find back all the drainers
      nders = 255;
      nders = ftrees[idx].findFreeSlotsAll(drainers,nders,0,false,SchedTreeBase::Drainer);
      derfs.resize(nders);
      for(int k=0;k<nders;k++) derfs[k] = ftinfos[idx][drainers[k]].mFsId;
      sort(derfs.begin(),derfs.end());
      set_symmetric_difference(drainerfs[idx].begin(),drainerfs[idx].end(),derfs.begin(),derfs.end(),inserter(symdif,symdif.begin()));
      assert(symdif.empty());
      // check that we find back all the drainings
      ndings = 255;
      ndings = ftrees[idx].findFreeSlotsAll(drainings,ndings,0,false,SchedTreeBase::Draining);
      dingfs.resize(ndings);
      for(int k=0;k<ndings;k++) dingfs[k] = ftinfos[idx][drainings[k]].mFsId;
      sort(dingfs.begin(),dingfs.end());
      set_symmetric_difference(drainingfs[idx].begin(),drainingfs[idx].end(),dingfs.begin(),dingfs.end(),inserter(symdif,symdif.begin()));
      assert(symdif.empty());
      // detailed check
      set<SchedTreeBase::tFastTreeIdx> allDrainingIdxs;
      for(size_t k=0;k<nders;k++)
      {
        // get the drainer
        SchedTreeBase::tFastTreeIdx drainerIdx = drainers[k];
        SchedTreeBase::tFastTreeIdx drainingWNodesCount;
        const DrainingInverseFastTree::tWeightedNode *drainingWNodes;
        // ask for all the drainings
        set<SchedTreeBase::tFastTreeIdx> drainingIdxs;
        assert(diftrees[idx].getFullInverseLookup(drainerIdx,drainingWNodes,&drainingWNodesCount));
        for(int j=0;j<drainingWNodesCount;j++) {
          drainingIdxs.insert(drainingWNodes[j].mIdx);
          allDrainingIdxs.insert(drainingWNodes[j].mIdx);
        }
        if(!drainingIdxs.size()) {
          // every draining must have a closer drainer than the current one
          for(int j=0;j<ndings;j++){
            assert(maxDningToDnerSimil[idx][ftinfos[idx][drainings[j]].mFsId] >
            treeDepthSimilarity(ftinfos[idx][drainerIdx].mFullGeotag,ftinfos[idx][drainings[j]].mFullGeotag));
          }
        }
        else {
          // the drainer is the closest one for all the found drainings
          for(auto dningit=drainingIdxs.begin(); dningit!=drainingIdxs.end();dningit++){
            assert(maxDningToDnerSimil[idx][ftinfos[idx][*dningit].mFsId] ==
                treeDepthSimilarity(ftinfos[idx][drainerIdx].mFullGeotag,ftinfos[idx][*dningit].mFullGeotag));
          }
        }
      }
      // finally, check that all the draining has at least a drainer
      sort(drainings,drainings+ndings);
      set_symmetric_difference(drainings,drainings+ndings,allDrainingIdxs.begin(),allDrainingIdxs.end(),inserter(symdif,symdif.begin()));
      assert(symdif.empty());
    }

    // ========= BALANCERS/BALANCING TEST =========
    //
    {
      SchedTreeBase::tFastTreeIdx balancers[255],balancings[255],nbers,nbings;
      vector<unsigned long> berfs,bingfs;
      set<unsigned long> symdif;
      // check that we find back all the balancers
      nbers = 255;
      nbers = ftrees[idx].findFreeSlotsAll(balancers,nbers,0,false,SchedTreeBase::Balancer);
      berfs.resize(nbers);
      for(int k=0;k<nbers;k++) berfs[k] = ftinfos[idx][balancers[k]].mFsId;
      sort(berfs.begin(),berfs.end());
      set_symmetric_difference(balancerfs[idx].begin(),balancerfs[idx].end(),berfs.begin(),berfs.end(),inserter(symdif,symdif.begin()));
      assert(symdif.empty());
      // check that we find back all the balancings
      nbings = 255;
      nbings = ftrees[idx].findFreeSlotsAll(balancings,nbings,0,false,SchedTreeBase::Balancing);
      bingfs.resize(nbings);
      for(int k=0;k<nbings;k++) bingfs[k] = ftinfos[idx][balancings[k]].mFsId;
      sort(bingfs.begin(),bingfs.end());
      set_symmetric_difference(balancingfs[idx].begin(),balancingfs[idx].end(),bingfs.begin(),bingfs.end(),inserter(symdif,symdif.begin()));
      assert(symdif.empty());
      // detailed check
      set<SchedTreeBase::tFastTreeIdx> allBalancingIdxs;
      for(size_t k=0;k<nbers;k++)
      {
        // get the balancers
        SchedTreeBase::tFastTreeIdx balancerIdx = balancers[k];
        SchedTreeBase::tFastTreeIdx balancingWNodesCount;
        const DrainingInverseFastTree::tWeightedNode *balancingWNodes;
        // ask for all the balancings
        set<SchedTreeBase::tFastTreeIdx> balancingIdxs;
        assert(biftrees[idx].getFullInverseLookup(balancerIdx,balancingWNodes,&balancingWNodesCount));
        for(int j=0;j<balancingWNodesCount;j++) {
          balancingIdxs.insert(balancingWNodes[j].mIdx);
          allBalancingIdxs.insert(balancingWNodes[j].mIdx);
        }
        if(!balancingIdxs.size()) {
          // every balancing must have a closer balancer than the current one
          for(int j=0;j<nbings;j++){
            assert(maxBcingToBcerSimil[idx][ftinfos[idx][balancings[j]].mFsId] >
            treeDepthSimilarity(ftinfos[idx][balancerIdx].mFullGeotag,ftinfos[idx][balancings[j]].mFullGeotag));
          }
        }
        else {
          // the balancer is the closest one for all the found balancings
          for(auto bcingit=balancingIdxs.begin(); bcingit!=balancingIdxs.end();bcingit++){
            assert(maxBcingToBcerSimil[idx][ftinfos[idx][*bcingit].mFsId] ==
                treeDepthSimilarity(ftinfos[idx][balancerIdx].mFullGeotag,ftinfos[idx][*bcingit].mFullGeotag));
          }
        }

      }
      // finally, check that all the balacing has at least a drainer
      sort(balancings,balancings+nbings);
      set_symmetric_difference(balancings,balancings+nbings,allBalancingIdxs.begin(),allBalancingIdxs.end(),inserter(symdif,symdif.begin()));
      assert(symdif.empty());
    }

    // illustrate some display facility
    if(idx ==0)
    {
      // display the SlowTree
      cout << "====== Illustrating the display of a SlowTree ======" <<std::endl;
      cout << trees[idx] << endl;
      cout << "====================================================" <<std::endl<<std::endl;
      // display the FastTree
      cout << "====== Illustrating the display of a Placement FastTree ======" <<std::endl;
      cout << ftrees[idx] << endl;
      cout << "==============================================================" <<std::endl<<std::endl;
      // display the FastTree
      cout << "====== Illustrating the display of an Access FastTree ======" <<std::endl;
      cout << ftrees2[idx] << endl;
      cout << "============================================================" <<std::endl<<std::endl;
      // display the info array related to the fast tree
      cout << "====== Illustrating the display of a Tree Nodes Information Table ======" <<std::endl;
      cout << ftinfos[idx];
      cout << "=========================================================================" <<std::endl<<std::endl;
      // display the mapping from fs to FastTree nodes
      cout << "====== Illustrating the display of a Fs2TreeIdxMap ======" <<std::endl;
      cout << ftmaps[idx];
      cout << "=========================================================" <<std::endl<<std::endl;
      // display the draining inverse fast tree lookup table
      cout << "====== Illustrating the display of a Draining Inverse Fast Tree Lookup Table ======" <<std::endl;
      cout << diftrees[idx];
      cout << "===================================================================================" <<std::endl<<std::endl;
      cout << "====== Illustrating the display of a Balacing Inverse Fast Tree Lookup Table ======" <<std::endl;
      // display the balancing inverse fast tree lookup table
      cout << biftrees[idx];
      cout << "===================================================================================" <<std::endl<<std::endl;
    }
#endif // FUNCTIONAL_TEST

    idx++;
  }

#if RUN_BURNIN_TEST==1
  const size_t nbIter = 10000;
  vector<SchedTreeBase::tFastTreeIdx> replicaIdxs(3*schedGroups.size());

  clock_t begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    char buffer[bufferSize];
    assert( ftrees[i%schedGroups.size()].copyToBuffer(buffer,bufferSize) == 0 );
    FastPlacementTree *ftree = (FastPlacementTree*)buffer;
    SchedTreeBase::tFastTreeIdx repId;
    for(int k=0;k<3;k++) {
      //ftree->placeNewReplica(0,false,repId);
      ftree->findFreeSlot(repId);
      replicaIdxs[3*(i%schedGroups.size())+k] = repId;
    }
  }
  clock_t elapsed = clock() - begin;
  cout << "REPLICA PLACEMENT SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << 3*schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " placements/sec " << endl;
  cout << "----------------------------" << endl << endl;


  begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    char buffer[bufferSize];
    assert( ftrees[i%schedGroups.size()].copyToBuffer(buffer,bufferSize) == 0 );
    char buffer2[bufferSize];
    assert( ftrees2[i%schedGroups.size()].copyToBuffer(buffer2,bufferSize) == 0 );
  }
  elapsed = clock() - begin;
  cout << "FAST TREE COPY ONLY SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << 6*schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " copies/sec " << endl;
  cout << "----------------------" << endl<< endl;


  begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    char buffer[bufferSize];
    assert( ftrees[i%schedGroups.size()].copyToBuffer(buffer,bufferSize) == 0 );
    FastPlacementTree *ftree = (FastPlacementTree*)buffer;
    char buffer2[bufferSize];
    assert( ftrees2[i%schedGroups.size()].copyToBuffer(buffer2,bufferSize) == 0 );
    FastAccessTree *ftree2 = (FastAccessTree*)buffer2;
    // update the tree
    for(int k=0;k<3;k++) {
      ftree->decrementFreeSlot(replicaIdxs[3*(i%schedGroups.size())+k]);
      ftree2->incrementFreeSlot(replicaIdxs[3*(i%schedGroups.size())+k]);
    }
  }
  elapsed = clock() - begin;
  cout << "TREE REPOPULATING SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << 6*schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " repop/sec " << endl;
  cout << "----------------------" << endl<< endl;


  begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    char buffer[bufferSize];
    assert( ftrees2[i%schedGroups.size()].copyToBuffer(buffer,bufferSize) == 0 );
    FastAccessTree *ftree = (FastAccessTree*)buffer;
    // pick a random geolocation which makes sense in the tree
    const string &clientGeoString = ftinfos[i%schedGroups.size()][rand()%ftinfos[i%schedGroups.size()].size()].mFullGeotag;
    SchedTreeBase::tFastTreeIdx closestNode =
        geomaps[i%schedGroups.size()].getClosestFastTreeNode(clientGeoString.c_str());
    // update the tree
    for(int k=0;k<3;k++) {
      ftree->incrementFreeSlot(replicaIdxs[3*(i%schedGroups.size())+k]);
    }
    // access the replicas
    SchedTreeBase::tFastTreeIdx repId;
    //ftree->findFreeSlot(closestNode,true,repId,false);
    ftree->findFreeSlot(repId,closestNode,true,true);
  }
  elapsed = clock() - begin;
  cout << "FILE ACCESS 1 REP SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << 3*schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " access/sec " << endl;
  cout << "----------------------" << endl<< endl;

  begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    char buffer[bufferSize];
    assert( ftrees2[i%schedGroups.size()].copyToBuffer(buffer,bufferSize) == 0 );
    FastAccessTree *ftree = (FastAccessTree*)buffer;
    // update the tree
    for(int k=0;k<3;k++) {
      ftree->incrementFreeSlot(replicaIdxs[3*(i%schedGroups.size())+k]);
    }    // access the replicas
    SchedTreeBase::tFastTreeIdx repIdxs[3];
    ftree->findFreeSlotsAll(repIdxs,3);
  }
  elapsed = clock() - begin;
  cout << "FILE ACCESS ALL REP SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << 3*schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " access/sec " << endl;
  cout << "----------------------" << endl <<endl;

  vector< vector<SchedTreeBase::tFastTreeIdx> > drainers(schedGroups.size()),balancers(schedGroups.size());
  SchedTreeBase::tFastTreeIdx fsize;
  for(size_t idx=0;idx<schedGroups.size();idx++) {
    drainers[idx].resize(128);
    fsize = ftrees[idx].findFreeSlotsAll(&drainers[idx][0],drainers[idx].size(),0,false,SchedTreeBase::Drainer);
    assert(fsize);
    drainers[idx].resize(fsize);
    balancers[idx].resize(128);
    fsize = ftrees[idx].findFreeSlotsAll(&balancers[idx][0],balancers[idx].size(),0,false,SchedTreeBase::Balancer);
    assert(fsize);
    drainers[idx].resize(fsize);
  }

  begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    SchedTreeBase::tFastTreeIdx destFs,srcFs;
    destFs = balancers[i%schedGroups.size()][(rand()*balancers[i%schedGroups.size()].size())/RAND_MAX];
    biftrees[i%schedGroups.size()].getRandomInverseLookup(destFs,srcFs);
  }
  elapsed = clock() - begin;
  cout << "BALANCING PLACEMENT SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " placements/sec " << endl;
  cout << "----------------------" << endl << endl;

  begin = clock();
  for(size_t i=0;i<schedGroups.size()*nbIter;i++) {
    SchedTreeBase::tFastTreeIdx destFs,srcFs;
    destFs = drainers[i%schedGroups.size()][(rand()*drainers[i%schedGroups.size()].size())/RAND_MAX];
    diftrees[i%schedGroups.size()].getRandomInverseLookup(destFs,srcFs);
  }
  elapsed = clock() - begin;
  cout << "DRAINING PLACEMENT SPEED TEST" << endl;
  cout << "elapsed time : " << float(elapsed)/CLOCKS_PER_SEC << " sec." << endl;
  cout << "speed        : " << schedGroups.size()*nbIter/ (float(elapsed)/CLOCKS_PER_SEC) << " placements/sec " << endl;
  cout << "----------------------" << endl;

#endif // BURNIN_TEST

  return 0;
}
