//------------------------------------------------------------------------------
// File: FsView.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include <cfloat>
#include <curl/curl.h>
#include "common/config/ConfigParsing.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "mgm/FsView.hh"
#include "mgm/GeoBalancer.hh"
#include "mgm/Balancer.hh"
#include "mgm/GroupBalancer.hh"
#include "mgm/convert/old/Converter.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/tgc/Constants.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/ZMQ.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/StringConversion.hh"
#include "common/Assert.hh"
#include "common/InstanceName.hh"
#include "mq/SharedHashWrapper.hh"
#include "common/Constants.hh"
#include "common/token/EosTok.hh"
#include "common/TransferQueue.hh"

using eos::common::RWMutexReadLock;

EOSMGMNAMESPACE_BEGIN

FsView FsView::gFsView;
std::atomic<bool> FsSpace::gDisableDefaults {false};

//------------------------------------------------------------------------------
// Destructor - destructs all the branches starting at this node
//------------------------------------------------------------------------------
GeoTreeElement::~GeoTreeElement()
{
  for (auto it = mSons.begin(); it != mSons.end(); it++) {
    delete it->second;
  }
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
GeoTree::GeoTree() : pLevels(8)
{
  pLevels.resize(1);
  pRoot = new GeoTreeElement;
  pLevels[0].insert(pRoot);
  pRoot->mTagToken = "<ROOT>";
  pRoot->mFullTag = "<ROOT>";
  pRoot->mFather = NULL;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
GeoTree::~GeoTree()
{
  delete pRoot;
}

//------------------------------------------------------------------------------
// Insert a FileSystem into the tree
//------------------------------------------------------------------------------
bool GeoTree::insert(const fsid_t& fs)
{
  if (pLeaves.count(fs)) {
    return false;
  }

  std::string geotag = getGeoTag(fs);
  // Tokenize the geotag (geo tag is like adas::acsd::csdw::fee)
  std::vector<std::string> geotokens;
  eos::common::StringConversion::EmptyTokenize(geotag, geotokens, ":");
  size_t s;
  s = geotokens.size();

  for (size_t i = 0; i < s; i++) {
    if (geotokens[i].size()) {
      geotokens.push_back(geotokens[i]);
    }
  }

  geotokens.erase(geotokens.begin(), geotokens.begin() + s);

  if (geotokens.empty()) {
    geotokens.push_back("");  // geotag is not provided
  }

  GeoTreeElement* father = pRoot;
  std::string fulltag = pRoot->mFullTag;
  // Insert all the geotokens in the tree
  GeoTreeElement* currentnode = pRoot;
  GeoTreeElement* currentleaf = NULL;

  for (int i = 0; i < (int)geotokens.size() - 1; i++) {
    const std::string& geotoken = geotokens[i];

    if (currentnode->mSons.count(geotoken)) {
      currentnode = father->mSons[geotoken];

      if (!fulltag.empty()) {
        fulltag += "::";
      }

      fulltag += geotoken;
    } else {
      currentnode = new GeoTreeElement;
      currentnode->mTagToken = geotoken;

      if (!fulltag.empty()) {
        fulltag += "::";
      }

      fulltag += geotoken;
      currentnode->mFullTag = fulltag;
      currentnode->mFather = father;
      father->mSons[geotoken] = currentnode;

      if ((int)pLevels.size() < i + 2) {
        pLevels.resize(i + 2);
      }

      pLevels[i + 1].insert(currentnode);
    }

    father = currentnode;
  }

  // Finally, insert the fs
  if (!father->mSons.count(geotokens.back())) {
    currentleaf = new GeoTreeElement;
    currentleaf->mFather = father;
    currentleaf->mTagToken = geotokens.back();

    if (!fulltag.empty()) {
      fulltag += "::";
    }

    fulltag += geotokens.back();
    currentleaf->mFullTag = fulltag;
    father->mSons[geotokens.back()] = currentleaf;

    if (pLevels.size() < geotokens.size() + 1) {
      pLevels.resize(geotokens.size() + 1);
    }

    pLevels[geotokens.size()].insert(currentleaf);
  } else {
    // assert(father->mSons[geotokens.back()]->mIsLeaf);
    currentleaf = father->mSons[geotokens.back()];
  }

  if (!currentleaf->mFsIds.count(fs)) {
    currentleaf->mFsIds.insert(fs);
    pLeaves[fs] = currentleaf;
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get number of file systems in the tree
//------------------------------------------------------------------------------
size_t GeoTree::size() const
{
  return pLeaves.size();
}

//------------------------------------------------------------------------------
// Remove a file system from the tree
//------------------------------------------------------------------------------
bool GeoTree::erase(const fsid_t& fs)
{
  GeoTreeElement* leaf;

  if (!pLeaves.count(fs)) {
    return false;
  } else {
    leaf = pLeaves[fs];
  }

  pLeaves.erase(fs);
  leaf->mFsIds.erase(fs);
  GeoTreeElement* father = leaf;

  if (leaf->mFsIds.empty() && leaf->mSons.empty()) {
    // Compute the depth for the current father
    int depth = -1;

    for (int i = (int)pLevels.size() - 1; i >= 0; i--) {
      if (pLevels[i].count(father)) {
        depth = i;
        break;
      }
    }

    assert(depth >= 0); // consistency check

    if (depth < 0) {
      return false;
    }

    // Go uproot until there is more than one branch
    while (father->mFather && father->mFather->mSons.size() == 1 &&
           father->mFather->mFsIds.empty()) {
      if (father->mFather == pRoot) {
        break;
      }

      pLevels[depth--].erase(father);
      // We don't update the father's sons list on purpose in order to keep
      // the reference for the destruction
      father = father->mFather;
    }

    // Erase the full branch
    if (father->mFather) {
      father->mFather->mSons.erase(father->mTagToken);
    }

    pLevels[depth].erase(father);
    delete father;
    // Update the pLevels size if needed
    int count = 0;

    for (auto it = pLevels.rbegin(); it != pLevels.rend(); it++) {
      if (!it->empty()) {
        if (count) {
          pLevels.resize(pLevels.size() - count);
        }

        break;
      }

      count++;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Get the geotag at which the fs is stored if found
//------------------------------------------------------------------------------
bool GeoTree::getGeoTagInTree(const fsid_t& fs, std::string& geoTag)
{
  if (!pLeaves.count(fs)) {
    return false;
  } else {
    geoTag = pLeaves[fs]->mFullTag;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get file system geotag
//------------------------------------------------------------------------------
std::string GeoTree::getGeoTag(const fsid_t& fs) const
{
  FileSystem* entry = FsView::gFsView.mIdView.lookupByID(fs);

  if (!entry) {
    return "";
  }

  return entry->GetString("stat.geotag");
}

//------------------------------------------------------------------------------
//               * * *   Class GeoTree::const_iterator * * *
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Copy assignment operator
//------------------------------------------------------------------------------
GeoTree::const_iterator&
GeoTree::const_iterator::operator= (const const_iterator& it)
{
  if (this != &it) {
    mIt = it.mIt;
    mCont = it.mCont;
  }

  return *this;
}

//------------------------------------------------------------------------------
// ++ operator pre-increment
//------------------------------------------------------------------------------
GeoTree::const_iterator&
GeoTree::const_iterator::operator++()
{
  if (mIt != mCont->end()) {
    ++mIt;
  }

  return *this;
}

//------------------------------------------------------------------------------
// ++ operator post-increment
//------------------------------------------------------------------------------
GeoTree::const_iterator
GeoTree::const_iterator::operator++(int)
{
  GeoTree::const_iterator it(*this);

  if (mIt != mCont->end()) {
    ++mIt;
  }

  return it;
}

//------------------------------------------------------------------------------
// -- operator pre-decrement
//------------------------------------------------------------------------------
GeoTree::const_iterator&
GeoTree::const_iterator::operator--()
{
  if (mIt != mCont->begin()) {
    --mIt;
  }

  return *this;
}

//------------------------------------------------------------------------------
// -- operator post-decrement
//------------------------------------------------------------------------------
GeoTree::const_iterator
GeoTree::const_iterator::operator--(int)
{
  GeoTree::const_iterator it(*this);

  if (mIt != mCont->begin()) {
    --mIt;
  }

  return it;
}

//------------------------------------------------------------------------------
// Indirection operator
//------------------------------------------------------------------------------
const eos::common::FileSystem::fsid_t&
GeoTree::const_iterator::operator*() const
{
  return mIt->first;
}

//------------------------------------------------------------------------------
// fsid_iterator: Iterate either over a given subset, or the pLeaves map.
// Yes this is weird, but we need it for certain BaseView functions.
//------------------------------------------------------------------------------
class fsid_iterator
{
public:
  //----------------------------------------------------------------------------
  // Constructor. Iterate through subset: if subset is nullptr, iterate through
  // tree instead.
  //----------------------------------------------------------------------------
  fsid_iterator(const std::set<eos::common::FileSystem::fsid_t>* subset,
                GeoTree* tree)
  {
    subsetValid = subset != nullptr;

    if (subsetValid) {
      subsetIter = subset->begin();
      subsetEnd = subset->end();
    } else {
      geotreeIter = tree->begin();
      geotreeEnd = tree->end();
    }
  }

  //----------------------------------------------------------------------------
  // Is the iterator still valid?
  //----------------------------------------------------------------------------
  bool valid() const
  {
    if (subsetValid) {
      return subsetIter != subsetEnd;
    }

    return geotreeIter != geotreeEnd;
  }

  //----------------------------------------------------------------------------
  // Advance
  //----------------------------------------------------------------------------
  void next()
  {
    if (!valid()) {
      return;
    }

    if (subsetValid) {
      subsetIter++;
    } else {
      geotreeIter++;
    }
  }

  eos::common::FileSystem::fsid_t operator*() const
  {
    if (subsetValid) {
      return *subsetIter;
    }

    return *geotreeIter;
  }

private:
  bool subsetValid;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator subsetIter;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator subsetEnd;

  GeoTree::const_iterator geotreeIter;
  GeoTree::const_iterator geotreeEnd;
};

//------------------------------------------------------------------------------
// Run an aggregator through the tree
//------------------------------------------------------------------------------
bool GeoTree::runAggregator(GeoTreeAggregator* aggregator) const
{
  if (pLevels.empty()) {
    return false;
  }

  // Build the GeoTags and the depth indexes
  size_t elemCount = 0;
  std::vector<std::string> geotags;
  std::vector<size_t> depthlevelsendindexes;

  for (auto itl = pLevels.begin(); itl != pLevels.end(); itl++) {
    geotags.resize(geotags.size() + itl->size());

    for (auto ite = itl->rbegin(); ite != itl->rend(); ite++) {
      // could be made faster and more complex but probably not necessary for the moment
      geotags[elemCount] = (*ite)->mTagToken;
      GeoTreeElement* element = *ite;

      while (element->mFather) {
        element = element->mFather;
        geotags[elemCount] = element->mTagToken + "::" + geotags[elemCount];
      }

      elemCount++;
    }

    depthlevelsendindexes.push_back(elemCount);
  }

  aggregator->init(geotags, depthlevelsendindexes);
  elemCount--;

  for (auto itl = pLevels.rbegin(); itl != pLevels.rend(); itl++) {
    for (auto ite = itl->begin(); ite != itl->end(); ite++) {
      (*ite)->mId = elemCount;

      if (!aggregator->aggregateLeavesAndNodes((*ite)->mFsIds, (*ite)->mSons,
          elemCount--)) {
        return false;
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// @brief Get the sums at each tree element
//------------------------------------------------------------------------------
const std::vector<double>* DoubleAggregator::getSums() const
{
  return &pSums;
}

//------------------------------------------------------------------------------
// @brief Get the averages at each tree element
//------------------------------------------------------------------------------
const std::vector<double>* DoubleAggregator::getMeans() const
{
  return &pMeans;
}

//------------------------------------------------------------------------------
// @brief Get the maximum deviations at each tree element
//------------------------------------------------------------------------------
const std::vector<double>* DoubleAggregator::getMaxAbsDevs() const
{
  return &pMaxAbsDevs;
}

//------------------------------------------------------------------------------
// @brief Get the standard deviations at each tree element
//------------------------------------------------------------------------------
const std::vector<double>* DoubleAggregator::getStdDevs() const
{
  return &pStdDevs;
}

//------------------------------------------------------------------------------
// Get the geotags at each tree element
//------------------------------------------------------------------------------
const std::vector<std::string>* DoubleAggregator::getGeoTags() const
{
  return &pGeoTags;
}

//------------------------------------------------------------------------------
// @brief Get the end index (excluded) for a given depth level
// @param depth the maximum depth to be reached (-1 for unlimited)
// @return the index of the first element in the vectors being deeper that depth
//------------------------------------------------------------------------------
size_t DoubleAggregator::getEndIndex(int depth) const
{
  if (depth < 0 || depth > (int)pDepthLevelsIndexes.size() - 1) {
    depth = pDepthLevelsIndexes.size() - 1;
  }

  return pDepthLevelsIndexes[depth];
};

//------------------------------------------------------------------------------
// @brief Constructor
// @param param Name of the parameter statistics have to be computed for
//------------------------------------------------------------------------------
DoubleAggregator::DoubleAggregator(const char* param):
  pParam(param), pView(NULL)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DoubleAggregator::~DoubleAggregator()
{}

//------------------------------------------------------------------------------
// @brief Set the view ordering the statistics. Needs to be set before running
// the aggregator.
// @param view Pointer to the view ordering the statistics
//------------------------------------------------------------------------------
void DoubleAggregator::setView(BaseView* view)
{
  pView = view;
}

//------------------------------------------------------------------------------
// Initialize
//------------------------------------------------------------------------------
bool
DoubleAggregator::init(const std::vector<std::string>& geotags,
                       const std::vector<size_t>& depthLevelsIndexes)
{
  // Check that the view is defined, this is necessary for the subsequent calls
  // to AggregateXXX.
  assert(pView);
  pGeoTags = geotags;
  pDepthLevelsIndexes = depthLevelsIndexes;
  return true;
}

//------------------------------------------------------------------------------
// Aggregate leaves
//------------------------------------------------------------------------------
bool
DoubleAggregator::aggregateLeaves(
  const std::set<eos::common::FileSystem::fsid_t>& leaves, const size_t& idx)
{
  // The following should happen only at the first call
  if ((int)idx > (int)pMeans.size() - 1) {
    pSums.resize(idx + 1);
    pMeans.resize(idx + 1);
    pMaxDevs.resize(idx + 1);
    pMinDevs.resize(idx + 1);
    pMaxAbsDevs.resize(idx + 1);
    pStdDevs.resize(idx + 1);
    pNb.resize(idx + 1);
  }

  pNb[idx] = pView->ConsiderCount(false, &leaves);

  if (pNb[idx]) {
    pSums[idx] = pView->SumDouble(pParam.c_str(), false, &leaves);
    pMeans[idx] = pView->AverageDouble(pParam.c_str(), false, &leaves);
    pMaxDevs[idx] = (pNb[idx] == 1) ? 0 : pView->MaxDeviation(pParam.c_str(), false,
                    &leaves);
    pMinDevs[idx] = (pNb[idx] == 1) ? 0 : pView->MinDeviation(pParam.c_str(), false,
                    &leaves);
    pStdDevs[idx] = (pNb[idx] == 1) ? 0 : pView->SigmaDouble(pParam.c_str(), false,
                    &leaves);
    pMaxAbsDevs[idx] = (pNb[idx] == 1) ? 0 : std::max(abs(pMaxDevs[idx]),
                       abs(pMinDevs[idx]));
  } else {
    pSums[idx] = 0;
    pMeans[idx] = 0;
    pMaxDevs[idx] = 0;
    pMinDevs[idx] = 0;
    pStdDevs[idx] = 0;
    pMaxAbsDevs[idx] = 0;
  }

  return true;
}

//------------------------------------------------------------------------------
// Aggregate nodes
//------------------------------------------------------------------------------
bool
DoubleAggregator::aggregateNodes(
  const std::map<std::string, GeoTreeElement*>& nodes,
  const size_t& idx, bool includeSelf)
{
  double pS, pM, pMAD, pSD, pMiD, pMaD;
  pS = pM = pMAD = pSD = 0;
  pMiD = DBL_MAX;
  pMaD = -DBL_MAX;
  long long pN = 0;

  for (auto it = nodes.begin(); it != nodes.end(); it++) {
    size_t i = it->second->mId;
    pS += pSums[i];
    pN += pNb[i];
  }

  if (pN) {
    pM = pS / pN;
  }

  for (auto it = nodes.begin(); it != nodes.end(); it++) {
    size_t i = it->second->mId;

    if (pNb[i]) { // consider this only if there is something there
      pMiD = std::min(pMiD, std::min((pMinDevs[i] + pMeans[i]) - pM,
                                     (pMaxDevs[i] + pMeans[i]) - pM));
      pMaD = std::max(pMaD, std::max((pMinDevs[i] + pMeans[i]) - pM,
                                     (pMaxDevs[i] + pMeans[i]) - pM));
      pSD += pNb[i] * (pStdDevs[i] * pStdDevs[i] + pMeans[i] * pMeans[i]);
    }
  }

  if (pN) {
    pSD = sqrt(pSD / pN - pM * pM);
    pMAD = std::max(fabs(pMaD), fabs(pMiD));
  }

  if (includeSelf) {
    pS += pSums[idx];
    pN += pNb[idx];

    if (pN) {
      pM = pS / pN;
    }

    pMiD = std::min(pMiD,
                    std::min((pMinDevs[idx] + pMeans[idx]) - pM,
                             (pMaxDevs[idx] + pMeans[idx]) - pM));
    pMaD = std::max(pMaD,
                    std::max((pMinDevs[idx] + pMeans[idx]) - pM,
                             (pMaxDevs[idx] + pMeans[idx]) - pM));
    pSD += pNb[idx] * (pStdDevs[idx] * pStdDevs[idx] + pMeans[idx] * pMeans[idx]);

    if (pN) {
      pSD = sqrt(pSD / pN - pM * pM);
      pMAD = std::max(fabs(pMaD), fabs(pMiD));
    }
  }

  pSums[idx] = pS;
  pMeans[idx] = pM;
  pMaxAbsDevs[idx] = pMAD;
  pStdDevs[idx] = pSD;
  pMinDevs[idx] = pMiD;
  pMaxDevs[idx] = pMaD;
  pNb[idx] = pN;
  return true;
}

//------------------------------------------------------------------------------
// @brief Constructor
// @param param Name of the parameter statistics have to be computed for
//------------------------------------------------------------------------------
LongLongAggregator::LongLongAggregator(const char* param):
  pParam(param), pView(NULL)
{}

//------------------------------------------------------------------------------
// @brief Destructor
//------------------------------------------------------------------------------
LongLongAggregator::~LongLongAggregator()
{}

//------------------------------------------------------------------------------
// @brief Set the view ordering the statistics. Needs to be set before running
//        the aggregator
// @param view Pointer to the view ordering the statistics
//------------------------------------------------------------------------------
void LongLongAggregator::setView(BaseView* view)
{
  pView = view;
}

//------------------------------------------------------------------------------
// Initialize
//------------------------------------------------------------------------------
bool
LongLongAggregator::init(const std::vector<std::string>& geotags,
                         const std::vector<size_t>& depthLevelsIndexes)
{
  assert(pView);
  pGeoTags = geotags;
  pDepthLevelsIndexes = depthLevelsIndexes;
  return true;
}

//------------------------------------------------------------------------------
//@brief Get the sums at each tree element
//------------------------------------------------------------------------------
const std::vector<long long>* LongLongAggregator::getSums() const
{
  return &pSums;
}

//------------------------------------------------------------------------------
// @brief Get the geotags at each tree element
//------------------------------------------------------------------------------
const std::vector<std::string>* LongLongAggregator::getGeoTags() const
{
  return &pGeoTags;
}

//------------------------------------------------------------------------------
// @brief Get the end index (excluded) for a given depth level
// @param depth the maximum depth to be reached (-1 for unlimited)
// @return the index of the first element in the vectors being deeper that depth
//------------------------------------------------------------------------------
size_t LongLongAggregator::getEndIndex(int depth) const
{
  if (depth < 0 || depth > (int)pDepthLevelsIndexes.size() - 1) {
    depth = pDepthLevelsIndexes.size() - 1;
  }

  return pDepthLevelsIndexes[depth];
};

//------------------------------------------------------------------------------
// Aggregate leaves
//------------------------------------------------------------------------------
bool
LongLongAggregator::aggregateLeaves(
  const std::set<eos::common::FileSystem::fsid_t>& leaves,
  const size_t& idx)
{
  // The following should happen only at the first call
  if ((int)idx > (int)pSums.size() - 1) {
    pSums.resize(idx + 1);
  }

  pSums[idx] = 0;
  pSums[idx] = pView->SumLongLong(pParam.c_str(), false, &leaves);
  return true;
}

//------------------------------------------------------------------------------
// Aggregate nodes
//------------------------------------------------------------------------------
bool
LongLongAggregator::aggregateNodes(
  const std::map<std::string, GeoTreeElement*>& nodes, const size_t& idx,
  bool includeSelf)
{
  long long pS = 0;

  for (auto it = nodes.begin(); it != nodes.end(); it++) {
    size_t i = it->second->mId;
    pS += pSums[i];
  }

  if (includeSelf) {
    pS += pSums[idx];
  }

  pSums[idx] = pS;
  return true;
};

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
FsSpace::FsSpace(const char* name)
  : BaseView(common::SharedHashLocator::makeForSpace(name)),
    mConverter(nullptr)
{
  mName = name;
  mType = "spaceview";
  mBalancer = new Balancer(name);
  mGroupBalancer = new GroupBalancer(name);
  mGeoBalancer = new GeoBalancer(name);

  // Start old converter if we're using the in-memory NS or if the new one
  // is disabled on purpose
  if (!gOFS->NsInQDB || getenv("EOS_FORCE_DISABLE_NEW_CONVERTER")) {
    eos_static_info("%s", "msg=\"start the old converter\"");
    mConverter = new Converter(name);
  } else {
    eos_static_info("%s", "msg=\"skip starting the old converter\"");
  }

  if (!gDisableDefaults) {
    // Disable balancing by default
    if (GetConfigMember("balancer").empty()) {
      SetConfigMember("balancer", "off");
    }

    // Set deviation treshold
    if (GetConfigMember("balancer.threshold").empty()) {
      SetConfigMember("balancer.threshold", "20");
    }

    // Set balancing rate per balancing stream
    if (GetConfigMember("balancer.node.rate").empty()) {
      SetConfigMember("balancer.node.rate", "25");
    }

    // Set parallel balancing streams per node
    if (GetConfigMember("balancer.node.ntx").empty()) {
      SetConfigMember("balancer.node.ntx", "2");
    }

    // Set drain rate per drain stream
    if (GetConfigMember("drain.node.rate").empty()) {
      SetConfigMember("drainer.node.rate", "25");
    }

    // Set parallel draining streams per node
    if (GetConfigMember("drainer.node.ntx").empty()) {
      SetConfigMember("drainer.node.ntx", "2");
    }

    // Set the grace period before drain start on opserror to 1 day
    if (GetConfigMember("graceperiod").empty()) {
      SetConfigMember("graceperiod", "86400");
    }

    // Set the time for a drain by default to 1 day
    if (GetConfigMember("drainperiod").empty()) {
      SetConfigMember("drainperiod", "86400");
    }

    // Set the scan IO rate by default to 100 MB/s
    if (GetConfigMember(eos::common::SCAN_IO_RATE_NAME).empty()) {
      SetConfigMember(eos::common::SCAN_IO_RATE_NAME, "100");
    }

    // Set the scan entry interval by default to 1 week
    if (GetConfigMember(eos::common::SCAN_ENTRY_INTERVAL_NAME).empty()) {
      SetConfigMember(eos::common::SCAN_ENTRY_INTERVAL_NAME, "604800");
    }

    // Set the scan disk rerun interval by default to 4 hours
    if (GetConfigMember(eos::common::SCAN_DISK_INTERVAL_NAME).empty()) {
      SetConfigMember(eos::common::SCAN_DISK_INTERVAL_NAME, "14400");
    }

    // Set the scan ns rate by default to 50 entries per second
    if (GetConfigMember(eos::common::SCAN_NS_RATE_NAME).empty()) {
      SetConfigMember(eos::common::SCAN_NS_RATE_NAME, "50");
    }

    // Set the scan ns rerun interval by default to 3 days
    if (GetConfigMember(eos::common::SCAN_NS_INTERVAL_NAME).empty()) {
      SetConfigMember(eos::common::SCAN_NS_INTERVAL_NAME, "259200");
    }

    // Set the fsck refresh interval by default to 2 hours
    if (GetConfigMember(eos::common::FSCK_REFRESH_INTERVAL_NAME).empty()) {
      SetConfigMember(eos::common::FSCK_REFRESH_INTERVAL_NAME, "7200");
    }

    // Disable quota by default
    if (GetConfigMember("quota").empty()) {
      SetConfigMember("quota", "off");
    }

    // Set the group modulo to 0
    if (GetConfigMember("groupmod").empty()) {
      SetConfigMember("groupmod", "0");
    }

    // Set the group size to 0
    if (GetConfigMember("groupsize").empty()) {
      SetConfigMember("groupsize", "0");
    }

    // Disable converter by default
    if (GetConfigMember("converter").empty()) {
      SetConfigMember("converter", "off");
    }

    // Set two converter streams by default
    if (GetConfigMember("converter.ntx").empty()) {
      SetConfigMember("converter.ntx", "2");
    }

    if (GetConfigMember("groupbalancer").empty()) {
      SetConfigMember("groupbalancer", "off");
    }

    // Set the groupbalancer max number of scheduled files by default
    if (GetConfigMember("groupbalancer.ntx").empty()) {
      SetConfigMember("groupbalancer.ntx", "10");
    }

    // Set the groupbalancer threshold by default
    if (GetConfigMember("groupbalancer.threshold").empty()) {
      SetConfigMember("groupbalancer.threshold", "5");
    }

    // Set the groupbalancer min file size by default
    if (GetConfigMember("groupbalancer.min_file_size").empty()) {
      SetConfigMember("groupbalancer.min_file_size", "1G");
    }

    // Set the groupbalancer max file size by default
    if (GetConfigMember("groupbalancer.max_file_size").empty()) {
      SetConfigMember("groupbalancer.max_file_size", "16G");
    }

    if (GetConfigMember("groupbalancer.file_attempts").empty()) {
      SetConfigMember("groupbalancer.file_attempts", "50");
    }

    // Set the default group balancer engine
    if (GetConfigMember("groupbalancer.engine").empty()) {
      SetConfigMember("groupbalancer.engine", "std");
    }

    if (GetConfigMember("groupbalancer.min_threshold").empty()) {
      SetConfigMember("groupbalancer.min_threshold", "0");
    }

    if (GetConfigMember("groupbalancer.max_threshold").empty()) {
      SetConfigMember("groupbalancer.max_threshold", "0");
    }

    if (GetConfigMember("geobalancer").empty()) {
      SetConfigMember("geobalancer", "off");
    }

    // Set the geobalancer max number of scheduled files by default
    if (GetConfigMember("geobalancer.ntx").empty()) {
      SetConfigMember("geobalancer.ntx", "10");
    }

    // Set the geobalancer threshold by default
    if (GetConfigMember("geobalancer.threshold").empty()) {
      SetConfigMember("geobalancer.threshold", "5");
    }

    // Disable lru by default
    if (GetConfigMember("lru").empty()) {
      SetConfigMember("converter", "off");
    }

    // Set one week lru interval by default
    if (GetConfigMember("lru.interval") == "604800") {
      SetConfigMember("converter.ntx", "2");
    }

    // Set the wfe off by default
    if (GetConfigMember("wfe").empty()) {
      SetConfigMember("wfe", "off");
    }

    // Set the wfe interval by default
    if (GetConfigMember("wfe.interval").empty()) {
      SetConfigMember("wfe.interval", "10");
    }

    // Set the wfe ntx by default
    if (GetConfigMember("wfe.ntx").empty()) {
      SetConfigMember("wfe.ntx", "1");
    }

    // Disable the 'file archived' garbage collector by default
    if (GetConfigMember("filearchivedgc").empty()) {
      SetConfigMember("filearchivedgc", "off");
    }

    // Set the default delay in seconds between queries from the tape-aware GC
    if (GetConfigMember(tgc::TGC_NAME_QRY_PERIOD_SECS).empty()) {
      SetConfigMember(tgc::TGC_NAME_QRY_PERIOD_SECS,
                      std::to_string(tgc::TGC_DEFAULT_QRY_PERIOD_SECS));
    }

    // Set the default number of available bytes the garbage collector is targetting
    if (GetConfigMember(tgc::TGC_NAME_AVAIL_BYTES).empty()) {
      SetConfigMember(tgc::TGC_NAME_AVAIL_BYTES,
                      std::to_string(tgc::TGC_DEFAULT_AVAIL_BYTES));
    }

    // Set the default of the script used to determine the number of free bytes in a given EOS space
    if (GetConfigMember(tgc::TGC_NAME_FREE_BYTES_SCRIPT).empty()) {
      SetConfigMember(tgc::TGC_NAME_FREE_BYTES_SCRIPT,
                      tgc::TGC_DEFAULT_FREE_BYTES_SCRIPT);
    }

    // Set the default total number of bytes that must be available before
    // garbage collection can start
    if (GetConfigMember(tgc::TGC_NAME_TOTAL_BYTES).empty()) {
      SetConfigMember(tgc::TGC_NAME_TOTAL_BYTES,
                      std::to_string(tgc::TGC_DEFAULT_TOTAL_BYTES));
    }

    //Switch off the tape REST API by default
    if (GetConfigMember(rest::TAPE_REST_API_SWITCH_ON_OFF).empty()) {
      SetConfigMember(rest::TAPE_REST_API_SWITCH_ON_OFF,
                      "off");
    }
  }

  if (mName == std::string("default")) {
    // Disable tracker by default
    if (GetConfigMember("tracker").empty()) {
      SetConfigMember("tracker", "off");
    }
  }
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
FsSpace::~FsSpace()
{
  if (mBalancer) {
    delete mBalancer;
  }

  if (mConverter) {
    delete mConverter;
  }

  if (mGroupBalancer) {
    delete mGroupBalancer;
  }

  if (mGeoBalancer) {
    delete mGeoBalancer;
  }

  mBalancer = nullptr;
  mConverter = nullptr;
  mGroupBalancer = nullptr;
  mGeoBalancer = nullptr;
}

//----------------------------------------------------------------------------
// Stop function stopping threads before destruction
//----------------------------------------------------------------------------
void FsSpace::Stop()
{
  if (mBalancer) {
    mBalancer->Stop();
  }

  if (mConverter) {
    mConverter->Stop();
  }

  if (mGroupBalancer) {
    mGroupBalancer->Stop();
  }

  if (mGeoBalancer) {
    mGeoBalancer->Stop();
  }
}

//------------------------------------------------------------------------------
// Check if quota is enabled for space
//-----------------------------------------------------------------------------
bool FsView::IsQuotaEnabled(const std::string& space)
{
  bool is_enabled = false;
  std::string key = "quota";

  if (mSpaceView.count(space)) {
    std::string is_on = mSpaceView[space]->GetConfigMember(key);
    is_enabled = (is_on == "on");
  }

  return is_enabled;
}


//----------------------------------------------------------------------------
//! Physical bytes available
//----------------------------------------------------------------------------
bool FsView::UnderNominalQuota(const std::string& space, bool isroot)
{
  if (isroot) {
    return true;
  }

  time_t now = time(NULL);
  {
    XrdSysMutexHelper scope_lock(mUsageMutex);
    {
      // return cached value
      auto it = mUsageOk.find(space);

      if (it != mUsageOk.end()) {
        if (it->second.second > now) {
          return it->second.first;
        }
      }
    }
  }
  {
    auto spaceobj = mSpaceView.find(space);

    if (spaceobj == mSpaceView.end()) {
      // no space, we don't block by nominal quota
      return true;
    }

    // refresh the nominal value
    std::string nominal = spaceobj->second->GetMember("cfg.nominalsize");

    if (nominal == "???") {
      // no setting, quota is fine
      return true;
    }

    uint64_t nominalbytes = strtoul(nominal.c_str(), 0, 10);
    uint64_t usedbytes = 0 ;

    for (auto fs = mIdView.begin(); fs != mIdView.end(); ++fs) {
      if (fs->second->GetSpace() != space) {
        // only account the requested space
        continue;
      }

      usedbytes += fs->second->GetUsedbytes();
    }

    bool usage_ok = false;

    if (usedbytes < nominalbytes) {
      usage_ok = true;
    }

    // store the current values
    XrdSysMutexHelper scope_lock(mUsageMutex);
    mUsageOk[space].first = usage_ok;
    mUsageOk[space].second = now + 30; // cache for 30 seconds
    return usage_ok;
  }
}


//------------------------------------------------------------------------------
// @brief return's the printout format for a given option
// @param option see the implementation for valid options
// @return std::string with format line passed to the printout routine
//------------------------------------------------------------------------------
std::string
FsView::GetNodeFormat(std::string option)
{
  std::string format;

  if (option == "m") {
    // monitoring format
    format = "member=type:format=os|";
    format += "member=hostport:format=os|";
    format += "member=status:format=os|";
    format += "member=cfg.status:format=os|";
    format += "member=cfg.txgw:format=os|";
    format += "member=heartbeatdelta:format=os|";
    format += "member=nofs:format=ol|";
    format += "avg=stat.disk.load:format=of|";
    format += "sig=stat.disk.load:format=of|";
    format += "sum=stat.disk.readratemb:format=ol|";
    format += "sum=stat.disk.writeratemb:format=ol|";
    format += "member=cfg.stat.net.ethratemib:format=ol|";
    format += "member=cfg.stat.net.inratemib:format=ol|";
    format += "member=cfg.stat.net.outratemib:format=ol|";
    format += "sum=stat.ropen:format=ol|";
    format += "sum=stat.wopen:format=ol|";
    format += "sum=stat.statfs.freebytes:format=ol|";
    format += "sum=stat.statfs.usedbytes:format=ol|";
    format += "sum=stat.statfs.capacity:format=ol|";
    format += "sum=stat.usedfiles:format=ol|";
    format += "sum=stat.statfs.ffree:format=ol|";
    format += "sum=stat.statfs.fused:format=ol|";
    format += "sum=stat.statfs.files:format=ol|";
    format += "sum=stat.balancer.running:format=ol:tag=stat.balancer.running|";
    format += "member=stat.gw.queued:format=os:tag=stat.gw.queued|";
    format += "member=cfg.stat.sys.vsize:format=ol|";
    format += "member=cfg.stat.sys.rss:format=ol|";
    format += "member=cfg.stat.sys.threads:format=ol|";
    format += "member=cfg.stat.sys.sockets:format=os|";
    format += "member=cfg.stat.sys.eos.version:format=os|";
    format += "member=cfg.stat.sys.xrootd.version:format=os|";
    format += "member=cfg.stat.sys.kernel:format=os|";
    format += "member=cfg.stat.sys.eos.start:format=os|";
    format += "member=cfg.stat.sys.uptime:format=os|";
    format += "sum=stat.disk.iops?configstatus@rw:format=ol|";
    format += "sum=stat.disk.bw?configstatus@rw:format=ol|";
    format += "member=cfg.stat.geotag:format=os|";
    format += "member=cfg.gw.rate:format=os|";
    format += "member=cfg.gw.ntx:format=os";
  } else if (option == "io") {
    // io format
    format = "header=1:member=hostport:width=32:format=-sS|";
    format += "member=cfg.stat.geotag:width=16:format=s|";
    format += "avg=stat.disk.load:width=10:format=f:tag=diskload|";
    format += "sum=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|";
    format += "sum=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|";
    format += "member=cfg.stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|";
    format += "member=cfg.stat.net.inratemib:width=10:format=l:tag=ethi-MiB|";
    format += "member=cfg.stat.net.outratemib:width=10:format=l:tag=etho-MiB|";
    format += "sum=stat.ropen:width=6:format=l:tag=ropen|";
    format += "sum=stat.wopen:width=6:format=l:tag=wopen|";
    format += "sum=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|";
    format += "sum=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|";
    format += "sum=stat.usedfiles:width=12:format=+l:tag=used-files|";
    format += "sum=stat.statfs.files:width=11:format=+l:tag=max-files|";
    format += "sum=stat.balancer.running:width=10:format=l:tag=bal-shd|";
    format += "member=inqueue:width=10:format=s:tag=gw-queue|";
    format += "sum=stat.disk.iops?configstatus@rw:width=6:format=l:tag=iops|";
    format += "sum=stat.disk.bw?configstatus@rw:width=9:format=l:unit=MB:tag=bw";
  } else if (option == "sys") {
    // system format
    format = "header=1:member=hostport:width=32:format=-sS|";
    format += "member=cfg.stat.geotag:width=16:format=s|";
    format += "member=cfg.stat.sys.vsize:width=12:format=+l:tag=vsize|";
    format += "member=cfg.stat.sys.rss:width=12:format=+l:tag=rss|";
    format += "member=cfg.stat.sys.threads:width=12:format=+l:tag=threads|";
    format += "member=cfg.stat.sys.sockets:width=10:format=s:tag=sockets|";
    format += "member=cfg.stat.sys.eos.version:width=12:format=s:tag=eos|";
    format += "member=cfg.stat.sys.xrootd.version:width=12:format=s:tag=xrootd|";
    format += "member=cfg.stat.sys.kernel:width=30:format=s:tag=kernel version|";
    format += "member=cfg.stat.sys.eos.start:width=32:format=s:tag=start|";
    format += "member=cfg.stat.sys.uptime:width=80:format=s:tag=uptime";
  } else if (option == "fsck") {
    // filesystem check statistics format
    format = "header=1:member=hostport:width=32:format=-sS|";
    format += "sum=stat.fsck.mem_n:width=8:format=l:tag=n(mem)|";
    format += "sum=stat.fsck.d_sync_n:width=8:format=l:tag=n(disk)|";
    format += "sum=stat.fsck.m_sync_n:width=8:format=l:tag=n(mgm)|";
    format += "sum=stat.fsck.orphans_n:width=12:format=l:tag=e(orph)|";
    format += "sum=stat.fsck.unreg_n:width=12:format=l:tag=e(unreg)|";
    format += "sum=stat.fsck.rep_diff_n:width=12:format=l:tag=e(layout)|";
    format += "sum=stat.fsck.rep_missing_n:width=12:format=l:tag=e(miss)|";
    format += "sum=stat.fsck.d_mem_sz_diff:width=12:format=l:tag=e(disksize)|";
    format += "sum=stat.fsck.m_mem_sz_diff:width=12:format=l:tag=e(mgmsize)|";
    format += "sum=stat.fsck.d_cx_diff:width=12:format=l:tag=e(disk-cx)|";
    format += "sum=stat.fsck.m_cx_diff:width=12:format=l:tag=e(mgm-cx)";
  } else if (option == "l") {
    // long format
    format = "header=1:member=type:width=10:format=-s|";
    format += "member=hostport:width=32:format=sS|";
    format += "member=cfg.stat.geotag:width=16:format=s|";
    format += "member=status:width=10:format=s|";
    format += "member=cfg.status:width=12:format=s:tag=activated|";
    format += "member=cfg.txgw:width=6:format=s|";
    format += "member=heartbeatdelta:width=16:format=s|";
    format += "member=nofs:width=5:format=s|";
    format += "sum=stat.balancer.running:width=10:format=l:tag=balan-shd|";
    format += "member=inqueue:width=10:format=s:tag=gw-queue";
  } else {
    // default format
    format = "header=1:member=type:width=10:format=-s|";
    format += "member=hostport:width=32:format=sS|";
    format += "member=cfg.stat.geotag:width=16:format=s|";
    format += "member=status:width=10:format=s|";
    format += "member=cfg.status:width=12:format=s:tag=activated|";
    format += "member=cfg.txgw:width=6:format=s|";
    format += "member=inqueue:width=10:format=s:tag=gw-queued|";
    format += "member=cfg.gw.ntx:width=8:format=s:tag=gw-ntx|";
    format += "member=cfg.gw.rate:width=8:format=s:tag=gw-rate|";
    format += "member=heartbeatdelta:width=16:format=s|";
    format += "member=nofs:width=5:format=s";
  }

  return format;
}

//------------------------------------------------------------------------------
// @brief return's the printout format for a given option
// @param option see the implementation for valid options
// @return std;:string with format line passed to the printout routine
//------------------------------------------------------------------------------
std::string
FsView::GetFileSystemFormat(std::string option)
{
  std::string format;

  if (option == "m") {
    // monitoring format
    format = "key=host:format=os|";
    format += "key=port:format=os|";
    format += "key=id:format=os|";
    format += "key=uuid:format=os|";
    format += "key=path:format=os|";
    format += "key=schedgroup:format=os|";
    format += "key=stat.boot:format=os|";
    format += "key=configstatus:format=os|";
    format += "key=headroom:format=os|";
    format += "key=stat.errc:format=os|";
    format += "key=stat.errmsg:format=oqs|";
    format += "key=stat.disk.load:format=of|";
    format += "key=stat.disk.readratemb:format=ol|";
    format += "key=stat.disk.writeratemb:format=ol|";
    format += "key=stat.net.ethratemib:format=ol|";
    format += "key=stat.net.inratemib:format=ol|";
    format += "key=stat.net.outratemib:format=ol|";
    format += "key=stat.ropen:format=ol|";
    format += "key=stat.wopen:format=ol|";
    format += "key=stat.statfs.freebytes:format=ol|";
    format += "key=stat.statfs.usedbytes:format=ol|";
    format += "key=stat.statfs.capacity:format=ol|";
    format += "key=stat.usedfiles:format=ol|";
    format += "key=stat.statfs.ffree:format=ol|";
    format += "key=stat.statfs.fused:format=ol|";
    format += "key=stat.statfs.files:format=ol|";
    format += "key=local.drain:format=os|";
    format += "key=local.drain.progress:format=ol:tag=progress|";
    format += "key=local.drain.files:format=ol|";
    format += "key=local.drain.bytesleft:format=ol|";
    format += "key=local.drain.failed:format=ol|";
    format += "key=local.drain.timeleft:format=ol|";
    format += "key=graceperiod:format=ol|";
    format += "key=drainperiod:format=ol|";
    format += "key=stat.active:format=os|";
    format += "key=scaninterval:format=os|";
    format += "key=scanreruninterval:format=os|";
    format += "key=stat.balancer.running:format=ol:tag=stat.balancer.running|";
    format += "key=stat.disk.iops:format=ol|";
    format += "key=stat.disk.bw:format=of|";
    format += "key=stat.geotag:format=os|";
    format += "key=stat.health:format=os|";
    format += "key=stat.health.redundancy_factor:format=os|";
    format += "key=stat.health.drives_failed:format=os|";
    format += "key=stat.health.drives_total:format=os|";
    format += "key=stat.health.indicator:format=os";
  } else if (option == "io") {
    // io format
    format = "header=1:key=hostport:width=32:format=-s|";
    format += "key=id:width=6:format=s|";
    format += "key=schedgroup:width=16:format=s|";
    format += "key=stat.geotag:width=16:format=s|";
    format += "key=stat.disk.load:width=10:format=f:tag=diskload|";
    format += "key=stat.disk.readratemb:width=12:format=f:tag=diskr-MB/s|";
    format += "key=stat.disk.writeratemb:width=12:format=f:tag=diskw-MB/s|";
    format += "key=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|";
    format += "key=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|";
    format += "key=stat.net.outratemib:width=10:format=l:tag=etho-MiB|";
    format += "key=stat.ropen:width=6:format=l:tag=ropen|";
    format += "key=stat.wopen:width=6:format=l:tag=wopen|";
    format += "compute=usage:width=6:format=f|";
    format += "key=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|";
    format += "key=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|";
    format += "key=stat.usedfiles:width=12:format=+l:tag=used-files|";
    format += "key=stat.statfs.files:width=11:format=+l:tag=max-files|";
    format += "key=stat.balancer.running:width=10:format=l:tag=bal-shd|";
    format += "key=stat.disk.iops:width=6:format=l:tag=iops|";
    format += "key=stat.disk.bw:width=9:format=l:unit=MB:tag=bw";
  } else if (option == "fsck") {
    // filesystem check statistics format
    format = "header=1:key=hostport:width=32:format=-s|";
    format += "key=id:width=6:format=s|";
    format += "key=stat.fsck.mem_n:width=8:format=l:tag=n(mem)|";
    format += "key=stat.fsck.d_sync_n:width=8:format=l:tag=n(disk)|";
    format += "key=stat.fsck.m_sync_n:width=8:format=l:tag=n(mgm)|";
    format += "key=stat.fsck.orphans_n:width=12:format=l:tag=e(orph)|";
    format += "key=stat.fsck.unreg_n:width=12:format=l:tag=e(unreg)|";
    format += "key=stat.fsck.rep_diff_n:width=12:format=l:tag=e(layout)|";
    format += "key=stat.fsck.rep_missing_n:width=12:format=l:tag=e(miss)|";
    format += "key=stat.fsck.d_mem_sz_diff:width=12:format=l:tag=e(disksize)|";
    format += "key=stat.fsck.m_mem_sz_diff:width=12:format=l:tag=e(mgmsize)|";
    format += "key=stat.fsck.d_cx_diff:width=12:format=l:tag=e(disk-cx)|";
    format += "key=stat.fsck.m_cx_diff:width=12:format=l:tag=e(mgm-cx)";
  } else if (option == "d") {
    // drain format
    format = "header=1:key=host:width=24:format=-S:condition=local.drain=!nodrain|";
    format += "key=port:width=4:format=s|";
    format += "key=id:width=6:format=s|";
    format += "key=path:width=32:format=s|";
    format += "key=local.drain:width=12:format=s|";
    format += "key=local.drain.progress:width=12:format=l:tag=progress|";
    format += "key=local.drain.files:width=12:format=+l:tag=files|";
    format += "key=local.drain.bytesleft:width=12:format=+l:tag=bytes-left:unit=B|";
    format += "key=local.drain.timeleft:width=11:format=l:tag=timeleft|";
    format += "key=local.drain.failed:width=12:format=+l:tag=failed";
  } else if (option == "l") {
    // long format
    format = "header=1:key=host:width=24:format=-S|";
    format += "key=port:width=4:format=s|";
    format += "key=id:width=6:format=s|";
    format += "key=uuid:width=36:format=s|";
    format += "key=path:width=32:format=s|";
    format += "key=schedgroup:width=16:format=s|";
    format += "key=headroom:width=10:format=+f|";
    format += "key=stat.boot:width=12:format=s|";
    format += "key=configstatus:width=14:format=s|";
    format += "key=local.drain:width=12:format=s|";
    format += "compute=usage:width=6:format=f|";
    format += "key=stat.active:width=8:format=s|";
    format += "key=scaninterval:width=14:format=s|";
    format += "key=stat.health:width=16:format=s|";
    format += "key=statuscomment:width=24:format=s";
  } else if (option == "e") {
    // error format
    format = "header=1:key=host:width=24:format=-S:condition=stat.errc=!0|";
    format += "key=id:width=6:format=s|";
    format += "key=path:width=32:format=s|";
    format += "key=stat.boot:width=12:format=s|";
    format += "key=configstatus:width=14:format=s|";
    format += "key=local.drain:width=12:format=s|";
    format += "key=stat.errc:width=3:format=s|";
    format += "key=stat.errmsg:width=0:format=s";
  } else {
    // default format
    format = "header=1:key=host:width=24:format=-S|";
    format += "key=port:width=4:format=s|";
    format += "key=id:width=6:format=s|";
    format += "key=path:width=32:format=s|";
    format += "key=schedgroup:width=16:format=s|";
    format += "key=stat.geotag:width=16:format=s|";
    format += "key=stat.boot:width=12:format=s|";
    format += "key=configstatus:width=14:format=s|";
    format += "key=local.drain:width=12:format=s|";
    format += "compute=usage:width=6:format=f|";
    format += "key=stat.active:width=8:format=s|";
    format += "key=stat.health:width=16:format=s";
  }

  return format;
}

//------------------------------------------------------------------------------
// @brief return's the printout format for a given option
// @param option see the implementation for valid options
// @return std;:string with format line passed to the printout routine
//------------------------------------------------------------------------------
std::string
FsView::GetSpaceFormat(std::string option)
{
  std::string format;

  if (option == "m") {
    // monitoring format
    format = "member=type:format=os|";
    format += "member=name:format=os|";
    format += "member=cfg.groupsize:format=ol|";
    format += "member=cfg.groupmod:format=ol|";
    format += "member=nofs:format=ol|";
    format += "avg=stat.disk.load:format=of|";
    format += "sig=stat.disk.load:format=of|";
    format += "sum=stat.disk.readratemb:format=ol|";
    format += "sum=stat.disk.writeratemb:format=ol|";
    format += "sum=stat.net.ethratemib:format=ol|";
    format += "sum=stat.net.inratemib:format=ol|";
    format += "sum=stat.net.outratemib:format=ol|";
    format += "sum=stat.ropen:format=ol|";
    format += "sum=stat.wopen:format=ol|";
    format += "sum=stat.statfs.usedbytes:format=ol|";
    format += "sum=stat.statfs.freebytes:format=ol|";
    format += "sum=stat.statfs.freebytes?configstatus@rw:format=ol|";
    format += "sum=stat.statfs.capacity:format=ol|";
    format += "sum=stat.usedfiles:format=ol|";
    format += "sum=stat.statfs.ffiles:format=ol|";
    format += "sum=stat.statfs.files:format=ol|";
    format += "geosched=totalspace:format=ol:tag=sched.capacity|";
    format += "sum=stat.statfs.capacity?configstatus@rw:format=ol|";
    format += "sum=<n>?configstatus@rw:format=ol|";
    format += "member=cfg.quota:format=os|";
    format += "member=cfg.nominalsize:format=ol|";
    format += "member=cfg.balancer:format=os|";
    format += "member=cfg.balancer.threshold:format=ol|";
    format += "sum=stat.balancer.running:format=ol:tag=stat.balancer.running|";
    format += "sum=stat.disk.iops?configstatus@rw:format=ol|";
    format += "sum=stat.disk.bw?configstatus@rw:format=ol";
  } else if (option == "io") {
    // io format
    format = "header=1:member=name:width=10:format=-s|";
    format += "avg=stat.geotag:width=32:format=-s|";
    format += "avg=stat.disk.load:width=10:format=f:tag=diskload|";
    format += "sum=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|";
    format += "sum=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|";
    format += "sum=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|";
    format += "sum=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|";
    format += "sum=stat.net.outratemib:width=10:format=l:tag=etho-MiB|";
    format += "sum=stat.ropen:width=6:format=l:tag=ropen|";
    format += "sum=stat.wopen:width=6:format=l:tag=wopen|";
    format += "sum=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|";
    format += "sum=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|";
    format += "sum=stat.usedfiles:width=12:format=+l:tag=used-files|";
    format += "sum=stat.statfs.files:width=11:format=+l:tag=max-files|";
    format += "sum=stat.balancer.running:width=10:format=l:tag=bal-shd";
  } else if (option == "fsck") {
    // filesystem check statistics format
    format = "header=1:member=name:width=10:format=-s|";
    format += "avg=stat.geotag:width=32:format=-s|";
    format += "sum=stat.fsck.mem_n:width=8:format=l:tag=n(mem)|";
    format += "sum=stat.fsck.d_sync_n:width=8:format=l:tag=n(disk)|";
    format += "sum=stat.fsck.m_sync_n:width=8:format=l:tag=n(mgm)|";
    format += "sum=stat.fsck.orphans_n:width=12:format=l:tag=e(orph)|";
    format += "sum=stat.fsck.unreg_n:width=12:format=l:tag=e(unreg)|";
    format += "sum=stat.fsck.rep_diff_n:width=12:format=l:tag=e(layout)|";
    format += "sum=stat.fsck.rep_missing_n:width=12:format=l:tag=e(miss)|";
    format += "sum=stat.fsck.d_mem_sz_diff:width=12:format=l:tag=e(disksize)|";
    format += "sum=stat.fsck.m_mem_sz_diff:width=12:format=l:tag=e(mgmsize)|";
    format += "sum=stat.fsck.d_cx_diff:width=12:format=l:tag=e(disk-cx)|";
    format += "sum=stat.fsck.m_cx_diff:width=12:format=l:tag=e(mgm-cx)";
  } else if (option == "l") {
    // long output format
    format = "header=1:member=type:width=10:format=-s|";
    format += "member=name:width=16:format=s|";
    format += "avg=stat.geotag:width=32:format=-s|";
    format += "member=cfg.groupsize:width=12:format=s|";
    format += "member=cfg.groupmod:width=12:format=s|";
    format += "sum=<n>?*@*:width=6:format=l:tag=N(fs)|";
    format += "sum=<n>?configstatus@rw:width=9:format=l:tag=N(fs-rw)|";
    format += "sum=stat.statfs.usedbytes:width=15:format=+l:unit=B|";
    format += "sum=stat.statfs.capacity:width=14:format=+l:unit=B|";
    format += "sum=stat.statfs.capacity?configstatus@rw:width=13:format=+l:tag=capacity(rw):unit=B|";
    format += "member=cfg.nominalsize:width=13:format=+l:tag=nom.capacity:unit=B|";
    format += "member=cfg.quota:width=6:format=s";
  } else {
    // default format
    format = "header=1:member=type:width=10:format=-s|";
    format += "member=name:width=16:format=s|";
    format += "avg=stat.geotag:width=32:format=-s|";
    format += "member=cfg.groupsize:width=12:format=s|";
    format += "member=cfg.groupmod:width=12:format=s|";
    format += "member=nofs:width=6:format=s:tag=N(fs)|";
    format += "sum=<n>?configstatus@rw:width=9:format=l:tag=N(fs-rw)|";
    format += "sum=stat.statfs.usedbytes:width=15:format=+l:unit=B|";
    format += "sum=stat.statfs.capacity:width=14:format=+l:unit=B|";
    format += "sum=stat.statfs.capacity?configstatus@rw:width=13:format=+l:tag=capacity(rw):unit=B|";
    format += "member=cfg.nominalsize:width=13:format=+l:tag=nom.capacity:unit=B|";
    format += "geosched=totalspace:width=14:format=+l:tag=sched.capacity:unit=B|";
    format += "member=cfg.quota:width=6:format=s|";
    format += "member=cfg.balancer:width=10:format=s:tag=balancing|";
    format += "member=cfg.balancer.threshold:width=11:format=+l:tag=threshold|";
    format += "member=cfg.converter:width=11:format=s:tag=converter|";
    format += "member=cfg.converter.ntx:width=6:format=+l:tag=ntx|";
    format += "member=cfg.stat.converter.active:width=8:format=+l:tag=active|";
    format += "member=cfg.wfe:width=11:format=s:tag=wfe|";
    format += "member=cfg.wfe.ntx:width=6:format=+l:tag=ntx|";
    format += "member=cfg.stat.wfe.active:width=8:format=+l:tag=active|";
    format += "member=cfg.groupbalancer:width=11:format=s:tag=intergroup";
  }

  return format;
}

//------------------------------------------------------------------------------
// @brief return's the printout format for a given option
// @param option see the implementation for valid options
// @return std;:string with format line passed to the printout routine
//------------------------------------------------------------------------------
std::string
FsView::GetGroupFormat(std::string option)
{
  std::string format;

  if (option == "m") {
    // monitoring format
    format = "member=type:format=os|";
    format += "member=cfg.status:format=os|";
    format += "member=name:format=os|";
    format += "member=nofs:format=os|";
    format += "avg=stat.disk.load:format=of|";
    format += "sig=stat.disk.load:format=of|";
    format += "sum=stat.disk.readratemb:format=ol|";
    format += "sum=stat.disk.writeratemb:format=ol|";
    format += "sum=stat.net.ethratemib:format=ol|";
    format += "sum=stat.net.inratemib:format=ol|";
    format += "sum=stat.net.outratemib:format=ol|";
    format += "sum=stat.ropen:format=ol|";
    format += "sum=stat.wopen:format=ol|";
    format += "sum=stat.statfs.usedbytes:format=ol|";
    format += "sum=stat.statfs.freebytes:format=ol|";
    format += "sum=stat.statfs.capacity:format=ol|";
    format += "sum=stat.usedfiles:format=ol|";
    format += "sum=stat.statfs.ffree:format=ol|";
    format += "sum=stat.statfs.files:format=ol|";
    format += "maxdev=stat.statfs.filled:format=of|";
    format += "avg=stat.statfs.filled:format=of|";
    format += "sig=stat.statfs.filled:format=of|";
    format += "member=cfg.stat.balancing:format=os:tag=stat.balancing|";
    format += "sum=stat.balancer.running:format=ol:tag=stat.balancer.running|";
  } else if (option == "io") {
    // io format
    format = "header=1:member=name:width=16:format=-s|";
    format += "avg=stat.geotag:width=32:format=s|";
    format += "avg=stat.disk.load:width=10:format=f:tag=diskload|";
    format += "sum=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|";
    format += "sum=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|";
    format += "sum=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|";
    format += "sum=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|";
    format += "sum=stat.net.outratemib:width=10:format=l:tag=etho-MiB|";
    format += "sum=stat.ropen:width=6:format=l:tag=ropen|";
    format += "sum=stat.wopen:width=6:format=l:tag=wopen|";
    format += "sum=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|";
    format += "sum=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|";
    format += "sum=stat.usedfiles:width=12:format=+l:tag=used-files|";
    format += "sum=stat.statfs.files:width=11:format=+l:tag=max-files|";
    format += "sum=stat.balancer.running:width=10:format=l:tag=bal-shd|";
  } else if (option == "l") {
    // long format
    format = "header=1:member=type:width=10:format=-s|";
    format += "member=name:width=16:format=s|";
    format += "member=cfg.status:width=12:format=s|";
    format += "avg=stat.geotag:width=32:format=s|";
    format += "key=stat.geotag:width=16:format=s|";
    format += "sum=<n>?*@*:width=6:format=l:tag=N(fs)";
  } else {
    // default format
    format = "header=1:member=type:width=10:format=-s|";
    format += "member=name:width=16:format=s|";
    format += "member=cfg.status:width=12:format=s|";
    format += "avg=stat.geotag:width=32:format=s|";
    format += "sum=<n>?*@*:width=6:format=l:tag=N(fs)|";
    format += "maxdev=stat.statfs.filled:width=12:format=f|";
    format += "avg=stat.statfs.filled:width=12:format=f|";
    format += "sig=stat.statfs.filled:width=12:format=f|";
    format += "member=cfg.stat.balancing:width=10:format=s|";
    format += "sum=stat.balancer.running:width=10:format=l:tag=bal-shd|";
  }

  return format;
}

//------------------------------------------------------------------------------
// Register a filesystem object in the filesystem view
//------------------------------------------------------------------------------
bool
FsView::Register(FileSystem* fs, const common::FileSystemCoreParams& coreParams,
                 bool registerInGeoTreeEngine)
{
  if (!fs) {
    return false;
  }

  // Check for queuepath collision
  if (mIdView.lookupByQueuePath(coreParams.getQueuePath())) {
    eos_err("msg=\"queuepath already registered\" qpath=\"%s\"",
            coreParams.getQueuePath().c_str());
    return false;
  }

  eos::common::FileSystem::fs_snapshot_t snapshot;

  if (fs->SnapShotFileSystem(snapshot)) {
    // Check if this is already in the view
    if (mIdView.lookupByPtr(fs) != 0) {
      // This filesystem is already there, this might be an update
      eos::common::FileSystem::fsid_t fsid = mIdView.lookupByPtr(fs);

      if (fsid != coreParams.getId()) {
        // Remove previous mapping
        mIdView.eraseById(fsid);
        // Setup new two way mapping
        mIdView.registerFileSystem(coreParams.getLocator(), coreParams.getId(), fs);
        eos_debug("updating mapping %u<=>%lld", coreParams.getId(), fs);
      }
    } else {
      mIdView.registerFileSystem(coreParams.getLocator(), coreParams.getId(), fs);
      eos_debug("registering mapping %u<=>%lld", coreParams.getId(), fs);
    }

    // Align view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
    // Check if we have already a node view
    if (mNodeView.count(coreParams.getFSTQueue())) {
      mNodeView[coreParams.getFSTQueue()]->insert(coreParams.getId());
      eos_debug("inserting into node view %s<=>%u", coreParams.getFSTQueue().c_str(),
                coreParams.getId());
    } else {
      FsNode* node = new FsNode(coreParams.getFSTQueue().c_str());
      mNodeView[coreParams.getFSTQueue()] = node;
      node->insert(coreParams.getId());
      node->SetNodeConfigDefault();
      eos_debug("creating/inserting into node view %s<=>%u",
                coreParams.getFSTQueue().c_str(),
                coreParams.getId());
    }

    // Align view by groupname
    // Check if we have already a group view
    if (mGroupView.count(coreParams.getGroup())) {
      mGroupView[coreParams.getGroup()]->insert(coreParams.getId());
      eos_debug("inserting into group view %s<=>%u", coreParams.getGroup().c_str(),
                coreParams.getId());
    } else {
      FsGroup* group = new FsGroup(coreParams.getGroup().c_str());
      mGroupView[coreParams.getGroup()] = group;
      group->insert(coreParams.getId());
      group->mIndex = coreParams.getGroupLocator().getIndex();
      eos_debug("creating/inserting into group view %s<=>%u",
                coreParams.getGroup().c_str(),
                coreParams.getId());
    }

    if (registerInGeoTreeEngine &&
        !gOFS->mGeoTreeEngine->insertFsIntoGroup(fs, mGroupView[coreParams.getGroup()],
            coreParams)) {
      // Roll back the changes
      if (UnRegister(fs, false)) {
        eos_err("could not insert insert fs %u into GeoTreeEngine : fs was "
                "unregistered and consistency is KEPT between FsView and "
                "GeoTreeEngine", coreParams.getId());
      } else {
        eos_crit("could not insert insert fs %u into GeoTreeEngine : fs could "
                 "not be unregistered and consistency is BROKEN between FsView "
                 "and GeoTreeEngine", coreParams.getId());
      }

      return false;
    }

    mSpaceGroupView[coreParams.getSpace()].insert(
      mGroupView[coreParams.getGroup()]);

    // Align view by spacename
    // Check if we have already a space view
    if (mSpaceView.count(coreParams.getSpace())) {
      mSpaceView[coreParams.getSpace()]->insert(coreParams.getId());
      eos_debug("inserting into space view %s<=>%u %x", coreParams.getSpace().c_str(),
                coreParams.getId(), fs);
    } else {
      FsSpace* space = new FsSpace(coreParams.getSpace().c_str());
      std::string grp_sz = "0";
      std::string grp_mod = "24";

      // Special case of spare space with has size 0 and mod 0
      if (coreParams.getSpace() == eos::common::EOS_SPARE_GROUP) {
        grp_mod = "0";
      }

      // Set new space default parameters
      if ((!space->SetConfigMember(std::string("groupsize"), grp_sz, true)) ||
          (!space->SetConfigMember(std::string("groupmod"), grp_mod, true))) {
        eos_err("failed setting space %s default config values",
                coreParams.getSpace().c_str());
        return false;
      }

      mSpaceView[coreParams.getSpace()] = space;
      space->insert(coreParams.getId());
      eos_debug("creating/inserting into space view %s<=>%u %x",
                coreParams.getSpace().c_str(), coreParams.getId(), fs);
    }
  }

  fs->applyCoreParams(coreParams);
  StoreFsConfig(fs);
  return true;
}

//------------------------------------------------------------------------------
// Store the filesystem configuration in the configuration engine
//------------------------------------------------------------------------------
void
FsView::StoreFsConfig(FileSystem* fs, bool save_config)
{
  if (fs) {
    std::string key, val;
    fs->CreateConfig(key, val);

    if (gOFS->mMaster->IsMaster() && FsView::gFsView.mConfigEngine &&
        !key.empty() && !val.empty()) {
      FsView::gFsView.mConfigEngine->SetConfigValue("fs", key.c_str(), val.c_str(),
          true, save_config);
    }
  }
}

//------------------------------------------------------------------------------
// Move a filesystem in to a target group
//------------------------------------------------------------------------------
bool
FsView::MoveGroup(FileSystem* fs, std::string group)
{
  if (!fs) {
    return false;
  }

  eos::common::FileSystem::fs_snapshot_t snapshot1;
  eos::common::FileSystem::fs_snapshot_t snapshot;

  if (fs->SnapShotFileSystem(snapshot1)) {
    fs->SetString("schedgroup", group.c_str());
    FsGroup* oldgroup = mGroupView.count(snapshot1.mGroup) ?
                        mGroupView[snapshot1.mGroup] : NULL;

    if (fs->SnapShotFileSystem(snapshot)) {
      // Remove from the original space
      if (mSpaceView.count(snapshot1.mSpace)) {
        FsSpace* space = mSpaceView[snapshot1.mSpace];
        space->erase(snapshot1.mId);
        eos_debug("unregister space %s from space view",
                  space->GetMember("name").c_str());

        if (!space->size()) {
          mSpaceView.erase(snapshot1.mSpace);
          delete space;
        }
      }

      // Remove from the original group
      if (mGroupView.count(snapshot1.mGroup)) {
        FsGroup* group = mGroupView[snapshot1.mGroup];

        if (!gOFS->mGeoTreeEngine->removeFsFromGroup(fs, group, false)) {
          // roll-back
          if (mSpaceView.count(snapshot1.mSpace)) {
            mSpaceView[snapshot1.mSpace]->insert(snapshot1.mId);
            eos_debug("inserting into space view %s<=>%u %x",
                      snapshot1.mSpace.c_str(), snapshot1.mId, fs);
          } else {
            FsSpace* space = new FsSpace(snapshot1.mSpace.c_str());
            mSpaceView[snapshot1.mSpace] = space;
            space->insert(snapshot1.mId);
            eos_debug("creating/inserting into space view %s<=>%u %x",
                      snapshot1.mSpace.c_str(), snapshot1.mId, fs);
          }

          eos_err("could not remove fs %u from GeoTreeEngine : fs was "
                  "registered back and consistency is KEPT between FsView"
                  " and GeoTreeEngine", snapshot.mId);
          return false;
        }

        group->erase(snapshot1.mId);
        eos_debug("unregister group %s from group view",
                  group->GetMember("name").c_str());

        if (!group->size()) {
          if (mSpaceGroupView.count(snapshot1.mSpace)) {
            mSpaceGroupView[snapshot1.mSpace].erase(mGroupView[snapshot1.mGroup]);
          }

          mGroupView.erase(snapshot1.mGroup);
          delete group;
        }
      }

      // Check if we have already a group view
      if (mGroupView.count(snapshot.mGroup)) {
        mGroupView[snapshot.mGroup]->insert(snapshot.mId);
        eos_debug("inserting into group view %s<=>%u",
                  snapshot.mGroup.c_str(), snapshot.mId, fs);
      } else {
        FsGroup* group = new FsGroup(snapshot.mGroup.c_str());
        mGroupView[snapshot.mGroup] = group;
        group->insert(snapshot.mId);
        group->mIndex = snapshot.mGroupIndex;
        group->SetConfigMember("status", "on");
        eos_debug("creating/inserting into group view %s<=>%u",
                  snapshot.mGroup.c_str(), snapshot.mId, fs);
      }

      if (!gOFS->mGeoTreeEngine->insertFsIntoGroup(fs, mGroupView[group],
          fs->getCoreParams())) {
        if (fs->SetString("schedgroup", group.c_str()) && UnRegister(fs, false)) {
          if (oldgroup && fs->SetString("schedgroup", oldgroup->mName.c_str()) &&
              Register(fs, fs->getCoreParams())) {
            eos_err("while moving fs, could not insert fs %u in group %s. fs "
                    "was registered back to group %s and consistency is KEPT "
                    "between FsView and GeoTreeEngine",
                    snapshot.mId, mGroupView[group]->mName.c_str(),
                    oldgroup->mName.c_str());
          } else {
            eos_err("while moving fs, could not insert fs %u in group %s. fs "
                    "was unregistered and consistency is KEPT between FsView "
                    "and GeoTreeEngine", snapshot.mId, mGroupView[group]->mName.c_str());
          }
        } else {
          eos_crit("while moving fs, could not insert fs %u in group %s. fs "
                   "could not be unregistered and consistency is BROKEN between "
                   "FsView and GeoTreeEngine", snapshot.mId, mGroupView[group]->mName.c_str());
        }

        return false;
      }

      mSpaceGroupView[snapshot.mSpace].insert(mGroupView[snapshot.mGroup]);

      // Check if we have already a space view
      if (mSpaceView.count(snapshot.mSpace)) {
        mSpaceView[snapshot.mSpace]->insert(snapshot.mId);
        eos_debug("inserting into space view %s<=>%u %x",
                  snapshot.mSpace.c_str(), snapshot.mId, fs);
      } else {
        FsSpace* space = new FsSpace(snapshot.mSpace.c_str());
        mSpaceView[snapshot.mSpace] = space;
        space->insert(snapshot.mId);
        eos_debug("creating/inserting into space view %s<=>%u %x",
                  snapshot.mSpace.c_str(), snapshot.mId, fs);
      }

      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Remove a file system
//------------------------------------------------------------------------------
bool
FsView::UnRegister(FileSystem* fs, bool unreg_from_geo_tree,
                   bool notify_fst)
{
  if (!fs) {
    return false;
  }

  // Delete in the configuration engine
  std::string key = fs->GetQueuePath();

  if (gOFS->mMaster->IsMaster() && FsView::gFsView.mConfigEngine) {
    FsView::gFsView.mConfigEngine->DeleteConfigValue("fs", key.c_str());
  }

  eos::common::FileSystem::fs_snapshot_t snapshot;

  if (fs->SnapShotFileSystem(snapshot)) {
    // Remove fs from node view & evt. remove node view
    if (mNodeView.count(snapshot.mQueue)) {
      FsNode* node = mNodeView[snapshot.mQueue];
      node->erase(snapshot.mId);

      if (node->size() == 0) {
        eos_debug("unregister node %s from node view", node->GetMember("name").c_str());
        mNodeView.erase(snapshot.mQueue);
        delete node;
      }
    }

    // Remove fs from group view & evt. remove group view
    if (mGroupView.count(snapshot.mGroup)) {
      FsGroup* group = mGroupView[snapshot.mGroup];

      if (unreg_from_geo_tree
          && !gOFS->mGeoTreeEngine->removeFsFromGroup(fs, group, false)) {
        if (Register(fs, fs->getCoreParams(), false)) {
          eos_err("could not remove fs %u from GeoTreeEngine : fs was "
                  "registered back and consistency is KEPT between FsView "
                  "and GeoTreeEngine", snapshot.mId);
        } else {
          eos_crit("could not remove fs %u from GeoTreeEngine : fs could not "
                   "be registered back and consistency is BROKEN between "
                   "FsView and GeoTreeEngine", snapshot.mId);
        }

        return false;
      }

      group->erase(snapshot.mId);
      eos_debug("msg=\"unregister group %s from group view\"",
                group->GetMember("name").c_str());

      if (!group->size()) {
        mSpaceGroupView[snapshot.mSpace].erase(mGroupView[snapshot.mGroup]);
        mGroupView.erase(snapshot.mGroup);
        delete group;
      }
    }

    // Remove fs from space view & evt. remove space view
    if (mSpaceView.count(snapshot.mSpace)) {
      FsSpace* space = mSpaceView[snapshot.mSpace];
      space->erase(snapshot.mId);
      eos_debug("msg=\"unregister space %s from space view\"",
                space->GetMember("name").c_str());

      if (!space->size()) {
        mSpaceView.erase(snapshot.mSpace);
        delete space;
      }
    }

    // Remove view by filesystem object and filesystem id
    if (!mIdView.eraseByPtr(fs)) {
      eos_static_crit("msg=\"no such file system to unregister\" ptr=%x fsid=%lu",
                      fs,  snapshot.mId);
    }

    // Remove mapping
    RemoveMapping(snapshot.mId, snapshot.mUuid);

    // Notify the FST to delete the fs object from local maps
    if (notify_fst) {
      fs->DeleteSharedHash();
    }

    delete fs;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Checks if a node has already a filesystem registered
//------------------------------------------------------------------------------
bool
FsView::ExistsQueue(std::string queue, std::string queuepath)
{
  if (mNodeView.count(queue)) {
    // Loop over all attached filesystems and compare the queue path
    for (auto it = mNodeView[queue]->begin(); it != mNodeView[queue]->end(); ++it) {
      FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

      if (fs && (fs->GetQueuePath() == queuepath)) {
        // This queuepath exists already, we cannot register
        return true;
      }
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Add view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
//------------------------------------------------------------------------------
bool
FsView::RegisterNode(const char* nodename)
{
  std::string nodequeue = nodename;

  if (mNodeView.count(nodequeue)) {
    eos_debug("msg=\"node already exists\" info=\"%s\"", nodequeue.c_str());
    return false;
  } else {
    FsNode* node = new FsNode(nodequeue.c_str());
    mNodeView[nodequeue] = node;
    node->SetNodeConfigDefault();
    eos_debug("msg=\"creating node\" info=\"%s\"", nodequeue.c_str());
    return true;
  }
}

//------------------------------------------------------------------------------
// Remove view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst - we have
// to remove all the connected filesystems via UnRegister(fs) to keep the
// space, group and node views in sync.
//------------------------------------------------------------------------------
bool
FsView::UnRegisterNode(const char* nodename)
{
  bool retc = true;
  bool has_fs = false;

  if (mNodeView.count(nodename)) {
    while (mNodeView.count(nodename) &&
           (mNodeView[nodename]->begin() != mNodeView[nodename]->end())) {
      eos::common::FileSystem::fsid_t fsid = *(mNodeView[nodename]->begin());
      FileSystem* fs = mIdView.lookupByID(fsid);

      if (fs) {
        has_fs = true;
        eos_static_debug("Unregister filesystem fsid=%llu node=%s queue=%s",
                         (unsigned long long) fsid, nodename, fs->GetQueue().c_str());
        retc |= UnRegister(fs);
      }
    }

    // Explicitly remove the node from the view here because no fs was removed
    if (!has_fs) {
      delete mNodeView[nodename];
      retc = (mNodeView.erase(nodename) ? true : false);
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Add view by spacename (= MQ queue) e.g. /eos/<host>:<port>/fst
//------------------------------------------------------------------------------
bool
FsView::RegisterSpace(const char* spacename)
{
  std::string spacequeue = spacename;

  if (mSpaceView.count(spacequeue)) {
    eos_debug("space is existing");
    return false;
  } else {
    FsSpace* space = new FsSpace(spacequeue.c_str());
    mSpaceView[spacequeue] = space;
    eos_debug("creating space view %s", spacequeue.c_str());
    return true;
  }
}

//------------------------------------------------------------------------------
// Remove view by spacename (= MQ queue) e.g. /eos/<host>:<port>/fst
//------------------------------------------------------------------------------
bool
FsView::UnRegisterSpace(const char* spacename)
{
  // We have to remove all the connected filesystems via UnRegister(fs) to keep
  // space, group and fs views in sync
  bool retc = true;
  bool has_fs = false;

  if (mSpaceView.count(spacename)) {
    while (mSpaceView.count(spacename) && mSpaceView[spacename]->size()) {
      eos::common::FileSystem::fsid_t fsid = *(mSpaceView[spacename]->begin());
      FileSystem* fs = mIdView.lookupByID(fsid);

      if (fs) {
        has_fs = true;
        eos_static_debug("Unregister filesystem fsid=%llu space=%s queue=%s",
                         (unsigned long long) fsid, spacename, fs->GetQueue().c_str());
        retc |= UnRegister(fs);
      }

      if (mSpaceView.count(spacename) == 0) {
        return true;
      }
    }

    if (!has_fs) {
      // We have to explicitly remove the space from the view here because no
      // fs was removed
      if (mSpaceView.count(spacename)) {
        delete mSpaceView[spacename];
        retc = (mSpaceView.erase(spacename) ? true : false);
      }
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Add view by groupname  e.g. default or default.0
//------------------------------------------------------------------------------
bool
FsView::RegisterGroup(const char* groupname)
{
  std::string groupqueue = groupname;

  if (mGroupView.count(groupqueue)) {
    eos_debug("group is existing");
    return false;
  } else {
    FsGroup* group = new FsGroup(groupqueue.c_str());
    mGroupView[groupqueue] = group;
    eos_debug("creating group view %s", groupqueue.c_str());
    return true;
  }
}

//------------------------------------------------------------------------------
// Remove view by groupname e.g. default or default.0
//------------------------------------------------------------------------------
bool
FsView::UnRegisterGroup(const char* groupname)
{
  // We have to remove all the connected filesystems via UnRegister(fs) to keep
  // the group view in sync.
  bool retc = true;
  bool has_fs = false;

  if (mGroupView.count(groupname)) {
    while (mGroupView.count(groupname) &&
           (mGroupView[groupname]->begin() != mGroupView[groupname]->end())) {
      eos::common::FileSystem::fsid_t fsid = *(mGroupView[groupname]->begin());
      FileSystem* fs = mIdView.lookupByID(fsid);

      if (fs) {
        has_fs = true;
        eos_static_debug("Unregister filesystem fsid=%llu group=%s queue=%s",
                         (unsigned long long) fsid, groupname, fs->GetQueue().c_str());
        retc |= UnRegister(fs);
      }
    }

    if (!has_fs) {
      std::string sgroupname = groupname;
      std::string spacename = "";
      std::string index = "";

      // remove the direct group reference here
      if (mSpaceGroupView.count(spacename)) {
        mSpaceGroupView[spacename].erase(mGroupView[groupname]);
      }

      // We have to explicitly remove the group from the view here because no
      // fs was removed
      delete mGroupView[groupname];
      retc = (mGroupView.erase(groupname) ? true : false);
      eos::common::StringConversion::SplitByPoint(groupname, spacename, index);
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Remove all filesystems by erasing all spaces
//------------------------------------------------------------------------------
void
FsView::Reset()
{
  {
    eos::common::RWMutexReadLock viewlock(ViewMutex);

    // Stop all the threads while taking only the read lock
    for (auto it = mSpaceView.begin(); it != mSpaceView.end(); ++it) {
      it->second->Stop();
    }
  }
  eos::common::RWMutexWriteLock viewlock(ViewMutex);

  while (mSpaceView.size()) {
    std::string name = mSpaceView.begin()->first;
    UnRegisterSpace(name.c_str());
  }

  // Remove all mappings
  mFilesystemMapper.clear();
  // Although this shouldn't be necessary, better run an additional cleanup
  mSpaceView.clear();
  mGroupView.clear();
  mNodeView.clear();
  {
    eos::common::RWMutexWriteLock gwlock(GwMutex);
    mGwNodes.clear();
  }
  mIdView.clear();
}


//------------------------------------------------------------------------------
// Clear all maps and delete all filesystem/group/space objects
//------------------------------------------------------------------------------
void
FsView::Clear()
{
  {
    eos::common::RWMutexReadLock rd_view_lock(ViewMutex);

    // Stop all the threads while taking only thre read lock
    for (auto it = mSpaceView.begin(); it != mSpaceView.end(); it++) {
      it->second->Stop();
    }
  }
  eos::common::RWMutexWriteLock wr_view_lock(ViewMutex);

  while (mSpaceView.size()) {
    UnRegisterSpace(mSpaceView.begin()->first.c_str());
  }

  mFilesystemMapper.clear();
  {
    // Remove all gateway nodes
    eos::common::RWMutexWriteLock wr_gw_lock(GwMutex);
    mGwNodes.clear();
  }
  mSpaceView.clear();
  mGroupView.clear();
  mNodeView.clear();
  mIdView.clear();
}

//------------------------------------------------------------------------------
// Find a filesystem specifying a queuepath
//------------------------------------------------------------------------------
FileSystem*
FsView::FindByQueuePath(std::string& queuepath)
{
  // Needs an external ViewMutex lock !!!!
  for (auto it = mIdView.begin(); it != mIdView.end(); ++it) {
    if (it->second && it->second->GetQueuePath() == queuepath) {
      return it->second;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Set global config
//------------------------------------------------------------------------------
bool
FsView::SetGlobalConfig(const std::string& key, const std::string& value)
{
  if (gOFS != NULL) {
    std::string ckey = SSTR(common::InstanceName::getGlobalMgmConfigQueue()
                            << "#" << key);

    if (value.empty()) {
      mq::SharedHashWrapper::makeGlobalMgmHash(gOFS->mMessagingRealm.get()).del(key);
    } else {
      mq::SharedHashWrapper::makeGlobalMgmHash(gOFS->mMessagingRealm.get()).set(key,
          value);
    }

    if (FsView::gFsView.mConfigEngine) {
      if (value.empty()) {
        FsView::gFsView.mConfigEngine->DeleteConfigValue("global", ckey.c_str());
      } else {
        FsView::gFsView.mConfigEngine->SetConfigValue("global", ckey.c_str(),
            value.c_str());
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Get global config
//------------------------------------------------------------------------------
std::string
FsView::GetGlobalConfig(const std::string& key)
{
  if (gOFS != NULL) {
    return mq::SharedHashWrapper::makeGlobalMgmHash(
             gOFS->mMessagingRealm.get()).get(key);
  }

  return "";
}

//------------------------------------------------------------------------------
// Heart beat checker set's filesystem to down if the heart beat is missing
//------------------------------------------------------------------------------
void
FsView::HeartBeatCheck(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(10));
    eos::common::RWMutexReadLock lock(ViewMutex);

    // Loop over all the nodes and update their status
    for (auto it_node = mNodeView.begin();
         it_node != mNodeView.end(); ++it_node) {
      if (it_node->second == nullptr) {
        continue;
      }

      auto* node = it_node->second;

      if (node->HasHeartbeat()) {
        if (node->GetActiveStatus() != eos::common::ActiveStatus::kOnline) {
          node->SetActiveStatus(eos::common::ActiveStatus::kOnline);
        }

        // Loop over all files sysytems in the current node and update status
        for (auto it_fsid = node->begin(); it_fsid != node->end(); ++it_fsid) {
          FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it_fsid);

          if (fs == nullptr) {
            continue;
          }

          std::string group = fs->GetString("schedgroup");

          if ((node->GetConfigMember("status") == "on") &&
              FsView::gFsView.mGroupView.count(group) &&
              (FsView::gFsView.mGroupView[group]->GetConfigMember("status") == "on")) {
            ssize_t max_ropen = fs->GetLongLong("max.ropen");
            ssize_t max_wopen = fs->GetLongLong("max.wopen");
            bool overloaded = ((max_ropen &&
                                (max_ropen <= fs->GetLongLong("stat.ropen"))) ||
                               (max_wopen && (max_wopen <= fs->GetLongLong("stat.wopen"))));

            if (!overloaded) {
              if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOnline) {
                fs->SetActiveStatus(eos::common::ActiveStatus::kOnline);
              }
            } else {
              if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOverload) {
                fs->SetActiveStatus(eos::common::ActiveStatus::kOverload);
              }
            }
          } else {
            if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOffline) {
              fs->SetActiveStatus(eos::common::ActiveStatus::kOffline);
            }
          }
        }
      } else {
        if (node->GetActiveStatus() != eos::common::ActiveStatus::kOffline) {
          node->SetActiveStatus(eos::common::ActiveStatus::kOffline);
        }

        // Loop over all files sysytems in the current node and update status
        for (auto it_fsid = node->begin(); it_fsid != node->end(); ++it_fsid) {
          FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it_fsid);

          if (fs == nullptr) {
            continue;
          }

          if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOffline) {
            fs->SetActiveStatus(eos::common::ActiveStatus::kOffline);
          }
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Re-apply drain status for file systems to re-trigger draining
//------------------------------------------------------------------------------
void
FsView::ReapplyDrainStatus()
{
  eos::common::RWMutexReadLock fs_rd_lock(ViewMutex);

  for (auto it = mIdView.begin(); it != mIdView.end(); ++it) {
    eos::common::ConfigStatus cs = it->second->GetConfigStatus();

    if ((cs == eos::common::ConfigStatus::kDrain) ||
        (cs == eos::common::ConfigStatus::kDrainDead) ||
        (cs == eos::common::ConfigStatus::kGroupDrain)) {
      it->second->SetConfigStatus(cs);
    }
  }
}

//------------------------------------------------------------------------------
// Return a view member variable
//------------------------------------------------------------------------------
std::string
BaseView::GetMember(const std::string& member) const
{
  if (member == "name") {
    return mName;
  }

  if (member == "type") {
    return mType;
  }

  if (member == "nofs") {
    char line[1024];
    snprintf(line, sizeof(line) - 1, "%llu", (unsigned long long) size());
    return std::string(line);
  }

  if (member == "inqueue") {
    XrdOucString s = "";
    s += (int) mInQueue;
    return s.c_str();
  }

  if (member == "heartbeat") {
    char line[1024];
    snprintf(line, sizeof(line) - 1, "%llu", (unsigned long long) mHeartBeat);
    return std::string(line);
  }

  if (member == "heartbeatdelta") {
    char line[1024];

    if (labs(time(NULL) - mHeartBeat) > 86400) {
      snprintf(line, sizeof(line) - 1, "~");
    } else {
      snprintf(line, sizeof(line) - 1, "%llu",
               (unsigned long long)(time(NULL) - mHeartBeat));
    }

    return std::string(line);
  }

  if (member == "status") {
    return mStatus;
  }

  // Check for global config value
  const std::string tag = "cfg.";

  if (member.find(tag) == 0) {
    std::string cfg_member = member;
    std::string val = "???";
    cfg_member.erase(0, tag.length());
    std::string value = GetConfigMember(cfg_member);

    if (!value.empty()) {
      val = value;
    }

    // It's otherwise hard to get the default into place
    if ((member == "cfg.stat.balancing") && ((val == "") || (val == "???"))) {
      val = "idle";
    }

    if ((member == "cfg.status") && val.empty()) {
      val = "off";
    }

    return val;
  }

  return "";
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsNode::FsNode(const char* name) : BaseView(
    common::SharedHashLocator::makeForNode(name))
{
  mName = name;
  mType = "nodesview";
  SetConfigMember("stat.hostport", GetMember("hostport"), false);
  mGwQueue = new eos::common::TransferQueue(
    eos::common::TransferQueueLocator(mName, "txq"),
    gOFS->mMessagingRealm.get(), false);
  eos_static_info("msg=\"FsNode constructor\" name=\"%s\" ptr=%p",
                  mName.c_str(), this);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FsNode::~FsNode()
{
  if (mGwQueue) {
    delete mGwQueue;
  }

  FsView::gFsView.mGwNodes.erase(mName); // unregister evt. gateway node
  eos_static_info("msg=\"FsNode destructor\" name=\"%s\" ptr=%p",
                  mName.c_str(), this);
}

//------------------------------------------------------------------------------
// Set the configuration default values for a node
//------------------------------------------------------------------------------
void
FsNode::SetNodeConfigDefault()
{
  eos_static_info("msg=\"set defaults\" node=%s", mName.c_str());

  // Define the manager ID
  if (!(GetConfigMember("manager").length())) {
    SetConfigMember("manager", gOFS->mMaster->GetMasterId(), true);
  }

  // By default set 2 balancing streams per node
  if (!(GetConfigMember("stat.balance.ntx").length())) {
    SetConfigMember("stat.balance.ntx", "2", true);
  }

  // By default set 25 MB/s stream balancing rate
  if (!(GetConfigMember("stat.balance.rate").length())) {
    SetConfigMember("stat.balance.rate", "25", true);
  }

  // Set the default sym key from the sym key store
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  // Store the sym key as configuration member
  if (!(GetConfigMember("symkey").length())) {
    SetConfigMember("symkey", symkey->GetKey64(), true);
  }

  // Set the default debug level to notice
  if (!(GetConfigMember("debug.level").length())) {
    SetConfigMember("debug.level", "info", true);
  }

  // Set by default as no transfer gateway
  if ((GetConfigMember("txgw") != "on") && (GetConfigMember("txgw") != "off")) {
    SetConfigMember("txgw", "off", true);
  }

  // set by default 10 transfers per gateway node
  if ((strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == 0) ||
      (strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == LONG_MAX)) {
    SetConfigMember("gw.ntx", "10", true);
  }

  // Set by default the gateway stream transfer speed to 120 Mb/s
  if ((strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == 0) ||
      (strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == LONG_MAX)) {
    SetConfigMember("gw.rate", "120", true);
  }

  // Set by default the MGM domain e.g. same geographical position as the MGM
  if (!(GetConfigMember("domain").length())) {
    SetConfigMember("domain", "MGM", true);
  }
}

//------------------------------------------------------------------------------
// Get member
//------------------------------------------------------------------------------
std::string
FsNode::GetMember(const std::string& member) const
{
  if (member == "hostport") {
    std::string hostport =
      eos::common::StringConversion::GetStringHostPortFromQueue(mName.c_str());
    return hostport;
  } else {
    return BaseView::GetMember(member);
  }
}

//------------------------------------------------------------------------------
// Get node active status
//------------------------------------------------------------------------------
eos::common::ActiveStatus
FsNode::GetActiveStatus()
{
  if (GetStatus() == "online") {
    return eos::common::ActiveStatus::kOnline;
  } else {
    return eos::common::ActiveStatus::kOffline;
  }
}

//------------------------------------------------------------------------------
// Set node active status
//------------------------------------------------------------------------------
bool
FsNode::SetActiveStatus(eos::common::ActiveStatus active)
{
  if (active == eos::common::ActiveStatus::kOnline) {
    SetStatus("online");
    return SetConfigMember("stat.active", "online", true);
  } else {
    SetStatus("offline");
    return SetConfigMember("stat.active", "offline", true);
  }
}

//------------------------------------------------------------------------------
// Check if node has a recent enough heartbeat ie. less then 60 seconds
//------------------------------------------------------------------------------
bool FsNode::HasHeartbeat() const
{
  if (mHeartBeat == 0) {
    return false;
  }

  return isHeartbeatRecent(mHeartBeat.load());
}

//------------------------------------------------------------------------------
// Set a configuration member variable (stored in the config engine)
// If 'isstatus'=true we just store the value in the shared hash but don't flush
// it into the configuration engine.
// => is used to set status variables on config queues (baseview queues)
//------------------------------------------------------------------------------
bool
BaseView::SetConfigMember(std::string key, std::string value,
                          bool isstatus)
{
  bool success = mq::SharedHashWrapper(gOFS->mMessagingRealm.get(),
                                       mLocator).set(key, value);

  if (key == "txgw") {
    eos::common::RWMutexWriteLock gwlock(FsView::gFsView.GwMutex);

    if (value == "on") {
      // we have to register this queue into the gw set for fast lookups
      FsView::gFsView.mGwNodes.insert(mLocator.getBroadcastQueue());
      // clear the queue if a machine is enabled
      // @todo (esindril): Clear also takes the HashMutex lock again - this
      // is undefined behaviour !!!
      FsView::gFsView.mNodeView[mLocator.getBroadcastQueue()]->mGwQueue->Clear();
    } else {
      FsView::gFsView.mGwNodes.erase(mLocator.getBroadcastQueue());
    }
  }

  // Register in the configuration engine
  if (gOFS->mMaster->IsMaster() && (!isstatus) && FsView::gFsView.mConfigEngine) {
    std::string node_cfg_name = mLocator.getConfigQueue();
    node_cfg_name += "#";
    node_cfg_name += key;
    std::string confval = value;
    FsView::gFsView.mConfigEngine->SetConfigValue("global", node_cfg_name.c_str(),
        confval.c_str());
  }

  return success;
}

//------------------------------------------------------------------------------
// Get a configuration member variable (stored in the config engine)
//------------------------------------------------------------------------------
std::string
BaseView::GetConfigMember(std::string key) const
{
  return mq::SharedHashWrapper(gOFS->mMessagingRealm.get(), mLocator).get(key);
}

//------------------------------------------------------------------------------
// Delete a configuration member variable (stored in the config engine)
//------------------------------------------------------------------------------
bool
BaseView::DeleteConfigMember(std::string key) const
{
  bool deleted = mq::SharedHashWrapper(gOFS->mMessagingRealm.get(),
                                       mLocator).del(key);

  // Delete in the configuration engine
  if (gOFS->mMaster->IsMaster() && FsView::gFsView.mConfigEngine) {
    std::string node_cfg_name = mLocator.getConfigQueue();
    node_cfg_name += "#";
    node_cfg_name += key;
    FsView::gFsView.mConfigEngine->DeleteConfigValue("global",
        node_cfg_name.c_str());
  }

  return deleted;
}

//------------------------------------------------------------------------------
// GetConfigKeys
//------------------------------------------------------------------------------
bool
BaseView::GetConfigKeys(std::vector<std::string>& keys)
{
  return mq::SharedHashWrapper(gOFS->mMessagingRealm.get(),
                               mLocator).getKeys(keys);
}

//------------------------------------------------------------------------------
// Class ConfigResetMonitor
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConfigResetMonitor::ConfigResetMonitor():
  mOrigConfEngine(nullptr)
{
  std::swap(mOrigConfEngine, FsView::gFsView.mConfigEngine);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ConfigResetMonitor::~ConfigResetMonitor()
{
  if (mOrigConfEngine == nullptr) {
    FsView::gFsView.mConfigEngine = gOFS->ConfEngine;
  } else {
    std::swap(FsView::gFsView.mConfigEngine, mOrigConfEngine);
  }
}

//------------------------------------------------------------------------------
// Class FsView
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Creates a new filesystem id based on a uuid
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
FsView::CreateMapping(std::string fsuuid)
{
  return mFilesystemMapper.allocate(fsuuid);
}

//------------------------------------------------------------------------------
// Adds a fsid=uuid pair to the mapping
//------------------------------------------------------------------------------
bool
FsView::ProvideMapping(std::string fsuuid, eos::common::FileSystem::fsid_t fsid)
{
  return mFilesystemMapper.injectMapping(fsid, fsuuid);
}

//------------------------------------------------------------------------------
// Return a fsid for a uuid
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
FsView::GetMapping(std::string fsuuid)
{
  return mFilesystemMapper.lookup(fsuuid);
}

//------------------------------------------------------------------------------
// Removes a mapping entry by fsid
//------------------------------------------------------------------------------
bool
FsView::RemoveMapping(eos::common::FileSystem::fsid_t fsid)
{
  return mFilesystemMapper.remove(fsid);
}

//------------------------------------------------------------------------------
// Removes a mapping entry by providing fsid + uuid
//------------------------------------------------------------------------------
bool
FsView::RemoveMapping(eos::common::FileSystem::fsid_t fsid, std::string fsuuid)
{
  return mFilesystemMapper.remove(fsid) | mFilesystemMapper.remove(fsuuid);
}

//------------------------------------------------------------------------------
// Print space information
//------------------------------------------------------------------------------
void
FsView::PrintSpaces(std::string& out, const std::string& table_format,
                    const std::string& table_mq_format, unsigned int outdepth,
                    const char* selection, const std::string& filter, const bool dont_color)
{
  std::vector<std::string> selections;
  std::string selected = selection ? selection : "";

  if (selection) {
    eos::common::StringConversion::Tokenize(selected, selections, ",");
  }

  TableFormatterBase table(dont_color);

  for (auto it = mSpaceView.begin(); it != mSpaceView.end(); ++it) {
    it->second->Print(table, table_format, table_mq_format, outdepth, filter,
                      dont_color);
  }

  out = table.GenerateTable(HEADER, selections);
}

//----------------------------------------------------------------------------
// Print group information
//----------------------------------------------------------------------------
void
FsView::PrintGroups(std::string& out, const std::string& table_format,
                    const std::string& table_mq_format, unsigned int outdepth,
                    const char* selection, const bool dont_color)
{
  std::vector<std::string> selections;
  std::string selected = selection ? selection : "";

  if (selection) {
    eos::common::StringConversion::Tokenize(selected, selections, ",");
  }

  TableFormatterBase table(dont_color);

  for (auto it = mGroupView.begin(); it != mGroupView.end(); ++it) {
    it->second->Print(table, table_format, table_mq_format, outdepth,
                      std::string(""), dont_color);
  }

  out = table.GenerateTable(HEADER, selections);
}

//------------------------------------------------------------------------------
// Print node information
//------------------------------------------------------------------------------
void
FsView::PrintNodes(std::string& out, const std::string& table_format,
                   const std::string& table_mq_format, unsigned int outdepth,
                   const char* selection, const bool dont_color)
{
  std::vector<std::string> selections;
  std::string selected = selection ? selection : "";

  if (selection) {
    eos::common::StringConversion::Tokenize(selected, selections, ",");
  }

  TableFormatterBase table(dont_color);

  for (auto it = mNodeView.begin(); it != mNodeView.end(); ++it) {
    it->second->Print(table, table_format, table_mq_format, outdepth,
                      std::string(""), dont_color);
  }

  out = table.GenerateTable(HEADER, selections);
}

//------------------------------------------------------------------------------
// Converts a config engine definition for a filesystem into the FsView
// representation.
// @note This method needs to be called with the ViewMutex locked for write
//------------------------------------------------------------------------------
bool
FsView::ApplyFsConfig(const char* inkey, const std::string& val,
                      bool first_unregister)
{
  std::map<std::string, std::string> configmap;

  if (!common::ConfigParsing::parseFilesystemConfig(val, configmap)) {
    eos_err("msg=\"failed parsing fs config entry\" data=\"%s\"",
            val.c_str());
    return false;
  }

  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(configmap["queuepath"],
      locator)) {
    eos_crit("msg=\"failed parsing queuepath: %s", configmap["queuepath"].c_str());
    return false;
  }

  const auto it = configmap.find("id");

  if (it == configmap.end()) {
    eos_static_err("msg=\"missing id from fs config entry\" value=\"%s\"",
                   val.c_str());
    return false;
  }

  eos::common::FileSystem::fsid_t fsid =
    eos::common::FileSystem::ConvertToFsid(it->second);

  if (fsid == 0ul) {
    eos_static_err("msg=\"no such fsid 0\" value=\"%s\"", it->second.c_str());
    return false;
  }

  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

  if (first_unregister && fs) {
    if (!UnRegister(fs)) {
      eos_static_warning("msg=\"failed to unregister file system\" fsid=%lu",
                         fsid);
    }

    fs = nullptr;
  }

  // Apply only the registration for a new filesystem if it does not exist
  if (fs == nullptr) {
    fs = new FileSystem(locator, gOFS->mMessagingRealm.get());
  }

  common::FileSystemUpdateBatch batch;
  batch.setId(fsid);
  batch.setStringDurable("uuid", configmap["uuid"]);

  for (auto it = configmap.begin(); it != configmap.end(); it++) {
    // Set config parameters except for the "configstatus" which can trigger a
    // drain job. This in turn could try to update the status of the file
    // system and will deadlock trying to get the transaction mutex. Therefore,
    // we update the configstatus outside this transaction.
    if (it->first != "configstatus") {
      batch.setStringDurable(it->first, it->second);
    }
  }

  fs->applyBatch(batch);
  auto it_cfg = configmap.find("configstatus");

  if (it_cfg != configmap.end()) {
    fs->SetString(it_cfg->first.c_str(), it_cfg->second.c_str());
  }

  if (!Register(fs, fs->getCoreParams())) {
    eos_err("msg=\"cannot register filesystem name=%s from configuration\"",
            configmap["queuepath"].c_str());
    return false;
  }

  // insert into the mapping
  FsView::gFsView.ProvideMapping(configmap["uuid"], fsid);
  return true;
}

//------------------------------------------------------------------------------
// Converts a config engine definition of a global variable into the FsView
// representation.
//------------------------------------------------------------------------------
bool
FsView::ApplyGlobalConfig(const char* key, std::string& val)
{
  // global variables are stored like key='<queuename>:<variable>' val='<val>'
  std::string configqueue = key;
  std::vector<std::string> tokens;
  std::vector<std::string> paths;
  std::string delimiter = "#";
  std::string pathdelimiter = "/";
  eos::common::StringConversion::Tokenize(configqueue, tokens, delimiter);
  eos::common::StringConversion::Tokenize(configqueue, paths, pathdelimiter);

  if (tokens.size() != 2) {
    eos_static_err("the key definition of config <%s> is invalid", key);
    return false;
  }

  if (paths.size() < 1) {
    eos_static_err("the queue name does not contain any /");
    return false;
  }

  // apply a new token generation value
  if (tokens[1] == "token.generation") {
    eos_static_info("token-generation := %s", val.c_str());
    eos::common::EosTok::sTokenGeneration = strtoull(val.c_str(), 0, 10);
  }  else if (tokens[1] == "policy.recycle") {
    eos_static_info("policy-recycle := %s", val.c_str());

    if (val == "on") {
      gOFS->enforceRecycleBin = true;
    } else {
      gOFS->enforceRecycleBin = false;
    }
  } else if (tokens[1] == "fusex.hbi") {
    gOFS->zMQ->gFuseServer.Client().SetHeartbeatInterval(atoi(val.c_str()));
  } else if (tokens[1] == "fusex.qti") {
    gOFS->zMQ->gFuseServer.Client().SetQuotaCheckInterval(atoi(val.c_str()));
  } else if (tokens[1] == "fusex.bca") {
    gOFS->zMQ->gFuseServer.Client().SetBroadCastMaxAudience(atoi(val.c_str()));
  } else if (tokens[1] == "fusex.bca_match") {
    gOFS->zMQ->gFuseServer.Client().SetBroadCastAudienceSuppressMatch(val.c_str());
  }

  common::SharedHashLocator locator;

  if (!common::SharedHashLocator::fromConfigQueue(tokens[0], locator)) {
    eos_static_err("could not understand global configuration: %s",
                   tokens[0].c_str());
    return false;
  }

  mq::SharedHashWrapper hash(gOFS->mMessagingRealm.get(), locator);
  bool success = hash.set(tokens[1].c_str(), val.c_str());
  hash.releaseLocks();

  // Here we build a set with the gw nodes for fast lookup in the TransferEngine
  if ((tokens[0].find("/node/")) != std::string::npos) {
    if (tokens[1] == "txgw") {
      std::string broadcast = "/eos/";
      broadcast += paths[paths.size() - 1];
      size_t dashpos = 0;

      // Remote the #<variable>
      if ((dashpos = broadcast.find("#")) != std::string::npos) {
        broadcast.erase(dashpos);
      }

      broadcast += "/fst";
      // The node might not yet exist!
      FsView::gFsView.RegisterNode(broadcast.c_str());
      eos::common::RWMutexWriteLock gwlock(GwMutex);

      if (val == "on") {
        // we have to register this queue into the gw set for fast lookups
        FsView::gFsView.mGwNodes.insert(broadcast.c_str());
      } else {
        FsView::gFsView.mGwNodes.erase(broadcast.c_str());
      }
    }
  }

  return success;
}

//------------------------------------------------------------------------------
// Broadcast new manager id to all the FST nodes
//------------------------------------------------------------------------------
void
FsView::BroadcastMasterId(const std::string master_id)
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  for (auto it = FsView::gFsView.mNodeView.begin();
       it != FsView::gFsView.mNodeView.end(); ++it) {
    it->second->SetConfigMember("manager", master_id, true);
  }
}

//------------------------------------------------------------------------------
// Collect all endpoints (<hostname>:<port>) matching the given queue or pattern
//------------------------------------------------------------------------------
std::set<std::string>
FsView::CollectEndpoints(const std::string& queue) const
{
  int fst_port;
  std::string fst_host;
  std::set<std::string> endpoints;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  for (const auto& elem : FsView::gFsView.mIdView) {
    FileSystem* fs = elem.second;

    if (fs == nullptr) {
      eos_static_err("msg=\"file system null\" fsid=%u", elem.first);
      continue;
    }

    if (queue == "*") {
      if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOnline) {
        eos_static_err("msg=\"file system not online\" fsid=%u", elem.first);
        continue;
      }
    } else {
      if (queue != fs->GetQueue()) {
        continue;
      } else {
        if (fs->GetActiveStatus() != eos::common::ActiveStatus::kOnline) {
          eos_static_err("msg=\"file system not online\" fsid=%u", elem.first);
          break;
        }
      }
    }

    fst_host = fs->GetHost();
    fst_port = fs->getCoreParams().getLocator().getPort();
    endpoints.insert(SSTR(fst_host << ":" << fst_port));
  }

  return endpoints;
}


//------------------------------------------------------------------------------
// Should the provided fsid participate in statistics calculations?
// Yes, if:
// - The filesystem exists (duh)
// - The filesystem is at-least-RO, booted and online
//
// Call with fsview lock at-least-read locked.
//------------------------------------------------------------------------------
bool BaseView::ShouldConsiderForStatistics(FileSystem* fs)
{
  if (!fs) {
    return false;
  }

  if (fs->GetConfigStatus() < eos::common::ConfigStatus::kRO) {
    return false;
  }

  if (fs->GetStatus() != eos::common::BootStatus::kBooted) {
    return false;
  }

  if (fs->GetActiveStatus() == eos::common::ActiveStatus::kOffline) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Computes the sum for <param> as long
// param="<param>[?<key>=<value] allows to select with matches
//------------------------------------------------------------------------------
long long
BaseView::SumLongLong(const char* param, bool lock,
                      const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  long long sum = 0;
  std::string sparam = param;
  size_t qpos = 0;
  std::string key = "";
  std::string value = "";
  bool isquery = false;

  if ((qpos = sparam.find("?")) != std::string::npos) {
    std::string query = sparam;
    query.erase(0, qpos + 1);
    sparam.erase(qpos);
    std::vector<std::string> token;
    std::string delimiter = "@";
    eos::common::StringConversion::Tokenize(query, token, delimiter);
    key = token[0];
    value = token[1];
    isquery = true;
  }

  if (isquery && key == "*" && value == "*") {
    // we just count the number of entries
    if (subset) {
      return subset->size();
    } else {
      return size();
    }
  }

  std::set<std::string> used_nodes;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (!fs) {
      continue;
    }

    // for query sum's we always fold in that a group and host has to be enabled
    if (!key.length() || fs->GetString(key.c_str()) == value) {
      if (isquery &&
          ((eos::common::FileSystem::GetActiveStatusFromString(
              fs->GetString("stat.active").c_str())
            == eos::common::ActiveStatus::kOffline) ||
           (eos::common::FileSystem::GetStatusFromString(
              fs->GetString("stat.boot").c_str()) !=
            eos::common::BootStatus::kBooted))) {
        continue;
      }

      if (sparam.compare(0, 8, "stat.net") == 0) {
        const std::string hostname = fs->getCoreParams().getHost();

        if (used_nodes.find(hostname) == used_nodes.end()) {
          used_nodes.insert(hostname);
          const std::string fst_queue = fs->GetQueue();
          auto it = FsView::gFsView.mNodeView.find(fst_queue);

          if (it != FsView::gFsView.mNodeView.end()) {
            try {
              sum += std::stoll(it->second->GetConfigMember(sparam.c_str()));
            } catch (...) {}
          }
        }
      } else {
        long long v = fs->GetLongLong(sparam.c_str());

        if (isquery && v && (sparam == "stat.statfs.capacity")) {
          // Correct the capacity(rw) value for headroom
          v -= fs->GetLongLong("headroom");
        }

        sum += v;
      }
    }
  }

  return sum;
}

//------------------------------------------------------------------------------
// Computes the sum for <param> as double
//------------------------------------------------------------------------------
double
BaseView::SumDouble(const char* param, bool lock,
                    const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  double sum = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs) {
      sum += fs->GetDouble(param);
    }
  }

  return sum;
}

//------------------------------------------------------------------------------
// Computes the average for <param>
//------------------------------------------------------------------------------
// @todo (esindril) The lock parameter should be removed as this function is
// never called without the lock taken
double
BaseView::AverageDouble(const char* param, bool lock,
                        const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  double sum = 0;
  int cnt = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    bool consider = true;
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs == nullptr) {
      continue;
    }

    if (mType == "groupview") {
      consider = ShouldConsiderForStatistics(fs);
    }

    if (consider) {
      cnt++;
      sum += fs->GetDouble(param);
    }
  }

  return (cnt) ? (double)(1.0 * sum / cnt) : 0;
}

//------------------------------------------------------------------------------
// Computes the maximum absolute deviation of <param> from the avg of <param>
//------------------------------------------------------------------------------
double
BaseView::MaxAbsDeviation(const char* param, bool lock,
                          const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  double avg = AverageDouble(param, false);
  double maxabsdev = 0;
  double dev = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    bool consider = true;
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs == nullptr) {
      continue;
    }

    if (mType == "groupview") {
      consider = ShouldConsiderForStatistics(fs);
    }

    if (consider) {
      dev = fabs(avg - fs->GetDouble(param));

      if (dev > maxabsdev) {
        maxabsdev = dev;
      }
    }
  }

  return maxabsdev;
}


//------------------------------------------------------------------------------
// Computes the maximum deviation of <param> from the avg of <param>
//------------------------------------------------------------------------------
double
BaseView::MaxDeviation(const char* param, bool lock,
                       const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  double avg = AverageDouble(param, false);
  double maxdev = -DBL_MAX;
  double dev = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    bool consider = true;
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs == nullptr) {
      continue;
    }

    if (mType == "groupview") {
      consider = ShouldConsiderForStatistics(fs);
    }

    if (consider) {
      dev = -(avg - fs->GetDouble(param));

      if (dev > maxdev) {
        maxdev = dev;
      }
    }
  }

  return maxdev;
}

//------------------------------------------------------------------------------
// Computes the maximum deviation of <param> from the avg of <param>
//------------------------------------------------------------------------------
double
BaseView::MinDeviation(const char* param, bool lock,
                       const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  double avg = AverageDouble(param, false);
  double mindev = DBL_MAX;
  double dev = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    bool consider = true;
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs == nullptr) {
      continue;
    }

    if (mType == "groupview") {
      consider = ShouldConsiderForStatistics(fs);
    }

    if (consider) {
      dev = -(avg - fs->GetDouble(param));

      if (dev < mindev) {
        mindev = dev;
      }
    }
  }

  return mindev;
}

//------------------------------------------------------------------------------
// Computes the sigma for <param>
//------------------------------------------------------------------------------
double
BaseView::SigmaDouble(const char* param, bool lock,
                      const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  double avg = AverageDouble(param, false);
  double sumsquare = 0;
  int cnt = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    bool consider = true;
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs == nullptr) {
      continue;
    }

    if (mType == "groupview") {
      consider = ShouldConsiderForStatistics(fs);
    }

    if (consider) {
      cnt++;
      sumsquare += pow((avg - fs->GetDouble(param)), 2);
    }
  }

  sumsquare = (cnt) ? sqrt(sumsquare / cnt) : 0;
  return sumsquare;
}

//------------------------------------------------------------------------------
// Computes the considered count
//------------------------------------------------------------------------------
long long
BaseView::ConsiderCount(bool lock,
                        const std::set<eos::common::FileSystem::fsid_t>* subset)
{
  eos::common::RWMutexReadLock fs_rd_lock;

  if (lock) {
    fs_rd_lock.Grab(FsView::gFsView.ViewMutex);
  }

  long long cnt = 0;
  fsid_iterator it(subset, this);

  for (; it.valid(); it.next()) {
    bool consider = true;
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs == nullptr) {
      continue;
    }

    if (mType == "groupview") {
      consider = ShouldConsiderForStatistics(fs);
    }

    if (consider) {
      cnt++;
    }
  }

  return cnt;
}

//------------------------------------------------------------------------------
// Print user defined format to out
//
// table_format
//-------------
// format has to be provided as a chain (separated by "|" ) of the following tags
// "member=<key>:width=<width>:format=[+][-][so]:unit=<unit>:tag=<tag>"
//  -> to print a member variable of the view
// "avg=<key>:width=<width>:format=[fo]"   -> to print the average
// "sum=<key>:width=<width>:format=[lo]    -> to print a sum
// "sig=<key>:width=<width>:format=[lo]    -> to print the standard deviation
// "maxdev=<key>:width=<width>;format=[lo] -> to print the maxdeviation
// "tag=<tag>"                             -> use tag as header not the variable name
// "header=1" -> put a header with description on top!
//               This must be the first format tag!!!
//
// table_mq_format
//-----------
// format has to be provided as a chain (separated by "|" ) of the following tags
// "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>:tag=<tag>"
//  -> to print a key of the attached children
// "header=1" -> put a header with description on top
//               This must be the first format tag!!!
// the formats are:
// 's' : print as string
// 'S' : print as short string
// 'l' : print as long long
// 'f' : print as double
// 'o' : print as <key>=<val>
// '-' : left align the printout
// '+' : convert numbers into k,M,G,T,P ranges
// the unit is appended to every number:
// e.g. 1500 with unit=B would end up as '1.5 kB'
// the command only appends to <out> and DOES NOT initialize it
// "tag=<tag>" -> use tag as header not the variable name
//------------------------------------------------------------------------------
void
BaseView::Print(TableFormatterBase& table, std::string table_format,
                const std::string& table_mq_format, unsigned outdepth,
                const std::string& filter, const bool dont_color)
{
  // Since we don't display the members with geodepth option, we proceed with
  // the non geodepth display first.
  if (outdepth > 0) {
    Print(table, table_format, table_mq_format, 0, filter, dont_color);

    // We force-print the header
    if (table_format.find("header=1") == std::string::npos) {
      if (table_format.find("header=0") != std::string::npos) {
        table_format.replace(table_format.find("header=0"), 8, "header=1");
      }

      table_format = "header=1:" + table_format;
    }
  }

  std::vector<std::string> formattoken;
  class DoubleAggregatedStats : public std::map<std::string, DoubleAggregator*>
  {
    BaseView* pThis;

  public:
    DoubleAggregatedStats(BaseView* This) : pThis(This) {}
    DoubleAggregator* operator[](const char* param)
    {
      if (!count(param)) {
        DoubleAggregator* aggreg = new DoubleAggregator(param);
        aggreg->setView(pThis);
        pThis->runAggregator(aggreg);
        insert(std::make_pair(param, aggreg));
      }

      return find(param)->second;
    }

    ~DoubleAggregatedStats()
    {
      for (auto it = begin(); it != end(); it++) {
        delete it->second;
      }
    }
  };
  class LongLongAggregatedStats : public
    std::map<std::string, LongLongAggregator*>
  {
    BaseView* pThis;
  public:
    LongLongAggregatedStats(BaseView* This) : pThis(This) {}

    LongLongAggregator* operator[](const char* param)
    {
      if (!count(param)) {
        LongLongAggregator* aggreg = new LongLongAggregator(param);
        aggreg->setView(pThis);
        pThis->runAggregator(aggreg);
        insert(std::make_pair(param, aggreg));
      }

      return find(param)->second;
    }

    ~LongLongAggregatedStats()
    {
      for (auto it = begin(); it != end(); it++) {
        delete it->second;
      }
    }
  };
  LongLongAggregatedStats longStats(this);
  DoubleAggregatedStats doubleStats(this);
  unsigned int nLines = 0;

  if (outdepth > 0) {
    nLines = longStats["lastHeartBeat"]->getGeoTags()->size();
    nLines = longStats["lastHeartBeat"]->getEndIndex(outdepth);
  } else {
    nLines = 1;
  }

  eos::common::StringConversion::Tokenize(table_format, formattoken, "|");
  TableHeader table_header;
  TableData table_data;
  TableHeader table_mq_header;
  TableData table_mq_data;

  for (unsigned int l = 0; l < nLines; l++) {
    table_data.emplace_back();
    table_header.clear();

    for (unsigned int i = 0; i < formattoken.size(); i++) {
      std::vector<std::string> tagtoken;
      std::map<std::string, std::string> formattags;
      eos::common::StringConversion::Tokenize(formattoken[i], tagtoken, ":");

      for (unsigned int j = 0; j < tagtoken.size(); j++) {
        std::vector<std::string> keyval;
        eos::common::StringConversion::Tokenize(tagtoken[j], keyval, "=");

        if (keyval.size() != 2) {
          eos_static_err("failed parsing \"%s\", expected 2 tokens");
          continue;
        }

        formattags[keyval[0]] = keyval[1];
      }

      // To save display space, we don't print out members with geodepth option
      if (outdepth > 0 && formattags.count("member")) {
        continue;
      }

      if (formattags.count("format")) {
        std::string header = "";
        std::string format = formattags["format"];
        unsigned int width = (formattags.count("width") ?
                              atoi(formattags["width"].c_str()) : 0);
        std::string unit = (formattags.count("unit") ? formattags["unit"] : "");

        if (formattags.count("geosched")) {
          if (formattags["geosched"] == "totalspace") {
            std::string nogroup;
            table_data.back().push_back(
              TableCell((long long)gOFS->mGeoTreeEngine->placementSpace(mName, nogroup),
                        format, unit));
            table_header.push_back(std::make_tuple("sched.capacity", width, format));
          }
        }

        // Normal member printout
        if (formattags.count("member")) {
          if ((format.find("+") != std::string::npos) &&
              (format.find("s") == std::string::npos)) {
            table_data.back().push_back(
              TableCell(strtoll(GetMember(formattags["member"]).c_str(), 0, 10),
                        format, unit));
          } else {
            std::string member = GetMember(formattags["member"]).c_str();

            if ((format.find("S") != std::string::npos)) {
              size_t colon = member.find(":");
              size_t dot = member.find(".");

              if (dot != std::string::npos) {
                member.erase(dot, (colon != std::string::npos) ? colon - dot : colon);
              }
            }

            table_data.back().push_back(TableCell(member, format));
          }

          // Header
          XrdOucString pkey = formattags["member"].c_str();

          if ((format.find("o") == std::string::npos)) { //for table output
            pkey.replace("stat.statfs.", "");
            pkey.replace("stat.", "");
            pkey.replace("cfg.", "");

            if (formattags.count("tag")) {
              pkey = formattags["tag"].c_str();
            }
          }

          header = pkey.c_str();
        }

        // Sum printout
        if (formattags.count("sum")) {
          if (!outdepth) {
            table_data.back().push_back(
              TableCell(SumLongLong(formattags["sum"].c_str(), false),
                        format, unit));
          } else {
            table_data.back().push_back(
              TableCell((*longStats[formattags["sum"].c_str()]->getSums())[l],
                        format, unit));
          }

          // Header
          XrdOucString pkey = formattags["sum"].c_str();

          if ((format.find("o") == std::string::npos)) {
            pkey.replace("stat.statfs.", "");
            pkey.replace("stat.", "");
            pkey.replace("cfg.", "");

            if (!formattags.count("tag")) {
              header = "sum(";
              header += pkey.c_str();
              header += ")";
            } else {
              header = formattags["tag"].c_str();
            }
          } else { //for monitoring output
            header = "sum.";
            header += pkey.c_str();
          }
        }

        // Avg printout
        if (formattags.count("avg")) {
          if (formattags["avg"] == "stat.geotag") {
            if (outdepth) {
              // This average means anything only when displaying along the
              // topology tree
              table_data.back().push_back(
                TableCell((*longStats["lastHeartBeat"]->getGeoTags())[l].c_str(),
                          format));
              // Header
              XrdOucString pkey = formattags["avg"].c_str();

              if ((format.find("o") == std::string::npos)) {
                pkey.replace("stat.statfs.", "");
                pkey.replace("stat.", "");
                pkey.replace("cfg.", "");
                header = pkey.c_str();
              } else { //for monitoring output
                header = "avg.";
                header += pkey.c_str();
              }
            }
          } else { // If not geotag special case
            if (!outdepth) {
              table_data.back().push_back(
                TableCell(AverageDouble(formattags["avg"].c_str(), false),
                          format, unit));
            } else {
              table_data.back().push_back(
                TableCell((*doubleStats[formattags["avg"].c_str()]->getMeans())[l],
                          format, unit));
            }

            // Header
            XrdOucString pkey = formattags["avg"].c_str();

            if ((format.find("o") == std::string::npos)) {
              pkey.replace("stat.statfs.", "");
              pkey.replace("stat.", "");
              pkey.replace("cfg.", "");

              if (!formattags.count("tag")) {
                header = "avg(";
                header += pkey.c_str();
                header += ")";
              } else {
                header = formattags["tag"].c_str();
              }
            } else { //for monitoring output
              header = "avg.";
              header += pkey.c_str();
            }
          } // end not geotag case
        }

        // Sig printout
        if (formattags.count("sig")) {
          if (!outdepth) {
            table_data.back().push_back(
              TableCell(SigmaDouble(formattags["sig"].c_str(), false),
                        format, unit));
          } else {
            table_data.back().push_back(
              TableCell((*doubleStats[formattags["sig"].c_str()]->getStdDevs())[l],
                        format, unit));
          }

          // Header
          XrdOucString pkey = formattags["sig"].c_str();

          if ((format.find("o") == std::string::npos)) {
            pkey.replace("stat.statfs.", "");
            pkey.replace("stat.", "");
            pkey.replace("cfg.", "");

            if (!formattags.count("tag")) {
              header = "sig(";
              header += pkey.c_str();
              header += ")";
            } else {
              header = formattags["tag"].c_str();
            }
          } else { //for monitoring output
            header = "sig.";
            header += pkey.c_str();
          }
        }

        // MaxDev printout
        if (formattags.count("maxdev")) {
          if (!outdepth) {
            table_data.back().push_back(
              TableCell(MaxAbsDeviation(formattags["maxdev"].c_str(), false),
                        format, unit));
          } else {
            table_data.back().push_back(
              TableCell((*doubleStats[formattags["maxdev"].c_str()]->getMaxAbsDevs())[l],
                        format, unit));
          }

          // Header
          XrdOucString pkey = formattags["maxdev"].c_str();

          if ((format.find("o") == std::string::npos)) {
            pkey.replace("stat.statfs.", "");
            pkey.replace("stat.", "");
            pkey.replace("cfg.", "");

            if (!formattags.count("tag")) {
              header = "dev(";
              header += pkey.c_str();
              header += ")";
            } else {
              header = formattags["tag"].c_str();
            }
          } else { //for monitoring output
            header = "dev.";
            header += pkey.c_str();
          }
        }

        // Build header
        if (!header.empty()) {
          table_header.push_back(std::make_tuple(header, width, format));
        }
      }
    }
  } // l from 0 to nLines

  if (outdepth > 0) {
    // Print table for geotag
    TableFormatterBase table_geo(dont_color);
    table_geo.SetHeader(table_header);
    table_geo.AddRows(table_data);
    table.AddString(table_geo.GenerateTable(HEADER).c_str());
  } else {
    //Get table from MQ side (second table)
    if (table_mq_format.length()) {
      // If a format was given for the filesystem children, forward it
      for (auto it = begin(); it != end(); ++it) {
        // auto it_fs = FsView::gFsView.mIdView.find(*it);
        FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

        if (fs) {
          table_mq_header.clear();
          fs->Print(table_mq_header, table_mq_data, table_mq_format, filter);
        }
      }
    }

    // Print table with information from MGM
    if (table_format.length() && !table_mq_format.length()) {
      table.SetHeader(table_header);
      table.AddRows(table_data);
    }

    // Print table with information from MGM and MQ. (Option "-l")
    if (table_format.length() && table_mq_format.length()) {
      table.SetHeader(table_header);
      table.AddRows(table_data);
//      TableFormatterBase table_mq;
      TableFormatterBase table_mq(dont_color);
      table_mq.SetHeader(table_mq_header);
      table_mq.AddRows(table_mq_data);
      table.AddString(table_mq.GenerateTable(HEADER).c_str());
    }

    // Print table with information only from MQ. (e.g. "fs ls")
    if (!table_format.length() && table_mq_format.length()) {
      table.SetHeader(table_mq_header);
      table.AddSeparator();
      table.AddRows(table_mq_data);
    }
  }
}

//------------------------------------------------------------------------------
// If a filesystem has not yet these parameters defined, we inherit them from
// the space configuration. This function has to be called with the a read lock
// on the View Mutex! It return true if the fs was modified and the caller should
// evt. store the modification to the config
//------------------------------------------------------------------------------
bool
FsSpace::ApplySpaceDefaultParameters(eos::mgm::FileSystem* fs, bool force)
{
  if (!fs) {
    return false;
  }

  bool modified = false;
  eos::common::FileSystem::fs_snapshot_t snapshot;

  if (fs->SnapShotFileSystem(snapshot, false)) {
    if (force || (!snapshot.mScanIoRate)) {
      if (GetConfigMember(eos::common::SCAN_IO_RATE_NAME).length()) {
        fs->SetString(eos::common::SCAN_IO_RATE_NAME,
                      GetConfigMember(eos::common::SCAN_IO_RATE_NAME).c_str());
        modified = true;
      }
    }

    if (force || (!snapshot.mScanEntryInterval)) {
      // try to apply the default
      if (GetConfigMember(eos::common::SCAN_ENTRY_INTERVAL_NAME).length()) {
        modified = true;
        fs->SetString(eos::common::SCAN_ENTRY_INTERVAL_NAME,
                      GetConfigMember(eos::common::SCAN_ENTRY_INTERVAL_NAME).c_str());
      }
    }

    if (force || (!snapshot.mScanDiskInterval)) {
      if (GetConfigMember(eos::common::SCAN_DISK_INTERVAL_NAME).length()) {
        modified = true;
        fs->SetString(eos::common::SCAN_DISK_INTERVAL_NAME,
                      GetConfigMember(eos::common::SCAN_DISK_INTERVAL_NAME).c_str());
      }
    }

    if (force || (!snapshot.mScanNsInterval)) {
      if (GetConfigMember(eos::common::SCAN_NS_INTERVAL_NAME).length()) {
        modified = true;
        fs->SetString(eos::common::SCAN_NS_INTERVAL_NAME,
                      GetConfigMember(eos::common::SCAN_NS_INTERVAL_NAME).c_str());
      }
    }

    if (force || (!snapshot.mScanNsRate)) {
      if (GetConfigMember(eos::common::SCAN_NS_RATE_NAME).length()) {
        fs->SetString(eos::common::SCAN_NS_RATE_NAME,
                      GetConfigMember(eos::common::SCAN_NS_RATE_NAME).c_str());
        modified = true;
      }
    }

    if (force || (!snapshot.mFsckRefreshInterval)) {
      if (GetConfigMember(eos::common::FSCK_REFRESH_INTERVAL_NAME).length()) {
        fs->SetString(eos::common::FSCK_REFRESH_INTERVAL_NAME,
                      GetConfigMember(eos::common::FSCK_REFRESH_INTERVAL_NAME).c_str());
        modified = true;
      }
    }

    if (force || (!snapshot.mGracePeriod)) {
      // try to apply the default
      if (GetConfigMember("graceperiod").length()) {
        fs->SetString("graceperiod", GetConfigMember("graceperiod").c_str());
        modified = true;
      }
    }

    if (force || (!snapshot.mDrainPeriod)) {
      // try to apply the default
      if (GetConfigMember("drainperiod").length()) {
        fs->SetString("drainperiod", GetConfigMember("drainperiod").c_str());
        modified = true;
      }
    }

    if (force || (!snapshot.mHeadRoom)) {
      // try to apply the default
      if (GetConfigMember("headroom").length()) {
        fs->SetString("headroom", GetConfigMember("headroom").c_str());
        modified = true;
      }
    }
  }

  return modified;
}

//------------------------------------------------------------------------------
// Re-evaluates the draining state in all groups and resets the state
//------------------------------------------------------------------------------
void
FsSpace::ResetDraining()
{
  eos_static_info("msg=\"reset drain state\" space=\"%s\"", mName.c_str());
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  // Iterate over all groups in this space
  for (auto sgit = FsView::gFsView.mSpaceGroupView[mName].begin();
       sgit != FsView::gFsView.mSpaceGroupView[mName].end();
       sgit++) {
    bool setactive = false;
    std::string lGroup = (*sgit)->mName;
    FsGroup::const_iterator git;

    for (git = (*sgit)->begin();
         git != (*sgit)->end(); git++) {
      FileSystem* entry = FsView::gFsView.mIdView.lookupByID(*git);

      if (entry) {
        eos::common::DrainStatus drainstatus =
          (eos::common::FileSystem::GetDrainStatusFromString(
             entry->GetString("local.drain").c_str()));

        if ((drainstatus == eos::common::DrainStatus::kDraining) ||
            (drainstatus == eos::common::DrainStatus::kDrainStalling)) {
          // if any mGroup filesystem is draining, all the others have
          // to enable the pull for draining!
          setactive = true;
        }
      }
    }

    // if the mGroup get's disabled we stop the draining
    if (FsView::gFsView.mGroupView[lGroup]->GetConfigMember("status") != "on") {
      setactive = false;
    }

    for (git = (*sgit)->begin(); git != (*sgit)->end(); git++) {
      eos::mgm::FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*git);

      if (fs) {
        if (setactive) {
          if (fs->GetString("stat.drainer") != "on") {
            fs->SetString("stat.drainer", "on");
          }
        } else {
          if (fs->GetString("stat.drainer") != "off") {
            fs->SetString("stat.drainer", "off");
          }
        }

        eos_static_info("fsid=%05d state=%s", fs->GetId(),
                        fs->GetString("stat.drainer").c_str());
      }
    }
  }
}

EOSMGMNAMESPACE_END
