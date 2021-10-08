/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/utils/PathProcessor.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/utils/Attributes.hh"
#include "common/Assert.hh"
#include "common/Path.hh"
#include <memory>
#include <numeric>
#include <folly/executors/IOThreadPoolExecutor.h>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SearchNode::SearchNode(NamespaceExplorer& expl, ContainerIdentifier expectedP,
                       ContainerIdentifier d, eos::SearchNode* prnt, folly::Executor* exec,
                       bool ignoreF)
  : explorer(expl), expectedParent(expectedP), id(d), qcl(explorer.qcl),
    parent(prnt), executor(exec), ignoreFiles(ignoreF),
    containerMd(MetadataFetcher::getContainerFromId(qcl, id))
{
  if (!ignoreFiles) {
    pendingFileMds = MetadataFetcher::getFileMDsInContainer(qcl, id, exec);
  }

  containerMap = MetadataFetcher::getContainerMap(qcl, id);
}

//------------------------------------------------------------------------------
// Can we visit this node? Possible only if:
// - No errors occurred while retrieving the container's metadata.
// - Has not been visited already.
//------------------------------------------------------------------------------
bool SearchNode::canVisit()
{
  if (visited) {
    return false;
  }

  if (containerMd.hasException() || containerMap.hasException()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Send off more requests if results are ready, otherwise do nothing.
// If search needs some result, it'll block.
//------------------------------------------------------------------------------
void SearchNode::handleAsync()
{
  if (!childrenLoaded && containerMap.ready()) {
    stageChildren();
  }
}

//------------------------------------------------------------------------------
// Get more subcontainers if available
//------------------------------------------------------------------------------
std::unique_ptr<SearchNode> SearchNode::expand()
{
  if (containerMd.hasException()) {
    return {};
  }

  NamespaceItem nodeItem;
  nodeItem.isFile = false;
  nodeItem.containerMd = getContainerInfo();
  explorer.handleLinkedAttrs(nodeItem);

  if (expansionFilteredOut) {
    return {}; // nope, this node is being filtered out
  }

  if (nodeItem.containerMd.parent_id() != expectedParent.getUnderlyingUInt64()) {
    std::cerr << "WARNING: Container #" << nodeItem.containerMd.id() <<
              " was expected to have #" <<
              expectedParent.getUnderlyingUInt64() << " as parent; instead it has #" <<
              nodeItem.containerMd.parent_id()
              << std::endl;
  }

  stageChildren();

  if (children.empty()) {
    return {}; // nullptr, node has no more children to expand
  }

  // Explicit transfer of ownership
  std::unique_ptr<SearchNode> retval = std::move(children.front());
  children.pop_front();
  return retval;
}

//------------------------------------------------------------------------------
// @todo (gbitzes): Remove this eventually, once we are confident the two find
// implementations match, apart for the order.
//------------------------------------------------------------------------------
struct FilesystemEntryComparator {
  bool operator()(const std::string& lhs, const std::string& rhs) const
  {
    for (size_t i = 0; i < std::min(lhs.size(), rhs.size()); i++) {
      if (lhs[i] != rhs[i]) {
        return lhs[i] < rhs[i];
      }
    }

    return lhs.size() > rhs.size();
  }
};

//------------------------------------------------------------------------------
// Unconditionally stage container mds, block if necessary. Call this only if:
// - Search really needs the result.
// - When prefetching, when you know containerMap is ready.
//------------------------------------------------------------------------------
void SearchNode::stageChildren()
{
  if (childrenLoaded) {
    return;
  }

  childrenLoaded = true;
  // containerMap is hashmap, thus unsorted... must sort first by filename.. sigh.
  // storing into a vector and calling std::sort might be faster, TODO
//  std::map<std::string, IContainerMD::id_t, FilesystemEntryComparator> sortedContainerMap; // Why not the std::less<string> comparator?
//  for (auto it = containerMap->cbegin(); it != containerMap->cend(); ++it) {
//    sortedContainerMap[it->first] = it->second;
//  }
  std::vector<std::pair<std::string, IContainerMD::id_t>> v;

  for (auto it = containerMap->begin(); it != containerMap->end(); ++it) {
    v.emplace_back(std::pair<std::string, IContainerMD::id_t> {it->first, it->second});
  }

  std::sort(v.begin(), v.end());

  for (auto it = v.begin(); it != v.end(); ++it) {
    children.emplace_back(new SearchNode(explorer, id,
                                         ContainerIdentifier(it->second), this, executor, ignoreFiles));
  }
}

//------------------------------------------------------------------------------
// Fetch a file entry
//------------------------------------------------------------------------------
bool SearchNode::fetchChild(eos::ns::FileMdProto& output)
{
  while (true) {
    try {
      return pendingFileMds.fetchNext(output);
    } catch (MDException& exc) {}
  }

  return false;
}

//------------------------------------------------------------------------------
// Get container md proto info
//------------------------------------------------------------------------------
eos::ns::ContainerMdProto& SearchNode::getContainerInfo()
{
  return containerMd.get();
}

//------------------------------------------------------------------------------
// Get file child count
//------------------------------------------------------------------------------
uint64_t SearchNode::getNumFiles()
{
  return pendingFileMds.size();
}

uint64_t SearchNode::getNumContainers()
{
  return containerMap->size();
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
NamespaceExplorer::NamespaceExplorer(const std::string& pth,
                                     const ExplorationOptions& opts,
                                     qclient::QClient& qclient,
                                     folly::Executor* exec)
  : path(pth), options(opts), qcl(qclient), executor(exec)
{
  if (options.populateLinkedAttributes && !opts.view) {
    throw_mdexception(EINVAL,
                      "NamespaceExplorer: asked to populate linked attrs, but view not provided");
  }

  std::vector<std::string> pathParts = eos::common::SplitPath(path);
  // This part is synchronous by necessity.
  staticPath.emplace_back(MetadataFetcher::getContainerFromId(qcl,
                          ContainerIdentifier(1)).get());

  if (pathParts.empty()) {
    // We're running a search on the root node, expand.
    dfsPath.emplace_back(new SearchNode(*this, ContainerIdentifier(1),
                                        ContainerIdentifier(1), nullptr, executor, opts.ignoreFiles));
  }

  // TODO: This for loop looks like a useful primitive for MetadataFetcher,
  // maybe move there?
  ContainerIdentifier parentID {};
  ContainerIdentifier nextId {};

  for (size_t i = 0; i < pathParts.size(); i++) {
    // We don't know if the last chunk of pathParts is supposed to be a container
    // or name..
    parentID = ContainerIdentifier(staticPath.back().id());
    bool threw = false;

    try {
      nextId = MetadataFetcher::getContainerIDFromName(qcl, parentID,
               pathParts[i]).get();
    } catch (const MDException& exc) {
      threw = true;
      // Maybe the user called "Find" on a single file, and the last chunk is
      // actually a file. Weird, but possible.

      if (i != pathParts.size() - 1) {
        // Nope, not last part.
        throw;
      }

      if (exc.getErrno() != ENOENT) {
        // Nope, different kind of error
        throw;
      }

      if (exc.getErrno() == ENOENT) {
        // This may throw again, propagate to caller if so
        FileIdentifier nextId = MetadataFetcher::getFileIDFromName(qcl, parentID,
                                pathParts[i]).get();
        lastChunk = MetadataFetcher::getFileFromId(qcl, nextId).get();
        searchOnFile = true;
      }
    }

    if (!threw) {
      if (i != pathParts.size() - 1) {
        staticPath.emplace_back(MetadataFetcher::getContainerFromId(qcl, nextId).get());
      } else {
        // Final node, expand
        dfsPath.emplace_back(new SearchNode(*this, parentID, nextId, nullptr, executor,
                                            opts.ignoreFiles));
      }
    }
  }
}

//------------------------------------------------------------------------------
// Build static path
//------------------------------------------------------------------------------
std::string NamespaceExplorer::buildStaticPath()
{
  if (staticPath.size() == 1) {
    return "/";
  }

  // TODO: Cache this?
  std::stringstream ss;

  for (size_t i = 0; i < staticPath.size(); i++) {
    if (i == 0) {
      // Root node
      ss << "/";
    } else {
      ss << staticPath[i].name() << "/";
    }
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Build depth-first-search path
//------------------------------------------------------------------------------
std::string NamespaceExplorer::buildDfsPath()
{
  // TODO: cache this somehow?
  std::stringstream ss;
  ss << buildStaticPath();

  for (size_t i = 0; i < dfsPath.size(); i++) {
    if (dfsPath[i]->getContainerInfo().id() == 1) {
      continue;
    } else {
      ss << dfsPath[i]->getContainerInfo().name() << "/";
    }
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Handle linked attributes
//------------------------------------------------------------------------------
void NamespaceExplorer::handleLinkedAttrs(NamespaceItem& result)
{
  result.attrs.clear();
  //----------------------------------------------------------------------------
  // Retrieve reference to linked attribute map
  //----------------------------------------------------------------------------
  google::protobuf::Map<std::string, std::string> const* attrMap = nullptr;

  if (result.isFile) {
    attrMap = &result.fileMd.xattrs();
  } else {
    attrMap = &result.containerMd.xattrs();
  }

  //----------------------------------------------------------------------------
  // Copy stuff, unfortunately
  //----------------------------------------------------------------------------
  result.attrs = {attrMap->begin(), attrMap->end() };

  //----------------------------------------------------------------------------
  // Do we even have linked attrs?
  //----------------------------------------------------------------------------
  if (!options.populateLinkedAttributes) {
    return;
  }

  auto link = attrMap->find("sys.attr.link");

  if (link == attrMap->end()) {
    //--------------------------------------------------------------------------
    // Nope, take fast path
    //--------------------------------------------------------------------------
    return;
  }

  //----------------------------------------------------------------------------
  // Cached entry exists?
  //----------------------------------------------------------------------------
  auto cached = cachedAttrs.find(link->second);

  if (cached != cachedAttrs.end()) {
    //--------------------------------------------------------------------------
    // Cache hit
    //--------------------------------------------------------------------------
    populateLinkedAttributes(cached->second, result.attrs, options.prefixLinks);
    return;
  }

  //----------------------------------------------------------------------------
  // Cache miss
  //----------------------------------------------------------------------------
  eos::IContainerMD::XAttrMap toStoreIntoCache;

  try {
    FileOrContainerMD item = options.view->getItem(link->second, true).get();

    if (item.file) {
      toStoreIntoCache = item.file->getAttributes();
    } else {
      toStoreIntoCache = item.container->getAttributes();
    }
  } catch (eos::MDException& e) {
    // toStoreIntoCache remains empty
  }

  cachedAttrs[link->second] = toStoreIntoCache;
  populateLinkedAttributes(toStoreIntoCache, result.attrs, options.prefixLinks);
}

//------------------------------------------------------------------------------
// Fetch children under current path
//------------------------------------------------------------------------------
bool NamespaceExplorer::fetch(NamespaceItem& item)
{
  // Handle weird case: Search was called on a single file
  if (searchOnFile) {
    if (searchOnFileEnded) {
      return false;
    }

    item.fullPath = buildStaticPath() + lastChunk.name();
    item.isFile = true;
    item.fileMd = lastChunk;
    searchOnFileEnded = true;
    return true;
  }

  while (!dfsPath.empty()) {
    dfsPath.back()->handleAsync();

    // Has top node been visited yet?
    if (dfsPath.back()->canVisit()) {
      dfsPath.back()->visit();
      item.isFile = false;
      item.fullPath = buildDfsPath();
      item.containerMd = dfsPath.back()->getContainerInfo();
      item.numFiles = dfsPath.back()->getNumFiles();
      item.numContainers = dfsPath.back()->getNumContainers();
      handleLinkedAttrs(item);
      item.expansionFilteredOut = false;

      if (options.expansionDecider) {
        item.expansionFilteredOut = !options.expansionDecider->shouldExpandContainer(
                                      item.containerMd, item.attrs, item.fullPath);
      }

      if (options.depthLimit > 0) {
        eos::common::Path cpath{item.fullPath};
        item.expansionFilteredOut = (item.expansionFilteredOut
                                     || (cpath.GetSubPathSize() > options.depthLimit));
      }

      dfsPath.back()->expansionFilteredOut = item.expansionFilteredOut;
      return true;
    }

    // Does the top node have any pending file children?
    if (!dfsPath.back()->expansionFilteredOut &&
        dfsPath.back()->fetchChild(item.fileMd)) {
      item.isFile = true;
      item.fullPath = buildDfsPath() + item.fileMd.name();
      item.expansionFilteredOut = false;
      handleLinkedAttrs(item);
      return true;
    }

    // Can we expand this node?
    std::unique_ptr<SearchNode> child = dfsPath.back()->expand();

    if (child) {
      dfsPath.push_back(std::move(child));
      continue;
    }

    // Node has neither files, nor containers, pop.
    dfsPath.pop_back();
  }

  // Search is over.
  return false;
}

EOSNSNAMESPACE_END
