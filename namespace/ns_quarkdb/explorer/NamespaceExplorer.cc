/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#include "common/Assert.hh"
#include <memory>
#include <numeric>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

EOSNSNAMESPACE_BEGIN

SearchNode::SearchNode(qclient::QClient &qcli, id_t d, eos::SearchNode *prnt)
: qcl(qcli), id(d), parent(prnt), containerMd(MetadataFetcher::getContainerFromId(qcl, id)) {

  fileMap = MetadataFetcher::getFilesInContainer(qcl, id);
  containerMap = MetadataFetcher::getSubContainers(qcl, id);
}

void SearchNode::handleAsync() {
  // Send off more requests if results are ready, otherwise do nothing.
  // If search needs some result, it'll block.

  if(!pendingFileMdsLoaded && fileMap.ready()) {
    stageFileMds();
  }

  if(!childrenLoaded && containerMap.ready()) {
    stageChildren();
  }
}

void SearchNode::stageFileMds() {
  // Unconditionally stage file mds, block if necessary. Call this only if:
  // - Search really needs the result.
  // - When prefetching, when you know fileMap is ready.
  if(pendingFileMdsLoaded) return;
  pendingFileMdsLoaded = true;

  // fileMap is hashmap, thus unsorted... must sort first by filename.. sigh.
  // storing into a vector and calling std::sort might be faster, TODO
  std::map<std::string, id_t> sortedFileMap;
  for(auto it = fileMap->begin(); it != fileMap->end(); it++) {
    sortedFileMap[it->first] = it->second;
  }

  for(auto it = sortedFileMap.begin(); it != sortedFileMap.end(); it++) {
    pendingFileMds.push_back(MetadataFetcher::getFileFromId(qcl, it->second));
  }
}

std::unique_ptr<SearchNode> SearchNode::expand() {
  stageChildren();

  if(children.empty()) {
    return {}; // nullptr, node has no more children to expand
  }

  // Explicit transfer of ownership
  std::unique_ptr<SearchNode> retval = std::move(children.front());
  children.pop_front();
  return retval;
}

void SearchNode::stageChildren() {
  // Unconditionally stage container mds, block if necessary. Call this only if:
  // - Search really needs the result.
  // - When prefetching, when you know containerMap is ready.
  if(childrenLoaded) return;
  childrenLoaded = true;

  // containerMap is hashmap, thus unsorted... must sort first by filename.. sigh.
  // storing into a vector and calling std::sort might be faster, TODO
  std::map<std::string, id_t> sortedContainerMap;
  for(auto it = containerMap->begin(); it != containerMap->end(); it++) {
    sortedContainerMap[it->first] = it->second;
  }

  for(auto it = sortedContainerMap.begin(); it != sortedContainerMap.end(); it++) {
    children.emplace_back(new SearchNode(qcl, it->second, this));
  }
}

id_t SearchNode::getID() const {
  return id;
}

bool SearchNode::fetchChild(eos::ns::FileMdProto &output) {
  stageFileMds();

  if(pendingFileMds.empty()) {
    return false;
  }

  output = pendingFileMds[0].get();
  pendingFileMds.pop_front();
  return true;
}

bool SearchNode::isVisited() {
  return visited;
}

void SearchNode::visit() {
  visited = true;
}

eos::ns::ContainerMdProto& SearchNode::getContainerInfo() {
  return containerMd.get();
}

NamespaceExplorer::NamespaceExplorer(const std::string &pth, const ExplorationOptions &opts, qclient::QClient &qclient)
: path(pth), options(opts), qcl(qclient) {

  std::vector<std::string> pathParts;

  eos::PathProcessor::splitPath(pathParts, path);

  if(pathParts.empty()) {
    MDException e(EINVAL);
    e.getMessage() << "Empty path provided";
    throw e;
  }

  // This part is synchronous by necessity.
  staticPath.emplace_back(MetadataFetcher::getContainerFromId(qcl, 1).get());

  // TODO: This for loop looks like a useful primitive for MetadataFetcher,
  // maybe move there?
  for(size_t i = 0; i < pathParts.size(); i++) {
    // We don't know if the last chunk of pathParts is supposed to be a container
    // or name..
    id_t parentID = staticPath.back().id();

    bool threw = false;
    id_t nextId = -1;

    try {
      nextId = MetadataFetcher::getContainerIDFromName(qcl, parentID, pathParts[i]).get();
    }
    catch(const MDException &exc) {
      threw = true;
      // Maybe the user called "Find" on a single file, and the last chunk is
      // actually a file. Weird, but possible.

      if(i != pathParts.size() - 1) {
        // Nope, not last part.
        throw;
      }

      if(exc.getErrno() != ENOENT) {
        // Nope, different kind of error
        throw;
      }

      if(exc.getErrno() == ENOENT) {
        // This may throw again, propagate to caller if so
        id_t nextId = MetadataFetcher::getFileIDFromName(qcl, parentID, pathParts[i]).get();
        lastChunk = MetadataFetcher::getFileFromId(qcl, nextId).get();
        searchOnFile = true;
      }
    }

    if(!threw) {
      if(i != pathParts.size() - 1) {
        staticPath.emplace_back(MetadataFetcher::getContainerFromId(qcl, nextId).get());
      }
      else {
        // Final node, expand
        dfsPath.emplace_back(new SearchNode(qcl, nextId, nullptr));
      }
    }
  }
}

std::string NamespaceExplorer::buildStaticPath() {
  // TODO: Cache this?
  std::stringstream ss;
  for(size_t i = 0; i < staticPath.size(); i++) {
    if(i == 0) {
      // Root node
      ss << "/";
    }
    else {
      ss << staticPath[i].name() << "/";
    }
  }
  return ss.str();
}

std::string NamespaceExplorer::buildDfsPath() {
  // TODO: cache this somehow?
  std::stringstream ss;
  ss << buildStaticPath();

  for(size_t i = 0; i < dfsPath.size(); i++) {
    ss << dfsPath[i]->getContainerInfo().name() << "/";
  }

  return ss.str();
}

bool NamespaceExplorer::fetch(NamespaceItem &item) {
  // Handle weird case: Search was called on a single file
  if(searchOnFile) {
    if(searchOnFileEnded) return false;

    item.fullPath = buildStaticPath() + lastChunk.name();
    item.isFile = true;
    item.fileMd = lastChunk;

    searchOnFileEnded = true;
    return true;
  }

  while(!dfsPath.empty()) {
    dfsPath.back()->handleAsync();

    // Has top node been visited yet?
    if(!dfsPath.back()->isVisited()) {
      dfsPath.back()->visit();

      item.isFile = false;
      item.fullPath = buildDfsPath();
      item.containerMd = dfsPath.back()->getContainerInfo();
      return true;
    }

    // Does the top node have any pending file children?
    if(dfsPath.back()->fetchChild(item.fileMd)) {
      item.isFile = true;
      item.fullPath = buildDfsPath() + item.fileMd.name();
      return true;
    }

    // Can we expand this node?
    std::unique_ptr<SearchNode> child = dfsPath.back()->expand();
    if(child) {
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
