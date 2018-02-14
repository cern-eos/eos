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

NamespaceExplorer::NamespaceExplorer(const std::string &pth, const ExplorationOptions &opts, qclient::QClient &qclient)
: path(pth), options(opts), qcl(qclient) {

  std::vector<std::string> pathParts;

  eos::PathProcessor::splitPath(pathParts, path);

  if(pathParts.empty()) {
    MDException e(EINVAL);
    e.getMessage() << "Empty path provided";
    throw e;
  }

  // This part is synchronous by necessity,
  SearchNode root;
  root.container = MetadataFetcher::getContainerFromId(qcl, 1).get();
  state.nodes.push_back(root);

  for(size_t i = 0; i < pathParts.size(); i++) {
    id_t parentID = state.nodes.back().container.id();
    id_t nextId = MetadataFetcher::getContainerIDFromName(qcl, parentID, pathParts[i]).get();

    SearchNode next;
    next.container = MetadataFetcher::getContainerFromId(qcl, nextId).get();
    state.nodes.push_back(next);
  }

  state.pendingFileIds.set_deleted_key("");
  state.pendingFileIds.set_empty_key("##_EMPTY_##");
  populatePendingItems(state.nodes.back().container.id());
}

void NamespaceExplorer::populatePendingItems(id_t container) {
  eos_assert(state.pendingFileIds.empty());
  state.pendingFileIds = MetadataFetcher::getFilesInContainer(qcl, container).get();

  for(auto it = state.pendingFileIds.begin(); it != state.pendingFileIds.end(); it++) {
    state.filesToGive.push(MetadataFetcher::getFileFromId(qcl, it->second));
  }
}

bool NamespaceExplorer::fetch(NamespaceItem &item) {
  if(!state.filesToGive.empty()) {
    item.fileMd = state.filesToGive.front().get();
    state.filesToGive.pop();
    return true;
  }

  return false;
}

EOSNSNAMESPACE_END
