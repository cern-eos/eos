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

  SearchNode root;
  root.container = MetadataFetcher::getContainerFromId(qcl, 1);
  state.nodes.push_back(root);

  for(size_t i = 0; i < pathParts.size(); i++) {
    id_t parentID = state.nodes.back().container.id();
    id_t nextId = MetadataFetcher::getContainerIDFromName(qcl, pathParts[i], parentID);

    SearchNode next;
    next.container = MetadataFetcher::getContainerFromId(qcl, nextId);
    state.nodes.push_back(next);
  }
}

bool NamespaceExplorer::fetch(NamespaceItem &item) {

}

EOSNSNAMESPACE_END
