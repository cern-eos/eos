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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Class for exploring the namespace
//------------------------------------------------------------------------------

#pragma once

#include "namespace/Namespace.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include <string>
#include <vector>

EOSNSNAMESPACE_BEGIN

class ContainerMDSvc;
class FileMdSvc;
class HierarchicalView;

struct ExplorationOptions {
  int depthLimit;
};

struct NamespaceItem {
  // A simple string for now, we can extend this later.
  std::string item;
};

// Represents a node in the search tree, including any unexplored children.
struct SearchNode {
  eos::ns::ContainerMdProto container;
  std::queue<id_t> unexploredChildContainers;
};

struct SearchState {
  std::vector<SearchNode> nodes;
};


//------------------------------------------------------------------------------
//! Class to recursively explore the QuarkDB namespace, starting from some path.
//! Useful for "Find" commands - no consistency guarantees, if a write is in
//! the flusher, it might not be seen here.
//!
//! This implementation is super slow right now, and synchronous! Simple DFS.
//------------------------------------------------------------------------------
class NamespaceExplorer {
public:
  //----------------------------------------------------------------------------
  //! Inject the QClient to use directly in the constructor. No ownership of
  //! underlying object.
  //----------------------------------------------------------------------------
  NamespaceExplorer(const std::string &path, const ExplorationOptions &options, qclient::QClient &qcl);

  //----------------------------------------------------------------------------
  //! Fetch next item.
  //----------------------------------------------------------------------------
  bool fetch(NamespaceItem &result);

  //----------------------------------------------------------------------------
  //! Get current search state.
  //----------------------------------------------------------------------------
  SearchState getSearchState() {
    return state;
  }

private:
  std::string path;
  ExplorationOptions options;
  qclient::QClient &qcl;

  SearchState state;
};

EOSNSNAMESPACE_END
