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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Class for exploring the namespace
//------------------------------------------------------------------------------

#pragma once

#include "common/FutureWrapper.hh"
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"
#include "proto/ContainerMd.pb.h"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/Identifiers.hh"
#include "namespace/ns_quarkdb/utils/FutureVectorIterator.hh"
#include <string>
#include <vector>
#include <deque>
#include <folly/futures/Future.h>

namespace folly {
class Executor;
}

namespace qclient
{
class QClient;
}

EOSNSNAMESPACE_BEGIN

class IView;

class ExpansionDecider {
public:
  //----------------------------------------------------------------------------
  //! Returns whether to expand the given container, or ignore it.
  //! Useful to filter out certain parts of the namespace tree.
  //----------------------------------------------------------------------------
  virtual bool shouldExpandContainer(
    const eos::ns::ContainerMdProto &containerMd,
    const eos::IContainerMD::XAttrMap &linkedAttrs) = 0;

};

struct ExplorationOptions {
  int depthLimit;
  std::shared_ptr<ExpansionDecider> expansionDecider;
  bool populateLinkedAttributes = false;
  bool prefixLinks = false; // only relevant if populateLinkedAttributes is true

  //----------------------------------------------------------------------------
  // You must supply the view if populateLinkedAttributes = true
  //----------------------------------------------------------------------------
  eos::IView *view = nullptr;

  //----------------------------------------------------------------------------
  // Ignore files?
  //----------------------------------------------------------------------------
  bool ignoreFiles = false;
};

struct NamespaceItem {
  // A simple string for now, we can extend this later.
  std::string fullPath;

  //----------------------------------------------------------------------------
  //! Extended attributes map: Only filled out if populateLinkedAttributes was
  //! set.
  //----------------------------------------------------------------------------
  eos::IContainerMD::XAttrMap attrs;
  bool isFile;
  bool expansionFilteredOut;

  // Only one of these are actually filled out.
  eos::ns::FileMdProto fileMd;
  eos::ns::ContainerMdProto containerMd;
};

class NamespaceExplorer;

//------------------------------------------------------------------------------
//! Represents a node in the search tree.
//------------------------------------------------------------------------------
class SearchNode
{
public:
  SearchNode(NamespaceExplorer &explorer, ContainerIdentifier expectedParent,
    ContainerIdentifier id, SearchNode* prnt, folly::Executor *exec, bool ignoreFiles);

  inline ContainerIdentifier getID() const
  {
    return id;
  }

  // Return false if this node has no more files to output
  bool fetchChild(eos::ns::FileMdProto& output); // sync, block if not available

  // Handle asynchronous operations - call this as often as possible!
  void handleAsync();

  // Explicit transfer of ownership
  std::unique_ptr<SearchNode> expand();

  // Activate
  void activate();

  // Clear children.
  void prefetchChildren();

  //----------------------------------------------------------------------------
  //! Can we visit this node? Possible only if:
  //! - No errors occurred while retrieving the container's metadata.
  //! - Has not been visited already.
  //----------------------------------------------------------------------------
  bool canVisit();

  inline void visit()
  {
    visited = true;
  }

  eos::ns::ContainerMdProto& getContainerInfo();
  bool expansionFilteredOut = false;

private:
  NamespaceExplorer &explorer;
  ContainerIdentifier expectedParent;
  ContainerIdentifier id;
  qclient::QClient& qcl;
  SearchNode* parent = nullptr;
  folly::Executor *executor = nullptr;
  bool ignoreFiles;

  bool visited = false;

  common::FutureWrapper<eos::ns::ContainerMdProto> containerMd;
  common::FutureWrapper<IContainerMD::ContainerMap> containerMap;

  FutureVectorIterator<eos::ns::FileMdProto> pendingFileMds;

  std::deque<std::unique_ptr<SearchNode>> children; // expanded containers
  bool childrenLoaded = false;

  eos::IContainerMD::XAttrMap attrs;

  // @todo (gbitzes): Replace this mess with a nice iterator object which
  // provides all children of a container, fully asynchronous with prefetching.
  void stageChildren();
};

//------------------------------------------------------------------------------
//! Class to recursively explore the QuarkDB namespace, starting from some path.
//! Useful for "Find" commands - no consistency guarantees, if a write is in
//! the flusher, it might not be seen here.
//!
//! Implemented by simple DFS on the namespace.
//------------------------------------------------------------------------------
class NamespaceExplorer
{
public:
  //----------------------------------------------------------------------------
  //! Inject the QClient to use directly in the constructor. No ownership of
  //! underlying object.
  //----------------------------------------------------------------------------
  NamespaceExplorer(const std::string& path, const ExplorationOptions& options,
                    qclient::QClient& qcl, folly::Executor *exec);

  //----------------------------------------------------------------------------
  //! Fetch next item.
  //----------------------------------------------------------------------------
  bool fetch(NamespaceItem& result);

private:
  friend class SearchNode;
  std::string buildStaticPath();
  std::string buildDfsPath();

  //----------------------------------------------------------------------------
  // Handle linked attributes
  //----------------------------------------------------------------------------
  void handleLinkedAttrs(NamespaceItem& result);

  //----------------------------------------------------------------------------
  // Retrieve linked container for  Handle linked attributes
  //----------------------------------------------------------------------------
  std::string path;
  ExplorationOptions options;
  qclient::QClient& qcl;
  folly::Executor* executor;

  std::vector<eos::ns::ContainerMdProto> staticPath;
  eos::ns::FileMdProto lastChunk;
  bool searchOnFile = false;
  bool searchOnFileEnded = false;

  std::vector<std::unique_ptr<SearchNode>> dfsPath;
  std::map<std::string, eos::IContainerMD::XAttrMap> cachedAttrs;
};

EOSNSNAMESPACE_END
