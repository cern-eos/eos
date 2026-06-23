/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
//! @brief Helpers to keep directory hierarchies within the maximum resolvable
//!        path depth when moving/renaming subtrees.
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include <cstddef>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Only scan a subtree for depth violations when the move/rename destination
//! sits within this many levels of the maximum allowed depth. For shallower
//! destinations a depth violation would require an implausibly deep source
//! subtree, so the (potentially expensive) scan is skipped and the create-time
//! depth guard is relied upon instead.
//------------------------------------------------------------------------------
static constexpr std::size_t kSubtreeDepthScanThreshold = 64;

//------------------------------------------------------------------------------
//! Maximum number of containers visited by subtreeExceedsRelDepth before it
//! gives up. Bounds the cost of the scan when it runs while the namespace view
//! lock is held; callers should treat exhaustion as "cannot verify".
//------------------------------------------------------------------------------
static constexpr std::size_t kSubtreeDepthScanBudget = 10000;

//------------------------------------------------------------------------------
//! Check whether the subtree rooted at @p source contains any directory more
//! than @p maxRelDepth levels below it (@p source itself is level 0).
//!
//! The traversal is bounded so it can be run safely while holding the
//! namespace view lock:
//!   - it never fetches containers beyond maxRelDepth+1 levels (the deepest
//!     level is detected through getNumContainers() without loading the
//!     children), and
//!   - it visits at most @p nodeBudget containers.
//!
//! @param svc            container metadata service used to fetch children
//! @param source         root of the subtree; the caller must hold a lock
//!                        covering it (e.g. the view lock)
//! @param maxRelDepth    maximum allowed relative depth of any descendant
//! @param nodeBudget     maximum number of containers to visit
//! @param budgetExceeded set to true if the scan stopped because nodeBudget
//!                        was reached before reaching a verdict
//!
//! @return true if a descendant deeper than maxRelDepth exists; false
//!         otherwise (also false when budgetExceeded is set)
//------------------------------------------------------------------------------
inline bool
subtreeExceedsRelDepth(IContainerMDSvc* svc, IContainerMDPtr source,
                       std::size_t maxRelDepth, std::size_t nodeBudget,
                       bool& budgetExceeded)
{
  budgetExceeded = false;
  std::vector<std::pair<IContainerMDPtr, std::size_t>> stack;
  stack.emplace_back(std::move(source), 0);
  std::size_t visited = 0;

  while (!stack.empty()) {
    IContainerMDPtr cont = std::move(stack.back().first);
    const std::size_t depth = stack.back().second;
    stack.pop_back();

    if (!cont) {
      continue;
    }

    if (++visited > nodeBudget) {
      budgetExceeded = true;
      return false;
    }

    if (cont->getNumContainers() == 0) {
      continue;
    }

    // Every child sits one level below. If that already exceeds the limit we
    // have a verdict without having to load the children themselves.
    if (depth + 1 > maxRelDepth) {
      return true;
    }

    for (auto it = eos::ContainerMapIterator(cont); it.valid(); it.next()) {
      stack.emplace_back(svc->getContainerMD(it.value()), depth + 1);
    }
  }

  return false;
}

EOSNSNAMESPACE_END
