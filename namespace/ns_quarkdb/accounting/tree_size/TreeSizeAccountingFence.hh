//------------------------------------------------------------------------------
//! @file TreeSizeAccountingFence.hh
//! @brief Fence live tree-size accounting while publishing recomputed counters
//------------------------------------------------------------------------------

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

#pragma once

#include "namespace/Namespace.hh"
#include <cstdint>
#include <unordered_set>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! How a tree-size accounting fence should release held live updates
//------------------------------------------------------------------------------
enum class TreeSizeAccountingFenceReleaseMode {
  AbortBeforePublish = 0,
  PublishSucceeded
};

//------------------------------------------------------------------------------
//! Request to acquire a fence for one validated publish window
//------------------------------------------------------------------------------
struct TreeSizeAccountingFenceRequest {
  std::unordered_set<uint64_t> coveredContainerIds;
  uint64_t validatedThroughSequence = 0;
};

//------------------------------------------------------------------------------
//! Counters describing how live accounting updates were handled by the fence
//------------------------------------------------------------------------------
struct TreeSizeAccountingFenceStats {
  bool acquired = false;
  uint64_t coveredContainerIds = 0;
  uint64_t includedInPublishUpdates = 0;
  uint64_t includedSubtreeAttachUpdates = 0;
  uint64_t includedSubtreeDetachUpdates = 0;
  uint64_t replayAfterPublishUpdates = 0;
  uint64_t unsequencedCoveredUpdates = 0;
  uint64_t passedThroughUpdates = 0;
  uint64_t drainedRawQueueUpdates = 0;
  uint64_t drainedBatchUpdates = 0;
  uint64_t inFlightCoveredUpdates = 0;
  bool inFlightWaitTimeout = false;
};

//------------------------------------------------------------------------------
//! Interface for components that can fence live tree-size accounting updates
//------------------------------------------------------------------------------
class ITreeSizeAccountingFence {
public:
  virtual ~ITreeSizeAccountingFence() = default;

  virtual TreeSizeAccountingFenceStats
  AcquireTreeSizeAccountingFence(const TreeSizeAccountingFenceRequest& request) = 0;

  virtual TreeSizeAccountingFenceStats
  ReleaseTreeSizeAccountingFence(TreeSizeAccountingFenceReleaseMode mode) = 0;
};

EOSNSNAMESPACE_END
