//------------------------------------------------------------------------------
//! @file TreeSizePublisher.cc
//! @brief Tree-size recomputation publish target planning
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizePublisher.hh"
#include "namespace/MDException.hh"
#include "namespace/MDLocking.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include <algorithm>
#include <exception>
#include <memory>
#include <unordered_set>

EOSNSNAMESPACE_BEGIN

namespace {

bool
HasNegativeCounters(const TreeSizeSubtreeCounters& counters)
{
  return (counters.treeBytes < 0) || (counters.treeFiles < 0) ||
         (counters.treeContainers < 0);
}

std::string
NormalizePublishError(const std::string& error)
{
  return error.empty() ? "tree-size counter publish failed" : error;
}

bool
HasPublishDiagnostics(const TreeSizePublishDiagnostics& diagnostics)
{
  return (diagnostics.missingCounters != 0) || (diagnostics.negativeCounters != 0) ||
         (diagnostics.duplicateTargets != 0) || (diagnostics.unplannedCounters != 0);
}

void
AddUniqueId(std::vector<uint64_t>& ids, uint64_t id)
{
  if (std::find(ids.begin(), ids.end(), id) == ids.end()) {
    ids.push_back(id);
  }
}

void
SetFirstError(std::string& target, const std::string& error)
{
  if (target.empty()) {
    target = error;
  }
}

} // namespace

TreeSizePublishPlanResult
TreeSizePublisher::Plan(
    const std::vector<uint64_t>& publish_order,
    const std::unordered_map<uint64_t, TreeSizeSubtreeCounters>& counters) const
{
  TreeSizePublishPlanResult result;
  std::unordered_set<uint64_t> planned_ids;
  planned_ids.reserve(publish_order.size());
  result.targets.reserve(publish_order.size());

  for (const auto container_id : publish_order) {
    if (!planned_ids.insert(container_id).second) {
      ++result.diagnostics.duplicateTargets;
      continue;
    }

    const auto counter = counters.find(container_id);

    if (counter == counters.end()) {
      ++result.diagnostics.missingCounters;
      continue;
    }

    if (HasNegativeCounters(counter->second)) {
      ++result.diagnostics.negativeCounters;
      continue;
    }

    result.targets.push_back(TreeSizePublishTarget{container_id, counter->second});
  }

  for (const auto& counter : counters) {
    if (planned_ids.count(counter.first) == 0) {
      ++result.diagnostics.unplannedCounters;
    }
  }

  return result;
}

TreeSizeMetadataPublisher::TreeSizeMetadataPublisher(IContainerMDSvc& container_svc)
    : mContainerSvc(container_svc)
{
}

TreeSizeCounterPublishResult
TreeSizeMetadataPublisher::PublishTreeSizeCounters(const TreeSizePublishTarget& target)
{
  std::shared_ptr<IContainerMD> container;

  try {
    container = mContainerSvc.getContainerMD(target.containerId);
  } catch (const eos::MDException&) {
    return TreeSizeCounterPublishResult{TreeSizeCounterPublishStatus::SkippedMissing, ""};
  }

  try {
    eos::MDLocking::ContainerWriteLock write_lock(container.get());
    container->setTreeSize(static_cast<uint64_t>(target.counters.treeBytes));
    container->setTreeFiles(static_cast<uint64_t>(target.counters.treeFiles));
    container->setTreeContainers(static_cast<uint64_t>(target.counters.treeContainers));
    mContainerSvc.updateStore(container.get());
    return TreeSizeCounterPublishResult{};
  } catch (const eos::MDException& e) {
    return TreeSizeCounterPublishResult{TreeSizeCounterPublishStatus::Failed, e.what()};
  } catch (const std::exception& e) {
    return TreeSizeCounterPublishResult{TreeSizeCounterPublishStatus::Failed, e.what()};
  } catch (...) {
    return TreeSizeCounterPublishResult{
        TreeSizeCounterPublishStatus::Failed,
        "tree-size counter publish failed with unknown exception"};
  }
}

TreeSizePublishApplyResult
TreeSizePublisher::Apply(const TreeSizePublishPlanResult& plan,
                         ITreeSizeCounterPublisher& publisher) const
{
  TreeSizePublishApplyResult result;
  const auto& targets = plan.targets;
  result.publishedContainerIds.reserve(targets.size());
  result.retryContainerIds.reserve(targets.size());

  if (HasPublishDiagnostics(plan.diagnostics)) {
    for (const auto& target : targets) {
      AddUniqueId(result.retryContainerIds, target.containerId);
    }
  }

  for (const auto& target : targets) {
    ++result.attemptedTargets;
    TreeSizeCounterPublishResult publish_result;

    try {
      publish_result = publisher.PublishTreeSizeCounters(target);
    } catch (const std::exception& e) {
      result.writeFailedContainerIds.push_back(target.containerId);
      AddUniqueId(result.retryContainerIds, target.containerId);

      if (result.failedContainerId == 0) {
        result.failedContainerId = target.containerId;
      }

      SetFirstError(result.error, NormalizePublishError(e.what()));
      continue;
    } catch (...) {
      result.writeFailedContainerIds.push_back(target.containerId);
      AddUniqueId(result.retryContainerIds, target.containerId);

      if (result.failedContainerId == 0) {
        result.failedContainerId = target.containerId;
      }

      SetFirstError(result.error,
                    "tree-size counter publish failed with unknown exception");
      continue;
    }

    switch (publish_result.status) {
    case TreeSizeCounterPublishStatus::Published:
      result.publishedContainerIds.push_back(target.containerId);
      break;
    case TreeSizeCounterPublishStatus::SkippedMissing:
      ++result.skippedMissingTargets;
      result.missingContainerIds.push_back(target.containerId);
      AddUniqueId(result.retryContainerIds, target.containerId);
      break;
    case TreeSizeCounterPublishStatus::Failed:
      result.writeFailedContainerIds.push_back(target.containerId);
      AddUniqueId(result.retryContainerIds, target.containerId);

      if (result.failedContainerId == 0) {
        result.failedContainerId = target.containerId;
      }

      SetFirstError(result.error, NormalizePublishError(publish_result.error));
      break;
    }
  }

  if (!result.writeFailedContainerIds.empty()) {
    result.status = result.publishedContainerIds.empty()
                        ? TreeSizePublishApplyStatus::PrePublishFailed
                        : TreeSizePublishApplyStatus::PartialPublishFailed;
  }

  return result;
}

EOSNSNAMESPACE_END
