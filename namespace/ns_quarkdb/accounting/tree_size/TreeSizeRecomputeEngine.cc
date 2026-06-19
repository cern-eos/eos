//------------------------------------------------------------------------------
//! @file TreeSizeRecomputeEngine.cc
//! @brief Synchronous tree-size recompute workflow
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputeEngine.hh"
#include "common/Logging.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingSequencer.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingService.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeCoverageClassifier.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournalCaptureScope.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeMissingMetadataRepair.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizePublisher.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputePublishDecision.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeReconciler.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeSnapshotBuilder.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeTopologyComposer.hh"
#include <algorithm>
#include <cerrno>
#include <exception>
#include <functional>
#include <list>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

namespace {

constexpr uint64_t kTreeSizeContainerProgressInterval = 256;
constexpr uint64_t kTreeSizeFileProgressInterval = 4096;

using TreeSizeDiscoveryProgressCallback =
    std::function<void(uint64_t discovered_containers)>;
using TreeSizeSnapshotFileProgressCallback = std::function<void(uint64_t snapshot_files)>;

bool
ShouldReportProgress(uint64_t value, uint64_t interval)
{
  return (value == 1) || ((interval != 0) && ((value % interval) == 0));
}

bool
StopRequested(const TreeSizeRecomputeEngineCallbacks& callbacks)
{
  return callbacks.stopRequested && callbacks.stopRequested();
}

TreeSizeSnapshotFile
BuildTreeSizeSnapshotFile(uint64_t file_id, IFileMD* file)
{
  TreeSizeSnapshotFile snapshot;
  snapshot.id = file_id;

  if (auto* quark_file = dynamic_cast<QuarkFileMD*>(file)) {
    const auto size_snapshot = quark_file->getTreeSizeSizeSnapshot();
    snapshot.size = size_snapshot.size;
    snapshot.sizeSnapshotSequence = size_snapshot.sequence;
    return snapshot;
  }

  snapshot.size = file->getSize();
  return snapshot;
}

void
NotifyPhase(const TreeSizeRecomputeEngineCallbacks& callbacks, const std::string& phase)
{
  if (callbacks.updatePhase) {
    callbacks.updatePhase(phase);
  }
}

void
NotifyProgress(const TreeSizeRecomputeEngineCallbacks& callbacks,
               uint64_t discovered_containers, uint64_t snapshot_containers,
               uint64_t snapshot_files, uint64_t publish_targets,
               uint64_t publish_applied_targets)
{
  if (callbacks.updateProgress) {
    callbacks.updateProgress(discovered_containers, snapshot_containers, snapshot_files,
                             publish_targets, publish_applied_targets);
  }
}

void
NotifyDiagnostics(const TreeSizeRecomputeEngineCallbacks& callbacks,
                  const TreeSizeRecomputeDiagnostics& diagnostics)
{
  if (callbacks.updateDiagnostics) {
    callbacks.updateDiagnostics(diagnostics);
  }
}

TreeSizeSnapshotContainer
BuildTreeSizeSnapshotContainer(
    IContainerMDPtr cont, IFileMDSvc& file_svc,
    std::vector<TreeSizeMissingMetadataReference>& missing_metadata_references,
    uint64_t& snapshot_files,
    const TreeSizeSnapshotFileProgressCallback& file_progress_callback)
{
  TreeSizeSnapshotContainer snapshot;
  snapshot.id = cont->getId();
  std::shared_ptr<IFileMD> tmp_fmd{nullptr};

  if (auto* quark_cont = dynamic_cast<QuarkContainerMD*>(cont.get())) {
    const auto membership_snapshot = quark_cont->getTreeSizeMembershipSnapshot();
    snapshot.fileMembershipSnapshotSequence = membership_snapshot.fileMembershipSequence;
    snapshot.childMembershipSnapshotSequence =
        membership_snapshot.childMembershipSequence;

    for (const auto file_id : membership_snapshot.fileIds) {
      try {
        tmp_fmd = file_svc.getFileMD(file_id);
      } catch (const MDException&) {
        missing_metadata_references.push_back(TreeSizeMissingMetadataReference{
            TreeSizeMissingMetadataKind::File, cont->getId(), file_id});
        continue;
      }

      snapshot.files.push_back(BuildTreeSizeSnapshotFile(file_id, tmp_fmd.get()));
      ++snapshot_files;

      if (file_progress_callback &&
          ShouldReportProgress(snapshot_files, kTreeSizeFileProgressInterval)) {
        file_progress_callback(snapshot_files);
      }
    }

    snapshot.childContainerIds = membership_snapshot.childContainerIds;

    return snapshot;
  }

  for (auto fit = FileMapIterator(cont); fit.valid(); fit.next()) {
    try {
      tmp_fmd = file_svc.getFileMD(fit.value());
    } catch (const MDException&) {
      missing_metadata_references.push_back(TreeSizeMissingMetadataReference{
          TreeSizeMissingMetadataKind::File, cont->getId(), fit.value()});
      continue;
    }

    snapshot.files.push_back(BuildTreeSizeSnapshotFile(fit.value(), tmp_fmd.get()));
    ++snapshot_files;

    if (file_progress_callback &&
        ShouldReportProgress(snapshot_files, kTreeSizeFileProgressInterval)) {
      file_progress_callback(snapshot_files);
    }
  }

  for (auto cit = ContainerMapIterator(cont); cit.valid(); cit.next()) {
    snapshot.childContainerIds.push_back(cit.value());
  }

  return snapshot;
}

std::unordered_set<uint64_t>
BuildTreeSizeCoveredIds(const TreeSizeSnapshot& snapshot)
{
  std::unordered_set<uint64_t> covered_ids;
  covered_ids.reserve(snapshot.directCounters.size());

  for (const auto& direct_counter : snapshot.directCounters) {
    covered_ids.insert(direct_counter.first);
  }

  return covered_ids;
}

std::vector<uint64_t>
SortedUnique(std::vector<uint64_t> values)
{
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
  return values;
}

void
AddUniqueId(std::vector<uint64_t>& ids, uint64_t id)
{
  if (std::find(ids.begin(), ids.end(), id) == ids.end()) {
    ids.push_back(id);
  }
}

std::vector<uint64_t>
BuildTreeSizePublishOrder(
    uint64_t root_id,
    const std::unordered_map<uint64_t, std::vector<uint64_t>>& child_container_ids)
{
  struct Frame {
    uint64_t id = 0;
    bool expanded = false;
  };

  std::vector<uint64_t> order;
  std::vector<Frame> stack;
  std::unordered_set<uint64_t> visiting;
  std::unordered_set<uint64_t> done;
  stack.push_back(Frame{root_id, false});

  while (!stack.empty()) {
    const auto frame = stack.back();
    stack.pop_back();

    if (done.count(frame.id) != 0) {
      continue;
    }

    if (frame.expanded) {
      visiting.erase(frame.id);
      done.insert(frame.id);
      order.push_back(frame.id);
      continue;
    }

    if (!visiting.insert(frame.id).second) {
      continue;
    }

    stack.push_back(Frame{frame.id, true});
    const auto children = child_container_ids.find(frame.id);

    if (children == child_container_ids.end()) {
      continue;
    }

    auto sorted_children = SortedUnique(children->second);

    for (auto it = sorted_children.rbegin(); it != sorted_children.rend(); ++it) {
      if ((done.count(*it) == 0) && (visiting.count(*it) == 0)) {
        stack.push_back(Frame{*it, false});
      }
    }
  }

  return order;
}

TreeSizeRecomputePublishDiagnostics
BuildTreeSizePublishDiagnostics(
    const TreeSizeJournalSnapshot& journal,
    const TreeSizeMissingMetadataRepairResult& missing_metadata_repair,
    const TreeSizeCoverageDiagnostics& coverage,
    const TreeSizeReconcileResult& reconcile_result,
    const TreeSizeTopologyComposeResult& topology_result,
    const TreeSizePublishPlanResult& publish_plan)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.missingJournalMetadata = journal.diagnostics.missingMetadata;
  diagnostics.missingReconcileMetadata = reconcile_result.diagnostics.missingMetadata;
  diagnostics.missingCoverageMetadata = coverage.missingMetadataEntries;
  diagnostics.postDiscoveryTopologyEntries = coverage.postDiscoveryTopologyEntries;
  diagnostics.postDiscoveryContainerIds = coverage.postDiscoveryContainerIds;
  diagnostics.discoveryMissingMetadata = missing_metadata_repair.missingReferences;
  diagnostics.resolvedDiscoveryMissingMetadata =
      missing_metadata_repair.resolvedReferences;
  diagnostics.unresolvedDiscoveryMissingMetadata =
      missing_metadata_repair.unresolvedReferences;
  diagnostics.suppressedJournalEntries = reconcile_result.diagnostics.suppressedEntries;
  diagnostics.unknownParents = reconcile_result.diagnostics.unknownParents;
  diagnostics.negativeCounters = reconcile_result.diagnostics.negativeCounters;
  diagnostics.unsupportedEvents = reconcile_result.diagnostics.unsupportedEvents;
  diagnostics.missingDirectCounters = topology_result.diagnostics.missingDirectCounters;
  diagnostics.missingTopology = topology_result.diagnostics.missingTopology;
  diagnostics.cycleEdges = topology_result.diagnostics.cycleEdges;
  diagnostics.publishMissingCounters = publish_plan.diagnostics.missingCounters;
  diagnostics.publishNegativeCounters = publish_plan.diagnostics.negativeCounters;
  diagnostics.publishDuplicateTargets = publish_plan.diagnostics.duplicateTargets;
  diagnostics.publishUnplannedCounters = publish_plan.diagnostics.unplannedCounters;
  return diagnostics;
}

std::list<std::list<IContainerMD::id_t>>
BreadthFirstSearchContainers(
    IContainerMD* cont, IContainerMDSvc& container_svc, uint32_t max_depth,
    std::vector<TreeSizeMissingMetadataReference>* missing_metadata_references,
    std::unordered_map<IContainerMD::id_t, IContainerMD::id_t>* discovered_parent_ids,
    uint64_t* discovered_containers,
    const TreeSizeDiscoveryProgressCallback& discovery_progress_callback)
{
  uint32_t num_levels = 0u;
  std::shared_ptr<IContainerMD> tmp_cont;
  std::list<std::list<IContainerMD::id_t>> depth(256);
  auto it_lvl = depth.begin();
  it_lvl->push_back(cont->getId());

  if (discovered_parent_ids) {
    (*discovered_parent_ids)[cont->getId()] = 0;
  }

  if (discovered_containers) {
    *discovered_containers = 1;

    if (discovery_progress_callback &&
        ShouldReportProgress(*discovered_containers,
                             kTreeSizeContainerProgressInterval)) {
      discovery_progress_callback(*discovered_containers);
    }
  }

  while ((it_lvl != depth.end()) && it_lvl->size()) {
    auto it_next_lvl = it_lvl;
    ++it_next_lvl;

    for (const auto& cid : *it_lvl) {
      try {
        tmp_cont = container_svc.getContainerMD(cid);
      } catch (const MDException& e) {
        if (missing_metadata_references) {
          uint64_t parent_id = 0;

          if (discovered_parent_ids) {
            const auto parent = discovered_parent_ids->find(cid);

            if (parent != discovered_parent_ids->end()) {
              parent_id = parent->second;
            }
          }

          missing_metadata_references->push_back(TreeSizeMissingMetadataReference{
              TreeSizeMissingMetadataKind::Container, parent_id, cid});
        } else {
          eos_static_err("error=\"%s\"", e.what());
        }

        continue;
      }

      for (auto subcont_it = ContainerMapIterator(tmp_cont); subcont_it.valid();
           subcont_it.next()) {
        if (it_next_lvl != depth.end()) {
          it_next_lvl->push_back(subcont_it.value());

          if (discovered_parent_ids) {
            (*discovered_parent_ids)[subcont_it.value()] = cid;
          }

          if (discovered_containers) {
            ++(*discovered_containers);

            if (discovery_progress_callback &&
                ShouldReportProgress(*discovered_containers,
                                     kTreeSizeContainerProgressInterval)) {
              discovery_progress_callback(*discovered_containers);
            }
          }
        } else {
          eos_static_notice("msg=\"reached maximum hierarchy depth\" "
                            "cxid=%08llx",
                            cid);
        }
      }
    }

    it_lvl = it_next_lvl;
    ++num_levels;

    if (max_depth && (num_levels == max_depth)) {
      break;
    }
  }

  depth.resize(num_levels);
  return depth;
}

TreeSizeRecomputeResult
MakeResult(int retc, std::string error = {})
{
  TreeSizeRecomputeResult result;
  result.retc = retc;
  result.error = std::move(error);
  return result;
}

std::string
BuildRetryReason(const TreeSizeRecomputePublishDecisionResult& decision,
                 const std::string& decision_reasons,
                 const TreeSizeAccountingFenceStats& fence_stats,
                 bool fence_blocked_by_unsequenced_update,
                 const TreeSizePublishApplyResult& publish_apply)
{
  if (decision.retryRequired) {
    return decision_reasons;
  }

  if (!fence_stats.acquired) {
    return "publish_fence_not_acquired";
  }

  if (fence_blocked_by_unsequenced_update) {
    return "unsequenced_covered_updates";
  }

  if (!publish_apply.writeFailedContainerIds.empty()) {
    return "write_failed_targets";
  }

  if (!publish_apply.missingContainerIds.empty()) {
    return "missing_targets";
  }

  if (!publish_apply.retryContainerIds.empty()) {
    return "publish_retry_targets";
  }

  return {};
}

} // namespace

TreeSizeRecomputeResult
TreeSizeRecomputeEngine::Recompute(
    const TreeSizeRecomputeRequest& request, IContainerMDSvc& container_svc,
    IFileMDSvc& file_svc, ITreeSizeAccountingService* accounting_service,
    const TreeSizeRecomputeEngineCallbacks& callbacks) const
{
  if (request.rootId == 0) {
    return MakeResult(ENOENT, "error: container not found");
  }

  const auto root_id = request.rootId;

  if (StopRequested(callbacks)) {
    return MakeResult(ECANCELED, "tree-size recompute stopped before execution");
  }

  if (!accounting_service) {
    eos_static_warning("msg=\"tree size recompute accounting service unavailable\" "
                       "cid=%llu root=\"%s\"",
                       static_cast<unsigned long long>(root_id),
                       request.rootSpecification.c_str());
    return MakeResult(ENOTSUP, "error: tree-size accounting service unavailable");
  }

  TreeSizeRecomputeDiagnostics attempt_diagnostics;
  NotifyDiagnostics(callbacks, attempt_diagnostics);
  NotifyPhase(callbacks, "loading_root");

  std::shared_ptr<IContainerMD> root_cont;

  try {
    root_cont = container_svc.getContainerMD(root_id);
  } catch (const MDException& e) {
    attempt_diagnostics.available = true;
    attempt_diagnostics.converged = false;
    attempt_diagnostics.publishMissingTargets = 1;
    attempt_diagnostics.missingContainerIds.push_back(root_id);
    NotifyDiagnostics(callbacks, attempt_diagnostics);
    return MakeResult(e.getErrno(), e.what());
  }

  if (root_cont == nullptr) {
    attempt_diagnostics.available = true;
    attempt_diagnostics.converged = false;
    attempt_diagnostics.publishMissingTargets = 1;
    attempt_diagnostics.missingContainerIds.push_back(root_id);
    NotifyDiagnostics(callbacks, attempt_diagnostics);
    return MakeResult(ENOENT, "error: container not found");
  }

  std::unique_ptr<TreeSizeJournalCaptureScope> capture_scope;
  uint64_t baseline_sequence = 0;

  try {
    NotifyPhase(callbacks, "capturing");
    capture_scope = accounting_service->StartTreeSizeJournalCapture();

    if (!capture_scope) {
      throw std::runtime_error("tree-size journal capture unavailable");
    }

    baseline_sequence = GetTreeSizeAccountingSequencer().LastReserved();
  } catch (const std::exception& e) {
    eos_static_warning("msg=\"tree size recompute start failed\" "
                       "cid=%llu root=\"%s\" error=\"%s\"",
                       static_cast<unsigned long long>(root_id),
                       request.rootSpecification.c_str(), e.what());
    return MakeResult(EIO, std::string("error: tree-size recompute start failed: ") +
                               e.what());
  }

  if (StopRequested(callbacks)) {
    return MakeResult(ECANCELED, "tree-size recompute stopped before discovery");
  }

  std::shared_ptr<IContainerMD> tmp_cont{nullptr};
  std::vector<TreeSizeMissingMetadataReference> missing_metadata_references;
  std::unordered_map<IContainerMD::id_t, IContainerMD::id_t> discovered_parent_ids;
  std::vector<TreeSizeSnapshotContainer> snapshot_containers;
  uint64_t discovered_containers = 0;
  uint64_t snapshot_file_count = 0;
  NotifyPhase(callbacks, "discovering");
  std::list<std::list<IContainerMD::id_t>> bfs = BreadthFirstSearchContainers(
      root_cont.get(), container_svc, request.maxDepth, &missing_metadata_references,
      &discovered_parent_ids, &discovered_containers,
      [&callbacks, &discovered_containers, &snapshot_containers,
       &snapshot_file_count](uint64_t) {
        NotifyProgress(callbacks, discovered_containers, snapshot_containers.size(),
                       snapshot_file_count, 0, 0);
      });

  NotifyPhase(callbacks, "snapshotting");
  NotifyProgress(callbacks, discovered_containers, snapshot_containers.size(),
                 snapshot_file_count, 0, 0);

  for (auto it_level = bfs.crbegin(); it_level != bfs.crend(); ++it_level) {
    for (const auto& id : *it_level) {
      if (StopRequested(callbacks)) {
        return MakeResult(ECANCELED, "tree-size recompute stopped during discovery");
      }

      try {
        tmp_cont = container_svc.getContainerMD(id);
      } catch (const MDException&) {
        uint64_t parent_id = 0;
        const auto parent = discovered_parent_ids.find(id);

        if (parent != discovered_parent_ids.end()) {
          parent_id = parent->second;
        }

        missing_metadata_references.push_back(TreeSizeMissingMetadataReference{
            TreeSizeMissingMetadataKind::Container, parent_id, id});
        continue;
      }

      snapshot_containers.push_back(BuildTreeSizeSnapshotContainer(
          tmp_cont, file_svc, missing_metadata_references, snapshot_file_count,
          [&callbacks, &discovered_containers,
           &snapshot_containers](uint64_t snapshot_files) {
            NotifyProgress(callbacks, discovered_containers, snapshot_containers.size(),
                           snapshot_files, 0, 0);
          }));

      if ((snapshot_containers.size() == 1) ||
          ((snapshot_containers.size() % kTreeSizeContainerProgressInterval) == 0)) {
        NotifyProgress(callbacks, discovered_containers, snapshot_containers.size(),
                       snapshot_file_count, 0, 0);
      }
    }
  }

  NotifyProgress(callbacks, discovered_containers, snapshot_containers.size(),
                 snapshot_file_count, 0, 0);

  try {
    if (StopRequested(callbacks)) {
      return MakeResult(ECANCELED, "tree-size recompute stopped before publish");
    }

    NotifyPhase(callbacks, "reconciling");
    const auto journal_snapshot = capture_scope->StopAndSnapshot();
    capture_scope.reset();
    const auto snapshot = TreeSizeSnapshotBuilder().Build(snapshot_containers);
    auto reported_snapshot_containers = snapshot.directCounters.size();
    auto reported_snapshot_files = snapshot_file_count;
    auto missing_metadata_repair = TreeSizeMissingMetadataRepair().Repair(
        missing_metadata_references, snapshot, journal_snapshot);
    auto covered_ids = BuildTreeSizeCoveredIds(snapshot);
    auto coverage = TreeSizeCoverageClassifier().Classify(covered_ids, journal_snapshot);
    TreeSizeReconcileOptions reconcile_options;
    reconcile_options.suppressedSequences = missing_metadata_repair.suppressedSequences;
    auto reconcile_result =
        TreeSizeReconciler().Reconcile(snapshot, journal_snapshot, reconcile_options);
    auto reconciled_snapshot = snapshot;
    reconciled_snapshot.childContainerIds = reconcile_result.childContainerIds;
    auto topology_result = TreeSizeTopologyComposer().Compose(
        reconciled_snapshot, reconcile_result.directCounters);
    auto publish_order =
        BuildTreeSizePublishOrder(root_id, reconciled_snapshot.childContainerIds);
    auto publish_plan =
        TreeSizePublisher().Plan(publish_order, topology_result.subtreeCounters);
    NotifyPhase(callbacks, "validating");
    NotifyProgress(callbacks, discovered_containers, reported_snapshot_containers,
                   reported_snapshot_files, publish_plan.targets.size(), 0);
    auto publish_diagnostics = BuildTreeSizePublishDiagnostics(
        journal_snapshot, missing_metadata_repair, coverage, reconcile_result,
        topology_result, publish_plan);
    auto decision = TreeSizeRecomputePublishDecision().Evaluate(publish_diagnostics);
    auto decision_reasons =
        TreeSizeRecomputePublishDecision::ReasonsToString(decision.reasonMask);

    bool fence_attempted = false;
    uint64_t fence_validated_sequence = 0;
    TreeSizeAccountingFenceStats fence_stats;
    TreeSizePublishApplyResult publish_apply;
    bool fence_blocked_by_unsequenced_update = false;
    const bool has_publish_targets = !publish_plan.targets.empty();

    if (has_publish_targets && accounting_service) {
      NotifyPhase(callbacks, "publishing");
      TreeSizeAccountingFenceRequest fence_request;
      fence_request.coveredContainerIds = covered_ids;
      fence_request.validatedThroughSequence =
          std::max({baseline_sequence, journal_snapshot.latestSequence,
                    snapshot.latestSnapshotSequence});
      fence_validated_sequence = fence_request.validatedThroughSequence;
      fence_attempted = true;
      fence_stats = accounting_service->AcquireTreeSizeAccountingFence(fence_request);

      if (fence_stats.acquired) {
        if (fence_stats.unsequencedCoveredUpdates != 0) {
          fence_blocked_by_unsequenced_update = true;
          fence_stats = accounting_service->ReleaseTreeSizeAccountingFence(
              TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
        } else {
          TreeSizeMetadataPublisher metadata_publisher(container_svc);
          publish_apply = TreeSizePublisher().Apply(publish_plan, metadata_publisher);
          const auto release_mode =
              publish_apply.publishedContainerIds.empty()
                  ? TreeSizeAccountingFenceReleaseMode::AbortBeforePublish
                  : TreeSizeAccountingFenceReleaseMode::PublishSucceeded;
          fence_stats = accounting_service->ReleaseTreeSizeAccountingFence(release_mode);
        }

        NotifyProgress(callbacks, discovered_containers, reported_snapshot_containers,
                       reported_snapshot_files, publish_plan.targets.size(),
                       publish_apply.publishedContainerIds.size());
      }
    }

    TreeSizeRecomputeDiagnostics status_diagnostics;
    const bool partial_publish_failed = !publish_apply.writeFailedContainerIds.empty() &&
                                        !publish_apply.publishedContainerIds.empty();
    std::vector<uint64_t> retry_container_ids = publish_apply.retryContainerIds;
    const bool fence_not_acquired = has_publish_targets && !fence_stats.acquired;

    if (decision.retryRequired || fence_not_acquired ||
        fence_blocked_by_unsequenced_update || !publish_apply.retryContainerIds.empty()) {
      AddUniqueId(retry_container_ids, root_id);
    }

    retry_container_ids = SortedUnique(std::move(retry_container_ids));
    const bool retryable_result = decision.retryRequired || fence_not_acquired ||
                                  fence_blocked_by_unsequenced_update ||
                                  !retry_container_ids.empty();

    status_diagnostics.available = true;
    status_diagnostics.converged = !retryable_result;
    status_diagnostics.publishable = has_publish_targets && fence_stats.acquired &&
                                     !fence_blocked_by_unsequenced_update;
    status_diagnostics.retryRequired = retryable_result;
    status_diagnostics.partialPublish = partial_publish_failed;
    status_diagnostics.retryReason =
        retryable_result
            ? BuildRetryReason(decision, decision_reasons, fence_stats,
                               fence_blocked_by_unsequenced_update, publish_apply)
            : std::string{};
    status_diagnostics.failedContainerId = publish_apply.failedContainerId;
    status_diagnostics.failedContainerCount =
        publish_apply.writeFailedContainerIds.size();
    status_diagnostics.retryCandidateCount = retry_container_ids.size();

    if (retryable_result) {
      status_diagnostics.retryRootContainerId = root_id;
    }

    status_diagnostics.retryContainerIds = retry_container_ids;
    status_diagnostics.missingContainerIds = publish_apply.missingContainerIds;
    status_diagnostics.writeFailedContainerIds = publish_apply.writeFailedContainerIds;

    status_diagnostics.discoveredContainers = discovered_containers;
    status_diagnostics.snapshotContainers = reported_snapshot_containers;
    status_diagnostics.snapshotFiles = reported_snapshot_files;
    status_diagnostics.discoveryMissingMetadata =
        missing_metadata_repair.missingReferences;
    status_diagnostics.discoveryResolvedMetadata =
        missing_metadata_repair.resolvedReferences;
    status_diagnostics.discoveryUnresolvedMetadata =
        missing_metadata_repair.unresolvedReferences;
    status_diagnostics.journalEntries = journal_snapshot.entries.size();
    status_diagnostics.baselineSequence = baseline_sequence;
    status_diagnostics.journalLatestSequence = journal_snapshot.latestSequence;
    status_diagnostics.fenceValidatedSequence = fence_validated_sequence;
    status_diagnostics.decisionReasonMask = decision.reasonMask;
    status_diagnostics.decisionReasons = decision_reasons;
    status_diagnostics.journalMissingMetadata =
        journal_snapshot.diagnostics.missingMetadata;
    status_diagnostics.nonIncreasingSequence =
        journal_snapshot.diagnostics.nonIncreasingSequence;
    status_diagnostics.reconcileMissingMetadata =
        reconcile_result.diagnostics.missingMetadata;
    status_diagnostics.reconcileSuppressedEntries =
        reconcile_result.diagnostics.suppressedEntries;
    status_diagnostics.unknownParents = reconcile_result.diagnostics.unknownParents;
    status_diagnostics.negativeCounters = reconcile_result.diagnostics.negativeCounters;
    status_diagnostics.unsupportedEvents = reconcile_result.diagnostics.unsupportedEvents;
    status_diagnostics.coverageMissingMetadata = coverage.missingMetadataEntries;
    status_diagnostics.coverageCoveredEntries = coverage.coveredEntries;
    status_diagnostics.coverageOutsideEntries = coverage.outsideCoverageEntries;
    status_diagnostics.coveragePostDiscoveryEntries =
        coverage.postDiscoveryTopologyEntries;
    status_diagnostics.coveragePostDiscoveryContainerIds =
        coverage.postDiscoveryContainerIds;
    status_diagnostics.composeMissingDirectCounters =
        topology_result.diagnostics.missingDirectCounters;
    status_diagnostics.composeMissingTopology =
        topology_result.diagnostics.missingTopology;
    status_diagnostics.composeCycleEdges = topology_result.diagnostics.cycleEdges;
    status_diagnostics.publishTargets = publish_plan.targets.size();
    status_diagnostics.publishOrderEntries = publish_order.size();
    status_diagnostics.publishMissingCounters = publish_plan.diagnostics.missingCounters;
    status_diagnostics.publishNegativeCounters =
        publish_plan.diagnostics.negativeCounters;
    status_diagnostics.publishDuplicateTargets =
        publish_plan.diagnostics.duplicateTargets;
    status_diagnostics.publishUnplannedCounters =
        publish_plan.diagnostics.unplannedCounters;
    status_diagnostics.publishApplyStatus = static_cast<uint32_t>(publish_apply.status);
    status_diagnostics.publishAttemptedTargets = publish_apply.attemptedTargets;
    status_diagnostics.publishAppliedTargets = publish_apply.publishedContainerIds.size();
    status_diagnostics.publishSkippedMissingTargets = publish_apply.skippedMissingTargets;
    status_diagnostics.publishMissingTargets = publish_apply.missingContainerIds.size();
    status_diagnostics.publishWriteFailedTargets =
        publish_apply.writeFailedContainerIds.size();
    status_diagnostics.fenceAvailable = accounting_service != nullptr;
    status_diagnostics.fenceAttempted = fence_attempted;
    status_diagnostics.fenceAcquired = fence_stats.acquired;
    status_diagnostics.fenceWaitTimeout = fence_stats.inFlightWaitTimeout;
    status_diagnostics.fenceCoveredIds = fence_stats.coveredContainerIds;
    status_diagnostics.fenceIncludedUpdates = fence_stats.includedInPublishUpdates;
    status_diagnostics.fenceIncludedSubtreeAttachUpdates =
        fence_stats.includedSubtreeAttachUpdates;
    status_diagnostics.fenceIncludedSubtreeDetachUpdates =
        fence_stats.includedSubtreeDetachUpdates;
    status_diagnostics.fenceReplayAfterUpdates = fence_stats.replayAfterPublishUpdates;
    status_diagnostics.fenceUnsequencedUpdates = fence_stats.unsequencedCoveredUpdates;
    status_diagnostics.fencePassedThroughUpdates = fence_stats.passedThroughUpdates;
    status_diagnostics.fenceDrainedRawUpdates = fence_stats.drainedRawQueueUpdates;
    status_diagnostics.fenceDrainedBatchUpdates = fence_stats.drainedBatchUpdates;
    status_diagnostics.fenceInFlightCoveredUpdates = fence_stats.inFlightCoveredUpdates;
    NotifyDiagnostics(callbacks, status_diagnostics);

    if (!publish_apply.writeFailedContainerIds.empty()) {
      eos_static_warning(
          "msg=\"tree size recompute write failures during best effort publish\" "
          "cid=%llu root=\"%s\" publish_attempted_targets=%llu "
          "publish_applied_targets=%zu write_failed_targets=%zu "
          "first_failed_cid=%llu retry_root_cid=%llu error=\"%s\"",
          static_cast<unsigned long long>(root_id), request.rootSpecification.c_str(),
          static_cast<unsigned long long>(publish_apply.attemptedTargets),
          publish_apply.publishedContainerIds.size(),
          publish_apply.writeFailedContainerIds.size(),
          static_cast<unsigned long long>(publish_apply.failedContainerId),
          static_cast<unsigned long long>(root_id), publish_apply.error.c_str());
    }

    if (retryable_result) {
      return MakeResult(EAGAIN,
                        std::string("error: tree-size recompute requires retry: ") +
                            status_diagnostics.retryReason);
    }

    return MakeResult(0);
  } catch (const std::exception& e) {
    eos_static_warning("msg=\"tree size recompute publish failed\" "
                       "cid=%llu root=\"%s\" error=\"%s\"",
                       static_cast<unsigned long long>(root_id),
                       request.rootSpecification.c_str(), e.what());
    return MakeResult(EIO, std::string("error: tree-size recompute publish failed: ") +
                               e.what());
  }
}

EOSNSNAMESPACE_END
