# QuarkDB Container Accounting

This directory contains the in-memory accounting layer used to maintain
subtree counters for QuarkDB namespace containers:

- `treesize`
- `treefiles`
- `treecontainers`

The counters are stored on container metadata, but their propagation is
asynchronous. Namespace changes emit accounting events, the accounting layer
walks the affected ancestor path, and a propagation thread applies aggregated
updates to container metadata.

## Problem

`eos ns recompute_tree_size` recomputes absolute subtree counters by scanning a
tree bottom-up. Before this change, that recompute could race with concurrent
file creation, directory creation, and directory moves:

- the recompute command could scan an old or mixed tree view;
- normal accounting deltas could still be queued but not yet applied;
- an absolute recompute write could overwrite pending delta-based accounting;
- a moved subtree could cause a queued delta to be propagated through the
  subtree's new parent chain instead of the parent chain that was valid when the
  change happened.

The global namespace mutex would prevent this, but it is not acceptable for EOS
instances with very large namespaces. The solution therefore keeps the existing
fine-grained namespace locking model and makes the accounting stream ordered and
self-invalidating.

## Event Model

`QuarkContainerAccounting` now queues typed accounting events:

- `Delta`: relative changes from normal namespace activity.
- `Reset`: absolute values produced by tree-size recomputation.
- `Stop`: shutdown signal for the accounting queue thread.

Each event carries two sequence numbers:

- `mSequence`: total accounting event order. This is used to resolve topology
  when walking ancestors.
- `mDeltaSequence`: order of real namespace accounting deltas. This is
  incremented for deltas and moves, but not for recompute resets.

The distinction matters because recompute resets must not make other recompute
resets stale. Only actual namespace changes should invalidate a recompute
snapshot.

## Delta Propagation

Normal file and container changes reserve an accounting sequence while the
corresponding `IFileMD` or `IContainerMD` write lock is still held. They publish
the already-sequenced delta to the accounting queue only after releasing the
namespace object lock. This closes the recompute race without taking the queue
mutex under a namespace object lock.

The queueing thread processes events by reserved sequence number, not by queue
arrival order. If a later event is published first, it is buffered until the
missing earlier sequence arrives. The queueing thread then expands each delta to
the ancestors that should receive the change and aggregates them in the current
update batch. The propagation thread applies the batch with container write
locks.

`mAppliedDeltaSequence` records the highest delta sequence whose deltas
have been applied. Conditional recompute resets use it to avoid overwriting
deltas that were visible to a recompute snapshot but had not yet reached the
metadata counters.

## Directory Moves

Directory moves use the container-id based `MoveTree()` instead of separate
`RemoveTree()` and `AddTree()` calls. This records the moved container's parent
change and queues the negative delta on the old parent and the positive delta
on the new parent under the same delta sequence. The pointer-based listener
entry points are kept as compatibility wrappers, but new accounting call sites
should pass container ids directly when they already have them.

The accounting layer keeps an in-memory parent history:

- `RecordMove()` stores `(sequence, oldParent, newParent)` for the moved
  container.
- `ParentAt(id, sequence)` returns the parent that was valid for a delta at
  that accounting sequence.

This prevents a delayed delta from being propagated through a parent chain that
only became true after the change was queued.

The parent history is intentionally in-memory only. It is not persisted to QDB
and does not survive MGM restart or failover.

## Recompute Resets

`recompute_tree_size` now treats an absolute recompute result as conditional.
For each container, it captures:

- the current accounting delta sequence at the start of the scan.

A recompute result is queued only if the accounting delta sequence has not
changed. At apply time the reset is still checked again:

- no later delta may have been queued;
- all deltas up to the reset snapshot must already be applied;

If any of these checks fails, the reset is skipped. The namespace remains
eventually correct through the normal delta stream and can be recomputed later
when the subtree is quiet.

## Bottom-Up Recompute

`TreeSizeSubcmd()` still walks the BFS result from leaves to root, but it now
keeps a command-local map of successfully recomputed child values. When a parent
is recomputed, it uses that map for children already processed in the same
command instead of immediately reading their persisted tree counters.

This avoids a second race: child reset events are queued asynchronously, so a
parent recompute must not depend on those child reset writes having reached QDB
before the parent scan continues.

If a child recompute is skipped because it raced with a namespace change, the child is
not entered in the command-local map and parents fall back to the current
metadata counters for that child.

## Stable File Container Id

`IFileMDChangeListener::Event` carries `containerId`. For tree accounting,
`QuarkFileMD::setSize` now reserves the accounting delta with the file container
id while updating the file metadata, then publishes the reserved delta after the
file write lock has been released.

This avoids looking up the file's container later, after the file may have been
moved.

## Guarantees

The design provides these guarantees while the MGM process stays alive:

- Accounting deltas are applied through the ancestor chain that was valid for
  the delta sequence.
- A recompute reset cannot overwrite pending or later accounting deltas.
- Parent recompute values do not depend on asynchronous child reset persistence
  within the same recompute command.
- Under active namespace activity, recompute resets may be skipped instead of forcing a
  global lock.
- After namespace changes stop and pending accounting deltas drain, subtree counters
  converge to the correct values.

## Limitations

The solution deliberately does not change persistency:

- accounting sequence state is in memory;
- parent move history is in memory;
- pending reset/delta ordering is not reconstructed after MGM restart/failover.

These limitations are acceptable for this implementation because the goal is to
avoid races during normal MGM operation without introducing a global namespace
lock or changing the persisted namespace schema.

## Relevant Entry Points

- `ContainerAccountingTypes.hh`
- `IFileMDChangeListener::ReserveAccountingDelta()`
- `IFileMDChangeListener::PublishAccountingDelta()`
- `QuarkContainerAccounting::QueueForUpdate()`
- `QuarkContainerAccounting::MoveTree()`
- `QuarkContainerAccounting::SetTreeIfAccountingUnchanged()`
- `QuarkContainerAccounting::AsyncQueueForUpdate()`
- `QuarkContainerAccounting::PropagateUpdates()`
- `NsCmd::TreeSizeSubcmd()`
- `NsCmd::UpdateTreeSize()`
