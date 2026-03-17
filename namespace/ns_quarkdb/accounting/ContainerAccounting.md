# ContainerAccounting: Tree Size Propagation and Recompute Fencing

## Problem

EOS containers track three aggregate values over their subtree:
`treeSize`, `treeFiles`, `treeContainers`. They are maintained
incrementally as files and directories are created, deleted, resized, or
moved.

Operators can also force an absolute recompute over a subtree
(`eos ns recompute_tree_size`). Doing this while concurrent file and
rename operations are flying through the namespace is the hard part: the
BFS recompute writes absolute values, the concurrent ops write
incremental deltas, and applying both naively produces double-counting,
underflow, or phantom files (EOS-6577).

## Solution at a glance

`QuarkContainerAccounting` exposes one production entry point:

```cpp
void RecomputeSubtreeWithFencing(
    const std::unordered_set<IContainerMD::id_t>& bfsIds,
    const std::function<void()>& runBfs);
```

`bfsIds` is the set of container ids the recompute will visit (collected
by a breadth-first walk over the subtree). `runBfs` is a callback the
caller supplies: it does the actual leaves-to-root recompute, calling
`UpdateTreeSize` on each container. The accounting layer wraps the
fence/drain/dirty-set protocol around that callback so concurrent
traffic on `bfsIds` cannot corrupt the absolute values it sets.

While a recompute is in flight, deltas for fenced containers are diverted
from the namespace into a **dirty journal** (a set of container ids).
Once `runBfs` returns, every container in that journal is recomputed
from current namespace state by an async background thread until the
dirty set converges.

## The protocol

```text
RecomputeSubtreeWithFencing(bfsIds, runBfs):
    if recompute already in flight: return                 # serialise
    fenceContainers(bfsIds)                                # divert deltas
    drainFencedDeltas                                      # capture in-flight
    discardJournal                                         # absolute reset
    runBfs()                                               # caller sets values
    drainFencedDeltas                                      # capture during-recompute
    dirty = collectAndFenceDirty                           # narrow fence
    if dirty.empty: unfence                                # done
    else:           scheduleRecompute(dirty)               # async tail
```

The five state-changing primitives (`fenceContainers`, `drainFencedDeltas`,
`discardJournal`, `collectAndFenceDirty`, `unfence`) are exposed in the
header but marked **internal — exposed for unit tests only**; production
callers must use `RecomputeSubtreeWithFencing`.

### Why `collectAndFenceDirty` is one call

Reading the dirty set and re-installing it as the new fence must happen
under a single write-lock. Doing them as two separate calls leaves a
window in which a `PropagateUpdates` cycle can drop a delta into the
just-emptied journal that the follow-up fence-narrow then wipes — a lost
update.

## Async dirty recompute

After `RecomputeSubtreeWithFencing` schedules a dirty set,
`AssistedRecomputeDirty` runs the convergence loop:

```text
while batch is non-empty and stalls < kMaxStallIterations:
    recomputeBatchLeafToRoot(batch)
    drainFencedDeltas
    batch = collectAndFenceDirty
    if batch shrunk: stalls = 0 else: ++stalls

if batch non-empty: recomputeBatchLeafToRoot(batch)        # final pass
unfence                                                    # releases in-flight
```

Convergence is detected by **shrinkage**, not by an absolute iteration
count: the loop keeps running as long as it's making progress; the stall
cap (`kMaxStallIterations = 5`) bounds work under sustained writes. The
final pass gives one extra reconciliation chance after the loop bails
out.

Each `recomputeContainer(id)` reads `id`'s current files and child
containers, computes correct absolute values, and sets them. If the
parent is not in the dirty set, a delta correction is queued for it via
the normal accounting pipeline.

`unfence()` is the single release point: it clears the fence, the
journal, and the `mRecomputeInFlight` claim. Every exit path (sync
no-dirty, async tail, exception cleanup) goes through it.

## Threading model

| Thread                  | Function                 | Role                                                                       |
|-------------------------|--------------------------|----------------------------------------------------------------------------|
| `mThread`               | `PropagateUpdates`       | Swaps batch indices; applies unfenced deltas; diverts fenced ids to journal |
| `mQueueForUpdateThread` | `AsyncQueueForUpdate`    | Dequeues from the concurrent queue; walks up the tree; populates the batch  |
| `mDirtyRecomputeThread` | `AssistedRecomputeDirty` | Waits on `scheduleRecompute`; runs the convergence loop above              |

All three threads are owned by `QuarkContainerAccounting` and started by
the constructor.

## Concurrency invariants

- **Only one recompute at a time.** `mRecomputeInFlight` is set via CAS
  by `RecomputeSubtreeWithFencing` and cleared by `unfence()`. A second
  invocation refuses early.
- **Fence state is guarded** by `mFenceMutex`. The hot path
  (`PropagateUpdates`) takes a brief write-lock per cycle; the rare
  state changes (`fenceContainers`, `collectAndFenceDirty`, `unfence`)
  take it once each.
- **`collectAndFenceDirty` is atomic** — read dirty set, install as new
  fence, clear journal, all under one write-lock.

## Known limitations

- Under sustained writes that touch the same dirty container without
  pause, the shrink-based stall cap (`kMaxStallIterations = 5`) bails
  out of the async loop with some residual drift. A subsequent
  `recompute_tree_size` issued from a quieter state will reconcile.
- The `mIdTreeInfosToUpdateQueue.empty()` poll in `drainFencedDeltas` is
  a 10 ms polling loop; intentional (the cleaner CV-on-empty alternative
  isn't worth the synchronisation complexity).

## Files

| File                                                  | Role                                                       |
|-------------------------------------------------------|------------------------------------------------------------|
| `ContainerAccounting.{hh,cc}`                         | The class and its threads                                  |
| `mgm/proc/admin/NsCmd.cc::TreeSizeSubcmd`             | Sole production caller of `RecomputeSubtreeWithFencing`    |
| `namespace/ns_quarkdb/NamespaceGroup.cc`              | Constructs and wires up the listener                       |
| `namespace/ns_quarkdb/tests/HierarchicalViewTest.cc`  | Fencing + race-condition tests                             |
