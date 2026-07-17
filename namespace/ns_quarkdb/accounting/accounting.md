# Container tree size accounting and its interaction with `eos ns recompute_tree_size`

This document describes how the container tree size accounting
(`QuarkContainerAccounting`) works, the race condition that existed between it,
the `eos ns recompute_tree_size` command and user activity, how that race was
fixed, and the limitations of the fix.

## How the accounting works

Each container carries three delta-maintained counters: `treeSize`,
`treeFiles` and `treeContainers`. They are updated by relative deltas, never
recomputed from scratch during normal operation, so any error introduced once
persists until an explicit recompute.

`QuarkContainerAccounting` is an `IFileMDChangeListener` fed by user activity:

- `IContainerMD::addFile` / `removeFile` / `addContainer` / `removeContainer`
  and `QuarkFileMD::setSize` emit `SizeChange` events carrying a
  `TreeInfos` delta (size / files / containers);
- rename and the FUSE server additionally call `AddTree` / `RemoveTree` when
  moving whole subtrees.

All of these only **enqueue** `(container id, delta)` pairs onto
`mIdTreeInfosToUpdateQueue` (a strict FIFO, `common/ConcurrentQueue.hh`).
The actual application is asynchronous, performed by two threads:

1. **Queueing thread** (`AsyncQueueForUpdate`): the single consumer of the
   queue. For each popped item it walks up the parent chain
   (`AccumulateUpdate`) and accumulates the delta into the current batch
   (`mBatch[mAccumulateIndx]`) for the container and each of its ancestors.
   Deltas targeting the same container are merged in the batch, which keeps
   the number of backend writes independent of the number of individual
   operations.
2. **Propagation thread** (`PropagateUpdates`): every `mUpdateIntervalSec`
   (default 5 s) it runs one propagation cycle (`PropagateUpdatesOnce`): swap
   the accumulate/commit batches, then apply each accumulated delta with
   `updateTreeSize` / `updateTreeFiles` / `updateTreeContainers` under the
   per-container MD write lock and persist it with `updateStore`.

Consequently the tree counters lag the namespace by up to one update interval
plus the queue backlog.

## The race condition (before the fix)

`NsCmd::TreeSizeSubcmd` (`eos ns recompute_tree_size`) walks the subtree
breadth-first and, bottom-up, recomputes each container from its file map and
its children, then stores the result with an **absolute** `setTreeSize()`.

There was no coordination whatsoever between the recompute and the accounting
pipeline — only short-lived per-container MD locks on each side. This made the
following systematic corruption possible:

1. a user operation changes a file (the change is immediately visible in the
   file map / file size) and enqueues its delta;
2. the recompute reads the container, **including** that change, and stores an
   absolute value;
3. up to ~5 s later the propagation thread applies the still-pending delta on
   top of the freshly recomputed absolute value → **double counting**.

Symmetrically, a delta applied between the recompute's read and its write was
silently overwritten (lost). Since the counters are delta-maintained, such
errors never healed — the tool meant to repair drift could itself introduce
permanent drift on any active subtree.

## The fix

`QuarkContainerAccounting::Flush()` synchronously applies every update queued
before the call, using a **FIFO barrier**:

1. `Flush()` pushes a reserved sentinel id (`sFlushBarrierId`,
   `id_t` max — id `0` being already reserved as the stop-thread sentinel)
   onto the update queue and waits.
2. Because the queue is a strict FIFO with a single consumer, when the
   queueing thread pops the barrier, every item enqueued before it — including
   any item that was "in flight" between `wait_pop` and the batch insert — has
   been fully accumulated into the batch. The thread then acknowledges the
   barrier by setting `mBarrierReached` and signalling `mBarrierCv`.
3. `Flush()` runs one propagation cycle (`PropagateUpdatesOnce`). The cycle is
   serialized with the periodic propagation thread by `mFlushMutex` (held
   across swap + apply + clear), which also guarantees the commit batch is
   empty outside a cycle, so a single cycle applies everything accumulated so
   far. Concurrent `Flush()` callers are serialized by `mDrainMutex`, so at
   most one barrier is ever in flight and a single acknowledgement flag
   suffices.

When the queueing thread exits (shutting down, id `0` sentinel) nobody can
acknowledge a barrier anymore. It therefore sets `mQueueThreadStopped` on its
way out, which releases any waiting `Flush()` and makes later ones return
immediately, so a caller can never block forever on a barrier that will not
come.

`Flush()` returns `false` in that case and propagates **nothing**. Whatever is
still in the queue can no longer be consumed, so those deltas are lost;
persisting the accumulated batch on top of them would write tree sizes that are
missing those deltas, and since the counters are delta-maintained, nothing would
ever repair them. Dropping the batch is also what the destructor already does —
it pushes the stop sentinel and joins the threads without a final propagation.
`TreeSizeSubcmd` therefore treats a `false` return as a hard stop: it aborts
with `EIO` rather than continuing to write absolute values on the way down.

If the accounting was constructed with `update_interval = 0` (no threads,
e.g. in tests), `Flush()` drains the queue inline in the calling thread.

On top of the flush, `TreeSizeSubcmd` uses **validate-and-retry** (optimistic
concurrency): instead of trying to order or filter the deltas racing with the
recompute, it detects the collision after the fact and repairs it.

1. `Flush()` the pre-existing backlog, then start recording the ids of the
   containers the accounting applies deltas to
   (`StartRecordingUpdatedContIds` / `PropagateUpdatesOnce`; a transient
   in-memory set, no sequence numbers, no persistence, no change to the
   accounting write path). Both sets are guarded by `mFlushMutex`, which a
   propagation cycle holds throughout, so the recording cannot be
   reconfigured mid-cycle and the taken set never cuts a cycle in half. User
   traffic never touches that mutex — it only reaches the queue.
2. Recompute the whole subtree bottom-up (as before, without any mid-pass
   flush — the periodic propagation keeps running independently).
3. Validate: `Flush()` and take the recorded set (`TakeUpdatedContIds`). An
   empty set is a clean pass: every recomputed value is exact. Otherwise it is
   the *dirty* set — containers whose value may have been corrupted by a delta
   applied after their absolute write — and only those are recomputed again,
   bottom-up, before validating again. At most 3 passes (the initial one plus
   2 retries); leftovers are reported to the caller (count, `retc` 0) and keep
   their last recomputed value, on top of which the accounting continues to
   apply deltas consistently.

The recording is **scoped to the containers under recompute**: the recompute
hands `StartRecordingUpdatedContIds` the ids it is about to rewrite (the
current pass's BFS list, narrowed to the dirty set before each retry) and the
propagation cycle only records deltas landing on those. This is not just an
optimisation of the intersection in step 3: a propagation cycle applies deltas
originating from the *whole namespace*, so recording everything would size the
set by the instance-wide activity for the duration of a pass — unbounded, and
unrelated to the command's own work — only to discard all of it but the part
falling inside the recomputed subtree. Scoping it bounds the set by the size of
the recomputed tree instead. It also makes the accounting write path marginally
cheaper: a hash lookup in the (empty, hence skipped) set of containers under
recompute, instead of a hash insert per applied delta.

A second concurrent `recompute_tree_size` is rejected with `EBUSY` rather than
queued: the recorded set is a single destructive-read buffer, and a caller
waiting behind a long recompute would hold a proc thread for its whole
duration.

Detection is deliberately **conservative**: any delta applied to a recomputed
container during the round marks it dirty, even if it landed harmlessly
before the container's absolute write (the write overwrote it). False
positives only cost a cheap re-recompute of the affected containers.
Detection is also **complete**: any corruption requires a delta applied to
the container between the recording mark and the validation flush, and a
corruptible container is by definition one this pass rewrites, hence one of
the containers under recompute — so it necessarily appears in the recorded
set. Scoping the recording to them therefore loses nothing.

Re-recomputing only the dirty set bottom-up is sound because the dirty set is
**upward-closed** within the recomputed tree: `AccumulateUpdate` applies a
delta to the origin container and all its ancestors alike, in the same
propagation cycle, so if a child was dirtied, its parent was too. A dirty
container's children are therefore either clean (their value is exact) or
dirty and re-recomputed first (deeper BFS level). Containers created or moved
during the pass are not in the BFS list, but they dirty their ancestors, whose
re-recompute reads the live children maps; deleted containers are simply
skipped.

Upward-closure is also what makes the **corrections propagate back up** the
tree. `NsCmd::UpdateTreeSize` derives a container's absolute value from its own
files plus the `treeSize` of its direct children, so a parent picks up a
corrected child simply by being recomputed after it — which the bottom-up walk
guarantees. Because the dirty set contains, for every dirty container, its
whole ancestor chain up to the recompute root, a retry pass does not just fix
the containers that were raced: it carries their corrected values all the way
back up to the root of the recomputed subtree. (Any delta inside the subtree
passes through that root on its way to `/`, so the root is dirty whenever
anything below it is.)

Narrowing the containers under recompute to the dirty set before a retry
(`UpdatedContIdsRecordingScope::Restart`) preserves all of this, because the
recorded set is scoped to exactly the ids the *next* pass rewrites:

- a delta landing on a container the retry pass rewrites also dirties that
  container's ancestors, which — by upward-closure — were dirtied together in
  the previous pass and are therefore under recompute too, hence recorded. The
  dirty set stays upward-closed from one pass to the next;
- a delta landing on a container that was clean in the previous pass and is not
  rewritten by the retry pass has nothing to corrupt: it applies on top of an
  exact value. Its ancestors that *are* being rewritten receive the same delta
  and are recorded, so the pass above it is still flagged.

Concurrent `recompute_tree_size` commands are serialized (they would consume
each other's recorded set, which is a single destructive-read buffer).

The design deliberately avoids any global lock (`eosViewRWMutex` is not taken
anywhere in this path): the recompute remains invisible to user traffic.

## Limitations

- **Sustained writers can exhaust the retry budget.** A container under
  continuous modification can keep invalidating itself every pass. After the
  3 passes the command stops, reports the number of still-dirty containers on
  `std_err` (with `retc` 0) and logs a warning; their values are approximate
  but the accounting stays consistent relative to them, and a later recompute
  converges as soon as they are quiet for one pass. On a quiet (or quieter)
  subtree the recompute converges to exact values, typically in a single
  validation.
- **Remaining undetected window.** A mutation whose effect is already visible
  to the recompute's read but whose delta is enqueued only *after* the final
  validation flush escapes detection (double count). This gap — between
  `QuarkFileMD::setSize` becoming visible and `QueueForUpdate` being called,
  ordered only by the *file* lock, not the container lock — is
  instruction-scale, compared to the seconds-wide windows of the previous
  schemes. No per-container locking scheme can close it completely.
- **`Flush()` and MD locks.** `Flush()` applies deltas under per-container MD
  write locks. It must therefore not be called while holding an MD lock on a
  container that may have pending updates, otherwise it can deadlock.
- **No propagation above the recompute root.** Corrections made by the
  recompute do not propagate to the ancestors of the recomputed subtree root
  (pre-existing behavior, unchanged by the fix). Likewise, the accounting
  never updates the root container `/` (id 1), by construction of the parent
  chain walk, so no delta can race the recompute of `/` itself.
- **Depth cap.** `AccumulateUpdate` walks at most 255 levels up; ancestors
  beyond that never receive deltas in normal operation either, so the
  recompute inherits (and does not worsen) that pre-existing behavior. The
  BFS itself is capped at 256 levels.
- **The analogous race in `eos ns recompute_quotanode` is not covered.** The
  quota recompute scans QDB-persisted file metadata, which lags behind the
  metadata `MetadataFlusher`, and then overwrites the live counters with
  `replaceCore()`. A `getMetadataFlusher()->synchronize()` call before the
  `QuotaRecomputer` scan would remove the systematic part of that race; ops
  landing during the scan itself would remain unprotected.
