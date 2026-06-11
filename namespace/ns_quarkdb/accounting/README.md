# Tree-Size Recompute And Async Accounting

This note documents the best-effort fix for races between
`eos ns recompute_tree_size` and asynchronous container tree accounting.

## Problem

Normal namespace modifications do not update all ancestor tree counters in the
caller thread. They enqueue a delta in `QuarkContainerAccounting`; a background
thread later walks the ancestor chain, batches the deltas and applies them to
`tree_size`, `tree_files` and `tree_containers`.

`eos ns recompute_tree_size` writes absolute tree counters after scanning a
subtree. If an old queued delta is applied after one of these absolute writes,
the freshly recomputed value can drift immediately, usually by being
double-counted.

The command is operational and best-effort: it is expected to repair old
accounting drift without blocking user traffic with long subtree-wide locks.
It does not try to provide a single atomic snapshot of the whole namespace
subtree.

## Approach

The fix registers one transient recompute context in
`QuarkContainerAccounting`. The context contains:

- a sorted vector of container ids found by the recompute BFS,
- a dirty flag recording whether async accounting touched that coverage,
- skipped deltas for the current recompute attempt, keyed by target container id.

Only one recompute context can be active. This keeps the API simple, but future
callers must preserve the single-owner lifecycle: the boolean recompute API has
less protection against stale callers accidentally touching a newer active
context than a tokenized context API would have.

During ancestor queue expansion, `QueueAncestorsForUpdate()` checks whether each
target container id belongs to the active recompute context using
`std::binary_search()` on the sorted id vector. If it does, queueing sets the
context dirty flag and stores the skipped additive delta in the recompute
context instead of adding it to the normal propagation batch. This keeps
recompute-covered write activity from building up in `mBatch` while a
propagation pass is busy with slower container lookups, locking and store
updates. `PropagateUpdatesOnce()` keeps the same check as a fallback for deltas
that were already batched before the recompute context became active.

The skipped delta is not replayed while the recompute can still retry or
finish cleanly. Instead, the dirty flag tells `eos ns recompute_tree_size` that
another recompute pass is needed. The next pass writes a fresh absolute value
based on what the namespace scan observes at that time. Skipped deltas are kept
only for the current attempt; if the command gives up with `EAGAIN`, those
last-attempt deltas are requeued so normal accounting still has a chance to
catch up.

## Recompute Loop

`NsCmd::TreeSizeSubcmd()` now does the following:

1. Resolve the requested container and run the existing BFS.
2. Register the BFS container ids in an accounting recompute context via an
   RAII guard. A second concurrent recompute is rejected with `EBUSY`.
3. Run up to two recompute attempts.
4. At the start of each attempt, clear the context dirty flag.
5. Recompute from leaves to root and write absolute tree counters via the
   existing `UpdateTreeSize()` path.
6. Flush pending tree-size accounting updates.
7. If the context is still clean, atomically clear it and return success.
8. If both attempts observed dirty containers, requeue the skipped deltas from
   the last attempt, leave the last best-effort values written and return
   `EAGAIN` with a warning.

The RAII guard aborts the recompute context if the command returns early. The
successful finish path checks and clears the context under the same mutex, so a
late dirty mark cannot be missed between the final dirty check and cleanup.

## Accounting Flush Barrier

`FlushTreeSizeUpdates()` gives the recompute command a way to drain accounting
work that was already queued before the dirty check.

In the normal MGM configuration, accounting is asynchronous
(`QuarkContainerAccounting` uses the default non-zero update interval). In that
mode, `FlushTreeSizeUpdates()` enqueues a barrier item after the already queued
updates and waits until the queueing thread reaches that barrier. Then it calls
`PropagateUpdatesOnce(is_admin_recompute=true)` so any remaining accumulated
updates targeting the recompute coverage are marked dirty before the recompute
command checks the dirty flag.

The recompute-triggered propagation pass is scoped: entries whose target
container is not in the recompute coverage are deferred back into the normal
accumulation batch. This avoids making the admin recompute command
synchronously perform metadata lookups, locks and backend writes for unrelated
accounting updates.

If a test or manually constructed accounting instance uses update interval `0`,
async worker threads are disabled. In that mode, `FlushTreeSizeUpdates()` drains
queued update items directly and then runs one scoped propagation pass.

Normal metadata operations remain non-blocking on this barrier. The barrier is
only used by the recompute command.

## Propagation Serialization

`mMutexPropagate` serializes propagation passes. It is separate from
`mMutexBatch` on purpose:

- `mMutexBatch` only protects the batch indexes and accumulated deltas.
- `mMutexPropagate` prevents two propagation passes from swapping and clearing
  the same commit batch concurrently.
- Queueing can continue to accumulate unrelated deltas while a propagation pass
  does slower container lookups, locking and store updates.
- Deltas covered by an active recompute context are diverted into that context
  during queue expansion, so sustained writes inside the recomputed subtree do
  not grow the normal propagation batch while the recompute is active.

## Limitations

This is still best-effort. It avoids applying stale additive deltas over freshly
written absolute recompute values on successful recompute, but it does not
create a globally consistent instant for very large subtrees. Under sustained
modifications, the command can observe dirty containers on both attempts and
return `EAGAIN`; operators should rerun the command when the subtree is quieter.

The intended behavior is that high write activity may make the recompute retry
or report best-effort failure, but a successful recompute should not silently
apply delayed queued deltas over the recomputed values.
