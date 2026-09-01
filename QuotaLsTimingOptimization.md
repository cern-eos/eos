# `eos quota ls` performance plan

## Context

On an instance with ~5000 quota nodes (one of them holding ~2000 user quotas),
`eos quota ls` takes ~6 seconds. Profiling the MGM side attributes ~5.35 s of
that 5.95 s wall clock to `Quota::PrintOut`; the remaining ~0.6 s is the proc
framework, the transport of ~6.8 MB of output, and client-side parsing.

The root cause is that `Quota::PrintOut` calls `Quota::LoadNodes()`
unconditionally on every invocation, and `LoadNodes()` ends by calling
`SpaceQuota::Refresh(5)` on **every** quota node. Each `Refresh` performs three
to four independent, fully serialised, by-name namespace path resolutions:

| Where | Share of MGM time | Cost per node |
|---|---|---|
| `SpaceQuota::Refresh` total | 4.05 s (76 %) | ~820 µs |
| ` └ UpdateLogicalSizeFactor` | 2.77 s (52 %) | ~560 µs |
| `    ├ _attr_ls(pPath)` | 1.92 s | ~390 µs |
| `    └ Policy::GetLayoutAndSpace` | 0.76 s | ~155 µs |
| ` └ AccountNsToSpace` | 1.24 s (23 %) | ~250 µs |
| `    └ UpdateQuotaNodeAddress` | 1.19 s | ~240 µs |
| ` └ UpdateIsSums` + ns getters | 0.07 s | ~15 µs |
| Printing (table formatting, output) | 1.05 s (20 %) | ~210 µs |

Two facts shape the plan:

- The per-node work is *not* the accounting itself (the in-memory quota node
  getters cost 36 ms in total). It is the repeated resolution of the quota
  node's path against the namespace, which goes to QuarkDB.
- Almost none of it needs to happen on the command path. The layout size factor
  changes only when an admin edits `sys.forced.*` or a space policy; the quota
  node's container id never changes; and usage counters are maintained
  incrementally by the namespace.

The existing 5-second refresh TTL in `SpaceQuota::Refresh(time_t age)` cannot
help at this scale, because a single `quota ls` takes longer than the TTL, so
consecutive invocations always re-do the full refresh.

**Goal:** bring the MGM side to ~1 s with no change to the command output.

Items are ordered by payoff and are independent — each can be developed,
reviewed and merged on its own.

---

## RESUME HERE

**Done and measured on the test instance: Items 1 and 2. `quota ls` 5.95 s ->
1.88 s.** They are staged.

**Item 3 is written but unstaged and never compiled** — see its "Steps — DONE"
section. All three items live in `mgm/quota/Quota.cc` and `mgm/quota/Quota.hh`,
ready to be split into three commits. Items 1 and 2 were only ever compiled on
the test VM.

**Next: build Item 3 on the test VM and re-measure**, in particular
`quota ls -p <path>`, which should drop to near zero, and `eos quota`, whose
second `PrintOut` should now hit the 5 s usage TTL instead of re-importing
everything.

**After that: Item 4 (table formatting)** is where the remaining user-visible
time actually is — ~975 ms of the ~1.3 s that is left.

---

## Item 1 — Stop recomputing the layout size factor on every refresh

**Expected saving: ~2.8 s (52 % of MGM time).**

`SpaceQuota::UpdateLogicalSizeFactor()` does a `gOFS->_attr_ls(pPath, ...)`
followed by `Policy::GetLayoutAndSpace(...)`, and `SpaceQuota::Refresh()` calls
it every time. `_attr_ls` alone resolves the path twice — once in
`eos::Prefetcher::prefetchItemAndWait()` and again in `eosView->getItem()` —
and `GetLayoutAndSpace` rebuilds its config-key vectors and copies config maps
out of the space view on every call, taking `FsView::gFsView.ViewMutex` while
doing so.

The result is a value that only changes when someone edits the quota
directory's `sys.forced.*` attributes or the space policy.

### Files

- `mgm/quota/Quota.hh` — `SpaceQuota` private members and the
  `UpdateLogicalSizeFactor` declaration.
- `mgm/quota/Quota.cc` — `SpaceQuota::UpdateLogicalSizeFactor()`,
  `SpaceQuota::Refresh()`, `Quota::SetQuotaTypeForId()`.

### Steps — DONE

Implemented as follows (the TTL is the *default* argument rather than an
opt-in, so no existing call site had to change):

1. `SpaceQuota::UpdateLogicalSizeFactor(time_t age = sLayoutFactorAge)` with
   `static constexpr time_t sLayoutFactorAge = 300;` in `SpaceQuota`'s private
   section, plus a `time_t mLastLayoutFactorRefresh` member next to
   `mLastRefresh`. The guard mirrors the one in `SpaceQuota::Refresh`: a
   non-zero `age` returns early when the cached value is younger than `age`
   seconds, `age == 0` forces recomputation.
2. `SpaceQuota::Refresh()` keeps its `UpdateLogicalSizeFactor()` call unchanged
   and simply picks up the 300 s default, so the ~5000 per-command calls become
   cheap no-ops. The constructor's call also stays as-is and still recomputes,
   because `mLastLayoutFactorRefresh` starts at 0.
3. `Quota::SetQuotaTypeForId` forces a recomputation with
   `UpdateLogicalSizeFactor(0)` before the raw↔logical byte conversion, so an
   admin setting a quota always uses a fresh factor. It is done in its own
   scope, because it needs `eosViewRWMutex` — which has to be taken *before*
   `pMapMutex` to match the order used in `LoadNodes`, and must not be held
   while the configuration is saved and broadcasted further down the function.

Note for Item 3: because the 300 s TTL is the default, the background refresher
gets the periodic recomputation for free by calling `Refresh()` — there is no
extra wiring to do, and no ~2.8 s spike lands on a user command beyond the one
that happens to cross the interval boundary.

### Risk

`mLayoutSizeFactor` is not display-only: `SpaceQuota::SetQuota` uses it to
derive `kUserLogicalBytesTarget` / `kGroupLogicalBytesTarget` from a byte
target, and `Quota::GetLayoutSizeFactor` is used to convert raw↔logical bytes
when reporting. The staleness window grows from 5 s to the chosen interval.
Step 3 covers the case that actually matters (an admin changing quota).

Note also that `Refresh(0)` — used by `ns quota recompute` via
`RefreshFromNsQuota` — now forces only the usage re-import, not the layout
factor recomputation. That is intentional (the factor does not depend on the ns
quota counters), but it is a behaviour change and belongs in the commit message.

### Optional follow-up

`UpdateLogicalSizeFactor` resolves `pPath` by name only to read the container's
attributes. Once Item 2 stores the container id, this can use
`eos::listAttributes(gOFS->eosView, container.get(), map)` from
`namespace/utils/Attributes.hh` on the container fetched by id, removing the
by-name resolution (and the duplicate prefetch) entirely. Worth doing as a
separate commit — it makes the remaining periodic recomputation cheap too.

---

## Item 2 — Bind the ns quota node by container id, not by path name

**Expected saving: ~1.2 s (22 % of MGM time).**

`SpaceQuota::UpdateQuotaNodeAddress()` calls
`gOFS->eosView->getContainer(pPath.c_str())`, resolving the path component by
component, on every refresh. The container id is already known: the
`SpaceQuota` constructor receives it as `cont_id` and uses
`gOFS->eosDirectoryService->getContainerMD(cont_id)` — but it is not stored, so
every subsequent refresh falls back to a by-name lookup.

The constructor's own comment already makes the argument for binding by id
("never re-resolve the path by name (which could rebind to a wrong or shadow
container)"), so this change also makes `UpdateQuotaNodeAddress` strictly more
correct: the container id is stable across renames, whereas `pPath` is re-keyed
after the fact by `LoadNodes`.

### Files

- `mgm/quota/Quota.hh` — add the member and document it.
- `mgm/quota/Quota.cc` — `SpaceQuota::SpaceQuota()`,
  `SpaceQuota::UpdateQuotaNodeAddress()`.

### Steps — DONE

1. Added `eos::IContainerMD::id_t mContId;` to `SpaceQuota`'s private members,
   initialised from the constructor's `cont_id` parameter (declared between
   `pPath` and `mQuotaNode`, with the initialiser list kept in the same order).
   The constructor body now uses `mContId` too, so the id has a single source.
2. `UpdateQuotaNodeAddress()` fetches the container with
   `gOFS->eosDirectoryService->getContainerMD(mContId)` instead of
   `gOFS->eosView->getContainer(pPath.c_str())`. Everything else is unchanged:
   `getQuotaNode(quotadir.get(), false)`, the null check, and the
   `catch (eos::MDException&)` that sets `mQuotaNode = nullptr` and returns
   false. This mirrors what the constructor does a few lines above.
3. Invariants verified: `registerQuotaNode` calls
   `pQuotaStats->registerNewNode(container->getId())`, so a quota node id *is*
   the container id, and `mContId` therefore equals the key
   `Quota::CreateQuotaObj` uses for `pMapInodeQuota`. There is exactly one
   `new SpaceQuota` call site, and all three `CreateQuotaObj` callers supply a
   real container id (`IConfigEngine` even guards with `if (cont_id && ...)`).
   `pPath` is still mutated by the rename re-key in `LoadNodes`; `mContId`
   never is, which is the point.

### Why this is also a correctness improvement

Commit `9d09d7de9` ("MGM: Prevent quota subsystem from creating shadow
containers", EOS-6601) reworked `SpaceQuota` so that it "binds to an existing
container by id (never resolving by name or creating)", because a transient QDB
error during a by-name lookup had been misread as "directory missing". That
commit converted the constructor and `LoadNodes` but left
`UpdateQuotaNodeAddress` resolving `pPath` by name, so every refresh reopened
the same hazard. This change finishes that work.

Consequence, and it is the desired one: a `SpaceQuota` whose container has been
removed no longer silently re-binds to a different container that later appears
at the same path. It reports no usage until the node is properly re-created,
instead of attaching to a shadow container.

### Risk

Low. If the container has been removed, `getContainerMD` throws
`eos::MDException` and the existing catch produces exactly the current
behaviour (`mQuotaNode = nullptr`, return false).

---

## Item 3 — Refresh only what is printed (DONE, not compiled)

> **Redesigned: no background thread.** The original plan below proposed an
> `AssistedThread` refresher. That was reconsidered and rejected — see
> "Why no thread" and "Revised implementation" at the end of this item. Read
> those two sections first; the intermediate prose is kept because it documents
> what `LoadNodes` does and why the discovery half is redundant here.

**Expected saving: ~0.35 s on a full `quota ls`, near-total on
`quota ls -p <path>`, and ~2 s on an instance whose quota nodes all carry
accounting data (see below).**

`Quota::PrintOut` calls `LoadNodes()` before it looks at the requested path, so
even `eos quota ls -p /eos/some/path/` pays the full all-nodes cost, and
`eos quota` (no arguments) pays it twice because `LsuserSubcmd` calls
`Quota::PrintOut` twice.

`LoadNodes()` does three distinct jobs, only the first two of which are
"discovery":

1. Enumerate ns quota node ids (`getQuotaStats()->getAllIds()`) and resolve
   each one's URI with `eosView->getUri()` to build `map_id_path`.
2. Re-key `pMapQuota` for quota nodes whose directory was renamed on another
   MGM, and create `SpaceQuota` objects for ids not yet known.
3. Refresh every `SpaceQuota` in `pMapQuota`.

Job 1 is currently cheap *only* because `QuarkQuotaStats::getAllIds()` is a QDB
`SCAN` over `quota:*:*` and therefore returns only nodes that already have
accounting data written to QuarkDB. On an instance where all ~5000 quota nodes
hold data, `getAllIds()` returns all of them and this loop adds roughly
5000 × (`getContainerMD` + `getUri`) ≈ 2 s on top of everything else. Treat the
measured 5.95 s as a lower bound.

Job 3 is the 4 s bulk addressed by Items 1 and 2; what remains after those two
is worth moving off the request path anyway, so a `quota ls` never blocks on
namespace work.

### What makes this safe

`quota set` does not rely on `LoadNodes` for visibility: `SetQuotaTypeForId`
calls `CreateQuotaDir` and then `CreateQuotaObj` synchronously, so a newly
configured quota node is in `pMapQuota` before the command returns. What
`LoadNodes`-on-every-`PrintOut` adds is only (a) discovery of nodes created on
*another* MGM, and (b) re-keying after a rename done on another MGM. Both
tolerate a bounded delay.

### Why no thread

A background refresher would remove the ~355 ms usage import from the command
path, taking `quota ls` from 1.88 s to roughly 1.5 s. That is the whole benefit.
The cost is that those 355 ms then run on every interval forever, whether or not
anybody uses `quota ls` — at a 5 s period that is ~7 % of a core permanently,
plus ~1000 acquisitions per second of `eosViewRWMutex` and `pMapMutex` (one per
node per pass). Both are *named* mutexes, so each lock and unlock also takes the
process-wide `sOpMutex` in `RWMutex::RecordMutexOp`, contending with every other
namespace thread in the MGM.

An on-demand refresh guarded by a TTL strictly dominates a fixed-interval
thread:

| `quota ls` frequency | 5 s thread | 5 s TTL on demand |
|---|---|---|
| never | 4.3 s of work/min | **0** |
| once a minute | 4.3 s of work/min | **0.36 s/min** |
| every 5 s | 4.3 s of work/min | 4.3 s/min (identical) |
| every 1 s | 4.3 s of work/min | 4.3 s/min (TTL caps it) |

The TTL is never worse and usually far better, because the work happens only
when someone asks. The thread's only edge is that the first request in a quiet
period does not pay the latency — and 1.88 s vs 1.5 s is not worth a permanent
background load. If `quota ls` needs to be meaningfully faster, Item 4 is the
answer, not precomputation.

### Why discovery is redundant on the command path here

While this MGM owns the namespace, every mutation of the quota-node set already
updates `pMapQuota` synchronously:

- `IView::registerQuotaNode` is called from exactly **one** place in the tree —
  the `SpaceQuota` constructor. A quota node cannot appear in the namespace
  without a `SpaceQuota` being created for it.
- create -> `Quota::CreateQuotaObj` (from `quota set` via `SetQuotaTypeForId`,
  or from config application in `IConfigEngine`)
- rename/move -> `Quota::CommitRenameNodes`, called from `Rename.inc`; it
  re-keys the whole subtree and notes "pMapInodeQuota is keyed by container id
  and needs no update"
- remove -> `Quota::RmSpaceQuota`

The re-key block in `LoadNodes` says so itself: "this syncs a slave with a
rename done by the master". With a single MGM acting on the namespace there is
nothing to sync, so rename detection — the part that resolves the URI of every
node — does not belong on the command path.

What discovery still earns on the command path is the case `Quota::PrintOut`
documents today: "have all quota nodes visible even if they are not in the
configuration file". Keeping a cheap version of it preserves that.

### Steps — DONE

Implemented exactly as planned, in five pieces:

1. `Quota::LoadNodes()` split into three private statics:
   - `Quota::DiscoverNodes(bool detect_renames)` — jobs 1 and 2. When
     `detect_renames` is false it resolves `eosView->getUri()` **only** for ids
     not already in `pMapInodeQuota` (normally none) and skips the re-key block
     entirely, which also spares an exclusive `pMapMutex` lock per command. The
     known-id set is built in its own scope first, because `Quota::Exists()`
     takes `pMapMutex` itself and must not be called while it is held.
   - `Quota::RefreshAllNodes()` — the usage import for every known node.
   - `Quota::RefreshResponsibleNode(const std::string& path)` — the usage
     import for the single node responsible for one path.
   - `LoadNodes()` is now `DiscoverNodes(true) + RefreshAllNodes()`, so its two
     callers (`IConfigEngine::ResetConfig`, `QdbMaster::SlaveToMaster`) keep
     today's behaviour including rename detection.
2. `Quota::PrintOut` calls `DiscoverNodes(false)` and then refreshes only what
   it is about to print — `RefreshAllNodes()` for a full listing,
   `RefreshResponsibleNode(path)` for a path-scoped one.
   `Quota::GetAllGroupsLogicalQuotaValues()` got the same substitution
   (`DiscoverNodes(false) + RefreshAllNodes()`; it always prints everything).
3. `static constexpr time_t sUsageRefreshAge = 5;` in `Quota`'s private section,
   passed to `SpaceQuota::Refresh` by both refresh helpers — exactly today's
   `Refresh(5)` semantics, relocated. That TTL never actually fired before,
   because one `quota ls` took longer than 5 s; at 1.3 s it starts firing, which
   is what makes `eos quota` (two `PrintOut` calls) and any monitoring poll
   cheap.
4. `RefreshAllNodes()` does not carry over the defect of the old loop, where
   `first = false` sat inside `if (!first) { ... }` so `Grab()` was never called
   and every iteration after the first ran `Refresh()` — dereferencing
   `mQuotaNode` and walking `pMapQuota` — holding neither `eosViewRWMutex` nor
   `pMapMutex`; it also used `std::advance(it, n)` on a `std::map`, i.e. O(n²).

   It now snapshots the container ids from `pMapInodeQuota` under a `pMapMutex`
   read lock, releases it, and then for each id takes `eosViewRWMutex` and
   `pMapMutex` read-locked (that order), looks the id up again, refreshes and
   releases. Snapshotting is what makes releasing mid-iteration safe. The
   snapshot is keyed by container id, not path, because a rename can re-key the
   path while the id stays stable — and `pMapInodeQuota` is a complete index of
   `pMapQuota` (`CreateQuotaObj` fills both, `RmSpaceQuota`, `CleanUp` and
   `CommitRenameNodes` keep them in sync, and the constructor cannot leave
   `mQuotaNode` null). `SpaceQuota::Refresh`'s documented lock contract is now
   actually honoured.
5. `UpdateTargetSums()` added at the top of `SpaceQuota::PrintOut`, so the
   aggregate "All users"/"All groups" **target** row cannot disagree with the
   per-id rows printed above it during the 5 s window now that the TTL fires.
   Free when nothing changed — `mDirtyTarget` guards it.

### Question raised and closed: should rename detection be kept for a slave?

Considered gating the flag on `gOFS->mMaster->IsMaster()` instead of passing a
flat `false`, so that a non-master MGM would keep re-keying renamed nodes. It
was rejected as unfounded:

- `IsMaster` has never appeared in `mgm/quota/` — `git log -S IsMaster --
  mgm/quota/` is empty. The quota subsystem has no notion of master and slave.
- The only mention of slave/master in the whole history of the quota code is the
  one comment in the re-key block, introduced three weeks ago by `a6ba5f6c9`
  ("MGM: Support for quota node rename / move"). No code backs it up.
- `Quota::LoadNodes()` has exactly two callers, both whole-view rebuilds
  (`IConfigEngine::ResetConfig`, `QdbMaster::SlaveToMaster`). Nothing maintains
  a quota view incrementally on a non-master, so there is no follower behaviour
  to preserve.

Do not reopen this without first finding a real producer of a rename that this
MGM does not itself apply.

### Behaviour change to call out in the commit message

A quota node that exists in the namespace but has no configuration entry is
still picked up, because `DiscoverNodes(false)` runs on the command path. What
leaves the command path is rename detection, which `CommitRenameNodes` already
does synchronously, and which `LoadNodes()` still performs at config
application and master transition.

The single-node on-demand paths (`GetIndividualQuota`, `GetGroupStatistics`,
`GetStatfs`, all `Refresh(60)`) are untouched.

---

## Item 4 — Table formatting (mechanism found; only the quota-side part done)

**This is where the remaining user-visible time is: ~975 ms of the ~1.3 s left
after Items 1 and 2.** Measured on the baseline profile, per full `quota ls`
over ~4956 nodes:

| | ms | calls |
|---|---|---|
| `TableFormatterBase::GenerateTable` | 483 | 9915 |
| row construction (`TableCell` + `AddRows`) | 318 | 4959 |
| collect ids (walk `mMapIdQuota`) | 68 | 4956 |
| translate uid/gid to names | 51 | 4956 |
| sort/unique | 33 | 4956 |
| append to `XrdOucString` | 22 | 14871 |

### The mechanism, traced by reading (no longer a guess)

`TableFormatterBase::GenerateBody` computes the colour of **every cell** with:

```cpp
row[i].SetColor(ChangeColor(std::get<0>(mHeader[i]), row[i].Str()));
```

Three facts make this the dominant cost:

1. `TableCell::Str()` builds a **`std::stringstream` per cell** just to render
   the value into a string (`TableCell.cc:654`). It has exactly one caller in
   the whole tree — this line.
2. `ChangeColor` (`TableFormatterBase.cc:423`) only ever returns a colour for
   four column names: `status`, `active`, `vol-status`, `ino-status`. Everything
   else gets `DEFAULT`.
3. `TableCell::SetColor` **ignores `DEFAULT`** (`TableCell.cc:408`:
   `if (color != DEFAULT) mColor = color;`).

So for 8 of the 10 columns of a quota table the entire statement is a
guaranteed no-op, and each one still pays for a `stringstream` construction plus
two `std::string` copies (`ChangeColor` takes both parameters **by value**). A
quota listing renders 3 tables per node, i.e. ~30 cells per node, of which ~24
are no-ops: roughly 120 000 pointless `stringstream` constructions per
`quota ls` on the test instance, plus 16 000 more for the single 2000-user node.
That is the right order of magnitude for the measured 483 ms + 318 ms.

Also confirmed while reading: `GenerateBody` constructs one more
`std::stringstream` per row (`TableFormatterBase.cc:232`), `AddRows` deep-copies
every `TableCell` via `std::copy` (`TableFormatterBase.cc:408`), the per-row
`TableRow` vector grows without `reserve` (~15 element moves per 10-cell row),
and `Style()` assigns 28 `std::string` members on **every** `GenerateTable`
call, including calls on empty tables.

### Done in the quota code

`SpaceQuota::PrintOut` called `GenerateTable` on the user and the group table
unconditionally, even when nothing had been added to them. An empty table
renders to the empty string, so the call was pure overhead — one wasted
`Style()` per listing for every node that has only user or only group quota.
Both calls are now guarded by `!uids.empty()` / `!gids.empty()`, which is
output-identical: with no header and no data, `GenerateTable` runs `Style()`,
then `GenerateBody` over an empty `mData`, and returns `""`.

### Not done — belongs in a separate MR against `common/table_formatter`

The three changes that actually recover the ~800 ms are all in the shared
formatter, which every EOS table command uses (`fs ls`, `node ls`, `fileinfo`,
…) and which is out of scope for the quota work:

1. Skip the colour step for columns that cannot be coloured. Provably
   output-identical, by facts 2 and 3 above: for any other column name
   `SetColor` discards the result, and `Str()` has no side effects. This is the
   big one.
2. Fast-path `TableCell::Str()` for `STRING` cells (`return mStrValue;`), which
   removes the `stringstream` from the two columns that *are* coloured — they
   are always strings. Leave the `UINT`/`INT` cases on the stream so that
   locale-dependent formatting cannot change.
3. Add an `AddRows(TableData&&)` overload that moves instead of deep-copying,
   and `reserve()` the row vector at the call sites.

What remains after those is inherent: the command genuinely emits ~6.8 MB of
text, ~1.4 KB per node, and each node's table has its own column widths, so the
formatters cannot be merged without changing the output.

---

## Verification

No output should change. Capture a baseline before touching anything:

```bash
eos quota ls > /tmp/quota_ls.before
eos quota ls -m > /tmp/quota_ls_m.before
eos quota > /tmp/quota_user.before
```

After each item:

```bash
# 1. output must be byte-identical (usage counters may legitimately differ if
#    files were written in between - do this on an idle instance)
eos quota ls  | diff -u /tmp/quota_ls.before   - && echo "ls OK"
eos quota ls -m | diff -u /tmp/quota_ls_m.before - && echo "ls -m OK"
eos quota     | diff -u /tmp/quota_user.before - && echo "quota OK"

# 2. wall clock, three runs
for i in 1 2 3; do /usr/bin/time -f %e eos quota ls > /dev/null; done

# 3. namespace work per command: AttrLs should stop growing by ~1 per quota
#    node once Item 1 lands
eos ns stat | grep -E 'AttrLs|QuotaLock'
eos quota ls > /dev/null
eos ns stat | grep -E 'AttrLs|QuotaLock'

# 4. path-scoped listing must no longer pay the all-nodes cost (Item 3)
/usr/bin/time -f %e eos quota ls -p /eos/<a-quota-node>/ > /dev/null
```

Functional checks that must still pass:

```bash
# quota set/rm are visible immediately and the raw<->logical conversion is right
eos quota set -u <uid> -v 10G -p /eos/<node>/
eos quota ls -p /eos/<node>/
eos quota rm  -u <uid> -v -p /eos/<node>/

# a brand new quota node shows up
eos mkdir -p /eos/<new>/ && eos quota set -g <gid> -v 1G -p /eos/<new>/
eos quota ls -p /eos/<new>/

# usage accounting still tracks writes
eos cp /etc/hostname /eos/<node>/probe && eos quota ls -p /eos/<node>/

# ns quota recompute still works (exercises Refresh(0) via RefreshFromNsQuota)
eos ns quota recompute /eos/<node>/
```

Existing test coverage to run: `unit_tests` (`PolicyTests.cc` covers
`GetLayoutAndSpace`) plus the quota system tests. For Item 3 also run
`test/eos-rename-test` (added by `a6ba5f6c9`), since it is the coverage for the
re-keying that `DiscoverNodes(false)` skips, and check that a config reload
(`eos config reload`) still rebuilds the whole view — that path goes through
`ResetConfig` -> `LoadNodes()` -> `DiscoverNodes(true)`.

## Expected result

| After | MGM side | Wall clock |
|---|---|---|
| baseline | ~5.35 s | 5.95 s |
| Item 1 | ~2.2 s *(measured)* | 2.79 s *(measured)* |
| Item 1 + 2 | ~1.3 s *(measured)* | 1.88 s *(measured)* |
| Item 1 + 2 + 3 | ~1.1 s, and no namespace work on the command path | ~1.7 s |

Items 1 and 2 are done: 5.95 s → 1.88 s, a 3.2x improvement. `AttrLs` in
`eos ns stat` now grows by one per quota node *once* at startup (config load
constructs every `SpaceQuota`, and the constructor computes the factor) instead
of once per node per `quota ls`.

Roughly one `quota ls` every `sLayoutFactorAge` seconds crosses the layout
factor TTL boundary and pays the ~2.8 s recompute, so an occasional ~4.7 s run
among the fast ones is expected. Item 3 does **not** change this — only the
rejected background-thread design would have, and it was rejected on the grounds
that a permanent background load is a worse trade (see "Why no thread"). Item
1's optional follow-up — reading the container's attributes by id instead of
`_attr_ls(pPath)` — is the cheap way to shrink that spike, and it is now easy
because `mContId` exists.

**Note on what Item 3 is now worth.** With Items 1 and 2 done, the remaining
~1.3 s of MGM time is roughly 1.05 s of table formatting and only ~0.25 s of
namespace work, so Item 3 buys little wall clock *on this test instance*. Its
value is elsewhere and still real:

- it removes the ~2 s that appears on an instance whose quota nodes all carry
  accounting data, where `getAllIds()` returns every node and the `getUri` loop
  in `LoadNodes` runs at full scale (see Item 3's opening section);
- it makes `quota ls -p <path>` stop paying the all-nodes cost, which it still
  does today;
- it fixes the unlocked-refresh defect described in step 4.

For pure wall clock on an instance like the test one, the table formatting is
now the bigger target.

Once these land, the dominant remaining cost is table formatting — about
0.8 s for `TableFormatterBase::GenerateTable` plus row construction across
~5000 single-row tables, each with its own `ostringstream` and width-correction
pass. Worth a separate look only after the namespace work is gone.

## Out of scope (separate commits, noted while reading the code)

- `SpaceQuota::PrintOut` translates uid/gid names *before* `std::sort` +
  `std::unique`, so every id is translated once per quota tag — about 5× more
  `Mapping::UidToUserName` / `GidToGroupName` calls than needed. Steady-state
  cost is only ~50 ms because the name cache absorbs it, but it was ~460 ms on
  a cold cache, and `Mapping::UidToUserName` deliberately does not
  negative-cache an NSS *error*, so a flaky LDAP/sss backend makes this
  expensive. Moving the sort/unique above the translation block is a two-line
  fix.
- `SpaceQuota::PrintOut` masks the gid with `0xfffffff` (seven `f`s) where the
  uid path uses `0xffffffff`; since `Index()` is `(tag << 32) | id`, gids
  ≥ 2^28 are silently truncated in the listing.
- `SpaceQuota::mLastEnableCheck` is initialised in the constructor and never
  used again.
- `eos_info("uids_size=%i, gids_size=%i", ...)` in `SpaceQuota::PrintOut`
  passes `size_t` to `%i`. Note that this message is invisible in practice
  anyway: the MGM installs a *deny* log filter containing `PrintOut` and
  `AddQuota` (see `XrdMgmOfsConfigure.cc`), which suppresses `eos_info` and
  `eos_debug` from any function with those names. Use `eos_notice` or rename
  the function if this log line is wanted.
