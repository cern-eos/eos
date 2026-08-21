# MGM File Scheduler Architecture — the FlatScheduler

> **Audience:** developers working on file placement / access scheduling in the
> MGM. This document is the overview of the `mgm/placement/` **FlatScheduler**:
> its data model, the layers, how a placement and an access request flow through
> it, and the design decisions worth knowing before changing any of it.
>
> **Context:** the MGM still ships a second, older scheduler (the *geotree*
> engine under `mgm/geotree/` + `mgm/geotreeengine/`). The flat scheduler was
> written to replace it. Both are live: the geotree engine is the default, and a
> space opts into the flat scheduler with one config key. §1 explains the
> coexistence; the rest of the document is about the flat scheduler.

---

## 1. Two schedulers, one facade

`Scheduler::Placement` and `Scheduler::Access` (`mgm/scheduler/Scheduler.cc`)
are the **sole facade for scheduling decisions**. Every placement and access
request in the MGM goes through these two entry points; the choice of engine
lives inside them and nowhere else:

```
Placement  → FlatSchedulerPlacement → (fail) → GeoTreePlacement   (geotree engine)
Access     → FlatSchedulerAccess    → (fail) → GeoTreeAccess      (geotree engine)
```

The `FlatScheduler*` and `GeoTree*` halves are **private** helpers. The two
public entry points are the only scheduling API the rest of the MGM calls, so
switching a space's engine is one config key with no caller outside this class
naming an engine. The drain-destination path funnels through the same facade via
`Scheduler::PlaceDrainReplica` (§10).

**How the choice is made.** Each space resolves to a **`SchedConfig`**: which
engine serves it (`SchedEngineT::kGeoTree` or `kFlat`) and, when that is the
flat scheduler, which `PlacementStrategyT` it picks with. The two are separate
types on purpose — the engine used to be smuggled into `PlacementStrategyT` as
an extra enumerator, `kGeoTreeLegacy`, which made "is this a strategy" and
"which engine" the same question. Routing now asks the engine:

```cpp
if (!config.IsFlat()) {
  return EINVAL;              // hand this space to the geotree engine
}
```

`SchedConfig` is two bytes and trivially copyable, so it rides in a single
`std::atomic<SchedConfig>` for the global default and in one packed
`atomic<int32_t>` per space (`ClusterMgr::mConfiguredSchedConfig`, `-1` meaning
"no override"). Engine and strategy therefore move as a unit: a scheduling
thread can never catch the flat engine paired with a strategy from a different
configuration.

Two places decide the default, and the second overrides the first:

- `FsScheduler`'s constructor default (`FsScheduler.hh`), and
- **`SchedConfigFromStr`'s fallback**, which is the one that actually governs —
  `FsScheduler::LoadConfig` pushes `GetConfigMember("scheduler.type")` through
  it for *every* space at startup, and an unconfigured space arrives as the
  empty string.

Both resolve to `kGeoTreeSchedConfig`, so an untouched installation runs geotree
end to end. A space is opted in (and back out) by name:

```
space config <space> space.scheduler.type=flat:geo   # flat scheduler, geo-aware
space config <space> space.scheduler.type=geotree    # the legacy engine
```

(The CLI takes the `space.` prefix; the config member itself is
`scheduler.type`.)

**The value names the engine first.** That is deliberate: the previous grammar
spelled these two `geo` and `geotree` — four characters apart, and *different
engines*, since `geo` is `kGeoScheduler`, a flat-scheduler strategy, while
`geotree` was the routing marker. Every flat strategy now carries the `flat:`
prefix (`kFlatEnginePrefix`) and `geotree` names the legacy engine on its own;
the prefix is refused on `geotree` itself, which would be a contradiction.

Two parsers sit behind it, and which one a caller gets is the point:
- `ParseSchedConfig` is **strict** — `std::optional`, `nullopt` for a value it
  cannot name. Everything that can refuse the input uses it, so
  `space config space.scheduler.type=roundrobbin` is now an `EINVAL` instead of
  a success message that silently left the space on geotree. `SpaceCmd` stores
  `SchedConfigToStr` of the parse rather than the raw string, so a configuration
  written under the old grammar rewrites itself the first time it is touched.
- `SchedConfigFromStr` is the **lenient** wrapper,
  `value_or(kGeoTreeSchedConfig)`, for the two read paths with nowhere to report
  a bad value: the boot restore, which must not abort over one unparseable
  space, and the per-request override in `Scheduler::FlatSchedulerPlacement`.

Every pre-prefix spelling still parses to what it always meant, so an upgrade
moves no space between engines; `IsDeprecatedSchedConfigSpelling` is what lets
the command layer say so.

When opted in, the flat scheduler serves that space's placement, access **and**
draining.

With the marker gone, **every `PlacementStrategyT` enumerator is a real
strategy**: `MakeSelectionStrategy` returns nullptr only for the `Count`
sentinel, the strategy array has no hole, and `IsValidPlacementStrategy` is back
to meaning what it says. Retiring geotree is now a mechanical `s/flat://` in the
name table plus deleting `SchedEngineT` and the three `IsFlat()` guards in
`Scheduler.cc`.

### Why the flat scheduler exists

The geotree engine is a header-only, ~2.4k-line templated core (`SlowTree` →
`FastTree`) wrapped by a ~6k-line engine with thread-local snapshot copies,
background refresh threads and multiple mutexes. It is fast but hard to test in
isolation and hard to change safely. The flat scheduler is ~8k lines (a large
share of it comment), with a flat data model, pluggable strategies, RCU
snapshots, and focused unit tests.

| | Geotree (legacy) | FlatScheduler |
|---|---|---|
| Model | `SlowTree` (pointer tree) → `FastTree` (flat sorted arrays), per group | `ClusterData` (two flat arrays), per space |
| Geo-awareness | Native (the tree *is* the geo hierarchy) | In the hierarchy + the descent (§5) and the access proximity filter (§6) |
| Capacity/fill | fill ratio, free space, spreading cap | fill-decayed weight + `bookingsize` free-space check + prebooking (§7) |
| Concurrency | thread-local FastTree copies + refresh threads | RCU copy-on-write snapshots + in-place atomics (§8) |
| Size | ~11.7k LOC (`mgm/geotree` + `mgm/geotreeengine`) | ~8.1k LOC (`mgm/placement`) |
| Status | default | opt-in per space |

---

## 2. Data model — `ClusterDataTypes.hh`

The topology is stored **flat, in two vectors indexed by `item_id_t` (int32)**,
using the sign of the id to distinguish node kinds:

- `item_id_t > 0` → a **disk** (leaf); maps 1:1 to an EOS `fsid`, stored at
  `disks[id - 1]`.
- `item_id_t < 0` → a **bucket** (interior hierarchy node), stored at
  `buckets[-id]`.
- `item_id_t == 0` → the **root** bucket slot.

> "Flat" = the whole tree is a pair of contiguous arrays; parent→child links are
> just integers in `Bucket.items`, navigable by the sign of the id.

Core types:

- **`Disk`** — packed to **exactly 16 bytes** (`static_assert sizeof(Disk)==16`)
  so 4000 disks fit in a 64 kB cache. Fields: `fsid_t id`, atomic `ops`
  (`FsOpMask`, §9), atomic `active_status`, atomic `weight` (uint8), atomic
  `percent_used` (uint8), atomic `free_gib` (uint32), atomic `booked_gib`
  (uint32). `ops` is one bit per `SchedOp` — the traffic class (client or
  internal) crossed with the direction (read, update, create) — and
  `Disk::AllowsOp()` is the single question every selection point asks of it.
  It replaced the one-dimensional `config_status`, which could not say "no
  client traffic but keep draining through this disk". The atomics allow live
  status/weight/space updates **without rebuilding the topology**. The three
  space-related fields are not interchangeable: `percent_used` drives how *attractive* a disk is
  (`GetEffectiveWeight`), `free_gib` whether a given file *fits*, `booked_gib`
  remembers what was placed but is not yet visible to the FST's statfs (§7).
- **`Bucket`** — `id`, `parent`, `geo_atom` (the geotag atom naming it, e.g.
  `"rack3"`, empty for the root and groups), `total_weight`, `bucket_type`,
  `level`, `std::vector<item_id_t> items` (children), an atomic `denied_ops`
  mask (§9), plus three fields the descent reads directly:
  - `child_type` (`ChildType`: `kNone`, `kDisks`, `kGroups`, `kGeoBuckets`) —
    **the kind of the children, stated rather than sniffed**. The builder calls
    `RecordChildType()` before every append and *refuses* a child of another
    kind, which turns the never-mix rule from a convention into an enforced
    invariant, and lets `PlaceInBucket` dispatch on one byte without looking at
    a child (§5).
  - `flat_view` — for a scheduling group, the id of its synthetic leaf view
    (below); 0 for every other bucket.
  - `group_index` — the scheduling-group index of a group bucket,
    `kNoGroupIndex` otherwise; the way back from bucket to index for display.

  `total_weight` is the **raw capacity-weight sum** of the whole subtree: the
  incremental `AddDisk`/`RemoveDisk` path and the bottom-up
  `AggregateBucketWeights` rebuild agree on that one definition. Fill decay is
  *not* folded in here; it is applied live at disk level by `GetEffectiveWeight`
  when a leaf is actually weighed, so branch weights stay deterministic instead
  of freezing the fill levels seen at commit time.
- **`BucketType`** — `GROUP, RACK, ROOM, SITE, ROOT, NODE`, plus `FLATVIEW` (the
  synthetic leaf view, §5) and the `INVALID` sentinel a default-constructed
  `Bucket` (a hole in the pre-allocated id range) carries, so that a hole is
  never mistaken for a `GROUP` (which is type 0). Note that `bucket_type` says
  what a bucket *is*, `child_type` what it *holds* — the descent branches on the
  latter.
- **`ClusterData`** — the snapshot: `vector<Disk> disks`, `vector<Bucket>
  buckets`, `vector<item_id_t> disk_parents` (the bucket each disk hangs from,
  kept in step with `disks`; what the access path resolves a replica's geo
  position with), and `unordered_map<uint64_t, item_id_t> geotag_index` (keyed
  by `GeoChildKey`: one geotag atom below one parent → the bucket naming it).
  The geotag is carried **structurally** by the hierarchy and read back with
  `GetGeoTag()`, which walks up concatenating each bucket's `geo_atom` — there is
  no per-disk geotag side-channel and no hash → string registry. `geotag_index`
  is a lookup accelerator only: a hit is verified against the bucket's `geo_atom`
  (see `FindGeoChild`), so a 64-bit hash collision degrades to a correct miss
  rather than resolving to the wrong bucket.
  `GetBucket(id)`/`GetDisk(fsid)` are the single validity definition: they
  return `nullptr` for a positive/out-of-range id, a hole, or a slot whose
  stored id does not match — every setter and the descent guards go through
  them instead of re-deriving the sign-and-range test.

### The two arrays, concretely

A snapshot of one space, with a single group populated, looks like this. There
are no pointers and no tree nodes anywhere — the parent→child links are the
integers in `items`, and the *sign* of each one says what it names:

```
  buckets[]   index = -id                             disks[]   index = fsid
  ┌─────┬──────────┬──────────┬───────────────┐   ┌──────┬────────┬───────┬──────┐
  │ id  │ type     │ geo_atom │ items[]       │   │ fsid │ weight │ %used │ free │
  ├─────┼──────────┼──────────┼───────────────┤   ├──────┼────────┼───────┼──────┤
  │   0 │ ROOT     │ ""       │  -1  -2  -3   │   │  42  │  120   │  38%  │ 4TB  │
  │  -2 │ GROUP    │ ""       │  -4 -12 -20   │   │  43  │  100   │  91%  │ 200G │
  │  -4 │ SITE     │ "site1"  │  -5           │   │  77  │  100   │  61%  │ 2TB  │
  │  -5 │ ROOM     │ "room2"  │  -6           │   └──────┴────────┴───────┴──────┘
  │  -6 │ RACK     │ "rack3"  │  42  43  44   │
  │ -31 │ FLATVIEW │ ""       │  42 43 44 …   │ ← every disk under group -2,
  └─────┴──────────┴──────────┴───────────────┘   reached only via -2.flat_view,
                                                  never listed in anyone's items
  id sign IS the type:  0 = root   <0 = bucket   >0 = disk
```

Two structural rules the builder guarantees and the descent then relies on:

- **A bucket never mixes disks and sub-buckets**, and the builder enforces it by
  refusing the mismatched append (`Bucket::RecordChildType`) rather than trusting
  callers. An untagged disk gets a `<nogeotag>` placeholder bucket under its
  group instead of hanging off the group directly.
- **Bucket type is positional, not free-form.** Geotag atom *n* becomes level
  *n* (`GeoLevelToBucketType`): site, room, rack, and anything deeper reported as
  a node, truncated at `kMaxGeoDepth = 8`.

Alongside the tree, each scheduling group carries a **flat leaf view**: a
synthetic `FLATVIEW` bucket holding every disk of the group's subtree, pointed at
by `Bucket::flat_view` and written straight into its slot — it is never pushed
into anyone's `items`, so the ordinary descent cannot stumble into it. The
builder keeps it in step disk by disk (`TrackDiskInFlatView` /
`UntrackDiskFromFlatView`), so an incremental insert maintains it as cheaply as a
full build. §5 explains what it buys.

### Shape of the hierarchy

```
root  (level 0)
 └── scheduling group        allocated id       (level 1)
      └── site               allocated id       (level 2)
           └── room                             (level 3)
                └── rack                        (level 4)
                     └── disks
```

**The scheduling group sits above the geo levels, not below them.** Every
replica of a file must live in one scheduling group, so the group is what a
placement picks *first* and the geo levels are what it spreads over *inside* that
choice. Putting geo on top would let a scattered placement cross group
boundaries.

The consequence is that the geo hierarchy is **replicated per group**: the same
`site::room::rack` exists once below each group that has disks there. That is why
`geotag_index` is keyed by *(parent, atom)*, not by the atom alone.

Every bucket id, groups included, is handed out by
`ClusterMgr::SnapshotBuilder::AllocBucketId()`, which counts down from below every id already
in use, so nothing can be allocated twice. A scheduling group is therefore not
addressable by arithmetic on its index: `ClusterData::group_buckets` maps the
index a request carries — `forced_group_index`, or the group an `FsDescription`
names — to the bucket, and `Bucket::group_index` is the way back for display.
Group ids used to be derived as `-10 - index`, which meant a group registered
into a live snapshot found its id already taken by a geo bucket of the previous
build, and the insert had to be refused until the next full rebuild.

Topology is built by `BuildClusterData()` (`ClusterBuilder.cc`) from a
`vector<FsDescription>` — the cut that lets the builder be tested without an
`FsView` behind it. `EosClusterMgrHandler` fills those descriptions in under the
view lock and knows nothing about the shape of the tree. (The builder tokenises
geotag strings rather than walking each `FsGroup`'s `GeoTree`, which is itself
just the result of tokenising the same strings — walking it would buy nothing and
cost the `FsView` dependency the tests avoid.)

---

## 3. Layered architecture

```
XrdMgmOfs::mFsScheduler  (FsScheduler)          ── MGM-facing facade, per-space
      │
      ├── per-space ClusterMgr → ClusterData     ── RCU copy-on-write snapshot
      │        ├── SnapshotBuilder                ── build transaction: Commit/Abandon
      │        ├── ClusterBuilder                 ── FsDescription[] → topology
      │        └── ClusterDataFormatter           ── all human-readable rendering
      │
      └── FlatScheduler                           ── engine: strategy array + descent + access
              └── SelectionStrategy[]             ── placement interface + shared helpers
                    ├── RoundRobinStrategy         ── RR / TL-RR / random / fid
                    ├── WeightedRandomStrategy      ── weighted rendezvous placement (also kGeoScheduler)
                    └── WeightedRoundRobinStrategy  ── cumulative weight table + stride
```

- **`FsScheduler`** (`FsScheduler.hh/.cc`) — top-level facade. Owns a
  space→`ClusterMgr` map and one `FlatScheduler`; resolves the per-space strategy
  (global default + per-space overrides), takes RCU read locks, and forwards to
  the engine. The per-space configuration — fill limits, disabled branches and
  the strategy override — is **not** held here: each `ClusterMgr` owns its own.
  The fill limits and disabled branches are stamped onto every snapshot the
  manager commits, so a rebuild carries them forward with no re-stamping; the
  `SchedConfig` override does not shape the snapshot (its engine half is read to
  route the request, before a snapshot is even consulted) and so
  lives in a lock-free atomic on the manager, read straight on the scheduling
  path. The only per-space state left on `FsScheduler` is the global default
  strategy. A space is bound to a manager the moment it is first configured (an
  empty manager is created eagerly), so configuration always has a manager to
  live on and there is no separate pending store (§9).
- **`FlatScheduler`** (`FlatScheduler.hh/.cc`) — the engine. Holds
  `std::array<unique_ptr<SelectionStrategy>, TOTAL_PLACEMENT_STRATEGIES>` and
  descends the bucket hierarchy. Space-agnostic; operates on `const
  ClusterData&`.
- **`SelectionStrategy`** (`SelectionStrategy.hh`) — abstract **placement**
  interface (`Placement`) plus the shared validity helpers every strategy must
  agree on: `ValidateArgs` (resolves the start bucket through `GetBucket`, the
  same validity definition used everywhere else), `ValidDisk` (bounds +
  `excludefs` + the disabled-branch test + online + the disk's own `ops` mask
  allows the requested `SchedOp` + free space) and `IsAccessCandidate`
  (zero-fsid + `forcedfsid` narrowing + `excludefs` + `ValidDisk`). Since the
  branch rules speak the same `FsOpMask` vocabulary as a disk's own permissions
  (§9), both are the same question and `ValidDisk` asks it once — there is no
  separate placement-only selector any more. Header-only. A strategy picks
  *items within one bucket*; geo awareness belongs to the `FlatScheduler` layer (§5, §6), not here. Access does
  **not** go through this interface — it does not vary per placement strategy —
  but the helpers above are shared with the access path.
- **`ClusterBuilder`** (`ClusterBuilder.hh/.cc`) — turns a
  `vector<FsDescription>` into a topology. `BuildClusterData` is the bulk build
  and `AddFsToCluster` the single-filesystem step it is made of, which is also
  what the incremental insert calls, so bulk and incremental cannot diverge (§10).
- **`ClusterDataFormatter`** (`.hh/.cc`) — every human-readable rendering of a
  snapshot. Kept out of `ClusterDataTypes.hh` so the hot data header pulls in no
  formatting machinery and an admin surface change never recompiles the engine.
- **`InlinedVector`** (`InlinedVector.hh`) — a small growable array whose first N
  elements live inside the object. Every per-bucket scratch buffer on the hot
  paths (the branch lists of the descent, the candidate list of an access, the
  ranked/cumulative tables of the weighted strategies) uses it, so a normal
  request never reaches the allocator while an oversized bucket still works, at
  the cost of one allocation.
- **`ClusterMgr` / `ClusterMgr::SnapshotBuilder`** (`ClusterMgr.hh`) — build and
  hold the snapshot. `ClusterMgr` keeps `ClusterData` in an `atomic_unique_ptr`
  under an RCU mutex; the private `AddClusterData` swaps a whole new snapshot and
  frees the old pointer outside the write lock. `SnapshotBuilder` is a nested,
  short-lived build transaction: it accumulates a private `ClusterData` draft and
  finalizes it exactly once — `Commit()` publishes it via `AddClusterData`,
  `Abandon()` discards it and keeps the manager's current snapshot. The
  destructor commits any build not yet finalized, so a scope-exit build still
  publishes; an incremental update that fails part-way calls `Abandon()` so a
  degraded draft never goes live (e.g. `InsertFs` keeps the old snapshot on a
  failed add instead of publishing one with the disk missing). It is the only
  caller of the private `AddClusterData`, and its own constructors are private,
  so a build can be opened only through the
  `ClusterMgr::GetSnapshotBuilder{,WithData}()` factories.

---

## 4. Strategies

Placement strategy is per-space. All strategies pick items within a single
bucket; the enum has more values than there are classes because one class can
back several seeding behaviours.

- **`RoundRobinStrategy`** (`RoundRobinStrategy.hh/.cc`) — one class
  backing **four** enum values (`kRoundRobin`, `kThreadLocalRoundRobin`,
  `kRandom`, `kFidRandom`), differentiated by a pluggable **`RRSeeder`**.
  `MakeRRSeeder` selects:
  - `GlobalRRSeeder` → shared atomic `RRSeed` (strong global fairness,
    contended);
  - `ThreadLocalRRSeeder` → per-thread randomized cursors (fast, per-thread
    fair);
  - `RandomSeeder` → uniform random;
  - `FidSeeder` → `seed = index ^ replicas ^ fid` (deterministic per file).

  `Placement` picks via `pickIndexRR` up to `MAX_PLACEMENT_ATTEMPTS = 100`,
  skipping duplicates and invalid disks.
- **`WeightedRandomStrategy`** — stateless. `Placement` is weighted **rendezvous
  hashing** (Efraimidis–Spirakis): every candidate is scored once by `-log(u) /
  weight` with `u` from a hash of `(fid, item, salt)`, the `n_replicas` lowest
  scores win; a request without a file identity falls back to a per-call random
  draw. It has no access path of its own (see §6).
- **`WeightedRoundRobinStrategy`** — builds a cumulative weight table of the
  valid candidates per call and walks the weight space in strides from a
  per-bucket RR cursor, so successive requests spread proportionally to the
  weights. It has no access path of its own (see §6).
- **`kGeoScheduler` has no class of its own** — `MakeSelectionStrategy` backs it
  with `WeightedRandomStrategy`. Geo awareness is not a way of picking items
  inside a bucket, it is a way of walking the hierarchy, and that lives in the
  descent (§5). What the strategy value selects is therefore only *the picker at
  each level* — a capacity-aware one — plus the fact that the descent is allowed
  to follow the client's geotag at all.

**Round-robin cursors.** RR needs a persistent per-bucket cursor so successive
files spread across disks:
- **`RRSeed<T>`** (`RRSeed.hh`) — atomic counters, one per bucket;
  `Get(index, n) = fetch_add(n)` atomically reserves a contiguous RR window.
  Backs `GlobalRRSeeder`. The counters sit in fixed-size chunks allocated on
  demand rather than in one contiguous array, so the table can **grow with a
  topology that gained buckets without ever moving a counter a concurrent
  placement is reading** — growth takes a mutex, reads stay lock-free.
- **`ThreadLocalRRSeed`** — a `thread_local` vector of plain counters
  **initialized to random values** so threads don't all start at bucket 0
  (avoids thundering-herd). No atomics/locks → faster, at the cost of only
  per-thread fairness.

---

## 5. Placement: call path and geo-aware descent

`FsScheduler::Schedule(space, args)`:
1. Resolve per-space strategy; RCU read-lock; fetch the space's `ClusterData`.
2. Retry `FlatScheduler::Schedule` up to `MAX_GROUPS_TO_TRY = 10` (with a fresh
   salt each time) until `IsValidPlacement(n_replicas)`.

`FlatScheduler::Schedule(cluster_data, args)` validates replicas / bucket id,
falls back to `mDefaultStrategy` if needed, rejects a null/invalid strategy with
`EINVAL`, grows the per-bucket strategy state to the snapshot it is about to
descend (`EnsureCapacity`, §8), then runs a **chain descent** from the root/space
bucket.

`FlatScheduler::PlaceInBucket()` is the single door into a bucket — strategy
picks, forced starts and spilled shortfalls all funnel through it, which is why
the guards common to every route live there and nowhere else: the id is resolved
through `GetBucket` (so a hole or an out-of-range id from a strategy cannot be
followed), the depth is capped at `MAX_PLACEMENT_DEPTH` (so a malformed hierarchy
cannot spin under the RCU read lock), and a disabled or empty bucket takes
nothing. It then dispatches on `child_type` — one byte, no child inspected — to
one helper per kind of level:

| `child_type`  | helper                   | rule of the level                       |
|---------------|--------------------------|-----------------------------------------|
| `kDisks`      | `PlaceOnDisks`           | the strategy picks the whole quota, leaves |
| `kGroups`     | `PlaceInGroup`           | exactly one group takes everything      |
| `kGeoBuckets` | `PlaceAcrossGeoBranches` | split home/away, then spill the shortfall |

Splitting the descent this way keeps each rule stated once, in a function named
after the level it governs, instead of interleaved in one recursive body; the
recursion itself stays in `PlaceInBucket`.

The quota split hands the remainder to the first branches, so 3 replicas over 2
sites go 2 + 1.

**Geo awareness.** `Schedule()` splits the client's geolocation into atoms and
carries them down the descent in a `DescentContext`, together with
`ncollocatedfs` — the number of replicas that should stay next to the client.
The caller derives it from the placement policy and layout
(`Scheduler::GetCollocatedReplicas`, shared with the geotree path so the two
cannot drift):

| policy       | collocated replicas |
|--------------|---------------------|
| `kScattered` | 1, or 0 without a client geotag |
| `kHybrid`    | depends on layout: 1 plain, `n-1` replica, `n - redundancy` otherwise |
| `kGathered`  | all of them |

At a geo level the descent looks up the child matching the next atom
(`FindGeoChild`, an `O(1)` index hit). That branch gets `min(quota,
ncollocatedfs)` replicas and keeps consuming atoms as it goes deeper; the rest
spread over the siblings, which give up on the client's path. **A scheduling
group is not a geo level** — descending into one leaves the atom index where it
is, otherwise the group would eat the client's site.

**When geo placement is off.** The atoms are only carried down when the request
is geo scheduled *and* the client has a geolocation *and* `ncollocatedfs > 0` —
so `kGeoScheduler` is what buys the geo descent, and every other strategy places
as if the client were untagged. A descent with no geo constraint left to honour
has no reason to walk the geo levels: a scheduling group is then served straight
from its **flat leaf view** (`Bucket::flat_view`, a synthetic `FLATVIEW` bucket
holding every disk of the group's subtree, never linked as anyone's child), one
strategy pick over the whole group instead of one per level. Nothing is lost by
it — the group *is* the failure domain, its disks sit on distinct nodes by
design. Disabled branches survive the shortcut too: the interior buckets are
never visited, so instead of the bucket refusing entry, each candidate disk
answers for itself in `SelectionStrategy::ValidDisk()`, which resolves
`IsBranchDenied()` against the disk's real geo ancestry — the flat view holds
ids and never becomes a parent, so that ancestry is intact. The walk is only
paid while a rule is live; `HasDeniedBranches()` short-circuits it to a
single load otherwise.

**Deficit redistribution (up-root fallback).** A branch that cannot take its
share leaves a shortfall, which the spill pass at the tail of
`PlaceAcrossGeoBranches()` offers to every branch of the bucket — the ones not
yet asked first, the ones
already asked after — with everything already placed pushed onto `excludefs` so
nothing comes back twice. This keeps an undersized home branch
from failing the placement outright — geotree's `allowUpRoot`, without the
`goto`. The one place it must *not* happen is the group level: spilling a
shortfall into a second group would split a file's replicas across two groups, so
a group that cannot take the whole file is abandoned whole (the retry loop above
picks another with a fresh salt).

### A request, end to end

Three replicas for a client at `site1::room2::rack3` with `ncollocatedfs = 1`,
over the snapshot drawn in §2:

```
FsScheduler::Schedule(space, args)
  │ RCU read lock → ClusterMgr → snapshot ptr        ┐ up to 10 attempts,
  └─► FlatScheduler::Schedule(cluster_data, args)    │ salt = 0,1,2…
        geo_atoms = [site1, room2, rack3]            │ retried ONLY on ENOSPC
        │                                            ┘
        └─► PlaceInBucket(0, n=3, atom=0)
              children are GROUPs → ask strategy for exactly 1, atom stays 0
              │                                    (no spill here — ever)
              └─► PlaceInBucket(-11, n=3, atom=0)
                    home = FindGeoChild(-11,"site1") = -23,  n_home = min(3,1) = 1
                    │
                    ├─ HOME  ─► PlaceInBucket(-23, n=1, atom=1)   ← atom advances
                    │             └─► room2 ─► rack3 ─► strategy → disk 42
                    │
                    ├─ AWAY  n=2: ask strategy for 2+1 branches, drop home,
                    │        split remainder-first → site2:1, site3:1
                    │        └─► PlaceInBucket(-24, n=1, atom=3)  ← atom = END,
                    │              └─► … ─► strategy → disk 77       no geo
                    │                                               preference
                    └─ SPILL (only if still short of 3)               below here
                         offer the deficit to every child of -11:
                         unasked ones first, then the ones already asked;
                         everything placed so far is pushed onto excludefs
                         so a revisited branch must yield a *different* disk
        │
        └─ 3/3 filled → BookDiskSpace(bookingsize) on 42, 77, …
```

Three interactions worth reading off that walk:

- **The descent never asks what a bucket *is*, only what it *holds*.** Every
  decision above comes from `child_type` and `geo_atom`; `bucket_type` is read
  by the operator-facing views and by the builder, not by the descent. Which is
  why site, room and rack are interchangeable here — a site with racks directly
  under it behaves exactly like a room with them — while the group level stands
  out purely because its children are groups.
- **`atom_index` is the geo cursor, and it only ever moves one way.** It advances
  into the home branch and jumps straight to `geo_atoms.size()` on entering an
  away branch — "the client's location is behind us" — so everything below an
  away branch is chosen on weight and round-robin alone.
- **The strategy is never asked to rank a whole bucket.** At a disk bucket it is
  asked for the remaining quota in one call; at an inner bucket for
  `n_away + (home ? 1 : 0)` — one extra so the home branch can be dropped from
  the answer without running short. Widening that request is not free: see §4,
  `RoundRobinStrategy` folds `n_replicas` into its seed, and
  `WeightedRoundRobinStrategy` dedups strided samples.

The same request from a client with **no** geotag (or under any strategy other
than `kGeoScheduler`) collapses to two hops — root → group → the group's flat
leaf view — with one strategy call over every disk of the group. That is the
common case in a production installation, and it is the reason the flat view is
maintained at all: the geo walk exists to honour a constraint, and there is no
point paying for it when there is none.

---

## 6. Access: the proximity filter

The access path has no descent — the candidates are the few replicas of one
file — so geo awareness is a **proximity filter** in `FlatScheduler::Access`.

1. `MarkUnavailableReplicas` flags every replica that cannot serve the request
   (offline, mask does not allow the operation, excluded, under a denied
   branch) into `unavailfs`. For a RAIN read this refuses the request
   (`ENETUNREACH`) unless at least `n_replicas` stripes are still up.
2. The remaining replicas are ranked by how many leading geotag atoms they share
   with the client (`ClusterData::GetGeoOverlap`, a bounded walk up the
   `disk_parents` index comparing atom *strings*). Everything below the best
   overlap is demoted **for the selection call only**.
3. `AccessRandom` picks one of the closest reachable replicas **uniformly at
   random** — geotree's closest-replica-first ordering, with a uniform tie-break
   within the closest set. Access is deliberately *not* capacity-weighted: a
   weighted (rendezvous/HRW) pick is deterministic per file, so every read of a
   hot file would land on the same replica and bottleneck one disk. Weight steers
   where new files are *placed*, not which replica an existing file is read from.

Three edges are deliberate:
- Demotions never reach the caller's `unavailfs` — a distant replica is not an
  unreachable stripe and must not be reported to the RAIN driver as one.
- A `forcedfsid` skips the filter entirely — forcing beats proximity, as it does
  in geotree; demoting the forced target would turn a valid forced access into a
  failure.
- A client whose geotag matches nothing (or is empty) demotes nothing, so the
  pre-geo selection is preserved bit for bit.

Overlap compares atom *strings* (the buckets carry `geo_atom`), so there is no
hash-collision caveat: two distinct atoms can never rank as a false match.

---

## 7. Free space and prebooking

`Disk` carries `free_gib` (does a file fit?) alongside `percent_used` (how
attractive is the disk?). Points worth knowing:

- The unit is GiB (`kFreeSpaceUnit`), free space rounded **down** on the way in
  (`FreeSpaceToGiB`) and a booking rounded **up** on the way out
  (`BookingToUnits`), so the check errs toward refusing.
- Usable free space is `stat.statfs.freebytes` minus the headroom the FST keeps
  back, matching the geotree definition. This has exactly one implementation,
  `GetUsableFreeBytes()` in `FsScheduler.cc`, shared by the topology builder and
  the FST publish listener so the two can never drift.
- A `bookingsize` of 0 skips the check — what callers that do not know the size
  ask for, and what the access path always wants (an existing replica is readable
  however full its disk is). Otherwise `bookingsize` flows
  `PlacementArgs::bookingsize` → `ValidDisk` → `HasRoomFor()`.

**Prebooking.** Concurrent placements must see each other's reservations instead
of over-committing a disk until the next full rebuild. At the one point where a
placement becomes final, `FlatScheduler::Schedule` calls
`ClusterData::BookDiskSpace` for every selected disk. Only a *returned* placement
books — a failed descent leaves nothing behind, so the salt-retry loop needs no
rollback, and the drain path (which sets `bookingsize` to the drained file's
size) gets booking for free. `BookSpace` debits `free_gib` (CAS loop, clamped at
zero) and credits `booked_gib`.

**Reconciliation with FST publishes.** `free_gib` is not only set at snapshot
build: the `FileSystemMonitorThread` (`mgm/ofs/cmds/FsConfigListener.inc`)
subscribes to `stat.statfs.freebytes` and `stat.statfs.filled` (either key
refreshes both fields in one FsView lookup) and routes updates through
`FsScheduler::SetDiskFreeSpace`. Because the published figure cannot yet include
bytes that are booked but not written, the publish handler retires `booked_gib`
(`exchange(0)`) and stores `max(reported − booked, 0)`: every booking discounts
**exactly one** publish — the first one after it, the only one that cannot see
the write. A booking whose write already landed is subtracted once too often
(safe direction, corrected by the next publish); a booking whose file was never
written stops haunting the disk after one publish interval instead of leaking
forever. A full rebuild likewise drops outstanding bookings. The MGM-side check
is a coarse filter either way — the FST's own `bookingsize` check at open is the
backstop.

---

## 8. Concurrency model

- `ClusterMgr` holds `ClusterData` in an `atomic_unique_ptr` guarded by an RCU
  mutex. Readers take an `RCUReadLock` for wait-free reads; a full topology
  change swaps a new snapshot under the write lock and frees the old pointer
  outside it.
- Live per-disk status/weight/space changes mutate the atomics in the current
  snapshot **in place** — no rebuild, no epoch bump. A `ClusterMgr` that never
  committed a snapshot (a space present only as an empty group) holds a null
  `ClusterData`; every live setter, `GetState`, and the `FsScheduler`
  `Schedule`/`Access` paths null-check it and fail softly rather than
  dereferencing it.
- `FsScheduler` RCU-swaps its single space-keyed `ClusterMgr` map. The per-space
  fill limits, disabled branches and strategy override live on each `ClusterMgr`,
  not in an `FsScheduler` map: the fill limits and disabled branches sit under a
  small config mutex and `AddClusterData` stamps them onto every snapshot before
  it goes live, while the config setters mutate the current snapshot in place;
  the strategy override is a lock-free atomic read on the scheduling path.
  `mClusterRcuMutex` has no separate config mutex beside it: its RCU domain
  already carries an exclusive **writer lock**, so it doubles as the sole
  serializer of the map publishers. Each publisher — the config setters' eager
  bind (`ConfigureSpace`), the rebuild, and the incremental `InsertFs` — holds
  the writer lock across its whole read-modify-swap, which is why they swap the
  map by hand rather than through `ScopedRCUWrite` (that would re-lock the
  non-recursive writer lock). Crucially the frequent case — configuring or
  inserting into a space that **already** has a manager — takes only the RCU
  **read** side and mutates the manager alone; the writer lock is reached only to
  bind a brand new space. Lock order is
  `FsView → mClusterRcuMutex(write) → ClusterMgr::mConfigMutex → ClusterMgr::mClusterMgrRcu`;
  the writer lock is never held while the FsView lock is acquired (the rebuild
  takes the FsView lock inside `MakeClusterMgr` and releases it before publishing),
  matching the boot restore.
- Each snapshot carries an **epoch**; a rebuild or an incremental
  insert/remove bumps it. In-place atomic edits do not. The epoch is what the
  capacity cache keys on for immediate invalidation (§10).
- **The engine outlives the topologies it serves.** `FlatScheduler` is
  constructed with a bucket count, but a snapshot can grow past it — a new
  scheduling group, a new rack. Every `Schedule` therefore calls
  `EnsureCapacity(buckets.size())`, which grows the per-bucket state of every
  strategy (the RR cursor tables) before descending. The growth is one-way and
  guarded by a memo that is raised only *after* the strategies have been told, so
  a concurrent placement that skips the growth is by construction looking at
  state already grown; the tables themselves grow by chunk so no counter a
  concurrent reader holds is ever relocated. A topology that outgrew the engine
  used to throw out of the RR seed lookup.

---

## 9. Configuration and operator surface

All flat-scheduler tuning lives in **space config members** — the source of
truth is persisted by `SpaceCmd.cc` / `SchedCmd.cc` and restored by
`FsScheduler::LoadConfig` through the normal config path (no bespoke replay like
geotree's `geosched:` keys) — the boot restore is the scheduler's own, the MGM
configure only calls it. The authoritative per-space configuration — fill
limits, disabled branches and the strategy override — lives **on the space's
`ClusterMgr`**. Fill limits and disabled branches are stamped onto every
snapshot the manager commits — a rebuild reuses the manager, so they ride
through with no re-stamping; the strategy override does not shape the snapshot
and is held in a lock-free atomic instead, read straight on the scheduling path.
Configuring a space that has no manager yet binds an empty one to it on the spot
(`ConfigureSpace`), so config always has a manager to live on; a rebuild that
does not rebuild that space (it has no filesystem yet) carries the configured
manager forward rather than pruning it (`HasConfiguration` counts any of the
three). Configuring or inserting into a space that already has a manager is the
frequent case and takes only the RCU **read** side — it touches the manager, not
the map; binding a brand new space takes the RCU **writer** lock, which is the
sole serializer of the map publishers (the rebuild, `ConfigureSpace`, and
`InsertFs`), so no separate config mutex is needed (§8). Runtime changes land on
the live snapshot through plain atomic stores with no epoch bump. The only
per-space state left on `FsScheduler` is the global default strategy
(`mDefaultPlctStrategy`, an atomic).

**Strategy type** — `space config <space> space.scheduler.type=geo|geotree|...`
(§1). The global default is an atomic on `FsScheduler`; a per-space override
is an atomic on the space's `ClusterMgr`, bound eagerly like the other config.

**Configurable fill thresholds.** `kDefaultFillCapPercent` /
`kDefaultFillWarnPercent` (95/80) are only the defaults. Each snapshot carries a
`FillLimits` pair of atomics that `GetEffectiveWeight` reads:

```
space config <space> space.scheduler.fillratiolimit=<cap>
space config <space> space.scheduler.fillratiowarn=<warn>
```

`FsScheduler::SetFillLimits` is the one validation point (`warn < cap <= 100`);
the single-key setters validate against the stored pair, so lowering the cap
below the configured warning requires lowering the warning first (the boot
restore applies both as a pair for that reason).

**Per-filesystem permissions.** What a single filesystem accepts is an
`FsOpMask` (`common/FsOps.hh`) — one bit per `SchedOp`, i.e. per traffic class
(`kClient`, `kInternal`) crossed with direction (`kRead`, `kUpdate`, `kCreate`).
It is stored on the filesystem's shared hash under `sched.ops` and mirrored onto
`Disk::ops` through `FsScheduler::SetDiskOps`. Every scheduling decision asks
that mask and nothing else; the legacy `configstatus` key survives only as a
*derived projection* of it (`DeriveLegacyConfigStatus`) for the FSTs, the
geotree engine, the capacity sums and monitoring.

The operator surface is `fs config <fsid> <key>=<value>`, and by fan-out
`node config` / `space config`:

```
fs config <fsid> sched=<spec>       # what the filesystem accepts, no side effects
fs config <fsid> drain=on|off       # start/stop draining, on its own durable key
fs config <fsid> configstatus=rw|wo|ro|drain|off|empty   # legacy compatibility
```

`<spec>` is a preset — `rw`, `ro`, `wo`, `drain`, `internal`, `clientro`,
`none` — or the explicit `client:<ruc>[,internal:<ruc>]` form, where the letters
are `r` read, `u` update, `c` create and `w` for both writes; a class the spec
does not mention is allowed nothing. The headline case the old ladder could not
express is `sched=internal`: no client traffic at all while drain, balancing,
conversion and fsck keep flowing through the disk.

Two keys sit next to the mask and are deliberately *not* folded into it:
`lifecycle` (`kActive`, `kEmpty`, `kOff`) and `drain.requested`. Splitting the
drain verb out means a master failover re-arms drains from what the operator
actually asked for rather than from a status a drain is only one of several ways
to reach. All three are resolved on read (`ResolveSchedOps`, `ResolveLifecycle`,
`ResolveDrainRequested`), falling back to translating the legacy `configstatus`
when the new key has never been written — so an instance upgraded in place
behaves identically from first boot with no migration write.

**Disabled branches** — deny a geotag branch a set of operations:

```
eos sched disable add <space> <geotag> [<spec>]   # default all
eos sched disable rm  <space> <geotag> [<spec>]
eos sched disable ls  [<space>]
```

`<spec>` is the same `FsOpMask` vocabulary, read as what the branch is closed
*for*. `ParseDeniedSpec` accepts three alias groups plus the explicit form, and
every one of them lands on a mask:

| `<spec>` | Denied `SchedOp`s | Mask | Persisted as |
|---|---|---|---|
| `plct` | both classes × create | `0x24` | `client:c,internal:c` |
| `access` | both classes × {read, update} | `0x1b` | `client:ru,internal:ru` |
| `all` *(default)* | all six | `0x3f` | `client:ruc,internal:ruc` |
| `client` | client × {read, update, create} | `0x07` | `client:ruc` |
| `internal` | internal × {read, update, create} | `0x38` | `internal:ruc` |
| `client:<ruc>[,internal:<ruc>]` | exactly the letters named | — | itself, normalised |

Normalised means `w` expands to `uc` and the letters come back in `r,u,c` order,
so `client:w` is stored as `client:uc`.

The persisted form is `eos::common::FormatSchedOps` — **the same grammar a
filesystem's own `sched.ops` carries**, so the two persisted representations of
a mask read alike and a store/restore round trip cannot lose an alias that has
no exact mask. It is deliberately *not* `FormatSchedMask`: that one prefers
preset names, and `ro` on a deny mask reads as the exact opposite of what the
rule does. `DeniedOpsToStr` — which does print the aliases back — is display
only, for `disable ls` and the log lines.

`eos sched disable add default rack3 client` closes a rack to users while drain
and balancing keep working through it.

Two simplifications against geotree, both consequences of the flat design:
- Geotree's five optypes collapse onto the same six-bit mask a filesystem
  carries: every placement kind flows through one `Schedule` descent and every
  read through one `Access` path, so the rule and the disk answer the same
  question and `ValidDisk` asks it once (§3).
- Per-group scope drops: the geo hierarchy repeats below every group, so one rule
  flags its branch under each of them.

`add` ORs into the branch's existing rule and `rm` clears the named bits,
dropping the rule when nothing is left — the opposite of `fs config sched=`,
which replaces the mask outright.

Mechanics: a rule sets `Bucket::denied_ops` (atomic) only on the bucket its
geotag resolves to — the descent passes *down* through it and the access check
walks *up* through it, so the subtree needs no marking.
`ClusterData::ApplyDisabledBranches` clears and re-resolves the whole rule set
(one path for add, rm and the commit-time stamp `AddClusterData` applies from
the manager's rules; a geotag matching no bucket is skipped, not an error).
Placement enforces it as an entry guard in
`PlaceInBucket` (so the spill pass re-routes the shortfall); access enforces it via
`IsAccessCandidate` and `MarkUnavailableReplicas` (disabled replicas reported like
unreachable ones, RAIN semantics follow). Persisted as **one** space config
member, `scheduler.denied`, holding `<geotag>=<spec>` pairs separated by `;` —
one key rather than one per operation, because a rule now denies an arbitrary
set of them. The two keys it replaced (`scheduler.disabled.plct` /
`.access`, comma-separated geotag lists) are still read at boot so a space
configured before the upgrade restores unchanged, and are deleted the first time
a rule of that space is touched.

**State introspection** — `sched show state [space]` prints one block per space
(strategy vs global default, fill limits vs defaults, epoch, topology/status/
weight/space aggregates, disabled branches). `sched ls <space> bucket|disk|all`
dumps the full topology. The data layer produces a plain `ClusterStateSummary`
(`ClusterData::GetStateSummary()`); formatting is entirely in the `FsScheduler`
layer, so a gRPC surface can serialize the same struct.

**Admin transport.** The `sched` command is reachable through the normal proc
path **and** both gRPC interfaces — the WNC interface
(`GrpcWncInterface::Sched`) and the REST gateway (`SchedRequest` →
`GrpcRestGwInterface::SchedCall`). All are thin wrappers that call
`SchedCmd::ProcessRequest()`; no scheduling logic is reimplemented in the
transport layer. The `eos.console.SchedProto` message lives in the shared
`grpc-proto` project (next to `Recycle.proto`) so console and gRPC builds share
one definition.

---

## 10. Integration points

**Startup.** Member `XrdMgmOfs::mFsScheduler` is constructed in `XrdMgmOfs.cc`
(1024 initial buckets + `EosClusterMgrHandler`). The topology is built during
configure (`XrdMgmOfsConfigure.cc` → `FsScheduler::UpdateClusterData`, then
`FsScheduler::LoadConfig` for the persisted per-space configuration):
`EosClusterMgrHandler::MakeClusterMgr` reads
`FsView::gFsView.mSpaceGroupView` under the view lock into a
`vector<FsDescription>` per space, and `BuildClusterData` turns each into a root
bucket + one GROUP bucket per scheduling group + one `Disk` per filesystem
(weight = capacity in TiB, used% from `stat.statfs.filled`, geotag from
`stat.geotag`). It is handed the existing manager map so that a space configured
but not yet populated is carried forward rather than dropped (§9).

**Live updates.** Permission mask via `FsScheduler::SetDiskOps`, status and
weight via `SetDiskStatus`/`SetDiskWeight` (all driven from `FsView.cc`, which
watches the `sched.ops` key on the filesystem's shared hash); fill level and
free space via `SetDiskPercentUsed`/`SetDiskFreeSpace` from the FST publish
listener (§7) — all in-place atomic edits, no rebuild.

**Incremental topology.** Structural changes reach the scheduler inline, next to
the geotree hooks in `FsView::Register` / `UnRegister` / `MoveGroup`:

```
Register   → FsScheduler::InsertFs(space, DescribeFs(fs, group_index))
UnRegister → FsScheduler::RemoveFs(space, fsid)
MoveGroup  → RemoveFs(old space, fsid) + InsertFs(new space, ...)
```

The hook sites hold the `ViewMutex` write lock, so the change is *fed in*, not
re-scanned: `InsertFs` copies the space's snapshot, applies the one change
(shared `AddFsToCluster`, the same per-fs step the bulk build uses), and the
handler commit swaps it in RCU-safely with an epoch bump. It is a **no-op until
`IsRunning()`** so boot registrations are absorbed by the single full build at
the end of configure. Best-effort: a failed add `Abandon()`s the draft, so the
previous snapshot stays live untouched (stale, never degraded) until the next
`sched configure forcerefresh` or rebuild repairs it — the half-built draft is never
published. The manager owns its fill limits and disabled branches and stamps them
onto every snapshot it commits, so an insert that creates the very branch a
stored rule names disables it automatically at commit — no separate post-insert
re-resolve. (The `FsCmd.cc` full rebuilds are kept as a belt-and-braces
path while geotree remains the default.)

**Placement capacity.** `Scheduler::GetPlacementCapacity(space)` answers the
`sched.capacity` column of `space ls` and the coarse ENOSPC guard on every FUSE
file create. It sums `ClusterData::GetWritableFreeGiB()` — one pass over the
disks summing `free_gib` where the disk is online, allows a client create and
is not under a branch denied that operation (the same criteria `ValidDisk`
applies, prebookings already discounted). Because the FUSE guard asks on every
create and this is an O(disks) pass, `ClusterMgr` caches it (value + steady-clock expiry + the epoch it
was computed at, all atomic): a hit is three loads, an epoch bump invalidates
immediately, and the TTL (default 5 s) only bounds staleness against in-place
edits. It is deliberately *not* a maintained running total — that would be a
second source of truth every space-mutating call could desynchronize.

**Draining.** Whether a filesystem is draining is the durable `drain.requested`
key, not a configuration status: `FsView::ReapplyDrainStatus` re-arms from it
after a master failover, and drain completion clears both it and the mask
(`fs config <fsid> drain=on|off` is the operator verb, §9). The *destination* of
a drain transfer is a normal internal-create placement into the same group — the
flat side needs no dedicated drain policy.
`DrainTransferJob::SelectDstFs` delegates to `Scheduler::PlaceDrainReplica`,
which tries the flat path first (setting `bookingsize` to the drained file's
size so the placement books space) and falls back to the geotree engine's
dedicated `draining` policy. The drain job never names an engine.

**Master transition.** `QdbMaster::SlaveToMaster` calls
`mFsScheduler->UpdateClusterData()` next to the geotree `forceRefresh`, so a
failover rebuilds the flat snapshot too (re-reading fs availability and
permission masks; each manager re-stamps its own fill limits and disabled
branches onto the rebuilt snapshot).

---

## 11. Tests

`unit_tests/mgm/placement/`:
- `SchedulerTests.cc` — RR / random / weighted / weighted-RR / exclude /
  forced-group / access / fill-level / flat-view placement / concurrency.
- `ClusterBuilderTests.cc` — hierarchy shape, geotag round-trip, geo-descent
  (scattered / gathered, deficit spill, group boundaries), geo-access
  (`GeoOverlap.*` / `GeoAccess.*`), free space (`FreeSpace.*` / `Prebooking.*`),
  disabled branches, incremental topology, writable capacity.
- `FsSchedulerTests.cc` — strategy resolution, fill limits, disabled branches,
  state summary, insert/remove, placement capacity.
- `ClusterMgrTests.cc`, `SelectionStrategyTests.cc`, `RRSeedTests.cc`,
  `ThreadLocalRRSeedTests.cc`.

The `Scheduler.cc` bridge is covered by
`unit_tests/mgm/scheduler/SchedulerBridgeTests.cc`: both bridge functions have an
overload taking the `placement::FsScheduler` explicitly (the production entry
points are one-line forwarders passing `*gOFS->mFsScheduler`), so the tests drive
the whole argument translation — excludes, already-used, bookingsize, forced
group, target geotag, strategy override, isRW, forcedfsid, RAIN stripe counting,
the `eos.excludefsid` post-filter (`Scheduler::ApplyAccessExclusionFilter`) —
against an injected topology with no `gOFS` involved.

**Microbenchmark:** `test/microbenchmarks/mgm/BM_FlatScheduler.cc`, one case per
strategy over a synthetic group-of-disks topology, plus three that time the
shapes actually seen in production: `BM_NoGeoTagScheduler` (untagged disks, so
every group holds a `<nogeotag>` placeholder and the descent takes the flat-view
shortcut), `BM_GeoScheduler` (2 sites × 2 rooms × 2 racks below every group, a
client in one rack — this is what times the home/away split and the spill pass)
and `BM_NoGeoTagGeoTopology` (that same geo topology entered by a client with no
geotag, i.e. the flat view over a deep hierarchy). Two rules learned the hard
way: **size the bucket count for the placeholder and flat-view buckets too**, and
**fail the benchmark on a failed placement** (`SkipWithError`) — an undersized
topology otherwise reports the timings of errors as if they were placements.

The suite carries a pre-existing ODR violation on a protobuf symbol unrelated to
placement, so it needs `ASAN_OPTIONS=detect_odr_violation=0`. Run the **whole**
binary rather than a `--gtest_filter`: the placement suites are spread over
enough fixture names that a filter quietly misses some of them.

```
ASAN_OPTIONS=detect_odr_violation=0 ./unit_tests/eos-unit-tests
```

---

*Architecture reference for the `mgm/placement/` FlatScheduler. It names classes
and functions rather than line numbers on purpose — keep the names in sync when
the code moves, and prefer explaining a decision over listing what changed.*
