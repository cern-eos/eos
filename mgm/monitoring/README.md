# Cumulative counter lifetimes

All-tags entries are bounded runtime state. Idle garbage collection removes an
entry; later traffic creates a new lifetime under the same Prometheus labels.
`eos_io_shaping_all_counter_generation` identifies that lifetime without a
changing label or retained tombstone. One gauge covers both operations and both
counter families. Ambiguous normalized identities omit the generation gauge.

`eos_io_shaping_counter_epoch` changes on MGM construction, explicit counter
clears and GC retention changes. It stays stable across ordinary idle expiry and
FST stream resets. A scrape interrupted by a clear discards its counter families
and exposes `eos_io_shaping_counter_snapshot_consistent=0`; consumers should hold
an earlier consistent frame rather than interpret the missing counters as idle.

With complete metadata from the same source scrape, a changed generation in the
same epoch can be measured across an interval shorter than the unchanged GC idle
limit: old-lifetime traffic in that interval would have prevented expiry, so the
new lifetime's full value is the interval delta. Longer intervals, epoch changes
and missing metadata cannot prove this and require a fresh baseline. A generation
change matters even when the new counter exceeds the previous value.

The generation gauge shares the all-tags export cap. No FST protocol extension,
unbounded retired-counter map or generation label is introduced.
