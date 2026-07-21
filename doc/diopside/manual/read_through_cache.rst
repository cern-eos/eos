.. index::
   single: Read-through cache
   pair: Using; Read-through cache

.. highlight:: rst

.. _read-through-cache:

Read-through cache
==================

EOS can declare one space as a **cache space** for another **backend space**.
Cache filesystems (typically NVMe) store plain sparse journals and serve
read-mostly traffic, fetching missing ranges from the MGM-scheduled backend
layout. When a cache filesystem is full, the cache FST **bridges** reads from
the backend without admitting new journal data.

Configuration
-------------

Configure read-through cache settings on the **backend** space with
``eos space config`` (typically ``default``). No environment variables are
required:

.. code-block:: bash

  eos space config default space.cachespace=cache
  eos space config default space.cache.low_watermark=70
  eos space config default space.cache.high_watermark=85

Defaults for the watermarks are ``70`` / ``85``. The MGM stores these keys on
the backend space and pushes the watermarks to the filesystems of the linked
cache space. The cache FST reads ``cache.low_watermark`` /
``cache.high_watermark`` from its local filesystem configuration first and
falls back to the values embedded in the open capability, so newly added
cache filesystems work before the next explicit configuration push.

Cache filesystems should be configured for plain single-replica placement
only. Authoritative writes always go to the backend space.

Directory policy
++++++++++++++++

A directory can override the space-level binding with
``sys.forced.cachespace`` (same inheritance model as other ``sys.forced.*``
attributes on the parent directory):

.. code-block:: bash

  eos attr set sys.forced.cachespace=cache /eos/path/to/branch
  eos attr set sys.forced.cachespace=none /eos/path/to/branch   # disable
  eos attr rm  sys.forced.cachespace /eos/path/to/branch        # fall back to space

When the attribute is present it wins over ``space.cachespace``:

* a cache space name selects that space for reads under the directory
* ``none`` or an empty value disables read-through caching for that tree
* removing the attribute restores the space-level policy

The named cache space must exist and must not be the same as the file's
backend space.

Behaviour
---------

* Read-only opens of files in a space with ``cachespace`` set are redirected
  to an online cache filesystem chosen by rendezvous-hashing the ``fid`` over
  the *current* online members of the cache space. Adding or removing NVMes
  remaps only about ``1/N`` of files on their next read (the previous journal
  is truncated); ``cache_location`` is rewritten and is not a sticky pin.
* The cache FST serves hits from a per-fid sparse journal under
  ``<cache-fs>/.eoscache/<fid/10000>/<hexfid>`` (same sharding as FST
  replicas). Cache opens do not require a local FMD/replica on the cache
  filesystem path; a transient FMD is built from the capability.
* Misses are fetched from the backend FST contact embedded in the capability
  and optionally written into the journal when admission allows.
* If used capacity is above the high watermark, the FST evicts LRU journals
  down to the low watermark in a background thread, skipping journals still
  referenced by open files. While above the watermark new data is bridged
  instead of admitted; already-cached ranges are still served.
* All concurrent opens of the same file share one journal instance per
  filesystem; the journal index is persisted (data synced first) on close,
  on truncation and periodically, and an index failing validation on load
  resets the journal.
* After an FST restart the cache accounting is rebuilt by scanning the
  ``.eoscache`` directory of the filesystem.
* On authoritative write/truncate, the MGM notifies the cache FST to
  **truncate** the journal for that fid. If the notification fails, the
  ``cache_location`` reference is dropped so reads are no longer steered to
  the stale journal. Stale journals are also discarded when the size/mtime
  identity embedded in the capability no longer matches.

Placement and topology changes
------------------------------

Cache FS selection uses **rendezvous hashing** of the file id over the online
members of the configured cache space:

* The assignment is recomputed on every read open from the *current* online set.
* ``cache_location`` on the file metadata is updated when the assignment
  changes; it is a last-known pointer for ``eos file info`` and truncate
  notify, **not** a sticky pin to the first NVMe that ever served the file.
* When a file remaps (cache FS added/removed/offline), the MGM best-effort
  truncates the journal on the previous cache FS so orphans do not linger
  until LRU eviction.
* Expanding the cache space therefore spreads load onto new NVMes for about
  ``1/N`` of files on their next read, without requiring a bulk rebuild.

Journal layout on the cache filesystem::

  <cache-fs>/.eoscache/<fid/10000>/<hexfid>
  <cache-fs>/.eoscache/<fid/10000>/<hexfid>.idx

Example for ``fxid=00003859`` (decimal 14425)::

  /data/cache/.eoscache/00000001/00003859
  /data/cache/.eoscache/00000001/00003859.idx

Metadata and operations
-----------------------

Each file may carry a single ``cache_location`` filesystem id (not counted as
a normal replica). It is shown in ``eos file info`` as ``Cache:`` /
``cachefsid=`` / JSON ``cache_location``.

Drop the cache pointer and truncate the journal on the cache FST (requires
write permission on the parent directory, same as stripe drop)::

  eos file drop /eos/path/to/file cache
  eos file drop fxid:00003859 cache
  eos file drop fid:14425 cache

Invalidation on mutation is automatic: an authoritative write or truncate
notifies the cache FST (``cachetruncate``). Independently, the open capability
carries size and ``mgm.cache.mtime``; a mismatch against the journal header
truncates the stale journal on the next cache open.

Limits (v1)
-----------

* One cache location per file globally
* No write-through / write-back
* Balancer/drainer/adjustreplica ignore cache journals
* Backend layouts may be plain, replica, or RAIN; the cache store remains a
  plain sparse journal
* A file needs at least one backend disk replica to open; cache-only files are
  not authoritative
