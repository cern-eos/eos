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
cache space so FSTs pick them up from the shared hash.

Cache filesystems should be configured for plain single-replica placement
only. Authoritative writes always go to the backend space.

Behaviour
---------

* Read-only opens of files in a space with ``cachespace`` set are redirected
  to an online cache filesystem (preferring an existing ``cache_location``).
* The cache FST serves hits from a per-fid sparse journal under
  ``<cache-fs>/.eoscache/``.
* Misses are fetched from the backend FST contact embedded in the capability
  and optionally written into the journal when admission allows.
* If used capacity is above the high watermark, the FST evicts LRU journals
  down to the low watermark. If admission is still not possible, the open
  runs in bridge-only mode.
* On authoritative write/truncate, the MGM best-effort notifies the cache FST
  to **truncate** the journal for that fid. Stale journals are also discarded
  when size/mtime identity no longer matches.

Metadata
--------

Each file may carry a single ``cache_location`` filesystem id (not counted as
a normal replica). It is shown in ``eos file info`` as ``Cache:`` /
``cachefsid=`` / JSON ``cache_location``.

Limits (v1)
-----------

* One cache location per file globally
* No write-through / write-back
* Balancer/drainer/adjustreplica ignore cache journals
* Backend layouts may be plain, replica, or RAIN; the cache store remains a
  plain sparse journal
* A file needs at least one backend disk replica to open; cache-only files are
  not authoritative
