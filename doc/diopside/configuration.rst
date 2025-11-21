
ZSTD-compressed rotating logging
--------------------------------

EOS supports an optional ZSTD-compressed, time-rotated logging mode that replaces the default stderr/fan-out file writes with compact `.zst` streams.

- Enable via environment:

  - ``EOS_ZSTD_LOGGING=1``: enable compressed logging
  - ``EOS_ZSTD_ROTATION=<seconds>``: rotation interval (default ``3600``)
  - ``EOS_ZSTD_LEVEL=<1..19>``: optional compression level (default ``1``)

- Layout:

  - Base directory is taken from ``$XRDLOGDIR`` if set, otherwise ``/var/log/eos/<service>``.
  - Real compressed segments live under ``<base>/logs/``.
  - Top-level symlinks remain in ``<base>/`` and are relative to ``logs/``:

    - Main: ``xrdlog.<service>.zstd -> logs/xrdlog.<service>-YYYYmmdd-HHMMSS.zst``
    - Per-tag: ``<tag>.zstd -> logs/<tag>-YYYYmmdd-HHMMSS.zst``

- Behavior:

  - The logging system flushes a ZSTD frame header on segment open to keep new segments readable.
  - Each record is flushed to allow ``zstdcat``/``zstdtail``-style tools to follow files in near real time.
  - In ZSTD mode, legacy fan-out FILE* writes are suppressed and stderr is redirected so that its lines are appended to the main compressed stream.
  - On startup in ZSTD mode, if a plain ``xrdlog.<service>`` exists with content (e.g. created by XRootD before EOS logging starts), its contents are migrated into the compressed main stream and the plain file is unlinked.

- Per-tag naming:

  - Per-tag compressed streams are only created for the canonical fan-out tag set used by MGM (e.g. ``Grpc``, ``Http``, ``DrainJob``, ``ZMQ`` …). Source-module names are mapped into this set using the same alias rules as the fan-out configuration (e.g. ``HttpHandler`` → ``Http``, ``Drainer`` → ``DrainJob``). Modules without a canonical tag do not get their own `.zst` stream.

