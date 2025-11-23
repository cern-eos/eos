EOS Logging Guide
=================

This document explains how the EOS logging facility works and how to use and configure it in your code. The logging API lives in `common/Logging.hh` and is implemented in `common/Logging.cc`.


Overview
--------

- A global singleton `eos::common::Logging` manages logging.
- You can log from:
  - Classes that inherit `eos::common::LogId` using `eos_*` macros.
  - Static/free functions using `eos_static_*` macros.
  - Thread functions using `eos_thread_*` macros.
- Messages are formatted and written to:
  - `stderr` (always),
  - optionally to syslog,
  - optionally to per-file fan-out FILE* streams.
- Log levels are syslog-compatible: DEBUG, INFO, NOTICE, WARNING, ERR, CRIT, ALERT, EMERG, plus a special SILENT channel.
- A circular in-memory buffer is also maintained per level for recent messages.


Basic usage in classes
----------------------

If you have a class, inherit from `eos::common::LogId`. This gives you:
- `char logId[40]`, `char cident[256]` and `VirtualIdentity vid` members,
- helpers to set them (e.g. `SetLogId`).

Then use the macros which automatically include function, file and line:

```c++
class MyService : public eos::common::LogId {
public:
  void run() {
    SetLogId("my-service-logid", "my-svc");  // optional, sets logId and client identifier
    eos_info("starting service port=%d", 1094);
    eos_debug("internal state x=%d", 42);
    eos_warning("slow operation took %d ms", 512);
    eos_err("operation failed code=%d", rc);
  }
};
```

Macros available for classes inheriting `LogId`:
- `eos_debug(...)`, `eos_info(...)`, `eos_notice(...)`, `eos_warning(...)`,
  `eos_err(...)`, `eos_crit(...)`, `eos_alert(...)`, `eos_emerg(...)`,
  `eos_log(priority, ...)` and `eos_silent(...)`.

Notes:
- Messages are emitted only if their level is enabled in the current log mask.
- These macros use your object’s `logId`, `cident` and `vid`.


Logging from static/free functions
----------------------------------

Use the `eos_static_*` macros. These don’t require a `LogId` instance:

```c++
void helper() {
  eos_static_info("helper started");
  if (error) eos_static_err("failed: %s", why.c_str());
}
```

Available macros:
`eos_static_debug`, `eos_static_info`, `eos_static_notice`, `eos_static_warning`,
`eos_static_err`, `eos_static_crit`, `eos_static_alert`, `eos_static_emerg`,
`eos_static_log`, `eos_static_silent`.


Logging from thread functions
-----------------------------

Use the `eos_thread_*` macros. They expect a thread-local `LogId` named
`tlLogId` and a `VirtualIdentity vid` in scope. Example:

```c++
void threadMain() {
  eos::common::LogId tlLogId;
  tlLogId.SetLogId(eos::common::LogId::GenerateLogId().c_str(), "worker");
  eos::common::VirtualIdentity vid; // fill if available
  eos_thread_info("worker up");
}
```

Available macros:
`eos_thread_debug`, `eos_thread_info`, `eos_thread_notice`, `eos_thread_warning`,
`eos_thread_err`, `eos_thread_crit`, `eos_thread_alert`, `eos_thread_emerg`.


Enabling and filtering logs
---------------------------

Set the global log level (mask) to include all levels up to a priority:

```c++
eos::common::Logging::GetInstance().SetLogPriority(LOG_INFO); // enables INFO and more severe
```

Check whether a given level would log (useful for expensive formatting):

```c++
if (EOS_LOGS_DEBUG) { /* build detailed diagnostics and eos_debug(...) */ }
```

Function-level filtering is supported via allow/deny lists:

```c++
// Deny list (comma-separated): suppress logs from these function names at INFO+
eos::common::Logging::GetInstance().SetFilter("noisyFunc,spammyFunc");

// Pass-through (accept) list: only these functions will log at INFO+.
eos::common::Logging::GetInstance().SetFilter("PASS:importantFunc,criticalFunc");
```

Notes:
- Filtering applies to levels INFO and above (to reduce flood).
- Use raw function identifiers (`__FUNCTION__` names).


Syslog duplication
------------------

Duplicate messages to syslog:

- At runtime:
  ```c++
  eos::common::Logging::GetInstance().SetSysLog(true);
  ```
- Or via environment variable at process start:
  ```
  EOS_LOG_SYSLOG=1    # or "true"
  ```

ZSTD-compressed rotating logs (replacement mode)
------------------------------------------------

Logging can optionally replace stdout/fan-out outputs with ZSTD-compressed, time-rotated files (similar to the audit logger).

- Enable via environment:
  ```
  EOS_ZSTD_LOGGING=1            # enable compressed logging (replaces fan-out files)
  EOS_ZSTD_LOGGING_ROTATION=3600  # rotate every N seconds (default: 3600 = 1 hour)
  EOS_ZSTD_LOGGING_LEVEL=1        # optional compression level (1..19), default 1
  ```
- Location:
  - Base directory: `$XRDLOGDIR` if set; otherwise `/var/log/eos/<service>`.
  - Real segments are stored under `<base>/logs/`.
  - Top-level symlinks remain in `<base>/` and are relative:
    - Main: `<base>/xrdlog.<service>.zstd -> logs/xrdlog.<service>-YYYYmmdd-HHMMSS.zst`
    - Per-tag: `<base>/<tag>.zstd -> logs/<tag>-YYYYmmdd-HHMMSS.zst`
- Behavior:
  - A ZSTD frame header is flushed immediately when a new segment opens to avoid “unexpected end of file” for empty segments.
  - Each message is flushed to make the stream tail-able (e.g., with `zstdcat` or a `zstdtail` utility).
  - Rotation creates fresh segments under `logs/`; per-stream top-level symlinks are atomically updated to relative targets.
  - When enabled, stdout printing and fan-out FILE* writes are suppressed (compressed streams are authoritative). Stderr is redirected and its lines are written to the main compressed stream.
  - On startup in ZSTD mode, if a plain `xrdlog.<service>` exists with content (e.g., created by XRootD early), its contents are migrated into the compressed main stream and the plain file is unlinked.

Per-tag file names in ZSTD mode
-------------------------------

- Only canonical fan-out tags produce per-tag `.zst` streams. The allowed set matches MGM’s fan-out list:
  `Grpc, Balancer, Converter, DrainJob, ZMQ, MetadataFlusher, Http, Master, Recycle, LRU, WFE, Wnc, WFE::Job, GroupBalancer, GroupDrainer, GeoBalancer, GeoTreeEngine, ReplicationTracker, FileInspector, Mounts, OAuth, TokenCmd`
- Source module names are mapped to this set using MGM’s aliasing (e.g., `HttpHandler` → `Http`, `Drainer` → `DrainJob`). Non-matching module names are skipped to avoid proliferation of files.


Fan-out to additional files
---------------------------

You can route log lines to additional FILE* streams based on source file tags:

```c++
// Send all messages to a file
FILE* all = fopen("/var/log/eos/all.log", "a");
eos::common::Logging::GetInstance().AddFanOut("*", all);

// Send messages that don’t match any other tag (besides “*”) to a file
FILE* other = fopen("/var/log/eos/other.log", "a");
eos::common::Logging::GetInstance().AddFanOut("#", other);

// Send messages originating from Foo.cc to a dedicated file (tag is the basename without .cc)
FILE* foo = fopen("/var/log/eos/foo.log", "a");
eos::common::Logging::GetInstance().AddFanOut("Foo", foo);
```

Rules:
- Tag is the source filename basename without extension (e.g. `Foo` for `Foo.cc`).
- `*` routes all messages; `#` routes messages not claimed by any other tag.
- Fan-out writes are flushed after each message.


Rate limiting
-------------

To suppress repetitive bursts (same file/line/priority), enable the rate limiter:

```c++
eos::common::Logging::GetInstance().EnableRateLimiter();
```

When enabled, repeated messages at levels below WARNING (i.e. INFO/NOTICE/DEBUG)
from the same source location within ~5 seconds may be suppressed with a single
“suppressed” indicator line.


Circular in-memory buffer
-------------------------

For each priority level, a circular buffer of recent messages is kept internally
(default size: `EOSCOMMONLOGGING_CIRCULARINDEXSIZE`, 10,000). This is used
internally for diagnostics and can be resized:

```c++
eos::common::Logging::GetInstance().SetIndexSize(20000);
```


Short format
------------

Logging supports a compact “short” format (internal switch) that prints abbreviated
lines (time, func, level, thread id, and source). The default is the verbose format,
which also includes identifiers (logid, unit, client identity).


Special channel: SILENT
-----------------------

`LOG_SILENT` can be used with `eos_silent(...)` or `eos_static_silent(...)` to always
format and enqueue a message without level checks. Internally it maps to DEBUG for
the circular buffer, but is not filtered by the log mask.


Initialization and lifecycle
----------------------------

- The logging singleton is constructed via a static initializer (`sLoggingInit`)
  in each translation unit that includes `Logging.hh`. You do not need to create it manually.
- A background thread prints log buffers to `stderr` and fan-out streams.
- Call `eos::common::Logging::GetInstance().shutDown(/*gracefully=*/true)` during
  controlled shutdown if you need a graceful drain of the queue (optional).


Best practices
--------------

- Set the unit name once at process start (used in the verbose format):
  ```c++
  eos::common::Logging::GetInstance().SetUnit("mgm");
  ```
- For services, derive from `LogId` to benefit from structured fields (`logId`, `cident`, `vid`).
- Use `EOS_LOGS_DEBUG` checks for heavy debug-only formatting.
- Prefer function-level filtering (deny or pass lists) to reduce noise.
- Use fan-out carefully; keep file descriptors open for the process lifetime.


Environment variables
---------------------

- `EOS_LOG_SYSLOG=1|true` — duplicate log messages to syslog in addition to `stderr`.
- `EOS_MGM_LOG_BUFFERS=<N>` — cap the maximum number of internal log buffers (default 2048).


API reference (selected)
------------------------

- `Logging::SetLogPriority(int pri)` — set active log levels (e.g. `LOG_INFO`).
- `Logging::SetFilter(const char* filter)` — set accept or deny filters.
- `Logging::SetUnit(const char* unit)` — set unit (service) tag.
- `Logging::SetSysLog(bool onoff)` — enable/disable syslog duplication.
- `Logging::EnableRateLimiter()` — enable rate limiting of repetitive messages.
- `Logging::AddFanOut(const char* tag, FILE* fd)` — add a fan-out sink (`*`, `#`, or source file tag).
- `Logging::AddFanOutAlias(const char* alias, const char* tag)` — alias one tag to another.
- `Logging::SetIndexSize(size_t size)` — size of per-level circular memory.


Examples
--------

Minimal service setup:

```c++
int main() {
  using eos::common::Logging;
  auto& log = Logging::GetInstance();
  log.SetUnit("mgm");
  log.SetLogPriority(LOG_INFO);
  log.SetSysLog(true);
  log.EnableRateLimiter();
  eos_static_info("service starting");
  // ...
}
```

Route `Server.cc` logs to a separate file:

```c++
FILE* fx = fopen("/var/log/eos/fxserver.log", "a");
eos::common::Logging::GetInstance().AddFanOut("Server", fx); // from FuseServer/Server.cc
```



