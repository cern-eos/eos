## EOS Audit Logging

### Overview

EOS implements structured audit logging for successful operations that modify the namespace or file metadata. Audit entries are encoded as JSON (one record per line), written directly into ZSTD-compressed log segments, and rotated every 5 minutes. A symlink `audit.zstd` always points to the current active segment.

This document explains what is logged, the record format, where files are written, rotation behavior, how to parse the logs, and where audit hooks are integrated in the codebase.

### Scope: What gets logged

- **Successful namespace-affecting operations by identified users**:
  - **Files**: CREATE, DELETE, RENAME/MOVE, TRUNCATE, WRITE (commit), UPDATE (open for write without create/truncate)
  - **Directories**: MKDIR, RMDIR, RENAME/MOVE
  - **Symlinks**: SYMLINK creation, DELETE
  - **Metadata**: CHMOD, CHOWN, SET_XATTR, RM_XATTR, SET_ACL
- **Optional**: READ and LIST can be enabled later (not default; high volume).
- **Excluded**: Failed attempts, internal non-human activities (e.g. purge/version housekeeping).

### Record format (protobuf → JSON)

Each audit line is a JSON serialization of the `eos.audit.AuditRecord` protobuf (`proto/Audit.proto`). Key elements:

- **Common fields**
  - `timestamp` (int64): seconds since epoch (server time)
  - `path` (string): absolute path to object; directory paths end with '/'
  - `operation` (enum): one of CREATE, DELETE, RENAME, WRITE, TRUNCATE, SET_XATTR, RM_XATTR, SET_ACL, CHMOD, CHOWN, MKDIR, RMDIR, SYMLINK, UPDATE
  - `client_ip` (string), `account` (string)
  - `auth` (mechanism string + attributes map)
  - `authorization` (reasons[])
  - `trace_id` (string): server trace id
  - `target` (string): for rename/symlink target path
  - `uuid` (string): client/session id (empty if placeholder `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
  - `tid` (string): client trace identifier
  - `app` (string): client application
  - `svc` (string): emitting service (e.g. "mgm")

- **State snapshots**
  - `before` / `after` (Stat): include `ctime`, `mtime`, `uid`, `gid`, `mode` (uint32), `mode_octal` (string), `size` (uint64), `checksum` (hex string for files)
  - `attrs` (repeated AttrChange): `{ name, before, after }` for xattr changes (non-system attributes)

- **Nanosecond resolution times**
  - `Stat.ctime_ns` and `Stat.mtime_ns` provide full-resolution strings in the form `seconds.nanoseconds` (e.g. `1730985600.123456789`).

- **Source and version metadata**
  - `src_file`, `src_line`: source file and line where the audit call originated
  - `version`: software version used when emitting the record

Example JSON line (pretty-printed for readability):

```json
{
  "timestamp": 1730985600,
  "path": "/eos/user/a/alice/data/file.txt",
  "operation": "WRITE",
  "client_ip": "192.0.2.10",
  "account": "alice",
  "auth": { "mechanism": "krb5", "attributes": {"principal": "alice@EXAMPLE.ORG"} },
  "authorization": { "reasons": ["uid-match"] },
  "trace_id": "srv-abc123",
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "tid": "cli-xyz789",
  "app": "eoscp",
  "svc": "mgm",
  "before": { "ctime": 1730980000, "mtime": 1730981000, "uid": 1000, "gid": 1000, "mode": 420, "mode_octal": "0100644", "size": 1024, "checksum": "a1b2..." },
  "after":  { "ctime": 1730980000, "mtime": 1730985600, "ctime_ns": "1730980000.000000000", "mtime_ns": "1730985600.123456789", "uid": 1000, "gid": 1000, "mode": 420, "mode_octal": "0100644", "size": 4096, "checksum": "dead..." },
  "src_file": "mgm/FuseServer/Server.cc",
  "src_line": 2600,
  "version": "<eos-version>"
}
```

### Log files, rotation, and location

- **Location**: `<logdir>/audit/` where `logdir` is derived from `XRDLOGDIR` (see `mgm/XrdMgmOfsConfigure.cc`).
  - Directory is created on startup if missing; mode 0755; owned appropriately by the service user.
- **Active segment symlink**: `<logdir>/audit/audit.zstd` points to the current segment file.
- **Segments**: Files are ZSTD-compressed; rotated every 5 minutes.
  - Filenames include seconds for uniqueness: `audit-YYYYMMDD-HHMMSS.zst`
  - On rotation, the symlink is atomically updated to the new segment.

### ZSTD stream and flushing

- On opening a new segment, the ZSTD frame header is flushed immediately to avoid `zstdcat` errors on empty files.
- Each record is written and flushed so small bursts are visible promptly.

### Implementation details

- `common/Audit.hh`, `common/Audit.cc` implement the audit writer:
  - Thread-safe writer with internal locking
  - Base directory configurable via `setBaseDirectory` or during construction
  - `audit(const AuditRecord&)` and a convenience overload to populate from `VirtualIdentity`, operation, path, etc.
  - Automatic rotation based on time; symlink management (`audit.zstd`)
  - Normalizes placeholder UUID to empty string

### Integration points (where audits are emitted)

- Core MGM (`mgm/`):
  - `XrdMgmOfs.hh`: `std::unique_ptr<eos::common::Audit> mAudit` member
  - `XrdMgmOfsConfigure.cc`: initializes `mAudit` with `<logdir>/audit/`
  - Operations:
    - Files: `XrdMgmOfsFile.cc::open` (CREATE, TRUNCATE, UPDATE), `fsctl/Commit.cc` (WRITE)
    - Directories: `Mkdir.cc` (MKDIR), `Remdir.cc` (RMDIR)
    - Metadata: `Chmod.cc` (CHMOD), `Chown.cc` (CHOWN), `Attr.cc` (SET_XATTR, RM_XATTR)
    - Symlinks: `Link.cc` (SYMLINK)
    - Delete: `Rm.cc` (DELETE)

- FUSE server (`mgm/FuseServer/Server.cc`):
  - Directories: `OpSetDirectory` (MKDIR, UPDATE/RENAME/MOVE; xattr changes), `OpDeleteDirectory` (RMDIR)
  - Files: `OpSetFile` (CREATE, UPDATE, RENAME/MOVE; CHMOD/CHOWN detection; xattr changes), `OpDeleteFile` (DELETE)
  - Symlinks: `OpSetLink` (SYMLINK), `OpDeleteLink` (DELETE)

### Directory path convention

- Directory paths in audit entries include a trailing slash `/` for unambiguous parsing.

### Mode representation

- `mode` is stored as an integer (uint32) and `mode_octal` as a string in octal for convenience.

### Parsing and tooling

- Stream current audit records:

```bash
zstdcat <logdir>/audit/audit.zstd | jq '.'
```

- Follow audit logs across rotations (like `tail -F`):

```bash
zstdtail <logdir>/audit/audit.zstd
# Or with filtering:
zstdtail <logdir>/audit/audit.zstd -- jq 'select(.operation == "DELETE")'
```

- Historical segments are named `audit-YYYYMMDD-HHMMSS.zst`. Each line is a standalone JSON record; consumers can ingest line-by-line.

### Testing and performance

- Unit tests: `unit_tests/common/AuditTests.cc`
  - Rotation and symlink behavior
  - Benchmark: writes 100,000 records and measures elapsed time

### Notes and caveats

- Only successful operations are logged.
- READ/LIST are intentionally omitted by default due to volume; can be added later.
- The audit writer flushes after each record for operational visibility; adjust if batching is later desired.


