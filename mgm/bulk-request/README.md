## Bulk Request Module

This module implements the data structures and business logic for EOS bulk requests (stage, evict, cancel). The focus is on a clear domain model, simple construction via a small factory, and pluggable persistence through DAO factories. Trivial implementations are header-only to reduce file count and build time.

### Layout

```
bulk-request/
  BulkRequest.hh, BulkRequest.cc           # Base model and shared utilities
  File.hh                                   # Header-only representation of a file entry
  FileCollection.hh                         # Header-only container for files

  prepare/
    StageBulkRequest.hh                     # Header-only stage request
    EvictBulkRequest.hh                     # Header-only evict request
    CancellationBulkRequest.hh              # Header-only cancel request
    manager/
      PrepareManager.cc                     # Orchestration utilities
      BulkRequestPrepareManager.cc          # High-level prepare orchestration
    query-prepare/
      QueryPrepareResult.cc                 # Query/inspection helpers

  business/
    BulkRequestBusiness.hh, .cc             # Business operations using DAO factory

  dao/
    IBulkRequestDAO.hh                      # DAO interface
    factories/
      AbstractDAOFactory.hh                 # Abstract persistency factory
      ProcDirectoryDAOFactory.cc            # Proc-dir implementation factory
    proc/
      ProcDirectoryBulkRequestDAO.hh, .cc   # Proc-dir DAO implementation
      ProcDirectoryBulkRequestLocations.cc  # Proc-dir path helpers
      ProcDirBulkRequestFile.cc             # Proc-dir file IO helpers
    proc/cleaner/
      BulkRequestProcCleaner.cc             # Cleanup utilities
      BulkRequestProcCleanerConfig.cc       # Cleaner configuration

  exception/
    BulkRequestException.hh                 # Header-only generic bulk exception
    PersistencyException.hh                 # Header-only persistency exception

  BulkRequestFactory.hh, .cc                # Factory for concrete bulk requests
```

### Key Concepts

- Model
  - `BulkRequest` is the abstract base containing common state and utilities.
  - `StageBulkRequest`, `EvictBulkRequest`, `CancellationBulkRequest` are small, header-only concrete types that only specialize `getType()` and construction.
  - `File` (header-only) captures a path and optional error state.
  - `FileCollection` (header-only) manages the per-request set of files while preserving insertion order and providing views/filtering.

- Business Layer
  - `BulkRequestBusiness` provides read/write operations for bulk requests using an injected `AbstractDAOFactory` to remain storage-agnostic.

- Persistence
  - `IBulkRequestDAO` defines the persistence API.
  - The `proc` implementation uses the filesystem under a proc-style directory. The `ProcDirectoryDAOFactory` wires its concrete DAOs.
  - Clean-up helpers maintain the proc directory structure.

- Factory
  - `BulkRequestFactory` centralizes construction of concrete `BulkRequest` types.

- Exceptions
  - `BulkRequestException` and `PersistencyException` are header-only; use them for module-specific errors.

### Header-only Consolidation

To reduce file count and simplify navigation, trivial implementations were made header-only:
- `File`, `FileCollection`
- `StageBulkRequest`, `EvictBulkRequest`, `CancellationBulkRequest`
- `BulkRequestException`, `PersistencyException`

The removed `.cc` files were pruned from `eos/mgm/CMakeLists.txt`.

### Adding a New Bulk Request Type

1. Create a new header under `prepare/` deriving from `BulkRequest`.
   - Implement a minimal constructor and `getType()` inline in the header.
2. Register the new type in `BulkRequestFactory` for construction.
3. Extend any business or DAO logic if the new type requires different persistence or behavior.
4. Update tests accordingly.

Example minimal header-only request:
```c++
class MyNewBulkRequest : public BulkRequest {
public:
  explicit MyNewBulkRequest(const std::string& id) : BulkRequest(id) {}
  Type getType() const override { return /* new enum value */; }
};
```

### Error Handling Guidelines

- Throw `PersistencyException` for storage/DAO failures.
- Throw `BulkRequestException` for domain errors (invalid inputs, unsupported transitions, etc.).
  - Prefer precise, actionable error messages. Upstream layers should map these to appropriate responses/logging.

### Notes

- Keep trivial logic inline in headers to avoid unnecessary source files.
- Prefer descriptive names and minimize cross-header dependencies.
- When adding persistence backends, implement a new `AbstractDAOFactory` and corresponding DAOs; avoid branching in the business layer.


