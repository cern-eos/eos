## MGM source layout (management service)

This directory contains the EOS MGM (Management) service sources. The code is organized into focused subdirectories. The most important pieces:

### OFS (XRootD plugin)
- `ofs/`: XRootD OFS plugin implementation (entry points and integration).
  - `ofs/XrdMgmOfs.cc`: main OFS plugin implementation.
  - `ofs/XrdMgmOfs*.hh`: OFS headers (trace, security, file helpers).
  - `ofs/cmds/`: high‑level OFS commands (access, metadata ops, routing, etc.), each command implemented as a separate translation unit included by `XrdMgmOfs.cc`.
  - `ofs/fsctl/`: low‑level fsctl handlers (open, readlink, stat, mkdir, utimes, commit helper, …).
- `authz/`: OFS authorization integration (`XrdMgmAuthz.*`), used by the plugin.

### Core MGM logic
- `access/`, `acl/`: access checks and ACL handling.
- `filesystem/`, `fsview/`: filesystem abstraction, cluster/FS view.
- `policy/`, `quota/`, `scheduler/`: placement, quota and scheduling logic.
- `pathrouting/`, `routeendpoint/`: namespace path routing endpoints.
- `groupbalancer/`, `groupdrainer/`, `balancer/`: balancing and draining logic.
- `recycle/`: recycle bin implementation and policies.
- `namespacestats/`, `inspector/`: namespace statistics and inspection tools.
- `utils/`: internal utilities (registries, helpers, status).
- `misc/`: shared MGM helpers and constants (e.g. `AuditHelpers.hh`, `Constants.hh`, `IdTrackerWithValidity.hh`).
- `xattr/`: XAttr helper headers (`XattrLock.hh`, `XattrSet.hh`).

### Frontends and protocols
- `grpc/`: gRPC interfaces/servers for MGM.
- `http/`: HTTP/REST handlers and models.

### Command processing
- `proc/`: MGM console/protocol command implementations (admin/user/proc_*).
- `commandmap/`: command table and dispatching.

### Other notable areas
- `convert/`: data conversion pipeline elements.
- `egroup/`, `devices/`, `drain/`, `fsck/`: service‑specific subsystems.

### CTA Implementation
- `cta/`: CTA related sources (will appear when the CTA branch is merged)

### Build notes
- Include paths are set in `mgm/CMakeLists.txt` to expose:
  - project root (`${CMAKE_SOURCE_DIR}`),
  - common includes,
  - `mgm/`, `mgm/ofs/` and subfolders.
- OFS sources are compiled from `ofs/`, `ofs/cmds/` and `ofs/fsctl/`.

This structure separates the XRootD plugin surface (ofs) from core MGM logic and service subsystems, making responsibilities clear and keeping build boundaries clean. 

