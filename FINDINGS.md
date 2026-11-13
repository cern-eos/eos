# Findings

Pre-existing issues and follow-ups found while working on other tasks, kept out
of those changes. See "Scope Discipline" in `AGENTS.md`; the severity uses the
P0-P5 scale of "Debug And Ticket Priorities" there.

## The MGM logs every refused PIO open of an obfuscated or encrypted file at ERROR

- File: `mgm/ofs/XrdMgmOfsFile.cc:1733-1745` (PIO refusal in
  `XrdMgmOfsFile::open`), `mgm/ofs/XrdMgmOfsFile.cc:4417` (`eos_err` in
  `XrdMgmOfsFile::Emsg`).
- Symptom: every PIO open of an obfuscated or encrypted file, read or write,
  is refused with `ENOTSUP` and logged at ERROR through `Emsg`. The refusal is
  expected: the RAIN plugin always falls back to a gateway access, and the
  access works. A client using the plugin on such files produces one ERROR line
  per file in the MGM log.
- Evidence: `Emsg` calls `eos_err` for every error it returns. VM check of
  2026-10-23 (EOS-6676): `Unable to open - PIO is not supported for obfuscated
  or encrypted files /eos/dev/test/piofb/f; Operation not supported`, followed
  by a successful gateway write.
- Severity: P4 (wrong log: an expected refusal the client recovers from is
  logged at ERROR; no data or metadata damage).
- Blocks the task: no (follow-up of MR !522, EOS-6676).

## The RAIN plugin hides why the MGM refused a PIO open

- File: `fst/xrdcl_plugins/RainFile.cc:248` (`OpenPio`), reported at
  `fst/xrdcl_plugins/RainFile.cc:162` and `:183`.
- Symptom: when the MGM refuses the PIO open request, `OpenPio` returns its own
  `errNotImplemented` status ("error PIO open request") instead of the status
  of the query. The fallback warnings, and the caller, then only show
  `[ERROR] Operation is not implemented`, whatever the reason: obfuscated or
  encrypted file, non-RAIN layout, anonymous client.
- Evidence: plugin log of the VM check of 2026-10-23 (EOS-6676):
  `failed PIO write open, fall back to gateway write`,
  `err="[ERROR] Operation is not implemented"`; the reason was only in the MGM
  log. `RainFile.cc:246` logs the status of the query at ERROR before replacing
  it; whether that status carries the MGM's message was not checked.
- Severity: P4 (wrong error output: the plugin reports the wrong reason; the
  fallback works and nothing persists).
- Blocks the task: no (follow-up of MR !522, EOS-6676).

## Tree accounting events truncate the container id to 32 bits

- File: `namespace/ns_quarkdb/ContainerMD.cc` (`addFile`, `removeFile`,
  `addContainer`, `removeContainer`), read in
  `namespace/ns_quarkdb/accounting/ContainerAccounting.cc` (`fileMDChanged`).
- Symptom: the container id of a tree delta is passed in the `location` field
  of `IFileMDChangeListener::Event`, which is `IFileMD::location_t`
  (`uint32_t`). A container id above 2^32 is truncated, and the delta goes to
  another container or is lost.
- Evidence: code reading only (`typedef uint32_t location_t;` in
  `namespace/interface/IFileMD.hh`). Not reproduced: no instance known to have
  container ids above 2^32.
- Severity: P5 now (no reachable path known); P2 on an instance whose
  container ids pass 2^32 (wrong tree counters that survive a restart).
- Blocks the task: no (found while fixing EOS-6681; the new rmdir correction
  calls `QueueForUpdate` directly and does not use this path).
