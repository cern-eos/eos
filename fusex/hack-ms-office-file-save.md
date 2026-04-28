# hack-ms-office-file-save

## TL;DR

Microsoft Office (Word, Excel, PowerPoint) does not save files in place. It
performs a "save-by-replace" sequence that ends with the original file being
renamed to a temporary backup and then deleted. On a fusex mount this means
every save creates a brand-new EOS file, the previous content is gone, and
the EOS version directory (`.sys.v#.<file>/`) ends up tracking the wrong
file.

This hack is an **opt-in** rename hook in eosxd that, when an Office save is
detected, snapshots the source file into its version directory **before**
the rename propagates to the MGM, and then puts the version directory back
in place after the rename so the snapshot is not collected together with
the temporary backup.

It is enabled per-mount via `hack-ms-office-file-save` in the eosxd JSON
config and per-directory via the `sys.versioning` extended attribute. It
is mutually exclusive with `tmp-fake-rename`.

## The problem: how MS Office saves a file

As soon as a user *opens* a document `v1.docx` in Office, an owner-lock
sibling is created next to it — `~$v1.docx` (or, for long file names,
the truncated form `~$<name with the first two characters dropped>`) —
to indicate the document is open for editing. The owner-lock lives for the entire duration of the editing
session and is removed only when the document is closed; it is *not*
part of the save sequence.

When the user clicks Save (with the document still open, hence with the
owner-lock present), Office executes the following save-by-replace
sequence on the underlying filesystem:

1. Write the new content to a temporary file (e.g. `aaaa.tmp`).
2. Rename the original document to a temporary backup:
   `v1.docx` → `bbbb.tmp`.
3. Rename the temporary file to the original name:
   `aaaa.tmp` → `v1.docx`.
4. Delete the backup: `rm bbbb.tmp`.

This is not a bug in Office, it is its standard save-by-replace pattern.

### Why this is a problem on EOS

Once the sequence above completes, the EOS file that holds the new
content is a freshly created FMD; from EOS's perspective the original
`v1.docx` has been *deleted* and a *different* file has appeared at the
same path. Two consequences:

- All EOS metadata associated with the original FMD (xattrs, ACLs that are
  not directory-inherited, file id, history, etc.) is lost (this will be worked-around by Reva)
- More importantly, the MGM rename handler renames the file's version
  directory alongside the file itself. So the rename of `v1.docx` onto
  `bbbb.tmp` also moves `.sys.v#.v1.docx/` to `.sys.v#.bbbb.tmp/`, and
  the subsequent deletion of `bbbb.tmp` purges the entire version
  history along with it.

The end result is that on every Office save the user loses access to all
previous versions of the document.

## What the hack does

The hack hooks into eosxd's `rename` operation. When it detects that a
rename matches the backup phase of Office's save sequence (the document
being renamed onto a `*.tmp` backup name while an owner-lock sibling is
present), it does two things:

### Before letting the rename through: snapshot the source

The hack issues a server-side TPC copy of the source file into the EOS
version directory of that file:

```
<parent>/.sys.v#.<basename>/<ctime>.<fxid_hex>
```

This is done via the `eos file copy` proc command.

Note on the target path: EOS supports a generic "atomic upload" mode in
which a client first writes to a hidden temporary path prefixed with
`.sys.a#.` (see `EOS_COMMON_PATH_ATOMIC_FILE_PREFIX` in
`common/Path.hh`). When the FST closes such a file, it sends a commit
to the MGM; the MGM detects the atomic prefix and runs a "de-atomize"
finalization that renames the file from its `.sys.a#.<name>` temporary
to its visible `<name>`, and on the way also performs versioning and
clean-up of any previous file at the target (see
`mgm/ofs/fsctl/Commit.cc` / `CommitHelper.cc`, search for `de-atomize
file`).

Our snapshot target — `<parent>/.sys.v#.<basename>/<ctime>.<fxid_hex>`
— deliberately uses **only** the version-directory prefix `.sys.v#.`
and **never** the atomic prefix `.sys.a#.`. That way the FST commit
takes the regular path and writes the file exactly where we asked for,
without any of the de-atomize rename/versioning side-effects that
would otherwise touch the source FMD's identity.

Once the copy completes, the content of the original `v1.docx` exists
as a durable file inside the version directory, even though `v1.docx`
itself is about to be renamed away.

The hack then calls `eos file purge` to enforce the retention defined by
`sys.versioning` (best-effort: the snapshot itself is already durable, so
a purge failure is logged but not fatal).

### After the rename: restore the version directory name

The MGM rename handler renames the file's version directory alongside
the file itself. So once the `v1.docx` → `bbbb.tmp` rename has completed
on the server, the version directory we just populated has been moved
from `.sys.v#.v1.docx/` to `.sys.v#.bbbb.tmp/`. If left there, the
subsequent `rm bbbb.tmp` would also purge the version directory we just
created.

To prevent that, the hack waits for the rename to be flushed to the MGM
(it forces the rename to be synchronous) and then issues an
`eos file rename` to move `.sys.v#.bbbb.tmp/` back to `.sys.v#.v1.docx/`.
Once Office finishes the sequence and `v1.docx` exists again at its
original path (as a brand-new FMD), the version directory is now
correctly aligned with it and the snapshot is preserved.

### Why this works

After the full Office save, the new `v1.docx` is a different FMD from
the original, but it lives at the same path. The version directory is
indexed by *path* (`<parent>/.sys.v#.<basename>/`) and not by FMD id, so
once the version directory is back in place, the snapshot we wrote
before the rename is reachable as a previous version of the new
`v1.docx` and shows up in `eos file versions`.

## Configuration

### Mount-level (eosxd JSON config)

```json
{
  "options": {
    "hack-ms-office-file-save": true
  }
}
```

The flag defaults to `false`. It is mutually exclusive with
`tmp-fake-rename`: if both are set to `true`, eosxd disables
`tmp-fake-rename` at startup and logs an error.

### Directory-level (xattr)

The hack is a no-op unless `sys.versioning` is set to a positive integer
on the parent directory:

```
eos attr set sys.versioning=5 /eos/.../path/to/office-docs/
```

The value is interpreted as the maximum number of versions to keep
(passed to `eos file purge`). With `sys.versioning` unset or zero, the
hook short-circuits and behaves like a normal rename.

## Code entry points

A developer following this hack should start in
`fusex/eosxd/eosfuse.cc::EosFuse::rename` and read forward:

1. `EosFuse::rename` (`fusex/eosxd/eosfuse.cc`)
   The rename hook lives just before the `Track::Monitor` block that
   issues the actual `mds.mv` call. It is gated by:
   - `Instance().Config().options.hack_ms_office_file_save`
   - `is_office_extension(name)` — source basename ends in `.docx`,
     `.xlsx`, `.pptx`, `.doc`, `.xls` or `.ppt` (case-insensitive).
   - `ends_with_case_ins(newname, ".tmp")` — destination basename ends in
     `.tmp` (case-insensitive).
   - `has_office_owner_lock(p1md, name)` — a sibling named `~$<name>` or
     the truncated form `~$<name[2:]>` (used by Office for long file
     names) exists in the parent's `local_children`.
   When all four hold and `sys.versioning > 0` on the parent, the hook
   calls `backend::versionFile` before the rename and
   `backend::renameVersionDir` after it.

2. `backend::versionFile` (`fusex/backend/backend.cc`)
   Drives the three proc commands that snapshot the source:
   - `mgm.cmd=mkdir mgm.option=p` to create the version directory.
   - `mgm.cmd=file mgm.subcmd=copy` server-side TPC copy of the source
     into `<version_dir>/<ctime>.<fxid_hex>`.
   - `mgm.cmd=file mgm.subcmd=purge` to enforce `sys.versioning`
     retention.

3. `backend::renameVersionDir` (`fusex/backend/backend.cc`)
   Issues an `mgm.cmd=file mgm.subcmd=rename` to move
   `<parent>/.sys.v#.<from>/` to `<parent>/.sys.v#.<to>/`.

4. `backend::procCommand` (`fusex/backend/backend.cc`)
   Helper used by both `versionFile` and `renameVersionDir`. Issues a
   `/proc/user/` request synchronously with the user's identity and the
   standard fusex envelope (`mgm.uuid`, `mgm.retc`, `mgm.cid`, `eos.app`,
   `fuse.v`).

5. Helpers in the anonymous namespace at the top of
   `fusex/eosxd/eosfuse.cc`:
   - `ends_with_case_ins`, `is_office_extension`
   - `has_office_owner_lock` — note that this checks the parent's
     `local_children` map directly rather than using `mds.lookup`,
     because a freshly-created sibling's md may have `id() == 0` until
     the server response arrives, which is racy enough to miss the case
     we care about (Office having just created its owner-lock).

## Debugging

Every operation performed by the hack emits an `eos_static_debug` log
prefixed with `hack-ms-office-file-save:`. Enabling debug-level logs in
eosxd (or filtering on that prefix) is sufficient to follow the full
flow:

- `evaluating rename`: predicates evaluated for every rename — useful to
  understand why the hook does or does not fire.
- `snapshotting source before rename`: the source path, parent
  `sys.versioning`, source ctime and fxid that will be used to build the
  version filename.
- `snapshot taken`: the snapshot proc commands all returned successfully.
- `skipping snapshot - sys.versioning unset or equal to 0 on parent`:
  the hook's gating condition matched but the parent has no versioning.
- `moving version dir back from .sys.v#.<newname>/ to .sys.v#.<name>/`:
  the post-rename undo about to happen.
- Inside `versionFile` / `renameVersionDir`: per-operation entry and
  `... ok` traces, plus `eos_static_err` lines on every failure path.

## Tests

`test/fusex/eos-msoffice-save-test` simulates Office's save sequence with
shell commands and verifies that:

- with the flag on and `sys.versioning > 0`, a snapshot appears under
  `.sys.v#.v1.docx/` and `eos file versions` lists it;
- with the flag on but `sys.versioning` unset on the parent, the hook is
  a no-op;
- on a separate mount with the flag off (optional argument), the hook is
  also a no-op, even when `sys.versioning` is set on the parent.

Run it as:

```
eos-msoffice-save-test <mount-with-hack-on> <eos-prefix> [<mount-with-hack-off>]
```
