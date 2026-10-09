# EOS RAIN XrdCl client plugin

`libEosRainClient.so` is an XRootD client (XrdCl) file plugin that accesses
EOS RAIN files (RAID-DP and Reed-Solomon layouts) in **parallel IO (PIO)
mode**. The client talks to every FST that holds a stripe and runs the RAIN
codec itself, so no gateway FST sits in the data path.

- **Read:** the plugin reads the data stripes directly and rebuilds missing or
  corrupted blocks from the parity stripes.
- **Write:** the plugin splits the file into stripes, computes the parity,
  writes all the stripes and commits the file to the MGM.

If the PIO open fails, for reads or writes, the plugin falls back to the
normal XrdCl implementation, and the MGM redirects the client to a gateway FST
as usual. See [Fallback summary](#fallback-summary).

The plugin reuses the FST RAIN layout implementation
(`fst/layout/RainMetaLayout`, `RaidDpLayout`, `ReedSLayout`), the same code a
gateway FST runs in `XrdFstOfsFile`.

## Files

| File | Content |
|------|---------|
| `RainPlugin.cc/hh` | Plugin factory (`XrdClGetPlugIn`), logging set-up |
| `RainFile.cc/hh` | `XrdCl::FilePlugIn` implementation |

## Enabling the plugin

The plugin is built by default and shipped in the `eos-server` package. Enable
it with the standard XrdCl plugin mechanism, for example
`/etc/xrootd/client.plugins.d/eos-rain.conf`:

```
url = root://eos-mgm.example.org:*
lib = /usr/lib64/libEosRainClient.so
enable = true
```

Alternatively, set `XRD_PLUGIN=/usr/lib64/libEosRainClient.so` for a single
process.

When loaded, the plugin sends its log to `/tmp/rain/xrdcp_rain.log` and
redirects the process `stdout`/`stderr` to that file. The log level follows
`XRD_LOGLEVEL`, either as a number `0-7` or as a name such as `debug`; the
default is `info`.

## PIO open request

Both reads and writes start with the same kind of request: an opaque file
query to the MGM.

```
<path>?mgm.pcmd=open[&<write options>]
```

The MGM handles it in `XrdMgmOfs::OpenPio` (`mgm/ofs/fsctl/OpenPio.cc`). That call
runs a normal `XrdMgmOfsFile::open` with `eos.cli.access=pio` and returns the
redirection info as the query response instead of redirecting the client. The
response looks like this:

```
pio.0=fst1:1095&pio.1=fst2:1095&...&mgm.lid=<layout id>&mgm.logid=<logid>&cap.sym=...&cap.msg=...
```

- `pio.<i>` is the `host:port` of the FST holding stripe `i`.
- `mgm.lid` is the RAIN layout of the file. The client uses it to set up the
  codec: stripe count, parity count and block size.
- Everything from `mgm.logid` onwards is the signed capability. The plugin
  appends `eos.app=rainplugin` and passes the result as opaque info to every
  stripe open, together with `mgm.replicaindex=<i>`, `fst.readahead=true` and
  `fst.blocksize=<stripe width>`.

Each stripe is opened at `root://<pio.i>//<path>`.

## Read

1. **Open.** The file is opened with `OpenFlags::Read` and no write flag. The
   plugin sends the PIO open request described above. For reads the MGM gives
   every stripe a *plain* layout without checksum. Each FST then serves its
   stripe file as-is, header included, and knows nothing about RAIN.

   The client creates a `RaidDpLayout` or `ReedSLayout` and calls `OpenPio()`
   in read-only mode. That call:
   - opens all stripes asynchronously and reads the RAIN header of each one;
   - validates the headers (`ValidateHeader()`) and rebuilds the mapping
     between logical and physical stripes;
   - takes the logical file size from a valid header.

2. **Read / VectorRead.** Requests are split per stripe block and sent to the
   data stripes. If a block cannot be read, or fails its check, the layout
   rebuilds it from the parity stripes. This works as long as no more than
   `<number of parity stripes>` stripes are unavailable.

3. **Stat.** The size reported is the logical file size.

4. **Close.** All stripes are closed.

Unsupported on a PIO read handle: `Write` and `Truncate` (these return
`errNotSupported`), `Fcntl`, `Visa`, `SetProperty` and `GetProperty`, except
`GetProperty("LastURL")` which returns the URL given at open. Checksum queries
such as the one of `xrdcp --cksum` therefore go to the MGM. This also applies
to a PIO write handle.

**Failure:** if the PIO read open fails (the file is not RAIN, the MGM query
fails, headers are invalid, too many stripes are unavailable, ...), the plugin
falls back to a normal XrdCl open, and the MGM redirects the client to a
gateway FST.

## Write

A PIO write is attempted for any open that creates or overwrites a file, that
is with `OpenFlags::New` or `OpenFlags::Delete`. An update-only open (`Update`
or `Write` without `New`/`Delete`) goes straight to the normal gateway write.

### Requirements

- **Authenticated client.** Any authentication method works (krb5, gsi,
  tokens, sss, ...). The mapped identity is used for the permission and quota
  checks as for any other write. Anonymous (nobody) identities are refused.
  The client commits the file itself; the MGM accepts its commit and drop
  requests when they carry the **PIO write capability** issued at open (see
  [Authorization](#authorization-of-commit-and-drop)).
- **RAIN layout.** The target directory must resolve to a RAIN layout. The MGM
  checks this, with a dry run of the layout policy, *before* it changes the
  namespace.

If either condition is not met, the PIO open fails and the plugin falls back
to a normal gateway write. No namespace entry is left behind.

### Open

The plugin extends the PIO open request with the open flags and mode. These
are the same tags that `mgm.pcmd=redirect` accepts, parsed by
`XrdMgmOfsFile::GetClientOpenFlags`:

| Opaque key | Meaning |
|------------|---------|
| `eos.client.openflags=rw,cr` | Exclusive creation (`OpenFlags::New`) |
| `eos.client.openflags=rw,tr` | Create or overwrite (`OpenFlags::Delete`) |
| `eos.client.openmode=<octal>` | Creation mode, taken from the XrdCl `Access::Mode` (default `0644`) |
| `eos.client.mkpath=1` | Create the parent directories (`OpenFlags::MakePath`) |

A write request without `cr` or `tr` (update of an existing file) is refused
by the MGM with `ENOTSUP`.

The MGM creates or truncates the file and schedules the stripes as for a
gateway write. The response has the same format as for a read. The file id,
path and stripe file systems needed for the commit are only in the encrypted
capability; the client sends it back and the MGM takes them from there.

For a write, two things differ from a read:

- The stripes keep the **RAIN layout** in the capability, not a plain one.
- The capability carries the signed flag **`mgm.pio.write=1`**.

On the FST side (`XrdFstOfsFile`, `RainMetaLayout`), a stripe opened with
`mgm.pio.write=1`:

- is never an entry server, so it only handles its local stripe exactly like a
  non-entry stripe of a gateway write. This includes the stripe checksum and
  the local FMD with the RAIN layout id and the logical size;
- **never contacts the MGM**: it sends no commit, and no drop on failure. On a
  client disconnect or a `delete` fcntl it still removes its local stripe
  file.

The client then:

1. creates the RAIN layout and marks the stripes as RAIN-aware
   (`SetPioWrRainStripes(true)`), so truncate offsets are sent as logical
   offsets;
2. opens all stripes with `SFS_O_CREAT | SFS_O_RDWR | SFS_O_TRUNC`. If a
   single stripe fails to open, the whole open fails;
3. starts the parity computation thread, the same one a gateway entry server
   uses;
4. creates the file checksum object for the checksum type of the layout.

### Write / Truncate

`Write` goes through `RainMetaLayout::Write()`:

- data is split into stripe blocks and written asynchronously to the data
  stripes;
- as soon as a group of blocks is complete, its parity is computed and written
  to the parity stripes;
- non-sequential writes switch the layout to the sparse (non-streaming) parity
  computation.

The client also feeds every written buffer to the file checksum and keeps
track of the logical file size.

`Truncate` truncates all stripes and marks the checksum dirty if the checksum
no longer covers the whole file.

### Close and commit

The sequence mirrors a gateway close (`XrdFstOfsFile::_close_wr`):

1. **Checksum.** The checksum is finalized. If it does not cover the whole
   file (non-sequential writes, or a truncate that extended the file), it is
   recomputed by reading the file back through the layout.
2. **Commit to the MGM**, a single request for the whole file:

   ```
   mgm.pcmd=commit&mgm.pio.commit=1&mgm.size=<logical size>&mgm.checksum=<xs>&mgm.mtime=...&mgm.mtime_ns=...&mgm.logid=...[&mgm.modified=1]&cap.*
   ```

   `mgm.checksum` is sent only if the layout has a checksum. The MGM takes the
   file id, the path and all the stripe file systems (`mgm.fsid<i>`) from the
   capability. It commits the size and checksum and registers all the stripe
   locations in one namespace update.
3. **Layout close.** The layout writes the remaining parity, updates and
   writes the stripe headers, truncates the stripes to their final size and
   closes them.

Commits go to the MGM endpoint of the original URL, so XrdCl follows any
redirect to the current master.

### Authorization of commit and drop

The PIO write response contains the capability signed by the MGM (`cap.sym`,
`cap.msg`, `cap.key`, `cap.format`). The client cannot read or modify it. Its
payload includes `mgm.pio.write=1`, `mgm.pio.uid=<client uid>` (the uid taken
before any sticky owner mapping), `mgm.fid`, `mgm.path`, `mgm.lid` and the
scheduled stripes `mgm.fsid<i>`.

The plugin sends all the `cap.*` fields back with the commit
(`mgm.pio.commit=1`) and the drop (`mgm.pio.drop=1`).
`CommitHelper::extract_pio_capability` decodes the capability and accepts the
request, from any client, trusted or not, only if:

- the capability is authentic. An expired capability is accepted as long as
  the upload is not older than `EOS_MGM_PIO_MAX_UPLOAD_AGE` seconds (MGM
  environment, default 86400), since uploads can outlast the capability
  validity;
- it is a PIO write capability for a RAIN layout;
- it was issued to the calling uid;
- it holds the file id, the path and a file system for every stripe;
- the request uses only PIO parameters: `mgm.drop.fsid`,
  `mgm.reconstruction`, `mgm.fusex`, `mgm.commit.verify`, the alternative
  checksum and the OwnCloud chunk parameters are refused.

PIO writers are handled the same way whatever their authentication: a
trusted client (`sss` or local) also needs the capability, and anonymous
(nobody) identities are refused at open. The client cannot choose the file,
path or locations of the commit or drop, it only provides the size, checksum
and modification time. A PIO drop always drops the whole file. Commit and drop
requests without `mgm.pio.commit` or `mgm.pio.drop` are the internal FST ones
and still require a trusted client (`sss` or local), exactly as before.

Not covered: the committed size and checksum are taken as sent by the client.
A capability can also be replayed, within the maximum upload age, to drop the
file it was issued for.

### Failure handling

| Failure | Action |
|---------|--------|
| PIO open fails before the MGM created the file (not RAIN, anonymous client, ...) | Fall back to a gateway write |
| PIO open fails after the MGM created the file (missing info, stripe open failure) | Mark the opened stripes for deletion (`Remove()`), drop the file at the MGM (`mgm.pcmd=drop&mgm.pio.drop=1`), fall back to a gateway write |
| Write error, checksum error or commit error at close | Mark all stripes for deletion, close the layout, drop the file at the MGM |
| Layout close fails after a successful commit | Drop the file at the MGM. The locations are registered, so the MGM also deletes the stripes. |
| File object destroyed without close | Mark all stripes for deletion, drop the file at the MGM |

A stripe marked for deletion is removed by its FST on close or disconnect,
without any interaction with the MGM.

## Fallback summary

| Open type | PIO attempted | On PIO failure |
|-----------|---------------|----------------|
| Read (`Read`, no write flags) | yes | normal (gateway) read |
| Create/overwrite (`New` or `Delete`) | yes | normal (gateway) write |
| Update only | no | normal (gateway) write |
