# EOS ACL (Access Control List)

This module implements access control list interpretation for the EOS MGM. ACL rules control read, write, delete, and other permissions on directories and files.

## Rule Format

ACL rules are strings with the following format:

```
<subject>:<permissions>
```

Multiple rules are comma-separated. Example:

```
u:1234:rwx,g:5678:r-x,z:rx
```

## Subject Types

| Prefix | Description | Example |
|--------|-------------|---------|
| `u:` | User by UID or username | `u:1234:rwx`, `u:alice:rwx` |
| `g:` | Group by GID or groupname | `g:5678:rx`, `g:physics:rx` |
| `egroup:` | External group (e-group) by name | `egroup:my-vo-group:rx` |
| `z:` | Zero / everyone (all users) | `z:rx` |
| `k:` | Authentication key | `k:mykey:rwx` |

## Extended Attributes

ACLs are stored in extended attributes:

| Attribute | Description |
|-----------|-------------|
| `sys.acl` | System ACL (directory or file). Always evaluated. |
| `user.acl` | User ACL. Only evaluated when `sys.eval.useracl` is set. |
| `sys.eval.useracl` | When present, enables evaluation of `user.acl` for the path. |

File-level `user.acl` overrides directory `user.acl` when `sys.eval.useracl` is set on the file.

## Supported Permissions

### Basic Permissions (sys.acl and user.acl)

| Flag | Description |
|------|-------------|
| `r` | Read permission |
| `w` | Write permission (implies update) |
| `wo` | Write-once permission (create files, no delete) |
| `x` | Browse permission (list directory, stat) |
| `d` | Delete permission (implicit when not denied) |
| `u` | Update permission (modify existing files) |
| `m` | Mode change permission (chmod) |
| `c` | Owner change permission (chown, directories only) |
| `q` | Quota setting permission (**sys.acl only**) |
| `i` | Immutable flag (when denied via `!i`, directory is immutable) |

### System ACL Only Permissions

These permissions are only valid in `sys.acl` (not in `user.acl`):

| Flag | Description |
|------|-------------|
| `a` | Archiving permission |
| `A` | System ACL modification permission (modify `sys.acl`) |
| `X` | System attribute modification permission (modify `sys.*` extended attributes) |
| `p` | Prepare/workflow permission |
| `t` | Token issuing permission (allows the referenced user/group/egroup to issue tokens on that path) |

## Modifiers

| Modifier | Description |
|----------|-------------|
| `!` | Deny the following permission |
| `+` | Reallow (only applies to `d` and `u`) |

### Denial and Reallow Logic

- `!d` forbids deletion (even if write is granted).
- `!u` denies update (modify existing files).
- `+d` reallows deletion after a prior `!d` denial.
- `+u` reallows update after a prior `!u` denial.

**Important:** `+d` and `+u` are only effective in `sys.acl`. They are ignored when specified in `user.acl`.

## Rule Parsing Logic

1. **Token precedence:** If the client presents a valid token, the token's ACL completely replaces both `sys.acl` and `user.acl` for that request.

2. **Rule combination:** When no token is present, `sys.acl` and `user.acl` (if `sys.eval.useracl` is set) are concatenated. Rules are processed in order: sys.acl first, then user.acl.

3. **Matching:** For each rule, the subject is matched against the client's virtual identity:
   - User match: `u:<uid>` or `u:<username>` vs. client UID
   - Group match: `g:<gid>` or `g:<groupname>` vs. client GID(s)
   - E-group match: membership is checked via `EgroupRefresh`
   - `z:` matches all users
   - Key match: `k:<key>` vs. client auth key

4. **Multiple groups:** If secondary groups are enabled, the client is matched against all of their groups. A rule matches if it matches the user, any group, any e-group, or `z:`.

5. **Permission accumulation:** When a rule matches, each permission character in the rule is processed. Denials (`!`) and reallows (`+`) are tracked. After all rules are processed, a final pass applies reallows (which override earlier denials) and denials.

6. **Write and update:** Granting `w` implicitly grants `u` (update). Granting `wo` (write-once) does not grant delete.

7. **Quota:** The `q` permission is only honored when specified in `sys.acl`; it is ignored in `user.acl`.

## Space ACLs

Space-level ACLs apply to all directories that reference a space via `sys.forced.space`. If a directory does not reference a space, ACLs from the default space are used. Space ACLs are configured via `eos space config`:

```bash
eos space config <space-name> space.attr.sys.acl=<value>
```

### Add-on modes

A prefix before the ACL value controls how it is merged with directory ACLs:

| Prefix | Mode | Effect |
|--------|------|--------|
| `=<` | Left | Insert space ACL first (evaluated before directory ACL) |
| `=>` | Right | Append space ACL last (evaluated after directory ACL) |
| `=\|` | Conditional | Use space ACL only if the directory has no `sys.acl` |
| `=` | Overwrite | Replace directory `sys.acl` with space ACL |

### Examples

```bash
# Prepend space ACL (first evaluated; < may need quoting in shell)
eos space config default "space.attr.sys.acl=<u:poweruser:rwxqmcXA"

# Append space ACL (last evaluated; > may need quoting in shell)
eos space config default "space.attr.sys.acl=>u:poweruser:rwxqmcXA"

# Use space ACL only when directory has no sys.acl (| must be escaped in shell)
eos space config default "space.attr.sys.acl=\|u:poweruser:rwxqmcXA"

# Overwrite all directory ACLs with the space ACL
eos space config default space.attr.sys.acl=u:poweruser:rwxqmxcXA
```

### Remove and inspect

```bash
# Remove space ACL
eos space config rm default space.attr.sys.acl

# Show space configuration (including ACLs)
eos space status default
```

## Validation

ACL syntax is validated via regex. Two modes exist:

- **Generic:** Allows numeric IDs and names for `u`/`g`; supports `k:` and `egroup:`.
- **Numeric:** Restricts `u`/`g` to numeric IDs only (used when `check_numeric` is true).

System ACLs (`sys.acl`) allow the additional flags `p`, `t`, `!m`, `!d`, `+d`, `!u`, `+u` in the regex. User ACLs do not support `p` or `t`.

## ID Conversion

`Acl::ConvertIds()` can convert between numeric and string representations of UIDs/GIDs in ACL rules. By default it converts names to numeric IDs; with `to_string=true` it converts numeric IDs to names.
