# EOS BATS tests

The instance tests in `test/<suite>/` use [bats-core](https://bats-core.readthedocs.io/).
`eos.bash` here holds the helpers that all suites share. bats-core 1.14.0,
bats-support 0.3.0 and bats-assert 2.2.0 are git submodules in `vendor/`, pinned to
their release tags: the same bats on each platform, no package to install.
`eos-bats-test` runs it. Get them with `git submodule update --init test/bats/vendor`
(a full EOS checkout with `--recursive` has them). To update one:

```bash
git -C test/bats/vendor/bats-core fetch --tags
git -C test/bats/vendor/bats-core checkout v<new version>
git add test/bats/vendor/bats-core     # and change the versions above
```

## Run a suite

On the MGM host of a test instance:

```bash
eos-bats-test <suite> <prefix> [<bats options>]
eos-bats-test contention /eos/dockertest
eos-bats-test contention /eos/dockertest --filter "rm of one file"
eos-bats-test                      # list the suites
eos-bats-test contention --list    # list the tests, the names for --filter
eos-bats-test contention /eos/dockertest --filter-tags '!slow'  # skip the slow checks
```

From the source tree, use `test/eos-bats-test`. The tests work in `<prefix>/<suite>`.
The JUnit report goes to `/tmp/eos-bats-reports/<suite>/report.xml`, or to
`$EOS_TEST_REPORT_DIR`. The output of each test goes to `/tmp/eos-<suite>/checks`.

## Start a new suite

```bash
test/eos-bats-test --new <suite>
test/eos-bats-test <suite> /eos/dev    # it runs at once, on a test instance
```

The template has a test directory for the suite, its removal and the undo of the
changes at the end, and example tests. The packages and CI find a new suite
without a change.

## Layout of a suite

```text
test/<suite>/
  <suite>.bash        settings and helpers; starts with "load ../bats/eos"
  setup_suite.bash    setup_suite and teardown_suite: once for the whole run
  1-<part>.bats       the tests; bats runs the files in name order
  2-<part>.bats
```

Each `.bats` file starts with `load <suite>`. `setup_suite` calls `eos_setup_suite`
first. CMake installs each directory of `test/` with `.bats` files as a suite, and
the RPM and Debian packages take all of `/usr/share/eos/test`: a new suite needs no
build or package edit.

## Run a suite in CI

A job that extends `.dock8s_bats_template` (k8s) or `.helm_bats_template` (helm) runs
one suite and shows its JUnit report in the MR when it fails:

```yaml
k8s_<suite>:
  extends: .dock8s_bats_template
  variables:
    BATS_SUITE: <suite>
    BATS_ENV: <SETTING>=<value>   # optional, the settings of the suite
```

The job keeps the suite logs (`/tmp/eos-<suite>`) in its artifacts, in `bats-logs/`,
when it fails. Put the set up that the suite needs in `setup_suite`, not in the
job, so that a run on a test instance does the same as CI.

A job that needs more steps writes its own `script` with
`!reference [.dock8s_bats_run, script]` (or `.helm_bats_run`) in it. See the
contention jobs.

## What eos.bash gives

| Name | Use |
|---|---|
| `$suite`, `$testdir`, `$logs` | name of the suite, `$EOS_TEST_PREFIX/$suite`, local log directory |
| `$url` | the MGM URL for `xrdcp` and `xrdfs`: `$EOS_MGM_URL` or `root://localhost` |
| `eos_setup_suite` | clears the markers of the last run, runs what `undo` left, fails if the MGM does not answer |
| `eos_teardown_suite` | runs `undo` and removes `$testdir` |
| `undo <cmd>` | run the command at the end of the suite, to undo a change of the instance (last first) |
| `space_set <space> <key> <value>` | set `space.<key>` and undo it at the end. `<key>` as in `eos space status -m` |
| `space_value <space> <key>` | one value of `eos space status -m` |
| `eos_as <uid> <gid> <args>` | `eos -r <uid> <gid> <args>`: the command as another user |
| `upload <local> <path> [opts]`, `download <path> <local> [opts]` | `xrdcp` to and from an EOS path |
| `same_file <local> <path>` | true if the EOS file has the bytes of the local file |
| `make_file <local> <size>` | a local file of random bytes, `<size>` as for `head -c` (`16M`) |
| `json <filter> <eos args>` | `eos -j <args> \| jq -r <filter>`, e.g. `json .id fileinfo $f` |
| `attr_value <path> <key>` | the value of an extended attribute, empty if not set |
| `fs_value <fsid> <key>` | one value of `eos fs ls -m`, e.g. `configstatus`, `host`, `local.drain` |
| `fs_set <fsid> <key> <value>` | `eos fs config`, undone at the end of the suite |
| `replicas <path>`, `replica_file <path> <fsid>` | the fsids of a file, and the path of one replica on its FST |
| `on_fst <fsid> <cmd>` | run a shell command on the host of a file system (here, or through `$EOS_FST_EXEC <host> <cmd>`; else the test skips) |
| `is <expected> <cmd>` | true if the command prints `<expected>` |
| `$http_url`, `http <method> <path> [curl opts]` | the HTTPS endpoint of the MGM (`$EOS_HTTP_URL`, else the XrdHttp port of `/etc/xrd.cf.mgm`), and a request to it |
| `token_for <path> <perm> [<s>]` | a token for nobody; a `<path>` with a final `/` covers the tree |
| `recycle_entries <path>`, `recycle_as_owner <action> <path>` | `<uid> <gid> <key>` of each recycle bin entry under a path (`"<path> "` for the path only), and `restore -f` or `purge -k` of one entry as its owner |
| `mount_fuse <dir>`, `umount_fuse <dir>` | an eosxd mount of `/eos`, ready when `$testdir` is in it, and its unmount |
| `log_has <regex>` | true if the MGM log has a matching line since the start of the suite |
| `make_testdir`, `remove_testdir` | an empty `$testdir` that all users may write, and its removal |
| `wait_writable <dir>` | wait until the instance accepts writes (a new instance refuses them for a while) |
| `wait_until <s> <cmd>` | retry a command every second until it succeeds, for at most `<s>` seconds; at the end it prints the last output |
| `need <part>` | skip the test if the set up put `<part>` in `$skip` |
| `md_field <path> <key>` | one value of `eos fileinfo -m` |
| `fs_ids` | the ids of all file systems |
| `none <cmd>` | true if the command prints nothing; else it prints the lines |
| `together <cmd> ...` | run commands at the same time, print each exit code |
| `run_race`, `run_pairs` | n copies of a command, or 8 pairs of two commands, at the same time |
| `mgm_log` | the lines of the MGM log (`$mgmlog`) since the start of the suite |
| `order_checking_on`, `order_checking_restore` | turn on the lock order checking of the MGM for the suite, and restore it at the end |
| `dump_stacks <why>` | save the MGM thread stacks once, in `$logs/stacks/mgm.txt` |
| `mgm_down <why>` | mark the MGM as down: the next tests skip |
| `setup`, `teardown` | see below. A file with its own calls `eos_setup` and `eos_teardown` in them |
| `run -0`, `assert_output` ... | from bats-assert and bats-support |

After each test, `teardown` copies the output of the test to `$logs/checks`. At a
timeout it saves the MGM stacks. After a failure or a timeout it asks the MGM once
(15 s). If the MGM does not answer, it writes `$logs/mgm.down`, and it saves the
stacks if the MGM process is still there. From then on, `setup` skips each test at
once ("the MGM is down since: ..."). So a crash or a hang ends the run fast, with one
failure.

After a failure, `$logs/failed/<n>.mgm.log` has the MGM log lines of test `<n>`:
CI keeps them with the suite logs.

`BATS_TEST_TIMEOUT` (default 600 s) stops a test that hangs.
`XRD_REQUESTTIMEOUT` (default 60 s) stops each xrootd request, the console too.

## Change eos.bash

`selftest.bats` tests the helpers of `eos.bash` and `runtest.bash` with stubs of
`eos` and `pgrep`, without an instance. pre-commit runs it when one of these files
changes; or run `test/bats/vendor/bats-core/bin/bats test/bats/selftest.bats`. pre-commit also
runs shellcheck on the BATS files when shellcheck is installed (EPEL: `ShellCheck`).

## Convert an old test

| Old | BATS |
|---|---|
| `pass`/`fail` counters, `check "<what>" '<cond>'` | `@test "<what>" { <cond> }`. The last command gives the result |
| set up at the start of the script | `setup_suite`, or `setup_file` for one file |
| `trap ... EXIT`, clean up at the end | `teardown_suite` or `teardown_file`: they also run after a failure or an interrupt |
| a feature probe and `if` around the tests | the probe adds to `skip`, the tests call `need <part>` |
| state from one test to the next | `export` it in `setup_suite`/`setup_file`, or write a file in `$logs` |
| the test prefix as argument | `EOS_TEST_PREFIX` (`eos-bats-test` sets it), `$testdir` |
| `exit 1` in a check | `return 1` |
| a `runtest` line (`<info> <auth> <expect> <env> <mode> ...`) | the same line in an `@test`, with `load ../bats/runtest`: see `runtest.bash`. Fast, but the lines stay hard to read |

A test written in the BATS way is easier to read and its failures say more. These
are the lines that the old test scripts have most:

| Old script | BATS |
|---|---|
| a command that must succeed | the command: a failure ends the test |
| a command that must fail | `run <cmd>` then `assert_failure`, or `run -2 <cmd>` for one exit code |
| `xrdcp` up, `xrdcp` down and `diff` | `upload $src $dst` then `same_file $src $dst` |
| `eos -r 3 4 ...`, `EOSROLE='-r 3 4'` | `eos_as 3 4 ...` |
| `<cmd> \| grep <text>` as a check | `run -0 <cmd>` then `assert_output --partial <text>` |
| `eos space config default space.x=1`, and the restore later | `space_set default x 1`: it is undone at the end, also after a failure |
| `eos fs config $fs configstatus=drain`, and the restore later | `fs_set $fs configstatus drain` |
| another change of the instance (`vid`, `access` ...) and its clean up | the change, then `undo <the command that reverts it>` |
| `sleep N` before a check | `wait_until <max> <the check>`: it goes on as soon as the check is true. Examples: `wait_until 600 is drained fs_value 3 local.drain`, `wait_until 60 log_has "<regex>"` |
| `eos -j ... \| jq ...` | `json <filter> <eos args>` |
| `exec_cmd eos-fst1 "<cmd on a replica>"` with `eos fileinfo --fullpath` | `on_fst $fs "<cmd> $(replica_file $f $fs)"` for each `fs` of `replicas $f` |
| `eos recycle ls ... \| awk '{print $10}'` | `recycle_entries "$path "`, `recycle_as_owner "restore -f" $path` |
| `eos token --path ... --expires ...` | `token_for $dir/ rwx`, then `$url/$dir/f?authz=$token` |
| `curl -k -L https://<host>:<port>/...` | `http GET <path>` |
| an `eosxd` mount and its unmount | `mount_fuse $dir`, `umount_fuse $dir` |
| `dd if=/dev/urandom of=$f bs=1M count=16` | `make_file $f 16M` |
| a fixed path of the instance, e.g. `/eos/$EOS_TEST_INSTANCE/test/...` | `$testdir/...` (each suite has its own) |

Wait for a state, not for a time: a `sleep` is too long on a fast instance and too
short on a slow CI. Only a wait with no state to see (e.g. the 60 s quota cache)
keeps its `sleep`, with a comment that says why.

Not for BATS yet: `eos-fsck-test`, `eos-fst-close-test` and `eos-drain-test` change
files on the FST disks. In CI a BATS suite runs in the MGM pod, which cannot reach
the FST pods, so these scripts stay on the CI runner. `on_fst` works where the FSTs
are on the host of the suite (a dev box), or where `EOS_FST_EXEC` reaches them.

A test that fails only sometimes (a known race) can be retried:
`BATS_TEST_RETRIES=2` at the top of its file, or in `setup_file`. The JUnit report
shows each try. A retry hides a bug, so add a comment with the ticket of the race.

Put one behavior in one `@test`, with a name that says what must be true. Keep the
steps that only prepare it (mkdir, upload) in the same test, or in `setup_file`.

bats runs each test with `set -eET`. Know these traps:

- A command that fails in the middle of a test ends the test. Write `cmd || true`
  if the old test ignored its result.
- `a && b` ends the test only if `b` fails. `! cmd` never ends the test, except as
  the last command.
- `n=$(grep -c ...)` fails when the count is 0: write `|| true`. The same for
  `x=$(for ...; do a && b; done)`: the last command of the loop gives the status,
  write `if a; then b; fi`.
- Old set up code that ignores errors: run it with `set +e` (see
  `contention/setup_suite.bash`).
- Do not name a function `setup`, `teardown` or `load`: bats uses these names.
- `wait` without a pid also waits for the bats timeout watchdog, until the
  timeout: write `cmd & ... wait $!`.
- A process that stays after the test (a daemon, a mount) must close fd 3, or
  bats waits for it: `eosxd ... 3>&-`.
- A test can print to fd 3 to show a line also when it passes: `echo "# ..." >&3`.
