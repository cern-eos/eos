## EOS console command framework

Console dispatch and completion use `CommandRegistry` and `IConsoleCommand`.
`CommandFramework.cc` registers the native commands once; each command exposes a
`Register*` function and is listed in `console/CMakeLists.txt`.

### Source layout

- `CommandFramework.*`: command interface, registry, context, and registration.
- `ConsoleMain.*`: process options, interactive shell, request transport, and dispatch.
- `ConsoleCompletion.*`: readline and shell completion.
- `ConsoleCli.hh`: shared CLI11 argument parsing and validation helpers.
- `commands/native/`: active command implementations and aliases.
- `commands/helpers/`: protobuf request helpers shared with the console and other EOS targets.
- `commands/HealthCommand.*`: health checks also used by the MGM gRPC interfaces.

`EosConsoleCommands-Objects` is linked into `eos`, the MGM, and the unit tests.
`EosConsoleHelpers-Objects` is also used by the gRPC namespace client and FST
targets. These shared dependencies must be considered when changing a helper.

### Commands and arguments

`IConsoleCommand` exposes `name()`, `description()`, `requiresMgm(args)`,
`run(args, context)`, and `printHelp()`. Override `printHelp(args)` for focused
subcommand pages reached through `eos help command subcommand`. Commands can also provide `helpText()`
and `complete(args)` for completion. `CommandContext` supplies global protobuf
options, JSON/silent output settings, and the request/output callbacks.

Define options, subcommands, argument counts, and validation with CLI11 where
possible. Keep `run()` arguments as a vector so spaces and punctuation inside an
argument are preserved. `ParseCliArguments()` handles CLI11's reversed-vector
input convention without changing the caller's arguments. Helpers with the
legacy `ParseCommand(const char*)` interface can use `TokenizeArguments()` to
adapt a command string to the same parser. Help text should come from that
parser where possible so accepted options and usage stay together.

Preserve established command-specific options when configuring help: `du -h`
means human-readable sizes, while `du --help` requests help. Invalid arguments
must set `global_retc = EINVAL` and return without sending a request. Server
return codes must propagate through `global_retc`.

Protobuf commands use the existing `ICmdHelper` transport. CGI commands use the
`clientCommand` and `outputResult` callbacks in `CommandContext`. Changing a
request from one transport to the other requires checking the corresponding MGM
dispatcher; a native command name is not proof that a legacy CGI handler exists.

### Aliases and compatibility entry points

Aliases forward vectors to another registered command with the same context.
For example, `info` and `fileinfo` prepend `info` and invoke `file`.

Some active commands still use C-style entry points: `cat` calls `com_cat()` and
`com_cp()`, and `squash` uses `com_squash()` internally. Readline path completion
invokes `ls` directly with an argument vector. The old command table, unused
legacy source copies, and unused compatibility adapters have been removed.

### Completion

Bash, Zsh and the interactive console use the same command completion metadata.
Every registered command has an explicit schema, including commands that take
no arguments. Registry-wide tests require a matching coverage entry when adding
a command.

Commands with a shared CLI11 execution schema expose that schema directly.
Manual command grammars have separate completion-only CLI11 definitions in
`CompletionAdminSchemas.cc` and `CompletionStorageSchemas.cc`; update those
definitions alongside changes to their execution grammar. Completion inspects
the definitions without running parse callbacks, validators, or commands.
Help text is not parsed to infer a grammar.

The completer tracks subcommands, option arity, combined short flags, repeated
options, exclusions, `--`, finite value suggestions and positional path kinds.
Freeform names, numbers and identifiers remain freeform even when examples are
offered. Local paths are listed locally; EOS paths use read-only MGM listings.
Completing an option or a subcommand does not require an MGM connection.
Aliases inherit the completion of their target command and prepended arguments.

### Tests

Console tests are part of `eos-unit-tests`. `ConsoleTestFixture.hh` runs registered
commands through the real IPC client transport against a local fake MGM, captures
the request, and checks decoded protobuf requests with the MGM command factory.
Use it to verify serialized options and paths, return codes, and rejection of
invalid arguments before any request is sent. It does not execute operations on
a running EOS instance.

Build `eos-unit-tests` and run it directly. The process tests build and launch
the matching `eos` executable, with an IPC responder and temporary files. In an
installed test package they use the installed console. They cover the process
option parser as well as the command registry; neither layer substitutes for
the other. Run console tests in shuffled order when changing shared globals.

### Help layout

Use `ConsoleHelp::HelpPage` blocks for new static help rather than embedding
screen-width line breaks in strings. Sections, paragraphs, usage, definitions
and literal examples have explicit roles. Layout measures UTF-8 display cells,
wraps prose and usage at word boundaries, and keeps literal examples and long
tokens intact. Color is added after layout. Standard CLI11 help uses its option
and command metadata with a copied formatter; custom formatters stay untouched.

The console entry point activates layout independently from color. Only a
terminal destination uses adaptive layout, with `EOS_HELP_WIDTH` as an explicit
override. Batch, JSON and redirected output retain the previous layout.
Namespace root help becomes an operation index on terminals, with focused
subcommand pages and `--help-all` for the full manual. Existing legacy pages
are not heuristically reflowed. `EOS_PAGER` enables optional direct-exec paging
for long interactive help; `--no-pager` disables it. Data output never goes
through this renderer.

### Validation contracts

The CLI11 schemas cover `config`, `group`, `df`, `route`, `recycle`, `evict`,
`register`, `rm`, `du`, `ls`, `cd`, `chmod`, `chown`, and the protobuf operations
in `FileHelper`. The schemas describe required operands, mutually exclusive
options, dependent flags, numeric ranges, and generated help. Aliases retain
the caller's argument vector. The shell lexer handles quotes and escapes but
does not expand variables, globs, or shell commands.

- Decimal counters retain decimal meaning with leading zeros: `010` is ten,
  and `08` is eight. Hexadecimal is accepted only where explicitly documented.
- File/container identifiers require an exact prefix, full numeric consumption,
  and a value representable as `uint64`. A path ending in `fid:123` is a path.
- A batch `rm` validates every operand before sending its first request.
- `--` ends option parsing. Literal names containing help text remain single
  operands. Preserve `ls -h`, `du -h`, and `chown -h` command-specific meanings.
- Permission modes are octal; UID/GID and filesystem fields use their actual
  wire/server ranges. Checksum input consists of complete hexadecimal byte pairs.
- Recycle sizes and counts accept established decimal/binary units and exact
  fractional quantities only when the final value is integral and in range.
  Lifetimes retain their documented unit suffixes and zero where supported.
- Comments are metadata tokens before `--`. Protobuf carries them as strings;
  legacy CGI uses the existing marker-based CURL codec and MGM decodes it.
  New clients talking to older MGMs may show encoded legacy audit text until
  the server is updated. App/role fields reject `&` and control characters
  because those fields have no negotiated encoding contract.

### Remaining review work

The console still contains manual and hybrid grammars. Command names registered
in C++ or an inclusion of CLI11 do not prove that all parsing uses CLI11.
The remaining families are `access`, `accounting`, `acl`, `attr`, `backup`, `cat`,
`convert`, `daemon`, `debug`, `devices`, `file check`, `find`/`newfind`, `fs`,
`fsck`, `fuse`, `fusex`, `geosched`, `health`, `inspector`, `io`, `map`, `monit`,
`node`, `ns`, `oldfind`, `quota`, `reconnect`, `report`, `role`, `rtlog`, `sched`,
`scitoken`, `space`, `squash`, `stat`, `test`, `token`, `tracker`, and `vid`.
Migrate each grammar with tests of its actual request and supported command
forms; a permissive CLI11 wrapper around a second parser is insufficient.

Server inspection also found issues requiring separate namespace/instance
testing before changing behavior:

- Root `recycle purge --all DATE` ignores the date and selects the entire recycle
  prefix. Help states that scope; do not use it as a date-filtered operation.
- Recycle configuration's internal attribute command checks `open()` but does
  not propagate the result from `close()`/`GetRetc()`.
- Generic `FileCmd::ResolvePath()` treats numeric metadata IDs as file IDs even
  when the metadata type says container. Generic directory operations therefore
  retain literal `cid:`/`cxid:` paths for the existing identifier resolver.
- Ordinary file touch checksum persistence and numeric chown IDs above `INT_MAX`
  need server-side verification; serialized request tests do not certify those
  existing server paths.

The fake MGM validates transport selection, encoded requests, validation before
IPC, and error propagation. It does not certify namespace changes, recycle
policies, data movement, locks, or external XRootD/S3 services. In particular,
recycle-bin add/remove paths containing spaces, `&`, and `%` need a disposable
instance test to verify the resulting `sys.recycle` attribute end to end.

### Terminal styling

The global `--color=auto|always|never` option controls human-readable output.
`auto` (the default) checks each output destination independently with `isatty`
and terminfo; missing, unknown and `dumb` terminal types remain plain. A nonempty
`NO_COLOR` disables automatic styling, including `NO_COLOR=0`. Explicit `always`
overrides that preference and supports redirected human output, such as a pager.
`never` disables all generated styling.

Batch (`-b`), JSON (`-j`), shell-completion records and raw data remain unstyled,
including when `always` is requested. Existing monitoring formats retain their
plain representation. Exit codes, whitespace, stream selection and field values
are unaffected by styling alone. Use `--color=never` in scripts which explicitly need plain human
output; scripts supporting older EOS versions must not require the new option.

`ConsoleColor` owns this policy, activated by the console entry point. Shared
helpers retain their existing server behavior outside that entry point. CLI11
help is decorated through a temporary
formatter after wrapping and alignment, while `helpText()` remains plain. Prompt
escapes alone use Readline's invisible-character markers. Semantic diagnostics
and local health tables use the destination stream's policy. The existing
protobuf `dontcolor` flag covers both streams, so server coloring is permitted
only when both destinations permit it. No protocol fields or
server defaults change. Static and CLI11 help share post-layout styling through
`PrintLegacyHelp` and `PrintCliHelp`; pure help strings stay plain. Registry-wide
tests cover every help target, including deliberate empty pages, and preserve
streams, exit codes and exact unstyled text. Help uses bold green command paths,
cyan options, yellow argument placeholders and subdued grammar separators.
Section and usage labels are emphasized separately from descriptions. Legacy
command names come from copied completion metadata loaded only when colored help
needs it; no parser callbacks, validators or namespace requests run. Shared
helpers retain no dependency on the console registry. Further table styling
should use semantic fields rather than replacements in arbitrary server text.

`ConsoleColorTest`, `ConsoleColorRequest` and `ConsoleColorProcessTest` cover the
policy, the existing wire flag, exact plain help, real pseudo-terminals, mixed
stream redirection, environment overrides and machine-output precedence.

### Traffic shaping display

Human `io shaping ls`, `policy ls`, `pressure ls`, and `config ls` output is
rendered in the console from the existing JSON reply. It uses aligned tables
when they fit and stacked details on narrow terminals. Identities remain
complete, rates carry SI units, and unavailable pressure values are distinct
from measured zero. System statistics, controller feedback, protected cohorts,
and published FST actuator values have separate sections. No engine-wide total
is inferred by adding rows from a particular projection.

Headings, identities, enabled states, warnings, and pressure indicators use the
shared semantic color policy. Entity-type labels and values are yellow; read
fields and operations are blue, and write fields and operations are magenta.
These roles apply to table headings, values, and wrapped detail labels. Warning
and status colors take precedence, and unavailable or unlimited values remain
dimmed. Layout measures terminal cells before adding styles. Control characters
in structured field values are escaped so a label
cannot inject terminal formatting. Successful shaping mutations retain their
message text and style only the `success:` label.

This human layout intentionally changes. Explicit shaping `--json` and global
`-j` keep their existing requests and output, and `io stat -m` and `io ns -m`
retain their monitoring representation. Other IO commands use their existing
printing path. Rendering uses a single request; malformed or unsupported replies
fall back to the original response without retrying an operation. It uses the
existing JsonCpp and console layout/color helpers, with no new dependencies.
