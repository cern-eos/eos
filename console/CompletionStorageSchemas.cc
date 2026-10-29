#include "console/CommandCompletion.hh"
#include <CLI/CLI.hpp>
#include <initializer_list>

namespace {
using Schema = CommandCompletionSchema;
using Path = CompletionPath;

void
Flags(Schema& schema, CLI::App& command, std::initializer_list<const char*> names)
{
  for (const auto* name : names) {
    schema.flag(command, name);
  }
}

void
Values(Schema& schema, CLI::App& command, std::initializer_list<const char*> names)
{
  for (const auto* name : names) {
    schema.value(command, name);
  }
}

const std::vector<std::string> kChecksums = {"none",   "adler",  "adler32",  "blake3",
                                             "crc32",  "crc32c", "md5",      "sha1",
                                             "sha256", "crc64",  "xxhash64", "hwh64"};
const std::vector<std::string> kFsStates = {"off", "empty", "drain", "ro", "wo", "rw"};

void
ConfigureFile(Schema& schema)
{
  auto& app = *schema.app;
  auto source = [&](const char* name) -> CLI::App& {
    auto& command = schema.sub(app, name);
    schema.argument(command, "source", {}, Path::Eos);
    return command;
  };

  auto& adjust = source("adjustreplica");
  schema.argument(adjust, "space");
  schema.argument(adjust, "subgroup");
  schema.flag(adjust, "--nodrop");
  schema.value(adjust, "--exclude-fs");

  auto& check = source("check");
  schema.argument(
      check, "checks",
      {"%size", "%checksum", "%nrep", "%diskchecksum", "%force", "%output", "%silent"});

  auto& convert = source("convert");
  schema.argument(convert, "layout");
  schema.argument(convert, "target-space");
  schema.argument(convert, "placement-policy", {"scattered", "hybrid:", "gathered:"});
  schema.argument(convert, "checksum", kChecksums);
  schema.flag(convert, "--rewrite");

  auto& copy = source("copy");
  Flags(schema, copy, {"-f", "-s", "-c"});
  schema.argument(copy, "destination", {}, Path::Eos);

  auto& drop = source("drop");
  schema.argument(drop, "location", {"cache"});
  schema.flag(drop, "-f");

  auto& info = source("info");
  Flags(schema, info,
        {"--path", "--fid", "--fxid", "--size", "--checksum", "--fullpath", "--proxy",
         "-m", "-s,--silent", "--env"});

  auto& layout = source("layout");
  layout.allow_non_standard_option_names();
  auto* stripes = schema.value(layout, "-stripes");
  auto* checksum = schema.value(layout, "-checksum", kChecksums);
  auto* type = schema.value(layout, "-type");
  stripes->excludes(checksum)->excludes(type);
  checksum->excludes(type);

  for (const auto* name : {"move", "replicate"}) {
    auto& command = source(name);
    schema.argument(command, "source-fsid");
    schema.argument(command, "destination-fsid");
  }

  for (const auto* name : {"purge", "version"}) {
    auto& command = source(name);
    auto* count = schema.argument(command, "purge-version");
    if (std::string(name) == "version") {
      schema.flag(command, "--create-folder")->excludes(count);
    }
  }

  auto& rename = source("rename");
  schema.argument(rename, "new-path", {}, Path::Eos);
  auto& renameLink = source("rename_with_symlink");
  schema.argument(renameLink, "destination-dir", {}, Path::EosDirectory);
  auto& share = source("share");
  schema.argument(share, "lifetime");
  auto& symlink = source("symlink");
  schema.flag(symlink, "-f");
  schema.argument(symlink, "target", {}, Path::Eos);
  auto& tag = source("tag");
  schema.argument(tag, "location");

  auto& touch = source("touch");
  Flags(schema, touch, {"-a", "-n", "-0"});
  auto* lock = schema.flag(touch, "-l");
  auto* unlock = schema.flag(touch, "-u");
  lock->excludes(unlock);
  // The optional hardlink path is on a storage server, not in the EOS namespace.
  auto* first = schema.argument(touch, "size-or-link-or-lifetime");
  auto* second = schema.argument(touch, "checksum-or-audience", {"user", "app"});
  unlock->excludes(first)->excludes(second);

  auto& verify = source("verify");
  verify.allow_non_standard_option_names();
  schema.argument(verify, "fsid");
  Flags(schema, verify,
        {"-checksum", "-commitchecksum", "-commitsize", "-commitfmd", "-resync"});
  schema.value(verify, "-rate");
  auto& versions = source("versions");
  schema.argument(versions, "grab-version");
  auto& workflow = source("workflow");
  schema.argument(workflow, "workflow");
  schema.argument(workflow, "event");
}

std::vector<std::string>
FsConfiguration()
{
  std::vector<std::string> result;
  for (const auto& state : kFsStates) {
    result.push_back("configstatus=" + state);
  }
  for (const auto* state : {"rw", "wo", "ro", "drain", "internal", "clientro", "none"}) {
    result.push_back(std::string("sched=") + state);
  }
  for (const auto* key : {"drain=on",
                          "drain=off",
                          "headroom=",
                          "scaninterval=",
                          "scan_rain_interval=",
                          "scanrate=",
                          "scan_disk_interval=",
                          "scan_ns_interval=",
                          "scan_ns_rate=",
                          "fsck_refresh_interval=",
                          "scan_altxs_rate=",
                          "scan_altxs_interval=",
                          "altxs_sync=0",
                          "altxs_sync=1",
                          "altxs_sync_interval=",
                          "graceperiod=",
                          "drainperiod=",
                          "forcegeotag=",
                          "s3credentials=",
                          "sharedfs="}) {
    result.emplace_back(key);
  }
  return result;
}

void
ConfigureFs(Schema& schema)
{
  auto& app = *schema.app;
  auto& add = schema.sub(app, "add");
  schema.value(add, "-m,--manual");
  schema.argument(add, "uuid");
  schema.argument(add, "node-or-host");
  // This is a path on the target FST, not in the EOS namespace or on the client.
  schema.argument(add, "mountpoint");
  schema.argument(add, "space-or-group");
  schema.argument(add, "status", kFsStates);
  schema.argument(add, "sharedfs");

  auto& boot = schema.sub(app, "boot");
  schema.argument(boot, "identifier", {"*"});
  schema.flag(boot, "--syncdisk")->excludes(schema.flag(boot, "--syncmgm"));
  for (const auto* name : {"clone", "compare"}) {
    auto& command = schema.sub(app, name);
    schema.argument(command, "sourceid");
    schema.argument(command, "targetid");
  }

  auto& config = schema.sub(app, "config");
  schema.argument(config, "fsid");
  schema.argument(config, "setting", FsConfiguration());
  // --comment is stripped as console metadata before FsHelper parses the command.
  schema.value(config, "--comment");
  auto& dropdel = schema.sub(app, "dropdeletion");
  schema.argument(dropdel, "fsid");
  auto& ghosts = schema.sub(app, "dropghosts");
  schema.argument(ghosts, "fsid");
  schema.value(ghosts, "--fxid")->expected(1, -1);
  auto& drop = schema.sub(app, "dropfiles");
  schema.argument(drop, "fsid");
  schema.flag(drop, "-f");
  auto& dump = schema.sub(app, "dumpmd");
  schema.argument(dump, "fsid");
  Flags(schema, dump, {"--count", "--fid", "--fxid", "--path", "--size", "-m", "-s"});

  auto& ls = schema.sub(app, "ls");
  Flags(schema, ls,
        {"-m", "-l", "-e", "--io", "--fsck", "-d,--drain", "-D,--drain_jobs",
         "-F,--failed_drain_jobs", "-s", "-b,--brief"});
  schema.argument(ls, "matchlist");
  auto& mv = schema.sub(app, "mv");
  schema.flag(mv, "--force");
  schema.argument(mv, "source");
  schema.argument(mv, "destination");
  auto& rm = schema.sub(app, "rm");
  schema.argument(rm, "identifier");
  schema.argument(rm, "mountpoint");
  auto& status = schema.sub(app, "status");
  Flags(schema, status, {"-r", "-l"});
  schema.argument(status, "identifier");
  schema.argument(status, "mountpoint");
}

void
ConfigureIo(Schema& schema)
{
  auto& app = *schema.app;
  auto& stat = schema.sub(app, "stat");
  Flags(schema, stat, {"-l", "-a", "-m", "-n", "-t", "-d", "-x", "--ss"});
  Values(schema, stat, {"--sa", "--si"});
  for (const auto* name : {"enable", "disable"}) {
    auto& command = schema.sub(app, name);
    Flags(schema, command, {"-r", "-p", "-n"});
    schema.value(command, "--udp");
  }
  auto& report = schema.sub(app, "report");
  schema.argument(report, "path", {}, Path::Eos);
  auto& ns = schema.sub(app, "ns");
  ns.allow_non_standard_option_names();
  Flags(schema, ns, {"-a", "-m", "-n", "-b", "-100", "-1000", "-10000", "-w", "-f"});

  auto& shaping = schema.sub(app, "shaping");
  schema.sub(shaping, "enable");
  schema.sub(shaping, "disable");
  auto& ls = schema.sub(shaping, "ls");
  auto* grouping = ls.add_option_group("Grouping")->require_option(0, 1);
  Flags(schema, *grouping, {"--apps", "--users", "--groups", "--nodes", "--fs", "--all"});
  Flags(schema, ls, {"--json", "--sys"});
  schema.flag(ls, "--resolve-ids")->excludes(schema.flag(ls, "--no-resolve-ids"));
  schema.value(ls, "--window", {"1", "5", "15", "60", "300"});

  auto& policy = schema.sub(shaping, "policy");
  auto& policyLs = schema.sub(policy, "ls");
  Flags(schema, policyLs, {"--apps", "--users", "--groups", "--controller", "--json"});
  for (const auto* name : {"set", "rm"}) {
    auto& command = schema.sub(policy, name);
    Values(schema, command, {"--app", "--uid", "--gid"});
    if (std::string(name) == "set") {
      Values(schema, command,
             {"--limit-read", "--limit-write", "--reservation-read",
              "--reservation-write", "--controller-limit-read",
              "--controller-limit-write"});
      schema.flag(command, "--enable")->excludes(schema.flag(command, "--disable"));
    }
  }

  auto& pressure = schema.sub(shaping, "pressure");
  auto& pressureLs = schema.sub(pressure, "ls");
  Flags(schema, pressureLs, {"--apps", "--json"});
  auto& config = schema.sub(shaping, "config");
  auto& configLs = schema.sub(config, "ls");
  schema.flag(configLs, "--json");
  auto& set = schema.sub(config, "set");
  Values(schema, set,
         {"--estimators-period", "--policy-period", "--report-period", "--system-window",
          "--detail-auto-low-cardinality", "--detail-auto-high-cardinality",
          "--controller-min-limit", "--active-node-rate-threshold",
          "--io-pressure-threshold", "--max-delay", "--gc-idle"});
  schema.value(set, "--detail", {"aggregate", "fs"});
  for (const auto* name : {"--detail-auto", "--limits", "--reservations"}) {
    schema.value(set, name, {"enabled", "disabled"});
  }
}

std::vector<std::string>
SpaceConfiguration()
{
  std::vector<std::string> result;
  for (const auto* key : {"space.attr.",
                          "space.nominalsize=",
                          "space.balancer.threshold=",
                          "space.balancer.node.rate=",
                          "space.balancer.node.ntx=",
                          "space.balancer.max-queue-jobs=",
                          "space.balancer.max-thread-pool-size=",
                          "space.balancer.update.interval=",
                          "space.drainer.tx.minrate=",
                          "space.drainer.fs.ntx=",
                          "space.drainer.retries=",
                          "space.groupbalancer.ntx=",
                          "space.groupbalancer.min_threshold=",
                          "space.groupbalancer.max_threshold=",
                          "space.groupbalancer.min_file_size=",
                          "space.groupbalancer.max_file_size=",
                          "space.groupbalancer.file_attempts=",
                          "space.groupbalancer.threshold=",
                          "space.groupbalancer.blocklist=",
                          "space.geobalancer.ntx=",
                          "space.geobalancer.threshold=",
                          "space.groupdrainer.threshold=",
                          "space.groupdrainer.group_refresh_interval=",
                          "space.groupdrainer.retry_interval=",
                          "space.groupdrainer.retry_count=",
                          "space.groupdrainer.ntx=",
                          "space.lru.interval=",
                          "space.encryptionkey=",
                          "encryptionkey=",
                          "fs.max.ropen=",
                          "fs.max.wopen=",
                          "space.wfe.interval=",
                          "space.wfe.maxpending=",
                          "space.headroom=",
                          "space.scaninterval=",
                          "space.scan_rain_interval=",
                          "space.scanrate=",
                          "space.scan_disk_interval=",
                          "space.scan_ns_interval=",
                          "space.scan_ns_rate=",
                          "space.scheduler.fillratiolimit=",
                          "space.scheduler.fillratiowarn=",
                          "space.drainperiod=",
                          "space.graceperiod=",
                          "space.inspector.interval=",
                          "space.inspector.price.currency=",
                          "space.inspector.price.disk.tbyear=",
                          "space.inspector.price.tape.tbyear=",
                          "space.policy.layout=",
                          "space.policy.nstripes=",
                          "space.policy.checksum=",
                          "space.policy.blockchecksum=",
                          "space.policy.blocksize=",
                          "space.policy.bw=",
                          "space.policy.schedule=",
                          "space.policy.iopriority=",
                          "space.policy.iotype=",
                          "space.groupsize=",
                          "space.groupmod=",
                          "space.cachespace=",
                          "space.cache.low_watermark=",
                          "space.cache.high_watermark=",
                          "space.tgc.qryperiodsecs=",
                          "space.tgc.freebytesscript=",
                          "space.tgc.availbytes=",
                          "space.tgc.totalbytes="}) {
    result.emplace_back(key);
  }
  for (const auto* key :
       {"space.balancer", "space.drainer.purge.recycle", "space.groupbalancer",
        "space.geobalancer", "space.groupdrainer", "space.lru", "space.filearchivedgc",
        "space.tracker", "space.inspector", "space.geo.access.policy.write.exact",
        "space.geo.access.policy.read.exact", "space.altxs", "space.quota",
        "taperestapi.status", "taperestapi.stage", "space.taperestapi.status",
        "space.taperestapi.stage"}) {
    for (const auto* state : {"on", "off"}) {
      result.emplace_back(std::string(key) + "=" + state);
    }
  }
  for (const auto* value : {"on", "off", "paused"}) {
    result.emplace_back(std::string("space.wfe=") + value);
  }
  for (const auto* value : {"std", "minmax", "freespace"}) {
    result.emplace_back(std::string("space.groupbalancer.engine=") + value);
  }
  for (const auto* value :
       {"geotree", "flat:geo", "flat:roundrobin", "flat:random", "flat:fidrandom",
        "flat:weightedrandom", "flat:weightedroundrobin"}) {
    result.emplace_back(std::string("space.scheduler.type=") + value);
  }
  // A space also accepts the filesystem configuration keys with an fs. prefix.
  for (const auto& value : FsConfiguration()) {
    result.push_back("fs." + value);
  }
  return result;
}

void
ConfigureSpace(Schema& schema)
{
  auto& app = *schema.app;
  auto& ls = schema.sub(app, "ls");
  Flags(schema, ls, {"-s", "-m", "-l", "--io", "--fsck"});
  schema.value(ls, "-g");
  schema.argument(ls, "space");
  schema.sub(app, "tracker");
  auto& config = schema.sub(app, "config");
  schema.argument(config, "space");
  schema.argument(config, "setting", SpaceConfiguration());
  schema.flag(config, "--no-confirmation");
  auto& removeConfig = schema.sub(config, "rm");
  schema.argument(removeConfig, "space");
  std::vector<std::string> keys;
  for (const auto& assignment : SpaceConfiguration()) {
    keys.push_back(assignment.substr(0, assignment.find('=')));
  }
  schema.argument(removeConfig, "key", keys);
  schema.flag(removeConfig, "--no-confirmation");

  auto& define = schema.sub(app, "define");
  schema.argument(define, "space");
  schema.argument(define, "groupsize");
  schema.argument(define, "groupmod");
  for (const auto* name : {"set", "quota"}) {
    auto& command = schema.sub(app, name);
    schema.argument(command, "space");
    schema.argument(command, "state", {"on", "off"});
  }
  auto& rm = schema.sub(app, "rm");
  schema.argument(rm, "space");
  auto& status = schema.sub(app, "status");
  schema.argument(status, "space");
  schema.flag(status, "-m");
  auto& inspector = schema.sub(app, "inspector");
  schema.value(inspector, "-s,--space");
  Flags(schema, inspector,
        {"-c,--current", "-l,--last", "-m", "-p", "-e", "-C,--cost", "-U,--usage",
         "-L,--layouts", "-B,--birth", "-A,--access", "-a,--all", "-V,--vs",
         "-M,--money"});
  auto& nodeSet = schema.sub(app, "node-set");
  schema.argument(nodeSet, "space");
  schema.argument(nodeSet, "key");
  schema.argument(nodeSet, "value-or-file", {}, Path::Local);
  auto& nodeGet = schema.sub(app, "node-get");
  schema.argument(nodeGet, "space");
  schema.argument(nodeGet, "key");
  for (const auto* name : {"groupbalancer", "groupdrainer"}) {
    auto& group = schema.sub(app, name);
    auto& groupStatus = schema.sub(group, "status");
    schema.argument(groupStatus, "space");
    Flags(schema, groupStatus, {"-d,--detail", "-m"});
    if (std::string(name) == "groupdrainer") {
      auto& reset = schema.sub(group, "reset");
      schema.argument(reset, "space");
      schema.flag(reset, "--failed")->excludes(schema.flag(reset, "--all"));
    }
  }
  auto& reset = schema.sub(app, "reset");
  schema.argument(reset, "space");
  Flags(schema, reset,
        {"--egroup", "--mapping", "--drain", "--scheduledrain", "--schedulebalance",
         "--ns", "--nsfilesystemview", "--nsfilemap", "--nsdirectorymap"});
}

void
ConfigureQuota(Schema& schema)
{
  auto& app = *schema.app;
  auto path = [&](CLI::App& command, bool matching) {
    schema.value(command, "-p,--path", {}, Path::EosDirectory);
    if (matching) {
      schema.value(command, "-x", {}, Path::EosDirectory);
      schema.value(command, "-q", {}, Path::EosDirectory);
    }
    schema.argument(command, "directory", {}, Path::EosDirectory);
  };
  schema.flag(app, "-m");
  path(app, true);
  auto& ls = schema.sub(app, "ls");
  Flags(schema, ls, {"-n", "-m", "-s,--summary", "-r,--recursive"});
  Values(schema, ls, {"-u,--uid", "-g,--gid"});
  path(ls, true);
  auto& set = schema.sub(app, "set");
  Values(schema, set, {"-u,--uid", "-g,--gid", "-v,--volume", "-i,--inodes"});
  path(set, false);
  auto& rm = schema.sub(app, "rm");
  Values(schema, rm, {"-u,--uid", "-g,--gid"});
  // rm also accepts the historical --inode spelling
  Flags(schema, rm, {"-v,--volume", "-i,--inodes"});
  path(rm, false);
  auto& rmnode = schema.sub(app, "rmnode");
  path(rmnode, false);
}

void
ConfigureFind(Schema& schema, bool old)
{
  auto& app = *schema.app;
  app.allow_non_standard_option_names();
  Values(schema, app, {"--maxdepth", "-ctime", "-mtime"});
  schema.value(app, "--purge", {"atomic"});
  if (old) {
    Values(schema, app, {"-name", "-layoutstripes"});
  } else {
    Values(schema, app,
           {"-uid", "-nuid", "-gid", "-ngid", "-flag", "-nflag", "--layoutstripes",
            "--name,-name"});
    schema.value(app, "--format",
                 {"uid",   "gid",   "size",        "checksum", "checksumtype", "etag",
                  "fxid",  "pxid",  "cxid",        "fid",      "pid",          "cid",
                  "atime", "btime", "ctime",       "mtime",    "type",         "mode",
                  "files", "link",  "directories", "attr."});
  }
  Values(schema, app, {"-x", "-p"});
  if (old) {
    schema.value(app, "-c");
  }
  Flags(schema, app,
        {"-s",      "-d",          "-f",           "-0",          "-b",
         "--size",  "--fs",        "--checksum",   "--ctime",     "--mtime",
         "--fid",   "--nrep",      "--online",     "--fileinfo",  "--nunlink",
         "--uid",   "--gid",       "--stripediff", "--faultyacl", "--count",
         "--hosts", "--partition", "--childcount", "--xurl"});
  if (old) {
    Flags(schema, app, {"-j", "-m", "-1"});
  } else {
    Flags(schema, app,
          {"-g", "--skip-version-dirs", "--cache", "--du", "--du-si", "--du-h",
           "--treecount"});
  }
  schema.argument(app, "path", {}, Path::EosOrFile);
}

void
ConfigureCopy(Schema& schema)
{
  auto& app = *schema.app;
  Values(schema, app, {"--rate", "--streams", "--depth"});
  Flags(schema, app,
        {"--atomic", "--checksum", "-a,--append", "-p", "-n", "-S", "-d,--debug",
         "-s,--silent", "-k,--no-overwrite", "-P,--preserve", "-r,-R,--recursive"});
  schema.argument(app, "paths", {}, Path::Any, true);
}

void
ConfigureSquash(Schema& schema)
{
  auto& app = *schema.app;
  for (const auto* name :
       {"new", "pack", "unpack", "info", "rm", "relabel", "install", "new-release",
        "pack-release", "info-release", "trim-release", "rm-release"}) {
    auto& command = schema.sub(app, name);
    const std::string operation(name);
    // Package labels are local symlinks and can be dangling while packed.
    const bool directory = operation == "new" || operation == "install" ||
                           operation.find("-release") != std::string::npos;
    schema.argument(command, "path", {}, directory ? Path::LocalDirectory : Path::Local);
    if (operation == "pack" || operation == "unpack") {
      schema.flag(command, "-f");
    } else if (operation == "install") {
      auto* curl = schema.value(command, "--curl");
      schema.spellings[curl] = {"--curl="};
    } else if (operation == "new-release") {
      schema.argument(command, "version");
    } else if (operation == "trim-release") {
      schema.argument(command, "keep-days");
      schema.argument(command, "keep-versions");
    }
  }
}
} // namespace

bool
ConfigureStorageCompletion(CommandCompletionSchema& schema, const std::string& name)
{
  auto& app = *schema.app;
  if (name == "file") {
    ConfigureFile(schema);
  } else if (name == "fs") {
    ConfigureFs(schema);
  } else if (name == "io") {
    ConfigureIo(schema);
  } else if (name == "space") {
    ConfigureSpace(schema);
  } else if (name == "quota") {
    ConfigureQuota(schema);
  } else if (name == "find" || name == "oldfind") {
    ConfigureFind(schema, name == "oldfind");
  } else if (name == "cp" || name == "cat") {
    ConfigureCopy(schema);
  } else if (name == "rclone") {
    for (const auto* operation : {"copy", "sync"}) {
      auto& command = schema.sub(app, operation);
      schema.argument(command, "source", {}, Path::AnyDirectory);
      schema.argument(command, "destination", {}, Path::AnyDirectory);
      Flags(schema, command,
            {"--delete", "--noreplace", "--dryrun", "--atomic", "--versions", "--hidden",
             "-v,--verbose", "-s,--silent"});
    }
  } else if (name == "squash") {
    ConfigureSquash(schema);
  } else if (name == "role") {
    schema.argument(app, "user-role");
    schema.argument(app, "group-role");
  } else if (name == "stat") {
    schema.flag(app, "-f")->excludes(schema.flag(app, "-d"));
    schema.argument(app, "path", {}, Path::Eos);
  } else if (name == "mkdir" || name == "rmdir") {
    if (name == "mkdir") {
      schema.flag(app, "-p");
    }
    schema.argument(app, "path", {}, Path::EosDirectory);
  } else if (name == "ln") {
    schema.flag(app, "-f,--force");
    schema.argument(app, "link", {}, Path::Eos);
    schema.argument(app, "target", {}, Path::Eos);
  } else if (name == "version") {
    Flags(schema, app, {"-f,--features", "-m,--monitoring"});
  } else if (name == "who") {
    Flags(schema, app, {"-c", "-n", "-z", "-a", "-s", "-m"});
  } else if (name == "reconnect") {
    schema.argument(app, "protocol", {"gsi", "krb5", "unix", "sss"});
  } else if (name == "status" || name == "pwd" || name == "license") {
    // These commands ignore argv and do not implement command-level help flags.
    app.set_help_flag("");
  } else if (name != "whoami" && name != "motd") {
    return false;
  }
  return true;
}
