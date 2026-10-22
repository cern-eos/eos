#include "console/CommandCompletion.hh"

#include <CLI/CLI.hpp>

namespace {
void
Flags(CommandCompletionSchema& schema, CLI::App& app,
      std::initializer_list<const char*> names)
{
  for (const auto* name : names) {
    schema.flag(app, name);
  }
}

void
NamespaceCompletion(CommandCompletionSchema& schema, CLI::App& app)
{
  auto& stat = schema.sub(app, "stat");
  Flags(schema, stat, {"-a", "-x", "-m", "-n", "--reset"});
  auto& mutex = schema.sub(app, "mutex");
  Flags(schema, mutex,
        {"--toggletime", "--toggleorder", "--toggledeadlock", "--smplrate1",
         "--smplrate10", "--smplrate100"});
  schema.value(mutex, "--setblockedtime");
  auto& compact = schema.sub(app, "compact");
  schema.sub(compact, "off");
  auto& on = schema.sub(compact, "on");
  schema.argument(on, "delay");
  schema.argument(on, "interval");
  schema.argument(on, "type",
                  {"files", "directories", "all", "files-repair", "directories-repair",
                   "all-repair"});
  auto& master = schema.sub(app, "master");
  Flags(schema, master, {"--log", "--log-clear", "--enable", "--disable"});
  schema.argument(master, "hostname");
  auto& tree = schema.sub(app, "recompute_tree_size");
  schema.value(tree, "--depth");
  schema.argument(tree, "container", {"cid:", "cxid:"}, CompletionPath::EosDirectory);
  auto& quota = schema.sub(app, "recompute_quotanode");
  schema.argument(quota, "container", {"cid:", "cxid:"}, CompletionPath::EosDirectory);
  auto& update = schema.sub(app, "update_quotanode");
  schema.argument(update, "container", {"cid:", "cxid:"}, CompletionPath::EosDirectory);
  schema.argument(update, "accounting",
                  {"uid:", "gid:", "bytes:", "physicalbytes:", "inodes:"},
                  CompletionPath::None, true);
  auto& cache = schema.sub(app, "cache");
  auto& set = schema.sub(cache, "set");
  Flags(schema, set, {"-f", "-d"});
  schema.argument(set, "max_num");
  schema.argument(set, "max_size");
  auto& drop = schema.sub(cache, "drop");
  Flags(schema, drop, {"-f", "-d"});
  schema.argument(schema.sub(cache, "drop-single-file"), "file_id");
  schema.argument(schema.sub(cache, "drop-single-container"), "container_id");
  auto& drain = schema.sub(app, "drain");
  schema.sub(drain, "list");
  schema.argument(schema.sub(drain, "set"), "setting",
                  {"max-thread-pool-size=", "max-fs-per-node="});
  auto& reserve = schema.sub(app, "reserve-ids");
  schema.argument(reserve, "file_id");
  schema.argument(reserve, "container_id");
  auto& benchmark = schema.sub(app, "benchmark");
  schema.argument(benchmark, "threads");
  schema.argument(benchmark, "subdirs");
  schema.argument(benchmark, "subfiles");
  schema.argument(benchmark, "prefix", {}, CompletionPath::EosDirectory);
  auto& tracker = schema.sub(app, "tracker");
  const std::vector<std::string> trackers = {"drain", "balance", "fsck", "convert",
                                             "all"};
  schema.value(tracker, "--name", trackers);
  for (const auto* operation : {"list", "clear"}) {
    schema.value(schema.sub(tracker, operation), "--name", trackers);
  }
  auto& behaviour = schema.sub(app, "behaviour");
  schema.sub(behaviour, "list");
  const std::vector<std::string> behaviours = {"rain_min_fsid_entry",
                                               "disable_access_log"};
  auto& behaviour_set = schema.sub(behaviour, "set");
  schema.argument(behaviour_set, "name", behaviours);
  schema.argument(behaviour_set, "value", {"on", "off"});
  schema.argument(schema.sub(behaviour, "get"), "name", behaviours);
  schema.argument(schema.sub(behaviour, "clear"), "name",
                  {"rain_min_fsid_entry", "disable_access_log", "all"});
}

void
SchedulerCompletion(CommandCompletionSchema& schema, CLI::App& app)
{
  for (const auto* spelling : {"configure", "config"}) {
    auto& configure = schema.sub(app, spelling);
    schema.argument(schema.sub(configure, "type"), "type",
                    {"geotree", "flat:geo", "flat:roundrobin", "flat:random",
                     "flat:fidrandom", "flat:weightedrandom", "flat:weightedroundrobin",
                     "flat:weightedrr"});
    auto& weight = schema.sub(configure, "weight");
    schema.argument(weight, "space");
    schema.argument(weight, "fsid");
    schema.argument(weight, "weight");
    schema.sub(configure, "forcerefresh");
    auto& show = schema.sub(configure, "show");
    schema.argument(schema.sub(show, "type"), "space");
    schema.argument(schema.sub(show, "state"), "space");
  }
  auto& show = schema.sub(app, "show");
  schema.argument(schema.sub(show, "type"), "space");
  schema.argument(schema.sub(show, "state"), "space");
  for (const auto* spelling : {"disable", "disabled"}) {
    auto& disabled = schema.sub(app, spelling);
    for (const auto* operation : {"add", "rm"}) {
      auto& change = schema.sub(disabled, operation);
      schema.argument(change, "space");
      schema.argument(change, "geotag");
      // Explicit client:/internal: masks remain freeform and are validated by MGM.
      schema.argument(
          change, "spec",
          {"plct", "access", "all", "client", "internal", "client:", "internal:"});
    }
    schema.argument(schema.sub(disabled, "ls"), "space");
  }
  auto& ls = schema.sub(app, "ls");
  schema.argument(ls, "space");
  schema.argument(ls, "view", {"bucket", "disk", "tree", "all"});
}

void
GeoschedulerCompletion(CommandCompletionSchema& schema, CLI::App& app)
{
  auto& show = schema.sub(app, "show");
  Flags(schema, show, {"-c", "-m"});
  schema.argument(schema.sub(show, "tree"), "group");
  auto& snapshot = schema.sub(show, "snapshot");
  schema.argument(snapshot, "group");
  // Operation and group names are configured by the instance.
  schema.argument(snapshot, "optype");
  schema.sub(show, "param");
  schema.flag(schema.sub(show, "state"), "-m");
  auto& set = schema.sub(app, "set");
  schema.argument(set, "parameter",
                  {"skipSaturatedAccess", "skipSaturatedDrnAccess",
                   "skipSaturatedBlcAccess", "plctDlScorePenalty", "plctUlScorePenalty",
                   "accessDlScorePenalty", "accessUlScorePenalty", "fillRatioLimit",
                   "fillRatioCompTol", "saturationThres", "timeFrameDurationMs",
                   "penaltyUpdateRate", "proxyCloseToFs"});
  schema.argument(set, "index_or_value");
  schema.argument(set, "value");
  auto& updater = schema.sub(app, "updater");
  schema.sub(updater, "pause");
  schema.sub(updater, "resume");
  schema.sub(app, "forcerefresh");
  auto& disabled = schema.sub(app, "disabled");
  for (const auto* operation : {"add", "rm", "show"}) {
    auto& change = schema.sub(disabled, operation);
    schema.argument(change, "geotag");
    schema.argument(change, "optype");
    schema.argument(change, "group");
  }
  auto& access = schema.sub(app, "access");
  auto& direct = schema.sub(access, "setdirect");
  schema.argument(direct, "geotag");
  schema.argument(direct, "geotag_list");
  schema.argument(schema.sub(access, "cleardirect"), "geotag");
  schema.flag(schema.sub(access, "showdirect"), "-m");
}

void
VidCompletion(CommandCompletionSchema& schema, CLI::App& app)
{
  Flags(schema, schema.sub(app, "ls"),
        {"-u", "-g", "-s", "-U", "-G", "-y", "-a", "-N", "-l", "-n"});
  auto& set = schema.sub(app, "set");
  auto& geotag = schema.sub(set, "geotag");
  schema.argument(geotag, "match", {"default"});
  schema.argument(geotag, "geotag");
  auto& membership = schema.sub(set, "membership");
  schema.argument(membership, "uid");
  schema.argument(membership, "operation", {"-uids", "-gids", "+sudo", "-sudo"});
  schema.argument(membership, "ids");
  auto& map = schema.sub(set, "map");
  schema.argument(map, "authentication",
                  {"-krb5", "-gsi", "-https", "-sss", "-unix", "-tident", "-voms",
                   "-grpc", "-oauth2"});
  // Authentication match patterns and virtual IDs are instance-specific.
  schema.argument(map, "pattern");
  schema.argument(map, "virtual_id", {"vuid:", "vgid:"});
  schema.argument(map, "group_id", {"vgid:"});
  for (const auto* operation : {"enable", "disable"}) {
    schema.argument(
        schema.sub(app, operation), "authentication",
        {"krb5", "gsi", "sss", "unix", "https", "grpc", "oauth2", "tident", "ztn"});
  }
  for (const auto* operation : {"add", "remove"}) {
    auto& gateway = schema.sub(schema.sub(app, operation), "gateway");
    schema.argument(gateway, "hostname");
    schema.argument(
        gateway, "protocol",
        {"krb5", "gsi", "sss", "unix", "https", "grpc", "oauth2", "ztn", "*"});
  }
  schema.argument(schema.sub(app, "publicaccesslevel"), "level");
  schema.argument(schema.sub(app, "tokensudo"), "level", {"0", "1", "2", "3"});
  auto& rm = schema.sub(app, "rm");
  schema.argument(rm, "key");
  schema.argument(schema.sub(rm, "membership"), "uid");
}
} // namespace

bool
ConfigureAdminCompletion(CommandCompletionSchema& schema, const std::string& name)
{
  auto& app = *schema.app;
  if (name == "ns") {
    NamespaceCompletion(schema, app);
  } else if (name == "sched") {
    SchedulerCompletion(schema, app);
  } else if (name == "geosched") {
    GeoschedulerCompletion(schema, app);
  } else if (name == "vid") {
    VidCompletion(schema, app);
  } else if (name == "access") {
    for (const auto* operation : {"ban", "unban", "allow", "unallow"}) {
      auto& change = schema.sub(app, operation);
      for (const auto* type : {"user", "group", "host", "domain"}) {
        schema.argument(schema.sub(change, type), "identifier");
      }
    }
    Flags(schema, schema.sub(app, "ls"), {"-m", "-n"});
    for (const auto* operation : {"set", "rm"}) {
      auto& change = schema.sub(app, operation);
      for (const auto* type : {"redirect", "stall"}) {
        auto& rule = schema.sub(change, type);
        if (std::string(operation) == "set") {
          schema.argument(rule, "target_or_seconds");
        }
        schema.argument(rule, "request_type", {"r", "w", "ENOENT", "ENONET"});
      }
      auto& limit = schema.sub(change, "limit");
      if (std::string(operation) == "set") {
        schema.argument(limit, "frequency");
      }
      schema.argument(limit, "counter", {"rate:user:", "rate:group:"});
    }
  } else if (name == "accounting") {
    schema.flag(schema.sub(app, "report"), "-f");
    auto& config = schema.sub(app, "config");
    schema.value(config, "-e");
    schema.value(config, "-i");
  } else if (name == "acl") {
    Flags(schema, app, {"-l,--list", "-R,--recursive", "-f,--front", "--sys", "--user"});
    schema.value(app, "-p,--position");
    // Listing accepts a path first; modification accepts a freeform ACL rule first.
    schema.argument(app, "rule_or_identifier", {}, CompletionPath::EosDirectory);
    schema.argument(app, "identifier", {}, CompletionPath::EosDirectory);
  } else if (name == "attr") {
    Flags(schema, app, {"-r", "-V"});
    for (const auto* operation : {"ls", "unlink", "fold"}) {
      schema.argument(schema.sub(app, operation), "identifier", {}, CompletionPath::Eos);
    }
    const std::vector<std::string> keys = {"sys.acl",
                                           "user.acl",
                                           "sys.attr.link",
                                           "sys.forced.space",
                                           "sys.forced.cachespace",
                                           "sys.forced.group",
                                           "sys.forced.layout",
                                           "sys.forced.checksum",
                                           "sys.forced.blockchecksum",
                                           "sys.forced.nstripes",
                                           "sys.forced.blocksize",
                                           "sys.forced.placementpolicy",
                                           "sys.forced.nouserlayout",
                                           "sys.forced.atomic",
                                           "sys.forced.iotype",
                                           "sys.recycle",
                                           "sys.versioning",
                                           "sys.mask",
                                           "sys.eval.useracl",
                                           "sys.owner.auth",
                                           "sys.http.index"};
    for (const auto* operation : {"get", "rm"}) {
      auto& command = schema.sub(app, operation);
      schema.argument(command, "key", keys);
      schema.argument(command, "identifier", {}, CompletionPath::Eos);
    }
    auto& set = schema.sub(app, "set");
    schema.flag(set, "-c");
    std::vector<std::string> assignments = {"default=replica", "default=raiddp",
                                            "default=raid5",   "default=raid6",
                                            "default=archive", "default=qrain"};
    for (const auto& key : keys) {
      assignments.push_back(key + "=");
    }
    // Extended attribute names and values are intentionally open-ended.
    schema.argument(set, "assignment", assignments);
    schema.argument(set, "identifier", {}, CompletionPath::Eos);
    auto& link = schema.sub(app, "link");
    schema.argument(link, "origin", {}, CompletionPath::EosDirectory);
    schema.argument(link, "identifier", {}, CompletionPath::Eos);
  } else if (name == "archive") {
    for (const auto* operation : {"create", "put", "get", "purge", "delete", "list"}) {
      auto& command = schema.sub(app, operation);
      if (std::string(operation) != "create" && std::string(operation) != "list") {
        schema.flag(command, "--retry");
      }
      schema.argument(command, "path", {}, CompletionPath::EosDirectory);
    }
    schema.argument(schema.sub(app, "transfers"), "operation_or_uuid",
                    {"all", "put", "get", "purge"});
    schema.argument(schema.sub(app, "kill"), "job_uuid");
    schema.sub(app, "help");
  } else if (name == "backup") {
    schema.argument(app, "source", {}, CompletionPath::Any);
    schema.argument(app, "destination", {}, CompletionPath::Any);
    schema.value(app, "--ctime");
    schema.value(app, "--mtime");
    schema.value(app, "--excl_xattr");
  } else if (name == "convert") {
    auto& config = schema.sub(app, "config");
    schema.sub(config, "list");
    schema.argument(
        schema.sub(config, "set"), "assignment",
        {"status=on", "status=off", "max-thread-pool-size=", "max-queue-size="});
    schema.sub(app, "list");
    schema.sub(app, "clear");
    for (const auto* operation : {"file", "rule"}) {
      auto& command = schema.sub(app, operation);
      schema.argument(command, "identifier", {},
                      std::string(operation) == "rule" ? CompletionPath::EosDirectory
                                                       : CompletionPath::Eos);
      schema.argument(command, "layout",
                      {"plain:1", "replica:2", "raiddp:6", "raid5:5", "raid6:6",
                       "archive:6", "qrain:6"});
      // Space names are configured by the instance; the parser permits arbitrary
      // ordering.
      schema.argument(command, "conversion_options",
                      {"scattered", "hybrid", "gathered", "adler", "adler32", "blake3",
                       "crc32", "crc32c", "md5", "sha", "sha1", "crc64", "sha256",
                       "sha-256", "xxhash64", "hwh64"},
                      CompletionPath::None, true);
    }
  } else if (name == "daemon") {
    schema.sub(schema.sub(app, "sss"), "recreate");
    auto& seal = schema.sub(app, "seal");
    schema.argument(seal, "text_or_file", {}, CompletionPath::Local);
    schema.argument(seal, "key");
    auto& jwk = schema.sub(app, "jwk");
    // The legacy parser consumes a service placeholder before the key filename.
    schema.argument(jwk, "service");
    schema.argument(jwk, "key_file", {}, CompletionPath::Local);
    for (const auto* operation :
         {"config", "kill", "run", "stack", "stop", "module-init"}) {
      auto& command = schema.sub(app, operation);
      for (const auto* service : {"mq", "mgm", "fst", "qdb"}) {
        auto& instance = schema.sub(command, service);
        schema.argument(instance, "name");
        if (std::string(operation) == "run") {
          // Optional keys decrypt sealed init commands; never suggest their contents.
          schema.argument(instance, "key", {}, CompletionPath::None, true);
        }
        if (std::string(operation) == "config" && std::string(service) == "qdb") {
          schema.sub(instance, "coup");
          schema.sub(instance, "info");
          schema.sub(instance, "backup");
          for (const auto* member_operation : {"remove", "add", "promote"}) {
            schema.argument(schema.sub(instance, member_operation), "host_port");
          }
          schema.argument(schema.sub(instance, "new"), "role", {"observer"});
        }
      }
    }
  } else if (name == "debug") {
    schema.sub(app, "get");
    schema.sub(app, "this");
    for (const auto* level :
         {"debug", "info", "warning", "notice", "err", "crit", "alert", "emerg"}) {
      auto& command = schema.sub(app, level);
      schema.argument(command, "node_queue");
      schema.value(command, "--filter");
    }
  } else if (name == "devices") {
    Flags(schema, schema.sub(app, "ls"), {"-l", "-m", "--refresh"});
  } else if (name == "fsck") {
    schema.flag(schema.sub(app, "stat"), "-m");
    auto& config = schema.sub(app, "config");
    for (const auto* key : {"collect", "repair", "best-effort", "show-dark-files",
                            "show-offline", "show-no-replica"}) {
      schema.argument(schema.sub(config, key), "value", {"on", "off"});
    }
    for (const auto* key :
         {"collect-interval-min", "max-queued-jobs", "max-thread-pool-size"}) {
      schema.argument(schema.sub(config, key), "value");
    }
    const std::vector<std::string> errors = {
        "m_cx_diff",  "m_mem_sz_diff", "d_cx_diff",   "d_mem_sz_diff", "unreg_n",
        "rep_diff_n", "rep_missing_n", "blockxs_err", "orphans_n",     "stripe_err"};
    auto categories = errors;
    categories.push_back("all");
    schema.argument(schema.sub(config, "repair-category"), "category", categories);
    auto& report = schema.sub(app, "report");
    Flags(schema, report, {"-a", "-i", "-l", "-j,--json"});
    schema.value(report, "--error", errors)->expected(1, -1);
    auto& repair = schema.sub(app, "repair");
    schema.value(repair, "--fxid");
    schema.value(repair, "--fsid");
    schema.value(repair, "--error", errors);
    schema.flag(repair, "--async");
    auto& clean = schema.sub(app, "clean_orphans");
    schema.value(clean, "--fsid");
    schema.flag(clean, "--force-qdb-cleanup");
  } else if (name == "fuse") {
    for (const auto* operation : {"mount", "umount"}) {
      schema.argument(schema.sub(app, operation), "mountpoint", {},
                      CompletionPath::LocalDirectory);
    }
  } else if (name == "fusex") {
    Flags(schema, schema.sub(app, "ls"), {"-l", "-f", "-k", "-m"});
    auto& evict = schema.sub(app, "evict");
    schema.argument(evict, "uuid_or_mount", {"static", "autofs"});
    schema.argument(evict, "reason",
                    {"abort", "log2big", "setlog", "stacktrace", "sendlog", "resetbuffer",
                     "mem:", "idle:"});
    auto& caps = schema.sub(app, "caps");
    Flags(schema, caps, {"-t", "-i", "-p"});
    schema.argument(caps, "regexp");
    schema.argument(schema.sub(app, "dropcaps"), "uuid");
    auto& locks = schema.sub(app, "droplocks");
    schema.argument(locks, "inode");
    schema.argument(locks, "pid");
    auto& conf = schema.sub(app, "conf");
    schema.argument(conf, "heartbeat");
    schema.argument(conf, "quota_interval");
    schema.argument(conf, "broadcast_max");
    schema.argument(conf, "broadcast_match");
  } else if (name == "health") {
    Flags(schema, app, {"-a", "-m"});
    schema.argument(app, "section", {"all", "nodes", "drain", "placement"});
  } else if (name == "inspector") {
    Flags(schema, app,
          {"-c,--current", "-l,--last", "-m", "-p", "-e", "-C,--cost", "-U,--usage",
           "-L,--layouts", "-B,--birth", "-A,--access", "-a,--all", "-V,--vs",
           "-M,--money"});
    schema.value(app, "-s,--space");
  } else if (name == "map") {
    schema.sub(app, "ls");
    auto& link = schema.sub(app, "link");
    schema.argument(link, "source", {}, CompletionPath::EosDirectory);
    schema.argument(link, "destination", {}, CompletionPath::EosDirectory);
    schema.argument(schema.sub(app, "unlink"), "source", {},
                    CompletionPath::EosDirectory);
  } else if (name == "monit") {
    schema.sub(app, "enable");
    schema.sub(app, "disable");
    auto& config = schema.sub(app, "config");
    schema.sub(config, "ls");
    auto& set = schema.sub(config, "set");
    schema.value(set, "--port");
    schema.value(set, "--cache-ttl");
  } else if (name == "node") {
    auto& ls = schema.sub(app, "ls");
    Flags(schema, ls, {"-s", "-b,--brief", "-m", "-l", "--io", "--sys", "--fsck"});
    schema.argument(ls, "selection");
    for (const auto* operation : {"rm", "status"}) {
      schema.argument(schema.sub(app, operation), "node");
    }
    auto& set = schema.sub(app, "set");
    schema.argument(set, "node");
    schema.argument(set, "state", {"on", "off"});
    auto& config = schema.sub(app, "config");
    schema.argument(config, "node");
    schema.argument(config, "assignment",
                    {"error.simulation=", "publish.interval=", "debug.level=",
                     "stripexs=on", "stripexs=off", "cbox_forbid_rw_sync=true",
                     "cbox_forbid_rw_sync=false", "cbox_forbid_rw_sync=remove"});
  } else if (name == "scitoken") {
    auto& create = schema.sub(app, "create");
    for (const auto* option : {"--issuer", "--keyid", "--expires"}) {
      schema.value(create, option);
    }
    schema.value(create, "--claim")->take_all();
    schema.value(create, "--profile", {"wlcg", "scitokens1", "scitokens2", "atjwt"});
    schema.value(create, "--privkey", {}, CompletionPath::Local);
    schema.value(create, "--pubkey", {}, CompletionPath::Local);
    schema.argument(schema.sub(app, "dump"), "token");
    schema.value(schema.sub(app, "create-keys"), "--keyid");
  } else if (name == "token") {
    for (const auto* option : {"--token", "--expires", "--owner", "--group"}) {
      schema.value(app, option);
    }
    schema.value(app, "--origin")->take_all();
    schema.value(app, "--path", {}, CompletionPath::Eos);
    schema.value(app, "--permission", {"rx", "rwx", "rwx!d", "rwxq"});
    schema.flag(app, "--tree");
  } else if (name == "tracker") {
    // The standalone tracker alias has no arguments; its space is always default.
  } else if (name == "report") {
    Flags(schema, app, {"--read", "--write", "--json", "--addvector", "-s"});
    for (const auto* option :
         {"--regex", "-n", "--max-efficiency", "--start", "--stop"}) {
      schema.value(app, option);
    }
    schema.value(app, "--squash", {}, CompletionPath::EosDirectory);
    schema.argument(app, "file", {}, CompletionPath::Local);
  } else if (name == "rtlog") {
    schema.argument(app, "queue_or_seconds", {"*", "."});
    schema.argument(
        app, "seconds_or_level",
        {"debug", "info", "warning", "notice", "err", "crit", "alert", "emerg"});
    schema.argument(
        app, "level_or_filter",
        {"debug", "info", "warning", "notice", "err", "crit", "alert", "emerg"});
    schema.argument(app, "filter");
  } else if (name == "test") {
    for (const auto* operation : {"mkdir", "rmdir", "ls", "lsla"}) {
      schema.argument(schema.sub(app, operation), "count");
    }
  } else {
    return false;
  }
  return true;
}
