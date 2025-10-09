// ----------------------------------------------------------------------
// File: BasicWrappersNativeCommands.cc
// Purpose: Register thin native wrappers for legacy com_* commands
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

// Legacy symbols we wrap
extern int com_access(char*);
extern int com_accounting(char*);
extern int com_archive(char*);
extern int com_attr(char*);
extern int com_backup(char*);
extern int com_chmod(char*);
extern int com_chown(char*);
extern int com_clear(char*);
extern int com_console(char*);
extern int com_cp(char*);
extern int com_daemon(char*);
extern int com_debug(char*);
extern int com_du(char*);
extern int com_evict(char*);
extern int com_file(char*);
extern int com_fuse(char*);
extern int com_fusex(char*);
extern int com_geosched(char*);
extern int com_group(char*);
extern int com_health(char*);
extern int com_inspector(char*);
extern int com_license(char*);
extern int com_ln(char*);
extern int com_map(char*);
extern int com_member(char*);
extern int com_motd(char*);
extern int com_mv(char*);
extern int com_old_find(char*);
extern int com_print(char*);
extern int com_proto_access(char*);
extern int com_proto_acl(char*);
extern int com_proto_config(char*);
extern int com_proto_convert(char*);
extern int com_proto_debug(char*);
extern int com_proto_devices(char*);
extern int com_proto_df(char*);
extern int com_proto_find(char*);
extern int com_proto_fs(char*);
extern int com_proto_fsck(char*);
extern int com_proto_group(char*);
extern int com_proto_io(char*);
extern int com_proto_node(char*);
extern int com_proto_ns(char*);
extern int com_proto_qos(char*);
extern int com_protoquota(char*);
extern int com_proto_recycle(char*);
extern int com_proto_register(char*);
extern int com_proto_rm(char*);
extern int com_proto_route(char*);
extern int com_rclone(char*);
extern int com_rclone2(char*);
extern int com_rclone3(char*);
extern int com_rclone4(char*);
extern int com_rclone5(char*);
extern int com_reconnect(char*);
extern int com_report(char*);
extern int com_rm(char*);
extern int com_rmdir(char*);
extern int com_role(char*);
extern int com_rtlog(char*);
extern int com_scitoken(char*);
extern int com_squash(char*);
extern int com_test(char*);
extern int com_touch(char*);
extern int com_tracker(char*);
extern int com_vid(char*);
extern int com_who(char*);
extern int com_whoami(char*);
// (duplicates removed)

namespace {
class LegacyWrapperCommand : public IConsoleCommand {
public:
  using CFunc = int(*)(char*);
  LegacyWrapperCommand(const char* n, const char* d, CFunc f, bool req)
    : mName(n), mDesc(d?d:""), mFunc(f), mRequiresMgm(req) {}
  const char* name() const override { return mName.c_str(); }
  const char* description() const override { return mDesc.c_str(); }
  bool requiresMgm(const std::string& args) const override { return mRequiresMgm && !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str();
    return mFunc((char*)joined.c_str());
  }
  void printHelp() const override {}
private:
  std::string mName; std::string mDesc; CFunc mFunc; bool mRequiresMgm;
};

static const char* docFor(const char* nm)
{
  for (int i = 0; commands[i].name; ++i) if (std::string(nm) == commands[i].name) return commands[i].doc ? commands[i].doc : "";
  return "";
}

static bool needsMgm(const std::string& nm)
{
  return !(nm == "clear" || nm == "console" || nm == "cp" ||
           nm == "exit" || nm == "help" || nm == "json" ||
           nm == "pwd" || nm == "quit" || nm == "role" ||
           nm == "silent" || nm == "timing" || nm == "?" ||
           nm == ".q" || nm == "daemon" || nm == "scitoken");
}
}

// Resolve a legacy function pointer from the static commands[] table if present
using LegacyCFunc = int (*)(char*);
static LegacyCFunc getFunc(const char* nm)
{
  for (int i = 0; commands[i].name; ++i) {
    if (std::string(nm) == commands[i].name) return commands[i].func;
  }
  return nullptr;
}

void RegisterBasicWrappersNativeCommands()
{
  // High-level file ops
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("mv",      docFor("mv"),      &com_mv,           needsMgm("mv")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("ln",      docFor("ln"),      &com_ln,           needsMgm("ln")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("cp",      docFor("cp"),      &com_cp,           needsMgm("cp")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("rmdir",   docFor("rmdir"),   &com_rmdir,        needsMgm("rmdir")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("touch",   docFor("touch"),   &com_touch,        needsMgm("touch")));
  // 'cat' command wrapper omitted (no legacy symbol available in this tree)

  // Identity / info
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("whoami",  docFor("whoami"),  &com_whoami,       needsMgm("whoami")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("who",     docFor("who"),     &com_who,          needsMgm("who")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("vid",     docFor("vid"),     &com_vid,          needsMgm("vid")));

  // Find/report/quota
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("find",    docFor("find"),    &com_proto_find,   needsMgm("find")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("report",  docFor("report"),  &com_report,       needsMgm("report")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("quota",   docFor("quota"),   &com_protoquota,   needsMgm("quota")));

  // Mapping/print
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("file",    docFor("file"),    &com_file,         needsMgm("file")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("map",     docFor("map"),     &com_map,          needsMgm("map")));
  if (auto* f = getFunc("print")) CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("print",   docFor("print"),   f,        needsMgm("print")));

  // Admin / proto
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("access",  docFor("access"),  &com_access,       needsMgm("access")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("accounting", docFor("accounting"), &com_accounting, needsMgm("accounting")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("archive", docFor("archive"), &com_archive,      needsMgm("archive")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("attr",    docFor("attr"),    &com_attr,         needsMgm("attr")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("backup",  docFor("backup"),  &com_backup,       needsMgm("backup")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("chmod",   docFor("chmod"),   &com_chmod,        needsMgm("chmod")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("chown",   docFor("chown"),   &com_chown,        needsMgm("chown")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("console", docFor("console"), &com_console,      needsMgm("console")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("daemon",  docFor("daemon"),  &com_daemon,       needsMgm("daemon")));
  if (auto* f = getFunc("debug")) CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("debug",   docFor("debug"),   f,        needsMgm("debug")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("du",      docFor("du"),      &com_du,           needsMgm("du")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("evict",   docFor("evict"),   &com_evict,        needsMgm("evict")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("fuse",    docFor("fuse"),    &com_fuse,         needsMgm("fuse")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("fusex",   docFor("fusex"),   &com_fusex,        needsMgm("fusex")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("geosched",docFor("geosched"),&com_geosched,     needsMgm("geosched")));
  if (auto* f = getFunc("group")) CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("group",   docFor("group"),   f,        needsMgm("group")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("health",  docFor("health"),  &com_health,       needsMgm("health")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("inspector", docFor("inspector"), &com_inspector, needsMgm("inspector")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("license", docFor("license"), &com_license,      needsMgm("license")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("member",  docFor("member"),  &com_member,       needsMgm("member")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("motd",    docFor("motd"),    &com_motd,         needsMgm("motd")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("oldfind", docFor("oldfind"), &com_old_find,     needsMgm("oldfind")));
  for (const char* nm : {"proto_access","proto_acl","proto_config","proto_convert","proto_debug","proto_devices","proto_df","proto_fs","proto_fsck","proto_group","proto_io","proto_node","proto_ns","proto_qos","proto_recycle","proto_register","proto_rm","proto_route"}) {
    if (auto* f = getFunc(nm)) CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>(nm, docFor(nm), f, needsMgm(nm)));
  }

  // Misc
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("reconnect",docFor("reconnect"), &com_reconnect, needsMgm("reconnect")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("role",     docFor("role"),      &com_role,      needsMgm("role")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("rtlog",    docFor("rtlog"),     &com_rtlog,     needsMgm("rtlog")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("scitoken", docFor("scitoken"),  &com_scitoken,  needsMgm("scitoken")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("squash",   docFor("squash"),    &com_squash,    needsMgm("squash")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("test",     docFor("test"),      &com_test,      needsMgm("test")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("tracker",  docFor("tracker"),   &com_tracker,   needsMgm("tracker")));
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("rclone",   docFor("rclone"),    &com_rclone,    needsMgm("rclone")));
  for (const char* nm : {"rclone2","rclone3","rclone4","rclone5"}) {
    if (auto* f = getFunc(nm)) CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>(nm, docFor(nm), f, needsMgm(nm)));
  }
  CommandRegistry::instance().reg(std::make_unique<LegacyWrapperCommand>("clear",    docFor("clear"),     &com_clear,     needsMgm("clear")));
}