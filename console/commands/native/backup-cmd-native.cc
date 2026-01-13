// ----------------------------------------------------------------------
// File: backup-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>
#include <sys/time.h>

namespace {
class BackupCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "backup";
  }
  const char*
  description() const override
  {
    return "Backup Interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.size() < 2) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    const std::string& src = args[0];
    const std::string& dst = args[1];
    std::ostringstream in_cmd;
    in_cmd << "mgm.cmd=backup&mgm.backup.src=" << src
           << "&mgm.backup.dst=" << dst;
    // parse optional flags
    ConsoleArgParser p;
    p.addOption({"ctime", '\0', true, false, "<val>", "ctime window", ""});
    p.addOption({"mtime", '\0', true, false, "<val>", "mtime window", ""});
    p.addOption(
        {"excl_xattr", '\0', true, false, "<list>", "exclude xattrs", ""});
    std::vector<std::string> rest;
    if (args.size() > 2)
      rest.assign(args.begin() + 2, args.end());
    auto r = p.parse(rest);
    auto append_window = [&](const char* key) {
      std::string val = r.value(key, "");
      if (val.empty())
        return;
      char last = val.back();
      long seconds = 0;
      if (last == 's')
        seconds = 1;
      else if (last == 'm')
        seconds = 60;
      else if (last == 'h')
        seconds = 3600;
      else if (last == 'd')
        seconds = 24 * 3600;
      else {
        printHelp();
        global_retc = EINVAL;
        return;
      }
      long v = strtol(val.c_str(), nullptr, 10);
      if (v == 0L) {
        printHelp();
        global_retc = EINVAL;
        return;
      }
      struct timeval tv;
      if (gettimeofday(&tv, NULL)) {
        fprintf(stderr, "Error getting current timestamp\n");
        global_retc = EINVAL;
        return;
      }
      in_cmd << "&mgm.backup.ttime=" << key
             << "&mgm.backup.vtime=" << (tv.tv_sec - v * seconds);
    };
    if (r.has("ctime"))
      append_window("ctime");
    if (r.has("mtime"))
      append_window("mtime");
    if (r.has("excl_xattr")) {
      in_cmd << "&mgm.backup.excl_xattr=" << r.value("excl_xattr");
    }
    XrdOucString in = in_cmd.str().c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    std::ostringstream oss;
    oss << "Usage: backup <src_url> <dst_url> [options] " << std::endl
        << " " << std::endl
        << " optional arguments: " << std::endl
        << " --ctime|mtime <val>s|m|h|d use the specified timewindow to select "
           "entries for backup"
        << std::endl
        << " --excl_xattr val_1[,val_2]...[,val_n] extended attributes which "
           "are not enforced and"
        << std::endl
        << "              also not checked during the verification step"
        << std::endl;
    fprintf(stderr, "%s", oss.str().c_str());
  }
};
} // namespace

void
RegisterBackupNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<BackupCommand>());
}
