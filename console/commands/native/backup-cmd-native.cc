// ----------------------------------------------------------------------
// File: backup-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>
#include <sys/time.h>

namespace {
std::string MakeBackupHelp()
{
  std::ostringstream oss;
  oss << "Usage: backup <src_url> <dst_url> [options]\n\n"
      << "Options:\n"
      << "  --ctime, --mtime <val>s|m|h|d  use the specified timewindow to "
         "select entries for backup\n"
      << "  --excl_xattr val_1[,val_2]...   extended attributes which are not "
         "enforced and not checked during verification\n";
  return oss.str();
}

void ConfigureBackupApp(CLI::App& app,
                        std::string& ctime,
                        std::string& mtime,
                        std::string& excl_xattr)
{
  app.name("backup");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeBackupHelp();
      }));
  app.add_option("--ctime", ctime, "ctime window");
  app.add_option("--mtime", mtime, "mtime window");
  app.add_option("--excl_xattr", excl_xattr, "exclude xattrs");
}

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
    CLI::App app;
    app.allow_extras();
    std::string ctime;
    std::string mtime;
    std::string excl_xattr;
    ConfigureBackupApp(app, ctime, mtime, excl_xattr);
    std::vector<std::string> rest;
    if (args.size() > 2)
      rest.assign(args.begin() + 2, args.end());
    std::vector<std::string> cli_args = rest;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    auto append_window = [&](const char* key, const std::string& val) {
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
    if (!ctime.empty())
      append_window("ctime", ctime);
    if (!mtime.empty())
      append_window("mtime", mtime);
    if (!excl_xattr.empty()) {
      in_cmd << "&mgm.backup.excl_xattr=" << excl_xattr;
    }
    XrdOucString in = in_cmd.str().c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    std::string ctime;
    std::string mtime;
    std::string excl_xattr;
    ConfigureBackupApp(app, ctime, mtime, excl_xattr);
    const std::string help = app.help();
    fprintf(stderr, "%s", help.c_str());
  }
};
} // namespace

void
RegisterBackupNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<BackupCommand>());
}
