// ----------------------------------------------------------------------
// File: chown-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeChownHelp()
{
  std::ostringstream oss;
  oss << "Usage: chown [-r] [-h|--nodereference] <owner>[:<group>] <path>\n";
  oss << "       chown [-r] :<group> <path>\n";
  oss << "'[eos] chown ..' provides the change owner interface of EOS.\n";
  oss << "<path> is the file/directory to modify, <owner> has to be a user id "
         "or user name. <group> is optional and has to be a group id or group "
         "name.\n";
  oss << "To modify only the group use :<group> as identifier!\n";
  oss << "Remark: if you use the -r -h option and path points to a link the "
         "owner of the link parent will also be updated!\n";
  oss << "Options:\n";
  oss << "  -r                    recursive\n";
  oss << "  -h, --nodereference   don't follow symlinks\n";
  return oss.str();
}

void ConfigureChownApp(CLI::App& app,
                      bool& opt_r,
                      bool& opt_h,
                      std::string& owner,
                      std::string& path)
{
  app.name("chown");
  app.description("change owner interface of EOS");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeChownHelp();
      }));
  app.add_flag("-r", opt_r, "recursive");
  app.add_flag("-h,--nodereference", opt_h, "don't follow symlinks");
  app.add_option("owner", owner, "owner[:group] or :group")->required();
  app.add_option("path", path, "path")->required();
}

class ChownCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "chown";
  }
  const char*
  description() const override
  {
    return "Chown Interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str(), true);
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str(), true)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    bool opt_r = false;
    bool opt_h = false;
    std::string owner;
    std::string path;
    ConfigureChownApp(app, opt_r, opt_h, owner, path);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::string opt;
    if (opt_r)
      opt += 'r';
    if (opt_h)
      opt += 'h';

    XrdOucString in = "mgm.cmd=chown";
    if (!opt.empty()) {
      in += "&mgm.chown.option=";
      in += opt.c_str();
    }
    XrdOucString ap = abspath(path.c_str());
    in += "&mgm.path=";
    in += ap;
    in += "&mgm.chown.owner=";
    in += owner.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    bool opt_r = false;
    bool opt_h = false;
    std::string owner;
    std::string path;
    ConfigureChownApp(app, opt_r, opt_h, owner, path);
    const std::string help = app.help();
    fprintf(stderr, "%s", help.c_str());
  }
};
} // namespace

void
RegisterChownNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChownCommand>());
}
