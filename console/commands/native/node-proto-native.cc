// ----------------------------------------------------------------------
// File: node-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include "console/commands/helpers/NodeHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeNodeHelp()
{
  return "Usage: node ls|config|set|rm|txgw|proxygroupadd|proxygrouprm|"
         "proxygroupclear|status [OPTIONS]\n\n"
         "  ls [-s] [-b|--brief] [-m|-l|--sys|--io|--fsck] [<node>]\n"
         "    list nodes (substring match, comma-separated)\n\n"
         "  config <host:port> <key>=<value>\n"
         "    configure filesystem parameters for each filesystem of this node\n\n"
         "  set <queue-name>|<host:port> on|off\n"
         "    activate/deactivate node\n\n"
         "  rm <queue-name>|<host:port>\n"
         "    remove a node\n\n"
         "  txgw <queue-name>|<host:port> on|off\n"
         "    enable/disable node as transfer gateway\n\n"
         "  proxygroupadd|proxygrouprm <group-name> <queue-name>|<host:port>\n"
         "  proxygroupclear <queue-name>|<host:port>\n\n"
         "  status <queue-name>|<host:port>\n"
         "    print all defined variables for a node\n";
}

void ConfigureNodeApp(CLI::App& app)
{
  app.name("node");
  app.description("Node configuration");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeNodeHelp();
      }));
}

class NodeProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "node";
  }
  const char*
  description() const override
  {
    return "Node configuration";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
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
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    NodeHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureNodeApp(app);
    fprintf(stdout, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterNodeProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<NodeProtoCommand>());
}
