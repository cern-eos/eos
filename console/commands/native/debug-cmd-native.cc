// ----------------------------------------------------------------------
// File: debug-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeDebugHelp()
{
  std::ostringstream oss;
  oss << "Usage: debug get|this|<level> [node-queue] [--filter <unitlist>]\n"
      << "'[eos] debug ...' allows to get or set the verbosity of the EOS "
         "log files in MGM and FST services.\n\n"
      << "debug get : retrieve the current log level for the mgm and fsts "
         "node-queue\n\n"
      << "debug this : toggle EOS shell debug mode\n\n"
      << "debug  <level> [--filter <unitlist>] : set the MGM where the "
         "console is connected to into debug level <level>\n\n"
      << "debug  <level> <node-queue> [--filter <unitlist>] : set the "
         "<node-queue> into debug level <level>.\n"
      << "  - <node-queue> are internal EOS names e.g. "
         "'/eos/<hostname>:<port>/fst'\n"
      << "  - <unitlist> is a comma separated list of strings of software "
         "units which should be filtered out in the message log!\n\n"
      << "The allowed debug levels are: "
         "debug,info,warning,notice,err,crit,alert,emerg\n";
  return oss.str();
}

void ConfigureDebugApp(CLI::App& app,
                      std::string& mode,
                      std::string& node,
                      std::string& filter)
{
  app.name("debug");
  app.description("Set debug level");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeDebugHelp();
      }));
  app.add_option("mode", mode, "get|this|<level>")->required();
  app.add_option("node", node, "node-queue (e.g. /eos/host:port/fst)");
  app.add_option("--filter", filter, "comma-separated unit list to filter");
}

class DebugCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "debug";
  }
  const char*
  description() const override
  {
    return "Set debug level";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    std::string mode;
    std::string node;
    std::string filter;
    ConfigureDebugApp(app, mode, node, filter);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::string cmd;
    if (mode == "get" || mode == "this") {
      cmd = mode;
    } else {
      cmd = mode;
      if (!node.empty())
        cmd += " " + node;
      if (!filter.empty())
        cmd += " --filter " + filter;
    }

    class LocalHelper : public ICmdHelper {
    public:
      using ICmdHelper::ICmdHelper;
      bool
      ParseCommand(const char* arg) override
      {
        eos::console::DebugProto* debugproto = mReq.mutable_debug();
        eos::common::StringTokenizer tokenizer(arg);
        tokenizer.GetLine();
        std::string token;
        if (!tokenizer.NextToken(token)) {
          return false;
        }
        if (token == "get") {
          auto* get = debugproto->mutable_get();
          get->set_placeholder(true);
        } else if (token == "this") {
          global_debug = !global_debug;
          gGlobalOpts.mDebug = global_debug;
          fprintf(stdout, "info: toggling shell debugmode to debug=%d\n",
                  global_debug);
          mIsLocal = true;
        } else {
          auto* set = debugproto->mutable_set();
          set->set_debuglevel(token);
          if (tokenizer.NextToken(token)) {
            if (token == "--filter") {
              if (!tokenizer.NextToken(token))
                return false;
              set->set_filter(token);
            } else {
              set->set_nodename(token);
              if (tokenizer.NextToken(token)) {
                if (token != "--filter")
                  return false;
                else {
                  if (!tokenizer.NextToken(token))
                    return false;
                  set->set_filter(token);
                }
              }
            }
          }
        }
        return true;
      }
    };

    LocalHelper helper(gGlobalOpts);
    if (!helper.ParseCommand(cmd.c_str())) {
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
    fprintf(stderr, "%s", MakeDebugHelp().c_str());
  }
};
} // namespace

void
RegisterDebugNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DebugCommand>());
}
