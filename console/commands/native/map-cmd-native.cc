// ----------------------------------------------------------------------
// File: map-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeMapHelp()
{
  return "Usage: map [OPTIONS] ls|link|unlink ...\n\n"
         "'[eos] map ..' provides a namespace mapping interface for "
         "directories in EOS.\n\n"
         "Subcommands:\n"
         "  ls                                    list all defined mappings\n"
         "  link <source-path> <destination-path> create a symbolic link from "
         "source to destination\n"
         "  unlink <source-path>                   remove symbolic link from "
         "source\n\n"
         "Options:\n"
         "  -<option>  optional leading option passed to mgm.option\n";
}

void ConfigureMapApp(CLI::App& app,
                     std::string& subcmd,
                     std::string& arg1,
                     std::string& arg2)
{
  app.name("map");
  app.description("Mapping utilities");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeMapHelp();
      }));
  app.add_option("subcmd", subcmd, "ls|link|unlink")->required();
  app.add_option("arg1", arg1, "source path or first arg");
  app.add_option("arg2", arg2, "destination path (for link)");
}

class MapCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "map";
  }
  const char*
  description() const override
  {
    return "Mapping utilities";
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
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::vector<std::string> parse_args = args;
    std::string leading_opt;
    if (!parse_args.empty() && parse_args[0].size() > 1 &&
        parse_args[0][0] == '-') {
      leading_opt = parse_args[0].substr(1);
      parse_args.erase(parse_args.begin());
    }

    if (parse_args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    std::string subcmd;
    std::string arg1;
    std::string arg2;
    ConfigureMapApp(app, subcmd, arg1, arg2);

    std::vector<std::string> cli_args = parse_args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=map";

    if (!leading_opt.empty()) {
      in += "&mgm.option=";
      in += leading_opt.c_str();
    }

    if (subcmd == "ls") {
      in += "&mgm.subcmd=ls";
    } else if (subcmd == "link") {
      if (arg1.empty() || arg2.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=link&mgm.map.src=";
      in += arg1.c_str();
      in += "&mgm.map.dest=";
      in += arg2.c_str();
    } else if (subcmd == "unlink") {
      if (arg1.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=unlink&mgm.map.src=";
      in += arg1.c_str();
    } else {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeMapHelp().c_str());
  }
};
} // namespace

void
RegisterMapNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MapCommand>());
}
