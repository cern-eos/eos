// ----------------------------------------------------------------------
// File: accounting-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>

namespace {
std::string MakeAccountingHelp()
{
  std::ostringstream oss;
  oss << "Usage: accounting report [-f]\n"
      << "       accounting config -e [<expired>] -i [<invalid>]\n\n"
      << "  report  prints accounting report in JSON, data is served from "
         "cache if possible\n"
      << "          -f  force synchronous report instead of using cache\n\n"
      << "  config  configure caching behaviour\n"
      << "          -e  expiry time in minutes (default 10)\n"
      << "          -i  invalidity time in minutes, must be > expiry\n";
  return oss.str();
}

void ConfigureAccountingApp(CLI::App& app)
{
  app.name("accounting");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeAccountingHelp();
      }));
}

class AccountingCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "accounting";
  }
  const char*
  description() const override
  {
    return "Accounting tools";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    const std::string& sub = args[0];
    XrdOucString in = "mgm.cmd=accounting";
    if (sub == "report") {
      in += "&mgm.subcmd=report";
      CLI::App app;
      app.set_help_flag("");
      app.allow_extras();
      bool opt_f = false;
      app.add_flag("-f", opt_f, "force synchronous report");
      std::vector<std::string> rest(args.begin() + 1, args.end());
      std::vector<std::string> cli_args = rest;
      std::reverse(cli_args.begin(), cli_args.end());
      try {
        app.parse(cli_args);
      } catch (const CLI::ParseError&) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (opt_f) {
        in += "&mgm.option=f";
      }
    } else if (sub == "config") {
      in += "&mgm.subcmd=config";
      // -e <min> -i <min>
      std::vector<std::string> rest(args.begin() + 1, args.end());
      for (size_t i = 0; i < rest.size();) {
        const std::string& tok = rest[i];
        if (tok == "-e" || tok == "-i") {
          if (i + 1 >= rest.size()) {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
          const std::string& val = rest[i + 1];
          if (tok == "-e")
            in += "&mgm.accounting.expired=";
          else
            in += "&mgm.accounting.invalid=";
          in += val.c_str();
          i += 2;
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
      }
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
    CLI::App app;
    ConfigureAccountingApp(app);
    const std::string help = app.help();
    fprintf(stdout, "%s", help.c_str());
  }
};
} // namespace

void
RegisterAccountingNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AccountingCommand>());
}
