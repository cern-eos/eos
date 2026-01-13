// ----------------------------------------------------------------------
// File: accounting-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
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
      ConsoleArgParser p;
      p.addOption({"", 'f', false, false, "", "force synchronous report", ""});
      std::vector<std::string> rest(args.begin() + 1, args.end());
      auto r = p.parse(rest);
      if (r.has("f")) {
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
    fprintf(
        stdout,
        "Usage: accounting report [-f]                          : prints "
        "accounting report in JSON, data is served from cache if possible\n"
        "                                                    -f : forces a "
        "synchronous report instead of using the cache (only use this if the "
        "cached data is too old)\n"
        "       accounting config -e [<expired>] -i [<invalid>] : configure "
        "caching behaviour\n"
        "                                                    -e : expiry time "
        "in minutes, after this time frame asynchronous update happens, "
        "default is 10 minutes\n"
        "                                                    -i : invalidity "
        "time in minutes, after this time frame synchronous update happens, "
        "must be greater than expiry time, default is never\n");
  }
};
} // namespace

void
RegisterAccountingNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AccountingCommand>());
}
