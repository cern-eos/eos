// ----------------------------------------------------------------------
// File: fusex-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeFusexHelp()
{
  return "Usage: fusex <subcmd> [args...]\n\n"
         "Subcommands:\n"
         "  ls                                    list active FUSEX clients\n"
         "  evict <uuid> [reason]                  evict a client by UUID "
         "(reason base64-encoded)\n"
         "  caps [all|token|lock] [filter]         show capabilities, optional "
         "filter string\n"
         "  dropcaps <uuid>                        drop capabilities for client "
         "UUID\n"
         "  droplocks <inode> <pid>                drop locks for inode and "
         "process\n"
         "  conf [hb] [qc] [bc.max] [bc.match]     configure heartbeat (hb), "
         "queue cap (qc),\n"
         "                                         block cache max and match\n";
}

void ConfigureFusexApp(CLI::App& app, std::string& subcmd)
{
  app.name("fusex");
  app.description("Fuse(x) Administration");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeFusexHelp();
      }));
  app.add_option("subcmd", subcmd,
                 "ls|evict|caps|dropcaps|droplocks|conf")
      ->required();
}

class FusexCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fusex";
  }
  const char*
  description() const override
  {
    return "Fuse(x) Administration";
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

    CLI::App app;
    std::string subcmd;
    ConfigureFusexApp(app, subcmd);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::vector<std::string> remaining = app.remaining();
    std::reverse(remaining.begin(), remaining.end());

    XrdOucString in = "mgm.cmd=fusex";
    if (subcmd == "ls") {
      in += "&mgm.subcmd=ls";
    } else if (subcmd == "evict") {
      if (remaining.size() < 1) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString uuid = remaining[0].c_str();
      in += "&mgm.subcmd=evict&mgm.fusex.uuid=";
      in += uuid;
      if (remaining.size() > 1) {
        XrdOucString reason = remaining[1].c_str();
        XrdOucString b64;
        eos::common::SymKey::Base64(reason, b64);
        in += "&mgm.fusex.reason=";
        in += b64;
      }
    } else if (subcmd == "caps") {
      XrdOucString option = (remaining.size() > 0 ? remaining[0].c_str() : "");
      option.replace("-", "");
      in += "&mgm.subcmd=caps&mgm.option=";
      in += option;
      if (remaining.size() > 1) {
        std::ostringstream f;
        for (size_t i = 1; i < remaining.size(); ++i) {
          if (i > 1)
            f << ' ';
          f << remaining[i];
        }
        XrdOucString filter = f.str().c_str();
        if (filter.length()) {
          in += "&mgm.filter=";
          in += eos::common::StringConversion::curl_escaped(filter.c_str())
                    .c_str();
        }
      }
    } else if (subcmd == "dropcaps") {
      if (remaining.size() < 1) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=dropcaps&mgm.fusex.uuid=";
      in += remaining[0].c_str();
    } else if (subcmd == "droplocks") {
      if (remaining.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=droplocks&mgm.inode=";
      in += remaining[0].c_str();
      in += "&mgm.fusex.pid=";
      in += remaining[1].c_str();
    } else if (subcmd == "conf") {
      in += "&mgm.subcmd=conf";
      if (remaining.size() > 0) {
        in += "&mgm.fusex.hb=";
        in += remaining[0].c_str();
      }
      if (remaining.size() > 1) {
        in += "&mgm.fusex.qc=";
        in += remaining[1].c_str();
      }
      if (remaining.size() > 2) {
        in += "&mgm.fusex.bc.max=";
        in += remaining[2].c_str();
      }
      if (remaining.size() > 3) {
        in += "&mgm.fusex.bc.match=";
        in += remaining[3].c_str();
      }
    } else {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeFusexHelp().c_str());
  }
};
} // namespace

void
RegisterFusexNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FusexCommand>());
}
