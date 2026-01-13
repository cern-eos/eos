// ----------------------------------------------------------------------
// File: fusex-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
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
    if (!args.empty()) {
      std::ostringstream oss;
      for (size_t i = 0; i < args.size(); ++i) {
        if (i)
          oss << ' ';
        oss << args[i];
      }
      if (wants_help(oss.str().c_str())) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
    }
    if (args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString sub = args[0].c_str();
    XrdOucString in = "mgm.cmd=fusex";
    if (sub == "ls") {
      in += "&mgm.subcmd=ls";
    } else if (sub == "evict") {
      if (args.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString uuid = args[1].c_str();
      in += "&mgm.subcmd=evict&mgm.fusex.uuid=";
      in += uuid;
      if (args.size() > 2) {
        XrdOucString reason = args[2].c_str();
        XrdOucString b64;
        eos::common::SymKey::Base64(reason, b64);
        in += "&mgm.fusex.reason=";
        in += b64;
      }
    } else if (sub == "caps") {
      XrdOucString option = (args.size() > 1 ? args[1].c_str() : "");
      XrdOucString filter;
      option.replace("-", "");
      in += "&mgm.subcmd=caps&mgm.option=";
      in += option;
      if (args.size() > 2) {
        std::ostringstream f;
        for (size_t i = 2; i < args.size(); ++i) {
          if (i > 2)
            f << ' ';
          f << args[i];
        }
        filter = f.str().c_str();
        if (filter.length()) {
          in += "&mgm.filter=";
          in += eos::common::StringConversion::curl_escaped(filter.c_str())
                    .c_str();
        }
      }
    } else if (sub == "dropcaps") {
      if (args.size() < 2) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=dropcaps&mgm.fusex.uuid=";
      in += args[1].c_str();
    } else if (sub == "droplocks") {
      if (args.size() < 3) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=droplocks&mgm.inode=";
      in += args[1].c_str();
      in += "&mgm.fusex.pid=";
      in += args[2].c_str();
    } else if (sub == "conf") {
      in += "&mgm.subcmd=conf";
      if (args.size() > 1) {
        in += "&mgm.fusex.hb=";
        in += args[1].c_str();
      }
      if (args.size() > 2) {
        in += "&mgm.fusex.qc=";
        in += args[2].c_str();
      }
      if (args.size() > 3) {
        in += "&mgm.fusex.bc.max=";
        in += args[3].c_str();
      }
      if (args.size() > 4) {
        in += "&mgm.fusex.bc.match=";
        in += args[4].c_str();
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
    fprintf(
        stdout,
        "Usage: fusex <subcmd> [args...]\n"
        "subcommands:\n"
        "  ls                                    List active FUSEX clients\n"
        "  evict <uuid> [reason]                 Evict a client by UUID "
        "(reason base64-encoded)\n"
        "  caps [all|token|lock] [filter]        Show capabilities, optional "
        "filter string\n"
        "  dropcaps <uuid>                        Drop capabilities for client "
        "UUID\n"
        "  droplocks <inode> <pid>                Drop locks for inode and "
        "process\n"
        "  conf [hb] [qc] [bc.max] [bc.match]     Configure heartbeat (hb), "
        "queue cap (qc),\n"
        "                                           block cache max and "
        "match\n");
  }
};
} // namespace

void
RegisterFusexNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FusexCommand>());
}
