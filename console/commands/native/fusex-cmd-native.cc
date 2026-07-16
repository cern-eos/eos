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
  std::ostringstream oss;
  oss << "Usage: fusex ls [-l] [-f] [-m]                     :  print statistics about eosxd fuse clients\n"
      << "                [no option]                                          -  break down by client host [default]\n"
      << "                -l                                                   -  break down by client host and show statistics \n"
      << "                -f                                                   -  show ongoing flush locks\n"
      << "                -k                                                   -  show R/W locks\n"
      << "                -m                                                   -  show monitoring output format\n"
      << "\n"
      << "       fusex evict <uuid> [<reason>]                                 :  evict a fuse client\n"
      << "                                                              <uuid> -  uuid of the client to evict\n"
      << "                                                            <reason> -  optional text shown to the client why he has been evicted or an instruction for an action to the client\n"
      << "                                                                     - if the reason contains the keywoard 'abort' the abort handler will be called on client side (might create a stack trace/core)\n"
      << "                                                                     - if reason contains the keyword 'log2big' the client will effectily not be evicted, but will truncate his logfile to 0\n"
      << "                                                                     - if reason contains the keyword 'setlog' and 'debug','notice', 'error', 'crit', 'info', 'warning' the log level of the targeted mount is changed accordingly .e.g evict <uuid> \"setlog error\"\n"
      << "                                                                     - if reason contains the keyword 'stacktrace' the client will send a self-stacktrace with the next heartbeat message and it will be stored in /var/log/eos/mgm/eosxd-stacktraces.log e.g. evict <uuid> stacktrace\n"
      << "                                                                     - if reason contains the keyword 'sendlog' the client will send max. the last 512 lines of each log level and the log will be stored in /var/log/eos/mgm/eosxd-logtraces.log e.g. evict <uuid> sendlog\n"
      << "                                                                     - if reason contains the keyword 'resetbuffer' the client will reset the read-ahead and write-buffers in flight and possibly unlock a locked mount point\n"
      << "\n"
      << "       fusex evict static|autofs mem:<size-in-mb>|idle:<seconds>     :  evict all autofs or static mounts which have a resident memory footprint larger than <size-in-mb> or are idle longer than <seconds>\n"
      << "\n"
      << "       fusex dropcaps <uuid>                                         :  advice a client to drop all caps\n"
      << "\n"
      << "       fusex droplocks <inode> <pid>                                 :  advice a client to drop for a given (hexadecimal) inode and process id\n"
      << "\n"
      << "       fusex caps [-t | -i | -p [<regexp>] ]                         :  print caps\n"
      << "                -t                                                   -  sort by expiration time\n"
      << "                -i                                                   -  sort by inode\n"
      << "                -p                                                   -  display by path\n"
      << "                -t|i|p <regexp>>                                     -  display entries matching <regexp> for the used filter type\n"
      << "\n"
      << "examples:\n"
      << "           fusex caps -i ^0000abcd$                                  :  show caps for inode 0000abcd\n"
      << "           fusex caps -p ^/eos/$                                     :  show caps for path /eos\n"
      << "           fusex caps -p ^/eos/caps/                                 :  show all caps in subtree /eos/caps\n"
      << "       fusex conf [<heartbeat-in-seconds>] [quota-check-in-seconds] [max broadcast audience] [broadcast audience match]\n"
      << "                                                             :  show heartbeat and quota interval\n"
      << "                                                                     :  [ optional change heartbeat interval from [1-15] seconds ]\n"
      << "                                                                     :  [ optional set quota check interval from [1-16] seconds ]\n"
      << "examples:\n"
      << "   fusex conf                                                :  show heartbeat and quota interval\n"
      << "   fusex conf 10                                             :  define heartbeat interval as 10 seconds\n"
      << "   fusex conf 10 30                                          :  define heartbeat as 10 seconds and quota interval as 30 seconds\n"
      << "   fusex conf 0 0 256 @b[67]                                :  suppress broadcasts when more than 256 clients are conected and the target matches @b[67]\n";
  return oss.str();
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

    // Note: unlike other native commands using this reverse-parse idiom,
    // fusex has a required "subcmd" positional consumed ahead of the
    // extras. Once that positional is consumed out of the reversed
    // cli_args, app.remaining() already yields the leftover tokens in
    // original command-line order - reversing them again here would
    // scramble multi-argument subcommands like "conf <hb> <qc> <bc.max>
    // <bc.match>" (first/last arguments end up swapped).
    std::vector<std::string> remaining = app.remaining();

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
