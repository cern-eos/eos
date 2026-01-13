// ----------------------------------------------------------------------
// File: io-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {

class IoHelper : public ICmdHelper {
public:
  IoHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~IoHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::IoProto* io = mReq.mutable_io();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token)) {
      return false;
    }
    if (token == "stat") {
      eos::console::IoProto_StatProto* stat = io->mutable_stat();
      while (tokenizer.NextToken(token)) {
        if (token == "-a")
          stat->set_details(true);
        else if (token == "-m")
          stat->set_monitoring(true);
        else if (token == "-n")
          stat->set_numerical(true);
        else if (token == "-t")
          stat->set_top(true);
        else if (token == "-d")
          stat->set_domain(true);
        else if (token == "-x")
          stat->set_apps(true);
        else if (token == "--ss")
          stat->set_sample_stat(true);
        else if (token == "--sa") {
          if (!tokenizer.NextToken(token))
            return false;
          if (token.find_first_not_of("0123456789") != std::string::npos)
            return false;
          stat->set_time_ago(std::stoull(token));
        } else if (token == "--si") {
          if (!tokenizer.NextToken(token))
            return false;
          if (token.find_first_not_of("0123456789") != std::string::npos)
            return false;
          stat->set_time_interval(std::stoull(token));
        } else if (token == "-l")
          stat->set_summary(true);
        else
          return false;
      }
    } else if (token == "ns") {
      eos::console::IoProto_NsProto* ns = io->mutable_ns();
      while (tokenizer.NextToken(token)) {
        if (token == "-m")
          ns->set_monitoring(true);
        else if (token == "-b")
          ns->set_rank_by_byte(true);
        else if (token == "-n")
          ns->set_rank_by_access(true);
        else if (token == "-w")
          ns->set_last_week(true);
        else if (token == "-f")
          ns->set_hotfiles(true);
        else if (token == "-100")
          ns->set_count(eos::console::IoProto_NsProto::ONEHUNDRED);
        else if (token == "-1000")
          ns->set_count(eos::console::IoProto_NsProto::ONETHOUSAND);
        else if (token == "-10000")
          ns->set_count(eos::console::IoProto_NsProto::TENTHOUSAND);
        else if (token == "-a")
          ns->set_count(eos::console::IoProto_NsProto::ALL);
        else
          return false;
      }
    } else if (token == "report") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::IoProto_ReportProto* report = io->mutable_report();
      report->set_path(token);
    } else if (token == "enable" || token == "disable") {
      eos::console::IoProto_EnableProto* enable = io->mutable_enable();
      enable->set_switchx(token == "enable");
      while (tokenizer.NextToken(token)) {
        if (token == "-r")
          enable->set_reports(true);
        else if (token == "-p")
          enable->set_popularity(true);
        else if (token == "-n")
          enable->set_namespacex(true);
        else if (token == "--udp") {
          if (!tokenizer.NextToken(token) || token.find('-') == 0)
            return false;
          enable->set_upd_address(token);
        } else
          return false;
      }
    } else {
      return false;
    }
    return true;
  }
};

class IoProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "io";
  }
  const char*
  description() const override
  {
    return "IO Interface";
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
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    IoHelper helper(gGlobalOpts);
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
    fprintf(
        stdout,
        " Usage:\n\n"
        "io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x] [--ss] [--sa] [--si] : "
        "print io statistics\n"
        "\t  -l : show summary information (this is the default if -a,-t,-d,-x "
        "is not selected)\n"
        "\t  -a : break down by uid/gid\n"
        "\t  -m : print in <key>=<val> monitoring format\n"
        "\t  -n : print numerical uid/gids\n"
        "\t  -t : print top user stats\n"
        "\t  -d : break down by domains\n"
        "\t  -x : break down by application\n"
        "\t  --ss : show table with transfer sample statistics\n"
        "\t  --sa : start collection of statistics given number of seconds "
        "ago\n"
        "\t  --si : collect statistics over given interval of seconds\n"
        "\t  Note: this tool shows data for finished transfers only (using "
        "storage node reports)\n"
        "\t  Example: asking for data of finished transfers which were "
        "transferred during interval [now - 180s, now - 120s]:\n"
        "\t           eos io stat -x --sa 120 --si 60\n\n"
        "io enable [-r] [-p] [-n] [--udp <address>] : enable collection of io "
        "statistics\n"
        "\t         no arg. : start the colleciton thread\n"
        "\t              -r : enable collection of io reports\n"
        "\t              -p : enable popularity accounting\n"
        "\t              -n : enable report namespace\n"
        "\t --udp <address> : add a UDP message target for io UDP packtes (the "
        "configured targets are shown by 'io stat -l)\n\n"
        "io disable [-r] [-p] [-n] [--udp <address>] : disable collection of "
        "io statistics\n"
        "\t         no arg. : stop the collection thread\n"
        "\t              -r : disable collection of io reports\n"
        "\t              -p : disable popularity accounting\n"
        "\t              -n : disable report namespace\n"
        "\t --udp <address> : remove a UDP message target for io UDP packtes "
        "(the configured targets are shown by 'io stat -l)\n\n"
        "io report <path> : show contents of report namespace for <path>\n\n"
        "io ns [-a] [-n] [-b] [-100|-1000|-10000] [-w] [-f] : show namespace "
        "IO ranking (popularity)\n"
        "\t      -a :  don't limit the output list\n"
        "\t      -n :  show ranking by number of accesses\n"
        "\t      -b :  show ranking by number of bytes\n"
        "\t    -100 :  show the first 100 in the ranking\n"
        "\t   -1000 :  show the first 1000 in the ranking\n"
        "\t  -10000 :  show the first 10000 in the ranking\n"
        "\t      -w :  show history for the last 7 days\n"
        "\t      -f :  show the 'hotfiles' which are the files with highest "
        "number of present file opens\n");
  }
};
} // namespace

void
RegisterIoProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<IoProtoCommand>());
}
