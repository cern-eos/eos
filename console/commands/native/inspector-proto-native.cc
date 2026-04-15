// ----------------------------------------------------------------------
// File: inspector-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <iomanip>
#include <memory>
#include <sstream>

namespace {
std::string MakeInspectorHelp()
{
  std::ostringstream oss;
  oss << " usage:\n"
      << std::endl
      << "inspector [--current|-c] [--last|-l] [-m] [-p] [-e] [-s|--space "
         "<space_name>] [--all|-a] [--cost|-C] [--usage|-U] [--birth|-B] "
         "[--access|-A] [--vs|-V] [--layouts|-L] : show namespace inspector output\n"
      << std::endl
      << "Same request as 'space inspector'; default MGM space is \"default\" "
         "unless -s|--space is given.\n"
      << std::endl
      << "\t  -c  : show current scan\n"
      << "\t  -l  : show last complete scan\n"
      << "\t  -m  : print last scan in monitoring format ( by default this enables "
         "--cost --usage --birth --access --layouts)\n"
      << "\t  -A  : combined with -m prints access time distributions\n"
      << "\t  -V  : combined with -m prints birth time vs access time distributions\n"
      << "\t  -B  : combined with -m prints birth time distributions\n"
      << "\t  -C  : combined with -m prints cost information (storage price per "
         "user/group)\n"
      << "\t  -U  : combined with -m prints usage information (stored bytes per "
         "user/group)\n"
      << "\t  -L  : combined with -m prints layout statistics\n"
      << "\t  -a  : combined with -m or -C or -U removes the restriction to show only "
         "the top 10 user ranking\n"
      << "\t  -p  : combined with -c or -l lists erroneous files\n"
      << "\t  -e  : combined with -c or -l exports erroneous files on the MGM into "
         "/var/log/eos/mgm/FileInspector.<date>.list\n"
      << "\t  -s  : select target space, by default \"default\" space is used\n"
      << std::endl
      << "\t  -M|--money : money output\n"
      << std::endl
      << "space config <space-name> space.inspector=on|off                      : "
         "enable/disable the file inspector [ default=off ]\n"
      << "space config <space-name> space.inspector.interval=<sec>              : "
         "time interval after which the inspector will run, default 4h\n";
  return oss.str();
}

void ConfigureInspectorApp(CLI::App& app)
{
  app.name("inspector");
  app.description("Run inspector tools");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeInspectorHelp();
      }));
}

class InspectorHelper : public ICmdHelper {
public:
  explicit InspectorHelper(const GlobalOptions& opts) : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }
  bool
  ParseCommand(const char* arg) override
  {
    eos::common::StringTokenizer tok(arg);
    tok.GetLine();
    std::string token;
    eos::console::SpaceProto* space = mReq.mutable_space();
    eos::console::SpaceProto_InspectorProto* insp = space->mutable_inspector();
    insp->set_mgmspace("default");
    std::string options;

    while (tok.NextToken(token)) {
      if ((token == "-s") || (token == "--space")) {
        if (tok.NextToken(token)) {
          insp->set_mgmspace(token);
        } else {
          std::cerr << "error: no space specified" << std::endl;
          return false;
        }
      } else if (token == "-c" || token == "--current") {
        options += "c";
      } else if (token == "-l" || token == "--last") {
        options += "l";
      } else if (token == "-m") {
        options += "m";
      } else if (token == "-p") {
        options += "p";
      } else if (token == "-e") {
        options += "e";
      } else if (token == "-C" || token == "--cost") {
        options += "C";
      } else if (token == "-U" || token == "--usage") {
        options += "U";
      } else if (token == "-L" || token == "--layouts") {
        options += "L";
      } else if (token == "-B" || token == "--birth") {
        options += "B";
      } else if (token == "-A" || token == "--access") {
        options += "A";
      } else if (token == "-a" || token == "--all") {
        options += "Z";
      } else if (token == "-V" || token == "--vs") {
        options += "V";
      } else if (token == "-M" || token == "--money") {
        options += "M";
      } else {
        return false;
      }
    }

    insp->set_options(options);
    return true;
  }
};

class InspectorCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "inspector";
  }
  const char*
  description() const override
  {
    return "Run inspector tools";
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
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    InspectorHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute(true, true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureInspectorApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterInspectorNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<InspectorCommand>());
}
