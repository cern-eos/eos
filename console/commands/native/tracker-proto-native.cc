// ----------------------------------------------------------------------
// File: tracker-proto-native.cc
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
std::string MakeTrackerHelp()
{
  std::ostringstream oss;
  oss << " usage:\n"
      << std::endl
      << "tracker : print all file replication tracking entries\n"
      << std::endl
      << "This is the standalone alias for 'space tracker' (same request as the\n"
      << "space subcommand; the MGM space defaults to \"default\").\n"
      << std::endl
      << "Layout creation tracking for a space can be enabled or disabled with:\n"
      << "  space config <space-name> space.tracker=on|off [ default=off ]\n";
  return oss.str();
}

void ConfigureTrackerApp(CLI::App& app)
{
  app.name("tracker");
  app.description("Print file replication tracking entries");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeTrackerHelp();
      }));
}

// Minimal helper mirroring the legacy "space tracker" handling
class TrackerHelper : public ICmdHelper {
public:
  explicit TrackerHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  bool
  ParseCommand(const char* /*arg*/) override
  {
    eos::console::SpaceProto* space = mReq.mutable_space();
    eos::console::SpaceProto_TrackerProto* tracker = space->mutable_tracker();
    tracker->set_mgmspace("default");
    return true;
  }
};

class TrackerCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "tracker";
  }
  const char*
  description() const override
  {
    return "Print file replication tracking entries";
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

    TrackerHelper helper(*ctx.globalOpts);
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
    ConfigureTrackerApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterTrackerNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TrackerCommand>());
}
