// ----------------------------------------------------------------------
// File: tracker-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeTrackerHelp()
{
  return "Usage: tracker\n\n"
         "Print all file replication tracking entries.\n";
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

    TrackerHelper helper(gGlobalOpts);
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
