// ----------------------------------------------------------------------
// File: tracker-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
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
    fprintf(stderr,
            "Usage: space tracker\n"
            "       print all file replication tracking entries\n");
  }
};
} // namespace

void
RegisterTrackerNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TrackerCommand>());
}
