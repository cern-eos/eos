// ----------------------------------------------------------------------
// File: health-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/commands/HealthCommand.hh"
#include <memory>
#include <sstream>

namespace {
class HealthConsoleCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "health";
  }
  const char*
  description() const override
  {
    return "Cluster health check";
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
    ::HealthCommand cmd(joined.c_str());
    cmd.Execute();
    global_retc = 0;
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterHealthNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<HealthConsoleCommand>());
}
