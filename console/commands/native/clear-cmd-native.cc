// ----------------------------------------------------------------------
// File: clear-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>
#include <string.h>

namespace {
class ClearCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "clear";
  }
  const char*
  description() const override
  {
    return "Clear the terminal";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    if (!args.empty()) {
      if (args[0] == "-h" || args[0] == "--help" || args[0] == "\"-h\"" ||
          args[0] == "\"--help\"") {
        printHelp();
        return 0;
      }
    }
    int rc = system("clear");
    return rc;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: clear\n");
    fprintf(stderr, "'[eos] clear' is equivalent to the interactive shell "
                    "command to clear the screen.\n");
  }
};
} // namespace

void
RegisterClearNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ClearCommand>());
}
