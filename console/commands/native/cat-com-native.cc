// ----------------------------------------------------------------------
// File: cat-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

extern int com_cat(char*);

namespace {
class CatCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "cat";
  }
  const char*
  description() const override
  {
    return "Cat a file";
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
    return com_cat((char*)joined.c_str());
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: cat <path>\n");
  }
};
} // namespace

void
RegisterCatNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CatCommand>());
}
