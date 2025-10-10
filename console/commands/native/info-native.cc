// ----------------------------------------------------------------------
// File: info-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_file(char*);

namespace {
class InfoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "info"; }
  const char* description() const override { return "Retrieve file or directory information"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; oss << "info"; for (const auto& a : args) { oss << ' ' << a; }
    std::string joined = oss.str(); return com_file((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterInfoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<InfoCommand>());
}


