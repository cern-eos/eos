// ----------------------------------------------------------------------
// File: config-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_protoconfig(char*);

namespace {
class ConfigProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "config"; }
  const char* description() const override { return "Configuration System"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_protoconfig((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterConfigProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConfigProtoCommand>());
}


