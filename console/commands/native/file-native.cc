// ----------------------------------------------------------------------
// File: file-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_file(char*);

namespace {
class FileCommand : public IConsoleCommand {
public:
  const char* name() const override { return "file"; }
  const char* description() const override { return "File Handling"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_file((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterFileNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FileCommand>());
}


