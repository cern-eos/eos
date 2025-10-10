// ----------------------------------------------------------------------
// File: archive-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class ArchiveCommand : public IConsoleCommand {
public:
  const char* name() const override { return "archive"; }
  const char* description() const override { return "Archive Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.empty()) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string& sub = args[0];
    std::ostringstream in_cmd; in_cmd << "mgm.cmd=archive&mgm.subcmd=" << sub;
    if (sub == "create") {
      std::string path = (args.size() > 1 ? args[1] : gPwd.c_str());
      XrdOucString p = abspath(path.c_str()); in_cmd << "&mgm.archive.path=" << p.c_str();
    } else if (sub == "put" || sub == "get" || sub == "purge" || sub == "delete") {
      size_t i = 1; std::string tok; bool retry = false;
      if (i < args.size() && args[i].rfind("--",0)==0) { if (args[i] == "--retry") { retry = true; ++i; } else { printHelp(); global_retc = EINVAL; return 0; } }
      std::string path = (i < args.size() ? args[i] : std::string()); if (path.empty()) { printHelp(); global_retc = EINVAL; return 0; }
      if (retry) in_cmd << "&mgm.archive.option=r";
      XrdOucString p = abspath(path.c_str()); in_cmd << "&mgm.archive.path=" << p.c_str();
    } else if (sub == "transfers") {
      std::string tok = (args.size() > 1 ? args[1] : std::string());
      if (tok.empty()) in_cmd << "&mgm.archive.option=all"; else in_cmd << "&mgm.archive.option=" << tok;
    } else if (sub == "list") {
      std::string tok = (args.size() > 1 ? args[1] : std::string());
      if (tok.empty()) in_cmd << "&mgm.archive.path=/";
      else if (tok == "./" || tok == ".") { XrdOucString p = abspath(gPwd.c_str()); in_cmd << "&mgm.archive.path=" << p.c_str(); }
      else in_cmd << "&mgm.archive.path=" << tok;
    } else if (sub == "kill") {
      if (args.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      in_cmd << "&mgm.archive.option=" << args[1];
    } else { printHelp(); global_retc = EINVAL; return 0; }
    XrdOucString in = in_cmd.str().c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterArchiveNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ArchiveCommand>());
}


