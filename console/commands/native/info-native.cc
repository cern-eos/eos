// ----------------------------------------------------------------------
// File: info-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class InfoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "info";
  }
  const char*
  description() const override
  {
    return "Retrieve file or directory information";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty() || wants_help(args[0].c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // Build mgm.cmd=fileinfo with filters
    XrdOucString path = args[0].c_str();
    if ((!path.beginswith("fid:")) && (!path.beginswith("fxid:")) &&
        (!path.beginswith("pid:")) && (!path.beginswith("pxid:")) &&
        (!path.beginswith("inode:"))) {
      path = abspath(path.c_str());
    }
    XrdOucString in = "mgm.cmd=fileinfo&mgm.path=";
    in += path;
    // Collect remaining flags/options into mgm.file.info.option
    XrdOucString option = "";
    for (size_t i = 1; i < args.size(); ++i) {
      XrdOucString tok = args[i].c_str();
      if (tok.length()) {
        if (tok == "s") {
          option += "silent";
        } else {
          option += tok;
        }
      }
    }
    if (option.length()) {
      in += "&mgm.file.info.option=";
      in += option;
    }
    // If not silent, print output
    if (option.find("silent") == STR_NPOS) {
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    }
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterInfoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<InfoCommand>());
}
