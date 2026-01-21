// ----------------------------------------------------------------------
// File: mkdir-native.cc
// ----------------------------------------------------------------------

#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class MkdirCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "mkdir";
  }
  const char*
  description() const override
  {
    return "Create a directory";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return true;
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty() || args[0] == "--help" || args[0] == "-h") {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    bool parents = false;
    size_t idx = 0;
    if (args[0] == "-p") {
      parents = true;
      idx = 1;
    }
    std::ostringstream path;
    for (size_t i = idx; i < args.size(); ++i) {
      if (i > idx)
        path << ' ';
      path << args[i];
    }
    std::string p = path.str();
    if (p.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=mkdir";
    if (parents)
      in += "&mgm.option=p";
    XrdOucString ap = abspath(p.c_str());
    XrdOucString esc =
        eos::common::StringConversion::curl_escaped(ap.c_str()).c_str();
    in += "&mgm.path=";
    in += esc;
    in += "&eos.encodepath=1";
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: mkdir -p <path>                                                :  create directory <path>\n");
  }
};
} // namespace

void
RegisterMkdirNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MkdirCommand>());
}
