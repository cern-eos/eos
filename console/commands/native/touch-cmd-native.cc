// ----------------------------------------------------------------------
// File: touch-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class TouchCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "touch";
  }
  const char*
  description() const override
  {
    return "Touch a file";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    // Build mgm.cmd=file&mgm.subcmd=touch with options
    ConsoleArgParser p;
    p.addOption({"", 'a', false, false, "", "absorb", ""});
    p.addOption({"", 'n', false, false, "", "no placement", ""});
    p.addOption({"", '0', false, false, "", "truncate", ""});
    p.addOption({"", 'l', true, false, "<lifetime>", "lock with lifetime", ""});
    p.addOption({"", 'u', false, false, "", "unlock", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.empty()) {
      fprintf(
          stdout,
          "Usage: touch [-a] [-n] [-0] <path> [linkpath|size [hexchecksum]] | "
          "touch -l <path> [<lifetime> [<aud>=user|app]] | touch -u <path>\n");
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=file&mgm.subcmd=touch";
    XrdOucString path = pos[0].c_str();
    path = Path2FileDenominator(path) ? path : abspath(path.c_str());
    in += Path2FileDenominator(path) ? "&mgm.file.id=" : "&mgm.path=";
    in += path;
    if (r.has("n"))
      in += "&mgm.file.touch.nolayout=true";
    if (r.has("0"))
      in += "&mgm.file.touch.truncate=true";
    if (r.has("a"))
      in += "&mgm.file.touch.absorb=true";
    if (r.has("l")) {
      in += "&mgm.file.touch.lockop=lock";
      XrdOucString life = r.value("l").c_str();
      if (life.length()) {
        in += "&mgm.file.touch.lockop.lifetime=";
        in += life;
      }
      if (pos.size() > 1) {
        XrdOucString aud = pos[1].c_str();
        if (aud == "app")
          in += "&mgm.file.touch.wildcard=user";
        else if (aud == "user")
          in += "&mgm.file.touch.wildcard=app";
      }
    }
    if (r.has("u")) {
      in += "&mgm.file.touch.lockop=unlock";
    }
    if (pos.size() > 1 && !r.has("l")) {
      XrdOucString arg1 = pos[1].c_str();
      if (arg1.beginswith("/")) {
        in += "&mgm.file.touch.hardlinkpath=";
        in += arg1;
      } else {
        in += "&mgm.file.touch.size=";
        in += arg1;
      }
    }
    if (pos.size() > 2 && !r.has("l")) {
      in += "&mgm.file.touch.checksuminfo=";
      in += pos[2].c_str();
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterTouchNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TouchCommand>());
}
