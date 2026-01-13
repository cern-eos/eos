// ----------------------------------------------------------------------
// File: chown-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class ChownCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "chown";
  }
  const char*
  description() const override
  {
    return "Chown Interface";
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

    size_t idx = 0;
    std::string opt;
    while (idx < args.size() && args[idx].rfind("-", 0) == 0) {
      const std::string& o = args[idx];
      if (o == "-r") {
        if (opt.find('r') == std::string::npos) opt.push_back('r');
      } else if (o == "-h" || o == "--nodereference") {
        if (opt.find('h') == std::string::npos) opt.push_back('h');
      } else {
        printHelp(); global_retc = EINVAL; return 0;
      }
      ++idx;
    }

    if (idx + 1 >= args.size()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    const std::string& owner = args[idx];
    const std::string& path = args[idx + 1];

    XrdOucString in = "mgm.cmd=chown";
    if (!opt.empty()) {
      in += "&mgm.chown.option=";
      in += opt.c_str();
    }
    XrdOucString ap = abspath(path.c_str());
    in += "&mgm.path=";
    in += ap;
    in += "&mgm.chown.owner=";
    in += owner.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: chown [-r] [-h --nodereference] <owner>[:<group>] <path>\n");
    fprintf(stderr, "       chown [-r] :<group> <path>\n");
    fprintf(stderr,
            "'[eos] chown ..' provides the change owner interface of EOS.\n");
    fprintf(stderr,
            "<path> is the file/directory to modify, <owner> has to be a user id or user name. <group> is optional and has to be a group id or group name.\n");
    fprintf(stderr, "To modify only the group use :<group> as identifier!\n");
    fprintf(stderr,
            "Remark: if you use the -r -h option and path points to a link the owner of the link parent will also be updated!");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "                  -r : recursive\n");
  }
};
} // namespace

void
RegisterChownNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChownCommand>());
}
