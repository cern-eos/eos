// ----------------------------------------------------------------------
// File: test-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {

int RunCmd(const std::string& name, const std::vector<std::string>& args) {
  IConsoleCommand* cmd = CommandRegistry::instance().find(name);
  if (!cmd) {
    fprintf(stderr, "error: command '%s' not available\n", name.c_str());
    return EINVAL;
  }
  CommandContext ctx;
  ctx.serverUri = serveruri.c_str();
  ctx.globalOpts = &gGlobalOpts;
  ctx.json = json;
  ctx.silent = silent;
  ctx.interactive = interactive;
  ctx.timing = timing;
  ctx.userRole = user_role.c_str();
  ctx.groupRole = group_role.c_str();
  ctx.clientCommand = &client_command;
  ctx.outputResult = &output_result;
  return cmd->run(args, ctx);
}

class TestCommand : public IConsoleCommand {
public:
  const char* name() const override { return "test"; }
  const char* description() const override { return "Run performance test"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    if (args.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string& tag = args[0];
    unsigned int n = 0;
    try { n = std::stoul(args[1]); } catch (...) { printHelp(); global_retc = EINVAL; return 0; }

    auto make_base = [](unsigned int i) {
      char buf[32]; snprintf(buf, sizeof(buf), "/test/%02u", i); return std::string(buf);
    };

    int rc = 0;
    if (tag == "mkdir") {
      for (unsigned int i = 0; i < 10; ++i) {
        std::string base = make_base(i);
        rc |= RunCmd("mkdir", {base});
        for (unsigned int j = 0; j < n / 10; ++j) {
          char sub[64]; snprintf(sub, sizeof(sub), "%s/%05u", base.c_str(), j);
          rc |= RunCmd("mkdir", {sub});
        }
      }
    } else if (tag == "rmdir") {
      for (unsigned int i = 0; i < 10; ++i) {
        std::string base = make_base(i);
        for (unsigned int j = 0; j < n / 10; ++j) {
          char sub[64]; snprintf(sub, sizeof(sub), "%s/%05u", base.c_str(), j);
          rc |= RunCmd("rmdir", {sub});
        }
        rc |= RunCmd("rmdir", {base});
      }
    } else if (tag == "ls") {
      for (unsigned int i = 0; i < 10; ++i) {
        rc |= RunCmd("ls", {make_base(i)});
      }
    } else if (tag == "lsla") {
      for (unsigned int i = 0; i < 10; ++i) {
        rc |= RunCmd("ls", {"-la", make_base(i)});
      }
    } else {
      printHelp(); global_retc = EINVAL; return 0;
    }

    global_retc = rc;
    return 0;
  }
  void printHelp() const override {
    fprintf(stderr,
            "Usage: test [mkdir|rmdir|ls|lsla <N> ]                                             :  run performance test\n");
  }
};
} // namespace

void RegisterTestNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TestCommand>());
}
