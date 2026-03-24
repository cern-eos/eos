// ----------------------------------------------------------------------
// File: acl-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/commands/helpers/AclHelper.hh"
#include <CLI/CLI.hpp>
#include <memory>
#include <sstream>

namespace {
std::string MakeAclHelp()
{
  return "Usage: acl [-l|--list] [-R|--recursive] [-p|--position <pos>] "
         "[-f|--front] [--sys|--user] [<rule>] <identifier>\n\n"
         "Atomically set and modify ACLs for the given directory path/sub-tree.\n\n"
         "Options:\n"
         "  -l, --list       list ACL rules\n"
         "  -R, --recursive  apply to directories recursively\n"
         "  -p, --position   add the acl rule at specified position\n"
         "  -f, --front      add the acl rule at the front position\n"
         "  --user           handle user.acl rules on directory\n"
         "  --sys            handle sys.acl rules on directory (admin only)\n\n"
         "<identifier> can be <path>|cid:<cid-dec>|cxid:<cid-hex>\n\n"
         "<rule> format: [u|g|egroup]:<id> or [u|g|egroup]=<id> with permissions.\n"
         "  \":\" modifies, \"=\" sets. Use + and - to add/remove flags.\n\n"
         "Examples:\n"
         "  acl --user u:1001=rwx /eos/dev/\n"
         "  acl --user u:1001:-w /eos/dev\n"
         "  acl --front --user u:1001=rwx /eos/dev\n";
}

void ConfigureAclApp(CLI::App& app)
{
  app.name("acl");
  app.description("Acl Interface");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeAclHelp();
      }));
}

class AclProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "acl";
  }
  const char*
  description() const override
  {
    return "Acl Interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    (void)ctx;
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    AclHelper acl(gGlobalOpts);
    if (!acl.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = acl.Execute(true, true);
    return global_retc;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureAclApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterAclProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AclProtoCommand>());
}
