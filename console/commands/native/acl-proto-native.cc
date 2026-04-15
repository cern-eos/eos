// ----------------------------------------------------------------------
// File: acl-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/commands/helpers/AclHelper.hh"
#include <CLI/CLI.hpp>
#include <iomanip>
#include <memory>
#include <sstream>

namespace {
std::string MakeAclHelp()
{
  std::ostringstream oss;
  oss << "Usage: eos acl [-l|--list] [-R|--recursive]"
      << " [-p|--position <pos>] [-f|--front] "
      << "[--sys|--user] [<rule>] <identifier>" << std::endl
      << "  atomically set and modify ACLs for the given directory path/sub-tree"
      << std::endl
      << std::endl
      << "  -h, --help      : print help message" << std::endl
      << "  -R, --recursive : apply to directories recursively" << std::endl
      << "  -l, --list      : list ACL rules" << std::endl
      << "  -p, --position  : add the acl rule at specified position" << std::endl
      << "  -f, --front     : add the acl rule at the front position" << std::endl
      << "      --user      : handle user.acl rules on directory" << std::endl
      << "      --sys       : handle sys.acl rules on directory - admin only\n"
      << std::endl
      << "  <identifier> can be one of <path>|cid:<cid-dec>|cxid:<cid-hex>\n"
      << std::endl
      << "  <rule> is created similarly to chmod rules. Every rule begins with"
      << std::endl
      << "    [u|g|egroup] followed by \":\" or \"=\" and an identifier."
      << std::endl
      << "    \":\" is used to for modifying permissions while" << std::endl
      << "    \"=\" is used for setting/overwriting permissions." << std::endl
      << "    When modifying permissions every ACL flag can be added with"
      << std::endl
      << "    \"+\" or removed with \"-\"." << std::endl
      << "    By default rules are appended at the end of acls" << std::endl
      << "    This ordering can be changed via --position flag" << std::endl
      << "    which will add the new rule at a given position starting at 1 or"
      << std::endl
      << "    the --front flag which adds the rule at the front instead"
      << std::endl
      << std::endl
      << "Examples:" << std::endl
      << "  acl --user u:1001=rwx /eos/dev/" << std::endl
      << "    Set ACLs for user id 1001 to rwx" << std::endl
      << "  acl --user u:1001:-w /eos/dev" << std::endl
      << "    Remove \'w\' flag for user id 1001" << std::endl
      << "  acl --user u:1001:+m /eos/dev" << std::endl
      << "    Add change mode permission flag for user id 1001" << std::endl
      << "  acl --user u:1010= /eos/dev" << std::endl
      << "    Remove all ACls for user id 1001" << std::endl
      << "  acl --front --user u:1001=rwx /eos/dev" << std::endl
      << "     Add the user id 1001 rule to the front of ACL rules" << std::endl;
  return oss.str();
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
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    AclHelper acl(*ctx.globalOpts);
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
