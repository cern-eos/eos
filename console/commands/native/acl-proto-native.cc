// ----------------------------------------------------------------------
// File: acl-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/commands/helpers/AclHelper.hh"
#include <memory>
#include <sstream>

namespace {
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
    fprintf(
        stderr,
        "Usage: eos acl [-l|--list] [-R|--recursive] [-p|--position <pos>] "
        "[-f|--front] [--sys|--user] [<rule>] <identifier>\n"
        "  atomically set and modify ACLs for the given directory "
        "path/sub-tree\n\n"
        "  -h, --help      : print help message\n"
        "  -R, --recursive : apply to directories recursively\n"
        "  -l, --list      : list ACL rules\n"
        "  -p, --position  : add the acl rule at specified position\n"
        "  -f, --front     : add the acl rule at the front position\n"
        "      --user      : handle user.acl rules on directory\n"
        "      --sys       : handle sys.acl rules on directory - admin only\n\n"
        "  <identifier> can be one of <path>|cid:<cid-dec>|cxid:<cid-hex>\n\n"
        "  <rule> is created similarly to chmod rules. Every rule begins with\n"
        "    [u|g|egroup] followed by \":\" or \"=\" and an identifier.\n"
        "    \":\" is used to for modifying permissions while\n"
        "    \"=\" is used for setting/overwriting permissions.\n"
        "    When modifying permissions every ACL flag can be added with\n"
        "    \"+\" or removed with \"-\".\n"
        "    By default rules are appended at the end of acls\n"
        "    This ordering can be changed via --position flag\n"
        "    which will add the new rule at a given position starting at 1 or\n"
        "    the --front flag which adds the rule at the front instead\n\n"
        "Examples:\n"
        "  acl --user u:1001=rwx /eos/dev/\n"
        "    Set ACLs for user id 1001 to rwx\n"
        "  acl --user u:1001:-w /eos/dev\n"
        "    Remove 'w' flag for user id 1001\n"
        "  acl --user u:1001:+m /eos/dev\n"
        "    Add change mode permission flag for user id 1001\n"
        "  acl --user u:1010= /eos/dev\n"
        "    Remove all ACls for user id 1001\n"
        "  acl --front --user u:1001=rwx /eos/dev\n"
        "     Add the user id 1001 rule to the front of ACL rules\n");
  }
};
} // namespace

void
RegisterAclProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AclProtoCommand>());
}
