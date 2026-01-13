// ----------------------------------------------------------------------
// File: acl-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
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

    // Minimal native mapping of 'acl' to 'attr' operations
    bool list = false;
    bool recursive = false;
    bool userAcl = false;
    bool sysAcl = false;
    std::vector<std::string> positionals;

    for (size_t i = 0; i < args.size(); ++i) {
      const std::string& t = args[i];
      if (t == "-l" || t == "--list") {
        list = true;
        continue;
      }
      if (t == "-R" || t == "--recursive") {
        recursive = true;
        continue;
      }
      if (t == "--user") {
        userAcl = true;
        continue;
      }
      if (t == "--sys") {
        sysAcl = true;
        continue;
      }
      // ignore position/front for now (ordering handled server-side)
      if (t == "-p" || t == "--position") {
        if (i + 1 < args.size()) {
          ++i;
        }
        continue;
      }
      if (t == "-f" || t == "--front") {
        continue;
      }
      positionals.push_back(t);
    }

    const char* key = sysAcl ? "sys.acl" : (userAcl ? "user.acl" : "sys.acl");

    IConsoleCommand* attrCmd = CommandRegistry::instance().find("attr");
    if (!attrCmd) {
      fprintf(stderr,
              "error: 'attr' command not available for ACL operations\n");
      global_retc = EINVAL;
      return 0;
    }

    if (list) {
      if (positionals.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::vector<std::string> attrArgs;
      attrArgs.push_back("ls");
      // attr ls key <path>
      attrArgs.push_back(key);
      attrArgs.push_back(positionals.back());
      return attrCmd->run(attrArgs, ctx);
    }

    // Set rule
    if (positionals.size() < 2) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // Interpret last positional as identifier, prior ones as rule segments
    // (join with space to preserve quoted input)
    std::string identifier = positionals.back();
    std::string rule;
    for (size_t i = 0; i + 1 < positionals.size(); ++i) {
      if (i)
        rule.push_back(' ');
      rule += positionals[i];
    }
    std::vector<std::string> attrArgs;
    attrArgs.push_back("set");
    if (recursive)
      attrArgs.push_back("-r");
    std::string kv = std::string(key) + "=" + rule;
    attrArgs.push_back(kv);
    attrArgs.push_back(identifier);
    return attrCmd->run(attrArgs, ctx);
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
