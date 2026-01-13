// ----------------------------------------------------------------------
// File: token-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

namespace {
class TokenProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "token";
  }
  const char*
  description() const override
  {
    return "Token interface";
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
    // Parse token CLI: either --token <tok> OR --path <path> --expires
    // <expires> [--permission <perm>] [--owner <owner>] [--group <group>]
    // [--tree] [--origin <origin>...]
    XrdOucString in = "mgm.cmd=token";
    bool haveOp = false;
    for (size_t i = 0; i < args.size(); ++i) {
      const std::string& a = args[i];
      if (a == "--token" && i + 1 < args.size()) {
        in += "&mgm.token=";
        in += args[++i].c_str();
        haveOp = true;
      } else if (a == "--path" && i + 1 < args.size()) {
        in += "&mgm.path=";
        in += abspath(args[++i].c_str());
        haveOp = true;
      } else if (a == "--expires" && i + 1 < args.size()) {
        in += "&mgm.expires=";
        in += args[++i].c_str();
      } else if (a == "--permission" && i + 1 < args.size()) {
        in += "&mgm.perm=";
        in += args[++i].c_str();
      } else if (a == "--owner" && i + 1 < args.size()) {
        in += "&mgm.owner=";
        in += args[++i].c_str();
      } else if (a == "--group" && i + 1 < args.size()) {
        in += "&mgm.group=";
        in += args[++i].c_str();
      } else if (a == "--tree") {
        in += "&mgm.tree=1";
      } else if (a == "--origin" && i + 1 < args.size()) {
        in += "&mgm.origin=";
        in += args[++i].c_str();
      }
    }
    if (!haveOp) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: token --token  <token> | --path <path> --expires <expires> "
        "[--permission <perm>] [--owner <owner>] [--group <group>] [--tree] "
        "[--origin <origin1> [--origin <origin2>] ...]] \n"
        "    get or show a token\n\n"
        "       token --token <token> \n"
        "                                           : provide a JSON dump of a "
        "token - independent of validity\n"
        "             --path <path>                 : define the namespace "
        "restriction - if ending with '/' this is a directory or tree, "
        "otherwise it references a file\n"
        "             --path <path1>://:<path2>://: ...                        "
        "                   : define multi-path token which share ACLs for all "
        "of them"
        "             --permission <perm>           : define the token bearer "
        "permissions e.g 'rx' 'rwx' 'rwx!d' 'rwxq' - see acl command for "
        "permissions\n"
        "             --owner <owner>               : identify the bearer with "
        "as user <owner> \n"
        "             --group <group>               : identify the beaere with "
        "a group <group> \n"
        "             --tree                        : request a subtree token "
        "granting permissions for the whole tree under <path>\n"
        "              --origin <origin>            : restrict token usage to "
        "<origin> - multiple origin parameters can be provided\n"
        "                                             <origin> := "
        "<regexp:hostname>:<regex:username>:<regex:protocol>\n"
        "                                             - described by three "
        "regular extended expressions matching the \n"
        "                                               bearers hostname, "
        "possible authenticated name and protocol\n"
        "                                             - default is .*:.*:.* "
        "(be careful with proper shell escaping)\n"
        "\n"
        "Examples:\n"
        "          eos token --path /eos/ --permission rx --tree\n"
        "                                           : token with browse "
        "permission for the whole /eos/ tree\n"
        "          eos token --path /eos/file --permission rwx --owner foo "
        "--group bar\n"
        "                                           : token granting write "
        "permission for /eos/file as user foo:bar\n"
        "          eos token --token zteos64:...\n"
        "                                           : dump the given token\n");
  }
};
} // namespace

void
RegisterTokenProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TokenProtoCommand>());
}
