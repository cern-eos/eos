// ----------------------------------------------------------------------
// File: token-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/commands/helpers/TokenHelper.hh"
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
    TokenHelper token(gGlobalOpts);
    if (!token.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = token.Execute(true, true);
    return global_retc;
  }
  void
  printHelp() const override
  {
    std::ostringstream oss;
    oss << "Usage: token --token  <token> | --path <path> --expires <expires> [--permission <perm>] [--owner <owner>] [--group <group>] [--tree] [--origin <origin1> [--origin <origin2>] ...]] \n"
        << "    get or show a token\n\n"
        << "       token --token <token> \n"
        << "                                           : provide a JSON dump of a token - independent of validity\n"
        << "             --path <path>                 : define the namespace restriction - if ending with '/' this is a directory or tree, otherwise it references a file\n"
        << "             --path <path1>://:<path2>://: ..."
        << "                                           : define multi-path token which share ACLs for all of them\n"
        << "             --permission <perm>           : define the token bearer permissions e.g 'rx' 'rwx' 'rwx!d' 'rwxq' - see acl command for permissions\n"
        << "             --owner <owner>               : identify the bearer with as user <owner> \n"
        << "             --group <group>               : identify the beaere with a group <group> \n"
        << "             --tree                        : request a subtree token granting permissions for the whole tree under <path>\n"
        << "              --origin <origin>            : restrict token usage to <origin> - multiple origin parameters can be provided\n"
        << "                                             <origin> := <regexp:hostname>:<regex:username>:<regex:protocol>\n"
        << "                                             - described by three regular extended expressions matching the \n"
        << "                                               bearers hostname, possible authenticated name and protocol\n"
        << "                                             - default is .*:.*:.* (be careful with proper shell escaping)"
        << "\n"
        << "Examples:\n"
        << "          eos token --path /eos/ --permission rx --tree\n"
        << "                                           : token with browse permission for the whole /eos/ tree\n"
        << "          eos token --path /eos/file --permission rwx --owner foo --group bar\n"
        << "                                           : token granting write permission for /eos/file as user foo:bar\n"
        << "          eos token --token zteos64:...\n"
        << "                                           : dump the given token\n"
        << std::endl;
    std::cerr << oss.str();
  }
};
} // namespace

void
RegisterTokenProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TokenProtoCommand>());
}
