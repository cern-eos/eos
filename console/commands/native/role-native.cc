// ----------------------------------------------------------------------
// File: role-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/ConsoleMetadata.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeRoleHelp()
{
  std::ostringstream oss;
  oss << "Usage: role <user-role> [<group-role>]                       : select user role <user-role> [and group role <group-role>]\n"
      << "            <user-role> can be a virtual user ID (unsigned int) or a user mapping alias\n"
      << "            <group-role> can be a virtual group ID (unsigned int) or a group mapping alias\n";
  return oss.str();
}

class RoleCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "role";
  }
  const char*
  description() const override
  {
    return "Switch role or show roles";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    if (args.empty() || args.size() > 2 || args[0].empty() || args[0].front() == '-' ||
        !eos::console::IsSafeCgiMetadata(args[0]) ||
        (args.size() == 2 && (!eos::console::IsSafeCgiMetadata(args[1]) ||
                              (!args[1].empty() && args[1].front() == '-')))) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    user_role = args[0].c_str();
    if (args.size() > 1)
      group_role = args[1].c_str();
    else
      group_role = "";
    if (!silent)
      fprintf(stdout,
              "=> selected user role ruid=<%s> and group role rgid=<%s>\n",
              user_role.c_str(), group_role.c_str());
    gGlobalOpts.mUserRole = user_role.c_str();
    gGlobalOpts.mGroupRole = group_role.c_str();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeRoleHelp().c_str());
  }
};
} // namespace

void
RegisterRoleNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RoleCommand>());
}
