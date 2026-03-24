// ----------------------------------------------------------------------
// File: role-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
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
    if (args.empty() || wants_help(args[0].c_str())) {
      fprintf(stderr, "Usage: role <user-role> [<group-role>]\n");
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
  }
};
} // namespace

void
RegisterRoleNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RoleCommand>());
}
