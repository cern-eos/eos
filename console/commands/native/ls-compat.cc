// ----------------------------------------------------------------------
// File: ls-compat.cc
// Purpose: Provide legacy com_ls symbol delegating to native LsCommand
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <string>
#include <vector>

int
com_ls(char* arg)
{
  std::vector<std::string> argv;
  if (arg && *arg) {
    std::string s(arg);
    std::string cur;
    for (size_t i = 0; i < s.size(); ++i) {
      if (s[i] == ' ') {
        if (!cur.empty()) {
          argv.push_back(cur);
          cur.clear();
        }
      } else {
        cur.push_back(s[i]);
      }
    }
    if (!cur.empty())
      argv.push_back(cur);
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

  IConsoleCommand* cmd = CommandRegistry::instance().find("ls");
  if (!cmd)
    return -1;
  return cmd->run(argv, ctx);
}
