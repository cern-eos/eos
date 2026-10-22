// ----------------------------------------------------------------------
// File: access-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>

namespace {
std::string MakeAccessHelp()
{
  std::ostringstream oss;
  // clang-format off
  oss << "Usage: access ban|unban|allow|unallow|set|rm|ls [OPTIONS]\n"
        "'[eos] access ..' provides the access interface of EOS to "
        "allow/disallow hosts/domains and/or users\n\n"
        "Subcommands:\n"
        "access ban user|group|host|domain <identifier> : ban user, "
        "group, host or domain with identifier <identifier>\n"
        "\t <identifier> : can be a user name, user id, group name, group id, "
        "hostname or IP or domainname\n\n"
        "access unban user|group|host|domain <identifier> : unban user, "
        "group, host or domain with identifier <identifier>\n"
        "\t <identifier> : can be a user name, user id, group name, group id, "
        "hostname or IP or domainname\n\n"
        "access allow user|group|host|domain <identifier> : allows this "
        "user, group, host or domain access\n"
        "\t <identifier> : can be a user name, user id, group name, group id, "
        "hostname or IP or domainname\n\n"
        "access unallow user|group|host|domain <identifier> : unallows "
        "this user,group, host or domain access\n"
        "\t <identifier> : can be a user name, user id, group name, group id, "
        "hostname or IP or domainname\n\n"
        "\t HINT: if you add any 'allow' the instance allows only the listed "
        "identity. A banned identifier will still overrule an allowed "
        "identifier!\n\n"
        "access set redirect <target-host> [r|w|ENOENT|ENONET] : "
        "allows to set a global redirection to <target-host>\n"
        "\t <target-host>      : hostname to which all requests get "
        "redirected\n"
        "\t         [r|w]      : optional set a redirect for read/write "
        "requests seperatly\n"
        "\t      [ENOENT]      : optional set a redirect if a file is not "
        "existing\n"
        "\t      [ENONET]      : optional set a redirect if a file is offline\n"
        "\t                      <taget-hosts> can be structured like "
        "<host>:<port[:<delay-in-ms>] where <delay> holds each request for a "
        "given time before redirecting\n\n"
        "access set stall <stall-time> [r|w|ENOENT|ENONET] : "
        "allows to set a global stall time\n"
        "\t <stall-time> : time in seconds after which clients should "
        "rebounce\n"
        "\t         [r|w]      : optional set stall time for read/write "
        "requests seperatly\n"
        "\t      [ENOENT]      : optional set stall time if a file is not "
        "existing\n"
        "\t      [ENONET]      : optional set stall time if a file is offline\n"
        "\n"
        "access set limit <frequency> rate:{user,group}:{name}:<counter>\n"
        "\t rate:{user:group}:{name}:<counter> : stall the defined user group "
        "for 5s if the <counter> exceeds a frequency of <frequency> in a 5s "
        "interval\n"
        "\t                                      - the instantaneous rate can "
        "exceed this value by 33%%\n"
        "\t              rate:user:*:<counter> : apply to all users based on "
        "user counter\n"
        "\t              rate:group:*:<counter>: apply to all groups based on "
        "group counter\n"
        "\t                                      set <frequency> to 0 (zero) "
        "to continuously stall the user or group\n\n"
        "access set limit <frequency> threads:{*,max,<uid/username>}\n"
        "\t             threads:max            : set the maximum number of "
        "threads running in parallel\n"
        "\t             threads:*              : set the default thread pool "
        "limit for each user\n"
        "\t             threads:<uid/username> : set a specific thread pool "
        "limit for user <username/uid>\n\n"
        "access set limit <nfiles> rate:user:{name}:FindFiles :\n\tset find "
        "query limit to <nfiles> for user {name}\n\n"
        "access set limit <ndirs> rate:user:{name}:FindDirs:\n\tset find query "
        "limit to <ndirs> for user {name}\n\n"
        "access set limit <nfiles> rate:group:{name}:FindFiles :\n\tset find "
        "query limit to <nfiles> for group {name}\n\n"
        "access set limit <ndirs> rate:group:{name}:FindDirs :\n\tset find "
        "query limit to <ndirss> for group {name}\n\n"
        "access set limit <nfiles> rate:user:*:FindFiles :\n\tset default find "
        "query limit to <nfiles> for everybody\n\n"
        "access set limit <ndirs> rate:user:*:FindDirs :\n\tset default find "
        "query limit to <ndirss> for everybody\n\n"
        "\t HINT : rule strength => user-limit >> group-limit >> "
        "wildcard-limit\n\n"
        "access rm redirect [r|w|ENOENT|ENONET] : removes global "
        "redirection\n\n"
        "access rm stall [r|w|ENOENT|ENONET] : removes global "
        "stall time\n\n"
        "access rm limit rate:{user,group}:{name}:<counter> : remove rate "
        "limitation\n\n"
        "access rm limit threads:{max,*,<uid/username>} : remove thread pool "
        "limit\n\n"
        "access ls [-m] [-n] : print banned,unbanned user,group, hosts\n"
        "\t -m : output in monitoring format with <key>=<value>\n"
        "\t -n : don't translate uid/gids to names\n\n"
         "EXAMPLES:\n"
         "  # Ban host foo\n"
         "  eos access ban host foo\n\n"
         "  # Ban domain bar\n"
         "  eos access ban domain bar\n\n"
         "  # Allows user nobody from domain bar\n"
         "  eos access allow domain nobody@bar\n\n"
         "  # Use domain allow as whitelist - e.g. nobody@bar will additionally "
         "allow the nobody user from domain bar!\n"
         "  eos access allow domain -\n\n"
         "  # Allow only domain bar\n"
         "  eos access allow domain bar\n\n"
         "  # Redirect all requests to host foo\n"
         "  eos access set redirect foo\n\n"
         "  # Redirect all requests to host foo:1094 and hold each request for 1000ms\n"
         "  eos access set redirect foo:1094:1000\n\n"
         "  # Remove redirection to previously defined host foo\n"
         "  eos access rm redirect\n\n"
         "  # Stall all clients by 60 seconds\n"
         "  eos access set stall 60\n\n"
         "  # Print all defined access rules\n"
         "  eos access ls\n\n"
         "  # Limit the open for read rate to a frequency of 100 Hz for all users\n"
         "  eos access set limit 100 'rate:user:*:OpenRead'\n\n"
         "  # Limit the open for read rate for the ab user to 0 Hz, to continuously "
         "stall it\n"
         "  eos access set limit 0 rate:user:ab:OpenRead\n\n"
         "  # Limit the stat rate for the zp group to 2kHz\n"
         "  eos access set limit 2000 rate:group:zp:Stat\n\n"
         "  # Limit the thread pool usage to 500 threads per user\n"
         "  eos access set limit 500 'threads:*'\n\n"
         "  # Removes the defined limit\n"
         "  eos access rm limit 'rate:user:*:OpenRead'\n\n"
         "  # Removes the default per user thread pool limit\n"
         "  eos access rm limit 'threads:*'\n\n"
         "  # Add foo*.bar to the list of hosts which are stalled by limit rules "
         "(white list)\n"
         "  eos access stallhosts add stall 'foo*.bar'\n\n"
         "  # Remove foo*.bar from the list of hosts which are stalled by limit "
         "rules (white list)\n"
         "  eos access stallhosts remove stall 'foo*.bar'\n\n"
         "  # Add foo*.bar to the list of hosts which are never stalled by limit "
         "rules (black list)\n"
         "  eos access stallhosts add nostall 'foo*.bar'\n\n"
         "  # Remove foo*.bar from the list of hosts which are never stalled by "
         "limit rules (black list)\n"
         "  eos access stallhosts remove nostall 'foo*.bar'\n";
  // clang-format on
  return oss.str();
}

void ConfigureAccessApp(CLI::App& app)
{
  app.name("access");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeAccessHelp();
      }));
}

class AccessProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "access";
  }
  const char*
  description() const override
  {
    return "Access Interface";
  }
  std::string
  helpText() const override
  {
    return MakeAccessHelp();
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    // access ban|unban|allow|unallow|set|rm|ls ...
    if (args.empty() || wants_help(args[0].c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    const std::string& sub = args[0];
    XrdOucString in = "mgm.cmd=access";
    size_t i = 1;
    auto next = [&](std::string& out) -> bool {
      if (i < args.size()) {
        out = args[i++];
        return true;
      }
      return false;
    };

    auto finish = [&]() {
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      return 0;
    };

    if (sub == "ban" || sub == "unban" || sub == "allow" || sub == "unallow") {
      in += "&mgm.subcmd=";
      in += sub.c_str();
      std::string type, id;
      if (!next(type) || !next(id)) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (type == "host") {
        in += "&mgm.access.host=";
        in += id.c_str();
      } else if (type == "domain") {
        in += "&mgm.access.domain=";
        in += id.c_str();
      } else if (type == "user") {
        in += "&mgm.access.user=";
        in += id.c_str();
      } else if (type == "group") {
        in += "&mgm.access.group=";
        in += id.c_str();
      } else {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      return finish();
    }

    if (sub == "ls") {
      in += "&mgm.subcmd=ls";
      // options: -m, -n
      CLI::App app;
      app.set_help_flag("");
      bool opt_m = false;
      bool opt_n = false;
      app.add_flag("-m", opt_m, "monitor format");
      app.add_flag("-n", opt_n, "numeric ids");
      std::vector<std::string> rest(args.begin() + 1, args.end());
      std::vector<std::string> cli_args = rest;
      std::reverse(cli_args.begin(), cli_args.end());
      try {
        app.parse(cli_args);
      } catch (const CLI::ParseError&) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string option;
      if (opt_m)
        option += "m";
      if (opt_n)
        option += "n";
      if (!option.empty()) {
        in += "&mgm.access.option=";
        in += option.c_str();
      }
      return finish();
    }

    if (sub == "set" || sub == "rm") {
      in += "&mgm.subcmd=";
      in += sub.c_str();
      std::string type;
      if (!next(type)) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string id;
      bool has_id = next(id);
      if (!has_id) {
        if (sub == "rm") {
          id = "dummy";
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
      }
      std::string rtype;
      if (sub == "rm") {
        if (has_id) {
          rtype = id;
        }
      } else {
        next(rtype);
      }

      if (type == "redirect") {
        in += "&mgm.access.redirect=";
        in += id.c_str();
      } else if (type == "stall") {
        in += "&mgm.access.stall=";
        in += id.c_str();
      } else if (type == "limit") {
        in += "&mgm.access.stall=";
        in += id.c_str();
        if (rtype.empty()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        if ((rtype.rfind("rate:user:", 0) == 0 ||
             rtype.rfind("rate:group:", 0) == 0) &&
            (rtype.find(':', 11) != std::string::npos)) {
          in += "&mgm.access.type=";
          in += rtype.c_str();
        } else if (!rtype.empty()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        return finish();
      } else {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }

      if (!rtype.empty()) {
        if (rtype == "r" || rtype == "w" || rtype == "ENONET" ||
            rtype == "ENOENT") {
          in += "&mgm.access.type=";
          in += rtype.c_str();
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
      }
      return finish();
    }

    printHelp();
    global_retc = EINVAL;
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureAccessApp(app);
    const std::string help = app.help();
    eos::console::PrintLegacyHelp(help);
  }
};
} // namespace

void
RegisterAccessProtoNativeCommand()
{
  eos::console::CommandHelpSpec help;
  help.topics = {
      {{"ban"}, "access ban ", "Ban an identity."},
      {{"unban"}, "access unban ", "Remove an identity ban."},
      {{"allow"}, "access allow ", "Allow an identity."},
      {{"unallow"}, "access unallow ", "Remove an identity allowance."},
      {{"ban", "user"}, "access ban ", "Ban user identities."},
      {{"ban", "group"}, "access ban ", "Ban group identities."},
      {{"ban", "host"}, "access ban ", "Ban host identities."},
      {{"ban", "domain"}, "access ban ", "Ban domain identities."},
      {{"unban", "user"}, "access unban ", "Unban user identities."},
      {{"unban", "group"}, "access unban ", "Unban group identities."},
      {{"unban", "host"}, "access unban ", "Unban host identities."},
      {{"unban", "domain"}, "access unban ", "Unban domain identities."},
      {{"allow", "user"}, "access allow ", "Allow user identities."},
      {{"allow", "group"}, "access allow ", "Allow group identities."},
      {{"allow", "host"}, "access allow ", "Allow host identities."},
      {{"allow", "domain"}, "access allow ", "Allow domain identities."},
      {{"unallow", "user"}, "access unallow ", "Unallow user identities."},
      {{"unallow", "group"}, "access unallow ", "Unallow group identities."},
      {{"unallow", "host"}, "access unallow ", "Unallow host identities."},
      {{"unallow", "domain"}, "access unallow ", "Unallow domain identities."},
      {{"set"}, "", "Configure access rules."},
      {{"set", "redirect"}, "access set redirect ", "Redirect requests to another host."},
      {{"set", "stall"}, "access set stall ", "Stall matching requests."},
      {{"set", "limit"},
       "access set limit <frequency> rate:",
       "Set request-rate and query limits."},
      {{"rm"}, "", "Remove access rules."},
      {{"rm", "redirect"}, "access rm redirect ", "Remove request redirection."},
      {{"rm", "stall"}, "access rm stall ", "Remove a request stall."},
      {{"rm", "limit"}, "access rm limit rate:", "Remove request limits."},
      {{"ls"}, "access ls ", "List configured access rules."},
  };
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<AccessProtoCommand>(), std::move(help)));
}
