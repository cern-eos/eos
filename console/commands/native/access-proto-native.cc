// ----------------------------------------------------------------------
// File: access-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
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
      ConsoleArgParser p;
      p.addOption({"", 'm', false, false, "", "monitor format", ""});
      p.addOption({"", 'n', false, false, "", "numeric ids", ""});
      std::vector<std::string> rest(args.begin() + 1, args.end());
      auto r = p.parse(rest);
      if (!r.errors.empty() || !r.unknownTokens.empty() || !r.positionals.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string option;
      if (r.has("m"))
        option += "m";
      if (r.has("n"))
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
    fprintf(
        stderr,
        " Usage:\n"
        "access ban|unban|allow|unallow|set|rm|ls [OPTIONS]\n"
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
        "Examples:\n"
        " access ban host foo                            : Ban host foo\n"
        " access ban domain bar                          : Ban domain bar\n"
        " access allow domain nobody@bar                 : Allows user nobody "
        "from domain bar\n"
        " access allow domain -                          : use domain allow as "
        "whitelist - e.g. nobody@bar will additionally allow the nobody user "
        "from domain bar!\n"
        " access allow domain bar                        : Allow only domain "
        "bar\n"
        " access set redirect foo                        : Redirect all "
        "requests to host foo\n"
        " access set redirect foo:1094:1000              : Redirect all "
        "requests to host foo:1094 and hold each reqeust for 1000ms\n"
        " access rm redirect                             : Remove redirection "
        "to previously defined host foo\n"
        " access set stall 60                            : Stall all clients "
        "by 60 seconds\n"
        " access ls                                      : Print all defined "
        "access rules\n"
        " access set limit 100  rate:user:*:OpenRead     : Limit the open for "
        "read rate to a frequency of 100 Hz for all users\n"
        " access set limit 0    rate:user:ab:OpenRead    : Limit the open for "
        "read rate for the ab user to 0 Hz, to continuously stall it\n"
        " access set limit 2000 rate:group:zp:Stat       : Limit the stat rate "
        "for the zp group to 2kHz\n"
        " access set limit 500 threads:*                 : Limit the thread "
        "pool usage to 500 threads per user\n"
        " access rm limit rate:user:*:OpenRead           : Removes the defined "
        "limit\n"
        " access rm limit threads:*                      : Removes the default "
        "per user thread pool limit\n"
        " access stallhosts add stall foo*.bar           : Add foo*.bar to the "
        "list of hosts which are stalled by limit rules (white list)\n"
        " access stallhosts remove stall foo*.bar        : Remove foo*.bar "
        "from the list of hosts which are stalled by limit rules (white list)\n"
        " access stallhosts add nostall foo*.bar         : Add foo*.bar to the "
        "list of hosts which are never stalled by limit rules (black list)\n"
        " access stallhosts remove nostall foo*.bar      : Remove foo*.bar "
        "from the list of hosts which are never stalled by limit rules (black "
        "list)\n");
  }
};
} // namespace

void
RegisterAccessProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AccessProtoCommand>());
}
