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
  const char* name() const override { return "access"; }
  const char* description() const override { return "Access Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    // access ban|unban|allow|unallow|set|rm|ls ...
    if (args.empty() || wants_help(args[0].c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string& sub = args[0];
    XrdOucString in = "mgm.cmd=access";
    size_t i = 1;
    auto next = [&](std::string& out)->bool{ if (i<args.size()) { out = args[i++]; return true; } return false; };

    auto finish = [&](){ global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true); return 0; };

    if (sub == "ban" || sub == "unban" || sub == "allow" || sub == "unallow") {
      in += "&mgm.subcmd="; in += sub.c_str();
      std::string type, id; if (!next(type) || !next(id)) { printHelp(); global_retc = EINVAL; return 0; }
      if (type == "host") { in += "&mgm.access.host="; in += id.c_str(); }
      else if (type == "domain") { in += "&mgm.access.domain="; in += id.c_str(); }
      else if (type == "user") { in += "&mgm.access.user="; in += id.c_str(); }
      else if (type == "group") { in += "&mgm.access.group="; in += id.c_str(); }
      else { printHelp(); global_retc = EINVAL; return 0; }
      return finish();
    }

    if (sub == "ls") {
      in += "&mgm.subcmd=ls";
      // options: -m, -n
      ConsoleArgParser p; p.addOption({"", 'm', false, false, "", "monitor format", ""}); p.addOption({"", 'n', false, false, "", "numeric ids", ""});
      std::vector<std::string> rest(args.begin()+1, args.end()); auto r = p.parse(rest);
      std::string option; if (r.has("m")) option += "m"; if (r.has("n")) option += "n";
      if (!option.empty()) { in += "&mgm.access.option="; in += option.c_str(); }
      return finish();
    }

    if (sub == "set" || sub == "rm") {
      in += "&mgm.subcmd="; in += sub.c_str();
      std::string type; if (!next(type)) { printHelp(); global_retc = EINVAL; return 0; }
      std::string id; if (!next(id)) { if (sub == "rm") { /* rm type-as-id */ } else { printHelp(); global_retc = EINVAL; return 0; } }
      std::string rtype; if (sub == "rm") { rtype = id; } else { next(rtype); }

      if (type == "redirect") {
        in += "&mgm.access.redirect="; in += id.c_str();
      } else if (type == "stall") {
        in += "&mgm.access.stall="; in += id.c_str();
      } else if (type == "limit") {
        in += "&mgm.access.stall="; in += id.c_str();
        if (rtype.rfind("rate:user:",0)==0 || rtype.rfind("rate:group:",0)==0) { in += "&mgm.access.type="; in += rtype.c_str(); }
        else if (!rtype.empty()) { printHelp(); global_retc = EINVAL; return 0; }
        return finish();
      } else { printHelp(); global_retc = EINVAL; return 0; }

      if (!rtype.empty()) {
        if (rtype == "r" || rtype == "w" || rtype == "ENONET" || rtype == "ENOENT") { in += "&mgm.access.type="; in += rtype.c_str(); }
        else { printHelp(); global_retc = EINVAL; return 0; }
      }
      return finish();
    }

    printHelp(); global_retc = EINVAL; return 0;
  }
  void printHelp() const override {}
};
}

void RegisterAccessProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AccessProtoCommand>());
}


