// ----------------------------------------------------------------------
// File: attr-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "common/Utils.hh"
#include "common/SymKeys.hh"
#include <memory>
#include <sstream>

namespace {
class AttrCommand : public IConsoleCommand {
public:
  const char* name() const override { return "attr"; }
  const char* description() const override { return "Attribute Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.empty() || wants_help(args[0].c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string& sub = args[0];
    XrdOucString in = "mgm.cmd=attr";
    // Always request base64 encoding as legacy
    in += "&mgm.enc=b64";
    size_t i = 1;
    auto next = [&](std::string& out)->bool{ if (i<args.size()) { out = args[i++]; return true; } return false; };

    if (sub == "ls") {
      std::string identifier; if (!next(identifier)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString path = PathIdentifier(identifier.c_str(), true).c_str();
      in += "&mgm.subcmd=ls&mgm.path="; in += path;
    } else if (sub == "set" || sub == "link") {
      bool conditional = false;
      if (i < args.size() && args[i] == "-c") { conditional = true; ++i; }
      std::string keyval; if (!next(keyval)) { printHelp(); global_retc = EINVAL; return 0; }
      std::string path; if (!next(path)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString k = keyval.c_str(); XrdOucString v = "";
      int epos = k.find("=");
      if (sub == "link") { k = "sys.attr.link"; v = keyval.c_str(); }
      else if (epos != STR_NPOS) { v = k; v.erase(0, epos+1); k.erase(epos); if (k != "default" && k != "sys.attr.link") { XrdOucString v64; eos::common::SymKey::Base64(v, v64); v = v64; } }
      else { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      if (conditional) in += "&mgm.option=c";
      in += "&mgm.subcmd=set&mgm.attr.key="; in += k; in += "&mgm.attr.value="; in += v; in += "&mgm.path="; in += p;
    } else if (sub == "get") {
      std::string key, path; if (!next(key) || !next(path)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      in += "&mgm.subcmd=get&mgm.attr.key="; in += key.c_str(); in += "&mgm.path="; in += p;
    } else if (sub == "rm" || sub == "unlink") {
      std::string key, path; if (!next(key) || !next(path)) { printHelp(); global_retc = EINVAL; return 0; }
      if (sub == "unlink") { key = "sys.attr.link"; path = key; key = "sys.attr.link"; /* value is path */ }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      in += "&mgm.subcmd=rm&mgm.attr.key="; in += key.c_str(); in += "&mgm.path="; in += p;
    } else if (sub == "fold") {
      std::string identifier; if (!next(identifier)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = PathIdentifier(identifier.c_str(), true).c_str();
      in += "&mgm.subcmd=fold&mgm.path="; in += p;
    } else { printHelp(); global_retc = EINVAL; return 0; }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterAttrNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AttrCommand>());
}


