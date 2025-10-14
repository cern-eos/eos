// ----------------------------------------------------------------------
// File: cp-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

extern int com_cp(char*);

namespace {
class CpCommand : public IConsoleCommand {
public:
  const char* name() const override { return "cp"; }
  const char* description() const override { return "Copy files"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    ConsoleArgParser p;
    p.addOption({"atomic", '\0', false, false, "", "atomic upload", ""});
    p.addOption({"rate", '\0', true, false, "<rate>", "rate limit", ""});
    p.addOption({"streams", '\0', true, false, "<n>", "streams", ""});
    p.addOption({"depth", '\0', true, false, "<d>", "depth", ""});
    p.addOption({"checksum", '\0', false, false, "", "print checksums", ""});
    p.addOption({"", 'a', false, false, "", "append", ""});
    p.addOption({"", 'p', false, false, "", "mkdir -p destination path", ""});
    p.addOption({"", 'n', false, false, "", "hide progress", ""});
    p.addOption({"", 'S', false, false, "", "print summary", ""});
    p.addOption({"debug", 'd', true, false, "[lvl]", "debug", ""});
    p.addOption({"silent", 's', false, false, "", "silent", ""});
    p.addOption({"no-overwrite", 'k', false, false, "", "no overwrite", ""});
    p.addOption({"preserve", 'P', false, false, "", "preserve times", ""});
    p.addOption({"recursive", 'r', false, false, "", "recursive", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.size() < 2) {
      fprintf(stdout, "Usage: cp [options] <src> <dst>\n"); global_retc = EINVAL; return 0;
    }
    const std::string& src = pos[pos.size()-2];
    const std::string& dst = pos[pos.size()-1];
    auto isEOSPath = [&](const std::string& s){ return (!s.empty() && s[0] == '/') || s.rfind("/eos/",0)==0; };
    if (isEOSPath(src) && isEOSPath(dst) && !r.has("recursive")) {
      XrdOucString in = "mgm.cmd=file"; in += "&mgm.subcmd=copy";
      XrdOucString sp = abspath(src.c_str()); XrdOucString dp = abspath(dst.c_str());
      in += "&mgm.path="; in += sp;
      XrdOucString opt = "";
      if (r.has("no-overwrite") || r.has("k")) opt += "k";
      if (r.has("silent") || r.has("s")) opt += "s";
      if (r.has("checksum")) opt += "c";
      if (opt.length()) { in += "&mgm.file.option="; in += opt; }
      in += "&mgm.file.target="; in += dp;
      if (r.has("p")) {
        std::string parent = dp.c_str(); auto posSlash = parent.find_last_of('/'); if (posSlash != std::string::npos && posSlash > 0) parent = parent.substr(0, posSlash);
        XrdOucString min = "mgm.cmd=mkdir&mgm.option=p&mgm.path="; min += parent.c_str();
        ctx.outputResult(ctx.clientCommand(min, false, nullptr), true);
      }
      global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
      return 0;
    }
    // Fallback to legacy for non-EOS protocols, recursive or wildcard cases
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_cp((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterCpNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CpCommand>());
}


