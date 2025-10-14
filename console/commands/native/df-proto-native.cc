// ----------------------------------------------------------------------
// File: df-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>


namespace {
class DfProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "df"; }
  const char* description() const override { return "Get df output"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    // Parse: [-m|-H|-b] [path]
    bool mon=false, human1000=false, raw=false; std::string path;
    for (const auto& a: args) {
      if (a == "-m") mon = true; else if (a == "-H") human1000 = true; else if (a == "-b") raw = true; else path = a;
    }
    XrdOucString in = "mgm.cmd=df";
    if (mon) in += "&mgm.df.option=m";
    if (human1000) in += "&mgm.df.option=H";
    if (raw) in += "&mgm.df.option=b";
    if (!path.empty()) { in += "&mgm.path="; in += abspath(path.c_str()); }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void printHelp() const override {
    fprintf(stdout,
            " usage:\n"
            "df [-m|-H|-b] [path]\n"
            "'[eos] df ...' print unix like 'df' information (1024 base)\n"
            "\n"
            "Options:\n"
            "\n"
            "-m : print in monitoring format\n"
            "-H : print human readable in units of 1000\n"
            "-b : print raw bytes/number values\n");
  }
};
}

void RegisterDfProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DfProtoCommand>());
}


