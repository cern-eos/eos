// ----------------------------------------------------------------------
// File: devices-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>


namespace {
class DevicesProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "devices"; }
  const char* description() const override { return "Get Device Information"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    // devices ls [-l|-m] [--refresh]
    XrdOucString in = "mgm.cmd=devices&mgm.subcmd=ls";
    std::string option;
    for (const auto& a: args) {
      if (a == "-l") option = "l"; else if (a == "-m") option = "m"; else if (a == "--refresh") in += "&mgm.refresh=true";
    }
    if (!option.empty()) { in += "&mgm.option="; in += option.c_str(); }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: devices ls [-l] [-m] [--refresh]\n"
            "                                       : without option prints statistics per space of all storage devices used based on S.M.A.R.T information\n"
            "                                    -l : prints S.M.A.R.T information for each configured filesystem\n"
            "                                    -m : print montiroing output format (key=val)\n"
            "                             --refresh : forces to reparse the current available S.M.A.R.T information and output this\n"
            "\n"
            "                                  JSON : to retrieve JSON output, use 'eos --json devices ls' !\n");
  }
};
}

void RegisterDevicesProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DevicesProtoCommand>());
}



