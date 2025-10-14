// ----------------------------------------------------------------------
// File: sched-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_proto_sched(char*);

namespace {
class SchedProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "sched"; }
  const char* description() const override { return "Configure scheduler options"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_proto_sched((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            " Usage:\n"
            " sched configure type <schedtype>\n"
            "\t <schedtype> is one of roundrobin,weightedrr,tlrr,random,weightedrandom,geo\n"
            "\t if configured via space; space takes precedence\n"
            " sched configure weight <space> <fsid> <weight>\n"
            "\t configure weight for a given fsid in the given space\n"
            " sched configure show type [spacename]\n"
            "\t show existing configured scheduler; optionally for space\n"
            " sched configure forcerefresh [spacename]\n"
            "\t Force refresh scheduler internal state\n"
            " ls <spacename> <bucket|disk|all>\n");
  }
};
}

void RegisterSchedProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<SchedProtoCommand>());
}


