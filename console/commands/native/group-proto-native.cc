// ----------------------------------------------------------------------
// File: group-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_protogroup(char*);

namespace {
class GroupProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "group"; }
  const char* description() const override { return "Group configuration"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_protogroup((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            " usage:\n\n"
            "group ls [-s] [-g <depth>] [-b|--brief] [-m|-l|--io] [<groups>] : list groups\n"
            "\t <groups> : list <groups> only, where <groups> is a substring match and can be a comma seperated list\n"
            "\t       -s : silent mode\n"
            "\t       -g : geo output - aggregate group information along the instance geotree down to <depth>\n"
            "\t       -b : brief output\n"
            "\t       -m : monitoring key=value output format\n"
            "\t       -l : long output - list also file systems after each group\n"
            "\t     --io : print IO statistics for the group\n"
            "\t     --IO : print IO statistics for each filesystem\n\n"
            "group rm <group-name> : remove group\n\n"
            "group set <group-name> on|drain|off : activate/drain/deactivate group\n"
            "\t  => when a group is (re-)enabled, the drain pull flag is recomputed for all filesystems within a group\n"
            "\t  => when a group is (re-)disabled, the drain pull flag is removed from all members in the group\n"
            "\t  => when a group is in drain, all the filesystems in the group will be drained to other groups\n");
  }
};
}

void RegisterGroupProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<GroupProtoCommand>());
}


