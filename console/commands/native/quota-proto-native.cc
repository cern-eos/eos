// ----------------------------------------------------------------------
// File: quota-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_protoquota(char*);

namespace {
class QuotaProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "quota"; }
  const char* description() const override { return "Quota System configuration"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_protoquota((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: quota [<path>]\n"
            ": show personal quota for all or only the quota node responsible for <path>\n"
            "quota ls [-n] [-m] [-u <uid>] [-g <gid>] [[-p|x|q] <path>]\n"
            ": list configured quota and quota node(s)\n"
            "                                                                       -p : find closest matching quotanode\n"
            "                                                                       -x : as -p but <path> has to exist\n"
            "                                                                       -q : as -p but <path> has to be a quotanode\n"
            "quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] [[-p] <path>]\n"
            ": set volume and/or inode quota by uid or gid\n"
            "quota rm -u <uid>|-g <gid> [-v] [-i] [[-p] <path>]\n"
            ": remove configured quota type(s) for uid/gid in path\n"
            "quota rmnode [-p] <path>\n"
            ": remove quota node and every defined quota on that node\n"
            "\n"
            "General options:\n"
            "  -m : print information in monitoring <key>=<value> format\n"
            "  -n : don't translate ids, print uid and gid number\n"
            "  -u/--uid <uid> : print information only for uid <uid>\n"
            "  -g/--gid <gid> : print information only for gid <gid>\n"
            "  -p/--path <path> : print information only for path <path> - this can also be given without -p or --path\n"
            "  -v/--volume <bytes> : refer to volume limit in <bytes>\n"
            "  -i/--inodes <inodes> : refer to inode limit in number of <inodes>\n"
            "\n"
            "Notes:\n"
            "  => you have to specify either the user or the group identified by the unix id or the user/group name\n"
            "  => the space argument is by default assumed as 'default'\n"
            "  => you have to specify at least a volume or an inode limit to set quota\n"
            "  => for convenience all commands can just use <path> as last argument omitting the -p|--path e.g. quota ls /eos/ ...\n"
            "  => if <path> is not terminated with a '/' it is assumed to be a file so it won't match the quota node with <path>!/\n");
  }
};
}

void RegisterQuotaProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<QuotaProtoCommand>());
}


