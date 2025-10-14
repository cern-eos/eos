// ----------------------------------------------------------------------
// File: recycle-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

 

namespace {
class RecycleProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "recycle"; }
  const char* description() const override { return "Recycle Bin Functionality"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    XrdOucString in = "mgm.cmd=recycle";
    if (args.empty()) { in += "&mgm.subcmd=stat"; }
    else if (args[0] == "ls") { in += "&mgm.subcmd=ls"; for (size_t i=1;i<args.size();++i){ const auto& a=args[i]; if(a=="-g") in+="&mgm.option=g"; else if(a=="-m") in+="&mgm.option=m"; else if(a=="-n") in+="&mgm.option=n"; else { in+="&mgm.date="; in+=a.c_str(); } } }
    else if (args[0] == "purge") { in += "&mgm.subcmd=purge"; for (size_t i=1;i<args.size();++i){ const auto& a=args[i]; if(a=="-g") in+="&mgm.option=g"; else if(a=="-k"&&i+1<args.size()){ in+="&mgm.key="; in+=args[++i].c_str(); } else { in+="&mgm.date="; in+=a.c_str(); } } }
    else if (args[0] == "restore") { in += "&mgm.subcmd=restore"; bool haveKey=false; for (size_t i=1;i<args.size();++i){ const auto& a=args[i]; if(a=="-p") in+="&mgm.parents=1"; else if(a=="-f"||a=="--force-original-name") in+="&mgm.force=1"; else if(a=="-r"||a=="--restore-versions") in+="&mgm.versions=1"; else { in+="&mgm.key="; in+=a.c_str(); haveKey=true; } } if(!haveKey){ printHelp(); global_retc=EINVAL; return 0; } }
    else if (args[0] == "config") { in += "&mgm.subcmd=config"; for (size_t i=1;i<args.size();++i){ const auto& a=args[i]; if(a=="--add-bin"||a=="--remove-bin"){ if(i+1<args.size()){ in+="&mgm.config="; in+=a.c_str(); in+="&mgm.subtree="; in+=args[++i].c_str(); } } else if(a=="--lifetime"&&i+1<args.size()){ in+="&mgm.lifetime="; in+=args[++i].c_str(); } else if(a=="--ratio"&&i+1<args.size()){ in+="&mgm.ratio="; in+=args[++i].c_str(); } else if(a=="--size"&&i+1<args.size()){ in+="&mgm.size="; in+=args[++i].c_str(); } else if(a=="--inodes"&&i+1<args.size()){ in+="&mgm.inodes="; in+=args[++i].c_str(); } } }
    else { fprintf(stderr, "error: unsupported recycle subcommand\n"); global_retc = EINVAL; return 0; }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true); return 0;
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: recycle [ls|purge|restore|config ...]\n"
            "    provides recycle bin functionality\n"
            "  recycle [-m]\n"
            "    print status of recycle bin and config status if executed by root\n"
            "    -m     : display info in monitoring format\n\n"
            "  recycle ls [-g|<date> [<limit>]] [-m] [-n]\n"
            "    list files in the recycle bin\n"
            "    -g     : list files of all users (if done by root or admin)\n"
            "    <date> : can be <year>, <year>/<month> or <year>/<month>/<day> or <year>/<month>/<day>/<index>\n"
            "   <limit> : maximum number of entries to return when listing\n"
            "             e.g.: recycle ls 2018/08/12\n"
            "    -m     : display info in monitoring format\n"
            "    -n     : display numeric uid/gid(s) instead of names\n\n"
            "  recycle purge [-g|<date>] [-k <key>]\n"
            "    purge files in the recycle bin\n"
            "    -g       : empty recycle bin of all users (if done by root or admin)\n"
            "    -k <key> : purge only the given key\n"
            "    <date>   : can be <year>, <year>/<month> or <year>/<month>/<day>\n\n"
            "  recycle restore [-p] [-f|--force-original-name] [-r|--restore-versions] <recycle-key>\n"
            "    undo the deletion identified by the <recycle-key>\n"
            "    -p : create all missing parent directories\n"
            "    -f : move deleted files/dirs back to their original location (otherwise\n"
            "          the key entry will have a <.inode> suffix)\n"
            "     -r : restore all previous versions of a file\n\n"
            "  recycle config [--add-bin|--remove-bin] <sub-tree>\n"
            "    --add-bin    : enable recycle bin for deletions in <sub-tree>\n"
            "    --remove-bin : disable recycle bin for deletions in <sub-tree>\n\n"
            "  recycle config --lifetime <seconds>\n"
            "    configure FIFO lifetime for the recycle bin\n\n"
            "  recycle config --ratio <0..1.0>\n"
            "    configure the volume/inode keep ratio. E.g: 0.8 means files will only\n"
            "    be recycled if more than 80%% of the volume/inodes quota is used. The\n"
            "    low watermark is by default 10%% below the given ratio.\n\n"
            "  recycle config --size <value>[K|M|G]\n"
            "    configure the quota for the maximum size of the recycle bin. \n"
            "    If no unit is set explicitly then we assume bytes.\n\n"
            "  recycle config --inodes <value>[K|M|G]\n"
            "    configure the quota for the maximum number of inodes in the recycle\n"
            "    bin.\n");
  }
};
}

void RegisterRecycleProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RecycleProtoCommand>());
}


