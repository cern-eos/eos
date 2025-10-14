// ----------------------------------------------------------------------
// File: convert-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_convert(char*);

namespace {
class ConvertProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "convert"; }
  const char* description() const override { return "Convert Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_convert((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: convert <subcomand>                         \n"
            "  convert config list|set [<key>=<value>]          \n"
            "    list: list converter configuration parameters and status\n"
            "    set : set converter configuration parameters. Options:\n"
            "      status               : \"on\" or \"off\"     \n"
            "      max-thread-pool-size : max number of threads in converter pool [default 100]\n"
            "      max-queue-size       : max number of queued conversion jobs [default 1000]\n"
            "\n"
            "  convert list                                     \n"
            "    list conversion jobs                           \n"
            "\n"
            "  convert clear                                    \n"
            "    clear list of jobs stored in the backend       \n"
            "\n"
            "  convert file <identifier> <conversion>           \n"
            "    schedule a file conversion                     \n"
            "    <identifier> = fid|fxid|path                   \n"
            "    <conversion> = <layout:replica> [space] [placement] [checksum]\n"
            "\n"
            "  convert rule <identifier> <conversion>           \n"
            "    apply a conversion rule on the given directory \n"
            "    <identifier> = cid|cxid|path                   \n"
            "    <conversion> = <layout:replica> [space] [placement] [checksum]\n");
  }
};
}

void RegisterConvertProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConvertProtoCommand>());
}


