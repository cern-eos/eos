// ----------------------------------------------------------------------
// File: qos-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

 

namespace {
class QosProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "qos"; }
  const char* description() const override { return "QoS configuration"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    fprintf(stderr, "error: native 'qos' implementation missing; legacy fallback removed\n");
    global_retc = EINVAL; return 0;
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: qos list [<name>]               : list available QoS classes\n"
            "                                         If <name> is provided, list the properties of the given class\n"
            "       qos get <identifier> [<key>]    : get QoS property of item\n"
            "                                         If no <key> is provided, defaults to 'all'\n"
            "       qos set <identifier> <class>    : set QoS class of item\n\n"
            "Note: <identifier> = fid|fxid|cid|cxid|path\n"
            "      Recognized `qos get` keys: all | cdmi | checksum | class | disksize |\n"
            "                                 layout | id | path | placement | replica | size\n");
  }
};
}

void RegisterQosProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<QosProtoCommand>());
}


