// ----------------------------------------------------------------------
// File: route-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_route(char*);

namespace {
class RouteProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "route"; }
  const char* description() const override { return "Routing interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_route((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: route [ls|link|unlink]\n"
            "    namespace routing to redirect clients to external instances\n"
            "\n"
            "  route ls [<path>]\n"
            "    list all routes or the one matching for the given path\n"
            "      * as the first character means the node is a master\n"
            "      _ as the first character means the node is offline\n"
            "\n"
            "  route link <path> <dst_host>[:<xrd_port>[:<http_port>]],...\n"
            "    create routing from <path> to destination host. If the xrd_port\n"
            "    is omitted the default 1094 is used, if the http_port is omitted\n"
            "    the default 8000 is used. Several dst_hosts can be specified by\n"
            "    separating them with \",\". The redirection will go to the MGM\n"
            "    from the specified list\n"
            "    e.g route /eos/dummy/ foo.bar:1094:8000\n"
            "\n"
            "  route unlink <path>\n"
            "    remove routing matching path\n");
  }
};
}

void RegisterRouteProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RouteProtoCommand>());
}


