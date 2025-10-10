// ----------------------------------------------------------------------
// File: ProtoNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

// Legacy functions
extern int com_protoaccess(char*);  // maps to command name "access"
extern int com_acl(char*);          // "acl"
extern int com_protoconfig(char*);  // "config"
extern int com_convert(char*);      // "convert"
extern int com_proto_devices(char*);// "devices"
extern int com_protodf(char*);      // "df"
extern int com_proto_find(char*);   // "find" and "newfind"
extern int com_protofs(char*);      // "fs"
extern int com_proto_fsck(char*);   // "fsck"
extern int com_protogroup(char*);   // "group"
extern int com_protoio(char*);      // "io"
extern int com_protonode(char*);    // "node"
extern int com_ns(char*);           // "ns"
extern int com_qos(char*);          // "qos"
extern int com_protoquota(char*);   // "quota"
extern int com_protorecycle(char*); // "recycle"
extern int com_protoregister(char*);// "register"
extern int com_protorm(char*);      // "rm"
extern int com_route(char*);        // "route"
extern int com_proto_token(char*);  // "token"
extern int com_proto_space(char*);  // "space"
extern int com_proto_sched(char*);  // "sched"

namespace {
class Wrapper : public IConsoleCommand {
public:
  using CFunc = int(*)(char*);
  Wrapper(const char* n, const char* d, CFunc f) : mName(n), mDesc(d?d:""), mFunc(f) {}
  const char* name() const override { return mName.c_str(); }
  const char* description() const override { return mDesc.c_str(); }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return mFunc((char*)joined.c_str());
  }
  void printHelp() const override {}
private:
  std::string mName; std::string mDesc; CFunc mFunc;
};
}

void RegisterProtoNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("access",   "Access Interface",                  &com_protoaccess));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("acl",      "Acl Interface",                     &com_acl));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("config",   "Configuration System",              &com_protoconfig));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("convert",  "Convert Interface",                 &com_convert));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("devices",  "Get Device Information",            &com_proto_devices));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("df",       "Get df output",                     &com_protodf));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("find",     "Find files/directories",            &com_proto_find));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("newfind",  "Find files/directories (new)",      &com_proto_find));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("fs",       "File System configuration",         &com_protofs));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("fsck",     "File System Consistency Checking",  &com_proto_fsck));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("group",    "Group configuration",               &com_protogroup));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("io",       "IO Interface",                      &com_protoio));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("node",     "Node configuration",                &com_protonode));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("ns",       "Namespace Interface",               &com_ns));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("qos",      "QoS configuration",                 &com_qos));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("quota",    "Quota System configuration",        &com_protoquota));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("recycle",  "Recycle Bin Functionality",         &com_protorecycle));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("register", "Register a file",                   &com_protoregister));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("rm",       "Remove a file",                     &com_protorm));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("route",    "Routing interface",                 &com_route));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("token",    "Token interface",                   &com_proto_token));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("space",    "Space configuration",               &com_proto_space));
  CommandRegistry::instance().reg(std::make_unique<Wrapper>("sched",    "Configure scheduler options",        &com_proto_sched));
}


