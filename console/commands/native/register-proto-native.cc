// ----------------------------------------------------------------------
// File: register-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_protoregister(char*);

namespace {
class RegisterProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "register"; }
  const char* description() const override { return "Register a file"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_protoregister((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            "Usage: register [-u] <path> {tag1,tag2,tag3...}\n"
            "          :  when called without the -u flag the parent has to exist while the basename should not exist\n"
            "       -u :  if the file exists this will update all the provided meta-data of a file\n\n"
            "       tagN is optional, but can be one or many of: \n"
            "             size=100\n"
            "             uid=101 | username=foo\n"
            "             gid=102 | username=bar\n"
            "             checksum=abcdabcd\n"
            "             layoutid=00100112\n"
            "             location=1 location=2 ...\n"
            "             mode=777\n"
            "             btime=1670334863.101232\n"
            "             atime=1670334863.101232\n"
            "             ctime=1670334863.110123\n"
            "             mtime=1670334863.11234d\n"
            "             attr=\"sys.acl=u:100:rwx\"\n"
            "             attr=\"user.md=private\"\n"
            "             path=\"/eos/newfile\"   # can be used instead of the regular path argument of the path\n"
            "             atimeifnewer=1670334863.101233  # only update if this atime is newer than the existing one!\n");
  }
};
}

void RegisterRegisterProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RegisterProtoCommand>());
}


