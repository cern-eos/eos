// ----------------------------------------------------------------------
// File: register-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

namespace {
class RegisterProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "register";
  }
  const char*
  description() const override
  {
    return "Register a file";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // Build mgm request from tags
    // (uid/gid/size/path/xattr/ctime/mtime/atime/atimeifnewer/mode/location/layoutid/checksum)
    XrdOucString in = "mgm.cmd=register";
    bool update = false;
    std::vector<std::string> tags;
    for (const auto& a : args) {
      if (a == "-u") {
        update = true;
        continue;
      }
      tags.push_back(a);
    }
    if (update)
      in += "&mgm.update=1";
    for (const auto& t : tags) {
      size_t eq = t.find('=');
      if (eq == std::string::npos) {
        in += "&mgm.path=";
        in += abspath(t.c_str());
        continue;
      }
      std::string k = t.substr(0, eq), v = t.substr(eq + 1);
      if (k == "uid" || k == "username") {
        in += "&mgm.owner.";
        in += k.c_str();
        in += "=";
        in += v.c_str();
      } else if (k == "gid" || k == "groupname") {
        in += "&mgm.owner.";
        in += k.c_str();
        in += "=";
        in += v.c_str();
      } else if (k == "size" || k == "mode" || k == "layoutid" ||
                 k == "checksum") {
        in += "&mgm.";
        in += k.c_str();
        in += "=";
        in += v.c_str();
      } else if (k == "location") {
        in += "&mgm.location=";
        in += v.c_str();
      } else if (k == "path") {
        in += "&mgm.path=";
        in += abspath(v.c_str());
      } else if (k == "xattr") {
        in += "&mgm.xattr=";
        in += v.c_str();
      } else if (k == "ctime" || k == "mtime" || k == "btime" || k == "atime") {
        in += "&mgm.";
        in += k.c_str();
        in += "=";
        in += v.c_str();
      } else if (k == "atimeifnewer") {
        in += "&mgm.atime=";
        in += v.c_str();
        in += "&mgm.atimeifnewer=1";
      } else { /* ignore unknown */
      }
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: register [-u] <path> {tag1,tag2,tag3...}\n"
                    "          :  when called without the -u flag the parent "
                    "has to exist while the basename should not exist\n"
                    "       -u :  if the file exists this will update all the "
                    "provided meta-data of a file\n\n"
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
                    "             path=\"/eos/newfile\"   # can be used "
                    "instead of the regular path argument of the path\n"
                    "             atimeifnewer=1670334863.101233  # only "
                    "update if this atime is newer than the existing one!\n");
  }
};
} // namespace

void
RegisterRegisterProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RegisterProtoCommand>());
}
