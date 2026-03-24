// ----------------------------------------------------------------------
// File: register-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <memory>
#include <sstream>

namespace {
std::string MakeRegisterHelp()
{
  return "Usage: register [-u] <path> [tag1=val1 tag2=val2 ...]\n\n"
         "  -u  update existing file metadata (if file exists)\n\n"
         "Tags: size=100, uid=101|username=foo, gid=102|groupname=bar,\n"
         "  checksum=..., layoutid=..., location=1,2,..., mode=777,\n"
         "  btime=..., atime=..., ctime=..., mtime=..., path=...,\n"
         "  xattr=..., attr=\"sys.acl=u:100:rwx\", atimeifnewer=...\n";
}

void ConfigureRegisterApp(CLI::App& app)
{
  app.name("register");
  app.description("Register a file");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRegisterHelp();
      }));
}

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
    CLI::App app;
    ConfigureRegisterApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterRegisterProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RegisterProtoCommand>());
}
