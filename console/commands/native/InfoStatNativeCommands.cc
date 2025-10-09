// ----------------------------------------------------------------------
// File: InfoStatNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include <XrdPosix/XrdPosixXrootd.hh>
#include <memory>
#include <sstream>
// legacy command symbols
extern int com_file(char*);

namespace {
class InfoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "info"; }
  const char* description() const override { return "Retrieve file or directory information"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override { std::ostringstream oss; oss << "info"; for (const auto& a : args) { oss << ' ' << a; } std::string joined = oss.str(); return com_file((char*)joined.c_str()); }
  void printHelp() const override {}
};

class StatCommand : public IConsoleCommand {
public:
  const char* name() const override { return "stat"; }
  const char* description() const override { return "Run 'stat' on a file or directory"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    // Reuse legacy stat logic inline for simplicity
    XrdOucString option = "";
    XrdOucString path = "";
    for (const auto& a : args) {
      if (a == "--help" || a == "-h") {
        fprintf(stdout, "usage: stat [-f|-d]    <path>                                                  :  stat <path>\n");
        fprintf(stdout, "                    -f : checks if <path> is a file\n");
        fprintf(stdout, "                    -d : checks if <path> is a directory\n");
        global_retc = EINVAL; return 0;
      }
      if (!a.empty() && a[0] == '-') { XrdOucString p = a.c_str(); while (p.replace("-", "")){} option += p; }
      else { if (!path.length()) path = a.c_str(); }
    }
    if (!path.length()) path = gPwd.c_str();
    if ((option.length()) && ((option != "f") && (option != "d"))) {
      fprintf(stderr, "error: unknown option \"%s\"\n", option.c_str()); global_retc = EINVAL; return 0;
    }
    path = abspath(path.c_str());
    XrdOucString url = serveruri.c_str(); url += "/"; url += path;
    struct stat buf; XrdOucString sizestring;
    if (!XrdPosixXrootd::Stat(url.c_str(), &buf)) {
      if ((option.find("f") != STR_NPOS)) { global_retc = S_ISREG(buf.st_mode) ? 0 : 1; return 0; }
      if ((option.find("d") != STR_NPOS)) { global_retc = S_ISDIR(buf.st_mode) ? 0 : 1; return 0; }
      fprintf(stdout, "  File: '%s'", path.c_str());
      if (S_ISDIR(buf.st_mode)) {
        fprintf(stdout, " directory\n");
      } else if (S_ISREG(buf.st_mode)) {
        fprintf(stdout, "  Size: %llu            %s", (unsigned long long) buf.st_size,
                eos::common::StringConversion::GetReadableSizeString(sizestring,
                  (unsigned long long) buf.st_size, "B"));
        fprintf(stdout, " regular file\n");
      } else {
        fprintf(stdout, " symbolic link\n");
      }
      global_retc = 0;
    } else {
      fprintf(stderr, "error: failed to stat %s\n", path.c_str()); global_retc = EFAULT; return 0;
    }
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterInfoStatNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<InfoCommand>());
  CommandRegistry::instance().reg(std::make_unique<StatCommand>());
}


