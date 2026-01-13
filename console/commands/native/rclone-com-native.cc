// ----------------------------------------------------------------------
// File: rclone-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_rclone(char* arg1);

namespace {
class RClone {
public:
  int
  Exec(const std::string& joinedArgs)
  {
    return com_rclone((char*)joinedArgs.c_str());
  }
};

class RcloneCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "rclone";
  }
  const char*
  description() const override
  {
    return "RClone like command";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    return RClone().Exec(joined);
  }
  void
  printHelp() const override
  {
    fprintf(
        stdout,
        "Usage: rclone copy src-dir dst-dir [--delete] [--noupdate] [--dryrun] "
        "[--atomic] [--versions] [--hidden] [-v|--verbose] [-s|--silent]\n"
        "                                       : copy from source to "
        "destination [one-way sync]\n"
        "       rclone sync dir1 dir2 [--delete] [--noupdate] [--dryrun] "
        "[--atomic] [--versions] [--hidden] [-v|--verbose] [-s|--silent]\n"
        "                                       : bi-directional sync based on "
        "modification times\n"
        "                              --delete : delete based on mtimes "
        "(currently unsupported)!\n"
        "                            --noupdate : never update files, only "
        "create new ones!\n"
        "                            --dryrun   : simulate the command and "
        "show all actions, but don't do it!\n"
        "                            --atomic   : copy/sync also EOS atomic "
        "files\n"
        "                            --versions : copy/sync also EOS atomic "
        "files\n"
        "                            --hidden   : copy/sync also hidden "
        "files/directories\n"
        "                         -v --verbose  : display all actions, not "
        "only a summary\n"
        "                         -s --silent   : only show errors\n");
  }
};
} // namespace

void
RegisterRcloneNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RcloneCommand>());
}
