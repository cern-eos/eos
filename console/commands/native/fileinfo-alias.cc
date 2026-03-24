// ----------------------------------------------------------------------
// File: fileinfo-alias.cc
// Purpose: Provide 'fileinfo' alias for 'file info ...'
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <vector>

namespace {
class FileInfoAliasCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fileinfo";
  }
  const char*
  description() const override
  {
    return "Alias for 'file info'";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    IConsoleCommand* fileCmd = CommandRegistry::instance().find("file");
    if (!fileCmd) {
      fprintf(stderr, "error: 'file' command not available\n");
      return -1;
    }
    std::vector<std::string> forwarded;
    forwarded.reserve(args.size() + 1);
    forwarded.emplace_back("info");
    forwarded.insert(forwarded.end(), args.begin(), args.end());
    return fileCmd->run(forwarded, ctx);
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: fileinfo <path> [options] (alias for 'file info')\n");
  }
};
} // namespace

void
RegisterFileInfoAliasCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FileInfoAliasCommand>());
}
