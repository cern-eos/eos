// ----------------------------------------------------------------------
// File: squash-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_squash(char*);

namespace {
class SquashCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "squash";
  }
  const char*
  description() const override
  {
    return "Run squashfs utility function";
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
    return com_squash((char*)joined.c_str());
  }
  void
  printHelp() const override
  {
    fprintf(
        stdout,
        "Usage: squash new <path>                                              "
        "    : create a new squashfs under <path>\n"
        "\n"
        "       squash pack [-f] <path>                                        "
        "    : pack a squashfs image\n"
        "                                                                      "
        "      -f will recreate the package but keeps the symbolic link "
        "locally\n"
        "\n"
        "       squash unpack [-f] <path>                                      "
        "    : unpack a squashfs image for modification\n"
        "                                                                      "
        "      -f will atomically update the local package\n"
        "\n"
        "       squash info <path>                                             "
        "    : squashfs information about <path>\n"
        "\n"
        "       squash rm <path>                                               "
        "    : delete a squashfs attached image and its smart link\n"
        "\n"
        "       squash relabel <path>                                          "
        "    : relable a squashfs image link e.g. after an image move in the "
        "namespace\n"
        "\n"
        "       squash install --curl=https://<package>.tgz|.tar.gz <path>     "
        "    : create a squashfs package from a web archive under <path>\n"
        "       squash new-release <path> [<version>]                          "
        "      : create a new squashfs release under <path> - by default "
        "versions are made from timestamp, but this can be overwritten using "
        "the version field\n"
        "       squash pack-release <path>                                     "
        "    : pack a squashfs release under <path>\n"
        "       squash info-release <path>                                     "
        "    : show all release revisions under <path> <path>\n"
        "       squash trim-release <path> <keep-days> [<keep-versions>]       "
        "    : trim  releases older than <keep-days> and keep maximum "
        "<keep-versions> of release\n"
        "       squash rm-release <path>                                       "
        "    : delete all squahfs releases udner <path>\n");
  }
};
} // namespace

void
RegisterSquashNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<SquashCommand>());
}
