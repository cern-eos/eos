// ----------------------------------------------------------------------
// File: rm-native.cc
// ----------------------------------------------------------------------

#include "common/Path.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class RmCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "rm";
  }
  const char*
  description() const override
  {
    return "Remove a file";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return true;
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty() || wants_help(args[0].c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=rm&";
    bool noconfirm = false;
    std::string s1 = args[0];
    std::string s2 = args.size() > 1 ? args[1] : "";
    XrdOucString option;
    XrdOucString path;
    if (s1 == "-r" || s1 == "-rf" || s1 == "-fr") {
      option = "r";
      path = s2.c_str();
    } else if (s1 == "-rF" || s1 == "-Fr") {
      option = "rf";
      path = s2.c_str();
    } else if (s1 == "-F" || s1 == "--no-recycle-bin") {
      option = "f";
      path = s2.c_str();
    } else if (!s1.empty() && s1[0] == '-') {
      fprintf(stdout, "invalid option\n");
      global_retc = EINVAL;
      return 0;
    } else {
      option = "";
      path = s1.c_str();
    }
    if (args.size() > 2)
      for (size_t i = 2; i < args.size(); ++i) {
        path += " ";
        path += args[i].c_str();
      }
    if (std::string(path.c_str()) == "--no-confirmation") {
      noconfirm = true;
    }
    while (path.replace("\\ ", " ")) {
    }
    if (!path.length()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    unsigned long long id;
    if (Path2FileDenominator(path, id)) {
      in += "&mgm.file.id=";
      in += std::to_string(id).c_str();
      if (option.find("r") != STR_NPOS) {
        fprintf(stderr, "error: cannot use recursive delete with file id!\n");
        global_retc = EINVAL;
        return 0;
      }
    } else if (Path2ContainerDenominator(path, id)) {
      in += "&mgm.container.id=";
      in += std::to_string(id).c_str();
    } else {
      XrdOucString ap = abspath(path.c_str());
      in += "&mgm.path=";
      in += ap;
    }
    in += "&mgm.option=";
    in += option;
    if ((option == "r") && !noconfirm) {
      eos::common::Path cPath(path.c_str());
      if (cPath.GetSubPathSize() < 4) {
        std::string s;
        fprintf(stdout,
                "Do you really want to delete ALL files starting at %s ?\n",
                path.c_str());
        fprintf(stdout, "Confirm the deletion by typing => ");
        XrdOucString confirmation = "";
        for (int i = 0; i < 10; i++) {
          confirmation += (int)(9.0 * rand() / RAND_MAX);
        }
        fprintf(stdout, "%s\n", confirmation.c_str());
        fprintf(stdout, "                               => ");
        std::getline(std::cin, s);
        if (s == std::string(confirmation.c_str())) {
          fprintf(stdout, "\nDeletion confirmed\n");
          in += "&mgm.deletion=deep";
        } else {
          fprintf(stdout, "\nDeletion aborted\n");
          global_retc = EINTR;
          return 0;
        }
      }
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: rm [-rf] [-F|--no-recycle-bin] [--no-confirmation] "
            "[--no-globbing] [<path>|fid:<fid-dec>|fxid:<fid-hex>]           "
            "                    :  remove file <path>\n");
    fprintf(stderr,
            "                                                                    -r :  remove recursivly\n");
    fprintf(stderr,
            "                                                                    -f :  default force flag is ignored because there is no file by file feedback to confirm removing\n");
    fprintf(stderr,
            "                                                      --no-recycle-bin :\n"
            "                                                                    -F :  remove bypassing recycling policies (you have to take the root role to use this flag!)\n");
    fprintf(stderr,
            "                                                      --no-confirmation:  will not ask an interactive confirmation code if a recursive deletion is running in directory level < 4!\n");
  }
};
} // namespace

void
RegisterRmNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RmCommand>());
}
