// ----------------------------------------------------------------------
// File: MkdirRmNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "common/Path.hh"
#include <memory>
#include <sstream>

namespace {
class MkdirCommand : public IConsoleCommand {
public:
  const char* name() const override { return "mkdir"; }
  const char* description() const override { return "Create a directory"; }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.empty() || args[0] == "--help" || args[0] == "-h") {
      fprintf(stdout, "usage: mkdir -p <path>                                                :  create directory <path>\n");
      global_retc = EINVAL; return 0;
    }
    bool parents = false; size_t idx = 0;
    if (args[0] == "-p") { parents = true; idx = 1; }
    std::ostringstream path; for (size_t i=idx;i<args.size();++i){ if(i>idx) path<<' '; path<<args[i]; }
    std::string p = path.str(); if (p.empty()) { fprintf(stdout, "usage: mkdir -p <path>\n"); global_retc = EINVAL; return 0; }
    XrdOucString in = "mgm.cmd=mkdir"; if (parents) in += "&mgm.option=p";
    XrdOucString ap = abspath(p.c_str()); in += "&mgm.path="; in += ap;
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};

class RmCommand : public IConsoleCommand {
public:
  const char* name() const override { return "rm"; }
  const char* description() const override { return "Remove a file"; }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.empty() || wants_help(args[0].c_str())) {
      fprintf(stdout,
        "usage: rm [-rf] [-F|--no-recycle-bin] [--no-confirmation] [--no-globbing] [<path>|fid:<fid-dec>|fxid:<fid-hex>]                    :  remove file <path>\n");
      global_retc = EINVAL; return 0;
    }
    XrdOucString in = "mgm.cmd=rm&"; bool noconfirm = false;
    std::string s1 = args[0]; std::string s2 = args.size()>1?args[1]:"";
    XrdOucString option; XrdOucString path;
    if (s1 == "-r" || s1 == "-rf" || s1 == "-fr") { option = "r"; path = s2.c_str(); }
    else if (s1 == "-rF" || s1 == "-Fr") { option = "rf"; path = s2.c_str(); }
    else if (s1 == "-F" || s1 == "--no-recycle-bin") { option = "f"; path = s2.c_str(); }
    else if (!s1.empty() && s1[0] == '-') { fprintf(stdout, "invalid option\n"); global_retc = EINVAL; return 0; }
    else { option = ""; path = s1.c_str(); }
    if (args.size() > 2) for (size_t i=2;i<args.size();++i){ path += " "; path += args[i].c_str(); }
    if (std::string(path.c_str()) == "--no-confirmation") { noconfirm = true; }
    while (path.replace("\\ ", " ")) {}
    if (!path.length()) { fprintf(stdout, "usage: rm ...\n"); global_retc = EINVAL; return 0; }
    unsigned long long id;
    if (Path2FileDenominator(path, id)) { in += "&mgm.file.id="; in += std::to_string(id).c_str(); if (option.find("r") != STR_NPOS) { fprintf(stderr, "error: cannot use recursive delete with file id!\n"); global_retc = EINVAL; return 0; } }
    else if (Path2ContainerDenominator(path, id)) { in += "&mgm.container.id="; in += std::to_string(id).c_str(); }
    else { XrdOucString ap = abspath(path.c_str()); in += "&mgm.path="; in += ap; }
    in += "&mgm.option="; in += option;
    if ((option == "r") && !noconfirm) {
      eos::common::Path cPath(path.c_str());
      if (cPath.GetSubPathSize() < 4) {
        std::string s; fprintf(stdout, "Do you really want to delete ALL files starting at %s ?\n", path.c_str());
        fprintf(stdout, "Confirm the deletion by typing => "); XrdOucString confirmation = ""; for (int i=0;i<10;i++){ confirmation += (int)(9.0 * rand() / RAND_MAX);} fprintf(stdout, "%s\n", confirmation.c_str()); fprintf(stdout, "                               => ");
        std::getline(std::cin, s); if (s == std::string(confirmation.c_str())) { fprintf(stdout, "\nDeletion confirmed\n"); in += "&mgm.deletion=deep"; } else { fprintf(stdout, "\nDeletion aborted\n"); global_retc = EINTR; return 0; }
      }
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true); return 0;
  }
  void printHelp() const override {}
};
}

void RegisterMkdirRmNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<MkdirCommand>());
  CommandRegistry::instance().reg(std::make_unique<RmCommand>());
}


