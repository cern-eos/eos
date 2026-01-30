// ----------------------------------------------------------------------
// File: map-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class MapCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "map";
  }
  const char*
  description() const override
  {
    return "Mapping utilities";
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

    eos::common::StringTokenizer tok(joined.c_str());
    tok.GetLine();
    const char* t = tok.GetToken();
    if (!t) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString subcommand = t;
    XrdOucString in = "mgm.cmd=map";

    // Optional leading option (e.g. -something)
    XrdOucString arg = "";
    if (subcommand.beginswith("-")) {
      XrdOucString option = subcommand;
      option.erase(0, 1);
      in += "&mgm.option=";
      in += option;
      subcommand = tok.GetToken();
      arg = tok.GetToken();
    } else {
      arg = tok.GetToken();
    }

    if (!subcommand.length() ||
        ((subcommand != "ls") && (subcommand != "link") &&
         (subcommand != "unlink"))) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    if (subcommand == "ls") {
      in += "&mgm.subcmd=ls";
    } else if (subcommand == "link") {
      XrdOucString key = arg;
      XrdOucString value = tok.GetToken();
      if (!key.length() || !value.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=link&mgm.map.src=";
      in += key;
      in += "&mgm.map.dest=";
      in += value;
    } else if (subcommand == "unlink") {
      XrdOucString key = arg;
      if (!key.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=unlink&mgm.map.src=";
      in += key;
    }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: map [OPTIONS] ls|link|unlink ...\n"
                    "'[eos] map ..' provides a namespace mapping interface for "
                    "directories in EOS.\n"
                    "Options:\n"
                    "map ls :\n"
                    "                                                : list "
                    "all defined mappings\n"
                    "map link <source-path> <destination-path> :\n"
                    "                                                : create "
                    "a symbolic link from source-path to destination-path\n"
                    "map unlink <source-path> :\n"
                    "                                                : remove "
                    "symbolic link from source-path\n");
  }
};
} // namespace

void
RegisterMapNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MapCommand>());
}
