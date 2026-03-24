// ----------------------------------------------------------------------
// File: du-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>
namespace {
class DuCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "du";
  }
  const char*
  description() const override
  {
    return "Get du output";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }

  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    IConsoleCommand* findCmd = CommandRegistry::instance().find("find");
    if (!findCmd) {
      fprintf(stderr, "error: 'find' command not available\n");
      global_retc = EINVAL;
      return 0;
    }

    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i) {
        oss << ' ';
      }
      oss << args[i];
    }
    std::string joined = oss.str();

    if (wants_help(joined.c_str(), true)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    eos::common::StringTokenizer tokenizer(joined.c_str());
    tokenizer.GetLine();
    std::string token;
    bool printfiles = false;
    bool printreadable = false;
    bool printsummary = false;
    bool printsi = false;
    std::string path;

    do {
      if (!tokenizer.NextToken(token)) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }

      if (token == "-a") {
        printfiles = true;
      } else if (token == "-h") {
        printreadable = true;
      } else if (token == "-s") {
        printsummary = true;
      } else if (token == "--si") {
        printsi = true;
      } else {
        path = abspath(token.c_str());
        break;
      }
    } while (1);

    std::vector<std::string> findArgs;
    findArgs.emplace_back("--du");
    if (!printfiles) {
      findArgs.emplace_back("-d");
    }
    if (printsi) {
      findArgs.emplace_back("--du-si");
    }
    if (printreadable) {
      findArgs.emplace_back("--du-h");
    }
    if (printsummary) {
      findArgs.emplace_back("--maxdepth");
      findArgs.emplace_back("0");
    }
    findArgs.emplace_back(path);

    int rc = findCmd->run(findArgs, ctx);
    global_retc = rc;
    return rc;
  }

  void
  printHelp() const override
  {
    fprintf(stderr, "usage:\n"
                    "du [-a][-h][-s][--si] path\n"
                    "'[eos] du ...' print unix like 'du' information showing "
                    "subtreesize for directories\n"
                    "\n"
                    "Options:\n"
                    "\n"
                    "-a   : print also for files\n"
                    "-h   : print human readable in units of 1000\n"
                    "-s   : print only the summary\n"
                    "--si : print in si units\n");
  }
};
} // namespace

void
RegisterDuNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DuCommand>());
}
