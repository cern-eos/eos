// ----------------------------------------------------------------------
// File: inspector-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
class InspectorHelper : public ICmdHelper {
public:
  explicit InspectorHelper(const GlobalOptions& opts) : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }
  bool
  ParseCommand(const char* arg) override
  {
    eos::common::StringTokenizer tok(arg);
    tok.GetLine();
    std::string token;
    eos::console::SpaceProto* space = mReq.mutable_space();
    eos::console::SpaceProto_InspectorProto* insp = space->mutable_inspector();
    insp->set_mgmspace("default");
    std::string options;

    while (tok.NextToken(token)) {
      if ((token == "-s") || (token == "--space")) {
        if (tok.NextToken(token)) {
          insp->set_mgmspace(token);
        } else {
          std::cerr << "error: no space specified" << std::endl;
          return false;
        }
      } else if (token == "-c" || token == "--current") {
        options += "c";
      } else if (token == "-l" || token == "--last") {
        options += "l";
      } else if (token == "-m") {
        options += "m";
      } else if (token == "-p") {
        options += "p";
      } else if (token == "-e") {
        options += "e";
      } else if (token == "-C" || token == "--cost") {
        options += "C";
      } else if (token == "-U" || token == "--usage") {
        options += "U";
      } else if (token == "-L" || token == "--layouts") {
        options += "L";
      } else if (token == "-B" || token == "--birth") {
        options += "B";
      } else if (token == "-A" || token == "--access") {
        options += "A";
      } else if (token == "-a" || token == "--all") {
        options += "Z";
      } else if (token == "-V" || token == "--vs") {
        options += "V";
      } else if (token == "-M" || token == "--money") {
        options += "M";
      } else {
        return false;
      }
    }

    insp->set_options(options);
    return true;
  }
};

class InspectorCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "inspector";
  }
  const char*
  description() const override
  {
    return "Run inspector tools";
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
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    InspectorHelper helper(gGlobalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute(true, true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: inspector [args...]\n"
                    "forwards arguments to the inspector space subsystem\n");
  }
};
} // namespace

void
RegisterInspectorNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<InspectorCommand>());
}
