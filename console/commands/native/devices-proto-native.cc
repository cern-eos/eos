// ----------------------------------------------------------------------
// File: devices-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeDevicesHelp()
{
  return "Usage: devices ls [-l] [-m] [--refresh]\n\n"
         "Print statistics per space of all storage devices based on S.M.A.R.T.\n\n"
         "Options:\n"
         "  -l         print S.M.A.R.T information for each configured filesystem\n"
         "  -m         print monitoring output format (key=val)\n"
         "  --refresh  force reparse of current S.M.A.R.T information\n\n"
         "Use 'eos --json devices ls' for JSON output.\n";
}

void ConfigureDevicesApp(CLI::App& app)
{
  app.name("devices");
  app.description("Get Device Information");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeDevicesHelp();
      }));
}

// Ported DevicesHelper from com_proto_devices.cc
class DevicesHelper : public ICmdHelper {
public:
  explicit DevicesHelper(const GlobalOptions& opts) : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }
  bool
  ParseCommand(const char* arg) override
  {
    XrdOucString token;
    eos::console::DevicesProto* devices = mReq.mutable_devices();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::DevicesProto_LsProto* ls = devices->mutable_ls();
    ls->set_outformat(eos::console::DevicesProto_LsProto::NONE);

    if (token == "ls") {
      do {
        tokenizer.NextToken(token);
        if (!token.length()) {
          return true;
        }
        if (token == "-l") {
          ls->set_outformat(eos::console::DevicesProto_LsProto::LISTING);
        } else if (token == "-m") {
          ls->set_outformat(eos::console::DevicesProto_LsProto::MONITORING);
        } else if (token == "--refresh") {
          ls->set_refresh(true);
        } else {
          return false;
        }
      } while (token.length());
    } else {
      return false;
    }

    return true;
  }
};

class DevicesProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "devices";
  }
  const char*
  description() const override
  {
    return "Get Device Information";
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

    DevicesHelper helper(gGlobalOpts);
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
    CLI::App app;
    ConfigureDevicesApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterDevicesProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DevicesProtoCommand>());
}
