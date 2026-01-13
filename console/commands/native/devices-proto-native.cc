// ----------------------------------------------------------------------
// File: devices-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
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
    fprintf(stderr,
            "Usage: devices ls [-l] [-m] [--refresh]\n"
            "                                       : without option prints "
            "statistics per space of all storage devices used based on "
            "S.M.A.R.T information\n"
            "                                    -l : prints S.M.A.R.T "
            "information for each configured filesystem\n"
            "                                    -m : print monitoring output "
            "format (key=val)\n"
            "                             --refresh : forces to reparse the "
            "current available S.M.A.R.T information and output this\n"
            "\n"
            "                                  JSON : to retrieve JSON output, "
            "use 'eos --json devices ls' !\n");
  }
};
} // namespace

void
RegisterDevicesProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DevicesProtoCommand>());
}
