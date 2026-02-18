// ----------------------------------------------------------------------
// File: df-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeDfHelp()
{
  return "Usage: df [-m|-H|-b] [path]\n\n"
         "Print unix-like 'df' information (1024 base).\n\n"
         "Options:\n"
         "  -m  print in monitoring format\n"
         "  -H  print human readable in units of 1000\n"
         "  -b  print raw bytes/number values\n";
}

void ConfigureDfApp(CLI::App& app)
{
  app.name("df");
  app.description("Get df output");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeDfHelp();
      }));
}

// Ported DfHelper from com_proto_df.cc
class DfHelper : public ICmdHelper {
public:
  explicit DfHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::DfProto* dfproto = mReq.mutable_df();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;

    dfproto->set_si(true);
    dfproto->set_readable(true);

    if (!tokenizer.NextToken(token)) {
      return true;
    }

    if (token == "-m") {
      dfproto->set_monitoring(true);
      dfproto->set_readable(false);
    } else if (token == "-H") {
      dfproto->set_si(false);
      dfproto->set_readable(true);
    } else if (token == "-b") {
      dfproto->set_si(false);
      dfproto->set_readable(false);
    } else {
      if (token.substr(0, 1) != "/") {
        return false;
      }
    }

    std::string path = token;
    if (tokenizer.NextToken(token)) {
      if (token.substr(0, 1) == "-") {
        return false;
      }
      if (token.substr(0, 1) != "/") {
        return false;
      }
      path = token;
    }

    if (tokenizer.NextToken(token)) {
      return false;
    }
    dfproto->set_path(path);
    return true;
  }
};

class DfProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "df";
  }
  const char*
  description() const override
  {
    return "Get df output";
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
      if (i) {
        oss << ' ';
      }
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    DfHelper helper(gGlobalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureDfApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterDfProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DfProtoCommand>());
}
