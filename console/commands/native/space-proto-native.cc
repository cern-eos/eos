// ----------------------------------------------------------------------
// File: space-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <memory>
#include <sstream>

namespace {
std::string MakeSpaceHelp()
{
  return "Usage: space ls|config|set|rm|status|define|quota|tracker|inspector [OPTIONS]\n\n"
         "  ls [-s] [-g <depth>] [-m|-l|--io|--fsck] [<space>]\n"
         "    list spaces (substring match, comma-separated)\n"
         "  config <space> <key>=<value> | config rm <space> <key>\n"
         "    configure space attributes, balancer, drainer, etc.\n"
         "  set <space> on|off\n"
         "  rm <space>\n"
         "  status <space> [-m]\n"
         "  define <space> [<groupsize> [<groupmod>]]\n"
         "  quota <space> on|off\n"
         "  tracker [<space>]\n"
         "  inspector [options]\n";
}

void ConfigureSpaceApp(CLI::App& app)
{
  app.name("space");
  app.description("Space configuration");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeSpaceHelp();
      }));
}

class SpaceHelper : public ICmdHelper {
public:
  SpaceHelper(const GlobalOptions& opts) : ICmdHelper(opts) { mIsAdmin = true; }
  ~SpaceHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::SpaceProto* space = mReq.mutable_space();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token))
      return false;
    if (token == "ls") {
      eos::console::SpaceProto_LsProto* ls = space->mutable_ls();
      while (tokenizer.NextToken(token)) {
        if (token == "-s") {
          mIsSilent = true;
        } else if (token == "-g") {
          if (!tokenizer.NextToken(token) ||
              !eos::common::StringTokenizer::IsUnsignedNumber(token))
            return false;
          try {
            ls->set_outdepth(std::stoi(token));
          } catch (...) {
            return false;
          }
        } else if (token == "-m") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::MONITORING);
        } else if (token == "-l") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::LISTING);
        } else if (token == "--io") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::IO);
        } else if (token == "--fsck") {
          ls->set_outformat(eos::console::SpaceProto_LsProto::FSCK);
        } else if (token.find('-') != 0) {
          ls->set_selection(token);
        } else {
          return false;
        }
      }
    } else if (token == "config") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_ConfigProto* config = space->mutable_config();
      bool removing = false;
      if (token == "rm") {
        removing = true;
        if (!tokenizer.NextToken(token))
          return false;
      }
      config->set_mgmspace_name(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (removing) {
        config->set_remove(true);
        config->set_mgmspace_key(token);
      } else {
        std::string::size_type pos = token.find('=');
        if (pos != std::string::npos &&
            std::count(token.begin(), token.end(), '=') == 1) {
          config->set_mgmspace_key(token.substr(0, pos));
          config->set_mgmspace_value(token.substr(pos + 1));
        } else {
          return false;
        }
      }
    } else if (token == "set") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_SetProto* set = space->mutable_set();
      set->set_mgmspace(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "on")
        set->set_state_switch(true);
      else if (token == "off")
        set->set_state_switch(false);
      else
        return false;
    } else if (token == "rm") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_RmProto* rm = space->mutable_rm();
      rm->set_mgmspace(token);
    } else if (token == "status") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_StatusProto* status = space->mutable_status();
      status->set_mgmspace(token);
      if (tokenizer.NextToken(token)) {
        if (token == "-m")
          status->set_outformat_m(true);
        else
          return false;
      }
    } else if (token == "define") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_DefineProto* define = space->mutable_define();
      define->set_mgmspace(token);
      // optional groupsize
      if (!tokenizer.NextToken(token)) {
        define->set_groupsize(0);
        define->set_groupmod(24);
      } else {
        try {
          define->set_groupsize(std::stoi(token));
        } catch (...) {
          return false;
        }
        if (!tokenizer.NextToken(token)) {
          define->set_groupmod(24);
        } else {
          try {
            define->set_groupmod(std::stoi(token));
          } catch (...) {
            return false;
          }
        }
      }
    } else if (token == "quota") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::SpaceProto_QuotaProto* quota = space->mutable_quota();
      quota->set_mgmspace(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "on")
        quota->set_quota_switch(true);
      else if (token == "off")
        quota->set_quota_switch(false);
      else
        return false;
    } else {
      return false;
    }
    return true;
  }
};

class SpaceProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "space";
  }
  const char*
  description() const override
  {
    return "Space configuration";
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
    SpaceHelper helper(gGlobalOpts);
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
    ConfigureSpaceApp(app);
    fprintf(stdout, "%s", app.help().c_str());
  }
};

} // namespace

void
RegisterSpaceProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<SpaceProtoCommand>());
}
