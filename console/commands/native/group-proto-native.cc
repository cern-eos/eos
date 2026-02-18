// ----------------------------------------------------------------------
// File: group-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeGroupHelp()
{
  return "Usage: group ls|rm|set [OPTIONS]\n\n"
         "  ls [-s] [-g <depth>] [-b|--brief] [-m|-l|--io|--IO] [<groups>]\n"
         "    list groups (optional substring match)\n"
         "    -s : silent, -g : geo depth, -b : brief, -m : monitoring, -l : long\n"
         "    --io : IO stats per group, --IO : IO stats per filesystem\n\n"
         "  rm <group-name>\n"
         "    remove group\n\n"
         "  set <group-name> on|drain|off\n"
         "    activate/drain/deactivate group\n";
}

void ConfigureGroupApp(CLI::App& app)
{
  app.name("group");
  app.description("Group configuration");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeGroupHelp();
      }));
}

class GroupHelper : public ICmdHelper {
public:
  GroupHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~GroupHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::GroupProto* group = mReq.mutable_group();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token)) {
      return false;
    }
    if (token == "ls") {
      eos::console::GroupProto_LsProto* ls = group->mutable_ls();
      while (tokenizer.NextToken(token)) {
        if (token == "-s") {
          mIsSilent = true;
        } else if (token == "-g") {
          if (!tokenizer.NextToken(token) ||
              !eos::common::StringTokenizer::IsUnsignedNumber(token)) {
            std::cerr << "error: geodepth invalid" << std::endl;
            return false;
          }
          try {
            ls->set_outdepth(std::stoi(token));
          } catch (...) {
            std::cerr << "error: geodepth must be integer" << std::endl;
            return false;
          }
        } else if (token == "-b" || token == "--brief") {
          ls->set_outhost(true);
        } else if (token == "-m") {
          ls->set_outformat(eos::console::GroupProto_LsProto::MONITORING);
        } else if (token == "-l") {
          ls->set_outformat(eos::console::GroupProto_LsProto::LISTING);
        } else if (token == "--io") {
          ls->set_outformat(eos::console::GroupProto_LsProto::IOGROUP);
        } else if (token == "--IO") {
          ls->set_outformat(eos::console::GroupProto_LsProto::IOFS);
        } else if (token.find('-') != 0) {
          ls->set_selection(token);
        } else {
          return false;
        }
      }
    } else if (token == "rm") {
      if (!tokenizer.NextToken(token)) {
        return false;
      }
      eos::console::GroupProto_RmProto* rm = group->mutable_rm();
      rm->set_group(token);
    } else if (token == "set") {
      if (!tokenizer.NextToken(token)) {
        return false;
      }
      eos::console::GroupProto_SetProto* set = group->mutable_set();
      set->set_group(token);
      if (!tokenizer.NextToken(token)) {
        return false;
      }
      if (token == "on" || token == "off" || token == "drain") {
        set->set_group_state(token);
      } else {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }
};

class GroupProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "group";
  }
  const char*
  description() const override
  {
    return "Group configuration";
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
    GroupHelper helper(gGlobalOpts);
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
    ConfigureGroupApp(app);
    fprintf(stdout, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterGroupProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<GroupProtoCommand>());
}
