// ----------------------------------------------------------------------
// File: group-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {

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
    fprintf(
        stdout,
        " Usage:\n\n"
        "group ls [-s] [-g <depth>] [-b|--brief] [-m|-l|--io] [<groups>] : "
        "list groups\n"
        "\t <groups> : list <groups> only, where <groups> is a substring match "
        "and can be a comma seperated list\n"
        "\t       -s : silent mode\n"
        "\t       -g : geo output - aggregate group information along the "
        "instance geotree down to <depth>\n"
        "\t       -b : brief output\n"
        "\t       -m : monitoring key=value output format\n"
        "\t       -l : long output - list also file systems after each group\n"
        "\t     --io : print IO statistics for the group\n"
        "\t     --IO : print IO statistics for each filesystem\n\n"
        "group rm <group-name> : remove group\n\n"
        "group set <group-name> on|drain|off : activate/drain/deactivate "
        "group\n"
        "\t  => when a group is (re-)enabled, the drain pull flag is "
        "recomputed for all filesystems within a group\n"
        "\t  => when a group is (re-)disabled, the drain pull flag is removed "
        "from all members in the group\n"
        "\t  => when a group is in drain, all the filesystems in the group "
        "will be drained to other groups\n");
  }
};
} // namespace

void
RegisterGroupProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<GroupProtoCommand>());
}
