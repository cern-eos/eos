// ----------------------------------------------------------------------
// File: group-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <cstdint>
#include <memory>

namespace {
struct GroupOptions {
  CLI::App app{"Group configuration", "group"};
  CLI::App *ls, *rm, *set;
  uint32_t depth = 0;
  bool silent = false, brief = false, monitoring = false, listing = false;
  bool ioGroup = false, ioFs = false;
  std::string selection, groupName, state;
  const std::vector<std::string> states{"on", "drain", "off"};

  GroupOptions()
  {
    app.require_subcommand(1, 1);
    ls = app.add_subcommand("ls", "List groups");
    ls->add_flag("-s", silent, "Silent mode");
    ls->add_option("-g", depth, "Aggregate geotree information down to this depth")
        ->transform(eos::console::DecimalNumber);
    ls->add_flag("-b,--brief", brief, "Brief output");
    auto* monitor = ls->add_flag("-m", monitoring, "Monitoring key=value output");
    auto* list = ls->add_flag("-l", listing, "List file systems after each group");
    auto* groupIo = ls->add_flag("--io", ioGroup, "Group IO statistics");
    auto* fsIo = ls->add_flag("--IO", ioFs, "Filesystem IO statistics");
    monitor->excludes(list)->excludes(groupIo)->excludes(fsIo);
    list->excludes(groupIo)->excludes(fsIo);
    groupIo->excludes(fsIo);
    ls->add_option("groups", selection, "Substring selection; comma-separated groups")
        ->check(eos::console::NonEmptyArgument);
    rm = app.add_subcommand("rm", "Remove a group");
    rm->add_option("group-name", groupName)
        ->required()
        ->check(eos::console::NonEmptyArgument);
    set = app.add_subcommand("set", "Activate, drain, or deactivate a group");
    set->add_option("group-name", groupName)
        ->required()
        ->check(eos::console::NonEmptyArgument);
    set->add_option("state", state)->required()->check(CLI::IsMember(states));
  }
};

class GroupHelper : public ICmdHelper {
public:
  explicit GroupHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
  }
  bool
  ParseCommand(const char* arg) override
  {
    std::vector<std::string> args;
    return eos::console::TokenizeArguments(arg, args) && ParseArguments(args);
  }
  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    GroupOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      return false;
    }
    auto* group = mReq.mutable_group();
    if (*options.ls) {
      auto* ls = group->mutable_ls();
      mIsSilent = options.silent;
      ls->set_outdepth(options.depth);
      ls->set_outhost(options.brief);
      ls->set_selection(options.selection);
      using Ls = eos::console::GroupProto_LsProto;
      if (options.monitoring) {
        ls->set_outformat(Ls::MONITORING);
      } else if (options.listing) {
        ls->set_outformat(Ls::LISTING);
      } else if (options.ioGroup) {
        ls->set_outformat(Ls::IOGROUP);
      } else if (options.ioFs) {
        ls->set_outformat(Ls::IOFS);
      }
    } else if (*options.rm) {
      group->mutable_rm()->set_group(options.groupName);
    } else if (*options.set) {
      auto* set = group->mutable_set();
      set->set_group(options.groupName);
      set->set_group_state(options.state);
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
  std::string
  helpText() const override
  {
    return GroupOptions{}.app.help("", CLI::AppFormatMode::All);
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    GroupOptions options;
    return CompleteCliArguments(options.app, args,
                                {{options.set->get_option("state"), options.states}});
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    GroupHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
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
    GroupOptions options;
    eos::console::PrintCliHelp(options.app, CLI::AppFormatMode::All);
  }
};
} // namespace

void
RegisterGroupProtoNativeCommand()
{
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<GroupProtoCommand>(),
      {{{{"ls"}, "ls", "List scheduling groups."},
        {{"rm"}, "rm", "Remove a scheduling group."},
        {{"set"}, "set", "Activate, drain or deactivate a group."}},
       eos::console::ShortHelpPolicy::Always,
       {{"-g", 1}}}));
}
