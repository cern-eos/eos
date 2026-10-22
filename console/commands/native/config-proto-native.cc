// ----------------------------------------------------------------------
// File: config-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <cstdint>
#include <memory>

namespace {
struct ConfigOptions {
  CLI::App app{"Configuration System", "config"};
  CLI::App *ls, *dump, *exportConfig, *load, *reset, *save, *changelog;
  bool backup = false, forceExport = false, forceSave = false;
  std::string dumpName, exportName, loadName, saveName, comment;
  uint32_t lines = 10;

  ConfigOptions()
  {
    app.require_subcommand(1, 1);
    ls = app.add_subcommand("ls", "List configurations");
    ls->add_flag("-b,--backup", backup, "Include backup configurations");
    dump = app.add_subcommand("dump", "Dump the current or a named configuration");
    dump->add_option("name", dumpName)->check(eos::console::NonEmptyArgument);
    exportConfig = app.add_subcommand("export", "Export a configuration to QuarkDB");
    exportConfig->add_option("name", exportName)
        ->required()
        ->check(eos::console::NonEmptyArgument);
    exportConfig->add_flag("-f", forceExport, "Overwrite an existing configuration");
    load = app.add_subcommand("load", "Load a configuration");
    load->add_option("name", loadName)->required()->check(eos::console::NonEmptyArgument);
    reset = app.add_subcommand("reset", "Reset all configuration");
    save = app.add_subcommand("save", "Save a configuration");
    save->add_option("name", saveName)->required()->check(eos::console::NonEmptyArgument);
    save->add_flag("-f", forceSave, "Overwrite an existing configuration");
    save->add_option("-c,--comment", comment, "Comment recorded with the configuration");
    changelog = app.add_subcommand("changelog", "Show the configuration changelog");
    changelog->add_option("lines", lines, "Number of lines (default: 10)")
        ->transform(eos::console::DecimalNumber);
  }
};

class ConfigHelper : public ICmdHelper {
public:
  explicit ConfigHelper(const GlobalOptions& opts)
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
    ConfigOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      return false;
    }
    auto* config = mReq.mutable_config();
    if (*options.ls) {
      config->mutable_ls()->set_showbackup(options.backup);
    } else if (*options.dump) {
      config->mutable_dump()->set_file(options.dumpName);
    } else if (*options.exportConfig) {
      auto* exp = config->mutable_exp();
      exp->set_file(options.exportName);
      exp->set_force(options.forceExport);
    } else if (*options.load) {
      config->mutable_load()->set_file(options.loadName);
    } else if (*options.reset) {
      config->set_reset(true);
    } else if (*options.save) {
      auto* save = config->mutable_save();
      save->set_file(options.saveName);
      save->set_force(options.forceSave);
      if (options.save->get_option("--comment")->count()) {
        mReq.set_comment(options.comment);
      }
    } else if (*options.changelog) {
      config->mutable_changelog()->set_lines(options.lines);
    }
    return true;
  }
};

class ConfigProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "config";
  }
  const char*
  description() const override
  {
    return "Configuration System";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    ConfigHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  std::string
  helpText() const override
  {
    return ConfigOptions{}.app.help("", CLI::AppFormatMode::All);
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    ConfigOptions options;
    return CompleteCliArguments(options.app, args);
  }
  void
  printHelp() const override
  {
    ConfigOptions options;
    eos::console::PrintCliHelp(options.app, CLI::AppFormatMode::All);
  }
};
} // namespace

void
RegisterConfigProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConfigProtoCommand>());
}
