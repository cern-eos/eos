// ----------------------------------------------------------------------
// File: config-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

extern void com_config_help();

namespace {
// Ported from legacy com_proto_config.cc
class ConfigHelper : public ICmdHelper {
public:
  ConfigHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~ConfigHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::ConfigProto* config = mReq.mutable_config();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token)) {
      return false;
    }
    if (token == "ls") {
      eos::console::ConfigProto_LsProto* ls = config->mutable_ls();
      if (tokenizer.NextToken(token)) {
        if (token == "--backup" || token == "-b") {
          ls->set_showbackup(true);
        } else {
          return false;
        }
      }
    } else if (token == "dump") {
      eos::console::ConfigProto_DumpProto* dump = config->mutable_dump();
      if (tokenizer.NextToken(token)) {
        dump->set_file(token);
      }
    } else if (token == "reset") {
      if (tokenizer.NextToken(token)) {
        return false;
      }
      config->set_reset(true);
    } else if (token == "export") {
      if (!tokenizer.NextToken(token)) {
        return false;
      }
      eos::console::ConfigProto_ExportProto* exp = config->mutable_exp();
      if (token.find('-') != 0) {
        exp->set_file(token);
        if (tokenizer.NextToken(token)) {
          if (token == "-f") {
            exp->set_force(true);
          } else {
            return false;
          }
        }
      } else {
        return false;
      }
    } else if (token == "save") {
      if (!tokenizer.NextToken(token)) {
        return false;
      }
      eos::console::ConfigProto_SaveProto* save = config->mutable_save();
      if (token.find('-') != 0) {
        save->set_file(token);
      } else {
        return false;
      }
      while (tokenizer.NextToken(token)) {
        if (token == "-c" || token == "--comment") {
          std::string sline = arg;
          if (token == "-c") {
            size_t pos = sline.find("-c");
            sline.replace(pos, std::string("-c").length(), "--comment");
            parse_comment(sline.c_str(), token);
          } else {
            parse_comment(sline.c_str(), token);
          }
          mReq.set_comment(token);
          tokenizer.NextToken(token);
        } else if (token == "-f") {
          save->set_force(true);
        } else {
          return false;
        }
      }
    } else if (token == "load") {
      if (!tokenizer.NextToken(token)) {
        return false;
      }
      eos::console::ConfigProto_LoadProto* load = config->mutable_load();
      load->set_file(token);
    } else if (token == "changelog") {
      eos::console::ConfigProto_ChangelogProto* changelog =
          config->mutable_changelog();
      if (tokenizer.NextToken(token)) {
        if (token.find('-') == 0) {
          token.erase(0);
        }
        try {
          changelog->set_lines(std::stoi(token));
        } catch (...) {
          std::cerr << "error: argument needs to be numeric" << std::endl;
          return false;
        }
      } else {
        changelog->set_lines(10);
      }
    } else {
      return false;
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
    ConfigHelper helper(gGlobalOpts);
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
    fprintf(stderr,
            "Usage:\n"
            "config changelog|dump|export|load|ls|reset|save [OPTIONS]\n"
            "'[eos] config' provides the configuration interface to EOS.\n\n"
            "Subcommands:\n"
            "config changelog [#lines] : show the last #lines from the "
            "changelog - default is 10\n\n"
            "config dump [<name>] : dump configuration with name <name> or "
            "current one by default\n\n"
            "config export <name> [-f] : export a configuration stored on file "
            "to QuarkDB (you need to specify the full path!)\n"
            "\t -f : overwrite existing config name and create a timestamped "
            "backup\n\n"
            "config load <name> : load <name> config\n\n"
            "config ls [-b|--backup] : list existing configurations\n"
            "\t -b : show also backup & autosave files\n\n"
            "config reset : reset all configuration to empty state\n\n"
            "config save <name> [-f] [-c|--comment \"<comment>\"] : save "
            "config under <name>\n"
            "\t -f : overwrite existing config name and create a timestamped "
            "backup\n"
            "\t -c : add a comment entry to the config\n");
  }
};
} // namespace

void
RegisterConfigProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConfigProtoCommand>());
}
