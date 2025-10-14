// ----------------------------------------------------------------------
// File: config-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_protoconfig(char*);
extern void com_config_help();

namespace {
class ConfigProtoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "config"; }
  const char* description() const override { return "Configuration System"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); if (wants_help(joined.c_str())) { printHelp(); global_retc = EINVAL; return 0; }
    return com_protoconfig((char*)joined.c_str());
  }
  void printHelp() const override {
    fprintf(stdout,
            " usage:\n"
            "config changelog|dump|export|load|ls|reset|save [OPTIONS]\n"
            "'[eos] config' provides the configuration interface to EOS.\n"
            "\n"
            "Subcommands:\n"
            "config changelog [#lines] : show the last #lines from the changelog - default is 10\n"
            "\n"
            "config dump [<name>] : dump configuration with name <name> or current one by default\n"
            "\n"
            "config export <name> [-f] : export a configuration stored on file to QuarkDB (you need to specify the full path!)\n"
            "\t -f : overwrite existing config name and create a timestamped backup\n"
            "\n"
            "config load <name> : load <name> config\n"
            "\n"
            "config ls [-b|--backup] : list existing configurations\n"
            "\t -b : show also backup & autosave files\n"
            "\n"
            "config reset : reset all configuration to empty state\n"
            "\n"
            "config save <name> [-f] [-c|--comment \"<comment>\"] : save config under <name>\n"
            "\t -f : overwrite existing config name and create a timestamped backup\n"
            "\t -c : add a comment entry to the config\n");
  }
};
}

void RegisterConfigProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConfigProtoCommand>());
}


