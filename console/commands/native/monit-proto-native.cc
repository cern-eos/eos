// ----------------------------------------------------------------------
// File: monit-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCompletion.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"

#include <CLI/CLI.hpp>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {
std::string
MakeMonitHelp()
{
  return "Usage: monit [enable|disable|config]\n\n"
         "Configure EOS monitoring endpoints.\n\n"
         "Subcommands:\n"
         "  enable                      enable the Prometheus endpoint\n"
         "  disable                     disable the Prometheus endpoint\n"
         "  config ls                   list current monitoring configuration\n"
         "  config set --port N         set the Prometheus endpoint TCP port\n"
         "  config set --cache-ttl N    set scrape cache TTL in seconds\n";
}

bool
ParseUint32(const std::string& input, uint32_t& value)
{
  if (input.empty()) {
    return false;
  }

  char* end = nullptr;
  const unsigned long parsed = strtoul(input.c_str(), &end, 10);

  if (!end || *end != '\0' || parsed > std::numeric_limits<uint32_t>::max()) {
    return false;
  }

  value = static_cast<uint32_t>(parsed);
  return true;
}

class MonitHelper : public ICmdHelper {
public:
  MonitHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }

  bool
  ParseCommand(const char* arg) override
  {
    CLI::App app;
    app.name("monit");
    app.description("Configure EOS monitoring endpoints");
    app.set_help_flag("");
    app.allow_extras(false);
    app.formatter(std::make_shared<CLI::FormatterLambda>(
        [](const CLI::App*, std::string, CLI::AppFormatMode) {
          return MakeMonitHelp();
        }));

    std::optional<std::string> port;
    std::optional<std::string> cache_ttl;
    app.require_subcommand(1);

    auto* enable_cmd = app.add_subcommand("enable", "Enable the Prometheus endpoint");
    enable_cmd->callback([this]() { mReq.mutable_monit()->mutable_enable(); });

    auto* disable_cmd = app.add_subcommand("disable", "Disable the Prometheus endpoint");
    disable_cmd->callback([this]() { mReq.mutable_monit()->mutable_disable(); });

    auto* config_cmd = app.add_subcommand("config", "Configure the Prometheus endpoint");
    config_cmd->require_subcommand(1);

    auto* config_ls = config_cmd->add_subcommand("ls", "List monitoring configuration");
    config_ls->callback(
        [this]() { mReq.mutable_monit()->mutable_config()->mutable_ls(); });

    auto* config_set = config_cmd->add_subcommand("set", "Set monitoring configuration");
    config_set->require_option(1, 2);
    config_set->add_option("--port", port, "Prometheus endpoint TCP port");
    config_set->add_option("--cache-ttl", cache_ttl,
                           "Prometheus scrape cache TTL in seconds");
    config_set->callback([this, &port, &cache_ttl]() {
      auto* set = mReq.mutable_monit()->mutable_config()->mutable_set();

      if (port) {
        uint32_t parsed = 0;

        if (!ParseUint32(*port, parsed)) {
          throw CLI::ValidationError("--port", "invalid unsigned integer");
        }

        set->set_port(parsed);
      }

      if (cache_ttl) {
        uint32_t parsed = 0;

        if (!ParseUint32(*cache_ttl, parsed)) {
          throw CLI::ValidationError("--cache-ttl", "invalid unsigned integer");
        }

        set->set_cache_ttl_seconds(parsed);
      }
    });

    try {
      app.parse(std::string("monit ") + arg, true);
    } catch (const CLI::ParseError&) {
      return false;
    }

    return mReq.has_monit();
  }
};

class MonitProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "monit";
  }

  const char*
  description() const override
  {
    return "Configure EOS monitoring endpoints";
  }

  std::string
  helpText() const override
  {
    return MakeMonitHelp();
  }

  std::vector<std::string>
  complete(const std::vector<std::string>& args) const override
  {
    return eos_help_completion_candidates(name(), helpText(), args);
  }

  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }

  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    std::ostringstream oss;

    for (size_t i = 0; i < args.size(); ++i) {
      if (i) {
        oss << ' ';
      }

      oss << args[i];
    }

    const std::string joined = oss.str();

    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    MonitHelper helper(*ctx.globalOpts);

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
    fprintf(stderr, "%s", MakeMonitHelp().c_str());
  }
};
} // namespace

void
RegisterMonitNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MonitProtoCommand>());
}
