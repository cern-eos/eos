// ----------------------------------------------------------------------
// File: who-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeWhoHelp(const CLI::App* app)
{
  std::ostringstream oss;
  const std::string& name = app->get_name();
  oss << "Usage: " << (name.empty() ? "who" : name) << " [OPTION]...\n";
  const std::string desc = app->get_description();
  if (!desc.empty()) {
    oss << desc << "\n";
  }
  oss << "\nOptions:\n";

  std::vector<std::pair<std::string, std::string>> lines;
  size_t max_name = 0;
  for (const auto* opt : app->get_options()) {
    if (!opt || !opt->nonpositional()) {
      continue;
    }
    std::string opt_name = opt->get_name(false, true);
    if (opt_name.empty()) {
      continue;
    }
    for (size_t i = 0; i + 1 < opt_name.size(); ++i) {
      if (opt_name[i] == ',' && opt_name[i + 1] != ' ') {
        opt_name.insert(i + 1, " ");
        ++i;
      }
    }
    std::string opt_desc = opt->get_description();
    max_name = std::max(max_name, opt_name.size());
    lines.emplace_back(std::move(opt_name), std::move(opt_desc));
  }

  for (const auto& line : lines) {
    oss << "  " << line.first;
    if (!line.second.empty()) {
      size_t pad = (max_name > line.first.size()) ? (max_name - line.first.size()) : 0;
      oss << std::string(pad + 2, ' ') << line.second;
    }
    oss << "\n";
  }
  return oss.str();
}

void ConfigureWhoApp(CLI::App& app,
                     bool& opt_c,
                     bool& opt_n,
                     bool& opt_z,
                     bool& opt_a,
                     bool& opt_m,
                     bool& opt_s,
                     bool& opt_h)
{
  app.name("who");
  app.description("print statistics about active users (idle<5min)");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App* app, std::string, CLI::AppFormatMode) {
        return MakeWhoHelp(app);
      }));
  app.add_flag("-c", opt_c, "break down by client host");
  app.add_flag("-n", opt_n, "print id's instead of names");
  app.add_flag("-z", opt_z, "print auth protocols");
  app.add_flag("-a", opt_a, "print all");
  app.add_flag("-s", opt_s, "print summary for clients");
  app.add_flag("-m", opt_m, "print in monitoring format <key>=<value>");
  app.add_flag("-h,--help", opt_h, "help");
}

class WhoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "who";
  }
  const char*
  description() const override
  {
    return "Statistics about connected users";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    CLI::App app;
    bool opt_c = false;
    bool opt_n = false;
    bool opt_z = false;
    bool opt_a = false;
    bool opt_m = false;
    bool opt_s = false;
    bool opt_h = false;
    ConfigureWhoApp(app, opt_c, opt_n, opt_z, opt_a, opt_m, opt_s, opt_h);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    if (opt_h) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=who";
    std::string opts;
    if (opt_c)
      opts += 'c';
    if (opt_n)
      opts += 'n';
    if (opt_z)
      opts += 'z';
    if (opt_a)
      opts += 'a';
    if (opt_s)
      opts += 's';
    if (opt_m)
      opts += 'm';
    if (!opts.empty()) {
      in += "&mgm.option=";
      in += opts.c_str();
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    bool opt_c = false;
    bool opt_n = false;
    bool opt_z = false;
    bool opt_a = false;
    bool opt_m = false;
    bool opt_s = false;
    bool opt_h = false;
    ConfigureWhoApp(app, opt_c, opt_n, opt_z, opt_a, opt_m, opt_s, opt_h);
    const std::string help = app.help();
    fprintf(stderr, "%s", help.c_str());
  }
};
} // namespace

void
RegisterWhoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<WhoCommand>());
}
