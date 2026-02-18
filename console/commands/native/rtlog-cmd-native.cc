// ----------------------------------------------------------------------
// File: rtlog-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeRtlogHelp()
{
  return "Usage: rtlog [<queue>|*|.] [<sec>] [<debug>] [filter-word]\n\n"
         "Real-time logging. Query queue for log lines.\n\n"
         "  *  query all nodes\n"
         "  .  query only the connected MGM (default if omitted)\n"
         "  <sec>  seconds in the past (default 3600)\n"
         "  <debug>  debug level (default err)\n";
}

void ConfigureRtlogApp(CLI::App& app,
                       std::string& queue,
                       std::string& lines,
                       std::string& tag,
                       std::string& filter)
{
  app.name("rtlog");
  app.description("Real-time logging");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRtlogHelp();
      }));
  app.add_option("queue", queue, "queue (*|.|path)")->default_val(".");
  app.add_option("lines", lines, "seconds in past")->default_val("10");
  app.add_option("tag", tag, "debug level")->default_val("err");
  app.add_option("filter", filter, "filter word");
}

class RtlogCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "rtlog";
  }
  const char*
  description() const override
  {
    return "Real-time logging";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    std::string queue;
    std::string lines;
    std::string tag;
    std::string filter;
    ConfigureRtlogApp(app, queue, lines, tag, filter);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString q = queue.c_str();
    XrdOucString l = lines.c_str();
    XrdOucString t = tag.c_str();
    XrdOucString f = filter.c_str();

    if (!q.length() || (q != "." && q != "*" && !q.beginswith("/eos/"))) {
      f = t;
      t = l;
      l = q;
      q = ".";
    }

    XrdOucString in = "mgm.cmd=rtlog&mgm.rtlog.queue=";
    in += q;
    in += "&mgm.rtlog.lines=";
    in += (l.length() ? l.c_str() : "10");
    in += "&mgm.rtlog.tag=";
    in += (t.length() ? t.c_str() : "err");
    if (f.length()) {
      in += "&mgm.rtlog.filter=";
      in += f;
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeRtlogHelp().c_str());
  }
};
} // namespace

void
RegisterRtlogNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RtlogCommand>());
}
