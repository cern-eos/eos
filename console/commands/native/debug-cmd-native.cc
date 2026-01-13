// ----------------------------------------------------------------------
// File: debug-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
class DebugCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "debug";
  }
  const char*
  description() const override
  {
    return "Set debug level";
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
    class LocalHelper : public ICmdHelper {
    public:
      using ICmdHelper::ICmdHelper;
      bool
      ParseCommand(const char* arg) override
      {
        eos::console::DebugProto* debugproto = mReq.mutable_debug();
        eos::common::StringTokenizer tokenizer(arg);
        tokenizer.GetLine();
        std::string token;
        if (!tokenizer.NextToken(token)) {
          return false;
        }
        if (token == "get") {
          auto* get = debugproto->mutable_get();
          get->set_placeholder(true);
        } else if (token == "this") {
          global_debug = !global_debug;
          gGlobalOpts.mDebug = global_debug;
          fprintf(stdout, "info: toggling shell debugmode to debug=%d\n",
                  global_debug);
          mIsLocal = true;
        } else {
          auto* set = debugproto->mutable_set();
          set->set_debuglevel(token);
          if (tokenizer.NextToken(token)) {
            if (token == "--filter") {
              if (!tokenizer.NextToken(token))
                return false;
              set->set_filter(token);
            } else {
              set->set_nodename(token);
              if (tokenizer.NextToken(token)) {
                if (token != "--filter")
                  return false;
                else {
                  if (!tokenizer.NextToken(token))
                    return false;
                  set->set_filter(token);
                }
              }
            }
          }
        }
        return true;
      }
    };
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    LocalHelper helper(gGlobalOpts);
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
            "debug get|this|<level> [node-queue] [--filter <unitlist>]\n"
            "'[eos] debug ...' allows to get or set the verbosity of the EOS "
            "log files in MGM and FST services.\n\n"
            "debug get : retrieve the current log level for the mgm and fsts "
            "node-queue\n\n"
            "debug this : toggle EOS shell debug mode\n\n"
            "debug  <level> [--filter <unitlist>] : set the MGM where the "
            "console is connected to into debug level <level>\n\n"
            "debug  <level> <node-queue> [--filter <unitlist>] : set the "
            "<node-queue> into debug level <level>.\n"
            "  - <node-queue> are internal EOS names e.g. "
            "'/eos/<hostname>:<port>/fst'\n"
            "  - <unitlist> is a comma separated list of strings of software "
            "units which should be filtered out in the message log!\n\n"
            "The allowed debug levels are: "
            "debug,info,warning,notice,err,crit,alert,emerg\n");
  }
};
} // namespace

void
RegisterDebugNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DebugCommand>());
}
