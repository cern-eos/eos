// ----------------------------------------------------------------------
// File: sched-proto-native.cc
// ----------------------------------------------------------------------

#include "common/ParseUtils.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {

struct SchedHelper : public ICmdHelper {
  SchedHelper(const GlobalOptions& opts) : ICmdHelper(opts) { mIsAdmin = true; }
  ~SchedHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::SchedProto* sched = mReq.mutable_sched();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token))
      return false;
    if (token == "configure" || token == "config") {
      eos::console::SchedProto_ConfigureProto* config = sched->mutable_config();
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "type") {
        auto* type = config->mutable_type();
        if (!tokenizer.NextToken(token))
          return false;
        type->set_schedtype(token);
      } else if (token == "weight") {
        auto* w = config->mutable_weight();
        std::string space, id_str, weight_str;
        if (!(tokenizer.NextToken(space) && tokenizer.NextToken(id_str) &&
              tokenizer.NextToken(weight_str)))
          return false;
        int32_t item_id = 0;
        uint8_t weight = 0;
        try {
          item_id = std::stoi(id_str);
          unsigned long wtmp = std::stoul(weight_str);
          if (wtmp > 255)
            return false;
          weight = static_cast<uint8_t>(wtmp);
        } catch (...) {
          return false;
        }
        w->set_id(item_id);
        w->set_weight(weight);
        w->set_spacename(space);
      } else if (token == "show") {
        auto* showp = config->mutable_show();
        if (!tokenizer.NextToken(token))
          return false;
        if (token == "type") {
          showp->set_option(eos::console::SchedProto_ShowProto::TYPE);
          if (tokenizer.NextToken(token))
            showp->set_spacename(token);
        }
      } else if (token == "forcerefresh") {
        config->mutable_refresh();
      } else {
        return false;
      }
    } else if (token == "ls") {
      auto* ls = sched->mutable_ls();
      if (!tokenizer.NextToken(token))
        return false;
      ls->set_spacename(token);
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "bucket")
        ls->set_option(eos::console::SchedProto_LsProto::BUCKET);
      else if (token == "disk")
        ls->set_option(eos::console::SchedProto_LsProto::DISK);
      else
        ls->set_option(eos::console::SchedProto_LsProto::ALL);
    } else {
      return false;
    }
    return true;
  }
};

class SchedProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "sched";
  }
  const char*
  description() const override
  {
    return "Configure scheduler options";
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
    SchedHelper helper(gGlobalOpts);
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
            " sched configure type <schedtype>\n"
            "\t <schedtype> is one of "
            "roundrobin,weightedrr,tlrr,random,weightedrandom,geo\n"
            "\t if configured via space; space takes precedence\n"
            " sched configure weight <space> <fsid> <weight>\n"
            "\t configure weight for a given fsid in the given space\n"
            " sched configure show type [spacename]\n"
            "\t show existing configured scheduler; optionally for space\n"
            " sched configure forcerefresh [spacename]\n"
            "\t Force refresh scheduler internal state\n"
            " ls <spacename> <bucket|disk|all>\n");
  }
};
} // namespace

void
RegisterSchedProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<SchedProtoCommand>());
}
