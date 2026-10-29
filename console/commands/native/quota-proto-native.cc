// ----------------------------------------------------------------------
// File: quota-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <iomanip>
#include <memory>
#include <sstream>

namespace {
std::string MakeQuotaHelp()
{
  std::ostringstream oss;
  oss << "  quota [-m] [[-p|-x|-q] <path>]\n"
      << "    -m : print in monitoring <key>=<value> format\n"
      << "    -p/--path <path> : show only the quota node responsible for <path>; "
         "<path> can also be given as the last argument without -p\n"
      << "    -x <path> : as -p, but <path> has to exist\n"
      << "    -q <path> : as -p, but <path> has to be a quota node\n"
      << std::endl
      << "    Without <path>, your quota on every quota node is shown.\n"
      << std::endl
      << "    A path without a trailing '/' that is not an existing directory is "
         "treated as a file and does not match the quota node <path>/.\n"
      << std::endl
      << "  quota ls [-m] [-n] [-r] [-s] [-u <uid>] [-g <gid>] [[-p|-x|-q] <path>]\n"
      << "    -m : print in monitoring <key>=<value> format\n"
      << "    -n : print uid and gid numbers instead of names\n"
      << "    -r/--recursive : select every quota node at or below <path> instead of "
         "only the quota node responsible for <path>\n"
      << "    -s/--summary : print one line per selected quota node and a total line "
         "instead of the full tables\n"
      << "    -u/--uid <uid> : show only the quota of user <uid>, a name or a number\n"
      << "    -g/--gid <gid> : show only the quota of group <gid>, a name or a number\n"
      << "    -p/--path <path> : select the quota node responsible for <path>; "
         "<path> can also be given as the last argument without -p\n"
      << "    -x <path> : as -p, but <path> has to exist\n"
      << "    -q <path> : as -p, but <path> has to be a quota node\n"
      << std::endl
      << "    Without <path>, every quota node is selected.\n"
      << std::endl
      << "    A path without a trailing '/' that is not an existing directory is "
         "treated as a file and does not match the quota node <path>/.\n"
      << std::endl
      << "    The summary shows the project quota of a node if one is defined, "
         "otherwise the sum of all user quotas or, if there are none, the sum of "
         "all group quotas. With -u or -g it shows the quota of that user or "
         "group and skips the nodes where it has neither quota nor usage.\n"
      << std::endl
      << "    The total line sums the usage of all listed nodes. A limit is only "
         "summed if every listed node defines it with the same type, otherwise it "
         "is shown as '-'.\n"
      << std::endl
      << "  quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] [-p] <path>\n"
      << "    -u/--uid <uid> : set the quota of user <uid>, a name or a number\n"
      << "    -g/--gid <gid> : set the quota of group <gid>, a name or a number\n"
      << "    -v/--volume <bytes> : volume limit, for example 10T\n"
      << "    -i/--inodes <inodes> : limit on the number of files\n"
      << "    -p/--path <path> : directory of the quota node; it can also be given "
         "as the last argument without -p\n"
      << std::endl
      << "    Specify at least -v or -i. The quota node is created if <path> is not "
         "one yet.\n"
      << std::endl
      << "  quota rm -u <uid>|-g <gid> [-v|-i] [-p] <path>\n"
      << "    -u/--uid <uid> : remove the quota of user <uid>, a name or a number\n"
      << "    -g/--gid <gid> : remove the quota of group <gid>, a name or a number\n"
      << "    -v/--volume : remove only the volume limit\n"
      << "    -i/--inodes : remove only the inode limit\n"
      << "    -p/--path <path> : directory of the quota node; it can also be given "
         "as the last argument without -p\n"
      << std::endl
      << "    Without -v or -i, both limits are removed.\n"
      << std::endl
      << "  quota rmnode [-p] <path>\n"
      << "    -p/--path <path> : directory of the quota node; it can also be given "
         "as the last argument without -p\n"
      << std::endl
      << "    Removes the quota node and every quota defined on it.\n"
      << std::endl
      << "Examples:\n"
      << "    quota -> show your quota on every quota node\n"
      << "    quota ls /eos/lhc/raw/ -> quota node responsible for /eos/lhc/raw/\n"
      << "    quota ls -r -s /eos/lhc/ -> one line per quota node below /eos/lhc/\n"
      << "    quota ls -r -s -u alice /eos/lhc/ -> quota of alice on every quota "
         "node below /eos/lhc/\n"
      << "    quota set -g 99 -v 100T -i 10M /eos/lhc/raw/ -> project quota on "
         "/eos/lhc/raw/\n"
      << "    quota rm -u alice -v /eos/lhc/ -> remove the volume limit of alice\n";
  return oss.str();
}

void ConfigureQuotaApp(CLI::App& app)
{
  app.name("quota");
  app.description("Quota System configuration");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeQuotaHelp();
      }));
}

class QuotaHelper : public ICmdHelper {
public:
  QuotaHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~QuotaHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    eos::console::QuotaProto* quota = mReq.mutable_quota();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    tokenizer.NextToken(token);
    if (token == "" || token == "-m" || token == "--path" || token == "-p" ||
        token == "-x" || token == "-q" || (token.find('/') == 0)) {
      auto* lsuser = quota->mutable_lsuser();
      std::string aux;
      if (token == "") {
        aux = DefaultRoute(false);
        if (!aux.empty() && aux[0] == '/')
          lsuser->set_space(aux);
      } else {
        do {
          if (token == "-m") {
            lsuser->set_format(true);
            aux = DefaultRoute(false);
            if (!aux.empty() && aux[0] == '/')
              lsuser->set_space(aux);
          } else if (token == "--path" || token == "-p" || token == "-x" ||
                     token == "-q" || (token.find('/') == 0)) {
            if (token == "-x")
              lsuser->set_exists(true);
            if (token == "-q")
              lsuser->set_quotanode(true);
            if (token == "--path" || token == "-p" || token == "-x" ||
                token == "-q") {
              if (tokenizer.NextToken(token))
                lsuser->set_space(token);
              else
                return false;
            } else if (token.find('/') == 0) {
              lsuser->set_space(token);
              if (tokenizer.NextToken(token))
                return false;
            }
          } else {
            return false;
          }
        } while (tokenizer.NextToken(token));
      }
    } else if (token == "ls") {
      auto* ls = quota->mutable_ls();
      while (tokenizer.NextToken(token)) {
        if (token == "--uid" || token == "-u") {
          if (tokenizer.NextToken(token))
            ls->set_uid(token);
          else
            return false;
        } else if (token == "--gid" || token == "-g") {
          if (tokenizer.NextToken(token))
            ls->set_gid(token);
          else
            return false;
        } else if (token == "-m") {
          ls->set_format(true);
        } else if (token == "-s" || token == "--summary") {
          ls->set_summary(true);
        } else if (token == "-r" || token == "--recursive") {
          ls->set_recursive(true);
        } else if (token == "-n") {
          ls->set_printid(true);
        } else if (token == "--path" || token == "-p" || token == "-x" || token == "-q" ||
                   (token.find('/') == 0)) {
          if (token == "-x")
            ls->set_exists(true);
          if (token == "-q")
            ls->set_quotanode(true);
          if (token == "--path" || token == "-p" || token == "-q" ||
              token == "-x") {
            if (tokenizer.NextToken(token))
              ls->set_space(token);
            else
              return false;
          } else if (token.find('/') == 0) {
            ls->set_space(token);
            if (tokenizer.NextToken(token))
              return false;
          }
        } else {
          return false;
        }
      }
    } else if (token == "set") {
      auto* set = quota->mutable_set();
      while (tokenizer.NextToken(token)) {
        if (token == "--uid" || token == "-u") {
          if (tokenizer.NextToken(token))
            set->set_uid(token);
          else
            return false;
        } else if (token == "--gid" || token == "-g") {
          if (tokenizer.NextToken(token))
            set->set_gid(token);
          else
            return false;
        } else if (token == "--volume" || token == "-v") {
          if (tokenizer.NextToken(token))
            set->set_maxbytes(token);
          else
            return false;
        } else if (token == "--inodes" || token == "-i") {
          if (tokenizer.NextToken(token))
            set->set_maxinodes(token);
          else
            return false;
        } else if (token == "--path" || token == "-p" ||
                   (token.find('/') == 0)) {
          if (token == "--path" || token == "-p") {
            if (tokenizer.NextToken(token))
              set->set_space(token);
            else
              return false;
          } else if (token.find('/') == 0) {
            set->set_space(token);
            if (tokenizer.NextToken(token))
              return false;
          }
        } else {
          return false;
        }
      }
    } else if (token == "rm") {
      auto* rm = quota->mutable_rm();
      while (tokenizer.NextToken(token)) {
        if (token == "--uid" || token == "-u") {
          if (tokenizer.NextToken(token))
            rm->set_uid(token);
          else
            return false;
        } else if (token == "--gid" || token == "-g") {
          if (tokenizer.NextToken(token))
            rm->set_gid(token);
          else
            return false;
        } else if (token == "--volume" || token == "-v") {
          rm->set_type(eos::console::QuotaProto_RmProto::VOLUME);
        } else if (token == "--inodes" || token == "--inode" || token == "-i") {
          rm->set_type(eos::console::QuotaProto_RmProto::INODE);
        } else if (token == "--path" || token == "-p" || (token.find('/') == 0)) {
          if (token == "--path" || token == "-p") {
            if (tokenizer.NextToken(token))
              rm->set_space(token);
            else
              return false;
          } else if (token.find('/') == 0) {
            rm->set_space(token);
            if (tokenizer.NextToken(token))
              return false;
          }
        } else {
          return false;
        }
      }
    } else if (token == "rmnode") {
      auto* rmnode = quota->mutable_rmnode();
      tokenizer.NextToken(token);
      if (token == "--path" || token == "-p" || (token.find('/') == 0)) {
        if (token == "--path" || token == "-p") {
          if (tokenizer.NextToken(token))
            rmnode->set_space(token);
          else
            return false;
        } else if (token.find('/') == 0) {
          rmnode->set_space(token);
          if (tokenizer.NextToken(token))
            return false;
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }
};

class QuotaProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "quota";
  }
  const char*
  description() const override
  {
    return "Quota System configuration";
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
      if (i)
        oss << ' ';
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    QuotaHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (helper.NeedsConfirmation() && !helper.ConfirmOperation()) {
      global_retc = EINTR;
      return 0;
    }
    global_retc = helper.Execute(true, true);
    return 0;
  }
  std::string
  helpText() const override
  {
    return MakeQuotaHelp();
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureQuotaApp(app);
    eos::console::PrintLegacyHelp(app.help());
  }
};
} // namespace

void
RegisterQuotaProtoNativeCommand()
{
  eos::console::CommandHelpSpec help;
  help.topics = {
      {{}, "  quota [-m]", "Show your quota on the quota nodes."},
      {{"ls"}, "  quota ls ", "List configured quotas."},
      {{"set"}, "  quota set ", "Set user or group quota limits."},
      {{"rm"}, "  quota rm ", "Remove user or group quota limits."},
      {{"rmnode"}, "  quota rmnode ", "Remove a quota node."},
  };
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<QuotaProtoCommand>(), std::move(help)));
}
