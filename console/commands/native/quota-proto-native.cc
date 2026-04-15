// ----------------------------------------------------------------------
// File: quota-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <iomanip>
#include <memory>
#include <sstream>

namespace {
std::string MakeQuotaHelp()
{
  std::ostringstream oss;
  oss << "Usage:\n"
      << "    quota [<path>]                    : show personal quota for all or "
         "only the quota node responsible for <path>\n"
      << "    quota ls [-n] [-m] [-u <uid>] [-g <gid>] [[-p|x|q] <path>] : list "
         "configured quota and quota node(s)\n"
      << "                                                                       -p : "
         "find closest matching quotanode\n"
      << "                                                                       -x : "
         "as -p but <path> has to exist\n"
      << "                                                                       -q : "
         "as -p but <path> has to be a quotanode\n"
      << "    quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] [[-p] <path>] : set "
         "volume and/or inode quota by uid or gid\n"
      << "    quota rm -u <uid>|-g <gid> [-v] [-i] [[-p] <path>] : remove configured "
         "quota type(s) for uid/gid in path\n"
      << "    quota rmnode [-p] <path> [--really-want] : remove quota node and every "
         "defined quota on that node\n"
      << std::endl
      << "  General options:\n"
      << "    -m : print information in monitoring <key>=<value> format\n"
      << "    -n : don't translate ids, print uid and gid number\n"
      << "    -u/--uid <uid> : print information only for uid <uid>\n"
      << "    -g/--gid <gid> : print information only for gid <gid>\n"
      << "    -p/--path <path> : print information only for path <path> - this can also "
         "be given without -p or --path\n"
      << "    -v/--volume <bytes> : refer to volume limit in <bytes>\n"
      << "    -i/--inodes <inodes> : refer to inode limit in number of <inodes>\n"
      << std::endl
      << "  Notes:\n"
      << "    => you have to specify either the user or the group identified by the "
         "unix id or the user/group name\n"
      << "    => the space argument is by default assumed as 'default'\n"
      << "    => you have to specify at least a volume or an inode limit to set quota\n"
      << "    => for convenience all commands can just use <path> as last argument "
         "omitting the -p|--path e.g. quota ls /eos/ ...\n"
      << "    => if <path> is not terminated with a '/' it is assumed to be a file so "
         "it won't match the quota node with <path>/ !\n"
      << "    => rmnode requires confirmation unless --really-want is passed\n";
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
        } else if (token == "-n") {
          ls->set_printid(true);
        } else if (token == "--path" || token == "-p" || token == "-x" ||
                   token == "-q" || (token.find('/') == 0)) {
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
        } else if (token == "--inode" || token == "-i") {
          rm->set_type(eos::console::QuotaProto_RmProto::INODE);
        } else if (token == "--path" || token == "-p" ||
                   (token.find('/') == 0)) {
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
      bool dontask = false;
      auto* rmnode = quota->mutable_rmnode();
      tokenizer.NextToken(token);
      if (token == "--really-want") {
        dontask = true;
        tokenizer.NextToken(token);
      }
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
      if (!dontask) {
        fprintf(
            stderr,
            "Do you really want to delete the quota node under path: %s ?\n",
            rmnode->space().c_str());
        fprintf(stderr,
                "Use --really-want to skip interactive confirmation.\n");
        mNeedsConfirmation = true;
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
    global_retc = helper.Execute(true, true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureQuotaApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterQuotaProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<QuotaProtoCommand>());
}
