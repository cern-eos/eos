// ----------------------------------------------------------------------
// File: archive-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class ArchiveCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "archive";
  }
  const char*
  description() const override
  {
    return "Archive Interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty() || wants_help(args[0].c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    const std::string& sub = args[0];
    std::ostringstream in_cmd;
    in_cmd << "mgm.cmd=archive&mgm.subcmd=" << sub;

    if (sub == "create") {
      std::string path = (args.size() > 1 ? args[1] : gPwd.c_str());
      XrdOucString p = abspath(path.c_str());
      in_cmd << "&mgm.archive.path=" << p.c_str();
    } else if (sub == "put" || sub == "get" || sub == "purge" ||
               sub == "delete") {
      size_t i = 1;
      bool retry = false;
      if (i < args.size() && args[i].rfind("--", 0) == 0) {
        if (args[i] == "--retry") {
          retry = true;
          ++i;
        } else {
          printHelp(); global_retc = EINVAL; return 0;
        }
      }
      std::string path = (i < args.size() ? args[i] : std::string());
      if (path.empty()) {
        // default to current pwd if not provided
        path = gPwd.c_str();
      }
      if (retry) in_cmd << "&mgm.archive.option=r";
      XrdOucString p = abspath(path.c_str());
      in_cmd << "&mgm.archive.path=" << p.c_str();
    } else if (sub == "transfers") {
      std::string tok = (args.size() > 1 ? args[1] : std::string());
      if (tok.empty())
        in_cmd << "&mgm.archive.option=all";
      else
        in_cmd << "&mgm.archive.option=" << tok;
    } else if (sub == "list") {
      std::string tok = (args.size() > 1 ? args[1] : std::string());
      if (tok.empty())
        in_cmd << "&mgm.archive.path=/";
      else if (tok == "./" || tok == ".") {
        XrdOucString p = abspath(gPwd.c_str());
        in_cmd << "&mgm.archive.path=" << p.c_str();
      } else
        in_cmd << "&mgm.archive.path=" << tok;
    } else if (sub == "kill") {
      if (args.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
      in_cmd << "&mgm.archive.option=" << args[1];
    } else {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = in_cmd.str().c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    std::ostringstream oss;
    oss << "Usage: archive <subcmd> " << std::endl
        << "               create <path>                          : create archive file\n"
        << "               put [--retry] <path>                   : copy files from EOS to archive location\n"
        << "               get [--retry] <path>                   : recall archive back to EOS\n"
        << "               purge[--retry] <path>                  : purge files on disk\n"
        << "               transfers [all|put|get|purge|job_uuid] : show status of running jobs\n"
        << "               list [<path>]                          : show status of archived directories in subtree\n"
        << "               kill <job_uuid>                        : kill transfer\n"
        << "               delete <path>                          : delete files from tape, keeping the ones on disk\n"
        << "               help [--help|-h]                       : display help message\n";
    fprintf(stderr, "%s", oss.str().c_str());
  }
};
} // namespace

void
RegisterArchiveNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ArchiveCommand>());
}
