// ----------------------------------------------------------------------
// File: cp-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

extern int com_cp(char*);
namespace {
class CpCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "cp";
  }
  const char*
  description() const override
  {
    return "Copy files";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    ConsoleArgParser p;
    p.addOption({"atomic", '\0', false, false, "", "atomic upload", ""});
    p.addOption({"rate", '\0', true, false, "<rate>", "rate limit", ""});
    p.addOption({"streams", '\0', true, false, "<n>", "streams", ""});
    p.addOption({"depth", '\0', true, false, "<d>", "depth", ""});
    p.addOption({"checksum", '\0', false, false, "", "print checksums", ""});
    p.addOption({"", 'a', false, false, "", "append", ""});
    p.addOption({"", 'p', false, false, "", "mkdir -p destination path", ""});
    p.addOption({"", 'n', false, false, "", "hide progress", ""});
    p.addOption({"", 'S', false, false, "", "print summary", ""});
    p.addOption({"debug", 'd', true, false, "[lvl]", "debug", ""});
    p.addOption({"silent", 's', false, false, "", "silent", ""});
    p.addOption({"no-overwrite", 'k', false, false, "", "no overwrite", ""});
    p.addOption({"preserve", 'P', false, false, "", "preserve times", ""});
    p.addOption({"recursive", 'r', false, false, "", "recursive", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.size() < 2) {
      fprintf(stderr, "Usage: cp [options] <src> <dst>\n");
      global_retc = EINVAL;
      return 0;
    }
    const std::string& src = pos[pos.size() - 2];
    const std::string& dst = pos[pos.size() - 1];
    auto isEOSPath = [&](const std::string& s) {
      return (!s.empty() && s[0] == '/') || s.rfind("/eos/", 0) == 0;
    };
    if (isEOSPath(src) && isEOSPath(dst) && !r.has("recursive")) {
      XrdOucString in = "mgm.cmd=file";
      in += "&mgm.subcmd=copy";
      XrdOucString sp = abspath(src.c_str());
      XrdOucString dp = abspath(dst.c_str());
      in += "&mgm.path=";
      in += sp;
      XrdOucString opt = "";
      if (r.has("no-overwrite") || r.has("k"))
        opt += "k";
      if (r.has("silent") || r.has("s"))
        opt += "s";
      if (r.has("checksum"))
        opt += "c";
      if (opt.length()) {
        in += "&mgm.file.option=";
        in += opt;
      }
      in += "&mgm.file.target=";
      in += dp;
      if (r.has("p")) {
        std::string parent = dp.c_str();
        auto posSlash = parent.find_last_of('/');
        if (posSlash != std::string::npos && posSlash > 0)
          parent = parent.substr(0, posSlash);
        XrdOucString min = "mgm.cmd=mkdir&mgm.option=p&mgm.path=";
        min += parent.c_str();
        ctx.outputResult(ctx.clientCommand(min, false, nullptr), true);
      }
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
      return 0;
    }
    // Fallback to legacy for non-EOS protocols, recursive or wildcard cases
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    return com_cp((char*)joined.c_str());
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: cp [--async] [--atomic] [--rate=<rate>] [--streams=<n>] "
        "[--depth=<d>] [--checksum] [--no-overwrite|-k] [--preserve|-p] "
        "[--recursive|-r|-R] [-s|--silent] [-a] [-n] [-S] [-d[=][<lvl>] <src> "
        "<dst>\n"
        "'[eos] cp ..' provides copy functionality to EOS.\n"
        "          <src>|<dst> can be root://<host>/<path>, a local path "
        "/tmp/../ or an eos path /eos/ in the connected instance\n"
        "Options:\n"
        "       --atomic        : run an atomic upload where files are only "
        "visible with the target name when their are completely uploaded [ "
        "adds ?eos.atomic=1 to the target URL ]\n"
        "       --rate          : limit the cp rate to <rate>\n"
        "       --streams       : use <#> parallel streams\n"
        "       --depth         : depth for recursive copy\n"
        "       --checksum      : output the checksums\n"
        "       -a              : append to the target, don't truncate\n"
        "       -p              : create destination directory\n"
        "       -n              : hide progress bar\n"
        "       -S              : print summary\n"
        "   -d | --debug          : enable debug information (optional "
        "<lvl>=1|2|3)\n"
        "   -s | --silent         : no output outside error messages\n"
        "   -k | --no-overwrite   : disable overwriting of files\n"
        "   -P | --preserve       : preserves file creation and modification "
        "time from the source\n"
        "   -r | -R | --recursive : copy source location recursively\n"
        "\n"
        "Remark: \n"
        "       If you deal with directories always add a '/' in the end of "
        "source or target paths e.g. if the target should be a directory and "
        "not a file put a '/' in the end. To copy a directory hierarchy use "
        "'-r' and source and target directories terminated with '/' !\n"
        "\n"
        "Examples: \n"
        "       eos cp /var/data/myfile /eos/foo/user/data/                   "
        ": copy 'myfile' to /eos/foo/user/data/myfile\n"
        "       eos cp /var/data/ /eos/foo/user/data/                         "
        ": copy all plain files in /var/data to /eos/foo/user/data/\n"
        "       eos cp -r /var/data/ /eos/foo/user/data/                      "
        ": copy the full hierarchy from /var/data/ to /eos/foo/user/data/ => "
        "empty directories won't show up on the target!\n"
        "       eos cp -r --checksum --silent /var/data/ /eos/foo/user/data/  "
        ": copy the full hierarchy and just printout the checksum information "
        "for each file copied!\n"
        "\nS3:\n"
        "      URLs have to be written as:\n"
        "         as3://<hostname>/<bucketname>/<filename> as implemented in "
        "ROOT\n"
        "      or as3:<bucketname>/<filename> with environment variable "
        "S3_HOSTNAME set\n"
        "     and as3:....?s3.id=<id>&s3.key=<key>\n\n"
        "      The access id can be defined in 3 ways:\n"
        "      env S3_ACCESS_ID=<access-id>          [as used in ROOT  ]\n"
        "      env S3_ACCESS_KEY_ID=<access-id>      [as used in libs3 ]\n"
        "      <as3-url>?s3.id=<access-id>           [as used in EOS transfers "
        "]\n"
        "\n"
        "      The access key can be defined in 3 ways:\n"
        "      env S3_ACCESS_KEY=<access-key>        [as used in ROOT ]\n"
        "      env S3_SECRET_ACCESS_KEY=<access-key> [as used in libs3 ]\n");
  }
};
} // namespace

void
RegisterCpNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CpCommand>());
}
