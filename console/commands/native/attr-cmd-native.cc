// ----------------------------------------------------------------------
// File: attr-native.cc
// ----------------------------------------------------------------------

#include "common/SymKeys.hh"
#include "common/Utils.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {
std::string MakeAttrHelp()
{
  std::ostringstream oss;
  oss << "Usage: attr [OPTIONS] ls|set|get|rm ...\n\n";
  oss << "'[eos] attr ..' provides the extended attribute interface for "
         "directories in EOS.\n\n";
  oss << "Options:\n"
      << "  attr [-r] ls <identifier>\n"
      << "      List attributes of path\n"
      << "      -r : list recursive on all directory children\n"
      << "  attr [-r] set [-c] <key>=<value> <identifier>\n"
      << "      Use <key>= to set an empty value (e.g. sys.acl=)\n"
      << "      Set attributes of path (-r recursive, -c only if absent)\n"
      << "  attr [-r] set default=replica|raiddp|raid5|raid6|archive|qrain "
         "<identifier>\n"
      << "      Set EOS default layout attributes for the path\n"
      << "  attr [-r] [-V] get <key> <identifier>\n"
      << "      Get attributes of path (-r recursive, -V only print value)\n"
      << "  attr [-r] rm <key> <identifier>\n"
      << "      Delete attributes of path (-r recursive)\n"
      << "  attr [-r] link <origin> <identifier>\n"
      << "      Link attributes of <origin> under <identifier> (-r recursive)\n"
      << "  attr [-r] unlink <identifier>\n"
      << "      Remove attribute link of <identifier> (-r recursive)\n"
      << "  attr [-r] fold <identifier>\n"
      << "      Fold attributes of <identifier> when attr link is defined\n"
      << "      (identical attributes are removed locally)\n"
      << "\n"
      << "Remarks:\n";
  oss << "         <identifier> = "
         "<path>|fid:<fid-dec>|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>\n"
      << "                        deprecated pid:<pid-dec>|pxid:<pid-hex>\n";
  oss << "         If <key> starts with 'sys.' you have to be member of the "
         "sudoers group to see these attributes or modify.\n";
  oss << "\nAdministrator Variables:\n";
  oss << "         sys.forced.space=<space>              : enforces to use "
         "<space>    [configuration dependent]\n";
  oss << "         sys.forced.group=<group>              : enforces to use "
         "<group>, where <group> is the numerical index of <space>.<n>    "
         "[configuration dependent]\n";
  oss << "         sys.forced.layout=<layout>            : enforces to use "
         "<layout>   [<layout>=(plain,replica,raid5,raid6,archive,qrain)]\n";
  oss << "         sys.forced.checksum=<checksum>        : enforces to use "
         "file-level checksum <checksum>\n";
  oss << "                                              <checksum> = "
         "adler,crc32,crc32c,md5,sha\n";
  oss << "         sys.forced.blockchecksum=<checksum>   : enforces to use "
         "block-level checksum <checksum>\n";
  oss << "                                              <checksum> = "
         "adler,crc32,crc32c,md5,sha\n";
  oss << "         sys.forced.nstripes=<n>               : enforces to use <n> "
         "stripes[<n>= 1..16]\n";
  oss << "         sys.forced.blocksize=<w>              : enforces to use a "
         "blocksize of <w> - <w> can be 4k,64k,128k,256k or 1M \n";
  oss << "         sys.forced.placementpolicy=<policy>[:geotag] : enforces to "
         "use replica/stripe placement policy <policy> [<policy>="
         "{scattered|hybrid:<geotag>|gathered:<geotag>}]\n";
  oss << "         sys.forced.nouserplacementpolicy=1    : disables user "
         "defined replica/stripe placement policy\n";
  oss << "         sys.forced.nouserlayout=1             : disables the user "
         "settings with user.forced.<xxx>\n";
  oss << "         sys.forced.nofsselection=1            : disables user "
         "defined filesystem selection with environment variables for reads\n";
  oss << "         sys.forced.bookingsize=<bytes>        : set's the number of "
         "bytes which get for each new created replica\n";
  oss << "         sys.forced.minsize=<bytes>            : set's the minimum "
         "number of bytes a file to be stored must have\n";
  oss << "         sys.forced.maxsize=<bytes>            : set's the maximum "
         "number of bytes a file to be stored can have\n";
  oss << "         sys.forced.atomic=1                   : if present enforce "
         "atomic uploads e.g. files appear only when their upload is complete - "
         "during the upload they have the name <dirname>/.<basename>.<uuid>\n";
  oss << "         sys.forced.leasetime=86400            : allows to overwrite "
         "the eosxd client provided leasetime with a new value\n";
  oss << "         sys.forced.iotype=direct|sync|dsync|csync"
      << "                                               : force the given "
         "iotype for that directory\n";
  oss << "         sys.mtime.propagation=1               : if present a change "
         "under this directory propagates an mtime change up to all parents "
         "until the attribute is not present anymore\n";
  oss << "         sys.allow.oc.sync=1                   : if present, "
         "OwnCloud clients can sync pointing to this subtree\n";
  oss << "\n";
  oss << "         sys.lru.expire.empty=<age>            : delete empty "
         "directories older than <age>\n";
  oss << "         sys.lru.expire.match=[match1:<age1>,match2:<age2>..]\n";
  oss << "                                               : defines the rule "
         "that files with a given match will be removed if \n";
  oss << "                                                 they haven't been "
         "accessed longer than <age> ago. <age> is defined like "
         "3600,3600s,60min,1h,1mo,1y...\n";
  oss << "         sys.lru.lowwatermark=<low>\n";
  oss << "         sys.lru.highwatermark=<high>        : if the watermark "
         "reaches more than <high> %%, files will be removed until the usage is "
         "reaching <low> %%.\n";
  oss << "\n";
  oss << "         sys.lru.convert.match=[match1:<age1>,match2:<age2>,...]\n";
  oss << "                                                 defines the rule "
         "that files with a given match will be converted to the layouts "
         "defined by sys.conversion.<match> when their access time reaches "
         "<age>.\n";
  oss << "\n";
  oss << "         sys.stall.unavailable=<sec>           : stall clients for "
         "<sec> seconds if a needed file system is unavailable\n";
  oss << "         sys.redirect.enoent=<host[:port]>     : redirect clients "
         "opening non existing files to <host[:port]>\n";
  oss << "         sys.redirect.enonet=<host[:port]>     : redirect clients "
         "opening inaccessible files to <host[:port]>\n";
  oss << "         sys.recycle=....                      : define the recycle "
         "bin - WARNING: use the 'recycle' interface\n";
  oss << "         sys.recycle.keeptime=<seconds>        : define the time how "
         "long files stay in a recycle bin\n";
  oss << "         sys.recycle.keepratio=< 0 .. 1.0 >    : ratio of used/max "
         "quota for space and inodes in the recycle bin\n";
  oss << "         sys.versioning=<n>                    : keep <n> versions "
         "of a file\n";
  oss << "         sys.acl=<acllist>                     : set's an ACL\n";
  oss << "         sys.eval.useracl                      : enables the "
         "evaluation of user acls\n";
  oss << "         sys.mask                              : masks all unix "
         "access permissions\n";
  oss << "         sys.owner.auth=<owner-auth-list>      : set's additional "
         "owner on a directory\n";
  oss << "         sys.attr.link=<directory>             : symbolic links for "
         "attributes\n";
  oss << "         sys.http.index=<path>                 : show a static page "
         "as directory index\n";
  oss << "         sys.accounting.*=<value>              : set accounting "
         "attributes\n";
  oss << "         sys.proc=<opaque command>             : run arbitrary "
         "command on accessing the file\n";
  oss << "\nUser Variables:\n";
  oss << "         user.forced.space, user.forced.layout, user.forced.checksum, "
         "etc. (s.a. Administrator Variables)\n";
  oss << "\nExamples:\n";
  oss << "  attr set default=replica /eos/instance/2-replica\n";
  oss << "  attr set sys.forced.nstripes=10 /eos/instance/archive\n";
  oss << "  attr set sys.acl=g:xx::!d!u /eos/instance/no-update-deletion\n";
  oss << "  attr set sys.forced.atomic=1 /eos/dev/instance/atomic/\n";
  oss << "  attr set sys.attr.link=/eos/dev/origin-attr/ "
         "/eos/dev/instance/attr-linked/\n";
  return oss.str();
}

void ConfigureAttrApp(CLI::App& app,
                     bool& opt_r,
                     bool& opt_V,
                     std::string& subcmd)
{
  app.name("attr");
  app.description("Attribute Interface");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeAttrHelp();
      }));
  app.add_flag("-r", opt_r, "recursive");
  app.add_flag("-V", opt_V, "only print value (for get)");
  app.add_option("subcmd", subcmd, "ls|set|get|rm|link|unlink|fold")
      ->required();
}

class AttrCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "attr";
  }
  const char*
  description() const override
  {
    return "Attribute Interface";
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
      oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    bool opt_r = false;
    bool opt_V = false;
    std::string sub;
    ConfigureAttrApp(app, opt_r, opt_V, sub);

    std::vector<std::string> cli_args = args;
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::string optionStr;
    if (opt_r)
      optionStr += 'r';
    if (opt_V)
      optionStr += 'V';

    std::vector<std::string> remaining = app.remaining();

    size_t idx = 0;
    std::string arg;

    auto appendOption = [&](XrdOucString& target, const std::string& opt) {
      if (!opt.empty()) {
        target += "&mgm.option=";
        target += opt.c_str();
      }
    };

    auto send_set = [&](const std::string& key,
                        const std::string& value,
                        const std::string& path,
                        bool conditional) -> int {
      std::string opt = optionStr;
      if (conditional && opt.find('c') == std::string::npos) {
        opt.push_back('c');
      }
      XrdOucString cmd = "mgm.cmd=attr&mgm.enc=b64";
      appendOption(cmd, opt);
      XrdOucString k = key.c_str();
      XrdOucString v = value.c_str();
      if (key != "default" && key != "sys.attr.link") {
        XrdOucString v64;
        eos::common::SymKey::Base64(v, v64);
        v = v64;
      }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      cmd += "&mgm.subcmd=set&mgm.attr.key=";
      cmd += k;
      cmd += "&mgm.attr.value=";
      cmd += v;
      cmd += "&mgm.path=";
      cmd += p;
      return ctx.outputResult(ctx.clientCommand(cmd, false, nullptr), true);
    };

    if (idx >= remaining.size()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    arg = remaining[idx++];
    if (sub == "set" && arg == "-c") {
      if (optionStr.find('c') == std::string::npos) {
        optionStr.push_back('c');
      }
      if (idx >= remaining.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      arg = remaining[idx++];
    }

    XrdOucString in = "mgm.cmd=attr&mgm.enc=b64";
    appendOption(in, optionStr);

    if (sub.empty() || arg.empty() ||
        (sub != "ls" && sub != "set" && sub != "get" && sub != "rm" &&
         sub != "link" && sub != "unlink" && sub != "fold")) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    if (sub == "ls") {
      XrdOucString path = PathIdentifier(arg.c_str(), true).c_str();
      in += "&mgm.subcmd=ls&mgm.path=";
      in += path;
    } else if (sub == "set" || sub == "link") {
      std::string key = arg;
      std::string value;
      int epos = XrdOucString(key.c_str()).find("=");
      const bool has_equals = (sub == "set" && epos != STR_NPOS);
      if (sub == "link") {
        key = "sys.attr.link";
        value = arg;
      } else if (epos != STR_NPOS) {
        value = key.substr(epos + 1);
        key.erase(epos);
      } else {
        value.clear();
      }

      if (!value.empty() && value.rfind("\"", 0) == 0 &&
          value.size() >= 1 && value.back() != '"') {
        while (idx < remaining.size()) {
          value += " ";
          value += remaining[idx];
          if (!remaining[idx].empty() && remaining[idx].back() == '"') {
            ++idx;
            break;
          }
          ++idx;
        }
      }

      if (value.empty() && !has_equals) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }

      std::string path;
      if (sub == "link") {
        if (idx >= remaining.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        path = remaining[idx++];
      } else {
        if (idx >= remaining.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        path = remaining[idx++];
      }

      if (path.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }

      if (key == "default") {
        bool conditional = optionStr.find('c') != std::string::npos;
        std::vector<std::pair<std::string, std::string>> defaults;
        if (value == "replica") {
          defaults = {{"sys.forced.blocksize", "4k"},
                      {"sys.forced.checksum", "adler"},
                      {"sys.forced.layout", "replica"},
                      {"sys.forced.nstripes", "2"},
                      {"sys.forced.space", "default"}};
        } else if (value == "raiddp") {
          defaults = {{"sys.forced.blocksize", "1M"},
                      {"sys.forced.checksum", "adler"},
                      {"sys.forced.layout", "raiddp"},
                      {"sys.forced.nstripes", "6"},
                      {"sys.forced.space", "default"},
                      {"sys.forced.blockchecksum", "crc32c"}};
        } else if (value == "raid5") {
          defaults = {{"sys.forced.blocksize", "1M"},
                      {"sys.forced.checksum", "adler"},
                      {"sys.forced.layout", "raid5"},
                      {"sys.forced.nstripes", "5"},
                      {"sys.forced.space", "default"},
                      {"sys.forced.blockchecksum", "crc32c"}};
        } else if (value == "raid6") {
          defaults = {{"sys.forced.blocksize", "1M"},
                      {"sys.forced.checksum", "adler"},
                      {"sys.forced.layout", "raid6"},
                      {"sys.forced.nstripes", "6"},
                      {"sys.forced.space", "default"},
                      {"sys.forced.blockchecksum", "crc32c"}};
        } else if (value == "archive") {
          defaults = {{"sys.forced.blocksize", "1M"},
                      {"sys.forced.checksum", "adler"},
                      {"sys.forced.layout", "archive"},
                      {"sys.forced.nstripes", "8"},
                      {"sys.forced.space", "default"},
                      {"sys.forced.blockchecksum", "crc32c"}};
        } else if (value == "qrain") {
          defaults = {{"sys.forced.blocksize", "1M"},
                      {"sys.forced.checksum", "adler"},
                      {"sys.forced.layout", "qrain"},
                      {"sys.forced.nstripes", "12"},
                      {"sys.forced.space", "default"},
                      {"sys.forced.blockchecksum", "crc32c"}};
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }

        int retc = 0;
        for (const auto& entry : defaults) {
          retc = retc || send_set(entry.first, entry.second, path, conditional);
        }
        global_retc = retc;
        return 0;
      }

      std::string encodedValue = value;
      if (key != "default" && key != "sys.attr.link") {
        XrdOucString in = value.c_str();
        XrdOucString v64;
        eos::common::SymKey::Base64(in, v64);
        encodedValue = v64.c_str();
      }

      if (sub == "set" &&
          XrdOucString(key.c_str()).endswith(".forced.placementpolicy")) {
        XrdOucString in = encodedValue.c_str();
        XrdOucString ouc_policy;
        eos::common::SymKey::DeBase64(in, ouc_policy);
        std::string policy = ouc_policy.c_str();
        if (policy != "scattered" &&
            policy.rfind("hybrid:", 0) != 0 &&
            policy.rfind("gathered:", 0) != 0) {
          fprintf(stderr, "Error: placement policy '%s' is invalid\n",
                  policy.c_str());
          global_retc = EINVAL;
          return 0;
        }
        if (policy != "scattered") {
          std::string targetgeotag = policy.substr(policy.find(':') + 1);
          std::string tmp_geotag = eos::common::SanitizeGeoTag(targetgeotag);
          if (tmp_geotag != targetgeotag) {
            fprintf(stderr, "%s\n", tmp_geotag.c_str());
            global_retc = EINVAL;
            return 0;
          }
        }
      }

      XrdOucString k = key.c_str();
      XrdOucString v = encodedValue.c_str();
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      in += "&mgm.subcmd=set&mgm.attr.key=";
      in += k;
      in += "&mgm.attr.value=";
      in += v;
      in += "&mgm.path=";
      in += p;
    } else if (sub == "get") {
      std::string key = arg;
      if (idx >= remaining.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string path = remaining[idx++];
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      in += "&mgm.subcmd=get&mgm.attr.key=";
      in += key.c_str();
      in += "&mgm.path=";
      in += p;
    } else if (sub == "fold") {
      std::string path = arg;
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      in += "&mgm.subcmd=fold&mgm.path=";
      in += p;
    } else if (sub == "rm" || sub == "unlink") {
      std::string key = arg;
      std::string path;
      if (sub == "unlink") {
        key = "sys.attr.link";
        path = arg;
      } else {
        if (idx >= remaining.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        path = remaining[idx++];
      }
      if (key.empty() || path.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      in += "&mgm.subcmd=rm&mgm.attr.key=";
      in += key.c_str();
      in += "&mgm.path=";
      in += p;
    }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    bool opt_r = false;
    bool opt_V = false;
    std::string subcmd;
    ConfigureAttrApp(app, opt_r, opt_V, subcmd);
    const std::string help = app.help();
    fprintf(stderr, "%s", help.c_str());
  }
};
} // namespace

void
RegisterAttrNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AttrCommand>());
}
