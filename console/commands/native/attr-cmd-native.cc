// ----------------------------------------------------------------------
// File: attr-native.cc
// ----------------------------------------------------------------------

#include "common/SymKeys.hh"
#include "common/Utils.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

namespace {
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
    if (args.empty() || wants_help(args[0].c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    size_t idx = 0;
    std::string optionStr;
    // Optional leading options (e.g. -r, -V, -rV)
    if (idx < args.size() && args[idx].rfind("-", 0) == 0) {
      const std::string& opt = args[idx++];
      for (size_t j = 1; j < opt.size(); ++j) {
        char c = opt[j];
        if (c == 'r' || c == 'V' || c == 'c') {
          if (optionStr.find(c) == std::string::npos) optionStr.push_back(c);
        } else {
          printHelp(); global_retc = EINVAL; return 0;
        }
      }
    }

    if (idx >= args.size()) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string sub = args[idx++];

    XrdOucString in = "mgm.cmd=attr&mgm.enc=b64";

    auto next = [&](std::string& out) -> bool {
      if (idx < args.size()) { out = args[idx++]; return true; }
      return false;
    };

    auto appendOption = [&](bool conditional) {
      std::string opt = optionStr;
      if (conditional && opt.find('c') == std::string::npos) opt.push_back('c');
      if (!opt.empty()) {
        in += "&mgm.option=";
        in += opt.c_str();
      }
    };

    auto send_set = [&](const std::string& key,
                        const std::string& value,
                        const std::string& path,
                        bool conditional) -> int {
      XrdOucString cmd = "mgm.cmd=attr&mgm.enc=b64";
      std::string opt = optionStr;
      if (conditional && opt.find('c') == std::string::npos) {
        opt.push_back('c');
      }
      if (!opt.empty()) {
        cmd += "&mgm.option=";
        cmd += opt.c_str();
      }
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

    if (sub == "ls") {
      std::string identifier;
      if (!next(identifier)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString path = PathIdentifier(identifier.c_str(), true).c_str();
      appendOption(false);
      in += "&mgm.subcmd=ls&mgm.path=";
      in += path;
    } else if (sub == "set" || sub == "link") {
      bool conditional = false;
      if (idx < args.size() && args[idx] == "-c") { conditional = true; ++idx; }
      std::string keyval, path;
      if (!next(keyval) || !next(path)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString k = keyval.c_str();
      XrdOucString v = "";
      int epos = k.find("=");
      if (sub == "link") {
        k = "sys.attr.link";
        v = keyval.c_str();
      } else if (epos != STR_NPOS) {
        v = k;
        v.erase(0, epos + 1);
        k.erase(epos);
        if (k != "default" && k != "sys.attr.link") {
          XrdOucString v64;
          eos::common::SymKey::Base64(v, v64);
          v = v64;
        }
      } else {
        printHelp(); global_retc = EINVAL; return 0;
      }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();

      if (sub == "set" && k == "default") {
        std::vector<std::pair<std::string, std::string>> defaults;
        if (v == "replica") {
          defaults = {
            {"sys.forced.blocksize", "4k"},
            {"sys.forced.checksum", "adler"},
            {"sys.forced.layout", "replica"},
            {"sys.forced.nstripes", "2"},
            {"sys.forced.space", "default"},
          };
        } else if (v == "raiddp") {
          defaults = {
            {"sys.forced.blocksize", "1M"},
            {"sys.forced.checksum", "adler"},
            {"sys.forced.layout", "raiddp"},
            {"sys.forced.nstripes", "6"},
            {"sys.forced.space", "default"},
            {"sys.forced.blockchecksum", "crc32c"},
          };
        } else if (v == "raid5") {
          defaults = {
            {"sys.forced.blocksize", "1M"},
            {"sys.forced.checksum", "adler"},
            {"sys.forced.layout", "raid5"},
            {"sys.forced.nstripes", "5"},
            {"sys.forced.space", "default"},
            {"sys.forced.blockchecksum", "crc32c"},
          };
        } else if (v == "raid6") {
          defaults = {
            {"sys.forced.blocksize", "1M"},
            {"sys.forced.checksum", "adler"},
            {"sys.forced.layout", "raid6"},
            {"sys.forced.nstripes", "6"},
            {"sys.forced.space", "default"},
            {"sys.forced.blockchecksum", "crc32c"},
          };
        } else if (v == "archive") {
          defaults = {
            {"sys.forced.blocksize", "1M"},
            {"sys.forced.checksum", "adler"},
            {"sys.forced.layout", "archive"},
            {"sys.forced.nstripes", "8"},
            {"sys.forced.space", "default"},
            {"sys.forced.blockchecksum", "crc32c"},
          };
        } else if (v == "qrain") {
          defaults = {
            {"sys.forced.blocksize", "1M"},
            {"sys.forced.checksum", "adler"},
            {"sys.forced.layout", "qrain"},
            {"sys.forced.nstripes", "12"},
            {"sys.forced.space", "default"},
            {"sys.forced.blockchecksum", "crc32c"},
          };
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

      if (sub == "set" && k.endswith(".forced.placementpolicy")) {
        XrdOucString ouc_policy;
        eos::common::SymKey::DeBase64(v, ouc_policy);
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

      appendOption(conditional);
      in += "&mgm.subcmd=set&mgm.attr.key=";
      in += k;
      in += "&mgm.attr.value=";
      in += v;
      in += "&mgm.path=";
      in += p;
    } else if (sub == "get") {
      std::string key, path;
      if (!next(key) || !next(path)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      appendOption(false);
      in += "&mgm.subcmd=get&mgm.attr.key=";
      in += key.c_str();
      in += "&mgm.path=";
      in += p;
    } else if (sub == "rm" || sub == "unlink") {
      std::string key, path;
      if (sub == "unlink") {
        if (!next(path)) { printHelp(); global_retc = EINVAL; return 0; }
        key = "sys.attr.link";
      } else {
        if (!next(key) || !next(path)) { printHelp(); global_retc = EINVAL; return 0; }
      }
      XrdOucString p = PathIdentifier(path.c_str(), true).c_str();
      appendOption(false);
      in += "&mgm.subcmd=rm&mgm.attr.key=";
      in += key.c_str();
      in += "&mgm.path=";
      in += p;
    } else if (sub == "fold") {
      std::string identifier;
      if (!next(identifier)) { printHelp(); global_retc = EINVAL; return 0; }
      XrdOucString p = PathIdentifier(identifier.c_str(), true).c_str();
      appendOption(false);
      in += "&mgm.subcmd=fold&mgm.path=";
      in += p;
    } else {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: attr [OPTIONS] ls|set|get|rm ...\n");
    fprintf(stderr,
            "'[eos] attr ..' provides the extended attribute interface for directories in EOS.\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "attr [-r] ls <identifier> :\n");
    fprintf(stderr,
            "                                                : list attributes of path\n");
    fprintf(stderr, " -r : list recursive on all directory children\n");
    fprintf(stderr, "attr [-r] set [-c] <key>=<value> <identifier> :\n");
    fprintf(stderr,
            "                                                : set attributes of path (-r : recursive) (-c : only if attribute does not exist already)\n");
    fprintf(stderr,
            "attr [-r] set default=replica|raiddp|raid5|raid6|archive|qrain <identifier> :\n");
    fprintf(stderr,
            "                                                : set attributes of path (-r recursive) to the EOS defaults for replicas, dual-parity-raid (4+2), raid-6 (4+2) or archive layouts (5+3).\n");
    fprintf(stderr, " -r : set recursive on all directory children\n");
    fprintf(stderr, "attr [-r] [-V] get <key> <identifier> :\n");
    fprintf(stderr,
            "                                                : get attributes of path (-r recursive)\n");
    fprintf(stderr, " -r : get recursive on all directory children\n");
    fprintf(stderr, " -V : only print the value\n");
    fprintf(stderr, "attr [-r] rm  <key> <identifier> :\n");
    fprintf(stderr,
            "                                                : delete attributes of path (-r recursive)\n\n");
    fprintf(stderr, " -r : delete recursive on all directory children\n");
    fprintf(stderr, "attr [-r] link <origin> <identifier> :\n");
    fprintf(stderr,
            "                                                : link attributes of <origin> under the attributes of <identifier> (-r recursive)\n\n");
    fprintf(stderr, " -r : apply recursive on all directory children\n");
    fprintf(stderr, "attr [-r] unlink <identifier> :\n");
    fprintf(stderr,
            "                                                : remove attribute link of <identifier> (-r recursive)\n\n");
    fprintf(stderr, " -r : apply recursive on all directory children\n");
    fprintf(stderr, "attr [-r] fold <identifier> :\n");
    fprintf(stderr,
            "                                                : fold attributes of <identifier> if an attribute link is defined (-r recursive)\n\n");
    fprintf(stderr,
            "                                                  all attributes which are identical to the origin-link attributes are removed locally\n");
    fprintf(stderr, " -r : apply recursive on all directory children\n\n");
    fprintf(stderr, "Remarks:\n");
    fprintf(stderr,
            "         <identifier> = <path>|fid:<fid-dec>|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>\n"
            "                        deprecated pid:<pid-dec>|pxid:<pid-hex>\n");
    fprintf(stderr,
            "         If <key> starts with 'sys.' you have to be member of the sudoers group to see these attributes or modify.\n");
  }
};
} // namespace

void
RegisterAttrNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AttrCommand>());
}
