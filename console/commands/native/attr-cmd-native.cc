// ----------------------------------------------------------------------
// File: attr-native.cc
// ----------------------------------------------------------------------

#include "common/SymKeys.hh"
#include "common/Utils.hh"
#include "console/CommandFramework.hh"
#include <XrdOuc/XrdOucString.hh>
#include <memory>
#include <sstream>
#include <string>
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
    std::string sub;
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

    if (idx < args.size() && args[idx].rfind("-", 0) == 0) {
      const std::string& opt = args[idx++];
      optionStr = opt.substr(1);
      if (idx >= args.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      sub = args[idx++];
      if (idx >= args.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      arg = args[idx++];
      if (sub == "set" && arg == "-c") {
        if (optionStr.find('c') == std::string::npos) {
          optionStr.push_back('c');
        }
        if (idx >= args.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        arg = args[idx++];
      }
    } else {
      sub = args[idx++];
      if (sub == "set") {
        if (idx >= args.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        arg = args[idx++];
        if (arg == "-c") {
          if (optionStr.find('c') == std::string::npos) {
            optionStr.push_back('c');
          }
          if (idx >= args.size()) {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
          arg = args[idx++];
        }
      } else {
        if (idx >= args.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        arg = args[idx++];
      }
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
        while (idx < args.size()) {
          value += " ";
          value += args[idx];
          if (!args[idx].empty() && args[idx].back() == '"') {
            ++idx;
            break;
          }
          ++idx;
        }
      }

      if (value.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }

      std::string path;
      if (sub == "link") {
        if (idx >= args.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        path = args[idx++];
      } else {
        if (idx >= args.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        path = args[idx++];
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
      if (idx >= args.size()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string path = args[idx++];
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
        if (idx >= args.size()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        path = args[idx++];
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
    fprintf(stderr,
            "'[eos] attr ..' provides the extended attribute interface for directories in EOS.\n");
    fprintf(stderr, "Usage: attr [OPTIONS] ls|set|get|rm ...\n\n");
    fprintf(stderr,
            "Options:\n"
            "  attr [-r] ls <identifier>\n"
            "      List attributes of path\n"
            "      -r : list recursive on all directory children\n"
            "  attr [-r] set [-c] <key>=<value> <identifier>\n"
            "      Set attributes of path (-r recursive, -c only if absent)\n"
            "  attr [-r] set default=replica|raiddp|raid5|raid6|archive|qrain <identifier>\n"
            "      Set EOS default layout attributes for the path\n"
            "  attr [-r] [-V] get <key> <identifier>\n"
            "      Get attributes of path (-r recursive, -V only print value)\n"
            "  attr [-r] rm <key> <identifier>\n"
            "      Delete attributes of path (-r recursive)\n"
            "  attr [-r] link <origin> <identifier>\n"
            "      Link attributes of <origin> under <identifier> (-r recursive)\n"
            "  attr [-r] unlink <identifier>\n"
            "      Remove attribute link of <identifier> (-r recursive)\n"
            "  attr [-r] fold <identifier>\n"
            "      Fold attributes of <identifier> when attr link is defined\n"
            "      (identical attributes are removed locally)\n"
            "\n"
            "Remarks:\n");
    fprintf(stderr,
            "         <identifier> = <path>|fid:<fid-dec>|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>\n"
            "                        deprecated pid:<pid-dec>|pxid:<pid-hex>\n");
    fprintf(stderr,
            "         If <key> starts with 'sys.' you have to be member of the sudoers group to see these attributes or modify.\n");
    fprintf(stderr, "\nAdministrator Variables:\n");
    fprintf(stderr,
            "         sys.forced.space=<space>              : enforces to use <space>    [configuration dependent]\n");
    fprintf(stderr,
            "         sys.forced.group=<group>              : enforces to use <group>, where <group> is the numerical index of <space>.<n>    [configuration dependent]\n");
    fprintf(stderr,
            "         sys.forced.layout=<layout>            : enforces to use <layout>   [<layout>=(plain,replica,raid5,raid6,archive,qrain)]\n");
    fprintf(stderr,
            "         sys.forced.checksum=<checksum>        : enforces to use file-level checksum <checksum>\n");
    fprintf(stderr,
            "                                              <checksum> = adler,crc32,crc32c,md5,sha\n");
    fprintf(stderr,
            "         sys.forced.blockchecksum=<checksum>   : enforces to use block-level checksum <checksum>\n");
    fprintf(stderr,
            "                                              <checksum> = adler,crc32,crc32c,md5,sha\n");
    fprintf(stderr,
            "         sys.forced.nstripes=<n>               : enforces to use <n> stripes[<n>= 1..16]\n");
    fprintf(stderr,
            "         sys.forced.blocksize=<w>              : enforces to use a blocksize of <w> - <w> can be 4k,64k,128k,256k or 1M \n");
    fprintf(stderr,
            "         sys.forced.placementpolicy=<policy>[:geotag] : enforces to use replica/stripe placement policy <policy> [<policy>={scattered|hybrid:<geotag>|gathered:<geotag>}]\n");
    fprintf(stderr,
            "         sys.forced.nouserplacementpolicy=1    : disables user defined replica/stripe placement policy\n");
    fprintf(stderr,
            "         sys.forced.nouserlayout=1             : disables the user settings with user.forced.<xxx>\n");
    fprintf(stderr,
            "         sys.forced.nofsselection=1            : disables user defined filesystem selection with environment variables for reads\n");
    fprintf(stderr,
            "         sys.forced.bookingsize=<bytes>        : set's the number of bytes which get for each new created replica\n");
    fprintf(stderr,
            "         sys.forced.minsize=<bytes>            : set's the minimum number of bytes a file to be stored must have\n");
    fprintf(stderr,
            "         sys.forced.maxsize=<bytes>            : set's the maximum number of bytes a file to be stored can have\n");
    fprintf(stderr,
            "         sys.forced.atomic=1                   : if present enforce atomic uploads e.g. files appear only when their upload is complete - during the upload they have the name <dirname>/.<basename>.<uuid>\n");
    fprintf(stderr,
            "         sys.forced.leasetime=86400            : allows to overwrite the eosxd client provided leasetime with a new value\n");
    fprintf(stderr,
            "         sys.forced.iotype=direct|sync|dsync|csync"
            "                                               : force the given iotype for that directory\n");
    fprintf(stderr,
            "         sys.mtime.propagation=1               : if present a change under this directory propagates an mtime change up to all parents until the attribute is not present anymore\n");
    fprintf(stderr,
            "         sys.allow.oc.sync=1                   : if present, OwnCloud clients can sync pointing to this subtree\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.lru.expire.empty=<age>            : delete empty directories older than <age>\n");
    fprintf(stderr,
            "         sys.lru.expire.match=[match1:<age1>,match2:<age2>..]\n");
    fprintf(stderr,
            "                                               : defines the rule that files with a given match will be removed if \n");
    fprintf(stderr,
            "                                                 they haven't been accessed longer than <age> ago. <age> is defined like 3600,3600s,60min,1h,1mo,1y...\n");
    fprintf(stderr,
            "         sys.lru.lowwatermark=<low>\n");
    fprintf(stderr,
            "         sys.lru.highwatermark=<high>        : if the watermark reaches more than <high> %%, files will be removed until the usage is reaching <low> %%.\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.lru.convert.match=[match1:<age1>,match2:<age2>,match3:<age3>:<<size3>,match4:<age4>:><size4>...]\n");
    fprintf(stderr,
            "                                                 defines the rule that files with a given match will be converted to the layouts defined by sys.conversion.<match> when their access time reaches <age>. Optionally a size limitation can be given e.g. '*:1w:>1G' as 1 week old and larger than 1G or '*:1d:<1k' as one day old and smaller than 1k \n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.stall.unavailable=<sec>           : stall clients for <sec> seconds if a needed file system is unavailable\n");
    fprintf(stderr,
            "         sys.redirect.enoent=<host[:port]>     : redirect clients opening non existing files to <host[:port]>\n");
    fprintf(stderr,
            "               => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eos/public \n\n");
    fprintf(stderr,
            "         sys.redirect.enonet=<host[:port]>     : redirect clients opening inaccessible files to <host[:port]>\n");
    fprintf(stderr,
            "               => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eos/public \n\n");
    fprintf(stderr,
            "         sys.recycle=....                      : define the recycle bin for that directory - WARNING: never modify this variables via 'attr' ... use the 'recycle' interface\n");
    fprintf(stderr,
            "         sys.recycle.keeptime=<seconds>        : define the time how long files stay in a recycle bin before final deletions takes place. This attribute has to defined on the recycle - WARNING: never modify this variables via 'attr' ... use the 'recycle' interface\n\n");
    fprintf(stderr,
            "         sys.recycle.keepratio=< 0 .. 1.0 >    : ratio of used/max quota for space and inodes in the recycle bin under which files are still kept in the recycle bin even if their lifetime has exceeded. If not defined pure lifetime policy will be applied \n\n");
    fprintf(stderr,
            "         sys.versioning=<n>                    : keep <n> versions of a file e.g. if you upload a file <n+10> times it will keep the last <n+1> versions\n");
    fprintf(stderr,
            "         sys.acl=<acllist>                     : set's an ACL which is honored for open,rm & rmdir operations\n");
    fprintf(stderr,
            "               => <acllist> = <rule1>,<rule2>...<ruleN> is a comma separated list of rules\n");
    fprintf(stderr,
            "               => z:{u:<uid|username>|g:<gid|groupname>|egroup:<name>:{Aarw[o]Xximc(!u)\n");
    fprintf(stderr,
            "               e.g.: <acllist=\"u:300:rw,g:z2:rwo:egroup:eos-dev:rwx,u:500:rwm!d:u:600:rwqc\"\n\n");
    fprintf(stderr, "               => user id 300 can read + write\n");
    fprintf(stderr,
            "               => group z2 can read + write-once (create new files but can't delete)\n");
    fprintf(stderr,
            "               => members of egroup 'eos-dev' can read & write & browse\n");
    fprintf(stderr,
            "               => user id 500 can read + write into and chmod(m), but cannot delete the directory itself(!d)!\n");
    fprintf(stderr,
            "               => user id 600 can read + write and administer the quota node(q) and can change the directory ownership in child directories(c)\n");
    fprintf(stderr,
            "              '+d' : this tag can be used to overwrite a group rule excluding deletion via '!d' for certain users\n");
    fprintf(stderr,
            "              '+u' : this tag can be used to overwrite a rul excluding updates via '!u'\n");
    fprintf(stderr,
            "              'c'  : this tag can be used to grant chown permissions\n");
    fprintf(stderr,
            "              'q'  : this tag can be used to grant quota administrator permissions\n");
    fprintf(stderr,
            "               e.g.: sys.acl='z:!d' => 'z' is a rule for every user besides root e.g. nobody can delete here'b\n");
    fprintf(stderr,
            "                     sys.acl='z:i' => directory becomes immutable\n");
    fprintf(stderr,
            "         sys.eval.useracl                      : enables the evaluation of user acls if key is defined\n");
    fprintf(stderr,
            "         sys.mask                              : masks all unix access permissions with a given mask .e.g sys.mask=775 disables writing to others\n");
    fprintf(stderr,
            "         sys.owner.auth=<owner-auth-list>      : set's additional owner on a directory - open/create + mkdir commands will use the owner id for operations if the client is part of the owner authentication list\n");
    fprintf(stderr,
            "         sys.owner.auth=*                      : every person with write permission will be mapped to the owner uid/gid pair of the parent directory and quota will be accounted on the owner uid/gid pair\n");
    fprintf(stderr,
            "               => <owner-auth-list> = <auth1>:<name1>,<auth2>:<name2  e.g. krb5:nobody,gsi:DN=...\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.attr.link=<directory>             : symbolic links for attributes - all attributes of <directory> are visible in this directory and overwritten/extended by the local attributes\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.http.index=<path>                 : show a static page as directory index instead of the dynamic one\n");
    fprintf(stderr,
            "               => <path> can be a relative or absolute file path!\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.accounting.*=<value>              : set accounting attributes with value on the proc directory (common values) or quota nodes which translate to JSON output in the accounting report command\n");
    fprintf(stderr,
            "               => You have to create such an attribute for each leaf value in the desired JSON.\n");
    fprintf(stderr,
            "               => JSON objects: create a new key with a new name after a '.', e.g. sys.accounting.storagecapacity.online.totalsize=x or sys.accounting.storagecapacity.online.usedsize=y to add a new key-value to this object\n");
    fprintf(stderr,
            "               => JSON arrays: place a continuous whole number from 0 to the attribute name, e.g. sys.accounting.accessmode.{0,1,2,...}\n");
    fprintf(stderr,
            "               => array of objects: you can combine the above two to achieve arbitrary JSON output, e.g. sys.accounting.storageendpoints.0.name, sys.accounting.storageendpoints.0.id and sys.accounting.storageendpoints.1.name ...\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "         sys.proc=<opaque command>             : run arbitrary command on accessing the file\n");
    fprintf(stderr,
            "               => <opaque command> command to execute in opaque format, e.g. mgm.cmd=accounting&mgm.subcmd=report&mgm.format=fuse\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "User Variables:\n");
    fprintf(stderr, "         user.forced.space=<space>              : s.a.\n");
    fprintf(stderr, "         user.forced.layout=<layout>            : s.a.\n");
    fprintf(stderr, "         user.forced.checksum=<checksum>        : s.a.\n");
    fprintf(stderr, "         user.forced.blockchecksum=<checksum>   : s.a.\n");
    fprintf(stderr, "         user.forced.nstripes=<n>               : s.a.\n");
    fprintf(stderr, "         user.forced.blocksize=<w>              : s.a.\n");
    fprintf(stderr,
            "         user.forced.placementpolicy=<policy>[:geotag] : s.a.\n");
    fprintf(stderr,
            "         user.forced.nouserplacementpolicy=1            : s.a.\n");
    fprintf(stderr, "         user.forced.nouserlayout=1             : s.a.\n");
    fprintf(stderr, "         user.forced.nofsselection=1            : s.a.\n");
    fprintf(stderr, "         user.forced.atomic=1                   : s.a.\n");
    fprintf(stderr, "         user.stall.unavailable=<sec>           : s.a.\n");
    fprintf(stderr, "         user.acl=<acllist>                     : s.a.\n");
    fprintf(stderr, "         user.versioning=<n>                    : s.a.\n");
    fprintf(stderr,
            "         user.tag=<tag>                         : Tag <tag> to group files for scheduling and flat file distribution. Use this tag to define datasets (if <tag> contains space use tag with quotes)\n");
    fprintf(stderr, "\n\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "...................\n");
    fprintf(stderr, "....... Layouts ...\n");
    fprintf(stderr, "...................\n");
    fprintf(stderr, "- set 2 replica as standard layout ...\n");
    fprintf(stderr,
            "     |eos> attr set default=replica /eos/instance/2-replica\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr, "- set RAID-6 4+2 as standard layout ...\n");
    fprintf(stderr, "     |eos> attr set default=raid6 /eos/instance/raid-6\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr, "- set ARCHIVE 5+3 as standard layout ...\n");
    fprintf(stderr, "     |eos> attr set default=archive /eos/instance/archive\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr, "- set QRAIN 8+4 as standard layout ...\n");
    fprintf(stderr, "     |eos> attr set default=qrain /eos/instance/qrain\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr,
            "- re-configure a layout for different number of stripes (e.g. 10) ...\n");
    fprintf(stderr,
            "     |eos> attr set sys.forced.nstripes=10 /eos/instance/archive\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "................\n");
    fprintf(stderr, "....... ACLs ...\n");
    fprintf(stderr, "................\n");
    fprintf(stderr,
            "- forbid deletion and updates for group xx in a directory ...\n");
    fprintf(stderr,
            "     |eos> attr set sys.acl=g:xx::!d!u /eos/instance/no-update-deletion\n");
    fprintf(stderr, "\n");
    fprintf(stderr, ".....................\n");
    fprintf(stderr, "....... LRU Cache ...\n");
    fprintf(stderr, ".....................\n");
    fprintf(stderr,
            "- configure a volume based LRU cache with a low/high watermark \n");
    fprintf(stderr,
            "  e.g. when the cache reaches the high watermark it cleans the oldest files until low-watermark is reached ...\n");
    fprintf(stderr,
            "     |eos> quota set -g 99 -v 1T /eos/instance/cache/                           # define project quota on the cache\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.lowwatermark=90  /eos/instance/cache/               \n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.highwatermark=95  /eos/instance/cache/               # define 90 as low and 95 as high watermark\n");
    fprintf(stderr, "\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr, "- configure clean-up of empty directories ...\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.expire.empty=\"1h\" /eos/dev/instance/empty/          # remove automatically empty directories if they are older than 1 hour\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr,
            "- configure a time based LRU cache with an expiration time ...\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.expire.match=\"*.root:1mo,*.tgz:1w\"  /eos/dev/instance/scratch/\n");
    fprintf(stderr,
            "                                                                                # files with suffix *.root get removed after a month, files with *.tgz after one week\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.expire.match=\"*:1d\" /eos/dev/instance/scratch/      # all files older than a day are automatically removed\n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr,
            "- configure automatic layout conversion if a file has reached a defined age ...\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.convert.match=\"*:1mo\" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.convert.match=\"*:1mo:>2G\" /eos/dev/instance/convert/# convert all files older than a month and larger than 2Gb to the layout defined next\n");
    fprintf(stderr,
            "     |eos> attr set sys.conversion.*=20640542 /eos/dev/instance/convert/          # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 \n");
    fprintf(stderr,
            "     |eos> attr set sys.conversion.*=20640542|gathered:site1::rack2 /eos/dev/instance/convert/ # same thing specifying a placement policy for the replicas/stripes \n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr,
            "- configure automatic layout conversion if a file has not been used during the last 6 month ...\n");
    fprintf(stderr,
            "     |eos> attr set sys.lru.convert.match=\"*:6mo\" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next\n");
    fprintf(stderr,
            "     |eos> attr set sys.conversion.*=20640542  /eos/dev/instance/convert/         # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 \n");
    fprintf(stderr,
            "     |eos> attr set sys.conversion.*=20640542|gathered:site1::rack2 /eos/dev/instance/convert/ # same thing specifying a placement policy for the replicas/stripes \n");
    fprintf(stderr,
            "--------------------------------------------------------------------------------\n");
    fprintf(stderr, ".......................\n");
    fprintf(stderr, "....... Recycle Bin ...\n");
    fprintf(stderr, ".......................\n");
    fprintf(stderr,
            "- configure a recycle bin with 1 week garbage collection and 100 TB space ...\n");
    fprintf(stderr,
            "     |eos> recycle config --lifetime 604800                                     # set the lifetime to 1 week\n");
    fprintf(stderr,
            "     |eos> recycle config --size 100T                                           # set the size of 100T\n");
    fprintf(stderr,
            "     |eos> recycle config --add-bin /eos/dev/instance/                          # add's the recycle bin to the subtree /eos/dev/instance\n");
    fprintf(stderr, ".......................\n");
    fprintf(stderr, ".... Atomic Uploads ...\n");
    fprintf(stderr, ".......................\n");
    fprintf(stderr,
            "     |eos> attr set sys.forced.atomic=1 /eos/dev/instance/atomic/\n");
    fprintf(stderr, ".......................\n");
    fprintf(stderr, ".... Attribute Link ...\n");
    fprintf(stderr, ".......................\n");
    fprintf(stderr,
            "     |eos> attr set sys.attr.link=/eos/dev/origin-attr/ /eos/dev/instance/attr-linked/\n");
  }
};
} // namespace

void
RegisterAttrNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<AttrCommand>());
}
