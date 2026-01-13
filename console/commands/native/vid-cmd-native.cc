// ----------------------------------------------------------------------
// File: vid-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "common/Utils.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class VidCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "vid";
  }
  const char*
  description() const override
  {
    return "VID tools";
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
    eos::common::StringTokenizer tok(joined.c_str());
    tok.GetLine();
    XrdOucString sub = tok.GetTokenUnquoted();
    if (!sub.length() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    if (sub == "ls") {
      XrdOucString in = "mgm.cmd=vid&mgm.subcmd=ls";
      XrdOucString t;
      for (;;) {
        t = tok.GetTokenUnquoted();
        if (!t.length())
          break;
        if (t.beginswith("-")) {
          t.erase(0, 1);
          in += "&mgm.vid.option=";
          in += t;
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
      }
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      return 0;
    }

    if (sub == "set") {
      XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set";
      XrdOucString key = tok.GetTokenUnquoted();
      if (!key.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (key == "geotag") {
        XrdOucString match = tok.GetTokenUnquoted();
        if (!match.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        XrdOucString target = tok.GetTokenUnquoted();
        if (!target.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        std::string geotag = eos::common::SanitizeGeoTag(target.c_str());
        if (geotag != target.c_str()) {
          fprintf(stderr, "%s\n", geotag.c_str());
          return 0;
        }
        in += "&mgm.vid.cmd=geotag&mgm.vid.key=geotag:";
        in += match;
        in += "&mgm.vid.geotag=";
        in += target;
        global_retc =
            ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
        return 0;
      }
      if (key == "membership") {
        XrdOucString uid = tok.GetTokenUnquoted();
        if (!uid.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        XrdOucString type = tok.GetTokenUnquoted();
        if (!type.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        if (type == "-uids") {
          XrdOucString list = tok.GetTokenUnquoted();
          in += "&mgm.vid.cmd=membership&mgm.vid.source.uid=";
          in += uid;
          in += "&mgm.vid.key=";
          in += (XrdOucString)(uid + ":uids");
          in += "&mgm.vid.target.uid=";
          in += list;
        } else if (type == "-gids") {
          XrdOucString list = tok.GetTokenUnquoted();
          in += "&mgm.vid.cmd=membership&mgm.vid.source.uid=";
          in += uid;
          in += "&mgm.vid.key=";
          in += (XrdOucString)(uid + ":gids");
          in += "&mgm.vid.target.gid=";
          in += list;
        } else if (type == "+sudo") {
          in += "&mgm.vid.cmd=membership&mgm.vid.key=";
          in += (XrdOucString)(uid + ":root");
          in += "&mgm.vid.target.sudo=true";
        } else if (type == "-sudo") {
          in += "&mgm.vid.cmd=membership&mgm.vid.key=";
          in += (XrdOucString)(uid + ":root");
          in += "&mgm.vid.target.sudo=false";
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        global_retc =
            ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
        return 0;
      }
      if (key == "map") {
        XrdOucString type = tok.GetTokenUnquoted();
        if (!type.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
        bool hastype = false;
        auto add_auth = [&](const char* a) {
          in += "&mgm.vid.auth=";
          in += a;
          hastype = true;
        };
        if (type == "-krb5")
          add_auth("krb5");
        if (type == "-gsi")
          add_auth("gsi");
        if (type == "-https")
          add_auth("https");
        if (type == "-sss")
          add_auth("sss");
        if (type == "-unix")
          add_auth("unix");
        if (type == "-tident")
          add_auth("tident");
        if (type == "-voms")
          add_auth("voms");
        if (type == "-grpc")
          add_auth("grpc");
        if (type == "-oauth2")
          add_auth("oauth2");
        if (!hastype) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        XrdOucString pattern = tok.GetTokenUnquoted();
        if (!pattern.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        in += "&mgm.vid.pattern=";
        in += pattern;
        XrdOucString vid = tok.GetTokenUnquoted();
        if (!vid.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        if (vid.beginswith("vuid:")) {
          vid.replace("vuid:", "");
          in += "&mgm.vid.uid=";
          in += vid;
          XrdOucString vg = tok.GetTokenUnquoted();
          if (vg.length() && vg.beginswith("vgid:")) {
            vg.replace("vgid:", "");
            in += "&mgm.vid.gid=";
            in += vg;
          }
        } else if (vid.beginswith("vgid:")) {
          vid.replace("vgid:", "");
          in += "&mgm.vid.gid=";
          in += vid;
        } else {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        in += "&mgm.vid.key=<key>";
        global_retc =
            ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
        return 0;
      }
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    if (sub == "enable" || sub == "disable") {
      XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
      XrdOucString disableu =
          "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
      XrdOucString disableg =
          "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
      XrdOucString type = tok.GetTokenUnquoted();
      if (!type.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      auto set_default = [&](const char* a) {
        in += "&mgm.vid.auth=";
        in += a;
        disableu += a;
        disableu += ":\"<pwd>\":uid";
        disableg += a;
        disableg += ":\"<pwd>\":gid";
      };
      if (type == "krb5")
        set_default("krb5");
      else if (type == "sss")
        set_default("sss");
      else if (type == "gsi")
        set_default("gsi");
      else if (type == "https")
        set_default("https");
      else if (type == "unix")
        set_default("unix");
      else if (type == "grpc")
        set_default("grpc");
      else if (type == "oauth2")
        set_default("oauth2");
      else if (type == "tident")
        set_default("tident");
      else if (type == "ztn")
        set_default("ztn");
      else {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.vid.pattern=<pwd>";
      if (type != "unix") {
        in += "&mgm.vid.uid=0&mgm.vid.gid=0";
      } else {
        in += "&mgm.vid.uid=99&mgm.vid.gid=99";
      }
      in += "&mgm.vid.key=<key>";
      if (sub == "enable") {
        global_retc =
            ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      } else {
        global_retc =
            ctx.outputResult(ctx.clientCommand(disableu, true, nullptr), true);
        global_retc |=
            ctx.outputResult(ctx.clientCommand(disableg, true, nullptr), true);
      }
      return 0;
    }

    if (sub == "publicaccesslevel") {
      XrdOucString in =
          "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=publicaccesslevel";
      XrdOucString level = tok.GetTokenUnquoted();
      if (!level.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.vid.key=publicaccesslevel&mgm.vid.level=";
      in += level;
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      return 0;
    }

    if (sub == "tokensudo") {
      XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=tokensudo";
      XrdOucString lvl = tok.GetTokenUnquoted();
      if (!lvl.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.vid.key=tokensudo&mgm.vid.tokensudo=";
      in += lvl;
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      return 0;
    }

    if (sub == "add" || sub == "remove") {
      XrdOucString gw = tok.GetTokenUnquoted();
      if (gw != "gateway") {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString host = tok.GetTokenUnquoted();
      if (!host.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      XrdOucString protocol = tok.GetTokenUnquoted();
      if (!protocol.length())
        protocol = "*";
      XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
      XrdOucString disableu =
          "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
      XrdOucString disableg =
          "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
      in += "&mgm.vid.auth=tident&mgm.vid.pattern=\"";
      in += protocol;
      in += "@";
      in += host;
      in += "\"&mgm.vid.uid=0&mgm.vid.gid=0&mgm.vid.key=<key>";
      disableu += "tident:\"";
      disableu += protocol;
      disableu += "@";
      disableu += host;
      disableu += "\":uid";
      disableg += "tident:\"";
      disableg += protocol;
      disableg += "@";
      disableg += host;
      disableg += "\":gid";
      if (sub == "add")
        global_retc =
            ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      else {
        global_retc =
            ctx.outputResult(ctx.clientCommand(disableu, true, nullptr), true);
        global_retc |=
            ctx.outputResult(ctx.clientCommand(disableg, true, nullptr), true);
      }
      return 0;
    }

    if (sub == "rm") {
      XrdOucString key = tok.GetTokenUnquoted();
      if (!key.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (key == "membership") {
        key = tok.GetTokenUnquoted();
        key.insert("vid:", 0);
        XrdOucString key1 = key;
        XrdOucString key2 = key;
        key1 += ":uids";
        key2 += ":gids";
        XrdOucString in1 = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.key=";
        in1 += key1;
        XrdOucString in2 = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.key=";
        in2 += key2;
        global_retc =
            ctx.outputResult(ctx.clientCommand(in1, true, nullptr), true);
        global_retc |=
            ctx.outputResult(ctx.clientCommand(in2, true, nullptr), true);
        return 0;
      }
      XrdOucString in = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.key=";
      in += key;
      global_retc =
          ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
      return 0;
    }

    printHelp();
    global_retc = EINVAL;
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stdout,
        "Usage: vid ls [-u] [-g] [-s] [-U] [-G] [-g] [-a] [-l] [-n] : list "
        "configured policies\n"
        "                                        -u : show only user role "
        "mappings\n"
        "                                        -g : show only group role "
        "mappings\n"
        "                                        -s : show list of sudoers\n"
        "                                        -U : show user alias mapping\n"
        "                                        -G : show group alias "
        "mapping\n"
        "                                        -y : show configured "
        "gateways\n"
        "                                        -a : show authentication\n"
        "                                        -N : show maximum anonymous "
        "(nobody) access level deepness - the tree deepness where "
        "unauthenticated access is possible (default is 1024)\n"
        "                                        -l : show geo location "
        "mapping\n"
        "                                        -n : show numerical ids "
        "instead of user/group names\n"
        "\n"
        "       vid set membership <uid> -uids [<uid1>,<uid2>,...]\n"
        "       vid set membership <uid> -gids [<gid1>,<gid2>,...]\n"
        "       vid rm membership <uid>             : delete the membership "
        "entries for <uid>.\n"
        "       vid set membership <uid> [+|-]sudo \n"
        "       vid set map "
        "-krb5|-gsi|-https|-sss|-unix|-tident|-voms|-grpc|-oauth2 <pattern> "
        "[vuid:<uid>] [vgid:<gid>] \n"
        "           -voms <pattern>  : <pattern> is <group>:<role> e.g. to map "
        "VOMS attribute /dteam/cern/Role=NULL/Capability=NULL one should "
        "define <pattern>=/dteam/cern: \n"
        "           -sss key:<key>   : <key> has to be defined on client side "
        "via 'export XrdSecsssENDORSEMENT=<key>'\n"
        "           -grpc key:<key>  : <key> has to be added to the relevant "
        "GRPC request in the field 'authkey'\n"
        "           -https key:<key> : <key> has to be added to the relevant "
        "HTTP(S) request as a header 'x-gateway-authorization'\n"
        "           -oauth2 key:<oauth-resource> : <oauth-resource> describes "
        "the OAUTH resource endpoint to translate OAUTH tokens to user "
        "identities\n\n"
        "       vid set geotag <IP-prefix> <geotag>  : add to all IP's "
        "matching the prefix <prefix> the geo location tag <geotag>\n"
        "                                              N.B. specify the "
        "default assumption via 'vid set geotag default <default-tag>'\n"
        "       vid rm <key>                         : remove configured vid "
        "with name key - hint: use config dump to see the key names of vid "
        "rules\n"
        "\n"
        "       vid enable|disable krb5|gsi|sss|unix|https|grpc|oauth2|ztn\n"
        "                                            : enable/disables the "
        "default mapping via password or external database\n"
        "\n"
        "       vid add|remove gateway <hostname> "
        "[krb5|gsi|sss|unix|https|grpc]\n"
        "                                            : adds/removes a host as "
        "a (fuse) gateway with 'su' priviledges\n"
        "                                              [<prot>] restricts the "
        "gateway role change to the specified authentication method\n"
        "       vid publicaccesslevel <level>\n"
        "                                           : sets the deepest "
        "directory level where anonymous access (nobody) is possible\n"
        "       vid tokensudo 0|1|2|3\n"
        "                                           : configure sudo policy "
        "when tokens are used\n"
        "                                             0 : always allow token "
        "sudo (setting uid/gid from token) [default if not set]\n"
        "                                             1 : allow token sudo if "
        "transport is encrypted\n"
        "                                             2 : allow token sudo for "
        "strong authentication (not unix!)\n"
        "                                             3 : never allow token "
        "sudo\n");
  }
};
} // namespace

void
RegisterVidNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<VidCommand>());
}
