// ----------------------------------------------------------------------
// File: geosched-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "common/Utils.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class GeoschedCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "geosched";
  }
  const char*
  description() const override
  {
    return "Geographical scheduler control";
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
    eos::common::StringTokenizer subtokenizer(joined.c_str());
    subtokenizer.GetLine();
    XrdOucString cmd = subtokenizer.GetToken();
    std::set<std::string> supportedParam = {
        "skipSaturatedAccess",    "skipSaturatedDrnAccess",
        "skipSaturatedBlcAccess", "plctDlScorePenalty",
        "plctUlScorePenalty",     "accessDlScorePenalty",
        "accessUlScorePenalty",   "fillRatioLimit",
        "fillRatioCompTol",       "saturationThres",
        "timeFrameDurationMs",    "penaltyUpdateRate",
        "proxyCloseToFs"};
    XrdOucString in = "";
    if ((cmd != "show") && (cmd != "set") && (cmd != "updater") &&
        (cmd != "forcerefresh") && (cmd != "disabled") && (cmd != "access")) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    in = "mgm.cmd=geosched";
    if (cmd == "show") {
      XrdOucString subcmd = subtokenizer.GetToken();
      if (subcmd == "-c") {
        in += "&mgm.usecolors=1";
        subcmd = subtokenizer.GetToken();
      } else if (subcmd == "-m") {
        in += "&mgm.monitoring=1";
        subcmd = subtokenizer.GetToken();
      }
      if ((subcmd != "tree") && (subcmd != "snapshot") && (subcmd != "state") &&
          (subcmd != "param")) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (subcmd == "state") {
        in += "&mgm.subcmd=showstate";
        subcmd = subtokenizer.GetToken();
        if (subcmd == "-m") {
          in += "&mgm.monitoring=1";
        }
      }
      if (subcmd == "param") {
        in += "&mgm.subcmd=showparam";
      }
      if (subcmd == "tree") {
        in += "&mgm.subcmd=showtree";
        in += "&mgm.schedgroup=";
        XrdOucString group = subtokenizer.GetToken();
        if (group.length()) {
          in += group;
        }
      }
      if (subcmd == "snapshot") {
        in += "&mgm.subcmd=showsnapshot";
        in += "&mgm.schedgroup=";
        XrdOucString group = subtokenizer.GetToken();
        if (group.length()) {
          in += group;
        }
        in += "&mgm.optype=";
        XrdOucString optype = subtokenizer.GetToken();
        if (optype.length()) {
          in += optype;
        }
      }
    }
    if (cmd == "set") {
      XrdOucString parameter = subtokenizer.GetToken();
      if (!parameter.length()) {
        fprintf(stderr, "Error: parameter name is not provided\n");
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (supportedParam.find(parameter.c_str()) == supportedParam.end()) {
        fprintf(stderr, "Error: parameter %s not supported\n",
                parameter.c_str());
        return 0;
      }
      XrdOucString index = subtokenizer.GetToken();
      XrdOucString value = subtokenizer.GetToken();
      if (!index.length()) {
        fprintf(stderr, "Error: value is not provided\n");
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (!value.length()) {
        value = index;
        index = "-1";
      }
      double didx = 0.0;
      if (!sscanf(value.c_str(), "%lf", &didx)) {
        fprintf(stderr,
                "Error: parameter %s should have a numeric value, %s was "
                "provided\n",
                parameter.c_str(), value.c_str());
        return 0;
      }
      if (!XrdOucString(index.c_str()).isdigit()) {
        fprintf(stderr,
                "Error: index for parameter %s should have a numeric value, %s "
                "was provided\n",
                parameter.c_str(), index.c_str());
        return 0;
      }
      in += "&mgm.subcmd=set";
      in += "&mgm.param=";
      in += parameter.c_str();
      in += "&mgm.paramidx=";
      in += index.c_str();
      in += "&mgm.value=";
      in += value.c_str();
    }
    if (cmd == "updater") {
      XrdOucString subcmd = subtokenizer.GetToken();
      if (subcmd == "pause") {
        in += "&mgm.subcmd=updtpause";
      }
      if (subcmd == "resume") {
        in += "&mgm.subcmd=updtresume";
      }
    }
    if (cmd == "forcerefresh") {
      in += "&mgm.subcmd=forcerefresh";
    }
    if (cmd == "disabled") {
      XrdOucString subcmd = subtokenizer.GetToken();
      XrdOucString geotag, group, optype;
      if ((subcmd != "add") && (subcmd != "rm") && (subcmd != "show")) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      geotag = subtokenizer.GetToken();
      optype = subtokenizer.GetToken();
      group = subtokenizer.GetToken();
      if (!group.length() || !optype.length() || !geotag.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      std::string sgroup(group.c_str()), soptype(optype.c_str()),
          sgeotag(geotag.c_str());
      const char fbdChars[] = "&/,;%$#@!*";
      auto fbdMatch = sgroup.find_first_of(fbdChars);
      if (fbdMatch != std::string::npos && !(sgroup == "*")) {
        fprintf(stdout, "illegal character %c detected in group name %s\n",
                sgroup[fbdMatch], sgroup.c_str());
        return 0;
      }
      fbdMatch = soptype.find_first_of(fbdChars);
      if (fbdMatch != std::string::npos && !(soptype == "*")) {
        fprintf(stdout, "illegal character %c detected in optype %s\n",
                soptype[fbdMatch], soptype.c_str());
        return 0;
      }
      if (!(sgeotag == "*" && subcmd != "add")) {
        std::string tmp_geotag = eos::common::SanitizeGeoTag(sgeotag);
        if (tmp_geotag != sgeotag) {
          fprintf(stderr, "%s\n", tmp_geotag.c_str());
          return 0;
        }
      }
      in += ("&mgm.subcmd=disabled" + subcmd);
      if (geotag.length()) {
        in += ("&mgm.geotag=" + geotag);
      }
      in += ("&mgm.schedgroup=" + group);
      in += ("&mgm.optype=" + optype);
    }
    if (cmd == "access") {
      XrdOucString subcmd = subtokenizer.GetToken();
      XrdOucString geotag, geotag_list, optype;
      if ((subcmd != "setdirect") && (subcmd != "showdirect") &&
          (subcmd != "cleardirect") && (subcmd != "setproxygroup") &&
          (subcmd != "showproxygroup") && (subcmd != "clearproxygroup")) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      const char* token = 0;
      if ((token = subtokenizer.GetToken())) {
        geotag = token;
      }
      if ((token = subtokenizer.GetToken())) {
        geotag_list = token;
      }
      in += ("&mgm.subcmd=access" + subcmd);
      if (subcmd == "showdirect" || subcmd == "showproxygroup") {
        if (geotag.length()) {
          if (geotag != "-m" || geotag_list.length()) {
            printHelp();
            global_retc = EINVAL;
            return 0;
          } else {
            in += "&mgm.monitoring=1";
          }
        }
      } else {
        if (subcmd == "setdirect" || subcmd == "setproxygroup") {
          if (!geotag.length() || !geotag_list.length()) {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
          if (subcmd == "setdirect") {
            std::string tmp_list(geotag_list.c_str());
            auto geotags =
                eos::common::StringTokenizer::split<std::vector<std::string>>(
                    tmp_list, ',');
            tmp_list.clear();
            for (const auto& tag : geotags) {
              std::string tmp_tag = eos::common::SanitizeGeoTag(tag);
              if (tmp_tag != tag) {
                fprintf(stderr, "%s\n", tmp_tag.c_str());
                return 0;
              }
            }
          }
          in += ("&mgm.geotaglist=" + geotag_list);
        } else {
          if (!geotag.length() || geotag_list.length()) {
            printHelp();
            global_retc = EINVAL;
            return 0;
          }
        }
        std::string tmp_geotag = eos::common::SanitizeGeoTag(geotag.c_str());
        if (tmp_geotag != geotag.c_str()) {
          fprintf(stderr, "%s\n", tmp_geotag.c_str());
          return 0;
        }
        in += ("&mgm.geotag=" + geotag);
      }
    }
    if (subtokenizer.GetToken()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = output_result(client_command(in, true));
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: geosched show|set|updater|forcerefresh|disabled|access ...\n"
        "'[eos] geosched ..' Interact with the file geoscheduling engine in "
        "EOS.\n"
        "Options:\n"
        "       geosched show [-c|-m] tree [<scheduling group>]                "
        "    :  show scheduling trees\n"
        "                                                                      "
        "    :  if <scheduling group> is specified only the tree for this "
        "group is shown. If it's not all, the trees are shown.\n"
        "                                                                      "
        "    :  '-c' enables color display\n"
        "                                                                      "
        "    :  '-m' list in monitoring format\n"
        "       geosched show [-c|-m] snapshot [{<scheduling group>,*} "
        "[<optype>]] :  show snapshots of scheduling trees\n"
        "                                                                      "
        "    :  if <scheduling group> is specified only the snapshot(s) for "
        "this group is/are shown. If it's not all, the snapshots for all the "
        "groups are shown.\n"
        "                                                                      "
        "    :  if <optype> is specified only the snapshot for this operation "
        "is shown. If it's not, the snapshots for all the optypes are shown.\n"
        "                                                                      "
        "    :  <optype> can be one of the folowing "
        "plct,accsro,accsrw,accsdrain,plctdrain\n"
        "                                                                      "
        "    :  '-c' enables color display\n"
        "                                                                      "
        "    :  '-m' list in monitoring format\n"
        "       geosched show param                                            "
        "    :  show internal parameters\n"
        "       geosched show state [-m]                                       "
        "    :  show internal state\n"
        "                                                                      "
        "    :  '-m' list in monitoring format\n"
        "       geosched set <param name> [param index] <param value>          "
        "    :  set the value of an internal state parameter (all names can be "
        "listed with geosched show param)\n"
        "       geosched updater {pause|resume}                                "
        "    :  pause / resume the tree updater\n"
        "       geosched forcerefresh                                          "
        "    :  force a refresh of the trees/snapshots\n"
        "       geosched disabled add <geotag> {<optype>,*} {<scheduling "
        "subgroup>,*}      :  disable a branch of a subtree for the specified "
        "group and operation\n"
        "                                                                      "
        "            :  multiple branches can be disabled (by successive "
        "calls) as long as they have no intersection\n"
        "       geosched disabled rm {<geotag>,*} {<optype>,*} {<scheduling "
        "subgroup>,*}   :  re-enable a disabled branch for the specified group "
        "and operation\n"
        "                                                                      "
        "            :  when called with <geotag> *, the whole tree(s) are "
        "re-enabled, canceling all previous disabling\n"
        "       geosched disabled show {<geotag>,*} {<optype>,*} {<scheduling "
        "subgroup>,*} :  show list of disabled branches for the specified "
        "groups and operation\n"
        "       geosched access setdirect <geotag> <geotag_list>               "
        "    :  set a mapping between an accesser geotag and a set of target "
        "geotags\n"
        "                                                                      "
        "    :  these mappings specify which geotag can be accessed from which "
        "geotag without going through a firewall entrypoint\n"
        "                                                                      "
        "    :  geotag_list is of the form "
        "token1::token2,token3::token4::token5,...\n"
        "       geosched access showdirect [-m]                                "
        "    :  show mappings between accesser geotags and target geotags\n"
        "                                                                      "
        "    :  '-m' list in monitoring format\n"
        "       geosched access cleardirect {<geotag>|all}                     "
        "    :  clear a mapping between an accesser geotag and a set of target "
        "geotags\n"
        "       geosched access setproxygroup <geotag> <proxygroup>            "
        "    :  set the proxygroup acting as a firewall entrypoint for the "
        "given subtree\n"
        "                                                                      "
        "    :  if a client accesses a file from a geotag which does not have "
        "direct access to the subtree the replica is,\n"
        "                                                                      "
        "    :  it will be scheduled to access through a node from the given "
        "proxygroup\n"
        "       geosched access showproxygroup [-m]                            "
        "    :  show mappings between accesser geotags and target geotags\n"
        "                                                                      "
        "    :  '-m' list in monitoring format\n"
        "       geosched access clearproxygroup {<geotag>|all}                 "
        "    :  clear a mapping between an accesser geotag and a set of target "
        "geotags\n"
        "\nNote:\n"
        "       Make sure that geotags contain only alphanumeric segments "
        "which are no longer than 8 characters, in <tag1>::<tag2>::...::<tagN> "
        "format.\n");
  }
};
} // namespace

void
RegisterGeoschedNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<GeoschedCommand>());
}
