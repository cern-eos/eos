// ----------------------------------------------------------------------
// File: geosched-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "common/Utils.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeGeoschedHelp()
{
  return "Usage: geosched show|set|updater|forcerefresh|disabled|access ...\n\n"
         "'[eos] geosched ..' Interact with the file geoscheduling engine in "
         "EOS.\n\n"
         "Subcommands:\n"
         "  show [-c|-m] tree [<scheduling group>]     show scheduling trees\n"
         "  show [-c|-m] snapshot [<group>] [<optype>] show snapshots\n"
         "  show param                                show internal parameters\n"
         "  show state [-m]                           show internal state\n"
         "  set <param> [index] <value>                set parameter value\n"
         "  updater pause|resume                       pause/resume tree updater\n"
         "  forcerefresh                               force refresh\n"
         "  disabled add|rm|show <geotag> <optype> <group>\n"
         "  access setdirect|showdirect|cleardirect|setproxygroup|showproxygroup|clearproxygroup ...\n\n"
         "Options:\n"
         "  -c  enable color display\n"
         "  -m  list in monitoring format\n\n"
         "Note: Geotags must be alphanumeric segments, max 8 chars, format "
         "<tag1>::<tag2>::...::<tagN>\n";
}

void ConfigureGeoschedApp(CLI::App& app, std::string& subcmd)
{
  app.name("geosched");
  app.description("Geographical scheduler control");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeGeoschedHelp();
      }));
  app.add_option("subcmd", subcmd,
                 "show|set|updater|forcerefresh|disabled|access")
      ->required();
}

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
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    std::string subcmd;
    ConfigureGeoschedApp(app, subcmd);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
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
    fprintf(stderr, "%s", MakeGeoschedHelp().c_str());
  }
};
} // namespace

void
RegisterGeoschedNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<GeoschedCommand>());
}
