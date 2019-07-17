//------------------------------------------------------------------------------
// File: com_space_config.cc
// Author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your token) any later version.                                   *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/commands/ICmdHelper.hh"

#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"


void com_config_help();

//------------------------------------------------------------------------------
//! Class ConfigHelper
//------------------------------------------------------------------------------
class ConfigHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConfigHelper()
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConfigHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool ConfigHelper::ParseCommand(const char* arg)
{
  eos::console::SpaceProto* space = mReq.mutable_space();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "ls") {
    eos::console::SpaceProto_LsProto* ls = space->mutable_ls();

    while (tokenizer.NextToken(token)) {
      if (token == "-s") {
        mIsSilent = true;
      } else if (token == "-g") {
        if (!tokenizer.NextToken(token) || !eos::common::StringTokenizer::IsUnsignedNumber(token)) {
          std::cerr << "error: geodepth was not provided or it does not have "
                    << "the correct value: geodepth should be a positive "
                    << "integer" << std::endl;
          return false;
        }
        ls->set_outdepth(std::stoi(token));
      } else if (token == "-m") {
        mHighlight = false;
        ls->set_outformat(eos::console::SpaceProto_LsProto::MONITORING);
      } else if (token == "-l") {
        ls->set_outformat(eos::console::SpaceProto_LsProto::LISTING);
      } else if (token == "--io") {
        ls->set_outformat(eos::console::SpaceProto_LsProto::IO);
      } else if (token == "--fsck") {
        ls->set_outformat(eos::console::SpaceProto_LsProto::FSCK);
      } else if ((token.find('-') != 0)) { // does not begin with "-"
        ls->set_selection(token);
      } else {
        return false;
      }
    }

  }
  else if (token == "tracker") {
    eos::console::SpaceProto_TrackerProto* tracker = space->mutable_tracker();
    tracker->set_mgmspace("default");

  }
  else if (token == "inspector") {
    eos::console::SpaceProto_InspectorProto *inspector = space->mutable_inspector();
    inspector->set_mgmspace("default");

    std::string options;

    while (tokenizer.NextToken(token)) {
      if (token == "-c" || token == "--current") {
        options += "c";
      } else if (token == "-l" || token == "--last") {
        options += "l";
      } else if (token == "-m") {
        options += "m";
      } else if (token == "-p") {
        options += "p";
      } else if (token == "-e") {
        options += "e";
      } else {
        return false;
      }
    }
    inspector->set_options(options);

  }
  else if (token == "reset") {

    if (!tokenizer.NextToken(token)) return false;
    eos::console::SpaceProto_ResetProto *reset = space->mutable_reset();
    reset->set_mgmspace(token);

    while (tokenizer.NextToken(token)) {
      if (token == "--egroup") {
        reset->set_option(eos::console::SpaceProto_ResetProto::EGROUP);
      } else if (token == "--mapping") {
        reset->set_option(eos::console::SpaceProto_ResetProto::MAPPING);
      } else if (token == "--drain") {
        reset->set_option(eos::console::SpaceProto_ResetProto::DRAIN);
      } else if (token == "--scheduledrain") {
        reset->set_option(eos::console::SpaceProto_ResetProto::SCHEDULEDRAIN);
      } else if (token == "--schedulebalance") {
        reset->set_option(eos::console::SpaceProto_ResetProto::SCHEDULEBALANCE);
      } else if (token == "--ns") {
        reset->set_option(eos::console::SpaceProto_ResetProto::NS);
      } else if (token == "--nsfilesystemview") {
        reset->set_option(eos::console::SpaceProto_ResetProto::NSFILESISTEMVIEW);
      } else if (token == "--nsfilemap") {
        reset->set_option(eos::console::SpaceProto_ResetProto::NSFILEMAP);
      } else if (token == "--nsdirectorymap") {
        reset->set_option(eos::console::SpaceProto_ResetProto::NSDIRECTORYMAP);
      } else {
        return false;
      }
    }

  }
  else if (token == "define") {

    if (!tokenizer.NextToken(token)) return false;

    eos::console::SpaceProto_DefineProto *define = space->mutable_define();
    define->set_mgmspace(token);

    if (!tokenizer.NextToken(token)) {
      define->set_groupsize(0);
      define->set_groupmod(24);
    } else {
      define->set_groupsize(std::stoi(token));
      if (!tokenizer.NextToken(token)) {
        define->set_groupmod(24);
      } else {
        define->set_groupsize(std::stoi(token));
      }
    }

  }
  else if (token == "set") {
    if (!tokenizer.NextToken(token)) return false;

    eos::console::SpaceProto_SetProto *set = space->mutable_set();
    set->set_mgmspace(token);

    if (!tokenizer.NextToken(token)) return false;

    if (token == "on") {
      set->set_state_switch(true);
    } else if (token == "off") {
      set->set_state_switch(false);
    } else {
      return false;
    }

  }
  else if (token == "rm") {

    if (!tokenizer.NextToken(token)) return false;
    eos::console::SpaceProto_RmProto *rm = space->mutable_rm();
    rm->set_mgmspace(token);

  }
  else if (token == "status") {
    if (!tokenizer.NextToken(token)) return false;

    eos::console::SpaceProto_StatusProto *status = space->mutable_status();
    status->set_mgmspace(token);

    if (tokenizer.NextToken(token)) {
      if (token == "m") {
        status->set_outformat_m(true);
      } else {
        return false;
      }
    }

    std::string contents = eos::common::StringConversion::StringFromShellCmd("cat /var/eos/md/stacktrace 2> /dev/null");


  }
  else if (token == "node-set") {

    if (!tokenizer.NextToken(token)) return false;
    eos::console::SpaceProto_NodeSetProto* nodeset = space->mutable_nodeset();
    nodeset->set_mgmspace(token);

    if (!tokenizer.NextToken(token)) return false;
    nodeset->set_nodeset_key(token);

    if (!tokenizer.NextToken(token)) return false;


    if (token.find('/') == 0) { // if begins with "/"
      std::ifstream ifs(token, std::ios::in | std::ios::binary);
      if (!ifs) {
        std::cerr << "error: unable to read " << token << " - errno=" << errno << '\n';
        return false;
      }

      std::string val = std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
      if (val.length() > 512) {
        std::cerr << "error: the file contents exceeds 0.5 kB - configure a file hosted on the MGM using file:<mgm-path>\n";
        return false;
      }

      // store the value b64 encoded
      XrdOucString val64;
      eos::common::SymKey::Base64Encode((char*) val.c_str(), val.length(), val64);
      while (val64.replace("=", ":")) {}

      nodeset->set_nodeset_value( std::string (("base64:"+val64).c_str()));


    } else {
      nodeset->set_nodeset_value(token);
    }

  }
  else if (token == "node-get") {
    if (!tokenizer.NextToken(token)) return false;

    eos::console::SpaceProto_NodeGetProto *nodeget = space->mutable_nodeget();
    nodeget->set_mgmspace(token);

    if (!tokenizer.NextToken(token)) return false;

    nodeget->set_nodeget_key(token);

  }
  else if (token == "quota") {
    if (!tokenizer.NextToken(token)) return false;

    eos::console::SpaceProto_QuotaProto *quota = space->mutable_quota();
    quota->set_mgmspace(token);

    if (!tokenizer.NextToken(token)) return false;

    if (token == "on") {
      quota->set_quota_switch(true);
    } else if (token == "off") {
      quota->set_quota_switch(false);
    } else {
      return false;
    }


  }
  else if (token == "config") {

    if (!tokenizer.NextToken(token)) return false;

    eos::console::SpaceProto_ConfigProto* config = space->mutable_config();
    config->set_mgmspace_name(token);

    if (!tokenizer.NextToken(token)) return false;
    std::string::size_type pos = token.find('=');
    if (pos != std::string::npos && count(token.begin(), token.end(), '=') == 1 ) { // contains 1 and only 1 '='. It expects a token like <key>=<value>
      config->set_mgmspace_key(token.substr(0, pos));
      config->set_mgmspace_value(token.substr(pos+1, token.length()-1));
    } else {
      return false;
    }

  }
  else { // no proper subcommand
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Config command entry point
//------------------------------------------------------------------------------
int com_protoconfig(char* arg)
{
  if (wants_help(arg)) {
    com_config_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  ConfigHelper config;

  if (!config.ParseCommand(arg)) {
    com_config_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = config.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_config_help()
{
  std::ostringstream oss;
  oss
      << " usage:\n"

      << "config changelog|dump|export|load|ls|reset|save [OPTIONS]"
      << std::endl
      << "'[eos] config' provides the configuration interface to EOS." << std::endl
      << std::endl
      << "Subcommands:" << std::endl
      << "config changelog [-#lines]" << std::endl
      << "       show the last <#> lines from the changelog - default is 10" << std::endl
      << std::endl
      << "config dump [-cfgpqmsv] [<name>]" << std::endl
      << "       dump configuration with name <name> or current one by default" << std::endl
      << "       -c|--comments : " << "dump only comment config" << std::endl
      << "       -f|--fs       : " << "dump only file system config" << std::endl
      << "       -g|--global   : " << "dump only global config" << std::endl
      << "       -p|--policy   : " << "dump only policy config" << std::endl
      << "       -q|--quota    : " << "dump only quota config" << std::endl
      << "       -m|--map      : " << "dump only mapping config" << std::endl
      << "       -r|--route    : " << "dump only routing config" << std::endl
      << "       -s|--geosched : " << "dump only geosched config" << std::endl
      << "       -v|--vid      : " << "dump only virtual id config" << std::endl
      << std::endl
      << "config export [-f] [<name>]" << std::endl
      << "       export a configuration stored on file to QuarkDB - you need to specify the full path" << std::endl
      << "       -f : overwrite existing config name and create a timestamped backup" << std::endl
      << std::endl
      << "config load <name>"  << std::endl
      << "       load config (optionally with name)" << std::endl
      << std::endl
      << "config ls [-b|--backup]" << std::endl
      << "       list existing configurations" << std::endl
      << "       --backup|-b : show also backup & autosave files" << std::endl
      << std::endl
      << "config reset" << std::endl
      << "       reset all configuration to empty state" << std::endl
      << std::endl
      << "config save [-f] [<name>] [-c|--comment \"<comment>\"]" << std::endl
      << "       save config (optionally under name)" << std::endl
      << "       -f : overwrite existing config name and create a timestamped backup" << std::endl
      << "            If no name is specified the current config file is overwritten." << std::endl
      << "       -c : add a comment entry to the config" << std::endl
      << "            Extended option will also add the entry to the logbook." << std::endl;
  std::cerr << oss.str() << std::endl;

}
