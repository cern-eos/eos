//------------------------------------------------------------------------------
// @file: com_proto_config.cc
// @author: Fabio Luchetti - CERN
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

extern int com_config(char*);
void com_config_help();

//------------------------------------------------------------------------------
//! Class ConfigHelper
//------------------------------------------------------------------------------
class ConfigHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  ConfigHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConfigHelper() override = default;

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
  eos::console::ConfigProto* config = mReq.mutable_config();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "ls") {
    eos::console::ConfigProto_LsProto* ls = config->mutable_ls();

    if (tokenizer.NextToken(token)) {
      if (token == "--backup" || token == "-b") {
        ls->set_showbackup(true);
      } else {
        return false;
      }
    }
  } else if (token == "dump") {
    eos::console::ConfigProto_DumpProto* dump = config->mutable_dump();

    if (tokenizer.NextToken(token)) {
      dump->set_file(token);
    }
  } else if (token == "reset") {
    if (tokenizer.NextToken(token)) {
      return false;  // no need for more arguments
    }

    config->set_reset(true);
  } else if (token == "export") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::ConfigProto_ExportProto* exp = config->mutable_exp();

    // either "<file> or <file> -f
    if (token.find('-') != 0) { // does not begins with '-'
      exp->set_file(token);

      if (tokenizer.NextToken(token)) {
        if (token == "-f") {
          exp->set_force(true);
        } else {
          return false;
        }
      }
    } else {
      return false;
    }
  } else if (token == "save") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::ConfigProto_SaveProto* save = config->mutable_save();

    if (token.find('-') != 0) {
      save->set_file(token);
    } else {
      return false;
    }

    while (tokenizer.NextToken(token)) {
      if (token == "-c" ||
          token == "--comment") { // put the comment in the mReq object
        std::string sline = arg;

        if (token == "-c") {
          // have to replace "-c" with "--comment" in sline
          size_t pos = sline.find("-c");
          sline.replace(pos, std::string("-c").length(), "--comment");
          parse_comment(sline.c_str(), token);
        } else if (token == "--comment") {
          parse_comment(sline.c_str(), token);
        }

        mReq.set_comment(token);
        tokenizer.NextToken(token); // skip comment text
      } else if (token == "-f") {
        save->set_force(true);
      } else {
        return false;
      }
    }
  } else if (token == "load") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::ConfigProto_LoadProto* load = config->mutable_load();
    load->set_file(token);
  } else if (token == "changelog") {
    eos::console::ConfigProto_ChangelogProto* changelog =
      config->mutable_changelog();

    if (tokenizer.NextToken(token)) {
      if (token.find('-') == 0) {
        token.erase(0);  // remove first char to allow both -100 and 100
      }

      try {
        changelog->set_lines(std::stoi(token));
      } catch (const std::exception& e) {
        std::cerr << "error: argument needs to be numeric" << std::endl;
        return false;
      }
    } else {
      changelog->set_lines(10);
    }
  } else { // no proper subcommand
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

  ConfigHelper config(gGlobalOpts);

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
      << "config changelog|dump|export|load|ls|reset|save [OPTIONS]\n"
      << "'[eos] config' provides the configuration interface to EOS.\n"
      << std::endl
      << "Subcommands:\n"
      << "config changelog [-#lines] : show the last #lines from the changelog - default is 10\n"
      << std::endl
      << "config dump [<name>] : dump configuration with name <name> or current one by default\n"
      << std::endl
      << "config export <name> [-f] : export a configuration stored on file to QuarkDB (you need to specify the full path!)\n"
      << "\t -f : overwrite existing config name and create a timestamped backup\n"
      << std::endl
      << "config load <name> : load <name> config\n"
      << std::endl
      << "config ls [-b|--backup] : list existing configurations\n"
      << "\t -b : show also backup & autosave files\n"
      << std::endl
      << "config reset : reset all configuration to empty state\n"
      << std::endl
      << "config save <name> [-f] [-c|--comment \"<comment>\"] : save config under <name>\n"
      << "\t -f : overwrite existing config name and create a timestamped backup\n"
      << "\t -c : add a comment entry to the config\n";
  std::cerr << oss.str() << std::endl;
}
