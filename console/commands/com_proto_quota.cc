//------------------------------------------------------------------------------
// File: com_proto_quota.cc
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


#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include <iomanip>
#include <map>
#include "console/commands/ICmdHelper.hh"


void com_quota_help();

//------------------------------------------------------------------------------
//! Class QuotaHelper
//------------------------------------------------------------------------------
class QuotaHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaHelper()
  {
    mHighlight = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QuotaHelper() = default;

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
bool QuotaHelper::ParseCommand(const char* arg)
{
  eos::console::QuotaProto* quota = mReq.mutable_quota();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;


  tokenizer.NextToken(token);

  if (token == "" || token == "-m" || (token.find('/') == 0)) { // ... or begins with "/"
    // lsuser
    std::string aux;
    eos::console::QuotaProto_LsuserProto* lsuser = quota->mutable_lsuser();

    if (token == "") {
      aux = DefaultRoute();
      if ( aux.find('/') == 0 ) {
        lsuser->set_space(aux);
      }
    } else if (token == "-m") {
      lsuser->set_format(true);
      aux = DefaultRoute();
      if ( aux.find('/') == 0 ) {
        lsuser->set_space(aux);
      }
    } else if ( token.find('/') == 0 ) {
      lsuser->set_space(token);
      tokenizer.NextToken(token);
      if ( token == "-m" ) {
        lsuser->set_format(true);
      }
    }
    return true;
  }
  else if ( token == "ls" ) {

    eos::console::QuotaProto_LsProto *ls = quota->mutable_ls();

    while (tokenizer.NextToken(token)) {
      if (token == "--uid" || token == "-u") {
        if (tokenizer.NextToken(token)) {
          ls->set_uid(token);
        } else {
          return false;
        }
      } else if (token == "--gid" || token == "-g") {
        if (tokenizer.NextToken(token)) {
          ls->set_gid(token);
        } else {
          return false;
        }
      } else if (token == "-m") {
        mHighlight = false;
        ls->set_format(true);
      } else if (token == "-n") {
        ls->set_printid(true);
      } else if (token == "--path" || token == "-p" || (token.find('/') == 0)) {
        if (token == "--path" || token == "-p") {
          if (tokenizer.NextToken(token)) {
            ls->set_space(token);
          } else {
            return false;
          }
        } else if (token.find('/') == 0) {
          ls->set_space(token);
          if (tokenizer.NextToken(token)) { // for convenience can omit --path and use /some/path/ as *last* argument - e.g. quota ls /eos/ ...
            return false;
          }
        }
      } else { // no proper argument
        return false;
      }
    }
  }
  else if ( token == "set" ) {
    eos::console::QuotaProto_SetProto *set = quota->mutable_set();

    while (tokenizer.NextToken(token)) {
      if (token == "--uid" || token == "-u") {
        if (tokenizer.NextToken(token)) {
          set->set_uid(token);
        } else {
          return false;
        }
      } else if (token == "--gid" || token == "-g") {
        if (tokenizer.NextToken(token)) {
          set->set_gid(token);
        } else {
          return false;
        }
      } else if (token == "--volume" || token == "-v") {
        if (tokenizer.NextToken(token)) {
          set->set_maxbytes(token);
        } else {
          return false;
        }
      } else if (token == "--inodes" || token == "-i") {
        if (tokenizer.NextToken(token)) {
          set->set_maxinodes(token);
        } else {
          return false;
        }
      } else if (token == "--path" || token == "-p" || (token.find('/') == 0)) {
        if (token == "--path" || token == "-p") {
          if (tokenizer.NextToken(token)) {
            set->set_space(token);
          } else {
            return false;
          }
        } else if (token.find('/') == 0) {
          set->set_space(token);
          if (tokenizer.NextToken(token)) { // for convenience can omit --path and use /some/path/ as *last* argument - e.g. quota set /eos/ ...
            return false;
          }
        }
      } else { // no proper argument
        return false;
      }
    }
  }
  else if ( token == "rm" ) {
    eos::console::QuotaProto_RmProto *rm = quota->mutable_rm();

    while (tokenizer.NextToken(token)) {
      if (token == "--uid" || token == "-u") {
        if (tokenizer.NextToken(token)) {
          rm->set_uid(token);
        } else {
          return false;
        }
      } else if (token == "--gid" || token == "-g") {
        if (tokenizer.NextToken(token)) {
          rm->set_gid(token);
        } else {
          return false;
        }
      } else if (token == "--volume" || token == "-v") {
        rm->set_type(eos::console::QuotaProto_RmProto::VOLUME);
      } else if (token == "--inode" || token == "-i") {
        rm->set_type(eos::console::QuotaProto_RmProto::INODE);
      }
      else if (token == "--path" || token == "-p" || (token.find('/') == 0)) {
        if (token == "--path" || token == "-p") {
          if (tokenizer.NextToken(token)) {
            rm->set_space(token);
          } else {
            return false;
          }
        } else if (token.find('/') == 0) {
          rm->set_space(token);
          if (tokenizer.NextToken(token)) { // for convenience can omit --path and use /some/path/ as *last* argument - e.g. quota rm /eos/ ...
            return false;
          }
        }
      } else { // no proper argument
        return false;
      }
    }

  }
  else if ( token == "rmnode" ) {
    eos::console::QuotaProto_RmnodeProto *rmnode = quota->mutable_rmnode();

    if (token == "--path" || token == "-p") {
      if (tokenizer.NextToken(token)) {
        rmnode->set_space(token);
      } else {
        return false;
      }
    } else { // no proper argument
        return false;
    }

    std::cout << "Do you really want to delete the quota node under path" << token << " ?\n";
    std::cout << "Confirm the deletion by typing => ";
    std::string random_confirmation_string;
    for (int i = 0; i < 10; i++) {
      // coverity[DC.WEAK_CRYPTO]
      random_confirmation_string += std::to_string((int)(9.0 * rand() / RAND_MAX));
    }
    std::cout << random_confirmation_string << '\n';
    std::cout << "                               => ";

    std::string in_string;
    std::cin >> in_string;

    if (in_string == random_confirmation_string) {
      std::cout << "\nSending deletion request to server ...\n";
    } else {
      std::cout << "\nDeletion aborted!\n";
      return false;
    }

  }
  else { // no proper subcommand
    return false;
  }

  return true;

}

//------------------------------------------------------------------------------
// quota command entry point
//------------------------------------------------------------------------------
int com_protoquota(char* arg) {

  if (wants_help(arg)) {
    com_quota_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  QuotaHelper quota;

  if (!quota.ParseCommand(arg)) {
    com_quota_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = quota.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_quota_help() {
  std::ostringstream oss;
  std::vector<std::uint32_t> col_size = {0, 0};
  std::map<std::string, std::string> map_cmds = {
      {
          "quota [<path>]",
          ": show personal quota for all or only the quota node responsible for <path>"
      },
      {
          "quota ls [-n] [-m] [-u <uid>] [-g <gid>] [[-p] <path>]",
          ": list configured quota and quota node(s)"
      },
      {
          "quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] [[-p] <path>]",
          ": set volume and/or inode quota by uid or gid"
      },
      {
          "quota rm -u <uid>|-g <gid> [-v] [-i] [[-p] <path>]",
          ": remove configured quota type(s) for uid/gid in path"
      },
      {
          "quota rmnode -p <path>",
          ": remove quota node and every defined quota on that node"
      }
  };

  // Compute max width for command and description table
  for (auto it = map_cmds.begin(); it != map_cmds.end(); ++it) {
    if (col_size[0] < it->first.length()) {
      col_size[0] = it->first.length() + 1;
    }

    if (col_size[1] < it->second.length()) {
      col_size[1] = it->second.length() + 1;
    }
  }

  std::int8_t tab_size = 2;
  std::string usage_txt = "Usage:";
  std::string opt_txt = "General options:";
  std::string notes_txt = "Notes:";
  oss << usage_txt << std::endl;

  // Print the command and their description
  for (auto it = map_cmds.begin(); it != map_cmds.end(); ++it) {
    oss << std::setw(usage_txt.length()) << ""
        << std::setw(col_size[0]) << std::setiosflags(std::ios_base::left)
        << it->first
        << std::setw(col_size[1]) << std::setiosflags(std::ios_base::left)
        << it->second
        << std::endl;
  }

  std::uint32_t indent_len = usage_txt.length() + tab_size;
  // Print general options
  oss << std::endl << std::setw(usage_txt.length()) << ""
      << opt_txt << std::endl
      << std::setw(indent_len) << ""
      << "-m : print information in monitoring <key>=<value> format" << std::endl
      << std::setw(indent_len) << ""
      << "-n : don't translate ids, print uid and gid number" << std::endl
      << std::setw(indent_len) << ""
      << "-u/--uid <uid> : print information only for uid <uid>" << std::endl
      << std::setw(indent_len) << ""
      << "-g/--gid <gid> : print information only for gid <gid>" << std::endl
      << std::setw(indent_len) << ""
      << "-p/--path <path> : print information only for path <path> - this "
      << "can also be given without -p or --path" << std::endl
      << std::setw(indent_len) << ""
      << "-v/--volume <bytes> : refer to volume limit in <bytes>" << std::endl
      << std::setw(indent_len) << ""
      << "-i/--inodes <inodes> : refer to inode limit in number of <inodes>"
      << std::endl;
  indent_len = usage_txt.length() + tab_size;
  // Print extra notes
  oss << std::endl << std::setw(usage_txt.length()) << ""
      << notes_txt << std::endl
      << std::setw(indent_len) << ""
      << "=> you have to specify either the user or the group identified by the "
      << "unix id or the user/group name" << std::endl
      << std::setw(indent_len) << ""
      << "=> the space argument is by default assumed as 'default'" << std::endl
      << std::setw(indent_len) << ""
      << "=> you have to specify at least a volume or an inode limit to set quota" <<
      std::endl
      << std::setw(indent_len) << ""
      << "=> for convenience all commands can just use <path> as last argument "
      << "ommitting the -p|--path e.g. quota ls /eos/ ..." << std::endl
      << std::setw(indent_len) << ""
      << "=> if <path> is not terminated with a '/' it is assumed to be a file "
      << "so it won't match the quota node with <path>/ !" << std::endl;
  fprintf(stdout, "%s", oss.str().c_str());
}
