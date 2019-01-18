//------------------------------------------------------------------------------
//! @file com_proto_recycle.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
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
#include "common/StringConversion.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

extern int com_recycle(char*);
void com_recycle_help();

//------------------------------------------------------------------------------
//! Class RecycleHelper
//------------------------------------------------------------------------------
class RecycleHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RecycleHelper()
  {
    mHighlight = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RecycleHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;

private:

  //----------------------------------------------------------------------------
  //! Check if given date format respects the specifications
  //!
  //! @return true if ok, otherwise false
  //----------------------------------------------------------------------------
  bool CheckDateFormat(const std::string& sdate) const;
};


//------------------------------------------------------------------------------
// Check if given date format respects the specifications
//------------------------------------------------------------------------------
bool
RecycleHelper::CheckDateFormat(const std::string& sdate) const
{
  using eos::common::StringConversion;

  if (sdate.find('/') != std::string::npos) {
    std::vector<std::string> tokens;
    StringConversion::Tokenize(sdate, tokens, "/");

    if (tokens.size() > 3) {
      return false;
    }

    // All tokens must be numeric
    for (const auto token : tokens) {
      try {
        (void) std::stoi(token);
      } catch (...) {
        return false;
      }
    }
  } else {
    try {
      (void) std::stoi(sdate);
    } catch (...) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
RecycleHelper::ParseCommand(const char* arg)
{
  const char* option {nullptr};
  std::string soption;
  eos::console::RecycleProto* recycle = mReq.mutable_recycle();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  option = tokenizer.GetToken();
  std::string cmd = (option ? option : "");

  if ((cmd == "ls") || cmd.empty() || (cmd == "-m")) {
    eos::console::RecycleProto_LsProto* ls = recycle->mutable_ls();

    if (cmd.empty()) {
      ls->set_all(true);
    } else if (cmd == "-m") {
      ls->set_monitorfmt(true);
    } else {
      ls->set_fulldetails(true);

      while ((option = tokenizer.GetToken())) {
        soption = option;

        if (soption == "-g") {
          ls->set_all(true);
        } else if (soption == "-m") {
          ls->set_monitorfmt(true);
        } else if (soption == "-n") {
          ls->set_numericids(true);
        } else {
          // This must be a date format
          if (!CheckDateFormat(soption)) {
            std::cerr << "error: \"" << soption << "\" does not respect the "
                      << "date format" << std::endl;
            return false;
          }

          ls->set_date(soption);
        }
      }

      if (ls->all() && (!ls->date().empty())) {
        std::cerr << "error: -g and <date> can not be used together"
                  << std::endl;
        return false;
      }
    }
  } else if (cmd == "purge") {
    eos::console::RecycleProto_PurgeProto* purge = recycle->mutable_purge();

    while ((option = tokenizer.GetToken())) {
      soption = option;

      if (soption == "-g") {
        purge->set_all(true);
        break;
      } else {
        if (!CheckDateFormat(soption)) {
          std::cerr << "error: \"" << soption << "\" does not respect the "
                    << "date format" << std::endl;
          return false;
        }

        purge->set_date(soption);
        break;
      }
    }

    if (purge->all() && (!purge->date().empty())) {
      std::cerr << "error: -g and <date> can not be used together"
                << std::endl;
      return false;
    }
  } else if (cmd == "restore") {
    eos::console::RecycleProto_RestoreProto* restore = recycle->mutable_restore();

    while ((option = tokenizer.GetToken())) {
      soption = option;

      if ((soption == "-f") || (soption == "--force-original-name")) {
        restore->set_forceorigname(true);
      } else if ((soption == "-r") || (soption == "--restore-versions")) {
        restore->set_restoreversions(true);
      } else {
        // This must be the recycle-key
        restore->set_key(soption);
        break;
      }
    }

    if (restore->key().empty()) {
      return false;
    }
  } else if (cmd == "config") {
    eos::console::RecycleProto_ConfigProto* config = recycle->mutable_config();

    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    soption = option;

    if ((soption == "--add-bin") || (soption == "--remove-bin")) {
      if (soption == "--add-bin") {
        config->set_op(eos::console::RecycleProto_ConfigProto::ADD_BIN);
      } else {
        config->set_op(eos::console::RecycleProto_ConfigProto::RM_BIN);
      }

      if (!(option = tokenizer.GetToken())) {
        return false;
      }

      config->set_subtree(option);
    } else if (soption == "--lifetime") {
      config->set_op(eos::console::RecycleProto_ConfigProto::LIFETIME);

      if (!(option = tokenizer.GetToken())) {
        return false;
      }

      soption = option;
      int lifetime = 0;

      try {
        lifetime = std::stoi(soption);
      } catch (...) {
        return false;
      }

      config->set_lifetimesec(lifetime);
    } else if (soption == "--ratio") {
      config->set_op(eos::console::RecycleProto_ConfigProto::RATIO);

      if (!(option = tokenizer.GetToken())) {
        return false;
      }

      soption = option;
      float ratio = 0.0;

      try {
        ratio = std::stof(soption);
      } catch (...) {
        return false;
      }

      config->set_ratio(ratio);
    } else if (soption == "--size") {
      config->set_op(eos::console::RecycleProto::ConfigProto::SIZE);

      if (!(option = tokenizer.GetToken())) {
        return false;
      }

      soption = option;
      std::set<char> units {'K', 'M', 'G'};
      uint64_t size = eos::common::StringConversion::GetSizeFromString(soption);

      if (errno) {
        std::cerr << "error: specified size could not be converted" << std::endl;
        return false;
      }

      config->set_size(size);
    } else if (soption == "--inodes") {
      config->set_op(eos::console::RecycleProto::ConfigProto::INODES);

      if (!(option = tokenizer.GetToken())) {
        return false;
      }

      soption = option;
      std::set<char> units {'K', 'M', 'G'};
      uint64_t size = eos::common::StringConversion::GetSizeFromString(soption);

      if (errno) {
        std::cerr << "error: specified number of inodes could not be converted"
                  << std::endl;
        return false;
      }

      config->set_size(size);
    } else {
      return false;
    }
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Recycle command entrypoint
//------------------------------------------------------------------------------
int com_protorecycle(char* arg)
{
  if (wants_help(arg)) {
    com_recycle_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  RecycleHelper recycle;

  if (!recycle.ParseCommand(arg)) {
    com_recycle_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = recycle.Execute(false);

  // Fallback for aquamarine server when dealing with new proto commands
  if (global_retc != 0) {
    if ((recycle.GetError().find("no such user command") != std::string::npos) ||
        (global_retc == EIO)) {
      global_retc = com_recycle(arg);
    } else {
      std::cerr << recycle.GetError() << std::endl;
    }
  }

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_recycle_help()
{
  std::ostringstream oss;
  oss << "Usage: recycle [ls|purge|restore|config ...]" << std::endl
      << "    provides recycle bin functionality" << std::endl
      << "  recycle [-m]" << std::endl
      << "    print status of recycle bin and config status if executed by root"
      << std::endl
      << "    -m     : display info in monitoring format" << std::endl
      << std::endl
      << "  recycle ls [-g|<date>] [-m] [-n]" << std::endl
      << "    list files in the recycle bin" << std::endl
      << "    -g     : list files of all users (if done by root or admin)"
      << std::endl
      << "    <date> : can be <year>, <year>/<month> or <year>/<month>/<day>"
      << std::endl
      << "             e.g.: recycle ls 2018/08/12" << std::endl
      << "    -m     : display info in monitoring format" << std::endl
      << "    -n     : dispaly numeric uid/gid(s) instead of names" << std::endl
      << std::endl
      << "  recycle purge [-g|<date>]" << std::endl
      << "    purge files in the recycle bin" << std::endl
      << "    -g     : empties the recycle bin of all users (if done by root or admin)"
      << std::endl
      << "    <date> : can be <year>, <year>/<month> or <year>/<month>/<day>"
      << std::endl
      << std::endl
      << "   recycle restore [-f|--force-original-name] [-r|--restore-versions] <recycle-key>"
      << std::endl
      << "     undo the deletion identified by the <recycle-key>" << std::endl
      << "     -f : move deleted files/dirs back to their original location (otherwise"
      << std::endl
      << "          the key entry will have a <.inode> suffix)" << std::endl
      << "     -r : restore all previous versions of a file" << std::endl
      << std::endl
      << "  recycle config [--add-bin|--remove-bin] <sub-tree>" << std::endl
      << "    --add-bin    : enable recycle bin for deletions in <sub-tree>" <<
      std::endl
      << "    --remove-bin : disable recycle bin for deletions in <sub-tree>" <<
      std::endl
      << std::endl
      << "  recycle config --lifetime <seconds>" << std::endl
      << "    configure FIFO lifetime for the recycle bin" << std::endl
      << std::endl
      << "  recycle config --ratio <0..1.0>" << std::endl
      << "    configure the volume/inode keep ratio. E.g: 0.8 means files will only"
      << std::endl
      << "    be recycled if more than 80% of the volume/inodes quota is used. The"
      << std::endl
      << "    low watermark is by default 10% below the given ratio."
      << std::endl
      << std::endl
      << "  recycle config --size <value>[K|M|G]" << std::endl
      << "    configure the quota for the maximum size of the recycle bin. "
      << std::endl
      << "    If no unit is set explicitly then we assume bytes." << std::endl
      << std::endl
      << "  recycle config --inodes <value>[K|M|G]" << std::endl
      << "    configure the quota for the maximum number of inodes in the recycle"
      << std::endl
      << "    bin." << std::endl;
  std::cerr << oss.str() << std::endl;
}
