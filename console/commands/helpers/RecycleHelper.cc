//------------------------------------------------------------------------------
//! @file RecycleHelper.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "console/commands/helpers/RecycleHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"

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
      } else if (soption =="-k") {
	option = tokenizer.GetToken();
	if (option) {
	  purge->set_key(option);
	} else {
	  std::cerr << "error: you have to provide a key when using the -k option" << std::endl;
	  return false;
	}
      } else {
        if (!CheckDateFormat(soption)) {
          std::cerr << "error: \"" << soption << "\" does not respect the "
                    << "date format" << std::endl;
          return false;
        }

        purge->set_date(soption);
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
      }  else if (soption == "-p") {
	restore->set_makepath(true);
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
