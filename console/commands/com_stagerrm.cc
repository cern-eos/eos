//------------------------------------------------------------------------------
// File: com_stagerrm.cc
// Author: Jozsef Makai - CERN
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
#include "common/Path.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_stagerrm_help();

//------------------------------------------------------------------------------
//! Class StagerRmHelper
//------------------------------------------------------------------------------
class StagerRmHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  StagerRmHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = false;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~StagerRmHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

bool
StagerRmHelper::ParseCommand(const char* arg)
{
  const char* nextToken;
  std::string snextToken;
  eos::console::StagerRmProto* stagerRm = mReq.mutable_stagerrm();
  eos::common::StringTokenizer tokenizer(arg);
  XrdOucString path = tokenizer.GetLine();

  if (!(nextToken = tokenizer.GetToken())) {
    return false;
  } else {
    snextToken = nextToken;
    if (snextToken.substr(0, 2) == "--") {
      snextToken.erase(0, 2);

      // No other option besides --fsid is accepted
      if (snextToken != "fsid") {
        return false;
      }

      // Parse fsid
      if (!(nextToken = tokenizer.GetToken())) {
        std::cerr << "error: --fsid flag needs to be followed by value" << std::endl;
        return false;
      }

      snextToken = nextToken;
      try {
        uint64_t fsid = std::stoull(snextToken);
        stagerRm->mutable_stagerrmsinglereplica()->set_fsid(fsid);
      } catch (const std::exception& e) {
        std::cerr << "error: --fsid value needs to be numeric" << std::endl;
        return false;
      }

      path = tokenizer.GetToken();
    } else {
      // There was no option, use it as first path
      path = nextToken;
    }
  }

  while (path != "") {
    // remove escaped blanks
    while (path.replace("\\ ", " "));

    if (path != "") {
      auto file = stagerRm->add_file();
      auto fid = 0ull;

      if (Path2FileDenominator(path, fid)) {
        file->set_fid(fid);
      } else {
        path = abspath(path.c_str());
        file->set_path(path.c_str());
      }
    }

    path = tokenizer.GetToken();
  }

  // at least 1 path has to be given
  return stagerRm->file_size() > 0;
}

//------------------------------------------------------------------------------
// StagerRm command entry point
//------------------------------------------------------------------------------
int com_stagerrm(char* arg)
{
  if (wants_help(arg)) {
    com_stagerrm_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  StagerRmHelper stagerRm(gGlobalOpts);

  if (!stagerRm.ParseCommand(arg)) {
    com_stagerrm_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = stagerRm.Execute();
  return global_retc;
}

void com_stagerrm_help()
{
  std::ostringstream oss;
  oss << "Usage: stagerrm <path>|fid:<fid-dec>]|fxid:<fid-hex> [<path>|fid:<fid-dec>]|fxid:<fid-hex>] ..."
      << std::endl
      << "       Removes all disk replicas of the given files separated by space"
      << std::endl
      << "       This command requires write and p acl flag permission"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
