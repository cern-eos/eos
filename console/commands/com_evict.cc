//------------------------------------------------------------------------------
// File: com_evict.cc
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

void com_evict_help();

//------------------------------------------------------------------------------
//! Class EvictHelper
//------------------------------------------------------------------------------
class EvictHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  EvictHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = false;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~EvictHelper() override = default;

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
EvictHelper::ParseCommand(const char* arg)
{
  const char* nextToken;
  std::string sNextToken;
  eos::console::EvictProto* evict = mReq.mutable_evict();
  eos::common::StringTokenizer tokenizer(arg);
  XrdOucString path = tokenizer.GetLine();

  if (!(nextToken = tokenizer.GetToken())) {
    return false;
  }


  for (
      sNextToken = nextToken;
      sNextToken == "--fsid" || sNextToken == "--ignore-evict-counter" || sNextToken == "--ignore-removal-on-fst";
      nextToken = tokenizer.GetToken(), sNextToken = nextToken ? nextToken : ""
      ) {
    if (sNextToken == "--ignore-evict-counter") {
      evict->set_ignoreevictcounter(true);
    } else if (sNextToken == "--ignore-removal-on-fst") {
      evict->set_ignoreremovalonfst(true);
    } else if (sNextToken == "--fsid") {
      if (!(nextToken = tokenizer.GetToken())) {
        std::cerr << "error: --fsid needs to be followed by value" << std::endl;
        return false;
      } else {
        try {
          uint64_t fsid = std::stoull(nextToken);
          evict->mutable_evictsinglereplica()->set_fsid(fsid);
        } catch (const std::exception& e) {
          std::cerr << "error: --fsid value needs to be numeric" << std::endl;
          return false;
        }
      }
    }
  }

  if (evict->has_evictsinglereplica() && !evict->ignoreevictcounter()) {
    std::cerr << "error: --fsid can only be used with --ignore-evict-counter" << std::endl;
    return false;
  }

  if (!evict->has_evictsinglereplica() && evict->ignoreremovalonfst()) {
    std::cerr << "error: --ignore-removal-on-fst can only be used with --fsid" << std::endl;
    return false;
  }

  path = nextToken;
  while (path != "") {
    // remove escaped blanks
    while (path.replace("\\ ", " "));

    if (path != "") {
      auto file = evict->add_file();
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
  return evict->file_size() > 0;
}

//------------------------------------------------------------------------------
// Evict command entry point
//------------------------------------------------------------------------------
int com_evict(char* arg)
{
  if (wants_help(arg)) {
    com_evict_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  EvictHelper evict(gGlobalOpts);

  if (!evict.ParseCommand(arg)) {
    com_evict_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = evict.Execute();
  return global_retc;
}

void com_evict_help()
{
  std::ostringstream oss;
  oss << "Usage: evict [--fsid <fsid>] [--ignore-removal-on-fst] [--ignore-evict-counter] <path>|fid:<fid-dec>]|fxid:<fid-hex> [<path>|fid:<fid-dec>]|fxid:<fid-hex>] ...\n"
      << "    Removes disk replicas of the given files, separated by space\n"
      << std::endl
      << "  Optional arguments:\n"
      << "    --ignore-evict-counter  : Force eviction by bypassing evict counter\n"
      << "    --fsid <fsid>           : Evict disk copy only from a single fsid\n"
      << "    --ignore-removal-on-fst : Ignore file removal on fst, namespace-only operation\n"
      << std::endl
      << "    This command requires 'write' and 'p' acl flag permission\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
