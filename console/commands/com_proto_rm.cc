//------------------------------------------------------------------------------
// File: com_protorm.cc
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

extern int com_rm(char*);
void com_rm_help();

//------------------------------------------------------------------------------
//! Class RmHelper
//------------------------------------------------------------------------------
class RmHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  RmHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = false;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RmHelper() override = default;

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
RmHelper::ParseCommand(const char* arg)
{
  XrdOucString option;
  eos::console::RmProto* rm = mReq.mutable_rm();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();

  while ((option = tokenizer.GetToken(false)).length() > 0 &&
         (option.beginswith("-"))) {
    if ((option == "-r") || (option == "-rf") || (option == "-fr")) {
      rm->set_recursive(true);
    } else if ((option == "-F") || (option == "--no-recycle-bin")) {
      rm->set_bypassrecycle(true);
    } else if (option == "-rF" || option == "-Fr") {
      rm->set_recursive(true);
      rm->set_bypassrecycle(true);
    } else {
      return false;
    }
  }

  auto path = option;

  do {
    XrdOucString param = tokenizer.GetToken();

    if (param.length()) {
      path += " ";
      path += param;
    } else {
      break;
    }
  } while (true);

  // remove escaped blanks
  while (path.replace("\\ ", " "));

  if (path.length() == 0) {
    return false;
  }

  auto id = 0ull;

  if (Path2FileDenominator(path, id)) {
    rm->set_fileid(id);
    rm->set_recursive(false); // disable recursive option for files
    path = "";
  } else {
    if (Path2ContainerDenominator(path, id)) {
      rm->set_containerid(id);
      path = "";
    } else {
      path = abspath(path.c_str());
      rm->set_path(path.c_str());
    }
  }

  eos::common::Path cPath(path.c_str());

  if (path.length()) {
    mNeedsConfirmation = rm->recursive() && (cPath.GetSubPathSize() < 4); // "less then 4" is so arbitrary... but we use it that way. @todo review
  }

  return true;
}

//------------------------------------------------------------------------------
// Rm command entry point
//------------------------------------------------------------------------------
int com_protorm(char* arg)
{
  if (wants_help(arg)) {
    com_rm_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  RmHelper rm(gGlobalOpts);

  if (!rm.ParseCommand(arg)) {
    com_rm_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  if (rm.NeedsConfirmation() && !rm.ConfirmOperation()) {
    global_retc = EINTR;
    return EINTR;
  }

  global_retc = rm.Execute(true, true);

  return global_retc;
}

void com_rm_help()
{
  std::ostringstream oss;
  oss << "Usage: rm [-r|-rf|-rF] [--no-recycle-bin|-F] [<path>|fid:<fid-dec>|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>]"
      << std::endl
      << "            -r | -rf : remove files/directories recursively" << std::endl
      << "                     - the 'f' option is a convenience option with no additional functionality!"
      << std::endl
      << "                     - the recursive flag is automatically removed it the target is a file!"
      << std::endl << std::endl
      << " --no-recycle-bin|-F : remove bypassing recycling policies" << std::endl
      << "                     - you have to take the root role to use this flag!"
      << std::endl << std::endl
      << "            -rF | Fr : remove files/directories recursively bypassing recycling policies"
      << std::endl
      << "                     - you have to take the root role to use this flag!" <<
      std::endl
      << "                     - the recursive flag is automatically removed it the target is a file!"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
